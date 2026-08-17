#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
validate_products.py
====================

Descriptive validator for CDB-simplificado, full CDB, and RDB synthetic datasets
produced by `engorda_tables.py`. It runs on the ENGORDA OUTPUT (the synthetic Parquet under
DATAGEN_SYNTHETIC_BASE_URI) and checks it against the ACTUAL application rules of
the CETIP/NoMe platform, so structural/domain violations are caught BEFORE the
Oracle append and before the daily/operational batch runs on top of the data.

It is fully self-contained: it does NOT import from `engorda_tables.py`.

Authoritative rules (PK / FK graph / NOT NULL / column types and capacities) are read from the
Oracle data dictionary (ALL_* views) over JDBC. An optional application-capacity
contract supplies global code-level limits. A few semantic rules that are
NOT expressible in schema metadata (the CONDICAO_IF polymorphic subtype map and
product predicates) are curated below and, where possible,
verified against production data.

The eight check categories map directly to the failures observed in the
batch validation log:

  Cat 1  CONDICAO_IF polymorphism  -> ClassCastException JurosFlutuanteDO -> JurosFixoDO
  Cat 2  Domain conformance        -> out-of-product rows (FILTROS_FONTE image)
  Cat 3  Referential integrity     -> FK orphans / broken remap
  Cat 4  NOT NULL / capacity       -> ORA-01400, ORA-06502, ORA-12899 and ORA-01438
                                      (e.g. TCTPDETALHE_TRAN_SEM_FINA.COD_MOTIVO)
  Cat 5  Date coherence            -> bad revaluation input
  Cat 6  Lookup combinations       -> "SEM MODALIDADE / servico_ft nao encontrado"
  Cat 7  Shape conformance         -> per-IF cardinalities vs the production profile
                                      (see docs/cdb-shapes-findings.md)
  Cat 8  Log-derived invariants    -> registration uniqueness and optional persisted profile

Category 7 compares the per-instrument cardinality distribution ("shapes") of
the synthetic output against a baseline profile produced by
scripts/profile_cdb_shapes.py on the FILTERED raw data:

  spark-submit profile_cdb_shapes.py --product cdb_simplificado \
      --base-uri <raw> --apply-filtros-fonte \
      --label raw_filtered --report-path oci://.../profile_raw_filtered.json

Pass that JSON via --shape-baseline. Without it, only profile-enabled baseline-free
invariants run (for example, the operation ratio and simplificado resgate maximum),
and distribution checks are skipped with a WARN.

Environment variables
---------------------
  DATAGEN_SYNTHETIC_BASE_URI     base URI of the synthetic output (required)
  DATAGEN_SYNTHETIC_PREFIX       optional sub-prefix under the base
  DATAGEN_SOURCE_JDBC_URL        jdbc:oracle:thin:@host:1521:sid  (required unless --no-oracle)
  DATAGEN_SOURCE_DB_USER         Oracle user
  DATAGEN_SOURCE_DB_PASSWORD     Oracle password
  DATAGEN_SOURCE_SCHEMA          owner filter for ALL_* views (default: CETIP)

Usage
-----
  spark-submit --jars ojdbc8.jar validate_products.py \
      --product cdb_simplificado --report-path report.json \
      --fail-severity error --validate-against union
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import reduce
from time import perf_counter
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_products")


def _timed(label: str, operation):
    """Run an operation and log elapsed wall time without changing its result."""
    started = perf_counter()
    logger.info("[PERF] start %s", label)
    try:
        return operation()
    finally:
        logger.info("[PERF] end %s elapsed=%.1fs", label, perf_counter() - started)


def _check_is_skipped(check_id: str, skip_prefixes: List[str]) -> bool:
    return any(check_id.startswith(prefix) for prefix in skip_prefixes)


def _check_group_is_skipped(check_prefixes: Tuple[str, ...],
                            skip_prefixes: List[str]) -> bool:
    return bool(check_prefixes) and all(
        any(prefix.startswith(skip) for skip in skip_prefixes)
        for prefix in check_prefixes
    )


def _run_check_group(label: str, check_prefixes: Tuple[str, ...],
                     skip_prefixes: List[str], operation) -> List["Finding"]:
    """Skip a whole check group before Spark work when every check is excluded."""
    if _check_group_is_skipped(check_prefixes, skip_prefixes):
        logger.info("Skipped %s before execution (--skip-check).", label)
        return []
    return [
        finding for finding in _timed(label, operation)
        if not _check_is_skipped(finding.check_id, skip_prefixes)
    ]

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"

# ---------------------------------------------------------------------------
# Severities and exit policy
# ---------------------------------------------------------------------------
SEV_ERROR = "ERROR"
SEV_WARN = "WARN"
SEV_INFO = "INFO"
_SEV_ORDER = {SEV_INFO: 0, SEV_WARN: 1, SEV_ERROR: 2}

# ---------------------------------------------------------------------------
# Semantic rules NOT derivable from schema metadata.
# ---------------------------------------------------------------------------
# COD_TIPO_CONDICAO_IF -> physical joined-subclass table (from TipoCondicaoIFDO
# constants + CondicaoIFDO.hbm.xml). The subtype of a CONDICAO_IF row is resolved
# by Hibernate SOLELY by which subclass table holds its NUM_CONDICAO_IF (there is
# no <discriminator>); this map is what the application code assumes when it casts.
SUBTYPE_BY_TIPO: Dict[str, str] = {
    "1": "AMORTIZACAO",
    "2": "JUROS_FIXO",
    "3": "JUROS_FLUTUANTE",
    "4": "ATUALIZACAO_POS",
    "5": "SPREAD",
    "6": "PARTICIPACAO_LUCROS",
    "7": "PREMIO",
    "14": "ATUALIZACAO_PRE",
    "15": "PREMIO_OPCAO",
    "16": "TERMO",
    "17": "PARAMETRO_LIMITE",
    "20": "RESGATE",
    "21": "PREMIO_CONTRATO",
    "22": "OPCAO",
    "23": "RESET",
    "24": "DESDOBRAMENTO",
}
CONDICAO_IF_TABLE = "CONDICAO_IF"
CONDICAO_IF_PK = "NUM_CONDICAO_IF"
CONDICAO_IF_TIPO_COL = "COD_TIPO_CONDICAO_IF"

# Complete COD_TIPO_CONDICAO_IF inventory from the application
# (framework/.../instrumentofinanceiro/TipoCondicaoIFDO.java:42-73). Every code that can
# appear in production must have a REVIEWED physical joined-subclass mapping in
# SUBTYPE_BY_TIPO before a product using it can be validated strictly. Codes present here
# but absent from SUBTYPE_BY_TIPO (8 TRIGGER_IN, 18 TRIGGER_OUT, 19 TERMO_MOEDA,
# 25 TERMO_COMMODITY, 26 PAGTO_MONETARIO, 27 TERMO_INDICE, 28 CORRECAO, 29 TERMO_FLUXO,
# 30 TRIGGER_CCP) are NOT guessed: their physical subclass table was not confirmed in
# CondicaoIFDO.hbm.xml during this pass, so an observed instance is reported as
# `1b.unknown_tipo` (WARN) for review rather than silently accepted.
EXPECTED_CONDICAO_TYPE_CODES: Dict[str, str] = {
    "1": "AMORTIZACAO", "2": "JUROS_FIXO", "3": "JUROS_FLUTUANTE", "4": "ATUALIZACAO_POS",
    "5": "SPREAD", "6": "PARTICIPACAO_NOS_LUCROS", "7": "PREMIO", "8": "TRIGGER_IN",
    "14": "ATUALIZACAO_PRE", "15": "PREMIO_OPCAO", "16": "TERMO", "17": "PARAMETRO_LIMITE",
    "18": "TRIGGER_OUT", "19": "TERMO_MOEDA", "20": "RESGATE", "21": "PREMIO_CONTRATO",
    "22": "OPCAO", "23": "RESET", "24": "DESDOBRAMENTO", "25": "TERMO_COMMODITY",
    "26": "PAGTO_MONETARIO", "27": "TERMO_INDICE", "28": "CORRECAO", "29": "TERMO_FLUXO",
    "30": "TRIGGER_CCP",
}
# Codes with a physical subclass table still pending confirmation (must be reviewed against
# CondicaoIFDO.hbm.xml before full-product/RDB strict rollout).
UNMAPPED_CONDICAO_TYPE_CODES: Tuple[str, ...] = tuple(
    code for code in EXPECTED_CONDICAO_TYPE_CODES if code not in SUBTYPE_BY_TIPO
)

# Category 2 domain conformance is evaluated per-instrument with EXISTS semantics
# (see build_eligible_num_ifs / check_domain), matching the product SQL definitions in
# tests/{cdb_simplificado,cdb,rdb}.sql. The former row-level DOMAIN_RULES table was
# removed because it wrongly rejected valid copied closure rows (e.g. a title/resgate that
# did not itself match the qualifying predicate although its instrument qualified).

# Date-ordering business rules: (table, left_col, op, right_col). Compared only
# where both dates are present. op is "<=" or "<".
DATE_RULES: List[Tuple[str, str, str, str]] = [
    ("INSTRUMENTO_FINANCEIRO", "DAT_EMISSAO", "<=", "DAT_VENCIMENTO"),
    ("INSTRUMENTO_FINANCEIRO", "DAT_REGISTRO", "<=", "DAT_VENCIMENTO"),
    ("TITULO", "DAT_EMISSAO", "<=", "DAT_VENCIMENTO"),
    ("CONDICAO_IF", "DAT_INICIO_CONDICAO_IF", "<=", "DAT_FIM_CONDICAO_IF"),
]

OPERACAO_TABLE = "OPERACAO"
DADO_OPERACAO_TABLE = "DADO_OPERACAO"
OPERACAO_KEY_COL = "NUM_ID_OPERACAO"
TIPO_OPER_OBJETO_SERV_TABLE = "TIPO_OPER_OBJETO_SERV"
V_PARAMETRO_SIC_TABLE = "V_PARAMETRO_SIC"
TIPO_OPERACAO_TABLE = "TIPO_OPERACAO"
CONTA_PARTICIPANTE_TABLE = "CONTA_PARTICIPANTE"
V_FAMILIA_CONTAS_TABLE = "V_FAMILIA_CONTAS"
V_OBJETOS_SERVICO_TABLE = "V_OBJETOS_SERVICO"
ACCOUNT_REFERENCES: Tuple[Tuple[str, str], ...] = (
    ("TITULO", "NUM_CONTA_PARTICIPANTE"),
    ("DEPOSITO_AUTOMATICO_IF", "NUM_CONTA_PARTICIPANTE"),
    ("OPERACAO", "NUM_CONTA_PARTICIPANTE_P1"),
    ("OPERACAO", "NUM_CONTA_PARTICIPANTE_P2"),
)
CDB_TIPO_IF = 49
CDB_OBJETO_SERVICO = 44
SEM_MODALIDADE_IDS = (6, 16)


# ---------------------------------------------------------------------------
# Product profiles (multi-product validation contract)
# ---------------------------------------------------------------------------
# Evidence anchors (application source under framework/):
#   NUM_TIPO_IF: CDB = 49 (TipoIFDO.java:49), RDB = 50 (TipoIFDO.java:56).
#   Object service: CDB = 44 (ObjetoServicoDO.java:52), RDB = 45 (ObjetoServicoDO.java:80).
#   CONDICAO_IF polymorphism: joined-subclass, no discriminator (CondicaoIFDO.hbm.xml)
#     + type codes (TipoCondicaoIFDO.java:42-73).
#   COD_IF allocator: CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(num_tipo_if, date)
#     (engorda_tables.py:2235).
# Unresolved RDB values (COD_IF format body, platform code, modalidade, account rule,
# registration constants, shape) are NOT filled with CDB defaults; the profile marks them
# unsupported so the run is reported PARTIAL until Task 8 evidence closes them.
#
# Capability ledger: every product declares what STRICT semantic validation *requires*
# and what current evidence *supports*. Any required-but-unsupported capability produces an
# explicit unsupported finding (see build_capability_findings) and mechanically forces a
# PARTIAL verdict; it cannot vanish because a check function was disabled.
CAP_IDENTITY = "identity"
CAP_DOMAIN = "domain"
CAP_POLYMORPHISM = "polymorphism"
CAP_REFERENTIAL = "referential"
CAP_NOT_NULL = "not_null"
CAP_CAPACITY = "capacity"
CAP_DATES = "dates"
CAP_PRIMARY_KEYS = "primary_keys"
CAP_CLONE_MAP = "clone_map"
CAP_LOOKUP_TOS = "lookup_tos"
CAP_PLATFORM = "platform"
CAP_ACCOUNT = "account"
CAP_MODALIDADE = "modalidade"
CAP_COD_IF_FORMAT = "cod_if_format"
CAP_COD_OPERACAO_FORMAT = "cod_operacao_format"
CAP_MEU_NUMERO = "meu_numero"
CAP_SHAPE = "shape"
CAP_REGISTRATION_PROFILE = "registration_profile"
CAP_CREDITO_SCR_GRAPH = "credito_scr_graph"
CAP_CREDITO_SCR_LOOKUPS = "credito_scr_lookups"
CAP_IPOC_UNIQUENESS = "ipoc_uniqueness"
CAP_DICRE_GRAPH = "dicre_graph"
CAP_DICRE_TARGET_ELIGIBILITY = "dicre_target_eligibility"
CAP_LCI_METADATA = "lci_metadata"
CAP_LCI_GRAPH = "lci_graph"
CAP_LCI_TARGET_ELIGIBILITY = "lci_target_eligibility"
CAP_LCA_METADATA = "lca_metadata"
CAP_LCA_GRAPH = "lca_graph"
CAP_LCA_TARGET_ELIGIBILITY = "lca_target_eligibility"

ALL_CAPABILITIES: Tuple[str, ...] = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_POLYMORPHISM, CAP_REFERENTIAL, CAP_NOT_NULL,
    CAP_CAPACITY, CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_LOOKUP_TOS,
    CAP_PLATFORM, CAP_ACCOUNT, CAP_MODALIDADE, CAP_COD_IF_FORMAT,
    CAP_COD_OPERACAO_FORMAT, CAP_MEU_NUMERO, CAP_SHAPE, CAP_REGISTRATION_PROFILE,
)

# Hard shape-rule identifiers (subset toggled per product via hard_shape_rules).
SHAPE_RULE_OP_RATIO = "op_ratio"
SHAPE_RULE_RESGATE_MAX = "resgate_max"
SHAPE_RULE_DISTRIBUTION = "distribution"


@dataclass(frozen=True)
class ValidationProfile:
    name: str
    num_tipo_if: int
    default_clone_prefix: str
    simplified_domain: bool
    object_service_id: int
    object_service_code: Optional[str]
    cod_if_pattern: Optional[str]
    sic_enabled: bool
    platform_check_enabled: bool
    account_check_enabled: bool
    sem_modalidade_ids: Optional[Tuple[int, ...]]
    hard_shape_rules: Tuple[str, ...]
    registration_constants: Optional[Dict[str, Dict[str, object]]]
    required_capabilities: Tuple[str, ...]
    supported_capabilities: Tuple[str, ...]
    evidence_version: int
    pipeline: str = "instrumento_financeiro"

    def unsupported_required(self) -> Tuple[str, ...]:
        supported = set(self.supported_capabilities)
        return tuple(c for c in self.required_capabilities if c not in supported)


# CDB simplificado — the known-good product; every capability is supported.
_SIMPLIFICADO_REQUIRED = ALL_CAPABILITIES
# Full CDB — strict structural + domain + lookup; simplificado-only shape/registration
# rules are intentionally NOT required (they must not fail a valid escalonado/multi-resgate
# CDB). CDB object service 44 and the CDB code prefix are evidence-backed.
_CDB_REQUIRED = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_POLYMORPHISM, CAP_REFERENTIAL, CAP_NOT_NULL,
    CAP_CAPACITY, CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_LOOKUP_TOS,
    CAP_PLATFORM, CAP_ACCOUNT, CAP_MODALIDADE, CAP_COD_IF_FORMAT,
    CAP_COD_OPERACAO_FORMAT, CAP_MEU_NUMERO,
)
_CDB_SUPPORTED = tuple(
    capability for capability in _CDB_REQUIRED if capability != CAP_POLYMORPHISM
)
# RDB — strict structural mode only. Product-specific lookup, subtype allow-list,
# shape, and registration semantics remain BLOCKED until target evidence is captured.
_RDB_REQUIRED = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_POLYMORPHISM, CAP_REFERENTIAL, CAP_NOT_NULL,
    CAP_CAPACITY, CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_LOOKUP_TOS,
    CAP_COD_OPERACAO_FORMAT, CAP_MEU_NUMERO,
    # Blocked pending evidence — required so their absence is reported, not silently skipped:
    CAP_PLATFORM, CAP_ACCOUNT, CAP_MODALIDADE, CAP_COD_IF_FORMAT, CAP_SHAPE,
    CAP_REGISTRATION_PROFILE,
)
_RDB_SUPPORTED = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_REFERENTIAL, CAP_NOT_NULL, CAP_CAPACITY,
    CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_COD_OPERACAO_FORMAT,
    CAP_MEU_NUMERO,
)
_CREDITO_SCR_REQUIRED = (
    CAP_IDENTITY, CAP_REFERENTIAL, CAP_NOT_NULL, CAP_CAPACITY, CAP_DATES,
    CAP_PRIMARY_KEYS, CAP_CREDITO_SCR_GRAPH, CAP_CREDITO_SCR_LOOKUPS,
    CAP_IPOC_UNIQUENESS, CAP_REGISTRATION_PROFILE,
)
_DICRE_REQUIRED = (
    CAP_IDENTITY, CAP_REFERENTIAL, CAP_NOT_NULL, CAP_CAPACITY, CAP_DATES,
    CAP_PRIMARY_KEYS, CAP_DICRE_GRAPH, CAP_DICRE_TARGET_ELIGIBILITY,
    CAP_IPOC_UNIQUENESS, CAP_REGISTRATION_PROFILE,
)
_LCI_REQUIRED = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_POLYMORPHISM, CAP_REFERENTIAL, CAP_NOT_NULL,
    CAP_CAPACITY, CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_SHAPE,
    CAP_REGISTRATION_PROFILE, CAP_LCI_METADATA, CAP_LCI_GRAPH,
    CAP_LCI_TARGET_ELIGIBILITY,
)
_LCA_REQUIRED = (
    CAP_IDENTITY, CAP_DOMAIN, CAP_POLYMORPHISM, CAP_REFERENTIAL, CAP_NOT_NULL,
    CAP_CAPACITY, CAP_DATES, CAP_PRIMARY_KEYS, CAP_CLONE_MAP, CAP_SHAPE,
    CAP_REGISTRATION_PROFILE, CAP_LCA_METADATA, CAP_LCA_GRAPH,
    CAP_LCA_TARGET_ELIGIBILITY,
)

VALIDATION_PROFILES: Dict[str, ValidationProfile] = {
    "cdb_simplificado": ValidationProfile(
        name="cdb_simplificado",
        num_tipo_if=49,
        default_clone_prefix="sintetizacao_multiproduto/cdb_simplificado",
        simplified_domain=True,
        object_service_id=44,
        object_service_code="CDB",
        cod_if_pattern=r"^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$",
        sic_enabled=True,
        platform_check_enabled=True,
        account_check_enabled=True,
        sem_modalidade_ids=(6, 16),
        hard_shape_rules=(SHAPE_RULE_OP_RATIO, SHAPE_RULE_RESGATE_MAX, SHAPE_RULE_DISTRIBUTION),
        registration_constants=None,  # bound below to REGISTRATION_CONSTANTS
        required_capabilities=_SIMPLIFICADO_REQUIRED,
        supported_capabilities=_SIMPLIFICADO_REQUIRED,
        evidence_version=1,
    ),
    "cdb": ValidationProfile(
        name="cdb",
        num_tipo_if=49,
        default_clone_prefix="sintetizacao_multiproduto/cdb_completo",
        simplified_domain=False,
        object_service_id=44,
        object_service_code="CDB",
        cod_if_pattern=r"^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$",
        sic_enabled=True,
        platform_check_enabled=True,
        account_check_enabled=True,
        sem_modalidade_ids=(6, 16),
        hard_shape_rules=(SHAPE_RULE_OP_RATIO,),
        registration_constants=None,
        required_capabilities=_CDB_REQUIRED,
        # Full-product polymorphism remains partial until every application type code has a
        # reviewed physical-table mapping.
        supported_capabilities=_CDB_SUPPORTED,
        evidence_version=1,
    ),
    "rdb": ValidationProfile(
        name="rdb",
        num_tipo_if=50,
        default_clone_prefix="sintetizacao_multiproduto/rdb_completo",
        simplified_domain=False,
        object_service_id=45,  # ObjetoServicoDO.java:80 (NOT 44)
        object_service_code=None,  # UNKNOWN — do not assume 'RDB'
        # Generic CodigoIF normalization only. The RDB prefix/allocator remains unknown,
        # so CAP_COD_IF_FORMAT intentionally stays unsupported and forces PARTIAL.
        cod_if_pattern=r"^[A-Z0-9 -]{1,14}$",
        sic_enabled=False,  # (50,45) SIC mapping unverified — Task 8
        platform_check_enabled=False,
        account_check_enabled=False,
        sem_modalidade_ids=None,  # UNKNOWN for tipo 50
        hard_shape_rules=(),  # no type-50 baseline yet
        registration_constants=None,
        required_capabilities=_RDB_REQUIRED,
        supported_capabilities=_RDB_SUPPORTED,
        evidence_version=1,
    ),
    "lci": ValidationProfile(
        name="lci",
        num_tipo_if=81,
        default_clone_prefix="sintetizacao_multiproduto/lci",
        simplified_domain=True,
        object_service_id=75,
        object_service_code="LCI",
        # The single observed allocator sample is advisory, not a hard format contract.
        cod_if_pattern=None,
        sic_enabled=False,
        platform_check_enabled=True,
        account_check_enabled=True,
        sem_modalidade_ids=None,
        hard_shape_rules=(SHAPE_RULE_DISTRIBUTION,),
        registration_constants=None,
        required_capabilities=_LCI_REQUIRED,
        supported_capabilities=_LCI_REQUIRED,
        evidence_version=1,
        pipeline="lci",
    ),
    "lca": ValidationProfile(
        name="lca",
        num_tipo_if=96,
        default_clone_prefix="sintetizacao_multiproduto/lca",
        simplified_domain=True,
        object_service_id=843,
        object_service_code="LCA",
        cod_if_pattern=None,
        sic_enabled=False,
        platform_check_enabled=True,
        account_check_enabled=True,
        sem_modalidade_ids=None,
        hard_shape_rules=(SHAPE_RULE_DISTRIBUTION,),
        registration_constants=None,
        required_capabilities=_LCA_REQUIRED,
        supported_capabilities=_LCA_REQUIRED,
        evidence_version=1,
        pipeline="lca",
    ),
    "credito_scr": ValidationProfile(
        name="credito_scr",
        # 143 is observed in one Lastro-LCI batch, not used as product identity.
        num_tipo_if=143,
        default_clone_prefix="sintetizacao_multiproduto/credito_scr",
        simplified_domain=False,
        object_service_id=-1,
        object_service_code=None,
        cod_if_pattern=None,
        sic_enabled=False,
        platform_check_enabled=False,
        account_check_enabled=False,
        sem_modalidade_ids=None,
        hard_shape_rules=(),
        registration_constants=None,
        required_capabilities=_CREDITO_SCR_REQUIRED,
        supported_capabilities=_CREDITO_SCR_REQUIRED,
        evidence_version=1,
        pipeline="credito_scr",
    ),
    "dicre": ValidationProfile(
        name="dicre",
        # DICRE identity is CREDITO_DC, not any one subtype NUM_TIPO_IF.
        num_tipo_if=-1,
        default_clone_prefix="sintetizacao_multiproduto/dicre",
        simplified_domain=False,
        object_service_id=-1,
        object_service_code=None,
        cod_if_pattern=None,
        sic_enabled=False,
        platform_check_enabled=False,
        account_check_enabled=False,
        sem_modalidade_ids=None,
        hard_shape_rules=(),
        registration_constants=None,
        required_capabilities=_DICRE_REQUIRED,
        supported_capabilities=_DICRE_REQUIRED,
        evidence_version=1,
        pipeline="dicre",
    ),
}


def get_validation_profile(name: str) -> ValidationProfile:
    key = str(name).strip().lower()
    try:
        return VALIDATION_PROFILES[key]
    except KeyError as exc:
        raise SystemExit(
            f"unknown --product {name!r}; available: {sorted(VALIDATION_PROFILES)}"
        ) from exc


# ---------------------------------------------------------------------------
# Finding model
# ---------------------------------------------------------------------------
@dataclass
class Finding:
    check_id: str
    category: str
    severity: str
    table: str
    passed: bool
    count: int = 0
    column: Optional[str] = None
    sample: List = field(default_factory=list)
    hint: str = ""
    message: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------------
# Config / env
# ---------------------------------------------------------------------------
@dataclass
class Config:
    synthetic_base: str
    jdbc_url: Optional[str]
    jdbc_user: Optional[str]
    jdbc_password: Optional[str]
    schema: str
    product: str = "cdb_simplificado"


def resolve_input_base(profile: ValidationProfile, input_base_arg: Optional[str]) -> str:
    """Resolve the synthetic input tree for the selected product.

    Order (first match wins):
      1. explicit --input-base;
      2. DATAGEN_SYNTHETIC_BASE_URI + DATAGEN_CLONE_PREFIX;
      3. DATAGEN_SYNTHETIC_BASE_URI + profile.default_clone_prefix.

    DATAGEN_SYNTHETIC_PREFIX is intentionally NOT part of the multi-product interface.
    It is honored (with a deprecation warning) ONLY for cdb_simplificado, to bridge the
    already-deployed Data Flow application until its arguments are updated.
    """
    if input_base_arg and input_base_arg.strip():
        return input_base_arg.strip().rstrip("/")
    base = os.environ.get("DATAGEN_SYNTHETIC_BASE_URI", "").strip()
    if not base:
        raise SystemExit(
            "Input base is required: pass --input-base or set DATAGEN_SYNTHETIC_BASE_URI."
        )
    base = base.rstrip("/")
    clone_prefix = os.environ.get("DATAGEN_CLONE_PREFIX", "").strip().strip("/")
    if clone_prefix:
        return f"{base}/{clone_prefix}"
    legacy = os.environ.get("DATAGEN_SYNTHETIC_PREFIX", "").strip().strip("/")
    if legacy:
        if profile.name != "cdb_simplificado":
            raise SystemExit(
                "DATAGEN_SYNTHETIC_PREFIX is not supported for --product "
                f"{profile.name}; use DATAGEN_CLONE_PREFIX or --input-base."
            )
        logger.warning(
            "DATAGEN_SYNTHETIC_PREFIX is DEPRECATED; migrate the Data Flow arguments to "
            "DATAGEN_CLONE_PREFIX or --input-base. Honored here only for cdb_simplificado."
        )
        return f"{base}/{legacy}"
    return f"{base}/{profile.default_clone_prefix}"


def read_config(no_oracle: bool, profile: ValidationProfile,
                input_base_arg: Optional[str] = None) -> Config:
    resolved = resolve_input_base(profile, input_base_arg)
    jdbc_url = os.environ.get("DATAGEN_SOURCE_JDBC_URL", "").strip() or None
    jdbc_user = os.environ.get("DATAGEN_SOURCE_DB_USER", "").strip() or None
    jdbc_pwd = os.environ.get("DATAGEN_SOURCE_DB_PASSWORD", "")
    schema = os.environ.get("DATAGEN_SOURCE_SCHEMA", "CETIP").strip().upper()
    if not no_oracle and not (jdbc_url and jdbc_user):
        raise SystemExit(
            "Oracle metadata requires DATAGEN_SOURCE_JDBC_URL and "
            "DATAGEN_SOURCE_DB_USER (or run with --no-oracle)."
        )
    return Config(resolved.rstrip("/"), jdbc_url, jdbc_user, jdbc_pwd, schema, profile.name)


# ---------------------------------------------------------------------------
# Oracle metadata over JDBC
# ---------------------------------------------------------------------------
@dataclass
class ForeignKey:
    name: str
    child_table: str
    child_cols: Tuple[str, ...]
    parent_table: str
    parent_cols: Tuple[str, ...]


@dataclass
class Metadata:
    tables: set
    pk: Dict[str, List[str]]
    not_null: Dict[str, set]
    col_type: Dict[str, Dict[str, str]]
    fks: Dict[str, List[ForeignKey]]
    # These fields deliberately follow the original five positional fields.
    # Existing Metadata(set(), {}, {}, {}, {}) callers remain compatible.
    column_capacity: Dict[str, Dict[str, Dict[str, object]]] = field(default_factory=dict)
    nls_character_set: Optional[str] = None
    nls_nchar_character_set: Optional[str] = None

    def is_shared_key_fk(self, fk: ForeignKey) -> bool:
        pk_cols = self.pk.get(fk.child_table) or []
        return bool(pk_cols) and sorted(fk.child_cols) == sorted(pk_cols)


def _jdbc(spark: SparkSession, cfg: Config, query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("driver", ORACLE_DRIVER)
        .option("user", cfg.jdbc_user)
        .option("password", cfg.jdbc_password)
        .option("dbtable", f"({query}) t")
        .load()
    )


def load_oracle_metadata(spark: SparkSession, cfg: Config) -> Metadata:
    owner = cfg.schema

    q_cols = (
        f"SELECT table_name, column_name, data_type, nullable, data_length, char_length, "
        f"char_used, data_precision, data_scale "
        f"FROM all_tab_columns WHERE owner = '{owner}'"
    )
    q_pk = (
        f"SELECT c.table_name, cc.column_name, cc.position "
        f"FROM all_constraints c "
        f"JOIN all_cons_columns cc ON cc.owner=c.owner AND cc.constraint_name=c.constraint_name "
        f"WHERE c.owner='{owner}' AND c.constraint_type='P'"
    )
    q_fk = (
        f"SELECT c.constraint_name cname, c.table_name child, ccc.column_name child_col, "
        f"       ccc.position pos, pc.table_name parent, pcc.column_name parent_col "
        f"FROM all_constraints c "
        f"JOIN all_cons_columns ccc ON ccc.owner=c.owner AND ccc.constraint_name=c.constraint_name "
        f"JOIN all_constraints pc ON pc.owner=c.r_owner AND pc.constraint_name=c.r_constraint_name "
        f"JOIN all_cons_columns pcc ON pcc.owner=pc.owner "
        f"                          AND pcc.constraint_name=pc.constraint_name "
        f"                          AND pcc.position=ccc.position "
        f"WHERE c.owner='{owner}' AND c.constraint_type='R'"
    )
    q_nls = (
        "SELECT parameter, value FROM nls_database_parameters "
        "WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET')"
    )

    logger.info("Loading Oracle metadata (owner=%s) ...", owner)
    cols_rows = _jdbc(spark, cfg, q_cols).collect()
    pk_rows = _jdbc(spark, cfg, q_pk).collect()
    fk_rows = _jdbc(spark, cfg, q_fk).collect()

    tables: set = set()
    not_null: Dict[str, set] = {}
    col_type: Dict[str, Dict[str, str]] = {}
    column_capacity: Dict[str, Dict[str, Dict[str, object]]] = {}
    for r in cols_rows:
        t = (r["TABLE_NAME"] or "").upper()
        c = (r["COLUMN_NAME"] or "").upper()
        tables.add(t)
        col_type.setdefault(t, {})[c] = (r["DATA_TYPE"] or "").upper()
        values = r.asDict(recursive=True) if hasattr(r, "asDict") else {}
        column_capacity.setdefault(t, {})[c] = {
            "DATA_TYPE": values.get("DATA_TYPE", r["DATA_TYPE"]),
            "DATA_LENGTH": values.get("DATA_LENGTH"),
            "CHAR_LENGTH": values.get("CHAR_LENGTH"),
            "CHAR_USED": values.get("CHAR_USED"),
            "DATA_PRECISION": values.get("DATA_PRECISION"),
            "DATA_SCALE": values.get("DATA_SCALE"),
        }
        if (r["NULLABLE"] or "").upper() == "N":
            not_null.setdefault(t, set()).add(c)

    pk_tmp: Dict[str, List[Tuple[int, str]]] = {}
    for r in pk_rows:
        t = (r["TABLE_NAME"] or "").upper()
        pk_tmp.setdefault(t, []).append((int(r["POSITION"]), (r["COLUMN_NAME"] or "").upper()))
    pk = {t: [c for _, c in sorted(v)] for t, v in pk_tmp.items()}

    fk_tmp: Dict[str, Dict[str, dict]] = {}
    for r in fk_rows:
        cname = r["CNAME"]
        child = (r["CHILD"] or "").upper()
        parent = (r["PARENT"] or "").upper()
        entry = fk_tmp.setdefault(child, {}).setdefault(
            cname, {"parent": parent, "cols": []}
        )
        entry["cols"].append(
            (int(r["POS"]), (r["CHILD_COL"] or "").upper(), (r["PARENT_COL"] or "").upper())
        )
    fks: Dict[str, List[ForeignKey]] = {}
    for child, byname in fk_tmp.items():
        for cname, e in byname.items():
            ordered = sorted(e["cols"])
            fks.setdefault(child, []).append(
                ForeignKey(
                    name=cname,
                    child_table=child,
                    child_cols=tuple(c for _, c, _ in ordered),
                    parent_table=e["parent"],
                    parent_cols=tuple(p for _, _, p in ordered),
                )
            )

    logger.info(
        "Metadata: %d tables, %d with PK, %d with FK(s), %d with NOT NULL col(s).",
        len(tables), len(pk), len(fks), len(not_null),
    )
    nls: Dict[str, str] = {}
    try:
        nls = {
            (r["PARAMETER"] or "").upper(): (r["VALUE"] or "").upper()
            for r in _jdbc(spark, cfg, q_nls).collect()
        }
    except Exception as exc:  # noqa: BLE001
        # A missing NLS grant must not discard PK/FK/NOT NULL/core capacity metadata.
        logger.warning("Could not load Oracle NLS character sets: %s", exc)

    return Metadata(
        tables, pk, not_null, col_type, fks, column_capacity,
        nls.get("NLS_CHARACTERSET"), nls.get("NLS_NCHAR_CHARACTERSET"),
    )


# ---------------------------------------------------------------------------
# Synthetic output IO
# ---------------------------------------------------------------------------
def list_table_dirs(spark: SparkSession, base: str) -> List[str]:
    """List immediate sub-directories under `base` via the Hadoop FileSystem API."""
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(base)
        fs = p.getFileSystem(hconf)
        if not fs.exists(p):
            return []
        return sorted(
            st.getPath().getName()
            for st in fs.listStatus(p)
            if st.isDirectory()
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not auto-list tables under %s: %s", base, exc)
        return []


def read_synthetic_tables(
    spark: SparkSession, base: str, only: Optional[List[str]]
) -> Dict[str, DataFrame]:
    names = only if only else list_table_dirs(spark, base)
    if not names:
        raise SystemExit(
            f"No synthetic tables found under {base}. Pass --tables to be explicit."
        )
    tables: Dict[str, DataFrame] = {}
    for name in names:
        path = f"{base}/{name}"
        try:
            # The run fires hundreds of independent actions over these tables
            # (one count per NOT NULL column, per FK, per shape metric); caching
            # them (memory+disk) avoids re-reading the Parquet from Object
            # Storage on every action.
            tables[name.upper()] = spark.read.parquet(path).cache()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s (cannot read %s): %s", name, path, exc)
    logger.info("Read %d synthetic table(s): %s", len(tables), ", ".join(sorted(tables)))
    return tables


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _ci_map(df: DataFrame) -> Dict[str, str]:
    return {c.upper(): c for c in df.columns}


def resolve(df: DataFrame, name: str) -> Optional[str]:
    return _ci_map(df).get(name.upper())


def _is_string_type(data_type: str) -> bool:
    if not data_type:
        return False
    dt = data_type.upper()
    return any(k in dt for k in ("CHAR", "CLOB", "VARCHAR", "NCHAR", "NVARCHAR"))


def _norm_code(col):
    """Normalize an id/code column to a trimmed string, dropping a trailing .0."""
    return F.regexp_replace(F.trim(col.cast("string")), r"\.0$", "")


def _canon_key_col(col):
    """Spark expression equivalent of _canon_key for required lookup IDs."""
    value = F.trim(col.cast("string"))
    stripped = F.regexp_replace(
        F.regexp_replace(value, r"(\.\d*?)0+$", "$1"), r"\.$", ""
    )
    return F.when(value.rlike(r"^-?\d+\.\d*0*$"), stripped).otherwise(value)


def _oracle_null_equivalent(col):
    """Oracle treats an empty string as NULL; injected Parquet frames may not."""
    return col.isNull() | (F.trim(col.cast("string")) == "")


def _sample_keys(df: DataFrame, key_cols: List[str], n: int) -> List:
    cols = [c for c in key_cols if c in df.columns] or df.columns[:1]
    rows = df.select(*cols).limit(n).collect()
    out = []
    for r in rows:
        vals = [r[c] for c in cols]
        out.append(vals[0] if len(vals) == 1 else vals)
    return out


def _pk_cols_for(meta: Metadata, table: str, df: DataFrame) -> List[str]:
    pk = meta.pk.get(table) or []
    resolved = [resolve(df, c) for c in pk]
    return [c for c in resolved if c] or df.columns[:1]


def _is_uri(path: str) -> bool:
    return "://" in path


def write_text(spark: SparkSession, path: str, content: str) -> None:
    """Write a small text file locally or to any Spark-readable URI (oci://...).
    Plain open() would land on the Data Flow driver's ephemeral disk."""
    if not _is_uri(path):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(hpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def read_text(spark: SparkSession, path: str) -> str:
    """Read a small text file locally or from any Spark-readable URI (oci://...)."""
    if not _is_uri(path):
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.open(hpath)
    try:
        return jvm.org.apache.commons.io.IOUtils.toString(
            stream, jvm.java.nio.charset.StandardCharsets.UTF_8
        )
    finally:
        stream.close()


# ---------------------------------------------------------------------------
# Category 4 - capacity contracts (Oracle and application)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ApplicationCapacity:
    table: str
    column: str
    kind: str
    value: Optional[int] = None
    unit: Optional[str] = None
    integer_digits: Optional[int] = None
    decimal_digits: Optional[int] = None
    automatic: bool = False
    caller_dependent: bool = False
    confidence: str = ""
    ambiguous_sources: Tuple[str, ...] = ()


@dataclass
class _CapacityRule:
    check_id: str
    severity: str
    table: str
    column: str
    condition: object
    hint: str
    message: str


def _contract_error(message: str) -> ValueError:
    return ValueError(f"Invalid application capacity contract: {message}")


def _contract_int(value, field_name: str, key: str, *, allow_zero: bool = True) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or (value < 0 if allow_zero else value <= 0)
    ):
        comparator = "a non-negative" if allow_zero else "a positive"
        raise _contract_error(f"{key}.{field_name} must be {comparator} integer.")
    return value


def _contract_capacity(row: dict, location: str) -> ApplicationCapacity:
    """Parse one row; duplicate handling belongs to the enclosing contract."""
    table, column = row.get("table"), row.get("column")
    if (
        not isinstance(table, str)
        or not table.strip()
        or not isinstance(column, str)
        or not column.strip()
    ):
        raise _contract_error(f"{location} needs non-empty table and column strings.")
    key = (table.strip().upper(), column.strip().upper())
    declared, enforcement = row.get("declared_capacity"), row.get("enforcement")
    if not isinstance(declared, dict) or not isinstance(enforcement, dict):
        raise _contract_error(f"{key[0]}.{key[1]} needs declared_capacity and enforcement objects.")
    kind = declared.get("kind")
    if not isinstance(kind, str):
        raise _contract_error(f"{key[0]}.{key[1]}.declared_capacity.kind must be a string.")
    kind = kind.lower()
    if kind not in {"text", "numeric", "none", "format"}:
        raise _contract_error(f"{key[0]}.{key[1]} has unsupported capacity kind {kind!r}.")
    automatic = enforcement.get("automatic")
    caller_dependent = enforcement.get("caller_dependent")
    if not isinstance(automatic, bool) or not isinstance(caller_dependent, bool):
        raise _contract_error(f"{key[0]}.{key[1]}.enforcement flags must be booleans.")
    confidence = row.get("confidence")
    if not isinstance(confidence, str) or not confidence.strip():
        raise _contract_error(f"{key[0]}.{key[1]}.confidence must be a non-empty string.")
    kwargs = dict(
        table=key[0],
        column=key[1],
        kind=kind,
        automatic=automatic,
        caller_dependent=caller_dependent,
        confidence=confidence.lower(),
    )
    if kind == "text":
        value = _contract_int(
            declared.get("value"), "value", f"{key[0]}.{key[1]}", allow_zero=False
        )
        unit = declared.get("unit")
        if unit not in {"utf16_code_units", "characters"}:
            raise _contract_error(f"{key[0]}.{key[1]}.unit must be utf16_code_units or characters.")
        kwargs.update(value=value, unit=unit)
    elif kind == "numeric":
        kwargs.update(
            integer_digits=_contract_int(
                declared.get("integer_digits"), "integer_digits", f"{key[0]}.{key[1]}"
            ),
            decimal_digits=_contract_int(
                declared.get("decimal_digits"), "decimal_digits", f"{key[0]}.{key[1]}"
            ),
        )
    return ApplicationCapacity(**kwargs)


def _contract_source(row: dict) -> str:
    """Small diagnostic only; it never participates in capacity enforcement."""
    return (
        ".".join(
            str(row.get(part))
            for part in ("java_type", "java_property", "mapping_source")
            if row.get(part)
        )
        or "unidentified mapping"
    )


def _same_capacity(left: ApplicationCapacity, right: ApplicationCapacity) -> bool:
    """Compare definitions without considering diagnostic source tracking."""
    return replace(left, ambiguous_sources=()) == replace(right, ambiguous_sources=())


def parse_application_capacity_contract(
    payload: object,
) -> Dict[Tuple[str, str], ApplicationCapacity]:
    """Parse only global, declared application capacities.

    Serializer routes deliberately are not part of this representation: a route
    such as ASEL023's six-character truncation is not a table-column invariant.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise _contract_error("top-level object must contain a rows list.")
    capacities: Dict[Tuple[str, str], ApplicationCapacity] = {}
    for index, row in enumerate(payload["rows"]):
        location = f"rows[{index}]"
        if not isinstance(row, dict):
            raise _contract_error(f"{location} must be an object.")
        capacity = _contract_capacity(row, location)
        key = (capacity.table, capacity.column)
        source = _contract_source(row)
        if key in capacities:
            previous = capacities[key]
            sources = tuple(sorted(set(previous.ambiguous_sources + (source,))))
            if previous.kind == "ambiguous":
                capacities[key] = replace(previous, ambiguous_sources=sources)
            elif _same_capacity(previous, capacity):
                capacities[key] = replace(previous, ambiguous_sources=sources)
            else:
                capacities[key] = ApplicationCapacity(
                    table=capacity.table,
                    column=capacity.column,
                    kind="ambiguous",
                    ambiguous_sources=sources,
                )
        else:
            capacities[key] = replace(capacity, ambiguous_sources=(source,))
    return capacities


def load_application_capacity_contract(
    spark: SparkSession, path: Optional[str]
) -> Dict[Tuple[str, str], ApplicationCapacity]:
    if not path:
        return {}
    try:
        return parse_application_capacity_contract(json.loads(read_text(spark, path)))
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Could not load --application-capacity-contract {path}: {exc}") from exc


def _capacity_number_limit(integer_digits: int) -> Decimal:
    # Avoid Spark/Java floating-point literals for capacity comparisons.
    return Decimal(10) ** integer_digits


def _oracle_charset(name: Optional[str]) -> Optional[str]:
    return {
        "WE8ISO8859P1": "ISO-8859-1",
        "US7ASCII": "US-ASCII",
        "AL32UTF8": "UTF-8",
        "AL16UTF16": "UTF-16BE",
    }.get((name or "").upper())


def _as_capacity_int(value) -> Optional[int]:
    try:
        if value is None or isinstance(value, bool):
            return None
        result = int(value)
        return result if Decimal(str(value)) == Decimal(result) else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def _safe_decimal_cast(column_name: str, scale: int):
    """ANSI-safe numeric conversion; failed/out-of-range casts become NULL."""
    escaped = column_name.replace("`", "``")
    return F.expr(f"try_cast(`{escaped}` AS DECIMAL(38,{scale}))")


def _application_numeric_condition(
    column_name: str,
    capacity: ApplicationCapacity,
    working_scale: int,
    source_is_numeric: bool,
):
    source = F.col(column_name)
    # Keep the original precision for Spark numeric columns. Casting them to a
    # one-extra-decimal working type first would miss values such as 1.2301 for
    # a two-decimal application contract after the cast rounded it to 1.230.
    value = source if source_is_numeric else _safe_decimal_cast(column_name, working_scale)
    invalid = F.lit(False) if source_is_numeric else value.isNull()
    integer_overflow = F.abs(value) >= F.lit(_capacity_number_limit(capacity.integer_digits))
    if capacity.decimal_digits == 0:
        decimal_overflow = value != F.round(value, 0)
    else:
        decimal_overflow = value != F.round(value, capacity.decimal_digits)
    return source.isNotNull() & (invalid | integer_overflow | decimal_overflow)


def check_capacity(
    tables: Dict[str, DataFrame],
    meta: Metadata,
    application_capacities: Dict[Tuple[str, str], ApplicationCapacity],
    sample: int,
) -> List[Finding]:
    """Check all deterministic capacity rules with one aggregate action/table."""
    out: List[Finding] = []
    category = "Capacity"
    for table, df in tables.items():
        rules: List[_CapacityRule] = []
        capacities = meta.column_capacity.get(table, {})
        for col_upper, actual in _ci_map(df).items():
            app = application_capacities.get((table, col_upper))
            if app and app.kind == "ambiguous":
                sources = ", ".join(app.ambiguous_sources)
                ambiguous_hint = (
                    "Resolve the conflicting Java mappings into one global capacity contract "
                    "before enforcing this column; route/path-dependent limits are not global "
                    "rules."
                )
                ambiguous_message = (
                    "Application capacity is ambiguous across alternate Java mappings; no "
                    f"application capacity rule was enforced. Sources: {sources}."
                )
                out.append(
                    Finding(
                        "4.capacity.application_ambiguous",
                        category,
                        SEV_WARN,
                        table,
                        False,
                        column=col_upper,
                        hint=ambiguous_hint,
                        message=ambiguous_message,
                    )
                )
            elif app and app.kind in {"text", "numeric"}:
                severity = SEV_ERROR if app.automatic and app.confidence == "high" else SEV_WARN
                condition = None
                if app.kind == "text":
                    length = (
                        F.length(F.encode(F.col(actual).cast("string"), "UTF-16BE")) / F.lit(2)
                        if app.unit == "utf16_code_units"
                        else F.length(F.col(actual).cast("string"))
                    )
                    condition = F.col(actual).isNotNull() & (length > F.lit(app.value))
                    detail = f"{app.unit} > {app.value}"
                    check_id = "4.capacity.application_text"
                else:
                    detail = (
                        f"integer_digits={app.integer_digits}, decimal_digits={app.decimal_digits}"
                    )
                    check_id = "4.capacity.application_number"
                    # Keep every declared integer digit and one extra fractional
                    # digit. The latter catches a scale overflow without rounding
                    # it away during the Spark cast.
                    working_scale = max(app.decimal_digits + 1, 1)
                    if app.integer_digits + working_scale > 38:
                        unverified_hint = (
                            "The declared application numeric capacity exceeds the exact Spark "
                            "Decimal(38,*) working range. Validate it in the application/runtime "
                            "instead of accepting a rounded capacity check."
                        )
                        unverified_message = (
                            f"Application numeric capacity ({detail}) is unverifiable without "
                            "an inexact Decimal cast."
                        )
                        out.append(
                            Finding(
                                "4.capacity.application_number_unverified",
                                category,
                                SEV_WARN,
                                table,
                                False,
                                column=col_upper,
                                hint=unverified_hint,
                                message=unverified_message,
                            )
                        )
                        app = None
                    else:
                        source_is_numeric = isinstance(df.schema[actual].dataType, NumericType)
                        condition = _application_numeric_condition(
                            actual, app, working_scale, source_is_numeric
                        )
                if condition is not None:
                    enforcement = (
                        "automatic high-confidence"
                        if severity == SEV_ERROR
                        else "caller-dependent/uncertain"
                    )
                    application_hint = (
                        "Regenerate or normalize the value to the declared application capacity; "
                        "serializer route widths are intentionally not global column limits."
                    )
                    application_message = f"Application {enforcement} capacity exceeded ({detail})."
                    rules.append(
                        _CapacityRule(
                            check_id,
                            severity,
                            table,
                            col_upper,
                            condition,
                            application_hint,
                            application_message,
                        )
                    )

            oracle = capacities.get(col_upper)
            if not oracle:
                continue
            data_type = str(
                oracle.get("DATA_TYPE") or meta.col_type.get(table, {}).get(col_upper) or ""
            ).upper()
            if data_type in {"CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"}:
                char_used = str(oracle.get("CHAR_USED") or "").upper()
                char_length = _as_capacity_int(oracle.get("CHAR_LENGTH"))
                data_length = _as_capacity_int(oracle.get("DATA_LENGTH"))
                is_char = char_used == "C" or data_type in {"NCHAR", "NVARCHAR2"}
                charset_name = (
                    meta.nls_nchar_character_set
                    if data_type in {"NCHAR", "NVARCHAR2"}
                    else meta.nls_character_set
                )
                charset = _oracle_charset(charset_name)
                if is_char and char_length is not None:
                    value = F.col(actual).cast("string")
                    condition = value.isNotNull() & (F.length(value) > F.lit(char_length))
                    if charset:
                        encoded = F.encode(value, charset)
                        roundtrip = F.decode(encoded, charset)
                        condition = condition | (value.isNotNull() & (roundtrip != value))
                        if data_length is not None:
                            condition = condition | (
                                value.isNotNull() & (F.length(encoded) > F.lit(data_length))
                            )
                    else:
                        char_unverified_message = (
                            f"Oracle {data_type}({char_length} CHAR) character count is checked, "
                            f"but representability is unverifiable for {charset_name!r}."
                        )
                        out.append(
                            Finding(
                                "4.capacity.oracle_string_unverified",
                                category,
                                SEV_WARN,
                                table,
                                False,
                                column=col_upper,
                                hint="Grant/read NLS_DATABASE_PARAMETERS or configure a supported "
                                "Oracle target charset to verify character representability.",
                                message=char_unverified_message,
                            )
                        )
                    rules.append(
                        _CapacityRule(
                            "4.capacity.oracle_string",
                            SEV_ERROR,
                            table,
                            col_upper,
                            condition,
                            (
                                "Shorten the generated text to the Oracle CHAR-semantics limit "
                                "before load."
                            ),
                            (
                                f"Oracle {data_type}({char_length} CHAR) capacity/charset "
                                "representability exceeded (ORA-12899/ORA-06502)."
                            ),
                        )
                    )
                elif not is_char and data_length is not None:
                    if not charset:
                        oracle_charset_hint = (
                            "Grant/read NLS_DATABASE_PARAMETERS or configure a supported "
                            "Oracle target charset; do not assume UTF-8 bytes for an Oracle BYTE "
                            "column."
                        )
                        oracle_charset_message = (
                            f"Oracle {data_type} BYTE capacity ({data_length}) is unverifiable: "
                            f"unsupported/missing target charset {charset_name!r}."
                        )
                        out.append(
                            Finding(
                                "4.capacity.oracle_string_unverified",
                                category,
                                SEV_WARN,
                                table,
                                False,
                                column=col_upper,
                                hint=oracle_charset_hint,
                                message=oracle_charset_message,
                            )
                        )
                    else:
                        value = F.col(actual).cast("string")
                        encoded = F.encode(value, charset)
                        roundtrip = F.decode(encoded, charset)
                        oracle_byte_hint = (
                            "Use characters representable in the target Oracle charset and "
                            "keep the "
                            "encoded byte length within the BYTE limit before load."
                        )
                        oracle_byte_message = (
                            f"Oracle {data_type}({data_length} BYTE) capacity/charset "
                            f"representability exceeded for {charset_name} (ORA-12899/ORA-06502)."
                        )
                        rules.append(
                            _CapacityRule(
                                "4.capacity.oracle_string",
                                SEV_ERROR,
                                table,
                                col_upper,
                                value.isNotNull()
                                & ((F.length(encoded) > F.lit(data_length)) | (roundtrip != value)),
                                oracle_byte_hint,
                                oracle_byte_message,
                            )
                        )
            elif data_type in {"RAW", "BINARY", "VARBINARY"}:
                data_length = _as_capacity_int(oracle.get("DATA_LENGTH"))
                if data_length is not None:
                    rules.append(
                        _CapacityRule(
                            "4.capacity.oracle_string",
                            SEV_ERROR,
                            table,
                            col_upper,
                            F.col(actual).isNotNull()
                            & (F.length(F.col(actual)) > F.lit(data_length)),
                            (
                                "Keep the binary/RAW payload within the Oracle byte capacity "
                                "before load."
                            ),
                            (
                                f"Oracle {data_type}({data_length}) binary capacity exceeded "
                                "(ORA-12899)."
                            ),
                        )
                    )
            elif data_type == "NUMBER":
                precision = _as_capacity_int(oracle.get("DATA_PRECISION"))
                scale = _as_capacity_int(oracle.get("DATA_SCALE"))
                if precision is None:
                    continue  # unconstrained NUMBER has no precision capacity.
                scale = 0 if scale is None else scale
                exponent = precision - scale
                # 10**38 cannot be represented as Spark Decimal(38,*), so keep
                # the comparison exact instead of falling back to a float.
                if not (1 <= precision <= 38 and -38 <= scale <= 38 and -38 <= exponent <= 37):
                    oracle_number_hint = (
                        "Validate this NUMBER(p,s) with Oracle directly; its exponent is outside "
                        "the exact Spark Decimal comparison range used by this validator."
                    )
                    oracle_number_message = (
                        f"Oracle NUMBER({precision},{scale}) capacity is unverifiable without "
                        "float comparison."
                    )
                    out.append(
                        Finding(
                            "4.capacity.oracle_number_unverified",
                            category,
                            SEV_WARN,
                            table,
                            False,
                            column=col_upper,
                            hint=oracle_number_hint,
                            message=oracle_number_message,
                        )
                    )
                else:
                    source = F.col(actual)
                    numeric = _safe_decimal_cast(actual, max(scale, 0))
                    rounded = F.round(numeric, scale)
                    oracle_number_capacity_hint = (
                        "Reduce the rounded magnitude to fit Oracle NUMBER(p,s), including its "
                        "declared scale, before load (ORA-01438)."
                    )
                    oracle_number_capacity_message = (
                        f"Oracle NUMBER({precision},{scale}) rounded magnitude exceeds its "
                        "precision capacity."
                    )
                    rules.append(
                        _CapacityRule(
                            "4.capacity.oracle_number",
                            SEV_ERROR,
                            table,
                            col_upper,
                            source.isNotNull()
                            & (
                                numeric.isNull()
                                | rounded.isNull()
                                | (F.abs(rounded) >= F.lit(Decimal(10) ** exponent))
                            ),
                            oracle_number_capacity_hint,
                            oracle_number_capacity_message,
                        )
                    )

        if not rules:
            continue
        aggregate = df.agg(
            *[
                F.sum(F.when(rule.condition, F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias(f"capacity_{i}")
                for i, rule in enumerate(rules)
            ]
        ).collect()[0]
        pk_cols = _pk_cols_for(meta, table, df)
        for i, rule in enumerate(rules):
            count = int(aggregate[f"capacity_{i}"] or 0)
            bad = df.where(rule.condition) if count else None
            failed = count > 0
            out.append(
                Finding(
                    rule.check_id,
                    category,
                    rule.severity if failed else SEV_INFO,
                    table,
                    not failed,
                    count=count,
                    column=rule.column,
                    sample=_sample_keys(bad, pk_cols, sample) if bad is not None else [],
                    hint=rule.hint if failed else "",
                    message=rule.message,
                )
            )
    return out


# ---------------------------------------------------------------------------
# Category 1 - CONDICAO_IF polymorphism (the ClassCastException)
# ---------------------------------------------------------------------------
def check_polymorphism(
    tables: Dict[str, DataFrame], meta: Metadata, sample: int
) -> List[Finding]:
    out: List[Finding] = []
    cond = tables.get(CONDICAO_IF_TABLE)
    if cond is None:
        return [Finding("1.polymorphism", "CONDICAO_IF polymorphism", SEV_WARN,
                        CONDICAO_IF_TABLE, False,
                        message="CONDICAO_IF not in output; polymorphism check unavailable.")]

    nci = resolve(cond, CONDICAO_IF_PK)
    tipo = resolve(cond, CONDICAO_IF_TIPO_COL)
    if not nci or not tipo:
        return [Finding("1.polymorphism", "CONDICAO_IF polymorphism", SEV_WARN,
                        CONDICAO_IF_TABLE, False,
                        message=f"Missing {CONDICAO_IF_PK}/{CONDICAO_IF_TIPO_COL} in CONDICAO_IF.")]

    present_subtypes = {t: tables[t] for t in set(SUBTYPE_BY_TIPO.values()) if t in tables}

    hint_bind = (
        "Root cause: bind_shared_key_children binds each subtype table to parent "
        "rows 0..N independently, so a NUM_CONDICAO_IF can land in more than one / "
        "the wrong subtype table. FIX: partition CONDICAO_IF keys by "
        "COD_TIPO_CONDICAO_IF and bind each subtype child only to its matching keys."
    )

    # Membership of each NUM_CONDICAO_IF across subtype tables.
    memb = None
    for tname, sdf in present_subtypes.items():
        skey = resolve(sdf, CONDICAO_IF_PK)
        if not skey:
            out.append(Finding("1c.subtype_key_missing", "CONDICAO_IF polymorphism",
                               SEV_WARN, tname, False,
                               message=f"{tname} has no {CONDICAO_IF_PK} column."))
            continue
        piece = sdf.select(F.col(skey).cast("string").alias("nci")).withColumn("tbl", F.lit(tname))
        memb = piece if memb is None else memb.unionByName(piece)

    cond_norm = cond.select(
        F.col(nci).cast("string").alias("cnci"),
        _norm_code(F.col(tipo)).alias("ctipo"),
    )

    if memb is None:
        out.append(Finding("1a.no_subtype_rows", "CONDICAO_IF polymorphism", SEV_ERROR,
                           CONDICAO_IF_TABLE, False, count=cond_norm.count(),
                           sample=_sample_keys(cond_norm.selectExpr("cnci"), ["cnci"], sample),
                           hint=hint_bind,
                           message="No subtype tables present; every CONDICAO_IF is dangling."))
        return out

    agg = memb.groupBy("nci").agg(
        F.collect_set("tbl").alias("tbls"),
        F.count(F.lit(1)).alias("nrows"),
    )

    joined = cond_norm.join(agg, cond_norm["cnci"] == agg["nci"], "left")

    # Expected subtype table from COD_TIPO_CONDICAO_IF.
    map_pairs = []
    for k, v in SUBTYPE_BY_TIPO.items():
        map_pairs += [F.lit(k), F.lit(v)]
    expected_expr = F.create_map(*map_pairs)[F.col("ctipo")]
    joined = joined.withColumn("expected_tbl", expected_expr)
    # size(NULL) is -1 in some Spark versions and NULL in others; handle null
    # membership explicitly so "dangling" (0 subtype rows) is detected reliably.
    joined = joined.withColumn(
        "n_member_tbls",
        F.when(F.col("tbls").isNull(), F.lit(0)).otherwise(F.size(F.col("tbls"))),
    )

    # 1a - dangling: CONDICAO_IF with no subtype row at all.
    dangling = joined.where(F.col("n_member_tbls") == 0)
    c = dangling.count()
    out.append(Finding(
        "1a.dangling_condicao_if", "CONDICAO_IF polymorphism",
        SEV_ERROR if c else SEV_INFO, CONDICAO_IF_TABLE, c == 0, count=c,
        column=CONDICAO_IF_PK,
        sample=_sample_keys(dangling.select(F.col("cnci")), ["cnci"], sample),
        hint=hint_bind,
        message="CONDICAO_IF rows with NO row in any subtype table (Hibernate cannot type them).",
    ))

    # 1a - ambiguous: present in >1 subtype table -> THE ClassCastException.
    ambiguous = joined.where(F.col("n_member_tbls") > 1)
    c = ambiguous.count()
    out.append(Finding(
        "1a.ambiguous_subtype", "CONDICAO_IF polymorphism",
        SEV_ERROR if c else SEV_INFO, CONDICAO_IF_TABLE, c == 0, count=c,
        column=CONDICAO_IF_PK,
        sample=_sample_keys(
            ambiguous.select(F.col("cnci"), F.concat_ws("+", F.col("tbls")).alias("tbls")),
            ["cnci", "tbls"], sample),
        hint=hint_bind,
        message="NUM_CONDICAO_IF present in MORE THAN ONE subtype table -> "
                "JurosFlutuanteDO cannot be cast to JurosFixoDO.",
    ))

    # 1b - wrong subtype: single membership but not the one COD_TIPO says.
    wrong = joined.where(
        (F.col("n_member_tbls") == 1)
        & F.col("expected_tbl").isNotNull()
        & (F.col("tbls")[0] != F.col("expected_tbl"))
    )
    c = wrong.count()
    out.append(Finding(
        "1b.subtype_mismatch", "CONDICAO_IF polymorphism",
        SEV_ERROR if c else SEV_INFO, CONDICAO_IF_TABLE, c == 0, count=c,
        column=CONDICAO_IF_TIPO_COL,
        sample=_sample_keys(
            wrong.select(F.col("cnci"), F.col("ctipo"),
                         F.col("tbls")[0].alias("actual"), F.col("expected_tbl")),
            ["cnci", "ctipo", "actual", "expected_tbl"], sample),
        hint=hint_bind,
        message="Subtype table does not match COD_TIPO_CONDICAO_IF (e.g. tipo=2/JUROS_FIXO "
                "but the row lives in JUROS_FLUTUANTE).",
    ))

    # 1b - unknown tipo: COD_TIPO not in the curated map.
    unknown = joined.where(F.col("expected_tbl").isNull())
    c = unknown.count()
    if c:
        out.append(Finding(
            "1b.unknown_tipo", "CONDICAO_IF polymorphism", SEV_WARN,
            CONDICAO_IF_TABLE, False, count=c, column=CONDICAO_IF_TIPO_COL,
            sample=_sample_keys(unknown.select(F.col("ctipo")).distinct(), ["ctipo"], sample),
            hint="Add these COD_TIPO_CONDICAO_IF values to SUBTYPE_BY_TIPO "
                 "(verify against TipoCondicaoIFDO).",
            message="COD_TIPO_CONDICAO_IF value not in the curated subtype map.",
        ))

    # 1c - orphan subtype rows (key not in CONDICAO_IF).
    cond_keys = cond.select(F.col(nci).cast("string").alias("cnci")).dropDuplicates()
    for tname, sdf in present_subtypes.items():
        skey = resolve(sdf, CONDICAO_IF_PK)
        if not skey:
            continue
        child_keys = sdf.select(F.col(skey).cast("string").alias("cnci"))
        orphans = child_keys.join(cond_keys, "cnci", "left_anti")
        c = orphans.count()
        out.append(Finding(
            "1c.orphan_subtype", "CONDICAO_IF polymorphism",
            SEV_ERROR if c else SEV_INFO, tname, c == 0, count=c, column=CONDICAO_IF_PK,
            sample=_sample_keys(orphans, ["cnci"], sample),
            hint="Shared-key child references a NUM_CONDICAO_IF absent from CONDICAO_IF.",
            message=f"{tname} rows whose {CONDICAO_IF_PK} has no parent CONDICAO_IF.",
        ))

    return out


def verify_subtype_map_against_production(spark: SparkSession, cfg: Config) -> List[Finding]:
    """Confirm the curated COD_TIPO->table map matches production (best-effort)."""
    out: List[Finding] = []
    for tipo, table in SUBTYPE_BY_TIPO.items():
        if table not in {"JUROS_FIXO", "JUROS_FLUTUANTE", "AMORTIZACAO", "SPREAD",
                         "RESGATE", "ATUALIZACAO_POS", "ATUALIZACAO_PRE", "RESET",
                         "PARTICIPACAO_LUCROS", "DESDOBRAMENTO"}:
            continue
        q = (
            f"SELECT DISTINCT p.{CONDICAO_IF_TIPO_COL} tipo "
            f"FROM {cfg.schema}.{table} s "
            f"JOIN {cfg.schema}.{CONDICAO_IF_TABLE} p ON p.{CONDICAO_IF_PK} = s.{CONDICAO_IF_PK} "
            f"WHERE ROWNUM <= 50"
        )
        try:
            rows = _jdbc(spark, cfg, q).collect()
            found = {str(r["TIPO"]).split(".")[0] for r in rows if r["TIPO"] is not None}
            if found and tipo not in found:
                out.append(Finding(
                    "1.map_verify", "CONDICAO_IF polymorphism", SEV_WARN, table, False,
                    hint="Update SUBTYPE_BY_TIPO to match production.",
                    message=f"Production {table} rows carry COD_TIPO_CONDICAO_IF={sorted(found)}, "
                            f"but the curated map expects {tipo}.",
                ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Subtype-map verify skipped for %s: %s", table, exc)
    return out


def check_subtype_map_snapshot(snapshot: dict, source: str = "baseline") -> List[Finding]:
    """Compare a precomputed raw-Parquet subtype snapshot with the curated map."""
    category = "CONDICAO_IF polymorphism"
    observed = snapshot.get("observed_by_table") if isinstance(snapshot, dict) else None
    if not isinstance(observed, dict) or not observed:
        return [Finding(
            "1.map_snapshot", category, SEV_WARN, CONDICAO_IF_TABLE, False,
            hint="Regenerate the shape baseline with the current profile_cdb_shapes.py.",
            message=f"Subtype-map snapshot from {source} has no observed subtype mappings.",
        )]

    expected_by_table: Dict[str, set] = {}
    for tipo, table in SUBTYPE_BY_TIPO.items():
        expected_by_table.setdefault(table, set()).add(tipo)

    out: List[Finding] = []
    for table, values in sorted(observed.items()):
        found = {str(value) for value in values}
        expected = expected_by_table.get(table, set())
        unexpected = sorted(found - expected)
        if unexpected:
            out.append(Finding(
                "1.map_snapshot", category, SEV_WARN, table, False,
                column=CONDICAO_IF_TIPO_COL,
                sample=unexpected,
                hint="Review the raw baseline and update SUBTYPE_BY_TIPO only if the "
                     "application joined-subclass mapping changed.",
                message=f"Raw baseline {table} rows carry unexpected subtype value(s) "
                        f"{unexpected}; curated value(s): {sorted(expected)}.",
            ))

    if not out:
        out.append(Finding(
            "1.map_snapshot", category, SEV_INFO, CONDICAO_IF_TABLE, True,
            count=len(observed),
            message=f"{len(observed)} raw subtype table mapping(s) from {source} match "
                    "the curated application map.",
        ))
    return out


def verify_subtype_map_from_baseline(
    spark: SparkSession, baseline_path: str
) -> List[Finding]:
    """Load and verify the subtype snapshot embedded by profile_cdb_shapes.py."""
    try:
        baseline = json.loads(read_text(spark, baseline_path))
    except Exception as exc:  # noqa: BLE001
        return [Finding(
            "1.map_snapshot", "CONDICAO_IF polymorphism", SEV_WARN,
            CONDICAO_IF_TABLE, False,
            hint="Repair or regenerate the shape baseline before strict validation.",
            message=f"Subtype-map snapshot unavailable because the baseline could not be "
                    f"loaded: {exc}",
        )]
    snapshot = baseline.get("subtype_map")
    if snapshot is None:
        return [Finding(
            "1.map_snapshot", "CONDICAO_IF polymorphism", SEV_WARN,
            CONDICAO_IF_TABLE, False,
            hint="Regenerate the shape baseline with the current profile_cdb_shapes.py.",
            message=f"Subtype-map snapshot unavailable: baseline {baseline_path} has no "
                    "subtype_map.",
        )]
    source = baseline.get("base_uri") or baseline_path
    return check_subtype_map_snapshot(snapshot, str(source))


# ---------------------------------------------------------------------------
# Product identity preflight (runs before every semantic check)
# ---------------------------------------------------------------------------
def check_product_identity(
    tables: Dict[str, DataFrame], profile: ValidationProfile, sample: int
) -> List[Finding]:
    """Assert the output root universe is exactly the selected product's type.

    Errors (never skips) on: missing root table/columns, no active roots, wrong root
    type, or a mix of root types. This guarantees an RDB dataset validated as --product cdb
    fails loudly instead of yielding an empty-universe false pass.
    """
    cat = "Product identity"
    root = tables.get(SHAPE_ROOT_TABLE)
    if root is None:
        return [Finding("0.identity", cat, SEV_ERROR, SHAPE_ROOT_TABLE, False,
                        hint="Export INSTRUMENTO_FINANCEIRO in the synthetic output.",
                        message=f"Root table {SHAPE_ROOT_TABLE} absent; cannot validate "
                                f"product {profile.name}.")]
    tipo = resolve(root, "NUM_TIPO_IF")
    key = resolve(root, SHAPE_ROOT_KEY)
    excl = resolve(root, "DAT_EXCLUSAO")
    missing = [n for n, a in (("NUM_TIPO_IF", tipo), (SHAPE_ROOT_KEY, key)) if not a]
    if missing:
        return [Finding("0.identity", cat, SEV_ERROR, SHAPE_ROOT_TABLE, False,
                        column=",".join(missing),
                        hint="Root must carry NUM_TIPO_IF and NUM_IF for identity.",
                        message=f"Root table missing column(s) {missing} for product "
                                f"{profile.name}.")]
    active = root.where(_oracle_null_equivalent(F.col(excl))) if excl else root
    type_counts = (
        active.select(_norm_code(F.col(tipo)).alias("t"))
        .groupBy("t").count().collect()
    )
    present = {r["t"]: r["count"] for r in type_counts}
    expected = str(profile.num_tipo_if)
    n_expected = present.get(expected, 0)
    wrong = {t: c for t, c in present.items() if t != expected}
    out: List[Finding] = []
    out.append(Finding(
        "0.identity.type", cat,
        SEV_INFO if n_expected > 0 else SEV_ERROR, SHAPE_ROOT_TABLE, n_expected > 0,
        count=n_expected, column="NUM_TIPO_IF",
        hint="" if n_expected else
             f"No active roots of NUM_TIPO_IF={expected}. Wrong --product for this output?",
        message=f"Active {SHAPE_ROOT_TABLE} rows with NUM_TIPO_IF={expected} "
                f"(product {profile.name}).",
    ))
    out.append(Finding(
        "0.identity.mixed", cat,
        SEV_ERROR if wrong else SEV_INFO, SHAPE_ROOT_TABLE, not wrong,
        count=sum(wrong.values()), column="NUM_TIPO_IF",
        sample=sorted(wrong)[:sample],
        hint="Output mixes root types; a product output must contain a single NUM_TIPO_IF."
             if wrong else "",
        message=f"Foreign root NUM_TIPO_IF value(s) present besides {expected}: "
                f"{sorted(wrong)}." if wrong else
                f"Only NUM_TIPO_IF={expected} present.",
    ))
    return out


# ---------------------------------------------------------------------------
# Capability ledger: unsupported required capabilities force PARTIAL
# ---------------------------------------------------------------------------
def build_capability_findings(profile: ValidationProfile) -> List[Finding]:
    """One WARN 'unsupported' finding per required-but-unsupported capability.

    This set is derived from the profile, not from which check functions ran, so a
    required capability can never disappear because its check was disabled/skipped.
    """
    out: List[Finding] = []
    for cap in profile.unsupported_required():
        out.append(Finding(
            f"0.capability.{cap}", "Coverage", SEV_WARN, profile.name, False,
            column=cap,
            hint="Capture the target evidence for this product before enabling strict "
                 "validation of this capability; do NOT inherit CDB defaults.",
            message=f"Capability {cap!r} is REQUIRED for strict validation of product "
                    f"{profile.name!r} but is UNSUPPORTED by current evidence "
                    f"(forces PARTIAL).",
        ))
    return out


# ---------------------------------------------------------------------------
# Category 2 - Domain conformance (IF-level EXISTS eligibility)
# ---------------------------------------------------------------------------
def _active(df: DataFrame) -> DataFrame:
    col = resolve(df, "DAT_EXCLUSAO")
    return df.where(_oracle_null_equivalent(F.col(col))) if col else df


def _long_keys(df: DataFrame, col: str, alias: str) -> Optional[DataFrame]:
    actual = resolve(df, col)
    if not actual:
        return None
    return df.select(F.col(actual).cast("long").alias(alias))


def build_eligible_num_ifs(
    tables: Dict[str, DataFrame], profile: ValidationProfile
) -> Tuple[Optional[DataFrame], List[str]]:
    """IF-level eligible root set via left-semi (EXISTS) joins matching the product SQL.

    Returns (eligible_num_if_df keyed NUM_IF, missing_requirements). If a required table or
    column is absent, returns (None, [...]) so the caller reports availability rather than
    silently passing.
    """
    missing: List[str] = []
    root = tables.get(SHAPE_ROOT_TABLE)
    if root is None:
        return None, [SHAPE_ROOT_TABLE]
    root_tipo = resolve(root, "NUM_TIPO_IF")
    root_key = resolve(root, SHAPE_ROOT_KEY)
    if not root_tipo or not root_key:
        return None, [f"{SHAPE_ROOT_TABLE}.NUM_TIPO_IF/NUM_IF"]

    universe = _active(root.where(F.col(root_tipo).cast("long") == profile.num_tipo_if)) \
        .select(F.col(root_key).cast("long").alias("NUM_IF")).dropDuplicates()

    def require(table: str) -> Optional[DataFrame]:
        df = tables.get(table)
        if df is None:
            missing.append(table)
        return df

    titulo = require("TITULO")
    condicao = require("CONDICAO_IF")
    resgate = require("RESGATE")
    deposito = require("DEPOSITO_AUTOMATICO_IF")
    operacao = require(OPERACAO_TABLE)
    dado = require(DADO_OPERACAO_TABLE)
    lancamento = require("LANCAMENTO")
    especificacao = require("ESPECIFICACAO")
    espec_comitente = require("ESPECIFICACAO_COMITENTE")
    if missing:
        return None, missing

    # TITULO existence (non-escalonado for simplificado).
    esc = resolve(titulo, "COD_TIPO_ESCALONAMENTO")
    if profile.simplified_domain and not esc:
        return None, ["TITULO.COD_TIPO_ESCALONAMENTO"]
    titulo_ok = titulo
    if profile.simplified_domain and esc:
        titulo_ok = titulo.where(F.col(esc).isNull())
    tit_keys = _long_keys(titulo_ok, SHAPE_ROOT_KEY, "NUM_IF")

    # Active CONDICAO_IF joined to an active RESGATE row; for simplificado the resgate must
    # be 'SEM TABELA'. Full CDB / RDB accept any COD_COND_RESGATE.
    cif = _active(condicao)
    cif_key = resolve(cif, CONDICAO_IF_PK)
    cif_if = resolve(cif, SHAPE_ROOT_KEY)
    res = _active(resgate)
    res_key = resolve(res, CONDICAO_IF_PK)
    res_cond = resolve(res, "COD_COND_RESGATE")
    key_requirements = [
        (cif_key, "CONDICAO_IF.NUM_CONDICAO_IF"),
        (cif_if, "CONDICAO_IF.NUM_IF"),
        (res_key, "RESGATE.NUM_CONDICAO_IF"),
    ]
    missing_keys = [name for actual, name in key_requirements if not actual]
    if missing_keys:
        return None, missing_keys
    if profile.simplified_domain and not res_cond:
        return None, ["RESGATE.COD_COND_RESGATE"]
    res_ok = res
    if profile.simplified_domain and res_cond:
        res_ok = res.where(F.upper(F.trim(F.col(res_cond).cast("string"))) == "SEM TABELA")
    res_keys = res_ok.select(F.col(res_key).cast("long").alias(CONDICAO_IF_PK))
    cif_slim = cif.select(
        F.col(cif_key).cast("long").alias(CONDICAO_IF_PK),
        F.col(cif_if).cast("long").alias("NUM_IF"),
    )
    resgate_ifs = cif_slim.join(res_keys, CONDICAO_IF_PK, "leftsemi").select("NUM_IF")

    # Active non-resgate condição (COD_TIPO_CONDICAO_IF <> 20) — FLAGS_IF in the product SQL.
    cif_tipo = resolve(cif, CONDICAO_IF_TIPO_COL)
    if not cif_tipo:
        return None, ["CONDICAO_IF.COD_TIPO_CONDICAO_IF"]
    nonresgate_ifs = cif.where(_norm_code(F.col(cif_tipo)) != "20").select(
        F.col(cif_if).cast("long").alias("NUM_IF"))

    dep_keys = _long_keys(deposito, SHAPE_ROOT_KEY, "NUM_IF")
    missing_direct_keys = [
        name for frame, name in (
            (tit_keys, "TITULO.NUM_IF"),
            (dep_keys, "DEPOSITO_AUTOMATICO_IF.NUM_IF"),
        ) if frame is None
    ]
    if missing_direct_keys:
        return None, missing_direct_keys

    # Operation cluster: OPERACAO with DADO_OPERACAO and LANCAMENTO.
    op_if = _long_keys(operacao, SHAPE_ROOT_KEY, "NUM_IF")
    op_id = resolve(operacao, OPERACAO_KEY_COL)
    dado_op = resolve(dado, OPERACAO_KEY_COL)
    lan_op = resolve(lancamento, OPERACAO_KEY_COL)
    if not all([op_if is not None, op_id, dado_op, lan_op]):
        return None, ["OPERACAO/DADO_OPERACAO/LANCAMENTO key columns"]
    op_ids = operacao.select(
        F.col(op_id).cast("long").alias("OP"),
        F.col(resolve(operacao, SHAPE_ROOT_KEY)).cast("long").alias("NUM_IF"),
    )
    op_with_dado = op_ids.join(
        dado.select(F.col(dado_op).cast("long").alias("OP")).dropDuplicates(), "OP", "leftsemi")
    op_cluster = op_with_dado.join(
        lancamento.select(F.col(lan_op).cast("long").alias("OP")).dropDuplicates(),
        "OP", "leftsemi").select("NUM_IF")

    # Specification cluster: ESPECIFICACAO with ESPECIFICACAO_COMITENTE, reached via OPERACAO.
    esp_op = resolve(especificacao, OPERACAO_KEY_COL)
    esp_id = resolve(especificacao, "NUM_ID_ESPECIFICACAO")
    epc_id = resolve(espec_comitente, "NUM_ID_ESPECIFICACAO")
    if not all([esp_op, esp_id, epc_id]):
        return None, ["ESPECIFICACAO/ESPECIFICACAO_COMITENTE key columns"]
    esp_ok = especificacao.select(
        F.col(esp_op).cast("long").alias("OP"),
        F.col(esp_id).cast("long").alias("ESP"),
    ).join(
        espec_comitente.select(F.col(epc_id).cast("long").alias("ESP")).dropDuplicates(),
        "ESP", "leftsemi").select("OP")
    spec_cluster = op_ids.join(esp_ok, "OP", "leftsemi").select("NUM_IF")

    eligible = universe
    for req in (tit_keys, resgate_ifs, nonresgate_ifs, dep_keys, op_cluster, spec_cluster):
        eligible = eligible.join(req.dropDuplicates(), "NUM_IF", "leftsemi")
    return eligible.dropDuplicates(), []


def check_domain(
    tables: Dict[str, DataFrame], meta: Metadata, sample: int, profile: ValidationProfile
) -> List[Finding]:
    cat = "Domain conformance"
    root = tables.get(SHAPE_ROOT_TABLE)
    if root is None:
        return [Finding("2.domain", cat, SEV_ERROR, SHAPE_ROOT_TABLE, False,
                        message=f"{SHAPE_ROOT_TABLE} absent; cannot evaluate domain.")]
    eligible, missing = build_eligible_num_ifs(tables, profile)
    if eligible is None:
        return [Finding("2.domain.availability", cat, SEV_WARN, ",".join(missing), False,
                        hint="Include the required domain tables/columns in the output.",
                        message=f"Domain eligibility unavailable; missing: {missing}.")]

    tipo = resolve(root, "NUM_TIPO_IF")
    key = resolve(root, SHAPE_ROOT_KEY)
    active_roots = _active(root.where(F.col(tipo).cast("long") == profile.num_tipo_if)) \
        .select(F.col(key).cast("long").alias("NUM_IF")).dropDuplicates()
    ineligible = active_roots.join(eligible, "NUM_IF", "left_anti")
    c = ineligible.count()
    hint = (
        "Instrument is not a valid member of product "
        f"{profile.name} (product SQL eligibility): missing a qualifying "
        + ("non-escalonado title / 'SEM TABELA' resgate / " if profile.simplified_domain else "")
        + "active resgate path, non-resgate condição, deposit, operation cluster, or "
        "specification cluster."
    )
    return [Finding(
        "2.domain", cat, SEV_ERROR if c else SEV_INFO, SHAPE_ROOT_TABLE, c == 0, count=c,
        column="NUM_IF", sample=_sample_keys(ineligible, ["NUM_IF"], sample),
        hint=hint if c else "",
        message=f"Active {profile.name} roots not eligible per IF-level EXISTS domain.",
    )]


# ---------------------------------------------------------------------------
# Category 2b - Full-CDB variant conformance
# ---------------------------------------------------------------------------
def check_cdb_variant_rules(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    """Validate resgate-table and issuance-escalation structures for full CDB only."""
    if profile.name != "cdb":
        return []

    cat = "CDB variant conformance"
    out: List[Finding] = []

    def record(
        check_id: str,
        table: str,
        column: str,
        bad: DataFrame,
        keys: List[str],
        severity: str,
        hint: str,
        message: str,
    ) -> None:
        count = bad.count()
        out.append(Finding(
            check_id, cat, severity if count else SEV_INFO, table, count == 0,
            count=count, column=column,
            sample=_sample_keys(bad, keys, sample) if count else [],
            hint=hint if count else "", message=message,
        ))

    def try_cast(column: str, sql_type: str):
        escaped = column.replace("`", "``")
        return F.expr(f"try_cast(`{escaped}` as {sql_type})")

    root = tables.get("INSTRUMENTO_FINANCEIRO")
    titulo = tables.get("TITULO")
    condicao = tables.get("CONDICAO_IF")
    resgate = tables.get("RESGATE")
    missing_tables = [
        name for name, frame in (
            ("INSTRUMENTO_FINANCEIRO", root), ("TITULO", titulo),
            ("CONDICAO_IF", condicao), ("RESGATE", resgate),
        ) if frame is None
    ]
    if missing_tables:
        return [Finding(
            "2b.availability", cat, SEV_WARN, ",".join(missing_tables), False,
            hint="Include the core CDB tables before validating product variants.",
            message=f"CDB variant checks unavailable; missing: {missing_tables}.",
        )]

    root_cols = {
        name: resolve(root, name) for name in (
            "NUM_IF", "NUM_TIPO_IF", "DAT_EMISSAO", "DAT_VENCIMENTO",
            "COD_SITUACAO_IF",
        )
    }
    titulo_cols = {
        name: resolve(titulo, name) for name in ("NUM_IF", "COD_TIPO_ESCALONAMENTO")
    }
    cond_cols = {
        name: resolve(condicao, name) for name in (
            "NUM_CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF",
            "DAT_INICIO_CONDICAO_IF",
        )
    }
    res_cols = {
        name: resolve(resgate, name)
        for name in ("NUM_CONDICAO_IF", "COD_COND_RESGATE", "DAT_RESGATE")
    }
    required = {
        **{f"INSTRUMENTO_FINANCEIRO.{name}": value for name, value in root_cols.items()
           if name != "COD_SITUACAO_IF"},
        **{f"TITULO.{name}": value for name, value in titulo_cols.items()},
        **{f"CONDICAO_IF.{name}": value for name, value in cond_cols.items()
           if name != "DAT_INICIO_CONDICAO_IF"},
        **{f"RESGATE.{name}": value for name, value in res_cols.items()},
    }
    missing_columns = [name for name, value in required.items() if not value]
    if missing_columns:
        return [Finding(
            "2b.availability", cat, SEV_WARN, ",".join(missing_columns), False,
            hint="Include the required CDB variant columns in the validation input.",
            message=f"CDB variant checks unavailable; missing: {missing_columns}.",
        )]

    roots = _active(root).where(
        F.col(root_cols["NUM_TIPO_IF"]).cast("long") == profile.num_tipo_if
    ).select(
        F.col(root_cols["NUM_IF"]).cast("long").alias("NUM_IF"),
        try_cast(root_cols["DAT_EMISSAO"], "date").alias("root_emission"),
        try_cast(root_cols["DAT_VENCIMENTO"], "date").alias("root_maturity"),
        *(
            [_norm_code(F.col(root_cols["COD_SITUACAO_IF"])).alias("root_status")]
            if root_cols["COD_SITUACAO_IF"] else []
        ),
    ).dropDuplicates(["NUM_IF"])
    conditions = _active(condicao).select(
        F.col(cond_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
        F.col(cond_cols["NUM_IF"]).cast("long").alias("NUM_IF"),
        _norm_code(F.col(cond_cols["COD_TIPO_CONDICAO_IF"])).alias("condition_type"),
        *(
            [try_cast(cond_cols["DAT_INICIO_CONDICAO_IF"], "date").alias("condition_start")]
            if cond_cols["DAT_INICIO_CONDICAO_IF"] else []
        ),
    ).join(roots.select("NUM_IF"), "NUM_IF", "inner")
    resgate_parents = conditions.where(F.col("condition_type") == "20").join(
        _active(resgate).select(
            F.col(res_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
            _norm_code(F.col(res_cols["COD_COND_RESGATE"])).alias("resgate_mode"),
            try_cast(res_cols["DAT_RESGATE"], "date").alias("resgate_date"),
        ),
        "condition_key", "inner",
    )
    com_tabela = resgate_parents.where(F.col("resgate_mode") == "COM TABELA")

    schedule = tables.get("CONDICAO_RESGATE")
    if schedule is None:
        record(
            "2b.resgate_schedule_coverage", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
            com_tabela, ["NUM_IF", "condition_key"], SEV_ERROR,
            "Include at least one active CONDICAO_RESGATE per COM TABELA.",
            "COM TABELA resgates without an available redemption schedule.",
        )
    else:
        schedule_cols = {
            name: resolve(schedule, name) for name in (
                "NUM_CONDICAO_IF", "IND_EXCLUIDO", "DAT_RESGATE", "VAL_PERCENTUAL",
            )
        }
        schedule_required = [
            name for name in ("NUM_CONDICAO_IF", "DAT_RESGATE", "VAL_PERCENTUAL")
            if not schedule_cols[name]
        ]
        if schedule_required:
            com_count = com_tabela.count()
            schedule_count = schedule.count()
            count = max(com_count, schedule_count)
            sample_frame = schedule if schedule_count else com_tabela
            out.append(Finding(
                "2b.resgate_schedule_coverage", cat,
                SEV_ERROR if count else SEV_INFO, "CONDICAO_RESGATE", count == 0,
                count=count, column=",".join(schedule_required),
                sample=_sample_keys(
                    sample_frame,
                    ["NUM_IF", "NUM_CONDICAO_IF", "condition_key"],
                    sample,
                )
                if count else [],
                hint="Include the required redemption schedule columns." if count else "",
                message=f"Redemption schedule columns unavailable: {schedule_required}.",
            ))
        else:
            active_schedule = schedule
            if schedule_cols["IND_EXCLUIDO"]:
                excluded = _norm_code(F.col(schedule_cols["IND_EXCLUIDO"])).isin(
                    "S", "Y", "1"
                )
                active_schedule = schedule.where(F.coalesce(~excluded, F.lit(True)))
            children = active_schedule.select(
                F.col(schedule_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
                try_cast(schedule_cols["DAT_RESGATE"], "date").alias("schedule_date"),
                try_cast(schedule_cols["VAL_PERCENTUAL"], "double").alias(
                    "schedule_percentage"
                ),
            )
            child_keys = children.select("condition_key").dropDuplicates()
            record(
                "2b.resgate_schedule_coverage", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
                com_tabela.join(child_keys, "condition_key", "leftanti"),
                ["NUM_IF", "condition_key"], SEV_ERROR,
                "Generate at least one active schedule row per COM TABELA.",
                "COM TABELA resgates without active redemption schedule rows.",
            )
            record(
                "2b.resgate_schedule_parent", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
                children.join(
                    com_tabela.select("condition_key").dropDuplicates(),
                    "condition_key", "leftanti",
                ),
                ["condition_key", "schedule_date"], SEV_ERROR,
                "Attach schedules only to active type-20 COM TABELA resgates.",
                "Active schedule rows with invalid, inactive, or SEM TABELA parents.",
            )
            record(
                "2b.resgate_schedule_values", "CONDICAO_RESGATE",
                "DAT_RESGATE,VAL_PERCENTUAL",
                children.where(
                    F.col("schedule_date").isNull()
                    | F.col("schedule_percentage").isNull()
                    | F.isnan(F.col("schedule_percentage"))
                    | (F.abs(F.col("schedule_percentage")) == F.lit(float("inf")))
                ),
                ["condition_key", "schedule_date"], SEV_ERROR,
                "Populate a parseable date and percentage; percentages may exceed 100.",
                "Active schedule rows with null/invalid date or percentage.",
            )
            duplicate_dates = children.where(F.col("schedule_date").isNotNull()).groupBy(
                "condition_key", "schedule_date"
            ).count().where(F.col("count") > 1)
            record(
                "2b.resgate_schedule_unique_dates", "CONDICAO_RESGATE", "DAT_RESGATE",
                duplicate_dates, ["condition_key", "schedule_date"], SEV_ERROR,
                "Use each redemption date at most once per resgate condition.",
                "Duplicate active redemption dates under the same resgate condition.",
            )
            bounded = children.join(
                com_tabela.select("condition_key", "NUM_IF", "resgate_date"),
                "condition_key", "inner",
            ).join(roots, "NUM_IF", "inner")
            record(
                "2b.resgate_schedule_dates", "CONDICAO_RESGATE", "DAT_RESGATE",
                bounded.where(
                    F.col("schedule_date").isNotNull()
                    & (
                        (F.col("root_emission").isNotNull()
                         & (F.col("schedule_date") < F.col("root_emission")))
                        | (F.col("root_maturity").isNotNull()
                           & (F.col("schedule_date") > F.col("root_maturity")))
                        | (F.col("resgate_date").isNotNull()
                           & (F.col("schedule_date") > F.col("resgate_date")))
                    )
                ),
                ["NUM_IF", "condition_key", "schedule_date"], SEV_ERROR,
                "Keep schedule dates between issuance and redemption/maturity.",
                "Active redemption schedule rows outside the instrument date bounds.",
            )

    escal_titles = _active(titulo).where(
        _norm_code(F.col(titulo_cols["COD_TIPO_ESCALONAMENTO"])) == "EMISSAO"
    ).select(F.col(titulo_cols["NUM_IF"]).cast("long").alias("NUM_IF")).join(
        roots.select("NUM_IF"), "NUM_IF", "inner"
    ).dropDuplicates()
    escal_count = escal_titles.count()
    juros = tables.get("JUROS_FLUTUANTE")
    if escal_count and (not cond_cols["DAT_INICIO_CONDICAO_IF"] or juros is None):
        out.append(Finding(
            "2b.escalonamento_coverage", cat, SEV_ERROR, "CONDICAO_IF,JUROS_FLUTUANTE",
            False, count=escal_count, column="DAT_INICIO_CONDICAO_IF,NUM_CONDICAO_IF",
            sample=_sample_keys(escal_titles, ["NUM_IF"], sample),
            hint="Include dated active type-3 conditions and JUROS_FLUTUANTE rows.",
            message="EMISSAO escalonamento has no complete segment data.",
        ))
    elif escal_count and juros is not None and cond_cols["DAT_INICIO_CONDICAO_IF"]:
        juros_key = resolve(juros, "NUM_CONDICAO_IF")
        escal_conditions = conditions.where(F.col("condition_type") == "3").join(
            escal_titles, "NUM_IF", "inner"
        )
        if not juros_key:
            incomplete = escal_titles
            segments = None
        else:
            juros_counts = juros.select(
                F.col(juros_key).cast("long").alias("condition_key")
            ).groupBy("condition_key").count().withColumnRenamed("count", "juros_count")
            covered = escal_conditions.join(juros_counts, "condition_key", "left").fillna(
                0, ["juros_count"]
            )
            incomplete = escal_titles.join(
                escal_conditions.select("NUM_IF").dropDuplicates(), "NUM_IF", "leftanti"
            ).unionByName(
                covered.where(F.col("juros_count") < 1).select("NUM_IF")
            ).dropDuplicates()
            segments = covered.where(F.col("juros_count") >= 1)
        record(
            "2b.escalonamento_coverage", "CONDICAO_IF,JUROS_FLUTUANTE",
            "COD_TIPO_CONDICAO_IF,NUM_CONDICAO_IF", incomplete, ["NUM_IF"], SEV_ERROR,
            "Generate active dated type-3 segments, each with an interest row.",
            "EMISSAO titles without a complete floating-interest segment set.",
        )
        if segments is not None:
            dated = segments.join(roots, "NUM_IF", "inner")
            invalid_dates = dated.where(
                F.col("condition_start").isNull()
                | (F.col("root_emission").isNotNull()
                   & (F.col("condition_start") < F.col("root_emission")))
                | (F.col("root_maturity").isNotNull()
                   & (F.col("condition_start") > F.col("root_maturity")))
            ).select("NUM_IF")
            wrong_first = dated.groupBy("NUM_IF").agg(
                F.min("condition_start").alias("first_start"),
                F.min("root_emission").alias("root_emission"),
            ).where(
                F.col("first_start").isNull()
                | F.col("root_emission").isNull()
                | (F.col("first_start") != F.col("root_emission"))
            ).select("NUM_IF")
            record(
                "2b.escalonamento_dates", "CONDICAO_IF", "DAT_INICIO_CONDICAO_IF",
                invalid_dates.union(wrong_first).dropDuplicates(), ["NUM_IF"], SEV_ERROR,
                "Keep starts within IF dates and start EMISSAO at issuance.",
                "EMISSAO instruments with invalid segment dates or first start.",
            )
            duplicate_starts = segments.where(F.col("condition_start").isNotNull()).groupBy(
                "NUM_IF", "condition_start"
            ).count().where(F.col("count") > 1)
            record(
                "2b.escalonamento_unique_dates", "CONDICAO_IF", "DAT_INICIO_CONDICAO_IF",
                duplicate_starts, ["NUM_IF", "condition_start"], SEV_ERROR,
                "Use each active segment start at most once per instrument.",
                "Duplicate active escalonamento segment start dates.",
            )

            consistency_names = (
                "NUM_INDICE_VALORIZACAO", "VAL_TAXA_JUROS_FLUTUANTE",
                "IND_ANO_COMERCIAL", "IND_DIAS_CORRIDOS", "IND_INCORPORA_JUROS",
                "NUM_ID_TIPO_INDICADOR", "NOM_AGENDA_PAGAMENTO",
            )
            consistency_cols = {name: resolve(juros, name) for name in consistency_names}
            missing_consistency = [
                name for name, value in consistency_cols.items() if not value
            ]
            if missing_consistency and escal_count:
                out.append(Finding(
                    "2b.escalonamento_consistency", cat, SEV_ERROR, "JUROS_FLUTUANTE",
                    False, count=escal_count, column=",".join(missing_consistency),
                    sample=_sample_keys(escal_titles, ["NUM_IF"], sample),
                    hint="Include every base-rate configuration column.",
                    message=f"Cannot compare segment configuration: {missing_consistency}.",
                ))
            else:
                config = segments.select("NUM_IF", "condition_key").join(
                    juros.select(
                        F.col(juros_key).cast("long").alias("condition_key"),
                        *[
                            F.coalesce(F.trim(F.col(value).cast("string")), F.lit("<NULL>"))
                            .alias(name)
                            for name, value in consistency_cols.items()
                        ],
                    ),
                    "condition_key", "inner",
                )
                counts = config.groupBy("NUM_IF").agg(*[
                    F.countDistinct(F.col(name)).alias(name) for name in consistency_names
                ])
                inconsistent = counts.where(reduce(
                    lambda left, right: left | right,
                    [F.col(name) > 1 for name in consistency_names],
                ))
                record(
                    "2b.escalonamento_consistency", "JUROS_FLUTUANTE",
                    ",".join(consistency_names), inconsistent, ["NUM_IF"], SEV_ERROR,
                    "Keep base rate/index configuration constant across active segments.",
                    "EMISSAO segments with inconsistent base-rate configuration.",
                )

    pending = tables.get("PENDENCIA_IF")
    if pending is not None:
        pending_cols = {
            name: resolve(pending, name) for name in (
                "NUM_ID_PENDENCIA_IF", "NUM_IF", "NUM_ID_TIPO_PENDENCIA",
                "DAT_INICIO_PENDENCIA", "DAT_FIM_PENDENCIA",
            )
        }
        required_pending = [
            name for name in (
                "NUM_IF", "NUM_ID_TIPO_PENDENCIA", "DAT_INICIO_PENDENCIA",
                "DAT_FIM_PENDENCIA",
            ) if not pending_cols[name]
        ]
        if required_pending:
            out.append(Finding(
                "2b.pendencia_availability", cat, SEV_WARN, "PENDENCIA_IF", False,
                column=",".join(required_pending),
                hint="Include pending lifecycle columns to validate workflow history.",
                message=f"Pending-history checks unavailable: {required_pending}.",
            ))
        else:
            pending_rows = pending.select(
                *(
                    [F.col(pending_cols["NUM_ID_PENDENCIA_IF"]).alias("pending_key")]
                    if pending_cols["NUM_ID_PENDENCIA_IF"] else []
                ),
                F.col(pending_cols["NUM_IF"]).cast("long").alias("NUM_IF"),
                _norm_code(F.col(pending_cols["NUM_ID_TIPO_PENDENCIA"])).alias(
                    "pending_type"
                ),
                try_cast(pending_cols["DAT_INICIO_PENDENCIA"], "date").alias(
                    "pending_start"
                ),
                try_cast(pending_cols["DAT_FIM_PENDENCIA"], "date").alias("pending_end"),
                (
                    F.col(pending_cols["DAT_INICIO_PENDENCIA"]).isNotNull()
                    & (F.trim(F.col(pending_cols["DAT_INICIO_PENDENCIA"]).cast("string")) != "")
                ).alias("pending_start_present"),
                (
                    F.col(pending_cols["DAT_FIM_PENDENCIA"]).isNotNull()
                    & (F.trim(F.col(pending_cols["DAT_FIM_PENDENCIA"]).cast("string")) != "")
                ).alias("pending_end_present"),
                (
                    F.col(pending_cols["DAT_FIM_PENDENCIA"]).isNull()
                    | (F.trim(F.col(pending_cols["DAT_FIM_PENDENCIA"]).cast("string")) == "")
                ).alias("pending_open"),
            )
            pending_keys = (
                ["pending_key"] if "pending_key" in pending_rows.columns else ["NUM_IF"]
            )
            record(
                "2b.pendencia_dates", "PENDENCIA_IF",
                "DAT_INICIO_PENDENCIA<=DAT_FIM_PENDENCIA",
                pending_rows.where(
                    (F.col("pending_start_present") & F.col("pending_start").isNull())
                    | (F.col("pending_end_present") & F.col("pending_end").isNull())
                    | (
                        F.col("pending_start").isNotNull()
                        & F.col("pending_end").isNotNull()
                        & (F.col("pending_start") > F.col("pending_end"))
                    )
                ),
                pending_keys, SEV_WARN,
                "Close pending rows on or after their start date.",
                "Pending-history rows whose end precedes their start.",
            )
            if root_cols["COD_SITUACAO_IF"]:
                open_final = pending_rows.where(
                    F.col("pending_type").isin("1", "29") & F.col("pending_open")
                ).join(roots.select("NUM_IF", "root_status"), "NUM_IF", "inner").where(
                    F.col("root_status") == "0"
                )
                record(
                    "2b.pendencia_open_final", "PENDENCIA_IF",
                    "DAT_FIM_PENDENCIA,COD_SITUACAO_IF", open_final,
                    pending_keys, SEV_WARN,
                    "Close type-1/type-29 pending rows before returning the IF to status 0.",
                    "Final-status CDBs retaining an open registration pending row.",
                )
            else:
                out.append(Finding(
                    "2b.pendencia_open_final", cat, SEV_WARN,
                    "INSTRUMENTO_FINANCEIRO,PENDENCIA_IF", False,
                    column="COD_SITUACAO_IF,DAT_FIM_PENDENCIA",
                    hint="Include COD_SITUACAO_IF to validate open pending rows.",
                    message="Open final-status pending check unavailable without IF status.",
                ))

    return out


# ---------------------------------------------------------------------------
# Category 2c - RDB resgate schedule conformance
# ---------------------------------------------------------------------------
def check_rdb_resgate_schedule_rules(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    """Validate the observed SEM TABELA/COM TABELA RDB schedule contract."""
    if profile.name != "rdb":
        return []

    cat = "RDB resgate schedule conformance"
    out: List[Finding] = []

    def record(
        check_id: str,
        table: str,
        column: str,
        bad: DataFrame,
        keys: List[str],
        severity: str,
        hint: str,
        message: str,
    ) -> None:
        count = bad.count()
        out.append(Finding(
            check_id, cat, severity if count else SEV_INFO, table, count == 0,
            count=count, column=column,
            sample=_sample_keys(bad, keys, sample) if count else [],
            hint=hint if count else "", message=message,
        ))

    def try_cast(column: str, sql_type: str):
        escaped = column.replace("`", "``")
        return F.expr(f"try_cast(`{escaped}` as {sql_type})")

    root = tables.get("INSTRUMENTO_FINANCEIRO")
    condicao = tables.get("CONDICAO_IF")
    resgate = tables.get("RESGATE")
    missing_tables = [
        name for name, frame in (
            ("INSTRUMENTO_FINANCEIRO", root),
            ("CONDICAO_IF", condicao),
            ("RESGATE", resgate),
        ) if frame is None
    ]
    if missing_tables:
        return [Finding(
            "2c.rdb_resgate_schedule_availability", cat, SEV_WARN,
            ",".join(missing_tables), False,
            hint="Include the RDB root, condition, and resgate tables.",
            message=f"RDB schedule checks unavailable; missing: {missing_tables}.",
        )]

    root_cols = {
        name: resolve(root, name)
        for name in ("NUM_IF", "NUM_TIPO_IF", "DAT_EMISSAO", "DAT_VENCIMENTO")
    }
    cond_cols = {
        name: resolve(condicao, name)
        for name in ("NUM_CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF")
    }
    res_cols = {
        name: resolve(resgate, name)
        for name in ("NUM_CONDICAO_IF", "COD_COND_RESGATE", "DAT_RESGATE")
    }
    required = {
        **{f"INSTRUMENTO_FINANCEIRO.{name}": value for name, value in root_cols.items()},
        **{f"CONDICAO_IF.{name}": value for name, value in cond_cols.items()},
        **{f"RESGATE.{name}": value for name, value in res_cols.items()},
    }
    missing_columns = [name for name, value in required.items() if not value]
    if missing_columns:
        return [Finding(
            "2c.rdb_resgate_schedule_availability", cat, SEV_WARN,
            ",".join(missing_columns), False,
            hint="Include the columns required to resolve RDB schedule ownership and dates.",
            message=f"RDB schedule checks unavailable; missing: {missing_columns}.",
        )]

    roots = _active(root).where(
        F.col(root_cols["NUM_TIPO_IF"]).cast("long") == profile.num_tipo_if
    ).select(
        F.col(root_cols["NUM_IF"]).cast("long").alias("NUM_IF"),
        try_cast(root_cols["DAT_EMISSAO"], "date").alias("root_emission"),
        try_cast(root_cols["DAT_VENCIMENTO"], "date").alias("root_maturity"),
    ).dropDuplicates(["NUM_IF"])
    conditions = _active(condicao).select(
        F.col(cond_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
        F.col(cond_cols["NUM_IF"]).cast("long").alias("NUM_IF"),
        _norm_code(F.col(cond_cols["COD_TIPO_CONDICAO_IF"])).alias("condition_type"),
    ).join(roots.select("NUM_IF"), "NUM_IF", "inner")
    parents = conditions.where(F.col("condition_type") == "20").join(
        _active(resgate).select(
            F.col(res_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
            _norm_code(F.col(res_cols["COD_COND_RESGATE"])).alias("resgate_mode"),
            try_cast(res_cols["DAT_RESGATE"], "date").alias("resgate_date"),
        ),
        "condition_key", "inner",
    )
    com_tabela = parents.where(F.col("resgate_mode") == "COM TABELA")

    schedule = tables.get("CONDICAO_RESGATE")
    if schedule is None:
        record(
            "2c.rdb_resgate_schedule_coverage", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
            com_tabela, ["NUM_IF", "condition_key"], SEV_ERROR,
            "Include at least one active schedule row per COM TABELA RDB.",
            "COM TABELA RDBs without an available redemption schedule.",
        )
        return out

    schedule_cols = {
        name: resolve(schedule, name)
        for name in ("NUM_CONDICAO_IF", "IND_EXCLUIDO", "DAT_RESGATE", "VAL_PERCENTUAL")
    }
    missing_schedule = [
        name for name in ("NUM_CONDICAO_IF", "DAT_RESGATE", "VAL_PERCENTUAL")
        if not schedule_cols[name]
    ]
    if missing_schedule:
        count = max(com_tabela.count(), schedule.count())
        return [Finding(
            "2c.rdb_resgate_schedule_availability", cat,
            SEV_ERROR if count else SEV_INFO, "CONDICAO_RESGATE", count == 0,
            count=count, column=",".join(missing_schedule),
            hint="Include the required schedule columns." if count else "",
            message=f"RDB schedule columns unavailable: {missing_schedule}.",
        )]

    active_schedule = schedule
    if schedule_cols["IND_EXCLUIDO"]:
        excluded = _norm_code(F.col(schedule_cols["IND_EXCLUIDO"])).isin("S", "Y", "1")
        active_schedule = schedule.where(F.coalesce(~excluded, F.lit(True)))
    children = active_schedule.select(
        F.col(schedule_cols["NUM_CONDICAO_IF"]).cast("long").alias("condition_key"),
        try_cast(schedule_cols["DAT_RESGATE"], "date").alias("schedule_date"),
        try_cast(schedule_cols["VAL_PERCENTUAL"], "double").alias("schedule_percentage"),
    )
    child_keys = children.select("condition_key").dropDuplicates()

    record(
        "2c.rdb_resgate_schedule_coverage", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
        com_tabela.join(child_keys, "condition_key", "leftanti"),
        ["NUM_IF", "condition_key"], SEV_ERROR,
        "Generate at least one active schedule row per COM TABELA RDB.",
        "COM TABELA RDBs without active redemption schedule rows.",
    )
    record(
        "2c.rdb_resgate_schedule_parent", "CONDICAO_RESGATE", "NUM_CONDICAO_IF",
        children.join(
            com_tabela.select("condition_key").dropDuplicates(),
            "condition_key", "leftanti",
        ),
        ["condition_key", "schedule_date"], SEV_ERROR,
        "Attach active schedules only to type-20 COM TABELA RDB resgates.",
        "Active RDB schedule rows attached to invalid or SEM TABELA parents.",
    )
    record(
        "2c.rdb_resgate_schedule_values", "CONDICAO_RESGATE",
        "DAT_RESGATE,VAL_PERCENTUAL",
        children.where(
            F.col("schedule_date").isNull()
            | F.col("schedule_percentage").isNull()
            | F.isnan(F.col("schedule_percentage"))
            | (F.abs(F.col("schedule_percentage")) == F.lit(float("inf")))
        ),
        ["condition_key", "schedule_date"], SEV_ERROR,
        "Populate a parseable date and percentage; percentages may exceed 100.",
        "Active RDB schedule rows with null or invalid values.",
    )

    duplicate_dates = children.where(F.col("schedule_date").isNotNull()).groupBy(
        "condition_key", "schedule_date"
    ).count().where(F.col("count") > 1)
    record(
        "2c.rdb_resgate_schedule_unique_dates", "CONDICAO_RESGATE", "DAT_RESGATE",
        duplicate_dates, ["condition_key", "schedule_date"], SEV_WARN,
        "Review duplicate dates; captured RDB schedules use one row per date.",
        "Duplicate active redemption dates under one RDB resgate condition.",
    )

    bounded = children.join(
        com_tabela.select("condition_key", "NUM_IF", "resgate_date"),
        "condition_key", "inner",
    ).join(roots, "NUM_IF", "inner")
    record(
        "2c.rdb_resgate_schedule_dates", "CONDICAO_RESGATE", "DAT_RESGATE",
        bounded.where(
            F.col("schedule_date").isNotNull()
            & (
                (F.col("root_emission").isNotNull()
                 & (F.col("schedule_date") < F.col("root_emission")))
                | (F.col("root_maturity").isNotNull()
                   & (F.col("schedule_date") > F.col("root_maturity")))
                | (F.col("resgate_date").isNotNull()
                   & (F.col("schedule_date") > F.col("resgate_date")))
            )
        ),
        ["NUM_IF", "condition_key", "schedule_date"], SEV_WARN,
        "Review schedule dates outside issuance and redemption/maturity bounds.",
        "Active RDB schedule rows outside observed instrument date bounds.",
    )

    ordered = children.where(
        F.col("schedule_date").isNotNull() & F.col("schedule_percentage").isNotNull()
    ).withColumn(
        "previous_percentage",
        F.lag("schedule_percentage").over(
            Window.partitionBy("condition_key").orderBy("schedule_date")
        ),
    )
    record(
        "2c.rdb_resgate_schedule_percentages", "CONDICAO_RESGATE", "VAL_PERCENTUAL",
        ordered.where(
            F.col("previous_percentage").isNotNull()
            & (F.col("schedule_percentage") <= F.col("previous_percentage"))
        ),
        ["condition_key", "schedule_date"], SEV_WARN,
        "Review non-increasing percentages; all captured schedules increase by date.",
        "RDB schedule percentages that do not increase with redemption date.",
    )
    return out


# ---------------------------------------------------------------------------
# Category 2d/6d/8d - Credito SCR graph and insertion-route evidence
# ---------------------------------------------------------------------------
CREDITO_SCR_TABLE = "CREDITO_SCR"
HISTORICO_CREDITO_SCR_TABLE = "HISTORICO_CREDITO_SCR"
LOTE_TABLE = "LOTE"


def _credito_scr_unavailable(check_id: str, missing: List[str], severity: str) -> Finding:
    return Finding(
        check_id, "Credito SCR", severity, ",".join(sorted({m.split('.')[0] for m in missing})),
        False, hint="Export the complete Credito SCR graph and rerun with target metadata.",
        message=f"Check unavailable; missing required input: {', '.join(missing)}.",
    )


def _credito_scr_columns(
    tables: Dict[str, DataFrame], requirements: Dict[str, Tuple[str, ...]]
) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    resolved: Dict[str, Dict[str, str]] = {}
    missing: List[str] = []
    for table, names in requirements.items():
        df = tables.get(table)
        if df is None:
            missing.append(table)
            continue
        resolved[table] = {name: resolve(df, name) for name in names}
        missing.extend(
            f"{table}.{name}" for name, actual in resolved[table].items() if not actual
        )
    return resolved, missing


def _normalized_history_action(column):
    ascii_text = F.translate(
        F.upper(F.trim(column.cast("string"))),
        "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "AAAAAEEEEIIIIOOOOOUUUUC",
    )
    return F.regexp_replace(ascii_text, "[^A-Z0-9]", "")


def _credito_scr_text(column):
    return F.trim(column.cast("string"))


def _credito_scr_active(df: DataFrame) -> DataFrame:
    excluded = resolve(df, "DAT_EXCLUSAO")
    if not excluded:
        return df
    value = F.col(excluded)
    return df.where(value.isNull() | (F.trim(value.cast("string")) == ""))


def check_credito_scr_identity(
    tables: Dict[str, DataFrame], profile: ValidationProfile, sample: int
) -> List[Finding]:
    if profile.pipeline != "credito_scr":
        return []
    credit = tables.get(CREDITO_SCR_TABLE)
    if credit is None:
        return [_credito_scr_unavailable("0.identity.root", [CREDITO_SCR_TABLE], SEV_ERROR)]
    key = resolve(credit, "NUM_ID_CREDITO_SCR")
    if not key:
        return [_credito_scr_unavailable(
            "0.identity.root", [f"{CREDITO_SCR_TABLE}.NUM_ID_CREDITO_SCR"], SEV_ERROR
        )]
    active = _credito_scr_active(credit)
    count = active.count()
    return [Finding(
        "0.identity.root", "Product identity", SEV_INFO if count else SEV_ERROR,
        CREDITO_SCR_TABLE, count > 0, count=count, column="NUM_ID_CREDITO_SCR",
        sample=_sample_keys(active, [key], sample) if count else [],
        hint="Export at least one active CREDITO_SCR row." if not count else "",
        message="Active CREDITO_SCR rows define the credito_scr product universe.",
    )]


def check_credito_scr_metadata(
    meta: Metadata, no_oracle: bool, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "credito_scr":
        return []
    if no_oracle:
        return [Finding(
            "0.credito_scr_metadata", "Coverage", SEV_INFO, "Oracle metadata", True,
            message="Authoritative Credito SCR metadata deferred under --no-oracle; "
                    "the run is already marked PARTIAL.",
        )]
    required = (LOTE_TABLE, CREDITO_SCR_TABLE, HISTORICO_CREDITO_SCR_TABLE)
    missing = [table for table in required if table not in meta.tables]
    missing_pk = [table for table in required if table in meta.tables and not meta.pk.get(table)]
    failed = bool(missing or missing_pk)
    return [Finding(
        "0.credito_scr_metadata", "Coverage", SEV_ERROR if failed else SEV_INFO,
        ",".join(required), not failed, count=len(missing) + len(missing_pk),
        hint="Extract metadata from the receiving Oracle schema; do not infer constraints "
             "from the insertion log." if failed else "",
        message=f"Missing Oracle table metadata={missing}; missing PK metadata={missing_pk}."
                if failed else "Authoritative Oracle table and PK metadata are available.",
    )]


def check_credito_scr_graph(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "credito_scr":
        return []
    requirements = {
        LOTE_TABLE: (
            "NUM_ID_LOTE", "NOME_LOTE", "NUM_CONTA_PARTICIPANTE", "NUM_ID_TIPO_LOTE",
            "DAT_EXCLUSAO",
        ),
        CREDITO_SCR_TABLE: (
            "NUM_ID_CREDITO_SCR", "COD_CREDITO_SCR", "NUM_ID_LOTE", "DAT_EXCLUSAO",
        ),
        HISTORICO_CREDITO_SCR_TABLE: (
            "NUM_ID_HISTORICO_CREDITO_SCR", "NUM_ID_CREDITO_SCR", "COD_CREDITO_SCR",
            "NUM_ID_LOTE", "NUM_CONTA_PARTICIPANTE", "TXT_DESCRICAO",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_credito_scr_unavailable("2d.graph.availability", missing, SEV_ERROR)]

    lot_cols = columns[LOTE_TABLE]
    credit_cols = columns[CREDITO_SCR_TABLE]
    history_cols = columns[HISTORICO_CREDITO_SCR_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE]).select(
        _canon_key_col(F.col(lot_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _credito_scr_text(F.col(lot_cols["NOME_LOTE"])).alias("lot_name"),
        _canon_key_col(F.col(lot_cols["NUM_CONTA_PARTICIPANTE"])).alias("lot_account"),
        _canon_key_col(F.col(lot_cols["NUM_ID_TIPO_LOTE"])).alias("lot_type"),
    )
    credits = _credito_scr_active(tables[CREDITO_SCR_TABLE]).select(
        _canon_key_col(F.col(credit_cols["NUM_ID_CREDITO_SCR"])).alias("credit_id"),
        _credito_scr_text(F.col(credit_cols["COD_CREDITO_SCR"])).alias("credit_code"),
        _canon_key_col(F.col(credit_cols["NUM_ID_LOTE"])).alias("credit_lot_id"),
    )
    histories = tables[HISTORICO_CREDITO_SCR_TABLE].select(
        _canon_key_col(F.col(history_cols["NUM_ID_HISTORICO_CREDITO_SCR"])).alias("history_id"),
        _canon_key_col(F.col(history_cols["NUM_ID_CREDITO_SCR"])).alias("history_credit_id"),
        _credito_scr_text(F.col(history_cols["COD_CREDITO_SCR"])).alias("history_credit_code"),
        _canon_key_col(F.col(history_cols["NUM_ID_LOTE"])).alias("history_lot_id"),
        _canon_key_col(F.col(history_cols["NUM_CONTA_PARTICIPANTE"])).alias("history_account"),
        _normalized_history_action(F.col(history_cols["TXT_DESCRICAO"])).alias("action"),
    )
    inclusions = histories.where(F.col("action") == "INCLUSAO")
    out: List[Finding] = []

    duplicate_lots = lots.groupBy("lot_name", "lot_account", "lot_type").count().where(
        F.col("count") > 1
    )
    count = duplicate_lots.count()
    out.append(Finding(
        "2d.active_lot_natural_key", "Credito SCR graph",
        SEV_ERROR if count else SEV_INFO, LOTE_TABLE, count == 0, count=count,
        column="NOME_LOTE,NUM_CONTA_PARTICIPANTE,NUM_ID_TIPO_LOTE",
        sample=_sample_keys(duplicate_lots, ["lot_name", "lot_account", "lot_type"], sample),
        hint="Keep at most one active lot for the serialized application natural key."
             if count else "",
        message="Duplicate active lot natural keys.",
    ))

    lot_counts = lots.groupBy("lot_id").count().withColumnRenamed("count", "lot_count")
    bad_lot_links = credits.join(
        lot_counts, credits.credit_lot_id == lot_counts.lot_id, "left"
    ).where(F.coalesce(F.col("lot_count"), F.lit(0)) != 1)
    count = bad_lot_links.count()
    out.append(Finding(
        "2d.credit_active_lot", "Credito SCR graph", SEV_ERROR if count else SEV_INFO,
        CREDITO_SCR_TABLE, count == 0, count=count, column="NUM_ID_LOTE",
        sample=_sample_keys(bad_lot_links, ["credit_id", "credit_lot_id"], sample),
        hint="Point every active credit to exactly one active lot." if count else "",
        message="Active credits without exactly one active lot.",
    ))

    coded = credits.withColumn(
        "code_count", F.count(F.lit(1)).over(Window.partitionBy("credit_code"))
    )
    bad_codes = coded.where(
        F.col("credit_code").isNull() | (F.col("credit_code") == "")
        | (F.col("code_count") > 1)
    )
    count = bad_codes.count()
    out.append(Finding(
        "2d.credit_code_unique", "Credito SCR graph", SEV_ERROR if count else SEV_INFO,
        CREDITO_SCR_TABLE, count == 0, count=count, column="COD_CREDITO_SCR",
        sample=_sample_keys(bad_codes, ["credit_id", "credit_code"], sample),
        hint="Generate a nonblank unique active COD_CREDITO_SCR; do not infer its regex."
             if count else "",
        message="Active credits with blank or duplicate business codes.",
    ))

    inclusion_counts = inclusions.groupBy("history_credit_id").count().withColumnRenamed(
        "count", "inclusion_count"
    )
    bad_history_counts = credits.join(
        inclusion_counts, credits.credit_id == inclusion_counts.history_credit_id, "left"
    ).where(F.coalesce(F.col("inclusion_count"), F.lit(0)) != 1)
    count = bad_history_counts.count()
    out.append(Finding(
        "2d.inclusion_history", "Credito SCR graph", SEV_ERROR if count else SEV_INFO,
        HISTORICO_CREDITO_SCR_TABLE, count == 0, count=count, column="TXT_DESCRICAO",
        sample=_sample_keys(bad_history_counts, ["credit_id", "inclusion_count"], sample),
        hint="Preserve exactly one history action normalized to INCLUSAO per active credit."
             if count else "",
        message="Active credits without exactly one inclusion history row.",
    ))

    inclusion_identity = inclusions.join(
        credits, inclusions.history_credit_id == credits.credit_id, "inner"
    ).join(lots, F.col("credit_lot_id") == lots.lot_id, "left")
    bad_identity = inclusion_identity.where(
        ~F.col("history_credit_code").eqNullSafe(F.col("credit_code"))
        | ~F.col("history_lot_id").eqNullSafe(F.col("credit_lot_id"))
        | ~F.col("history_account").eqNullSafe(F.col("lot_account"))
    )
    count = bad_identity.count()
    out.append(Finding(
        "2d.inclusion_identity", "Credito SCR graph", SEV_ERROR if count else SEV_INFO,
        HISTORICO_CREDITO_SCR_TABLE, count == 0, count=count,
        column="NUM_ID_CREDITO_SCR,COD_CREDITO_SCR,NUM_ID_LOTE,NUM_CONTA_PARTICIPANTE",
        sample=_sample_keys(bad_identity, ["history_id", "credit_id"], sample),
        hint="Keep inclusion identity/ownership equal to the active credit and its lot."
             if count else "",
        message="Inclusion histories with mismatching identity or ownership.",
    ))
    return out


def check_credito_scr_target_frames(
    tables: Dict[str, DataFrame], modalidade: Optional[DataFrame],
    eligible_bases: Optional[DataFrame], feature_toggle: Optional[DataFrame], sample: int,
    profile: ValidationProfile, lookup_errors: Optional[Dict[str, str]] = None,
    account_profile: Optional[DataFrame] = None, registration_profile: bool = False,
    existing_ipocs: Optional[DataFrame] = None,
) -> List[Finding]:
    if profile.pipeline != "credito_scr":
        return []
    lookup_errors = lookup_errors or {}
    requirements = {
        LOTE_TABLE: ("NUM_ID_LOTE", "NUM_CONTA_PARTICIPANTE", "DAT_EXCLUSAO"),
        CREDITO_SCR_TABLE: (
            "NUM_ID_CREDITO_SCR", "NUM_ID_LOTE", "NUM_ID_MODALIDADE_CREDITO",
            "NUM_ID_BASE_CREDITO", "COD_IPOC", "DAT_SALDO_REMANESCENTE", "DAT_EXCLUSAO",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_credito_scr_unavailable("6d.lookup.availability", missing, SEV_WARN)]
    lot_cols, credit_cols = columns[LOTE_TABLE], columns[CREDITO_SCR_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE]).select(
        _canon_key_col(F.col(lot_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _canon_key_col(F.col(lot_cols["NUM_CONTA_PARTICIPANTE"])).alias("lot_account"),
    )
    credits = _credito_scr_active(tables[CREDITO_SCR_TABLE]).select(
        _canon_key_col(F.col(credit_cols["NUM_ID_CREDITO_SCR"])).alias("credit_id"),
        _canon_key_col(F.col(credit_cols["NUM_ID_LOTE"])).alias("credit_lot_id"),
        _canon_key_col(F.col(credit_cols["NUM_ID_MODALIDADE_CREDITO"])).alias("modalidade_id"),
        _canon_key_col(F.col(credit_cols["NUM_ID_BASE_CREDITO"])).alias("base_id"),
        _credito_scr_text(F.col(credit_cols["COD_IPOC"])).alias("ipoc"),
        F.to_date(F.col(credit_cols["DAT_SALDO_REMANESCENTE"])).alias("reference_date"),
    )
    out: List[Finding] = []

    if modalidade is None:
        out.append(_credito_scr_unavailable(
            "6d.lookup.modalidade", [lookup_errors.get("MODALIDADE_CREDITO", "MODALIDADE_CREDITO")],
            SEV_WARN,
        ))
    else:
        mid = resolve(modalidade, "NUM_ID_MODALIDADE_CREDITO")
        mcode = resolve(modalidade, "COD_MODALIDADE_CREDITO")
        if not mid or not mcode:
            out.append(_credito_scr_unavailable(
                "6d.lookup.modalidade", ["MODALIDADE_CREDITO required columns"], SEV_WARN
            ))
        else:
            modes = modalidade.select(
                _canon_key_col(F.col(mid)).alias("lookup_modalidade_id"),
                _credito_scr_text(F.col(mcode)).alias("modalidade_code"),
            ).dropDuplicates(["lookup_modalidade_id"])
            bad = credits.join(
                modes, credits.modalidade_id == modes.lookup_modalidade_id, "left"
            ).where(F.col("lookup_modalidade_id").isNull() | (F.col("modalidade_code") == "9999"))
            count = bad.count()
            out.append(Finding(
                "6d.lookup.modalidade", "Credito SCR target lookups",
                SEV_ERROR if count else SEV_INFO, CREDITO_SCR_TABLE, count == 0, count=count,
                column="NUM_ID_MODALIDADE_CREDITO", sample=_sample_keys(
                    bad, ["credit_id", "modalidade_id", "modalidade_code"], sample
                ),
                hint="Use a target modalidade that resolves and whose code is not 9999."
                     if count else "",
                message="Credits with missing or application-excluded modalidade.",
            ))

    if eligible_bases is None:
        out.append(_credito_scr_unavailable(
            "6d.lookup.base_eligibility",
            [lookup_errors.get("PARAMETRO_BASE_CREDITO", "PARAMETRO_BASE_CREDITO")], SEV_WARN,
        ))
    else:
        account = resolve(eligible_bases, "NUM_CONTA_PARTICIPANTE")
        base = resolve(eligible_bases, "NUM_ID_BASE_CREDITO")
        if not account or not base:
            out.append(_credito_scr_unavailable(
                "6d.lookup.base_eligibility", ["eligible base required columns"], SEV_WARN
            ))
        else:
            eligible = eligible_bases.select(
                _canon_key_col(F.col(account)).alias("eligible_account"),
                _canon_key_col(F.col(base)).alias("eligible_base"),
            ).dropDuplicates()
            credit_bases = credits.join(
                lots, credits.credit_lot_id == lots.lot_id, "left"
            ).join(
                F.broadcast(eligible),
                (F.col("lot_account") == F.col("eligible_account"))
                & (F.col("base_id") == F.col("eligible_base")),
                "left",
            )
            bad = credit_bases.where(F.col("eligible_base").isNull())
            count = bad.count()
            out.append(Finding(
                "6d.lookup.base_eligibility", "Credito SCR target lookups",
                SEV_ERROR if count else SEV_INFO, CREDITO_SCR_TABLE, count == 0, count=count,
                column="NUM_ID_BASE_CREDITO,NUM_CONTA_PARTICIPANTE",
                sample=_sample_keys(bad, ["credit_id", "lot_account", "base_id"], sample),
                hint="Use a type-1 credit base authorized for the lot participant."
                     if count else "",
                message="Credits whose base is not eligible for the lot participant.",
            ))

    if feature_toggle is None:
        out.append(_credito_scr_unavailable(
            "6d.lookup.ipoc_unique",
            [lookup_errors.get("TCTPFEATURE_TOGGLE", "TCTPFEATURE_TOGGLE")], SEV_WARN,
        ))
    else:
        start = resolve(feature_toggle, "DATA_INIC_VIG_FTRE")
        end = resolve(feature_toggle, "DATA_FIM_VIG_FTRE")
        enabled = resolve(feature_toggle, "IND_FTRE_HAB")
        if not start or not end or not enabled:
            out.append(_credito_scr_unavailable(
                "6d.lookup.ipoc_unique", ["feature toggle required columns"], SEV_WARN
            ))
        else:
            periods = feature_toggle.where(_norm_code(F.col(enabled)) == "S").select(
                F.to_date(F.col(start)).alias("toggle_start"),
                F.to_date(F.col(end)).alias("toggle_end"),
            )
            enabled_credits = credits.join(
                F.broadcast(periods),
                (F.col("reference_date") >= F.col("toggle_start"))
                & (F.col("reference_date") <= F.col("toggle_end")),
                "left_semi",
            )
            enabled_count = enabled_credits.limit(1).count()
            if not enabled_count:
                out.append(Finding(
                    "6d.lookup.ipoc_unique", "Credito SCR target lookups", SEV_INFO,
                    CREDITO_SCR_TABLE, True, column="COD_IPOC",
                    message="IPOC uniqueness toggle is disabled for all synthetic "
                            "credit reference dates.",
                ))
            elif existing_ipocs is None:
                out.append(_credito_scr_unavailable(
                    "6d.lookup.ipoc_unique",
                    [lookup_errors.get("CREDITO_SCR_TARGET", "target CREDITO_SCR IPOCs")],
                    SEV_WARN,
                ))
            else:
                target_ipoc_column = resolve(existing_ipocs, "COD_IPOC")
                if not target_ipoc_column:
                    out.append(_credito_scr_unavailable(
                        "6d.lookup.ipoc_unique", ["target CREDITO_SCR.COD_IPOC"], SEV_WARN
                    ))
                else:
                    duplicate_ipocs = credits.where(
                        F.col("ipoc").isNotNull() & (F.col("ipoc") != "")
                    ).groupBy("ipoc").count().where(F.col("count") > 1).select("ipoc")
                    target_ipocs = existing_ipocs.select(
                        _credito_scr_text(F.col(target_ipoc_column)).alias("ipoc")
                    ).where(
                        F.col("ipoc").isNotNull() & (F.col("ipoc") != "")
                    ).dropDuplicates()
                    conflicting_ipocs = duplicate_ipocs.unionByName(
                        target_ipocs
                    ).dropDuplicates()
                    bad = enabled_credits.join(conflicting_ipocs, "ipoc", "inner")
                    count = bad.count()
                    out.append(Finding(
                        "6d.lookup.ipoc_unique", "Credito SCR target lookups",
                        SEV_ERROR if count else SEV_INFO, CREDITO_SCR_TABLE,
                        count == 0, count=count, column="COD_IPOC",
                        sample=_sample_keys(bad, ["credit_id", "ipoc"], sample),
                        hint="Regenerate IPOCs duplicated in the output or active target when "
                             "HAB_VALIDACAO_UNIC_IPOC_SCR is active." if count else "",
                        message="Toggle-enabled synthetic credits with duplicate IPOCs.",
                    ))

    if registration_profile:
        if account_profile is None:
            out.append(_credito_scr_unavailable(
                "8d.profile.account_eligibility",
                [lookup_errors.get("CONTA_PARTICIPANTE", "CONTA_PARTICIPANTE")], SEV_WARN,
            ))
        else:
            account_columns = {
                name: resolve(account_profile, name)
                for name in (
                    "NUM_CONTA_PARTICIPANTE", "NUM_ID_SITUACAO_CONTA",
                    "COD_CONTA_PARTICIPANTE", "NUM_ID_AREA_ATUACAO", "COD_TIPO_ACESSO",
                )
            }
            missing_account = [
                name for name, actual in account_columns.items() if not actual
            ]
            if missing_account:
                out.append(_credito_scr_unavailable(
                    "8d.profile.account_eligibility",
                    [f"CONTA_PARTICIPANTE.{name}" for name in missing_account], SEV_WARN,
                ))
            else:
                eligible_accounts = account_profile.where(
                    _norm_code(F.col(account_columns["NUM_ID_SITUACAO_CONTA"])).isin("1", "2")
                    & F.col(account_columns["COD_CONTA_PARTICIPANTE"])
                    .cast("string").rlike(r"^[0-9]{5}\.40-[0-9]$")
                    & (_norm_code(F.col(account_columns["NUM_ID_AREA_ATUACAO"])) == "1")
                    & (_norm_code(F.col(account_columns["COD_TIPO_ACESSO"])) == "L")
                ).select(
                    _canon_key_col(F.col(account_columns["NUM_CONTA_PARTICIPANTE"]))
                    .alias("eligible_account")
                ).dropDuplicates()
                bad = lots.join(
                    F.broadcast(eligible_accounts),
                    lots.lot_account == eligible_accounts.eligible_account,
                    "left_anti",
                )
                count = bad.count()
                out.append(Finding(
                    "8d.profile.account_eligibility",
                    "Credito SCR observed insertion profile", SEV_WARN if count else SEV_INFO,
                    LOTE_TABLE, count == 0, count=count, column="NUM_CONTA_PARTICIPANTE",
                    sample=_sample_keys(bad, ["lot_id", "lot_account"], sample),
                    hint="Capture other Credito SCR routes before promoting these account "
                         "predicates to hard validation." if count else "",
                    message="Active lot accounts outside the observed Lastro-LCI role profile.",
                ))
    return out


def load_credito_scr_target_frames(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame],
    registration_profile: bool = False,
) -> Tuple[Dict[str, DataFrame], Dict[str, str]]:
    queries = {
        "MODALIDADE_CREDITO": (
            "SELECT NUM_ID_MODALIDADE_CREDITO, COD_MODALIDADE_CREDITO "
            f"FROM {cfg.schema}.MODALIDADE_CREDITO"
        ),
        "TCTPFEATURE_TOGGLE": (
            "SELECT IND_FTRE_HAB, DATA_INIC_VIG_FTRE, DATA_FIM_VIG_FTRE "
            f"FROM {cfg.schema}.TCTPFEATURE_TOGGLE "
            "WHERE COD_FTRE_TOG='HAB_VALIDACAO_UNIC_IPOC_SCR'"
        ),
    }
    frames: Dict[str, DataFrame] = {}
    errors: Dict[str, str] = {}
    for name, query in queries.items():
        try:
            remote = _jdbc(spark, cfg, query)
            rows = remote.collect()
            frames[name] = spark.createDataFrame(rows, remote.schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Credito SCR target lookup failed for %s: %s", name, exc)
            errors[name] = str(exc)

    lot = tables.get(LOTE_TABLE)
    credit = tables.get(CREDITO_SCR_TABLE)
    lot_id = resolve(lot, "NUM_ID_LOTE") if lot is not None else None
    lot_account = resolve(lot, "NUM_CONTA_PARTICIPANTE") if lot is not None else None
    credit_lot = resolve(credit, "NUM_ID_LOTE") if credit is not None else None
    credit_base = resolve(credit, "NUM_ID_BASE_CREDITO") if credit is not None else None
    if not all((lot_id, lot_account, credit_lot, credit_base)):
        errors["PARAMETRO_BASE_CREDITO"] = "credit/lot base-key columns unavailable"
    else:
        pairs = _credito_scr_active(credit).select(
            _canon_key_col(F.col(credit_lot)).alias("lot_id"),
            _canon_key_col(F.col(credit_base)).alias("base_id"),
        ).join(
            _credito_scr_active(lot).select(
                _canon_key_col(F.col(lot_id)).alias("lot_id"),
                _canon_key_col(F.col(lot_account)).alias("account_id"),
            ),
            "lot_id", "inner",
        ).select("account_id", "base_id").where(
            F.col("account_id").isNotNull() & F.col("base_id").isNotNull()
        ).dropDuplicates().limit(100_001).collect()
        if len(pairs) > 100_000:
            errors["PARAMETRO_BASE_CREDITO"] = "more than 100000 synthetic account/base pairs"
        else:
            base_rows = []
            base_schema = None
            try:
                for offset in range(0, len(pairs), 500):
                    predicates = " OR ".join(
                        "(pb.NUM_CONTA_PARTICIPANTE=" + _sql_literal(row["account_id"])
                        + " AND pb.NUM_ID_BASE_CREDITO=" + _sql_literal(row["base_id"]) + ")"
                        for row in pairs[offset:offset + 500]
                    )
                    query = (
                        "SELECT DISTINCT pb.NUM_CONTA_PARTICIPANTE, pb.NUM_ID_BASE_CREDITO "
                        f"FROM {cfg.schema}.PARAMETRO_BASE_CREDITO pb "
                        f"JOIN {cfg.schema}.BASE_CREDITO b "
                        "ON b.NUM_ID_BASE_CREDITO=pb.NUM_ID_BASE_CREDITO "
                        f"JOIN {cfg.schema}.TIPO_BASE_CREDITO tb "
                        "ON tb.NUM_ID_TIPO_BASE_CREDITO=b.NUM_ID_TIPO_BASE_CREDITO "
                        "WHERE TRIM(tb.COD_TIPO_BASE_CREDITO)='1' AND (" + predicates + ")"
                    )
                    remote = _jdbc(spark, cfg, query)
                    rows = remote.collect()
                    base_schema = base_schema or remote.schema
                    base_rows.extend(rows)
                frames["PARAMETRO_BASE_CREDITO"] = (
                    spark.createDataFrame(base_rows, base_schema)
                    if base_schema is not None else spark.createDataFrame(
                        [], "NUM_CONTA_PARTICIPANTE string, NUM_ID_BASE_CREDITO string"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Credito SCR base eligibility lookup failed: %s", exc)
                errors["PARAMETRO_BASE_CREDITO"] = str(exc)

    ipoc_column = resolve(credit, "COD_IPOC") if credit is not None else None
    if not ipoc_column:
        errors["CREDITO_SCR_TARGET"] = "CREDITO_SCR.COD_IPOC unavailable"
    else:
        ipocs = [
            row["ipoc"] for row in _credito_scr_active(credit).select(
                _credito_scr_text(F.col(ipoc_column)).alias("ipoc")
            ).where(F.col("ipoc").isNotNull() & (F.col("ipoc") != ""))
            .dropDuplicates().limit(100_001).collect()
        ]
        if len(ipocs) > 100_000:
            errors["CREDITO_SCR_TARGET"] = "more than 100000 distinct synthetic IPOCs"
        else:
            target_rows = []
            target_schema = None
            try:
                for offset in range(0, len(ipocs), 1000):
                    literals = ", ".join(
                        _sql_literal(value) for value in ipocs[offset:offset + 1000]
                    )
                    query = (
                        "SELECT COD_IPOC "
                        f"FROM {cfg.schema}.CREDITO_SCR WHERE DAT_EXCLUSAO IS NULL "
                        f"AND TRIM(COD_IPOC) IN ({literals})"
                    )
                    remote = _jdbc(spark, cfg, query)
                    rows = remote.collect()
                    target_schema = target_schema or remote.schema
                    target_rows.extend(rows)
                frames["CREDITO_SCR_TARGET"] = (
                    spark.createDataFrame(target_rows, target_schema)
                    if target_schema is not None else spark.createDataFrame([], "COD_IPOC string")
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Credito SCR target IPOC lookup failed: %s", exc)
                errors["CREDITO_SCR_TARGET"] = str(exc)
    if registration_profile:
        account_column = resolve(lot, "NUM_CONTA_PARTICIPANTE") if lot is not None else None
        if not account_column:
            errors["CONTA_PARTICIPANTE"] = "LOTE.NUM_CONTA_PARTICIPANTE unavailable"
        else:
            account_keys = [
                _canon_key(row["account"])
                for row in _credito_scr_active(lot).select(
                    _canon_key_col(F.col(account_column)).alias("account")
                ).where(F.col("account").isNotNull()).dropDuplicates().limit(1_000_001).collect()
            ]
            if len(account_keys) > 1_000_000:
                errors["CONTA_PARTICIPANTE"] = "more than 1000000 distinct lot accounts"
            elif not account_keys:
                errors["CONTA_PARTICIPANTE"] = "no active lot accounts"
            else:
                account_rows = []
                account_schema = None
                try:
                    for offset in range(0, len(account_keys), 1000):
                        literals = ", ".join(
                            _sql_literal(value) for value in account_keys[offset:offset + 1000]
                        )
                        query = (
                            "SELECT cp.NUM_CONTA_PARTICIPANTE, cp.NUM_ID_SITUACAO_CONTA, "
                            "cp.COD_CONTA_PARTICIPANTE, vf.NUM_ID_AREA_ATUACAO, "
                            "vf.COD_TIPO_ACESSO "
                            f"FROM {cfg.schema}.CONTA_PARTICIPANTE cp "
                            f"LEFT JOIN {cfg.schema}.V_FAMILIA_CONTAS vf "
                            "ON cp.COD_CONTA_PARTICIPANTE=vf.COD_CONTA_MEMBRO "
                            f"WHERE cp.NUM_CONTA_PARTICIPANTE IN ({literals})"
                        )
                        remote = _jdbc(spark, cfg, query)
                        rows = remote.collect()
                        account_schema = account_schema or remote.schema
                        account_rows.extend(rows)
                    frames["CONTA_PARTICIPANTE"] = spark.createDataFrame(
                        account_rows, account_schema
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Credito SCR account lookup failed: %s", exc)
                    errors["CONTA_PARTICIPANTE"] = str(exc)
    return frames, errors


def check_credito_scr_registration_profile(
    tables: Dict[str, DataFrame], sample: int, registration_profile: bool,
    profile: ValidationProfile,
) -> List[Finding]:
    if profile.pipeline != "credito_scr" or not registration_profile:
        return []
    requirements = {
        LOTE_TABLE: ("NUM_ID_LOTE", "IND_REVOLVENCIA", "DAT_EXCLUSAO"),
        CREDITO_SCR_TABLE: (
            "NUM_ID_CREDITO_SCR", "NUM_TIPO_IF", "COD_TIPO_PESSOA", "IND_MULTIPLO_IPOC",
            "NUM_ID_BASE_CREDITO", "NUM_ID_TIPO_CREDITO", "NUM_ID_MODALIDADE_CREDITO",
            "NUM_ID_INDEXADOR_CREDITO", "VAL_SALDO_REMANESCENTE", "VAL_CONTRATADO",
            "DAT_CONTRATACAO", "DAT_VENCIMENTO", "DAT_EXCLUSAO", "NUM_IF", "QTD_CREDITO",
            "DAT_SALDO_REMANESCENTE", "COD_CONTRATO_SCR",
            "COD_REFERENCIA_EXTERNA_DEVEDOR", "VAL_PERCENTUAL_INDEXADOR",
            "VAL_PERCENTUAL_TAXA_ANUAL", "COD_IPOC",
        ),
        HISTORICO_CREDITO_SCR_TABLE: (
            "NUM_ID_HISTORICO_CREDITO_SCR", "NUM_ID_CREDITO_SCR", "TXT_DESCRICAO",
            "COD_ID_CANAL", "NUM_TIPO_IF", "COD_TIPO_PESSOA", "IND_MULTIPLO_IPOC",
            "NUM_ID_LOTE", "VAL_SALDO_REMANESCENTE", "VAL_CONTRATADO",
            "DAT_CONTRATACAO", "DAT_VENCIMENTO", "NUM_IF_CREDITO", "QTD_CREDITO",
            "DAT_SALDO_REMANESCENTE", "COD_CONTRATO_SCR", "NUM_ID_TIPO_CREDITO",
            "NUM_ID_MODALIDADE_CREDITO", "NUM_ID_INDEXADOR_CREDITO",
            "COD_REFERENCIA_EXTERNA_DEVEDOR", "VAL_PERCENTUAL_INDEXADOR",
            "VAL_PERCENTUAL_TAXA_ANUAL", "COD_IPOC",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_credito_scr_unavailable("8d.profile.availability", missing, SEV_WARN)]
    lot_cols = columns[LOTE_TABLE]
    credit_cols = columns[CREDITO_SCR_TABLE]
    history_cols = columns[HISTORICO_CREDITO_SCR_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE])
    credits = _credito_scr_active(tables[CREDITO_SCR_TABLE])
    inclusion = tables[HISTORICO_CREDITO_SCR_TABLE].where(
        _normalized_history_action(F.col(history_cols["TXT_DESCRICAO"])) == "INCLUSAO"
    )
    out: List[Finding] = []

    constant_sets = (
        ("8d.profile.lot_constants", LOTE_TABLE, lots, lot_cols, {"IND_REVOLVENCIA": "N"},
         [lot_cols["NUM_ID_LOTE"]]),
        ("8d.profile.credit_constants", CREDITO_SCR_TABLE, credits, credit_cols,
         {"NUM_TIPO_IF": "143", "COD_TIPO_PESSOA": "PF", "IND_MULTIPLO_IPOC": "N",
          "NUM_ID_BASE_CREDITO": "505"}, [credit_cols["NUM_ID_CREDITO_SCR"]]),
        ("8d.profile.history_constants", HISTORICO_CREDITO_SCR_TABLE, inclusion, history_cols,
         {"COD_ID_CANAL": "6"}, [history_cols["NUM_ID_HISTORICO_CREDITO_SCR"]]),
    )
    for check_id, table, frame, cols, expected, keys in constant_sets:
        mismatch = reduce(
            lambda left, right: left | right,
            [~F.coalesce(_norm_code(F.col(cols[name])) == value, F.lit(False))
             for name, value in expected.items()],
        )
        bad = frame.where(mismatch)
        count = bad.count()
        out.append(Finding(
            check_id, "Credito SCR observed insertion profile",
            SEV_WARN if count else SEV_INFO, table, count == 0, count=count,
            column=",".join(expected), sample=_sample_keys(bad, keys, sample),
            hint="Review against additional inclusion samples before promoting this profile."
                 if count else "",
            message="Rows differing from one-batch Lastro-LCI insertion constants.",
        ))

    variants = credits.select(
        _canon_key_col(F.col(credit_cols["NUM_ID_CREDITO_SCR"])).alias("credit_id"),
        _canon_key_col(F.col(credit_cols["NUM_ID_TIPO_CREDITO"])).alias("credit_type"),
        _canon_key_col(F.col(credit_cols["NUM_ID_MODALIDADE_CREDITO"])).alias("modalidade"),
        _canon_key_col(F.col(credit_cols["NUM_ID_INDEXADOR_CREDITO"])).alias("indexer"),
    )
    observed = (
        ("17", "46", "2"), ("18", "47", "5"), ("18", "47", "1"),
        ("19", "12", "2"), ("20", "12", "2"),
    )
    observed_predicate = reduce(
        lambda left, right: left | right,
        [
            (F.col("credit_type") == credit_type)
            & (F.col("modalidade") == modalidade)
            & (F.col("indexer") == indexer)
            for credit_type, modalidade, indexer in observed
        ],
    )
    bad_variants = variants.where(~F.coalesce(observed_predicate, F.lit(False)))
    count = bad_variants.count()
    out.append(Finding(
        "8d.profile.variant_drift", "Credito SCR observed insertion profile",
        SEV_WARN if count else SEV_INFO, CREDITO_SCR_TABLE, count == 0, count=count,
        column="NUM_ID_TIPO_CREDITO,NUM_ID_MODALIDADE_CREDITO,NUM_ID_INDEXADOR_CREDITO",
        sample=_sample_keys(
            bad_variants, ["credit_id", "credit_type", "modalidade", "indexer"], sample
        ),
        hint="Confirm new combinations against target lookups and additional route samples."
             if count else "",
        message="Combinations outside the five observed insertion variants.",
    ))

    financial = credits.select(
        F.col(credit_cols["NUM_ID_CREDITO_SCR"]).alias("credit_id"),
        F.expr(f"try_cast(`{credit_cols['VAL_SALDO_REMANESCENTE']}` as decimal(38,10))")
        .alias("balance"),
        F.expr(f"try_cast(`{credit_cols['VAL_CONTRATADO']}` as decimal(38,10))")
        .alias("contracted"),
        F.expr(f"try_cast(`{credit_cols['DAT_CONTRATACAO']}` as date)").alias("contract_date"),
        F.expr(f"try_cast(`{credit_cols['DAT_VENCIMENTO']}` as date)").alias("maturity"),
    )
    bad_financial = financial.where(
        (F.col("balance").isNotNull() & F.col("contracted").isNotNull()
         & (F.col("balance") > F.col("contracted")))
        | (F.col("contract_date").isNotNull() & F.col("maturity").isNotNull()
           & (F.col("contract_date") > F.col("maturity")))
    )
    count = bad_financial.count()
    out.append(Finding(
        "8d.profile.financial_plausibility", "Credito SCR observed insertion profile",
        SEV_WARN if count else SEV_INFO, CREDITO_SCR_TABLE, count == 0, count=count,
        column="VAL_SALDO_REMANESCENTE,VAL_CONTRATADO,DAT_CONTRATACAO,DAT_VENCIMENTO",
        sample=_sample_keys(bad_financial, ["credit_id"], sample),
        hint="Review this plausible relationship; the insertion log contains no counterexample."
             if count else "",
        message="Rows outside financial/date relationships observed in one successful batch.",
    ))

    detail_pairs = (
        ("NUM_TIPO_IF", "NUM_TIPO_IF"), ("NUM_IF", "NUM_IF_CREDITO"),
        ("VAL_SALDO_REMANESCENTE", "VAL_SALDO_REMANESCENTE"),
        ("DAT_SALDO_REMANESCENTE", "DAT_SALDO_REMANESCENTE"),
        ("COD_CONTRATO_SCR", "COD_CONTRATO_SCR"), ("QTD_CREDITO", "QTD_CREDITO"),
        ("NUM_ID_TIPO_CREDITO", "NUM_ID_TIPO_CREDITO"),
        ("VAL_CONTRATADO", "VAL_CONTRATADO"), ("DAT_CONTRATACAO", "DAT_CONTRATACAO"),
        ("DAT_VENCIMENTO", "DAT_VENCIMENTO"),
        ("NUM_ID_MODALIDADE_CREDITO", "NUM_ID_MODALIDADE_CREDITO"),
        ("NUM_ID_INDEXADOR_CREDITO", "NUM_ID_INDEXADOR_CREDITO"),
        ("COD_TIPO_PESSOA", "COD_TIPO_PESSOA"),
        ("COD_REFERENCIA_EXTERNA_DEVEDOR", "COD_REFERENCIA_EXTERNA_DEVEDOR"),
        ("VAL_PERCENTUAL_INDEXADOR", "VAL_PERCENTUAL_INDEXADOR"),
        ("VAL_PERCENTUAL_TAXA_ANUAL", "VAL_PERCENTUAL_TAXA_ANUAL"),
        ("COD_IPOC", "COD_IPOC"), ("IND_MULTIPLO_IPOC", "IND_MULTIPLO_IPOC"),
    )
    current = credits.select(
        _canon_key_col(F.col(credit_cols["NUM_ID_CREDITO_SCR"])).alias("credit_id"),
        *[F.col(credit_cols[name]).alias(f"credit_{name}") for name, _ in detail_pairs],
    )
    snapshots = inclusion.select(
        _canon_key_col(F.col(history_cols["NUM_ID_CREDITO_SCR"])).alias("history_credit_id"),
        F.col(history_cols["NUM_ID_HISTORICO_CREDITO_SCR"]).alias("history_id"),
        *[F.col(history_cols[history_name]).alias(f"history_{history_name}")
          for _, history_name in detail_pairs],
    )
    compared = current.join(snapshots, current.credit_id == snapshots.history_credit_id, "inner")
    detail_mismatch = reduce(
        lambda left, right: left | right,
        [~F.col(f"credit_{credit_name}").eqNullSafe(F.col(f"history_{history_name}"))
         for credit_name, history_name in detail_pairs],
    )
    bad_details = compared.where(detail_mismatch)
    count = bad_details.count()
    out.append(Finding(
        "8d.profile.history_details", "Credito SCR observed insertion profile",
        SEV_WARN if count else SEV_INFO, HISTORICO_CREDITO_SCR_TABLE, count == 0, count=count,
        column=",".join(name for name, _ in detail_pairs),
        sample=_sample_keys(bad_details, ["credit_id", "history_id"], sample),
        hint="Confirm whether differences are legitimate post-inclusion lifecycle updates."
             if count else "",
        message="Inclusion details differing from current credit values.",
    ))
    return out


# ---------------------------------------------------------------------------
# Category 2f/6f/8f - DICRE graph and registration-route evidence
# ---------------------------------------------------------------------------
CREDITO_DC_TABLE = "CREDITO_DC"
HISTORICO_CREDITO_DC_TABLE = "HISTORICO_CREDITO_DC"
TCTPCHAV_IROP_ATIV_TABLE = "TCTPCHAV_IROP_ATIV"
TCTPDET_CHAV_IROP_CCB_TABLE = "TCTPDET_CHAV_IROP_CCB"
TCTPDET_CHAV_IROP_CMER_TABLE = "TCTPDET_CHAV_IROP_CMER"
TCTPIROP_ATIV_TABLE = "TCTPIROP_ATIV"
TCTPSOLI_IROP_ATIV_TABLE = "TCTPSOLI_IROP_ATIV"
DICRE_GRAPH_TABLES = (
    LOTE_TABLE,
    CREDITO_DC_TABLE,
    HISTORICO_CREDITO_DC_TABLE,
    TCTPCHAV_IROP_ATIV_TABLE,
    TCTPDET_CHAV_IROP_CCB_TABLE,
    TCTPDET_CHAV_IROP_CMER_TABLE,
    TCTPIROP_ATIV_TABLE,
    TCTPSOLI_IROP_ATIV_TABLE,
)
DICRE_IPOC_TOGGLE = "VALIDADOR_UNICIDADE_IPOC_LCA"


def _dicre_unavailable(check_id: str, missing: List[str], severity: str) -> Finding:
    return Finding(
        check_id, "DICRE", severity,
        ",".join(sorted({value.split(".")[0] for value in missing})), False,
        hint="Export the complete DICRE graph and make the bounded target lookup available.",
        message=f"Check unavailable; missing required input: {', '.join(missing)}.",
    )


def _dicre_text(column):
    """Exact textual business-key semantics: trim only, preserving case and '.0'."""
    return F.trim(column.cast("string"))


def check_dicre_identity(
    tables: Dict[str, DataFrame], profile: ValidationProfile, sample: int
) -> List[Finding]:
    if profile.pipeline != "dicre":
        return []
    root = tables.get(CREDITO_DC_TABLE)
    if root is None:
        return [_dicre_unavailable("0.identity.root", [CREDITO_DC_TABLE], SEV_ERROR)]
    key = resolve(root, "NUM_ID_CREDITO_DC")
    if not key:
        return [_dicre_unavailable(
            "0.identity.root", [f"{CREDITO_DC_TABLE}.NUM_ID_CREDITO_DC"], SEV_ERROR
        )]
    count = root.count()
    return [Finding(
        "0.identity.root", "Product identity", SEV_INFO if count else SEV_ERROR,
        CREDITO_DC_TABLE, count > 0, count=count, column="NUM_ID_CREDITO_DC",
        sample=_sample_keys(root, [key], sample) if count else [],
        hint="Export at least one CREDITO_DC row." if not count else "",
        message="All exported CREDITO_DC rows define the DICRE semantic universe; "
                "DAT_EXCLUSAO does not filter the root.",
    )]


def check_dicre_metadata(
    meta: Metadata, no_oracle: bool, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "dicre":
        return []
    if no_oracle:
        return [Finding(
            "0.dicre_metadata", "Coverage", SEV_INFO, "Oracle metadata", True,
            message="Authoritative DICRE metadata deferred under --no-oracle; local checks "
                    "still run and the verdict remains PARTIAL.",
        )]
    missing = [table for table in DICRE_GRAPH_TABLES if table not in meta.tables]
    missing_pk = [
        table for table in DICRE_GRAPH_TABLES if table in meta.tables and not meta.pk.get(table)
    ]
    failed = bool(missing or missing_pk)
    return [Finding(
        "0.dicre_metadata", "Coverage", SEV_ERROR if failed else SEV_INFO,
        ",".join(DICRE_GRAPH_TABLES), not failed, count=len(missing) + len(missing_pk),
        hint="Read table and PK metadata for all eight DICRE graph tables from Oracle."
             if failed else "",
        message=(f"Missing Oracle table metadata={missing}; missing PK metadata={missing_pk}."
                 if failed else
                 "Authoritative Oracle table and PK metadata cover the complete DICRE graph."),
    )]


def check_dicre_graph(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "dicre":
        return []
    requirements = {
        LOTE_TABLE: (
            "NUM_ID_LOTE", "NOME_LOTE", "NUM_CONTA_PARTICIPANTE",
            "NUM_ID_TIPO_LOTE", "NUM_TIPO_IF", "DAT_EXCLUSAO",
        ),
        CREDITO_DC_TABLE: (
            "NUM_ID_CREDITO_DC", "COD_CREDITO_DC", "NUM_ID_LOTE", "DAT_EXCLUSAO",
        ),
        HISTORICO_CREDITO_DC_TABLE: (
            "NUM_ID_HISTORICO_CREDITO_DC", "COD_CREDITO_DC", "NUM_ID_LOTE",
            "NUM_ID_TIPO_ACAO_HIST_CREDITO",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_dicre_unavailable("2f.graph.availability", missing, SEV_ERROR)]

    lot_cols = columns[LOTE_TABLE]
    root_cols = columns[CREDITO_DC_TABLE]
    history_cols = columns[HISTORICO_CREDITO_DC_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE]).select(
        _canon_key_col(F.col(lot_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _dicre_text(F.col(lot_cols["NOME_LOTE"])).alias("lot_name"),
        _dicre_text(F.col(lot_cols["NUM_CONTA_PARTICIPANTE"])).alias("lot_account_exact"),
        _dicre_text(F.col(lot_cols["NUM_ID_TIPO_LOTE"])).alias("lot_type_exact"),
        _dicre_text(F.col(lot_cols["NUM_TIPO_IF"])).alias("lot_if_type_exact"),
    )
    roots = tables[CREDITO_DC_TABLE].select(
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        _dicre_text(F.col(root_cols["COD_CREDITO_DC"])).alias("credit_code"),
        _canon_key_col(F.col(root_cols["NUM_ID_LOTE"])).alias("credit_lot_id"),
    )
    histories = tables[HISTORICO_CREDITO_DC_TABLE].select(
        _canon_key_col(F.col(history_cols["NUM_ID_HISTORICO_CREDITO_DC"])).alias("history_id"),
        _dicre_text(F.col(history_cols["COD_CREDITO_DC"])).alias("history_code"),
        _canon_key_col(F.col(history_cols["NUM_ID_LOTE"])).alias("history_lot_id"),
        _canon_key_col(F.col(history_cols["NUM_ID_TIPO_ACAO_HIST_CREDITO"])).alias("action"),
    )
    inclusions = histories.where(F.col("action") == "1")
    out: List[Finding] = []

    duplicate_lots = lots.groupBy(
        "lot_name", "lot_account_exact", "lot_type_exact", "lot_if_type_exact"
    ).count().where(F.col("count") > 1)
    count = duplicate_lots.count()
    out.append(Finding(
        "2f.active_lot_natural_key", "DICRE graph", SEV_ERROR if count else SEV_INFO,
        LOTE_TABLE, count == 0, count=count,
        column="NOME_LOTE,NUM_CONTA_PARTICIPANTE,NUM_ID_TIPO_LOTE,NUM_TIPO_IF",
        sample=_sample_keys(
            duplicate_lots,
            ["lot_name", "lot_account_exact", "lot_type_exact", "lot_if_type_exact"], sample,
        ),
        hint="Keep at most one active lot for the exact-trimmed four-column key."
             if count else "",
        message="Duplicate active DICRE lot business keys.",
    ))

    lot_counts = lots.groupBy("lot_id").count().withColumnRenamed("count", "lot_count")
    bad_lots = roots.join(
        lot_counts, roots.credit_lot_id == lot_counts.lot_id, "left"
    ).where(F.coalesce(F.col("lot_count"), F.lit(0)) != 1)
    count = bad_lots.count()
    out.append(Finding(
        "2f.credit_active_lot", "DICRE graph", SEV_ERROR if count else SEV_INFO,
        CREDITO_DC_TABLE, count == 0, count=count, column="NUM_ID_LOTE",
        sample=_sample_keys(bad_lots, ["credit_id", "credit_lot_id"], sample),
        hint="Point every exported CREDITO_DC row to exactly one active LOTE."
             if count else "",
        message="DICRE roots without exactly one active lot.",
    ))

    coded = roots.withColumn(
        "code_count", F.count(F.lit(1)).over(Window.partitionBy("credit_code"))
    )
    bad_codes = coded.where(
        F.col("credit_code").isNull() | (F.col("credit_code") == "")
        | (F.col("code_count") > 1)
    )
    count = bad_codes.count()
    out.append(Finding(
        "2f.credit_code_unique", "DICRE graph", SEV_ERROR if count else SEV_INFO,
        CREDITO_DC_TABLE, count == 0, count=count, column="COD_CREDITO_DC",
        sample=_sample_keys(bad_codes, ["credit_id", "credit_code"], sample),
        hint="Generate nonblank, unique exact-trimmed COD_CREDITO_DC values."
             if count else "",
        message="DICRE roots with blank or duplicate business codes.",
    ))

    inclusion_counts = inclusions.groupBy(
        "history_code", "history_lot_id"
    ).count().withColumnRenamed("count", "inclusion_count")
    bad_history = roots.join(
        inclusion_counts,
        (roots.credit_code == inclusion_counts.history_code)
        & (roots.credit_lot_id == inclusion_counts.history_lot_id),
        "left",
    ).where(F.coalesce(F.col("inclusion_count"), F.lit(0)) != 1)
    count = bad_history.count()
    out.append(Finding(
        "2f.inclusion_history", "DICRE graph", SEV_ERROR if count else SEV_INFO,
        HISTORICO_CREDITO_DC_TABLE, count == 0, count=count,
        column="COD_CREDITO_DC,NUM_ID_LOTE,NUM_ID_TIPO_ACAO_HIST_CREDITO",
        sample=_sample_keys(bad_history, ["credit_id", "credit_code", "credit_lot_id"], sample),
        hint="Keep exactly one action 1 history per exact code + canonical lot root identity."
             if count else "",
        message="DICRE roots without exactly one inclusion history action.",
    ))

    root_identity = roots.select(
        F.col("credit_code").alias("history_code"),
        F.col("credit_lot_id").alias("history_lot_id"),
    ).dropDuplicates()
    orphan_history = inclusions.join(
        root_identity, ["history_code", "history_lot_id"], "left_anti"
    )
    count = orphan_history.count()
    out.append(Finding(
        "2f.inclusion_history_orphan", "DICRE graph",
        SEV_ERROR if count else SEV_INFO, HISTORICO_CREDITO_DC_TABLE,
        count == 0, count=count, column="COD_CREDITO_DC,NUM_ID_LOTE",
        sample=_sample_keys(
            orphan_history, ["history_id", "history_code", "history_lot_id"], sample
        ),
        hint="Link action 1 history by exact code and canonical lot only." if count else "",
        message="Inclusion histories not linked to a DICRE root identity.",
    ))
    return out


def _dicre_edge_finding(
    check_id: str, child: DataFrame, parent: DataFrame, join_columns: List[str],
    table: str, column: str, sample_columns: List[str], sample: int,
) -> Finding:
    bad = child.join(parent.dropDuplicates(join_columns), join_columns, "left_anti")
    count = bad.count()
    return Finding(
        check_id, "DICRE IROP graph", SEV_ERROR if count else SEV_INFO,
        table, count == 0, count=count, column=column,
        sample=_sample_keys(bad, sample_columns, sample),
        hint="Remove the orphan edge or export its referenced DICRE graph row."
             if count else "",
        message="Orphan DICRE IROP edge.",
    )


def _dicre_duplicate_edge_finding(
    check_id: str, frame: DataFrame, edge_columns: List[str], table: str,
    column: str, sample: int,
) -> Finding:
    duplicate = frame.groupBy(*edge_columns).count().where(F.col("count") > 1)
    count = duplicate.count()
    return Finding(
        check_id, "DICRE IROP graph", SEV_ERROR if count else SEV_INFO,
        table, count == 0, count=count, column=column,
        sample=_sample_keys(duplicate, edge_columns, sample),
        hint="Keep each exported IROP business edge unambiguous." if count else "",
        message="Duplicate DICRE IROP business edges.",
    )


def check_dicre_irop_graph(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "dicre":
        return []
    requirements = {
        CREDITO_DC_TABLE: ("NUM_ID_CREDITO_DC",),
        TCTPCHAV_IROP_ATIV_TABLE: ("NUM_CHAV_IROP",),
        TCTPDET_CHAV_IROP_CCB_TABLE: ("NUM_CHAV_IROP",),
        TCTPDET_CHAV_IROP_CMER_TABLE: ("NUM_CHAV_IROP",),
        TCTPIROP_ATIV_TABLE: (
            "NUM_IROP_ATIV", "NUM_IDT_CRE_DC", "NUM_CHAV_IROP",
        ),
        TCTPSOLI_IROP_ATIV_TABLE: ("NUM_IROP_ATIV",),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_dicre_unavailable("2f.irop.availability", missing, SEV_ERROR)]

    roots = tables[CREDITO_DC_TABLE].select(
        _canon_key_col(F.col(columns[CREDITO_DC_TABLE]["NUM_ID_CREDITO_DC"]))
        .alias("credit_id")
    )
    keys = tables[TCTPCHAV_IROP_ATIV_TABLE].select(
        _canon_key_col(F.col(columns[TCTPCHAV_IROP_ATIV_TABLE]["NUM_CHAV_IROP"]))
        .alias("irop_key")
    )
    irops = tables[TCTPIROP_ATIV_TABLE].select(
        _canon_key_col(F.col(columns[TCTPIROP_ATIV_TABLE]["NUM_IROP_ATIV"]))
        .alias("irop_id"),
        _canon_key_col(F.col(columns[TCTPIROP_ATIV_TABLE]["NUM_IDT_CRE_DC"]))
        .alias("credit_id"),
        _canon_key_col(F.col(columns[TCTPIROP_ATIV_TABLE]["NUM_CHAV_IROP"]))
        .alias("irop_key"),
    )
    ccb = tables[TCTPDET_CHAV_IROP_CCB_TABLE].select(
        _canon_key_col(F.col(columns[TCTPDET_CHAV_IROP_CCB_TABLE]["NUM_CHAV_IROP"]))
        .alias("irop_key")
    )
    cmer = tables[TCTPDET_CHAV_IROP_CMER_TABLE].select(
        _canon_key_col(F.col(columns[TCTPDET_CHAV_IROP_CMER_TABLE]["NUM_CHAV_IROP"]))
        .alias("irop_key")
    )
    requests = tables[TCTPSOLI_IROP_ATIV_TABLE].select(
        _canon_key_col(F.col(columns[TCTPSOLI_IROP_ATIV_TABLE]["NUM_IROP_ATIV"]))
        .alias("irop_id")
    )
    out = [
        _dicre_edge_finding(
            "2f.irop.credit_edge", irops, roots, ["credit_id"], TCTPIROP_ATIV_TABLE,
            "NUM_IDT_CRE_DC", ["irop_id", "credit_id"], sample,
        ),
        _dicre_edge_finding(
            "2f.irop.key_edge", irops, keys, ["irop_key"], TCTPIROP_ATIV_TABLE,
            "NUM_CHAV_IROP", ["irop_id", "irop_key"], sample,
        ),
        _dicre_edge_finding(
            "2f.irop.ccb_key_edge", ccb, keys, ["irop_key"],
            TCTPDET_CHAV_IROP_CCB_TABLE, "NUM_CHAV_IROP", ["irop_key"], sample,
        ),
        _dicre_edge_finding(
            "2f.irop.cmer_key_edge", cmer, keys, ["irop_key"],
            TCTPDET_CHAV_IROP_CMER_TABLE, "NUM_CHAV_IROP", ["irop_key"], sample,
        ),
        _dicre_edge_finding(
            "2f.irop.request_edge", requests, irops.select("irop_id"), ["irop_id"],
            TCTPSOLI_IROP_ATIV_TABLE, "NUM_IROP_ATIV", ["irop_id"], sample,
        ),
        _dicre_duplicate_edge_finding(
            "2f.irop.credit_key_unique", irops, ["credit_id", "irop_key"],
            TCTPIROP_ATIV_TABLE, "NUM_IDT_CRE_DC,NUM_CHAV_IROP", sample,
        ),
        _dicre_duplicate_edge_finding(
            "2f.irop.ccb_key_unique", ccb, ["irop_key"],
            TCTPDET_CHAV_IROP_CCB_TABLE, "NUM_CHAV_IROP", sample,
        ),
        _dicre_duplicate_edge_finding(
            "2f.irop.cmer_key_unique", cmer, ["irop_key"],
            TCTPDET_CHAV_IROP_CMER_TABLE, "NUM_CHAV_IROP", sample,
        ),
        _dicre_duplicate_edge_finding(
            "2f.irop.request_unique", requests, ["irop_id"],
            TCTPSOLI_IROP_ATIV_TABLE, "NUM_IROP_ATIV", sample,
        ),
    ]
    return out


def _dicre_lookup_columns(
    frame: Optional[DataFrame], names: Tuple[str, ...]
) -> Tuple[Dict[str, str], List[str]]:
    if frame is None:
        return {}, list(names)
    columns = {name: resolve(frame, name) for name in names}
    return columns, [name for name, actual in columns.items() if not actual]


def _dicre_enabled_credits(roots: DataFrame, toggle: DataFrame) -> Optional[DataFrame]:
    start = resolve(toggle, "DATA_INIC_VIG_FTRE")
    end = resolve(toggle, "DATA_FIM_VIG_FTRE")
    enabled = resolve(toggle, "IND_FTRE_HAB")
    if not start or not end or not enabled:
        return None
    periods = toggle
    code = resolve(toggle, "COD_FTRE_TOG")
    if code:
        periods = periods.where(_dicre_text(F.col(code)) == DICRE_IPOC_TOGGLE)
    periods = periods.where(_dicre_text(F.col(enabled)) == "S").select(
        F.to_date(F.col(start)).alias("toggle_start"),
        F.to_date(F.col(end)).alias("toggle_end"),
    )
    return roots.join(
        F.broadcast(periods),
        (F.col("inclusion_date") >= F.col("toggle_start"))
        & (F.col("inclusion_date") <= F.col("toggle_end")),
        "left_semi",
    )


def check_dicre_target_frames(
    tables: Dict[str, DataFrame], lookup_frames: Dict[str, DataFrame], sample: int,
    profile: ValidationProfile, lookup_errors: Optional[Dict[str, str]] = None,
) -> List[Finding]:
    if profile.pipeline != "dicre":
        return []
    lookup_errors = lookup_errors or {}
    requirements = {
        LOTE_TABLE: (
            "NUM_ID_LOTE", "NUM_CONTA_PARTICIPANTE", "NUM_ID_TIPO_LOTE",
            "NUM_TIPO_IF", "DAT_EXCLUSAO",
        ),
        CREDITO_DC_TABLE: (
            "NUM_ID_CREDITO_DC", "NUM_ID_LOTE", "NUM_TIPO_IF",
            "NUM_CONTA_CUSTODIANTE", "NUM_ID_BASE_CREDITO", "DAT_INCLUSAO", "COD_IPOC",
            "NUM_ID_QUALIF_FINALIDADE", "NUM_ID_QUALIF_JUROS_A_CADA",
            "NUM_ID_QUALIF_AMORT_A_CADA", "NUM_ID_QUALIF_GARANTIA_ESPEC",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_dicre_unavailable("6f.lookup.availability", missing, SEV_ERROR)]
    lot_cols, root_cols = columns[LOTE_TABLE], columns[CREDITO_DC_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE]).select(
        _canon_key_col(F.col(lot_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _canon_key_col(F.col(lot_cols["NUM_CONTA_PARTICIPANTE"])).alias("emitter_account"),
        _canon_key_col(F.col(lot_cols["NUM_ID_TIPO_LOTE"])).alias("lot_type"),
        _canon_key_col(F.col(lot_cols["NUM_TIPO_IF"])).alias("guaranteed_if_type"),
    )
    roots = tables[CREDITO_DC_TABLE].select(
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        _canon_key_col(F.col(root_cols["NUM_ID_LOTE"])).alias("credit_lot_id"),
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])).alias("credit_if_type"),
        _canon_key_col(F.col(root_cols["NUM_CONTA_CUSTODIANTE"])).alias("custodian_account"),
        _canon_key_col(F.col(root_cols["NUM_ID_BASE_CREDITO"])).alias("base_id"),
        F.to_date(F.col(root_cols["DAT_INCLUSAO"])).alias("inclusion_date"),
        _dicre_text(F.col(root_cols["COD_IPOC"])).alias("ipoc"),
        *[
            _canon_key_col(F.col(root_cols[column])).alias(alias)
            for column, alias in (
                ("NUM_ID_QUALIF_FINALIDADE", "qualification_20"),
                ("NUM_ID_QUALIF_JUROS_A_CADA", "qualification_21"),
                ("NUM_ID_QUALIF_AMORT_A_CADA", "qualification_22"),
                ("NUM_ID_QUALIF_GARANTIA_ESPEC", "qualification_23"),
            )
        ],
    ).join(lots, F.col("credit_lot_id") == lots.lot_id, "left")
    out: List[Finding] = []

    account_frame = lookup_frames.get("DICRE_ACCOUNTS")
    account_names = (
        "NUM_CONTA_PARTICIPANTE", "NUM_ID_SITUACAO_CONTA", "COD_TIPO_ACESSO",
        "NUM_ID_AREA_ATUACAO", "NOM_SIMPLIFICADO",
    )
    account_cols, account_missing = _dicre_lookup_columns(account_frame, account_names)
    if account_missing:
        out.append(_dicre_unavailable(
            "6f.lookup.accounts",
            [lookup_errors.get("DICRE_ACCOUNTS", "DICRE_ACCOUNTS")]
            if account_frame is None else
            [f"DICRE_ACCOUNTS.{name}" for name in account_missing],
            SEV_WARN,
        ))
    else:
        accounts = account_frame.select(
            _canon_key_col(F.col(account_cols["NUM_CONTA_PARTICIPANTE"])).alias("account_id"),
            _canon_key_col(F.col(account_cols["NUM_ID_SITUACAO_CONTA"])).alias("status"),
            _dicre_text(F.col(account_cols["COD_TIPO_ACESSO"])).alias("access"),
            _canon_key_col(F.col(account_cols["NUM_ID_AREA_ATUACAO"])).alias("area"),
            _dicre_text(F.col(account_cols["NOM_SIMPLIFICADO"])).alias("short_name"),
        )
        emitters = accounts.where(
            F.col("status").isin("1", "2")
            & (F.col("access") == "L") & (F.col("area") == "1")
        ).select(
            F.col("account_id").alias("emitter_account"),
            F.col("short_name").alias("emitter_name"),
        )
        custodians = accounts.where(F.col("status").isin("1", "2")).select(
            F.col("account_id").alias("custodian_account"),
            F.col("short_name").alias("custodian_name"),
        )
        eligible_pairs = emitters.join(
            custodians, F.col("emitter_name") == F.col("custodian_name"), "inner"
        ).select("emitter_account", "custodian_account").dropDuplicates()
        bad = roots.join(
            F.broadcast(eligible_pairs), ["emitter_account", "custodian_account"], "left_anti"
        )
        count = bad.count()
        out.append(Finding(
            "6f.lookup.accounts", "DICRE target eligibility",
            SEV_ERROR if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
            column="NUM_CONTA_PARTICIPANTE,NUM_CONTA_CUSTODIANTE",
            sample=_sample_keys(
                bad, ["credit_id", "emitter_account", "custodian_account"], sample
            ),
            hint="Use active emitter/custodian accounts for the same participant; emitter "
                 "must have local access L in area 1." if count else "",
            message="DICRE rows with ineligible emitter/custodian account pairs.",
        ))

    base_frame = lookup_frames.get("DICRE_BASES")
    base_cols, base_missing = _dicre_lookup_columns(
        base_frame, ("NUM_CONTA_PARTICIPANTE", "NUM_ID_BASE_CREDITO")
    )
    if base_missing:
        out.append(_dicre_unavailable(
            "6f.lookup.base",
            [lookup_errors.get("DICRE_BASES", "DICRE_BASES")]
            if base_frame is None else [f"DICRE_BASES.{name}" for name in base_missing],
            SEV_WARN,
        ))
    else:
        bases = base_frame.select(
            _canon_key_col(F.col(base_cols["NUM_CONTA_PARTICIPANTE"])).alias("emitter_account"),
            _canon_key_col(F.col(base_cols["NUM_ID_BASE_CREDITO"])).alias("base_id"),
        ).dropDuplicates()
        bad = roots.join(F.broadcast(bases), ["emitter_account", "base_id"], "left_anti")
        count = bad.count()
        out.append(Finding(
            "6f.lookup.base", "DICRE target eligibility", SEV_ERROR if count else SEV_INFO,
            CREDITO_DC_TABLE, count == 0, count=count,
            column="NUM_CONTA_PARTICIPANTE,NUM_ID_BASE_CREDITO",
            sample=_sample_keys(bad, ["credit_id", "emitter_account", "base_id"], sample),
            hint="Use a base authorized for the emitter through exact type-base code 2."
                 if count else "",
            message="DICRE rows without an eligible type-2 credit base.",
        ))

    if_frame = lookup_frames.get("DICRE_IF_COMPATIBILITY")
    if_cols, if_missing = _dicre_lookup_columns(
        if_frame, ("NUM_TIPO_IF", "NUM_TIPO_IF_GARANTIDO", "NUM_ID_TIPO_LOTE")
    )
    if if_missing:
        out.append(_dicre_unavailable(
            "6f.lookup.if_compatibility",
            [lookup_errors.get("DICRE_IF_COMPATIBILITY", "DICRE_IF_COMPATIBILITY")]
            if if_frame is None else
            [f"DICRE_IF_COMPATIBILITY.{name}" for name in if_missing],
            SEV_WARN,
        ))
    else:
        compatible = if_frame.select(
            _canon_key_col(F.col(if_cols["NUM_TIPO_IF"])).alias("credit_if_type"),
            _canon_key_col(F.col(if_cols["NUM_TIPO_IF_GARANTIDO"])).alias("guaranteed_if_type"),
            _canon_key_col(F.col(if_cols["NUM_ID_TIPO_LOTE"])).alias("lot_type"),
        ).dropDuplicates()
        bad = roots.join(
            F.broadcast(compatible),
            ["credit_if_type", "guaranteed_if_type", "lot_type"], "left_anti",
        )
        count = bad.count()
        out.append(Finding(
            "6f.lookup.if_compatibility", "DICRE target eligibility",
            SEV_ERROR if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
            column="NUM_TIPO_IF,LOTE.NUM_TIPO_IF,LOTE.NUM_ID_TIPO_LOTE",
            sample=_sample_keys(
                bad, ["credit_id", "credit_if_type", "guaranteed_if_type", "lot_type"], sample
            ),
            hint="Use an active subtype enabled for lot type 2 and guaranteed LCA."
                 if count else "",
            message="DICRE subtype/guaranteed-LCA incompatibilities.",
        ))

    qualification_frame = lookup_frames.get("DICRE_QUALIFICATIONS")
    qualification_cols, qualification_missing = _dicre_lookup_columns(
        qualification_frame,
        ("NUM_ID_QUALIFICACAO", "NUM_ID_QUALIFICACAO_SUBGRUPO"),
    )
    if qualification_missing:
        out.append(_dicre_unavailable(
            "6f.lookup.qualifications",
            [lookup_errors.get("DICRE_QUALIFICATIONS", "DICRE_QUALIFICATIONS")]
            if qualification_frame is None else
            [f"DICRE_QUALIFICATIONS.{name}" for name in qualification_missing],
            SEV_WARN,
        ))
    else:
        required_qualifications = reduce(
            lambda left, right: left.unionByName(right),
            [
                roots.where(F.col(f"qualification_{subgroup}").isNotNull()).select(
                    "credit_id", "guaranteed_if_type",
                    F.col(f"qualification_{subgroup}").alias("qualification_id"),
                    F.lit(subgroup).alias("subgroup"),
                )
                for subgroup in ("20", "21", "22", "23")
            ],
        )
        eligible_qualifications = qualification_frame.select(
            _canon_key_col(F.col(qualification_cols["NUM_ID_QUALIFICACAO"]))
            .alias("qualification_id"),
            _canon_key_col(F.col(qualification_cols["NUM_ID_QUALIFICACAO_SUBGRUPO"]))
            .alias("subgroup"),
        ).dropDuplicates()
        bad = required_qualifications.join(
            F.broadcast(eligible_qualifications), ["qualification_id", "subgroup"], "left_anti"
        )
        count = bad.count()
        out.append(Finding(
            "6f.lookup.qualifications", "DICRE target eligibility",
            SEV_ERROR if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
            column="NUM_ID_QUALIF_*",
            sample=_sample_keys(bad, ["credit_id", "qualification_id", "subgroup"], sample),
            hint="Resolve each nonnull qualification through an enabled group-4 LCA "
                 "relationship for subgroup 20, 21, 22, or 23." if count else "",
            message="DICRE rows with ineligible nonnull qualifications.",
        ))

    toggle = lookup_frames.get("TCTPFEATURE_TOGGLE")
    if toggle is None:
        out.append(_dicre_unavailable(
            "6f.lookup.ipoc_unique",
            [lookup_errors.get("TCTPFEATURE_TOGGLE", "TCTPFEATURE_TOGGLE")], SEV_WARN,
        ))
    else:
        enabled_credits = _dicre_enabled_credits(roots, toggle)
        if enabled_credits is None:
            out.append(_dicre_unavailable(
                "6f.lookup.ipoc_unique", ["TCTPFEATURE_TOGGLE required columns"], SEV_WARN
            ))
        elif enabled_credits.limit(1).count() == 0:
            out.append(Finding(
                "6f.lookup.ipoc_unique", "DICRE target eligibility", SEV_INFO,
                CREDITO_DC_TABLE, True, column="COD_IPOC",
                message="VALIDADOR_UNICIDADE_IPOC_LCA is disabled for all synthetic "
                        "CREDITO_DC.DAT_INCLUSAO dates; target IPOCs are not required.",
            ))
        else:
            target = lookup_frames.get("CREDITO_DC_TARGET")
            target_column = resolve(target, "COD_IPOC") if target is not None else None
            if target is None or not target_column:
                out.append(_dicre_unavailable(
                    "6f.lookup.ipoc_unique",
                    [lookup_errors.get("CREDITO_DC_TARGET", "CREDITO_DC_TARGET.COD_IPOC")],
                    SEV_WARN,
                ))
            else:
                duplicate_synthetic = roots.where(
                    F.col("ipoc").isNotNull() & (F.col("ipoc") != "")
                ).groupBy("ipoc").count().where(F.col("count") > 1).select("ipoc")
                active_target = target
                target_exclusion = resolve(target, "DAT_EXCLUSAO")
                if target_exclusion:
                    active_target = target.where(F.col(target_exclusion).isNull())
                target_ipocs = active_target.select(
                    _dicre_text(F.col(target_column)).alias("ipoc")
                ).where(F.col("ipoc").isNotNull() & (F.col("ipoc") != "")).dropDuplicates()
                conflicts = duplicate_synthetic.unionByName(target_ipocs).dropDuplicates()
                bad = enabled_credits.join(conflicts, "ipoc", "inner")
                count = bad.count()
                out.append(Finding(
                    "6f.lookup.ipoc_unique", "DICRE target eligibility",
                    SEV_ERROR if count else SEV_INFO, CREDITO_DC_TABLE,
                    count == 0, count=count, column="COD_IPOC",
                    sample=_sample_keys(bad, ["credit_id", "ipoc"], sample),
                    hint="Regenerate exact-trimmed, case-sensitive IPOCs colliding within the "
                         "enabled synthetic rows or active target CREDITO_DC." if count else "",
                    message="Toggle-enabled DICRE rows with duplicate IPOCs.",
                ))
    return out


def _dicre_collect_values(
    frame: DataFrame, expression, alias: str, maximum: int = 100_000
) -> Tuple[List[str], Optional[str]]:
    rows = frame.select(expression.alias(alias)).where(
        F.col(alias).isNotNull()
    ).dropDuplicates().limit(maximum + 1).collect()
    if len(rows) > maximum:
        return [], f"more than {maximum} distinct synthetic keys"
    return [str(row[alias]) for row in rows], None


def load_dicre_target_frames(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame]
) -> Tuple[Dict[str, DataFrame], Dict[str, str]]:
    """Load only target rows addressed by distinct synthetic DICRE keys and pairs."""
    frames: Dict[str, DataFrame] = {}
    errors: Dict[str, str] = {}

    def run(name: str, query: str) -> None:
        try:
            remote = _jdbc(spark, cfg, query)
            rows = remote.collect()
            frames[name] = spark.createDataFrame(rows, remote.schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning("DICRE target lookup failed for %s: %s", name, exc)
            errors[name] = str(exc)

    def run_many(name: str, queries: List[str]) -> None:
        rows = []
        schema = None
        try:
            for query in queries:
                remote = _jdbc(spark, cfg, query)
                schema = schema or remote.schema
                rows.extend(remote.collect())
            if schema is not None:
                frames[name] = spark.createDataFrame(rows, schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning("DICRE target lookup failed for %s: %s", name, exc)
            errors[name] = str(exc)

    lot = tables.get(LOTE_TABLE)
    root = tables.get(CREDITO_DC_TABLE)
    if lot is None or root is None:
        return frames, {"DICRE": "LOTE/CREDITO_DC unavailable"}
    lot_id = resolve(lot, "NUM_ID_LOTE")
    lot_account = resolve(lot, "NUM_CONTA_PARTICIPANTE")
    lot_type = resolve(lot, "NUM_ID_TIPO_LOTE")
    guaranteed_type = resolve(lot, "NUM_TIPO_IF")
    root_lot = resolve(root, "NUM_ID_LOTE")
    root_type = resolve(root, "NUM_TIPO_IF")
    custodian = resolve(root, "NUM_CONTA_CUSTODIANTE")
    base = resolve(root, "NUM_ID_BASE_CREDITO")
    joined = None
    if all((lot_id, lot_account, lot_type, guaranteed_type, root_lot)):
        joined = root.select(
            _canon_key_col(F.col(root_lot)).alias("lot_id"),
            *([
                _canon_key_col(F.col(root_type)).alias("credit_if_type")
            ] if root_type else []),
            *([_canon_key_col(F.col(custodian)).alias("custodian")] if custodian else []),
            *([_canon_key_col(F.col(base)).alias("base_id")] if base else []),
        ).join(
            _credito_scr_active(lot).select(
                _canon_key_col(F.col(lot_id)).alias("lot_id"),
                _canon_key_col(F.col(lot_account)).alias("emitter"),
                _canon_key_col(F.col(lot_type)).alias("lot_type"),
                _canon_key_col(F.col(guaranteed_type)).alias("guaranteed_if_type"),
            ), "lot_id", "inner",
        )

    if joined is None or not custodian:
        errors["DICRE_ACCOUNTS"] = "synthetic account keys unavailable"
    else:
        emitter_values, emitter_error = _dicre_collect_values(
            joined, F.col("emitter"), "value"
        )
        custodian_values, custodian_error = _dicre_collect_values(
            joined, F.col("custodian"), "value"
        )
        if emitter_error or custodian_error:
            errors["DICRE_ACCOUNTS"] = emitter_error or custodian_error
        elif emitter_values or custodian_values:
            values = sorted(set(emitter_values + custodian_values))
            run_many(
                "DICRE_ACCOUNTS",
                [
                    "SELECT cp.NUM_CONTA_PARTICIPANTE, cp.NUM_ID_SITUACAO_CONTA, "
                    "vf.COD_TIPO_ACESSO, vf.NUM_ID_AREA_ATUACAO, "
                    "vcp.NOM_SIMPLIFICADO_ENTIDADE NOM_SIMPLIFICADO "
                    f"FROM {cfg.schema}.CONTA_PARTICIPANTE cp "
                    f"LEFT JOIN {cfg.schema}.V_FAMILIA_CONTAS vf "
                    "ON vf.COD_CONTA_MEMBRO=cp.COD_CONTA_PARTICIPANTE "
                    f"LEFT JOIN {cfg.schema}.V_CONTA_PARTICIPANTE vcp "
                    "ON vcp.COD_CONTA_PARTICIPANTE=cp.COD_CONTA_PARTICIPANTE "
                    "WHERE cp.NUM_CONTA_PARTICIPANTE IN ("
                    + ", ".join(_sql_literal(value) for value in values[offset:offset + 1000])
                    + ")"
                    for offset in range(0, len(values), 1000)
                ],
            )

    if joined is None or not base:
        errors["DICRE_BASES"] = "synthetic emitter/base pairs unavailable"
    else:
        pairs = joined.select("emitter", "base_id").where(
            F.col("emitter").isNotNull() & F.col("base_id").isNotNull()
        ).dropDuplicates().limit(100_001).collect()
        if len(pairs) > 100_000:
            errors["DICRE_BASES"] = "more than 100000 synthetic emitter/base pairs"
        elif pairs:
            run_many(
                "DICRE_BASES",
                [
                    "SELECT DISTINCT pb.NUM_CONTA_PARTICIPANTE, pb.NUM_ID_BASE_CREDITO "
                    f"FROM {cfg.schema}.PARAMETRO_BASE_CREDITO pb "
                    f"JOIN {cfg.schema}.BASE_CREDITO b "
                    "ON b.NUM_ID_BASE_CREDITO=pb.NUM_ID_BASE_CREDITO "
                    f"JOIN {cfg.schema}.TIPO_BASE_CREDITO tb "
                    "ON tb.NUM_ID_TIPO_BASE_CREDITO=b.NUM_ID_TIPO_BASE_CREDITO "
                    "WHERE TRIM(tb.COD_TIPO_BASE_CREDITO)='2' AND ("
                    + " OR ".join(
                        "(pb.NUM_CONTA_PARTICIPANTE=" + _sql_literal(row["emitter"])
                        + " AND pb.NUM_ID_BASE_CREDITO=" + _sql_literal(row["base_id"])
                        + ")" for row in pairs[offset:offset + 500]
                    ) + ")"
                    for offset in range(0, len(pairs), 500)
                ],
            )

    if joined is None or not root_type:
        errors["DICRE_IF_COMPATIBILITY"] = "synthetic IF compatibility triples unavailable"
    else:
        triples = joined.select(
            "credit_if_type", "guaranteed_if_type", "lot_type"
        ).dropna().dropDuplicates().limit(100_001).collect()
        if len(triples) > 100_000:
            errors["DICRE_IF_COMPATIBILITY"] = "more than 100000 synthetic IF triples"
        elif triples:
            run_many(
                "DICRE_IF_COMPATIBILITY",
                [
                    "SELECT DISTINCT h.NUM_TIPO_IF, h.NUM_TIPO_IF_GARANTIDO, "
                    "h.NUM_ID_TIPO_LOTE "
                    f"FROM {cfg.schema}.HABILITA_IF_TIPO_LOTE h "
                    f"JOIN {cfg.schema}.TIPO_IF subtype ON subtype.NUM_TIPO_IF=h.NUM_TIPO_IF "
                    f"JOIN {cfg.schema}.TIPO_IF guaranteed "
                    "ON guaranteed.NUM_TIPO_IF=h.NUM_TIPO_IF_GARANTIDO "
                    "WHERE h.DAT_EXCLUSAO IS NULL AND subtype.DAT_EXCLUSAO IS NULL "
                    "AND guaranteed.DAT_EXCLUSAO IS NULL AND h.NUM_ID_TIPO_LOTE=2 "
                    "AND TRIM(guaranteed.COD_TIPO_IF)='LCA' AND ("
                    + " OR ".join(
                        "(h.NUM_TIPO_IF=" + _sql_literal(row["credit_if_type"])
                        + " AND h.NUM_TIPO_IF_GARANTIDO="
                        + _sql_literal(row["guaranteed_if_type"])
                        + " AND h.NUM_ID_TIPO_LOTE=" + _sql_literal(row["lot_type"]) + ")"
                        for row in triples[offset:offset + 500]
                    ) + ")"
                    for offset in range(0, len(triples), 500)
                ],
            )

    qualification_columns = (
        ("NUM_ID_QUALIF_FINALIDADE", "20"),
        ("NUM_ID_QUALIF_JUROS_A_CADA", "21"),
        ("NUM_ID_QUALIF_AMORT_A_CADA", "22"),
        ("NUM_ID_QUALIF_GARANTIA_ESPEC", "23"),
    )
    qualification_rows = []
    for name, subgroup in qualification_columns:
        actual = resolve(root, name)
        if actual:
            qualification_rows.extend(
                (row["value"], subgroup)
                for row in root.select(
                    _canon_key_col(F.col(actual)).alias("value")
                ).where(F.col("value").isNotNull()).dropDuplicates().limit(100_001).collect()
            )
    qualification_rows = sorted(set(qualification_rows))
    if len(qualification_rows) > 100_000:
        errors["DICRE_QUALIFICATIONS"] = "more than 100000 synthetic qualification pairs"
    elif qualification_rows:
        run_many(
            "DICRE_QUALIFICATIONS",
            [
                "SELECT DISTINCT r.NUM_ID_QUALIFICACAO, "
                "s.NUM_ID_QUALIFICACAO_SUBGRUPO "
                f"FROM {cfg.schema}.REL_QUALIF_SUBG_TIPO_IF r "
                f"JOIN {cfg.schema}.QUALIFICACAO q "
                "ON q.NUM_ID_QUALIFICACAO=r.NUM_ID_QUALIFICACAO "
                f"JOIN {cfg.schema}.QUALIF_SUBG_TIPO_IF s "
                "ON s.NUM_ID_QUALIF_SUBG_TIPO_IF=r.NUM_ID_QUALIF_SUBG_TIPO_IF "
                f"JOIN {cfg.schema}.QUALIFICACAO_SUBGRUPO sg "
                "ON sg.NUM_ID_QUALIFICACAO_SUBGRUPO=s.NUM_ID_QUALIFICACAO_SUBGRUPO "
                f"JOIN {cfg.schema}.TIPO_IF tif ON tif.NUM_TIPO_IF=s.NUM_TIPO_IF "
                "WHERE sg.NUM_ID_QUALIFICACAO_GRUPO=4 AND r.IND_HABILITADO='S' "
                "AND q.NUM_ID_SITUACAO_QUALIFICACAO=0 "
                "AND TRIM(tif.COD_TIPO_IF)='LCA' AND ("
                + " OR ".join(
                    "(r.NUM_ID_QUALIFICACAO=" + _sql_literal(value)
                    + " AND s.NUM_ID_QUALIFICACAO_SUBGRUPO=" + _sql_literal(subgroup) + ")"
                    for value, subgroup in qualification_rows[offset:offset + 500]
                ) + ")"
                for offset in range(0, len(qualification_rows), 500)
            ],
        )
    else:
        frames["DICRE_QUALIFICATIONS"] = spark.createDataFrame(
            [], "NUM_ID_QUALIFICACAO string, NUM_ID_QUALIFICACAO_SUBGRUPO string"
        )

    run(
        "TCTPFEATURE_TOGGLE",
        "SELECT COD_FTRE_TOG, IND_FTRE_HAB, DATA_INIC_VIG_FTRE, DATA_FIM_VIG_FTRE "
        f"FROM {cfg.schema}.TCTPFEATURE_TOGGLE "
        f"WHERE COD_FTRE_TOG='{DICRE_IPOC_TOGGLE}'",
    )
    toggle = frames.get("TCTPFEATURE_TOGGLE")
    inclusion = resolve(root, "DAT_INCLUSAO")
    ipoc = resolve(root, "COD_IPOC")
    if toggle is not None and inclusion and ipoc:
        toggle_roots = root.select(
            F.to_date(F.col(inclusion)).alias("inclusion_date"),
            _dicre_text(F.col(ipoc)).alias("ipoc"),
        )
        enabled = _dicre_enabled_credits(toggle_roots, toggle)
        if enabled is not None:
            ipocs, error = _dicre_collect_values(enabled, F.col("ipoc"), "value")
            if error:
                errors["CREDITO_DC_TARGET"] = error
            elif ipocs:
                run_many(
                    "CREDITO_DC_TARGET",
                    [
                        "SELECT COD_IPOC, DAT_EXCLUSAO "
                        f"FROM {cfg.schema}.CREDITO_DC WHERE DAT_EXCLUSAO IS NULL "
                        "AND TRIM(COD_IPOC) IN ("
                        + ", ".join(
                            _sql_literal(value) for value in ipocs[offset:offset + 1000]
                        ) + ")"
                        for offset in range(0, len(ipocs), 1000)
                    ],
                )
            else:
                frames["CREDITO_DC_TARGET"] = spark.createDataFrame(
                    [], "COD_IPOC string, DAT_EXCLUSAO string"
                )
    return frames, errors


def check_dicre_registration_profile(
    tables: Dict[str, DataFrame], sample: int, registration_profile: bool,
    profile: ValidationProfile,
) -> List[Finding]:
    if profile.pipeline != "dicre" or not registration_profile:
        return []
    requirements = {
        LOTE_TABLE: (
            "NUM_ID_LOTE", "NUM_ID_TIPO_LOTE", "IND_REVOLVENCIA", "DAT_EXCLUSAO",
        ),
        CREDITO_DC_TABLE: (
            "NUM_ID_CREDITO_DC", "COD_CREDITO_DC", "NUM_ID_LOTE", "NUM_TIPO_IF",
            "VAL_PU", "VAL_PU_EMISSAO", "DAT_VAL_PU", "DAT_CONTRATACAO",
            "DAT_VENCIMENTO", "COD_TIPO_PESSOA", "NUM_ID_MODALIDADE_CREDITO",
            "NUM_ID_TIPO_GARANTIA", "NUM_ID_BASE_CREDITO", "NUM_ID_INDEXADOR_CREDITO",
            "NUM_ID_FORMA_PAGAMENTO", "NUM_ID_TIPO_AMORTIZACAO", "IND_INADIMPLENTE",
            "IND_MULTIPLO_IPOC", "IND_BAIXA_AUTOMATICA_VENC", "NUM_ID_UF",
        ),
        HISTORICO_CREDITO_DC_TABLE: (
            "NUM_ID_HISTORICO_CREDITO_DC", "COD_CREDITO_DC", "NUM_ID_LOTE",
            "NUM_ID_TIPO_ACAO_HIST_CREDITO", "VAL_PU", "DAT_VAL_PU",
            "IND_INADIMPLENTE", "DAT_IND_INADIMPLENTE",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_dicre_unavailable("8f.profile.availability", missing, SEV_WARN)]
    lot_cols = columns[LOTE_TABLE]
    root_cols = columns[CREDITO_DC_TABLE]
    history_cols = columns[HISTORICO_CREDITO_DC_TABLE]
    lots = _credito_scr_active(tables[LOTE_TABLE])
    roots = tables[CREDITO_DC_TABLE]
    inclusion = tables[HISTORICO_CREDITO_DC_TABLE].where(
        _canon_key_col(F.col(history_cols["NUM_ID_TIPO_ACAO_HIST_CREDITO"])) == "1"
    )
    out: List[Finding] = []

    lot_bad = lots.where(
        ~F.coalesce(
            (_dicre_text(F.col(lot_cols["NUM_ID_TIPO_LOTE"])) == "2")
            & (_dicre_text(F.col(lot_cols["IND_REVOLVENCIA"])) == "S"),
            F.lit(False),
        )
    )
    count = lot_bad.count()
    out.append(Finding(
        "8f.profile.lot_constants", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, LOTE_TABLE, count == 0, count=count,
        column="NUM_ID_TIPO_LOTE,IND_REVOLVENCIA",
        sample=_sample_keys(lot_bad, [lot_cols["NUM_ID_LOTE"]], sample),
        hint="Treat type-2/revolvencia-S as one DICREINCL batch observation only."
             if count else "",
        message="Active lots differing from the observed LCA DICRE batch constants.",
    ))

    root_expected = {
        "VAL_PU": "1", "VAL_PU_EMISSAO": "1", "COD_TIPO_PESSOA": "PJ",
        "NUM_ID_MODALIDADE_CREDITO": "46", "NUM_ID_TIPO_GARANTIA": "16",
        "NUM_ID_BASE_CREDITO": "4199", "IND_INADIMPLENTE": "N",
        "IND_MULTIPLO_IPOC": "N", "IND_BAIXA_AUTOMATICA_VENC": "N",
    }
    root_mismatch = reduce(
        lambda left, right: left | right,
        [
            ~F.coalesce(
                _canon_key_col(F.col(root_cols[name])) == expected, F.lit(False)
            )
            for name, expected in root_expected.items()
        ],
    )
    root_bad = roots.where(root_mismatch)
    count = root_bad.count()
    out.append(Finding(
        "8f.profile.root_constants", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
        column=",".join(root_expected),
        sample=_sample_keys(root_bad, [root_cols["NUM_ID_CREDITO_DC"]], sample),
        hint="Confirm drift against another successful DICRE route; these are not hard rules."
             if count else "",
        message="CREDITO_DC rows differing from common values in the observed batch.",
    ))

    observed_combinations = {
        ("53", "2", "268", "6"),
        ("139", "1", "267", "3"),
        ("166", "2", "268", "6"),
        ("167", "2", "267", "1"),
        ("177", "2", "297", "6"),
    }
    variants = roots.select(
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])).alias("if_type"),
        _canon_key_col(F.col(root_cols["NUM_ID_INDEXADOR_CREDITO"])).alias("indexer"),
        _canon_key_col(F.col(root_cols["NUM_ID_FORMA_PAGAMENTO"])).alias("payment"),
        _canon_key_col(F.col(root_cols["NUM_ID_TIPO_AMORTIZACAO"])).alias("amortization"),
    )
    observed = reduce(
        lambda left, right: left | right,
        [
            (F.col("if_type") == if_type) & (F.col("indexer") == indexer)
            & (F.col("payment") == payment) & (F.col("amortization") == amortization)
            for if_type, indexer, payment, amortization in observed_combinations
        ],
    )
    variant_bad = variants.where(~F.coalesce(observed, F.lit(False)))
    count = variant_bad.count()
    out.append(Finding(
        "8f.profile.subtype_combinations", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
        column="NUM_TIPO_IF,NUM_ID_INDEXADOR_CREDITO,NUM_ID_FORMA_PAGAMENTO,"
               "NUM_ID_TIPO_AMORTIZACAO",
        sample=_sample_keys(
            variant_bad, ["credit_id", "if_type", "indexer", "payment", "amortization"], sample
        ),
        hint="Validate new subtype combinations through live target FKs and compatibility."
             if count else "",
        message="Rows outside the five observed CCB/CMER/CCCM/CCIN/CDIV combinations.",
    ))

    root_history = roots.select(
        _dicre_text(F.col(root_cols["COD_CREDITO_DC"])).alias("code"),
        _canon_key_col(F.col(root_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        F.col(root_cols["VAL_PU"]).alias("root_val_pu"),
        F.col(root_cols["DAT_VAL_PU"]).alias("root_dat_val_pu"),
        F.col(root_cols["IND_INADIMPLENTE"]).alias("root_default"),
    ).join(
        inclusion.select(
            _dicre_text(F.col(history_cols["COD_CREDITO_DC"])).alias("code"),
            _canon_key_col(F.col(history_cols["NUM_ID_LOTE"])).alias("lot_id"),
            _canon_key_col(F.col(history_cols["NUM_ID_HISTORICO_CREDITO_DC"]))
            .alias("history_id"),
            F.col(history_cols["VAL_PU"]).alias("history_val_pu"),
            F.col(history_cols["DAT_VAL_PU"]).alias("history_dat_val_pu"),
            F.col(history_cols["IND_INADIMPLENTE"]).alias("history_default"),
        ), ["code", "lot_id"], "inner",
    )
    history_bad = root_history.where(
        ~F.col("root_val_pu").eqNullSafe(F.col("history_val_pu"))
        | ~F.col("root_dat_val_pu").eqNullSafe(F.col("history_dat_val_pu"))
        | ~F.col("root_default").eqNullSafe(F.col("history_default"))
    )
    count = history_bad.count()
    out.append(Finding(
        "8f.profile.history_copied_values", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, HISTORICO_CREDITO_DC_TABLE,
        count == 0, count=count, column="VAL_PU,DAT_VAL_PU,IND_INADIMPLENTE",
        sample=_sample_keys(history_bad, ["credit_id", "history_id"], sample),
        hint="Copied inclusion values are advisory because later lifecycle changes may drift."
             if count else "",
        message="Inclusion history copied values differing from current CREDITO_DC.",
    ))

    financial = roots.select(
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        F.expr(f"try_cast(`{root_cols['VAL_PU']}` as decimal(38,10))").alias("pu"),
        F.expr(f"try_cast(`{root_cols['VAL_PU_EMISSAO']}` as decimal(38,10))")
        .alias("issue_pu"),
        F.to_date(F.col(root_cols["DAT_VAL_PU"])).alias("pu_date"),
        F.to_date(F.col(root_cols["DAT_CONTRATACAO"])).alias("contract_date"),
        F.to_date(F.col(root_cols["DAT_VENCIMENTO"])).alias("maturity"),
    )
    financial_bad = financial.where(
        (F.col("pu").isNotNull() & F.col("issue_pu").isNotNull()
         & (F.col("pu") != F.col("issue_pu")))
        | (F.col("pu_date").isNotNull() & F.col("contract_date").isNotNull()
           & (F.col("pu_date") != F.col("contract_date")))
        | (F.col("contract_date").isNotNull() & F.col("maturity").isNotNull()
           & (F.col("contract_date") > F.col("maturity")))
    )
    count = financial_bad.count()
    out.append(Finding(
        "8f.profile.financial_dates", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
        column="VAL_PU,VAL_PU_EMISSAO,DAT_VAL_PU,DAT_CONTRATACAO,DAT_VENCIMENTO",
        sample=_sample_keys(financial_bad, ["credit_id"], sample),
        hint="These financial/date equalities are observed, not universal DICRE rules."
             if count else "",
        message="Rows outside the observed financial/date relationships.",
    ))

    uf_rows = roots.select(
        _canon_key_col(F.col(root_cols["NUM_ID_CREDITO_DC"])).alias("credit_id"),
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])).alias("if_type"),
        _canon_key_col(F.col(root_cols["NUM_ID_UF"])).alias("uf"),
    )
    uf_bad = uf_rows.where(
        ((F.col("if_type") == "53") & F.col("uf").isNull())
        | ((F.col("if_type") != "53") & F.col("uf").isNotNull())
    )
    count = uf_bad.count()
    out.append(Finding(
        "8f.profile.uf_behavior", "DICRE observed registration profile",
        SEV_WARN if count else SEV_INFO, CREDITO_DC_TABLE, count == 0, count=count,
        column="NUM_TIPO_IF,NUM_ID_UF",
        sample=_sample_keys(uf_bad, ["credit_id", "if_type", "uf"], sample),
        hint="Interpret UF with HAB_CAMPO_UF_EMISSAO_DICRE_LCA before making it hard."
             if count else "",
        message="UF behavior differing from the observed enabled-toggle batch.",
    ))

    closure_tables = all(table in tables for table in (
        TCTPIROP_ATIV_TABLE, TCTPDET_CHAV_IROP_CCB_TABLE,
        TCTPDET_CHAV_IROP_CMER_TABLE, TCTPSOLI_IROP_ATIV_TABLE,
    ))
    if not closure_tables:
        out.append(_dicre_unavailable(
            "8f.profile.irop_shape", ["complete IROP closure"], SEV_WARN
        ))
    else:
        irop_cols, irop_missing = _credito_scr_columns(tables, {
            TCTPIROP_ATIV_TABLE: ("NUM_IROP_ATIV", "NUM_IDT_CRE_DC", "NUM_CHAV_IROP"),
            TCTPDET_CHAV_IROP_CCB_TABLE: ("NUM_CHAV_IROP",),
            TCTPDET_CHAV_IROP_CMER_TABLE: ("NUM_CHAV_IROP",),
            TCTPSOLI_IROP_ATIV_TABLE: ("NUM_IROP_ATIV",),
        })
        if irop_missing:
            out.append(_dicre_unavailable("8f.profile.irop_shape", irop_missing, SEV_WARN))
        else:
            irops = tables[TCTPIROP_ATIV_TABLE].select(
                _canon_key_col(F.col(irop_cols[TCTPIROP_ATIV_TABLE]["NUM_IDT_CRE_DC"]))
                .alias("credit_id"),
                _canon_key_col(F.col(irop_cols[TCTPIROP_ATIV_TABLE]["NUM_IROP_ATIV"]))
                .alias("irop_id"),
                _canon_key_col(F.col(irop_cols[TCTPIROP_ATIV_TABLE]["NUM_CHAV_IROP"]))
                .alias("irop_key"),
            )
            ccb = tables[TCTPDET_CHAV_IROP_CCB_TABLE].select(
                _canon_key_col(F.col(irop_cols[TCTPDET_CHAV_IROP_CCB_TABLE]["NUM_CHAV_IROP"]))
                .alias("ccb_key")
            )
            cmer = tables[TCTPDET_CHAV_IROP_CMER_TABLE].select(
                _canon_key_col(F.col(irop_cols[TCTPDET_CHAV_IROP_CMER_TABLE]["NUM_CHAV_IROP"]))
                .alias("cmer_key")
            )
            requests = tables[TCTPSOLI_IROP_ATIV_TABLE].select(
                _canon_key_col(F.col(irop_cols[TCTPSOLI_IROP_ATIV_TABLE]["NUM_IROP_ATIV"]))
                .alias("request_irop_id")
            )
            closure_counts = irops.join(
                ccb.groupBy("ccb_key").count().withColumnRenamed("count", "ccb_count"),
                F.col("irop_key") == F.col("ccb_key"), "left",
            ).join(
                cmer.groupBy("cmer_key").count().withColumnRenamed("count", "cmer_count"),
                F.col("irop_key") == F.col("cmer_key"), "left",
            ).join(
                requests.groupBy("request_irop_id").count().withColumnRenamed(
                    "count", "request_count"
                ),
                F.col("irop_id") == F.col("request_irop_id"), "left",
            ).groupBy("credit_id").agg(
                F.count(F.lit(1)).alias("irop_count"),
                F.sum(F.coalesce(F.col("ccb_count"), F.lit(0))).alias("ccb_count"),
                F.sum(F.coalesce(F.col("cmer_count"), F.lit(0))).alias("cmer_count"),
                F.sum(F.coalesce(F.col("request_count"), F.lit(0))).alias("request_count"),
            )
            shape = variants.select("credit_id", "if_type").join(
                closure_counts, "credit_id", "left"
            ).fillna(0, ["irop_count", "ccb_count", "cmer_count", "request_count"])
            shape_bad = shape.where(
                ((F.col("if_type") == "53")
                 & ((F.col("irop_count") != 1) | (F.col("ccb_count") != 1)
                    | (F.col("cmer_count") != 0) | (F.col("request_count") != 1)))
                | ((F.col("if_type") == "139")
                   & ((F.col("irop_count") != 1) | (F.col("ccb_count") != 0)
                      | (F.col("cmer_count") != 1) | (F.col("request_count") != 1)))
                | ((~F.col("if_type").isin("53", "139")) & (F.col("irop_count") != 0))
            )
            count = shape_bad.count()
            out.append(Finding(
                "8f.profile.irop_shape", "DICRE observed registration profile",
                SEV_WARN if count else SEV_INFO, TCTPIROP_ATIV_TABLE,
                count == 0, count=count, column="observed CCB/CMER closure",
                sample=_sample_keys(shape_bad, ["credit_id", "if_type"], sample),
                hint="Do not promote observed CCB/CMER closure cardinality to a hard rule."
                     if count else "",
                message="IROP closure shape differing from the observed batch.",
            ))
    return out


# ---------------------------------------------------------------------------
# Category 0/2e/6e/8e - LCI registration-route evidence
# ---------------------------------------------------------------------------
LCI_OUTPUT_TABLES = (
    "INSTRUMENTO_FINANCEIRO", "TITULO", "CREDITO", "CONDICAO_IF",
    "JUROS_FIXO", "JUROS_FLUTUANTE", "ATUALIZACAO_POS", "RESGATE",
    "HISTORICO_PU_CURVA", "EVENTO", "DEPOSITO_AUTOMATICO_IF", "OPERACAO",
    "DADO_OPERACAO", "LANCAMENTO", "ESPECIFICACAO", "ESPECIFICACAO_COMITENTE",
    "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
)
LCI_CONDITION_SUBTYPES = {
    "2": "JUROS_FIXO",
    "3": "JUROS_FLUTUANTE",
    "4": "ATUALIZACAO_POS",
    "20": "RESGATE",
}
LCI_MEU_NUMERO_TOGGLE = "VALIDA_MEU_NUMERO_DEPOSITO"


def _lci_text(column):
    """Exact business-code semantics: trim only, preserving case and '.0'."""
    return F.trim(column.cast("string"))


def _lci_unavailable(check_id: str, missing: List[str], severity: str = SEV_WARN) -> Finding:
    return Finding(
        check_id, "LCI", severity,
        ",".join(sorted({value.split(".")[0] for value in missing})), False,
        hint="Export the complete LCI aggregate or make its bounded target lookup available.",
        message=f"Check unavailable; missing required input: {', '.join(missing)}.",
    )


def check_lci_metadata(
    meta: Metadata, no_oracle: bool, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lci":
        return []
    if no_oracle:
        return [Finding(
            "0.lci_metadata", "Coverage", SEV_WARN, "Oracle metadata", False,
            hint="Rerun with Oracle access; specs.json is not authoritative for this route.",
            message="Live Oracle table and PK metadata for the 18-table LCI aggregate is "
                    "unavailable under --no-oracle (forces PARTIAL).",
        )]
    missing = [table for table in LCI_OUTPUT_TABLES if table not in meta.tables]
    missing_pk = [
        table for table in LCI_OUTPUT_TABLES if table in meta.tables and not meta.pk.get(table)
    ]
    failed = bool(missing or missing_pk)
    return [Finding(
        "0.lci_metadata", "Coverage", SEV_ERROR if failed else SEV_INFO,
        ",".join(LCI_OUTPUT_TABLES), not failed, count=len(missing) + len(missing_pk),
        hint="Read live table and PK metadata for every LCI output table; do not fill the "
             "HISTORICO_PU_CURVA gap from specs.json." if failed else "",
        message=(f"Missing Oracle table metadata={missing}; missing PK metadata={missing_pk}."
                 if failed else
                 "Live Oracle table and PK metadata cover all 18 LCI output tables."),
    )]


def _lci_edge_findings(
    check_id: str, parents: DataFrame, child: DataFrame, child_table: str,
    parent_column: str, child_id_column: str, sample: int,
) -> List[Finding]:
    parent_counts = parents.groupBy("parent_id").count().withColumnRenamed(
        "count", "parent_count"
    )
    edges = child.select(
        _canon_key_col(F.col(parent_column)).alias("parent_id"),
        _canon_key_col(F.col(child_id_column)).alias("child_id"),
    )
    bad = edges.join(parent_counts, "parent_id", "left").where(
        F.coalesce(F.col("parent_count"), F.lit(0)) != 1
    )
    count = bad.count()
    duplicate = edges.groupBy("parent_id", "child_id").count().where(F.col("count") > 1)
    duplicate_count = duplicate.count()
    return [
        Finding(
            f"{check_id}.edge", "LCI graph", SEV_ERROR if count else SEV_INFO,
            child_table, count == 0, count=count, column=parent_column,
            sample=_sample_keys(bad, ["child_id", "parent_id"], sample),
            hint="Remove the orphan/ambiguous edge or export its one parent." if count else "",
            message="LCI child rows must resolve to exactly one aggregate parent.",
        ),
        Finding(
            f"{check_id}.duplicate", "LCI graph",
            SEV_ERROR if duplicate_count else SEV_INFO, child_table,
            duplicate_count == 0, count=duplicate_count,
            column=f"{child_id_column},{parent_column}",
            sample=_sample_keys(duplicate, ["child_id", "parent_id"], sample),
            hint="Keep each physical child-to-parent edge unambiguous."
                 if duplicate_count else "",
            message="Duplicate LCI physical graph edges.",
        ),
    ]


def check_lci_graph(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lci":
        return []
    missing_tables = [table for table in LCI_OUTPUT_TABLES if table not in tables]
    if missing_tables:
        return [_lci_unavailable("2e.output_tables", missing_tables, SEV_ERROR)]

    requirements = {
        "INSTRUMENTO_FINANCEIRO": ("NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "COD_IF"),
        "TITULO": ("NUM_IF",),
        "CREDITO": ("NUM_IF",),
        "CONDICAO_IF": ("NUM_CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF"),
        "HISTORICO_PU_CURVA": ("NUM_HISTORICO_PU_CURVA", "NUM_IF"),
        "EVENTO": ("NUM_EVENTO", "NUM_IF"),
        "DEPOSITO_AUTOMATICO_IF": ("NUM_IF",),
        "OPERACAO": ("NUM_ID_OPERACAO", "NUM_IF"),
        "DADO_OPERACAO": ("NUM_ID_DADO_OPERACAO", "NUM_ID_OPERACAO"),
        "LANCAMENTO": ("NUM_ID_LANCAMENTO", "NUM_ID_OPERACAO"),
        "ESPECIFICACAO": ("NUM_ID_ESPECIFICACAO", "NUM_ID_OPERACAO"),
        "ESPECIFICACAO_COMITENTE": (
            "NUM_ID_ESPECIFICACAO_COMITENTE", "NUM_ID_ESPECIFICACAO",
        ),
        "CARTEIRA_COMITENTE": ("NUM_CARTEIRA_COMITENTE", "NUM_IF"),
        "CARTEIRA_PARTICIPANTE": ("NUM_CARTEIRA_PARTICIPANTE", "NUM_IF"),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_lci_unavailable("2e.graph.availability", missing, SEV_ERROR)]

    root_cols = columns["INSTRUMENTO_FINANCEIRO"]
    roots_raw = _active(tables["INSTRUMENTO_FINANCEIRO"]).where(
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])) == str(profile.num_tipo_if)
    )
    roots = roots_raw.select(
        _canon_key_col(F.col(root_cols["NUM_IF"])).alias("parent_id"),
        _lci_text(F.col(root_cols["COD_IF"])).alias("business_code"),
    )
    coded = roots.withColumn(
        "code_count", F.count(F.lit(1)).over(Window.partitionBy("business_code"))
    )
    bad_codes = coded.where(
        F.col("business_code").isNull() | (F.col("business_code") == "")
        | (F.col("code_count") > 1)
    )
    count = bad_codes.count()
    out = [Finding(
        "2e.root_code", "LCI graph", SEV_ERROR if count else SEV_INFO,
        "INSTRUMENTO_FINANCEIRO", count == 0, count=count, column="COD_IF",
        sample=_sample_keys(bad_codes, ["parent_id", "business_code"], sample),
        hint="Generate nonblank, unique exact-trimmed active LCI COD_IF values."
             if count else "",
        message="Active LCI roots with blank or duplicate case-sensitive COD_IF values.",
    )]

    for table in ("TITULO", "CREDITO"):
        child_col = columns[table]["NUM_IF"]
        children = tables[table].select(
            _canon_key_col(F.col(child_col)).alias("parent_id")
        )
        counts = children.groupBy("parent_id").count().withColumnRenamed("count", "child_count")
        bad = roots.select("parent_id").join(counts, "parent_id", "left").where(
            F.coalesce(F.col("child_count"), F.lit(0)) != 1
        )
        child_count = bad.count()
        out.append(Finding(
            f"2e.one_{table.lower()}", "LCI graph",
            SEV_ERROR if child_count else SEV_INFO, table, child_count == 0,
            count=child_count, column="NUM_IF",
            sample=_sample_keys(bad, ["parent_id"], sample),
            hint=f"Keep exactly one {table} row per active LCI root."
                 if child_count else "",
            message=f"Active LCI roots without exactly one {table} row.",
        ))
        out.extend(_lci_edge_findings(
            f"2e.{table.lower()}", roots.select("parent_id"), tables[table], table,
            child_col, child_col, sample,
        ))

    direct_edges = (
        ("condition", "CONDICAO_IF", "NUM_IF", "NUM_CONDICAO_IF"),
        ("history", "HISTORICO_PU_CURVA", "NUM_IF", "NUM_HISTORICO_PU_CURVA"),
        ("event", "EVENTO", "NUM_IF", "NUM_EVENTO"),
        ("deposit", "DEPOSITO_AUTOMATICO_IF", "NUM_IF", "NUM_IF"),
        ("operation", "OPERACAO", "NUM_IF", "NUM_ID_OPERACAO"),
        ("wallet_comitente", "CARTEIRA_COMITENTE", "NUM_IF", "NUM_CARTEIRA_COMITENTE"),
        ("wallet_participante", "CARTEIRA_PARTICIPANTE", "NUM_IF",
         "NUM_CARTEIRA_PARTICIPANTE"),
    )
    for name, table, parent_name, child_name in direct_edges:
        out.extend(_lci_edge_findings(
            f"2e.{name}", roots.select("parent_id"), tables[table], table,
            columns[table][parent_name], columns[table][child_name], sample,
        ))

    operations = tables["OPERACAO"].select(
        _canon_key_col(F.col(columns["OPERACAO"]["NUM_ID_OPERACAO"])).alias("parent_id")
    )
    for name, table, child_name in (
        ("operation_data", "DADO_OPERACAO", "NUM_ID_DADO_OPERACAO"),
        ("launch", "LANCAMENTO", "NUM_ID_LANCAMENTO"),
        ("specification", "ESPECIFICACAO", "NUM_ID_ESPECIFICACAO"),
    ):
        out.extend(_lci_edge_findings(
            f"2e.{name}", operations, tables[table], table,
            columns[table]["NUM_ID_OPERACAO"], columns[table][child_name], sample,
        ))
    specifications = tables["ESPECIFICACAO"].select(
        _canon_key_col(F.col(columns["ESPECIFICACAO"]["NUM_ID_ESPECIFICACAO"]))
        .alias("parent_id")
    )
    out.extend(_lci_edge_findings(
        "2e.specification_holder", specifications, tables["ESPECIFICACAO_COMITENTE"],
        "ESPECIFICACAO_COMITENTE",
        columns["ESPECIFICACAO_COMITENTE"]["NUM_ID_ESPECIFICACAO"],
        columns["ESPECIFICACAO_COMITENTE"]["NUM_ID_ESPECIFICACAO_COMITENTE"], sample,
    ))
    return out


def check_lci_polymorphism(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lci":
        return []
    requirements = {
        "CONDICAO_IF": ("NUM_CONDICAO_IF", "COD_TIPO_CONDICAO_IF"),
        **{table: ("NUM_CONDICAO_IF",) for table in LCI_CONDITION_SUBTYPES.values()},
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_lci_unavailable("2e.condition.availability", missing, SEV_ERROR)]
    condition = _active(tables["CONDICAO_IF"]).select(
        _canon_key_col(F.col(columns["CONDICAO_IF"]["NUM_CONDICAO_IF"])).alias("condition_id"),
        _lci_text(F.col(columns["CONDICAO_IF"]["COD_TIPO_CONDICAO_IF"])).alias(
            "condition_type"
        ),
    )
    membership = None
    for table in LCI_CONDITION_SUBTYPES.values():
        frame = _active(tables[table]).select(
            _canon_key_col(F.col(columns[table]["NUM_CONDICAO_IF"])).alias("condition_id"),
            F.lit(table).alias("physical_table"),
        )
        membership = frame if membership is None else membership.unionByName(frame)
    counts = membership.groupBy("condition_id").pivot(
        "physical_table", list(LCI_CONDITION_SUBTYPES.values())
    ).count().fillna(0)
    known = condition.where(F.col("condition_type").isin(*LCI_CONDITION_SUBTYPES))
    joined = known.join(counts, "condition_id", "left").fillna(
        0, list(LCI_CONDITION_SUBTYPES.values())
    )
    expected_pairs = []
    for code, table in LCI_CONDITION_SUBTYPES.items():
        expected_pairs.extend((F.lit(code), F.lit(table)))
    joined = joined.withColumn(
        "expected_table", F.create_map(*expected_pairs)[F.col("condition_type")]
    )
    bad = joined.where(reduce(
        lambda left, right: left | right,
        [
            F.when(F.col("expected_table") == table, F.col(table) != 1)
            .otherwise(F.col(table) != 0)
            for table in LCI_CONDITION_SUBTYPES.values()
        ],
    ))
    bad_count = bad.count()
    unknown = condition.where(
        F.col("condition_type").isNull()
        | (F.col("condition_type") == "")
        | ~F.col("condition_type").isin(*LCI_CONDITION_SUBTYPES)
    )
    unknown_count = unknown.count()
    condition_ids = condition.select("condition_id").dropDuplicates()
    orphan = membership.join(condition_ids, "condition_id", "left_anti")
    orphan_count = orphan.count()
    return [
        Finding(
            "2e.condition_polymorphism", "LCI condition polymorphism",
            SEV_ERROR if bad_count else SEV_INFO, "CONDICAO_IF", bad_count == 0,
            count=bad_count, column="COD_TIPO_CONDICAO_IF,NUM_CONDICAO_IF",
            sample=_sample_keys(bad, ["condition_id", "condition_type"], sample),
            hint="For known LCI types, emit exactly one expected physical row and none in "
                 "the other known tables." if bad_count else "",
            message="Known LCI conditions with missing, duplicate, or wrong physical subtype.",
        ),
        Finding(
            "2e.unknown_condition_type", "LCI condition polymorphism",
            SEV_WARN if unknown_count else SEV_INFO, "CONDICAO_IF", unknown_count == 0,
            count=unknown_count, column="COD_TIPO_CONDICAO_IF",
            sample=_sample_keys(unknown, ["condition_id", "condition_type"], sample),
            hint="Capture another successful LCI variant before assigning a physical mapping."
                 if unknown_count else "",
            message="LCI condition types outside the four log-proven mappings.",
        ),
        Finding(
            "2e.subtype_orphan", "LCI condition polymorphism",
            SEV_ERROR if orphan_count else SEV_INFO, "CONDICAO_IF", orphan_count == 0,
            count=orphan_count, column="NUM_CONDICAO_IF",
            sample=_sample_keys(orphan, ["condition_id", "physical_table"], sample),
            hint="Remove subtype rows without a CONDICAO_IF parent." if orphan_count else "",
            message="Known LCI physical subtype rows without a condition parent.",
        ),
    ]


def _lci_active_target(frame: DataFrame) -> Tuple[Optional[DataFrame], bool]:
    exclusion = resolve(frame, "DAT_EXCLUSAO")
    if exclusion:
        return frame.where(_oracle_null_equivalent(F.col(exclusion))), True
    deleted = resolve(frame, "IND_EXCLUIDO")
    if deleted:
        return frame.where(_lci_text(F.col(deleted)) == "N"), True
    return None, False


def _lci_toggle_enabled_roots(roots: DataFrame, toggle: DataFrame) -> Optional[DataFrame]:
    columns = {name: resolve(toggle, name) for name in (
        "COD_FTRE_TOG", "IND_FTRE_HAB", "DATA_INIC_VIG_FTRE", "DATA_FIM_VIG_FTRE",
    )}
    if any(value is None for value in columns.values()):
        return None
    periods = toggle.where(
        (_lci_text(F.col(columns["COD_FTRE_TOG"])) == LCI_MEU_NUMERO_TOGGLE)
        & (_lci_text(F.col(columns["IND_FTRE_HAB"])) == "S")
    ).select(
        F.to_date(F.col(columns["DATA_INIC_VIG_FTRE"])).alias("toggle_start"),
        F.to_date(F.col(columns["DATA_FIM_VIG_FTRE"])).alias("toggle_end"),
    )
    return roots.join(
        F.broadcast(periods),
        (F.col("registration_date") >= F.col("toggle_start"))
        & (F.col("registration_date") <= F.col("toggle_end")),
        "left_semi",
    )


def _lci_collision_finding(
    check_id: str, category: str, source: DataFrame, target: Optional[DataFrame],
    keys: List[str], table: str, sample: int, active_required: bool = True,
) -> Finding:
    if target is None:
        return _lci_unavailable(check_id, [table], SEV_WARN)
    target_columns = {key: resolve(target, key) for key in keys}
    missing = [key for key, actual in target_columns.items() if not actual]
    if missing:
        return _lci_unavailable(check_id, [f"{table}.{key}" for key in missing], SEV_WARN)
    active = target
    if active_required:
        active, supported = _lci_active_target(target)
        if not supported:
            return Finding(
                check_id, category, SEV_WARN, table, False, column=",".join(keys),
                hint="Expose DAT_EXCLUSAO or log-proven IND_EXCLUIDO semantics before "
                     "classifying target rows as active.",
                message="Target wallet/code collision check is unavailable because active-row "
                        "semantics cannot be reconstructed.",
            )
    target_keys = active.select(*[
        _canon_key_col(F.col(target_columns[key])).alias(key) for key in keys
    ]).dropDuplicates()
    bad = source.join(F.broadcast(target_keys), keys, "inner")
    count = bad.count()
    return Finding(
        check_id, category, SEV_ERROR if count else SEV_INFO, table, count == 0,
        count=count, column=",".join(keys), sample=_sample_keys(bad, keys, sample),
        hint="Regenerate natural keys that collide with active target rows." if count else "",
        message="Synthetic LCI natural keys colliding with the target.",
    )


def check_lci_target_frames(
    tables: Dict[str, DataFrame], lookup_frames: Dict[str, DataFrame], sample: int,
    profile: ValidationProfile, lookup_errors: Optional[Dict[str, str]] = None,
) -> List[Finding]:
    if profile.pipeline not in {"lci", "lca"}:
        return []
    lookup_errors = lookup_errors or {}
    requirements = {
        "INSTRUMENTO_FINANCEIRO": (
            "NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "COD_IF", "NUM_ID_LOTE",
            "DAT_REGISTRO",
        ),
        "DEPOSITO_AUTOMATICO_IF": ("NUM_IF", "NUM_CONTROLE_LANCAMENTO"),
        "OPERACAO": (
            "NUM_ID_OPERACAO", "NUM_IF", "NUM_ID_TIPO_OPER_OBJETO_SERV", "COD_OPERACAO",
        ),
        "CARTEIRA_COMITENTE": (
            "NUM_ID_ENTIDADE", "COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
            "NUM_CONTA_PARTICIPANTE",
        ),
        "CARTEIRA_PARTICIPANTE": (
            "COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
            "NUM_CONTA_PARTICIPANTE",
        ),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_lci_unavailable("6e.lookup.availability", missing, SEV_ERROR)]
    root_cols = columns["INSTRUMENTO_FINANCEIRO"]
    roots = _active(tables["INSTRUMENTO_FINANCEIRO"]).where(
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])) == str(profile.num_tipo_if)
    ).select(
        _canon_key_col(F.col(root_cols["NUM_IF"])).alias("root_id"),
        _canon_key_col(F.col(root_cols["NUM_ID_LOTE"])).alias("lot_id"),
        _lci_text(F.col(root_cols["COD_IF"])).alias("business_code"),
        F.to_date(F.col(root_cols["DAT_REGISTRO"])).alias("registration_date"),
    )
    out: List[Finding] = []

    tipo = lookup_frames.get("LCI_TIPO_IF")
    if tipo is None:
        out.append(_lci_unavailable(
            "6e.lookup.tipo_if", [lookup_errors.get("LCI_TIPO_IF", "LCI_TIPO_IF")]
        ))
    else:
        tipo_cols = {name: resolve(tipo, name) for name in ("NUM_TIPO_IF", "COD_TIPO_IF")}
        if any(value is None for value in tipo_cols.values()):
            out.append(_lci_unavailable("6e.lookup.tipo_if", ["LCI_TIPO_IF required columns"]))
        else:
            active_tipo, supported = _lci_active_target(tipo)
            if not supported:
                out.append(_lci_unavailable("6e.lookup.tipo_if", ["TIPO_IF.DAT_EXCLUSAO"]))
            else:
                matches = active_tipo.where(
                    (_lci_text(F.col(tipo_cols["COD_TIPO_IF"])) == profile.object_service_code)
                    & (_canon_key_col(F.col(tipo_cols["NUM_TIPO_IF"]))
                       == str(profile.num_tipo_if))
                ).limit(2).count()
                out.append(Finding(
                    "6e.lookup.tipo_if", "LCI target eligibility",
                    SEV_INFO if matches == 1 else SEV_ERROR, "TIPO_IF", matches == 1,
                    count=0 if matches == 1 else matches, column="NUM_TIPO_IF,COD_TIPO_IF",
                    hint="Provide exactly one active exact-trimmed TIPO_IF LCI row for type 81."
                         if matches != 1 else "",
                    message="Active target LCI type resolves the expected root type.",
                ))

    lot_frame = lookup_frames.get("LCI_LOTES")
    lot_columns = {name: resolve(lot_frame, name) if lot_frame is not None else None for name in (
        "NUM_ID_LOTE", "NUM_ID_TIPO_LOTE", "NUM_CONTA_PARTICIPANTE",
    )}
    lots = None
    if lot_frame is None or any(value is None for value in lot_columns.values()):
        out.append(_lci_unavailable(
            "6e.lookup.lot",
            [lookup_errors.get("LCI_LOTES", "LCI_LOTES required columns")],
        ))
    else:
        active_lots, supported = _lci_active_target(lot_frame)
        if not supported:
            out.append(_lci_unavailable("6e.lookup.lot", ["LOTE.DAT_EXCLUSAO"]))
        else:
            lots = active_lots.select(
                _canon_key_col(F.col(lot_columns["NUM_ID_LOTE"])).alias("lot_id"),
                _canon_key_col(F.col(lot_columns["NUM_ID_TIPO_LOTE"])).alias("lot_type"),
                _canon_key_col(F.col(lot_columns["NUM_CONTA_PARTICIPANTE"]))
                .alias("issuer_account"),
            )
            bad = roots.join(lots, "lot_id", "left").where(
                F.col("issuer_account").isNull()
                | F.col("lot_type").isNull()
                | (F.col("lot_type") != ("2" if profile.pipeline == "lca" else "1"))
            )
            count = bad.count()
            out.append(Finding(
                "6e.lookup.lot", "LCI target eligibility", SEV_ERROR if count else SEV_INFO,
                "LOTE", count == 0, count=count, column="NUM_ID_LOTE,NUM_ID_TIPO_LOTE",
                sample=_sample_keys(bad, ["root_id", "lot_id", "lot_type"], sample),
                hint="Point each LCI root to one active target type-1 lot." if count else "",
                message="LCI roots without an active target type-1 lot.",
            ))

    accounts = lookup_frames.get("LCI_ACCOUNTS")
    account_names = (
        "NUM_CONTA_PARTICIPANTE", "NUM_ID_SITUACAO_CONTA", "COD_TIPO_ACESSO",
        "NUM_ID_AREA_ATUACAO",
    )
    account_columns = {
        name: resolve(accounts, name) if accounts is not None else None for name in account_names
    }
    if lots is None or accounts is None or any(value is None for value in account_columns.values()):
        out.append(_lci_unavailable(
            "6e.lookup.issuer_account",
            [lookup_errors.get("LCI_ACCOUNTS", "LCI_ACCOUNTS required columns")],
        ))
    else:
        eligible_accounts = accounts.select(
            _canon_key_col(F.col(account_columns["NUM_CONTA_PARTICIPANTE"]))
            .alias("issuer_account"),
            _canon_key_col(F.col(account_columns["NUM_ID_SITUACAO_CONTA"])).alias("status"),
            _lci_text(F.col(account_columns["COD_TIPO_ACESSO"])).alias("access"),
            _canon_key_col(F.col(account_columns["NUM_ID_AREA_ATUACAO"])).alias("area"),
        ).where(
            F.col("status").isin("1", "2")
            & ((F.lit(True)) if profile.pipeline == "lca" else
               ((F.col("access") == "L") & (F.col("area") == "1")))
        ).select("issuer_account").dropDuplicates()
        bad = roots.join(lots, "lot_id", "left").join(
            F.broadcast(eligible_accounts), "issuer_account", "left_anti"
        )
        count = bad.count()
        out.append(Finding(
            "6e.lookup.issuer_account", "LCI target eligibility",
            SEV_ERROR if count else SEV_INFO, "CONTA_PARTICIPANTE", count == 0,
            count=count, column="NUM_ID_SITUACAO_CONTA,COD_TIPO_ACESSO,NUM_ID_AREA_ATUACAO",
            sample=_sample_keys(bad, ["root_id", "issuer_account"], sample),
            hint="Use issuer status 1|2 with V_FAMILIA_CONTAS access L in area 1."
                 if count else "",
            message="LCI lot issuer accounts outside the log-proven target eligibility.",
        ))

    object_frame = lookup_frames.get("LCI_OBJECT_SERVICE")
    object_ok = False
    object_available = False
    if object_frame is not None:
        code = resolve(object_frame, "COD_OBJETO_SERVICO")
        low = resolve(object_frame, "IND_PLATAFORMA_BAIXA")
        if code and low:
            object_available = True
            object_ok = object_frame.where(
                (_lci_text(F.col(code)) == profile.object_service_code)
                & (_lci_text(F.col(low)) == "S")
            ).limit(1).count() == 1
    out.append(Finding(
        "6e.lookup.object_service", "LCI target eligibility",
        SEV_INFO if object_ok else SEV_ERROR if object_available else SEV_WARN,
        "V_OBJETOS_SERVICO", object_ok,
        count=0 if object_ok else 1, column="COD_OBJETO_SERVICO,IND_PLATAFORMA_BAIXA",
        hint="Expose exact code LCI enabled on the low platform." if not object_ok else "",
        message="Target low-platform LCI object-service evidence.",
    ))

    operation = tables["OPERACAO"]
    op_cols = columns["OPERACAO"]
    operations = operation.select(
        _canon_key_col(F.col(op_cols["NUM_ID_OPERACAO"])).alias("operation_id"),
        _canon_key_col(F.col(op_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"])).alias("route_id"),
        _lci_text(F.col(op_cols["COD_OPERACAO"])).alias("operation_code"),
    )
    route_frame = lookup_frames.get("LCI_ROUTES")
    route_names = (
        "NUM_ID_TIPO_OPER_OBJETO_SERV", "NUM_ID_OBJETO_SERVICO", "COD_TIPO_OPERACAO",
        "IND_DISPONIVEL_IDENTIFICACAO",
    )
    route_columns = {
        name: resolve(route_frame, name) if route_frame is not None else None
        for name in route_names
    }
    if route_frame is None or any(value is None for value in route_columns.values()):
        out.append(_lci_unavailable(
            "6e.lookup.route", [lookup_errors.get("LCI_ROUTES", "LCI_ROUTES required columns")]
        ))
    else:
        eligible_routes = route_frame.select(
            _canon_key_col(F.col(route_columns["NUM_ID_TIPO_OPER_OBJETO_SERV"]))
            .alias("route_id"),
            _canon_key_col(F.col(route_columns["NUM_ID_OBJETO_SERVICO"]))
            .alias("object_service_id"),
            _lci_text(F.col(route_columns["COD_TIPO_OPERACAO"])).alias("operation_type"),
            _lci_text(F.col(route_columns["IND_DISPONIVEL_IDENTIFICACAO"]))
            .alias("identification_available"),
        ).where(
            (F.col("object_service_id") == str(profile.object_service_id))
            & (F.col("operation_type") == "1")
            & (F.col("identification_available") == "S")
        ).select("route_id").dropDuplicates()
        bad = operations.join(F.broadcast(eligible_routes), "route_id", "left_anti")
        count = bad.count()
        out.append(Finding(
            "6e.lookup.route", "LCI target eligibility", SEV_ERROR if count else SEV_INFO,
            "OPERACAO", count == 0, count=count,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            sample=_sample_keys(bad, ["operation_id", "route_id"], sample),
            hint="Use an identification-enabled LCI object-service 75 route with exact "
                 "operation code 1."
                 if count else "",
            message="Synthetic LCI operations using ineligible target route IDs.",
        ))

    target_codes = lookup_frames.get("LCI_ROOT_CODES")
    if target_codes is None or not resolve(target_codes, "COD_IF"):
        out.append(_lci_unavailable(
            "6e.collision.cod_if",
            [lookup_errors.get("LCI_ROOT_CODES", "INSTRUMENTO_FINANCEIRO.COD_IF")],
        ))
    else:
        active_codes, supported = _lci_active_target(target_codes)
        if not supported:
            out.append(_lci_unavailable(
                "6e.collision.cod_if", ["INSTRUMENTO_FINANCEIRO.DAT_EXCLUSAO"]
            ))
        else:
            target_col = resolve(active_codes, "COD_IF")
            existing = active_codes.select(
                _lci_text(F.col(target_col)).alias("business_code")
            ).dropDuplicates()
            bad = roots.join(F.broadcast(existing), "business_code", "inner")
            count = bad.count()
            out.append(Finding(
                "6e.collision.cod_if", "LCI target collisions",
                SEV_ERROR if count else SEV_INFO, "INSTRUMENTO_FINANCEIRO",
                count == 0, count=count, column="COD_IF",
                sample=_sample_keys(bad, ["root_id", "business_code"], sample),
                hint="Allocate an exact-trimmed, case-sensitive COD_IF absent from active target."
                     if count else "",
                message="Synthetic active LCI COD_IF collisions with active target roots.",
            ))

    local_bad = operations.withColumn(
        "code_count", F.count(F.lit(1)).over(Window.partitionBy("operation_code"))
    ).where(
        F.col("operation_code").isNull() | (F.col("operation_code") == "")
        | (F.col("code_count") > 1)
    )
    count = local_bad.count()
    out.append(Finding(
        "6e.operation_code.local", "LCI target collisions",
        SEV_ERROR if count else SEV_INFO, "OPERACAO", count == 0, count=count,
        column="COD_OPERACAO",
        sample=_sample_keys(local_bad, ["operation_id", "operation_code"], sample),
        hint="Generate nonblank unique exact-trimmed operation codes." if count else "",
        message="Synthetic LCI operations with blank or duplicate COD_OPERACAO.",
    ))
    target_operation_codes = lookup_frames.get("LCI_OPERATION_CODES")
    if target_operation_codes is None or not resolve(target_operation_codes, "COD_OPERACAO"):
        out.append(_lci_unavailable(
            "6e.operation_code.target",
            [lookup_errors.get("LCI_OPERATION_CODES", "OPERACAO.COD_OPERACAO")],
        ))
    else:
        target_col = resolve(target_operation_codes, "COD_OPERACAO")
        active_target, supported = _lci_active_target(target_operation_codes)
        if not supported:
            out.append(_lci_unavailable("6e.operation_code.target", ["OPERACAO.DAT_EXCLUSAO"]))
        else:
            existing = active_target.select(
                _lci_text(F.col(target_col)).alias("operation_code")
            ).dropDuplicates()
            bad = operations.join(F.broadcast(existing), "operation_code", "inner")
            count = bad.count()
            out.append(Finding(
                "6e.operation_code.target", "LCI target collisions",
                SEV_ERROR if count else SEV_INFO, "OPERACAO", count == 0, count=count,
                column="COD_OPERACAO",
                sample=_sample_keys(bad, ["operation_id", "operation_code"], sample),
                hint="Regenerate operation codes colliding with active target OPERACAO."
                     if count else "",
                message="Synthetic LCI COD_OPERACAO collisions with active target operations.",
            ))

    toggle = lookup_frames.get("LCI_TOGGLE")
    if toggle is None:
        out.append(_lci_unavailable(
            "6e.meu_numero", [lookup_errors.get("LCI_TOGGLE", "TCTPFEATURE_TOGGLE")]
        ))
    else:
        enabled_roots = _lci_toggle_enabled_roots(roots, toggle)
        if enabled_roots is None:
            out.append(_lci_unavailable("6e.meu_numero", ["LCI_TOGGLE required columns"]))
        elif enabled_roots.limit(1).count() == 0:
            out.append(Finding(
                "6e.meu_numero", "LCI target collisions", SEV_INFO,
                "DEPOSITO_AUTOMATICO_IF", True, column="NUM_CONTROLE_LANCAMENTO",
                message="VALIDA_MEU_NUMERO_DEPOSITO is disabled for all root DAT_REGISTRO "
                        "dates; target controls are not required.",
            ))
        else:
            dep_cols = columns["DEPOSITO_AUTOMATICO_IF"]
            deposits = tables["DEPOSITO_AUTOMATICO_IF"].select(
                _canon_key_col(F.col(dep_cols["NUM_IF"])).alias("root_id"),
                _lci_text(F.col(dep_cols["NUM_CONTROLE_LANCAMENTO"])).alias("control"),
            ).join(enabled_roots.select("root_id"), "root_id", "inner")
            local = deposits.withColumn(
                "control_count", F.count(F.lit(1)).over(Window.partitionBy("control"))
            ).where(
                F.col("control").isNull() | (F.col("control") == "")
                | (F.col("control_count") > 1)
            )
            target_controls = lookup_frames.get("LCI_CONTROLS")
            target_control_col = (
                resolve(target_controls, "NUM_CONTROLE_LANCAMENTO")
                if target_controls is not None else None
            )
            collision = None
            target_unavailable = target_controls is None or target_control_col is None
            if not target_unavailable:
                active_controls, active_supported = _lci_active_target(target_controls)
                target_unavailable = not active_supported
                if active_supported:
                    controls = active_controls.select(
                        _lci_text(F.col(target_control_col)).alias("control")
                    ).dropDuplicates()
                    collision = deposits.join(F.broadcast(controls), "control", "inner")
            local_count = local.count()
            collision_count = collision.count() if collision is not None else 0
            if target_unavailable:
                out.append(_lci_unavailable(
                    "6e.meu_numero", [lookup_errors.get("LCI_CONTROLS", "active OPERACAO controls")]
                ))
            else:
                bad = local.unionByName(collision, allowMissingColumns=True)
                count = local_count + collision_count
                out.append(Finding(
                    "6e.meu_numero", "LCI target collisions",
                    SEV_ERROR if count else SEV_INFO, "DEPOSITO_AUTOMATICO_IF",
                    count == 0, count=count, column="NUM_CONTROLE_LANCAMENTO",
                    sample=_sample_keys(bad, ["root_id", "control"], sample),
                    hint="Generate nonblank unique controls absent from active target P1/P2."
                         if count else "",
                    message="Toggle-enabled deposits with local or target control collisions.",
                ))

    wallet_specs = (
        (
            "comitente", "CARTEIRA_COMITENTE", "LCI_WALLET_COMITENTE",
            ["NUM_ID_ENTIDADE", "COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
             "NUM_CONTA_PARTICIPANTE"],
        ),
        (
            "participante", "CARTEIRA_PARTICIPANTE", "LCI_WALLET_PARTICIPANTE",
            ["COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
             "NUM_CONTA_PARTICIPANTE"],
        ),
    )
    for name, table, target_name, keys in wallet_specs:
        source_rows = tables[table].select(*[
            _canon_key_col(F.col(columns[table][key])).alias(key) for key in keys
        ])
        duplicate = source_rows.groupBy(*keys).count().where(F.col("count") > 1)
        duplicate_count = duplicate.count()
        out.append(Finding(
            f"6e.wallet.{name}.local", "LCI target collisions",
            SEV_ERROR if duplicate_count else SEV_INFO, table, duplicate_count == 0,
            count=duplicate_count, column=",".join(keys),
            sample=_sample_keys(duplicate, keys, sample),
            hint="Keep each synthetic wallet natural key unique." if duplicate_count else "",
            message="Duplicate synthetic LCI wallet natural keys.",
        ))
        source = source_rows.dropDuplicates()
        out.append(_lci_collision_finding(
            f"6e.wallet.{name}", "LCI target collisions", source,
            lookup_frames.get(target_name), keys, target_name, sample,
            active_required=name != "comitente",
        ))
    return out


def load_lci_target_frames(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], maximum: int = 100_000,
    skip_prefixes: Sequence[str] = (),
    profile: Optional[ValidationProfile] = None,
) -> Tuple[Dict[str, DataFrame], Dict[str, str]]:
    """Load only target rows addressed by bounded distinct synthetic LCI keys."""
    profile = profile or VALIDATION_PROFILES["lci"]
    frames: Dict[str, DataFrame] = {}
    errors: Dict[str, str] = {}

    def run_many(name: str, queries: List[str], empty_schema: Optional[str] = None) -> None:
        if not queries:
            if empty_schema:
                frames[name] = spark.createDataFrame([], empty_schema)
            return
        rows, schema = [], None
        try:
            for query in queries:
                remote = _jdbc(spark, cfg, query)
                schema = schema or remote.schema
                rows.extend(remote.collect())
            frames[name] = spark.createDataFrame(rows, schema)
        except Exception as exc:  # noqa: BLE001
            logger.warning("LCI target lookup failed for %s: %s", name, exc)
            errors[name] = str(exc)

    def collect_values(frame: DataFrame, column, alias: str = "value") -> Optional[List[str]]:
        rows = frame.select(column.alias(alias)).where(
            F.col(alias).isNotNull() & (F.trim(F.col(alias).cast("string")) != "")
        ).dropDuplicates().limit(maximum + 1).collect()
        if len(rows) > maximum:
            return None
        return [str(row[alias]) for row in rows]

    def chunks(values: List[str], size: int = 1000):
        for offset in range(0, len(values), size):
            yield values[offset:offset + size]

    def wanted(check_id: str) -> bool:
        return not _check_is_skipped(check_id, skip_prefixes)

    root = tables.get("INSTRUMENTO_FINANCEIRO")
    operation = tables.get("OPERACAO")
    deposit = tables.get("DEPOSITO_AUTOMATICO_IF")
    if root is None:
        return frames, {"LCI": "INSTRUMENTO_FINANCEIRO unavailable"}
    root_columns = {name: resolve(root, name) for name in (
        "NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "NUM_ID_LOTE", "COD_IF", "DAT_REGISTRO",
    )}
    if any(root_columns[name] is None for name in ("NUM_IF", "NUM_TIPO_IF", "NUM_ID_LOTE")):
        return frames, {"LCI": "LCI root keys unavailable"}
    active_root = _active(root).where(
        _canon_key_col(F.col(root_columns["NUM_TIPO_IF"])) == str(profile.num_tipo_if)
    )

    if wanted("6e.lookup.tipo_if"):
        run_many(
            "LCI_TIPO_IF",
            [
                "SELECT NUM_TIPO_IF, COD_TIPO_IF, DAT_EXCLUSAO "
                f"FROM {cfg.schema}.TIPO_IF "
                f"WHERE NUM_TIPO_IF={profile.num_tipo_if} OR "
                f"TRIM(COD_TIPO_IF)={_sql_literal(profile.object_service_code or '')}"
            ],
        )
    if wanted("6e.lookup.object_service"):
        run_many(
            "LCI_OBJECT_SERVICE",
            [
                "SELECT COD_OBJETO_SERVICO, IND_PLATAFORMA_BAIXA "
                f"FROM {cfg.schema}.V_OBJETOS_SERVICO WHERE COD_OBJETO_SERVICO="
                f"{_sql_literal(profile.object_service_code or '')}"
            ],
        )

    lot_ids = collect_values(
        active_root, _canon_key_col(F.col(root_columns["NUM_ID_LOTE"]))
    )
    needs_lots = wanted("6e.lookup.lot") or wanted("6e.lookup.issuer_account")
    if lot_ids is None and needs_lots:
        errors["LCI_LOTES"] = f"more than {maximum} distinct synthetic lot IDs"
    elif needs_lots:
        run_many(
            "LCI_LOTES",
            [
                "SELECT NUM_ID_LOTE, NUM_ID_TIPO_LOTE, NUM_CONTA_PARTICIPANTE, "
                "NUM_TIPO_IF, DAT_EXCLUSAO "
                f"FROM {cfg.schema}.LOTE WHERE NUM_ID_LOTE IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(lot_ids)
            ],
            "NUM_ID_LOTE string, NUM_ID_TIPO_LOTE string, "
            "NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string, DAT_EXCLUSAO string",
        )
    lots = frames.get("LCI_LOTES")
    lot_account = resolve(lots, "NUM_CONTA_PARTICIPANTE") if lots is not None else None
    accounts = collect_values(lots, _canon_key_col(F.col(lot_account))) if lot_account else []
    if accounts is None and wanted("6e.lookup.issuer_account"):
        errors["LCI_ACCOUNTS"] = f"more than {maximum} distinct synthetic issuer accounts"
    elif wanted("6e.lookup.issuer_account"):
        run_many(
            "LCI_ACCOUNTS",
            [
                ("SELECT cp.NUM_CONTA_PARTICIPANTE, cp.NUM_ID_SITUACAO_CONTA, "
                 "CAST(NULL AS VARCHAR2(1)) COD_TIPO_ACESSO, "
                 "CAST(NULL AS NUMBER) NUM_ID_AREA_ATUACAO "
                 f"FROM {cfg.schema}.CONTA_PARTICIPANTE cp "
                 if profile.pipeline == "lca" else
                 "SELECT cp.NUM_CONTA_PARTICIPANTE, cp.NUM_ID_SITUACAO_CONTA, "
                 "vf.COD_TIPO_ACESSO, vf.NUM_ID_AREA_ATUACAO "
                 f"FROM {cfg.schema}.CONTA_PARTICIPANTE cp "
                 f"LEFT JOIN {cfg.schema}.V_FAMILIA_CONTAS vf "
                 "ON vf.COD_CONTA_MEMBRO=cp.COD_CONTA_PARTICIPANTE ")
                + "WHERE cp.NUM_CONTA_PARTICIPANTE IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(accounts)
            ],
            "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
            "COD_TIPO_ACESSO string, NUM_ID_AREA_ATUACAO string",
        )

    root_codes = (
        collect_values(active_root, _lci_text(F.col(root_columns["COD_IF"])))
        if root_columns["COD_IF"] else []
    )
    if root_codes is None and wanted("6e.collision.cod_if"):
        errors["LCI_ROOT_CODES"] = f"more than {maximum} distinct synthetic COD_IF values"
    elif wanted("6e.collision.cod_if"):
        run_many(
            "LCI_ROOT_CODES",
            [
                f"SELECT COD_IF, DAT_EXCLUSAO FROM {cfg.schema}.INSTRUMENTO_FINANCEIRO "
                "WHERE TRIM(COD_IF) IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(root_codes)
            ],
            "COD_IF string, DAT_EXCLUSAO string",
        )

    op_columns = {name: resolve(operation, name) if operation is not None else None for name in (
        "NUM_ID_TIPO_OPER_OBJETO_SERV", "COD_OPERACAO",
    )}
    route_ids = (
        collect_values(operation, _canon_key_col(F.col(op_columns["NUM_ID_TIPO_OPER_OBJETO_SERV"])))
        if operation is not None and op_columns["NUM_ID_TIPO_OPER_OBJETO_SERV"] else []
    )
    if route_ids is None and wanted("6e.lookup.route"):
        errors["LCI_ROUTES"] = f"more than {maximum} distinct synthetic route IDs"
    elif wanted("6e.lookup.route"):
        run_many(
            "LCI_ROUTES",
            [
                "SELECT tos.NUM_ID_TIPO_OPER_OBJETO_SERV, tos.NUM_ID_OBJETO_SERVICO, "
                "op.COD_TIPO_OPERACAO, tos.IND_DISPONIVEL_IDENTIFICACAO "
                f"FROM {cfg.schema}.TIPO_OPER_OBJETO_SERV tos "
                f"JOIN {cfg.schema}.TIPO_OPERACAO op "
                "ON op.NUM_ID_TIPO_OPERACAO=tos.NUM_ID_TIPO_OPERACAO "
                "WHERE tos.NUM_ID_TIPO_OPER_OBJETO_SERV IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(route_ids)
            ],
            "NUM_ID_TIPO_OPER_OBJETO_SERV string, NUM_ID_OBJETO_SERVICO string, "
            "COD_TIPO_OPERACAO string, IND_DISPONIVEL_IDENTIFICACAO string",
        )

    operation_codes = (
        collect_values(operation, _lci_text(F.col(op_columns["COD_OPERACAO"])))
        if operation is not None and op_columns["COD_OPERACAO"] else []
    )
    if operation_codes is None and wanted("6e.operation_code.target"):
        errors["LCI_OPERATION_CODES"] = (
            f"more than {maximum} distinct synthetic operation codes"
        )
    elif wanted("6e.operation_code.target"):
        run_many(
            "LCI_OPERATION_CODES",
            [
                f"SELECT COD_OPERACAO, DAT_EXCLUSAO FROM {cfg.schema}.OPERACAO "
                "WHERE TRIM(COD_OPERACAO) IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(operation_codes)
            ],
            "COD_OPERACAO string, DAT_EXCLUSAO string",
        )

    if wanted("6e.meu_numero"):
        run_many(
            "LCI_TOGGLE",
            [
                "SELECT COD_FTRE_TOG, IND_FTRE_HAB, DATA_INIC_VIG_FTRE, DATA_FIM_VIG_FTRE "
                f"FROM {cfg.schema}.TCTPFEATURE_TOGGLE "
                f"WHERE COD_FTRE_TOG='{LCI_MEU_NUMERO_TOGGLE}'"
            ],
        )
    controls: Optional[List[str]] = []
    toggle = frames.get("LCI_TOGGLE")
    deposit_if = resolve(deposit, "NUM_IF") if deposit is not None else None
    deposit_control = (
        resolve(deposit, "NUM_CONTROLE_LANCAMENTO") if deposit is not None else None
    )
    if toggle is not None and root_columns["DAT_REGISTRO"] and deposit_if and deposit_control:
        root_dates = active_root.select(
            _canon_key_col(F.col(root_columns["NUM_IF"])).alias("root_id"),
            F.to_date(F.col(root_columns["DAT_REGISTRO"])).alias("registration_date"),
        )
        enabled = _lci_toggle_enabled_roots(root_dates, toggle)
        if enabled is not None:
            enabled_deposits = deposit.select(
                _canon_key_col(F.col(deposit_if)).alias("root_id"),
                _lci_text(F.col(deposit_control)).alias("control"),
            ).join(enabled.select("root_id"), "root_id", "inner")
            controls = collect_values(enabled_deposits, F.col("control"))
    if controls is None and wanted("6e.meu_numero"):
        errors["LCI_CONTROLS"] = f"more than {maximum} distinct synthetic controls"
    elif wanted("6e.meu_numero"):
        run_many(
            "LCI_CONTROLS",
            [
                "SELECT NUM_CONTROLE_LANCAMENTO, DAT_EXCLUSAO FROM ("
                f"SELECT NUM_CONTROLE_LANCAMENTO_P1 NUM_CONTROLE_LANCAMENTO, DAT_EXCLUSAO "
                f"FROM {cfg.schema}.OPERACAO UNION ALL "
                f"SELECT NUM_CONTROLE_LANCAMENTO_P2, DAT_EXCLUSAO FROM {cfg.schema}.OPERACAO"
                ") WHERE TRIM(NUM_CONTROLE_LANCAMENTO) IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")"
                for batch in chunks(controls)
            ],
            "NUM_CONTROLE_LANCAMENTO string, DAT_EXCLUSAO string",
        )

    wallet_specs = (
        (
            "CARTEIRA_COMITENTE", "LCI_WALLET_COMITENTE",
            ["NUM_ID_ENTIDADE", "COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
             "NUM_CONTA_PARTICIPANTE"], None,
        ),
        (
            "CARTEIRA_PARTICIPANTE", "LCI_WALLET_PARTICIPANTE",
            ["COD_TIPO_POSICAO_CARTEIRA", "NUM_SISTEMA", "NUM_IF",
             "NUM_CONTA_PARTICIPANTE"], "DAT_EXCLUSAO",
        ),
    )
    for table, target_name, keys, active_column in wallet_specs:
        wallet_name = "comitente" if table == "CARTEIRA_COMITENTE" else "participante"
        if not wanted(f"6e.wallet.{wallet_name}"):
            continue
        frame = tables.get(table)
        actual = {key: resolve(frame, key) if frame is not None else None for key in keys}
        if frame is None or any(value is None for value in actual.values()):
            errors[target_name] = f"synthetic {table} natural key unavailable"
            continue
        selected = frame.select(*[
            _canon_key_col(F.col(actual[key])).alias(key) for key in keys
        ]).dropna().dropDuplicates().limit(maximum + 1).collect()
        if len(selected) > maximum:
            errors[target_name] = f"more than {maximum} distinct synthetic wallet keys"
            continue
        queries = []
        for batch_start in range(0, len(selected), 250):
            predicates = []
            for row in selected[batch_start:batch_start + 250]:
                predicates.append("(" + " AND ".join(
                    f"{key}={_sql_literal(str(row[key]))}" for key in keys
                ) + ")")
            if predicates:
                select_columns = ", ".join(keys + ([active_column] if active_column else []))
                queries.append(
                    f"SELECT {select_columns} FROM {cfg.schema}.{table} WHERE "
                    + " OR ".join(predicates)
                )
        schema = ", ".join(f"{key} string" for key in keys)
        if active_column:
            schema += f", {active_column} string"
        run_many(target_name, queries, schema)
    return frames, errors


def _lci_profile_bad_constants(
    table: str, frame: Optional[DataFrame], expected: Dict[str, object], sample: int,
) -> Finding:
    check_id = f"8e.profile.{table.lower()}_constants"
    if frame is None:
        return _lci_unavailable(check_id, [table], SEV_WARN)
    columns = {name: resolve(frame, name) for name in expected}
    missing = [name for name, actual in columns.items() if not actual]
    if missing:
        return _lci_unavailable(check_id, [f"{table}.{name}" for name in missing], SEV_WARN)
    bad = frame.where(reduce(
        lambda left, right: left | right,
        [
            ~F.coalesce(_canon_key_col(F.col(columns[name])) == str(value), F.lit(False))
            for name, value in expected.items()
        ],
    ))
    count = bad.count()
    return Finding(
        check_id, "LCI observed registration profile", SEV_WARN if count else SEV_INFO,
        table, count == 0, count=count, column=",".join(expected),
        sample=_sample_keys(bad, frame.columns[:1], sample),
        hint="Treat these values as one successful LCI batch observation only." if count else "",
        message="Rows differing from observed LCI registration constants.",
    )


def check_lci_registration_profile(
    tables: Dict[str, DataFrame], sample: int, registration_profile: bool,
    profile: ValidationProfile,
) -> List[Finding]:
    if profile.pipeline != "lci" or not registration_profile:
        return []
    out = [
        _lci_profile_bad_constants(
            "INSTRUMENTO_FINANCEIRO", tables.get("INSTRUMENTO_FINANCEIRO"),
            {
                "NUM_SISTEMA": 55, "NUM_TIPO_IF": 81, "NUM_ID_FORMA_PAGAMENTO": 19,
                "NUM_ID_MOTIVO_SITUACAO_IF": 22, "COD_SITUACAO_IF": 0,
                "VAL_NOMINAL_EMISSAO": 1, "VAL_NOMINAL_ATUAL": 1,
                "VAL_NOMINAL_EM": 1, "VAL_PU_CURVA": 1,
            }, sample,
        ),
        _lci_profile_bad_constants(
            "TITULO", tables.get("TITULO"),
            {
                "QTD_EMITIDA": 10, "NUM_ID_TIPO_REGIME_TITULO": 2,
                "IND_FRACIONAMENTO": "N", "NOM_FORMA_TITULO": "ESCRITURAL",
            }, sample,
        ),
        _lci_profile_bad_constants(
            "DEPOSITO_AUTOMATICO_IF", tables.get("DEPOSITO_AUTOMATICO_IF"),
            {"VAL_PRECO_UNITARIO": 1}, sample,
        ),
        _lci_profile_bad_constants(
            "OPERACAO", tables.get("OPERACAO"),
            {
                "QTD_OPERACAO": 10, "VAL_PRECO_UNITARIO": 1, "VAL_FINANCEIRO": 10,
                "COD_SITUACAO_OPERACAO": 402, "NUM_ID_MODALIDADE_LIQUIDACAO": 6,
            }, sample,
        ),
        _lci_profile_bad_constants(
            "ESPECIFICACAO", tables.get("ESPECIFICACAO"),
            {
                "QTD_ESPECIFICAR": 10, "NUM_ID_SITUACAO_ESPECIFICACAO": 2,
                "IND_EXCLUIDO": "N",
            }, sample,
        ),
        _lci_profile_bad_constants(
            "ESPECIFICACAO_COMITENTE", tables.get("ESPECIFICACAO_COMITENTE"),
            {
                "QTD_ESPECIFICADA": 10, "VAL_PRECO_UNITARIO": 1,
                "COD_TIPO_POSICAO_CARTEIRA": 1, "IND_EXCLUIDO": "N",
            }, sample,
        ),
        _lci_profile_bad_constants(
            "CARTEIRA_COMITENTE", tables.get("CARTEIRA_COMITENTE"),
            {
                "QTD_CARTEIRA_COMITENTE": 10, "NUM_SISTEMA": 55,
                "COD_TIPO_POSICAO_CARTEIRA": 1,
            }, sample,
        ),
        _lci_profile_bad_constants(
            "CARTEIRA_PARTICIPANTE", tables.get("CARTEIRA_PARTICIPANTE"),
            {
                "QTD_CARTEIRA_PARTICIPANTE": 10, "NUM_SISTEMA": 55,
                "COD_TIPO_POSICAO_CARTEIRA": 1,
            }, sample,
        ),
    ]
    credit = tables.get("CREDITO")
    if credit is None:
        out.append(_lci_unavailable("8e.profile.credit_mostly_null", ["CREDITO"], SEV_WARN))
    else:
        value_columns = [column for column in credit.columns if column.upper() != "NUM_IF"]
        if not value_columns:
            bad = credit.limit(0)
        else:
            bad = credit.where(reduce(
                lambda left, right: left | right,
                [
                    F.col(column).isNotNull()
                    & (F.trim(F.col(column).cast("string")) != "")
                    for column in value_columns
                ],
            ))
        count = bad.count()
        out.append(Finding(
            "8e.profile.credit_mostly_null", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "CREDITO", count == 0, count=count,
            column=",".join(value_columns), sample=_sample_keys(bad, [credit.columns[0]], sample),
            hint="Confirm populated CREDITO attributes against another LCI inclusion route."
                 if count else "",
            message="LCI CREDITO product rows differing from the observed mostly-null profile.",
        ))

    root = tables.get("INSTRUMENTO_FINANCEIRO")
    condition = tables.get("CONDICAO_IF")
    root_key = resolve(root, "NUM_IF") if root is not None else None
    root_type = resolve(root, "NUM_TIPO_IF") if root is not None else None
    condition_root = resolve(condition, "NUM_IF") if condition is not None else None
    condition_type = (
        resolve(condition, "COD_TIPO_CONDICAO_IF") if condition is not None else None
    )
    if not all((
        root is not None, condition is not None, root_key, root_type,
        condition_root, condition_type,
    )):
        out.append(_lci_unavailable(
            "8e.profile.condition_topology", ["INSTRUMENTO_FINANCEIRO/CONDICAO_IF columns"]
        ))
    else:
        roots = _active(root).where(
            _canon_key_col(F.col(root_type)) == "81"
        ).select(_canon_key_col(F.col(root_key)).alias("root_id"))
        topology = _active(condition).select(
            _canon_key_col(F.col(condition_root)).alias("root_id"),
            _lci_text(F.col(condition_type)).alias("condition_type"),
        ).groupBy("root_id").agg(
            F.sort_array(F.collect_list("condition_type")).alias("topology")
        )
        bad = roots.join(topology, "root_id", "left").where(
            ~F.coalesce(
                (F.col("topology") == F.array(F.lit("20"), F.lit("3")))
                | (F.col("topology") == F.array(F.lit("2"), F.lit("20")))
                | (F.col("topology") == F.array(F.lit("2"), F.lit("20"), F.lit("4"))),
                F.lit(False),
            )
        )
        count = bad.count()
        out.append(Finding(
            "8e.profile.condition_topology", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "CONDICAO_IF", count == 0, count=count,
            column="COD_TIPO_CONDICAO_IF",
            sample=_sample_keys(bad, ["root_id", "topology"], sample),
            hint="Observed topologies are floating 3+20, fixed 2+20, and indexed 4+2+20."
                 if count else "",
            message="LCI roots outside the three observed condition topologies.",
        ))

    subtype_frames = []
    for table, rate_column, observed in (
        ("JUROS_FLUTUANTE", "VAL_TAXA_JUROS_FLUTUANTE", ("97", "98", "99")),
        ("JUROS_FIXO", "VAL_TAXA_JUROS_FIXO", ("3.58", "6.7")),
    ):
        frame = tables.get(table)
        rate = resolve(frame, rate_column) if frame is not None else None
        if frame is None or not rate:
            out.append(_lci_unavailable(
                f"8e.profile.{table.lower()}_values", [f"{table}.{rate_column}"]
            ))
            continue
        bad = frame.where(~_canon_key_col(F.col(rate)).isin(*observed))
        count = bad.count()
        subtype_frames.append(Finding(
            f"8e.profile.{table.lower()}_values", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, table, count == 0, count=count,
            column=rate_column, sample=_sample_keys(bad, frame.columns[:1], sample),
            hint=f"Observed exact normalized rates are {observed}; retain new values as advisory."
                 if count else "",
            message="LCI rate values outside the observed batch.",
        ))
    out.extend(subtype_frames)
    update = tables.get("ATUALIZACAO_POS")
    update_expected = {
        "NUM_INDICE_VALORIZACAO": 19, "IND_INCORPORA_ATUALIZACAO": "N",
        "VAL_PERCENTUAL_PARAMETRO": 100, "COD_TIPO_UNIDADE_TEMPO_APLIC": "D",
        "COD_TIPO_PRAZO": "COMERCIAL", "COD_DESLOCAMENTO_INDICE": -2,
        "COD_TIPO_UNIDADE_TEMPO_PART": "M", "COD_TIPO_PRAZO_JUROS_PART": "CORRIDO",
        "NOM_AGENDA_PAGAMENTO": "CONSTANTE",
    }
    if update is not None and update.limit(1).count() == 0:
        out.append(Finding(
            "8e.profile.atualizacao_pos_constants", "LCI observed registration profile",
            SEV_INFO, "ATUALIZACAO_POS", True,
            message="No indexed LCI rows present; indexed-value advisory is not applicable.",
        ))
    else:
        out.append(_lci_profile_bad_constants(
            "ATUALIZACAO_POS", update, update_expected, sample
        ))

    if root is None or not root_key or not root_type or not resolve(root, "COD_IF"):
        out.append(_lci_unavailable("8e.profile.cod_if_allocator", ["INSTRUMENTO_FINANCEIRO"]))
    else:
        cod_if = resolve(root, "COD_IF")
        active_lci = _active(root).where(
            _canon_key_col(F.col(root_type)) == "81"
        )
        bad = active_lci.where(
            ~F.coalesce(_lci_text(F.col(cod_if)).rlike(r"^[0-9]{2}[A-Z][0-9]{8}$"), F.lit(False))
        )
        count = bad.count()
        out.append(Finding(
            "8e.profile.cod_if_allocator", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "INSTRUMENTO_FINANCEIRO", count == 0,
            count=count, column="COD_IF", sample=_sample_keys(bad, [root_key, cod_if], sample),
            hint="The allocator-like format is advisory; do not make it a hard root rule."
                 if count else "",
            message="LCI COD_IF values outside the single observed allocator format.",
        ))

    operation = tables.get("OPERACAO")
    op_id = resolve(operation, "NUM_ID_OPERACAO") if operation is not None else None
    op_root = resolve(operation, "NUM_IF") if operation is not None else None
    if root is None or operation is None or not all((root_key, root_type, op_id, op_root)):
        out.append(_lci_unavailable("8e.profile.async_closure", ["LCI closure columns"]))
    else:
        base = _active(root).where(
            _canon_key_col(F.col(root_type)) == "81"
        ).select(_canon_key_col(F.col(root_key)).alias("root_id"))

        def direct_count(table: str, key_name: str, alias: str) -> None:
            frame = tables.get(table)
            actual = resolve(frame, key_name) if frame is not None else None
            if frame is None or not actual:
                return
            nonlocal base
            counts = frame.select(
                _canon_key_col(F.col(actual)).alias("root_id")
            ).groupBy("root_id").count().withColumnRenamed("count", alias)
            base = base.join(counts, "root_id", "left")

        direct_count("HISTORICO_PU_CURVA", "NUM_IF", "history_count")
        direct_count("DEPOSITO_AUTOMATICO_IF", "NUM_IF", "deposit_count")
        direct_count("OPERACAO", "NUM_IF", "operation_count")
        direct_count("CARTEIRA_COMITENTE", "NUM_IF", "wallet_holder_count")
        direct_count("CARTEIRA_PARTICIPANTE", "NUM_IF", "wallet_participant_count")
        event = tables.get("EVENTO")
        event_root, event_type = (
            (resolve(event, "NUM_IF"), resolve(event, "NUM_TIPO_EVENTO_LEGADO"))
            if event is not None else (None, None)
        )
        if event_root and event_type:
            event_counts = event.select(
                _canon_key_col(F.col(event_root)).alias("root_id"),
                _lci_text(F.col(event_type)).alias("event_type"),
            ).groupBy("root_id").agg(
                F.count(F.lit(1)).alias("event_count"),
                F.sum(F.when(F.col("event_type") == "83", 1).otherwise(0)).alias("event_83"),
                F.sum(F.when(F.col("event_type") == "85", 1).otherwise(0)).alias("event_85"),
            )
            base = base.join(event_counts, "root_id", "left")
        expected_columns = {
            "history_count": 1, "deposit_count": 1, "operation_count": 1,
            "wallet_holder_count": 1, "wallet_participant_count": 1,
            "event_count": 2, "event_83": 1, "event_85": 1,
        }
        available = {
            name: value for name, value in expected_columns.items() if name in base.columns
        }
        bad = base.fillna(0, list(available)).where(reduce(
            lambda left, right: left | right,
            [F.col(name) != expected for name, expected in available.items()],
        )) if available else base
        count = bad.count()
        out.append(Finding(
            "8e.profile.async_closure", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "LCI aggregate", count == 0, count=count,
            column=",".join(available), sample=_sample_keys(bad, ["root_id"], sample),
            hint="Async closure cardinalities are observations; integrity remains hard elsewhere."
                 if count else "",
            message="LCI roots differing from observed history/event/deposit/operation/wallet "
                    "closure cardinalities.",
        ))

    dado = tables.get("DADO_OPERACAO")
    lancamento = tables.get("LANCAMENTO")
    specification = tables.get("ESPECIFICACAO")
    holder = tables.get("ESPECIFICACAO_COMITENTE")
    closure_requirements = {
        "operation": (operation, "NUM_ID_OPERACAO"),
        "data operation": (dado, "NUM_ID_OPERACAO"),
        "launch": (lancamento, "NUM_ID_OPERACAO"),
        "specification": (specification, "NUM_ID_OPERACAO"),
        "specification holder": (holder, "NUM_ID_ESPECIFICACAO"),
    }
    closure_missing = [
        name for name, (frame, key) in closure_requirements.items()
        if frame is None or not resolve(frame, key)
    ]
    dado_type = resolve(dado, "NUM_ID_TIPO_DADO_OPERACAO") if dado is not None else None
    spec_id = resolve(specification, "NUM_ID_ESPECIFICACAO") \
        if specification is not None else None
    if closure_missing or not dado_type or not spec_id:
        out.append(_lci_unavailable(
            "8e.profile.operation_closure",
            closure_missing + (["DADO/ESPECIFICACAO type/key columns"]
                               if not dado_type or not spec_id else []),
        ))
    else:
        operation_id = resolve(operation, "NUM_ID_OPERACAO")
        cluster = operation.select(
            _canon_key_col(F.col(operation_id)).alias("operation_id")
        )
        data_counts = dado.select(
            _canon_key_col(F.col(resolve(dado, "NUM_ID_OPERACAO"))).alias("operation_id"),
            _lci_text(F.col(dado_type)).alias("data_type"),
        ).groupBy("operation_id").agg(
            F.count(F.lit(1)).alias("data_count"),
            F.sum(F.when(F.col("data_type") == "265", 1).otherwise(0)).alias("data_265"),
            F.sum(F.when(F.col("data_type") == "269", 1).otherwise(0)).alias("data_269"),
        )
        launch_counts = lancamento.select(
            _canon_key_col(F.col(resolve(lancamento, "NUM_ID_OPERACAO"))).alias("operation_id")
        ).groupBy("operation_id").count().withColumnRenamed("count", "launch_count")
        specifications = specification.select(
            _canon_key_col(F.col(resolve(specification, "NUM_ID_OPERACAO")))
            .alias("operation_id"),
            _canon_key_col(F.col(spec_id)).alias("specification_id"),
        )
        specification_counts = specifications.groupBy("operation_id").count().withColumnRenamed(
            "count", "specification_count"
        )
        holder_counts = specifications.join(
            holder.select(
                _canon_key_col(F.col(resolve(holder, "NUM_ID_ESPECIFICACAO")))
                .alias("specification_id")
            ), "specification_id", "inner",
        ).groupBy("operation_id").count().withColumnRenamed("count", "holder_count")
        cluster = cluster.join(data_counts, "operation_id", "left").join(
            launch_counts, "operation_id", "left"
        ).join(specification_counts, "operation_id", "left").join(
            holder_counts, "operation_id", "left"
        ).fillna(0)
        bad = cluster.where(
            (F.col("data_count") != 2) | (F.col("data_265") != 1)
            | (F.col("data_269") != 1) | (F.col("launch_count") != 1)
            | (F.col("specification_count") != 1) | (F.col("holder_count") != 1)
        )
        count = bad.count()
        out.append(Finding(
            "8e.profile.operation_closure", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "OPERACAO", count == 0, count=count,
            column="DADO(265+269),LANCAMENTO,ESPECIFICACAO,ESPECIFICACAO_COMITENTE",
            sample=_sample_keys(bad, ["operation_id"], sample),
            hint="Exact async closure counts are advisory; graph integrity remains hard."
                 if count else "",
            message="LCI operations differing from the observed 2:1:1:1 async closure.",
        ))

    date_columns = {
        name: resolve(root, name) if root is not None else None
        for name in (
            "DAT_REGISTRO", "DAT_EMISSAO", "DAT_PU_CURVA", "DAT_ULTIMA_CORRECAO",
            "DAT_SITUACAO_IF", "DAT_VAL_NOMINAL_EM",
        )
    }
    if root is None or not root_key or any(value is None for value in date_columns.values()):
        out.append(_lci_unavailable(
            "8e.profile.copied_dates", [
                f"INSTRUMENTO_FINANCEIRO.{name}"
                for name, actual in date_columns.items() if not actual
            ] or ["INSTRUMENTO_FINANCEIRO"],
        ))
    else:
        dates = root.select(
            _canon_key_col(F.col(root_key)).alias("root_id"),
            *[F.to_date(F.col(actual)).alias(name) for name, actual in date_columns.items()],
        )
        bad = dates.where(reduce(
            lambda left, right: left | right,
            [
                ~F.col("DAT_REGISTRO").eqNullSafe(F.col(name))
                for name in date_columns if name != "DAT_REGISTRO"
            ],
        ))
        count = bad.count()
        out.append(Finding(
            "8e.profile.copied_dates", "LCI observed registration profile",
            SEV_WARN if count else SEV_INFO, "INSTRUMENTO_FINANCEIRO", count == 0,
            count=count, column=",".join(date_columns),
            sample=_sample_keys(bad, ["root_id"], sample),
            hint="Copied business-date equalities are advisory; DAT_INCLUSAO is runtime."
                 if count else "",
            message="LCI roots differing from observed DAT_REGISTRO-based copied dates.",
        ))

    if operation is not None:
        quantity = resolve(operation, "QTD_OPERACAO")
        price = resolve(operation, "VAL_PRECO_UNITARIO")
        financial = resolve(operation, "VAL_FINANCEIRO")
        if all((quantity, price, financial)):
            bad = operation.where(
                F.expr(f"try_cast(`{quantity}` as decimal(38,10))")
                * F.expr(f"try_cast(`{price}` as decimal(38,10))")
                != F.expr(f"try_cast(`{financial}` as decimal(38,10))")
            )
            count = bad.count()
            out.append(Finding(
                "8e.profile.operation_financial_identity", "LCI observed registration profile",
                SEV_WARN if count else SEV_INFO, "OPERACAO", count == 0, count=count,
                column="QTD_OPERACAO,VAL_PRECO_UNITARIO,VAL_FINANCEIRO",
                sample=_sample_keys(bad, [op_id] if op_id else operation.columns[:1], sample),
                hint="QTD*PU=VAL_FINANCEIRO is an observed registration relationship."
                     if count else "",
                message="Operations differing from the observed financial identity.",
            ))
    return out


# ---------------------------------------------------------------------------
# Category 3b - Primary-key integrity (all synthetic Oracle tables)
# ---------------------------------------------------------------------------
MAPA_CLONE_NUM_IF_TABLE = "MAPA_CLONE_NUM_IF"


def check_primary_keys(
    tables: Dict[str, DataFrame], meta: Metadata, sample: int, no_oracle: bool
) -> List[Finding]:
    """Every synthetic Oracle table must have its PK columns present, non-null, and unique.

    Under --no-oracle there is no PK metadata; emit ONE explicit unsupported finding rather
    than silently skipping PK validation.
    """
    cat = "Primary keys"
    if no_oracle:
        return [Finding("3b.pk_unsupported", cat, SEV_WARN, "*", False,
                        hint="Run with Oracle metadata to validate primary keys.",
                        message="PK validation unavailable under --no-oracle (no PK metadata).")]
    out: List[Finding] = []
    for table, df in tables.items():
        if table == MAPA_CLONE_NUM_IF_TABLE:
            continue  # artifact, not an Oracle table (see check_clone_map)
        if table not in meta.tables:
            continue  # not an Oracle table in the target schema
        pk = meta.pk.get(table) or []
        if not pk:
            out.append(Finding("3b.pk_missing_meta", cat, SEV_ERROR, table, False,
                               hint="Table has no primary key in Oracle metadata; confirm it "
                                    "is a real base table before appending.",
                               message=f"{table} has no PK defined in Oracle metadata."))
            continue
        resolved = [resolve(df, c) for c in pk]
        missing = [c for c, a in zip(pk, resolved) if not a]
        if missing:
            out.append(Finding("3b.pk_missing_cols", cat, SEV_ERROR, table, False,
                               column=",".join(missing),
                               hint="Export the full PK for every synthetic table.",
                               message=f"{table} missing PK column(s) {missing}."))
            continue
        null_pred = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in resolved])
        null_bad = df.where(null_pred)
        null_c = null_bad.count()
        out.append(Finding("3b.pk_not_null", cat, SEV_ERROR if null_c else SEV_INFO,
                           table, null_c == 0, count=null_c, column=",".join(pk),
                           sample=_sample_keys(null_bad, resolved, sample),
                           hint="PK columns must be non-null." if null_c else "",
                           message=f"{table} PK null in {null_c} row(s)."))
        total = df.select(*resolved).count()
        distinct = df.select(*resolved).dropDuplicates().count()
        dup = total - distinct
        out.append(Finding("3b.pk_unique", cat, SEV_ERROR if dup else SEV_INFO,
                           table, dup == 0, count=dup, column=",".join(pk),
                           hint="PK tuple must be unique." if dup else "",
                           message=f"{table} has {dup} duplicate PK tuple(s)."))
    return out


def check_clone_map(
    tables: Dict[str, DataFrame], profile: ValidationProfile, sample: int
) -> List[Finding]:
    """Validate the MAPA_CLONE_NUM_IF artifact against the synthetic root output."""
    cat = "Clone map"
    mapa = tables.get(MAPA_CLONE_NUM_IF_TABLE)
    root = tables.get(SHAPE_ROOT_TABLE)
    if mapa is None:
        return [Finding("3c.clone_map", cat, SEV_WARN, MAPA_CLONE_NUM_IF_TABLE, False,
                        hint="Emit MAPA_CLONE_NUM_IF alongside the synthetic tables.",
                        message="MAPA_CLONE_NUM_IF absent; clone-map integrity unchecked.")]
    orig = resolve(mapa, "NUM_IF_ORIG") or resolve(mapa, "NUM_IF_ORIGEM")
    novo = resolve(mapa, "NUM_IF_NOVO")
    kcol = resolve(mapa, "K") or resolve(mapa, "__clone_k")
    missing = [n for n, a in (("NUM_IF_ORIG", orig), ("NUM_IF_NOVO", novo)) if not a]
    if missing:
        return [Finding("3c.clone_map", cat, SEV_ERROR, MAPA_CLONE_NUM_IF_TABLE, False,
                        column=",".join(missing),
                        hint="MAPA_CLONE_NUM_IF must carry NUM_IF_ORIG and NUM_IF_NOVO.",
                        message=f"MAPA_CLONE_NUM_IF missing column(s) {missing}.")]
    out: List[Finding] = []
    # (NUM_IF_ORIG, K) uniqueness — one synthetic per source instance per K.
    if kcol:
        pair_total = mapa.select(orig, kcol).count()
        pair_distinct = mapa.select(orig, kcol).dropDuplicates().count()
        pair_dup = pair_total - pair_distinct
        out.append(Finding("3c.clone_map_orig_k_unique", cat,
                           SEV_ERROR if pair_dup else SEV_INFO, MAPA_CLONE_NUM_IF_TABLE,
                           pair_dup == 0, count=pair_dup, column="NUM_IF_ORIG,K",
                           hint="(NUM_IF_ORIG,K) must be unique." if pair_dup else "",
                           message=f"{pair_dup} duplicate (NUM_IF_ORIG,K) row(s)."))
    # NUM_IF_NOVO uniqueness.
    novo_total = mapa.select(novo).count()
    novo_distinct = mapa.select(novo).dropDuplicates().count()
    novo_dup = novo_total - novo_distinct
    out.append(Finding("3c.clone_map_novo_unique", cat,
                       SEV_ERROR if novo_dup else SEV_INFO, MAPA_CLONE_NUM_IF_TABLE,
                       novo_dup == 0, count=novo_dup, column="NUM_IF_NOVO",
                       hint="NUM_IF_NOVO must be unique." if novo_dup else "",
                       message=f"{novo_dup} duplicate NUM_IF_NOVO value(s)."))
    if root is not None:
        rkey = resolve(root, SHAPE_ROOT_KEY)
        rtipo = resolve(root, "NUM_TIPO_IF")
        if rkey and rtipo:
            roots = _active(root.where(F.col(rtipo).cast("long") == profile.num_tipo_if)) \
                .select(F.col(rkey).cast("long").alias("NUM_IF")).dropDuplicates()
            map_novo = mapa.select(F.col(novo).cast("long").alias("NUM_IF")).dropDuplicates()
            # every synthetic root has a map row.
            roots_wo_map = roots.join(map_novo, "NUM_IF", "left_anti")
            c1 = roots_wo_map.count()
            out.append(Finding("3c.clone_map_covers_roots", cat,
                               SEV_ERROR if c1 else SEV_INFO, MAPA_CLONE_NUM_IF_TABLE,
                               c1 == 0, count=c1, column="NUM_IF_NOVO",
                               sample=_sample_keys(roots_wo_map, ["NUM_IF"], sample),
                               hint="Every synthetic root needs a clone-map row." if c1 else "",
                               message=f"{c1} synthetic root(s) missing from MAPA_CLONE_NUM_IF."))
            # no map row points outside the synthetic root output.
            map_wo_root = map_novo.join(roots, "NUM_IF", "left_anti")
            c2 = map_wo_root.count()
            out.append(Finding("3c.clone_map_no_dangling", cat,
                               SEV_ERROR if c2 else SEV_INFO, MAPA_CLONE_NUM_IF_TABLE,
                               c2 == 0, count=c2, column="NUM_IF_NOVO",
                               sample=_sample_keys(map_wo_root, ["NUM_IF"], sample),
                               hint="Clone-map NUM_IF_NOVO must exist in the root output."
                                    if c2 else "",
                               message=f"{c2} MAPA_CLONE_NUM_IF row(s) point outside the root "
                                       f"output."))
    return out


# ---------------------------------------------------------------------------
# Category 3 - Referential integrity
# ---------------------------------------------------------------------------
# Union strategy is INVERTED relative to earlier versions: instead of pulling
# the parent's (possibly 100M+) distinct keys out of Oracle, we resolve what we
# can against the synthetic output and push only the RESIDUAL orphan candidates
# into Oracle as IN-list lookups. A clone-sized output has few distinct FK
# values, so this is minutes instead of an hour, never trips a parent-size cap,
# and correctly accepts FKs that point at production rows outside the cloned
# set (e.g. a clone's NUM_IF_ORIGEM referencing a real production instrument).
def _canon_key(value) -> str:
    """Canonical string for a key value on either side of the comparison:
    numeric-looking values lose trailing fractional zeros ('123.0000' -> '123')."""
    import re
    s = str(value).strip()
    if re.fullmatch(r"-?\d+\.\d*0*", s):
        s = s.rstrip("0").rstrip(".")
    return s


def _sql_literal(value: str) -> str:
    import re
    if re.fullmatch(r"-?\d+(\.\d+)?", value):
        return value
    return "'" + value.replace("'", "''") + "'"


def _residual_in_oracle(
    spark: SparkSession, cfg: Config, fk: ForeignKey, residual: List[tuple],
    batch_size: int = 1000,
) -> set:
    """Return the subset of residual key-tuples that DO exist in the Oracle parent."""
    exists: set = set()
    cols = ", ".join(fk.parent_cols)
    for i in range(0, len(residual), batch_size):
        batch = residual[i:i + batch_size]
        if len(fk.parent_cols) == 1:
            lits = ", ".join(_sql_literal(t[0]) for t in batch)
            pred = f"{fk.parent_cols[0]} IN ({lits})"
        else:
            tuples = ", ".join(
                "(" + ", ".join(_sql_literal(v) for v in t) + ")" for t in batch
            )
            pred = f"({cols}) IN ({tuples})"
        q = f"SELECT DISTINCT {cols} FROM {cfg.schema}.{fk.parent_table} WHERE {pred}"
        for r in _jdbc(spark, cfg, q).collect():
            exists.add(tuple(_canon_key(r[pc]) for pc in fk.parent_cols))
    return exists


def check_referential(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], meta: Metadata,
    sample: int, validate_against: str, max_residual_keys: int,
    skip_prefixes: Optional[List[str]] = None,
) -> Tuple[List[Finding], List[Tuple[str, str, str]]]:
    """Returns (findings, faltantes): faltantes is one (child_table, fk_column,
    missing_value) row per Oracle-verified single-column orphan — the exact
    input format of engorda_instrumentos.py --faltantes-parquet (TABELA/COLUNA/
    VALOR), so the generator can prune the sampling domain of instruments whose
    cluster references keys absent from the target."""
    def canon(col):
        # Native-function version of _canon_key: numeric-looking values lose
        # trailing fractional zeros; anything else passes through unchanged.
        stripped = F.regexp_replace(
            F.regexp_replace(col, r"(\.\d*?)0+$", "$1"), r"\.$", "")
        return F.when(col.rlike(r"^-?\d+\.\d*0*$"), stripped).otherwise(col)

    skip_prefixes = skip_prefixes or []
    skip_fk = _check_is_skipped("3.fk_", skip_prefixes)
    skip_orphan = _check_is_skipped("3.fk_orphan", skip_prefixes)
    skip_shared = _check_is_skipped("3.shared_key", skip_prefixes)
    out: List[Finding] = []
    faltantes: List[Tuple[str, str, str]] = []

    def append_shared_key_check(table: str, df: DataFrame, fk: ForeignKey,
                                child_actual: List[str]) -> None:
        if skip_shared or not meta.is_shared_key_fk(fk):
            return
        total = df.select(*child_actual).count()
        distinct = df.select(*child_actual).dropDuplicates().count()
        dup = total - distinct
        out.append(Finding(
            "3.shared_key_dup", "Referential integrity",
            SEV_ERROR if dup else SEV_INFO, table, dup == 0, count=dup,
            column=",".join(fk.child_cols),
            hint="Shared-key (PK==FK) 1:1 child has duplicate keys; "
                 "bind_shared_key_children should pair 1:1 with distinct parent keys.",
            message=f"Duplicate shared-key values in {table}.",
        ))

    for table, df in tables.items():
        for fk in meta.fks.get(table, []):
            fk_label = (
                f"{table}.{','.join(fk.child_cols)}->"
                f"{fk.parent_table}.{','.join(fk.parent_cols)}"
            )
            child_actual = [resolve(df, c) for c in fk.child_cols]
            if any(a is None for a in child_actual):
                continue  # FK columns not all present in the output
            if skip_fk:
                append_shared_key_check(table, df, fk, child_actual)
                continue

            not_null_all = reduce(lambda a, b: a & b,
                                  [F.col(a).isNotNull() for a in child_actual])
            child_keys = df.where(not_null_all).select(
                *[canon(F.col(a).cast("string")).alias(f"k{i}")
                  for i, a in enumerate(child_actual)]
            ).dropDuplicates()

            # Stage 1: resolve against the synthetic parent, when present.
            pdf = tables.get(fk.parent_table)
            residual_df = child_keys
            if pdf is not None:
                parent_actual = [resolve(pdf, pc) for pc in fk.parent_cols]
                if all(parent_actual):
                    pkeys = pdf.select(
                        *[canon(F.col(a).cast("string")).alias(f"k{i}")
                          for i, a in enumerate(parent_actual)]
                    ).dropna().dropDuplicates()
                    residual_df = child_keys.join(
                        pkeys, [f"k{i}" for i in range(len(child_actual))], "left_anti")

            residual_started = perf_counter()
            residual = [tuple(r[f"k{i}"] for i in range(len(child_actual)))
                        for r in residual_df.limit(max_residual_keys + 1).collect()]
            logger.info(
                "[PERF] fk synthetic-residual %s keys=%d elapsed=%.1fs",
                fk_label, len(residual), perf_counter() - residual_started,
            )

            # Stage 2: push the residual into Oracle (union mode).
            note = ""
            if residual and validate_against == "union" and cfg.jdbc_url:
                if len(residual) > max_residual_keys:
                    out.append(Finding(
                        "3.fk_unresolved", "Referential integrity", SEV_WARN, table,
                        False, column=",".join(fk.child_cols),
                        hint="Raise --max-residual-keys or check this FK offline.",
                        message=f"FK {table}.{list(fk.child_cols)} -> {fk.parent_table}: "
                                f"more than {max_residual_keys} keys unresolved against "
                                f"the synthetic output; Oracle lookup skipped.",
                    ))
                    continue
                try:
                    oracle_started = perf_counter()
                    oracle_input_count = len(residual)
                    found = _residual_in_oracle(spark, cfg, fk, residual)
                    logger.info(
                        "[PERF] fk oracle-residual %s input_keys=%d batches=%d "
                        "found=%d elapsed=%.1fs",
                        fk_label,
                        oracle_input_count,
                        (oracle_input_count + 999) // 1000,
                        len(found),
                        perf_counter() - oracle_started,
                    )
                    residual = [t for t in residual if t not in found]
                    note = " (checked against synthetic ∪ Oracle)"
                    # Oracle-verified misses on a single-column FK feed the
                    # generator's domain pruning (--faltantes-parquet).
                    if residual and len(fk.child_cols) == 1 and not skip_orphan:
                        faltantes.extend(
                            (table, fk.child_cols[0], t[0]) for t in residual)
                except Exception as exc:  # noqa: BLE001
                    out.append(Finding(
                        "3.fk_unresolved", "Referential integrity", SEV_WARN, table,
                        False, column=",".join(fk.child_cols),
                        hint="Oracle residual lookup failed; FK not fully verified.",
                        message=f"FK {table}.{list(fk.child_cols)} -> {fk.parent_table}: "
                                f"{exc}",
                    ))
                    continue
            elif residual and pdf is None:
                out.append(Finding(
                    "3.fk_unresolved", "Referential integrity", SEV_WARN, table, False,
                    column=",".join(fk.child_cols),
                    hint="Parent absent from output and no Oracle connection "
                         "(use --validate-against union).",
                    message=f"FK {table}.{list(fk.child_cols)} -> {fk.parent_table}: "
                            f"{len(residual)} key(s) unverifiable.",
                ))
                continue

            c = len(residual)
            if not skip_orphan:
                out.append(Finding(
                    "3.fk_orphan", "Referential integrity",
                    SEV_ERROR if c else SEV_INFO, table, c == 0, count=c,
                    column=",".join(fk.child_cols),
                    sample=[t[0] if len(t) == 1 else list(t) for t in residual[:sample]],
                    hint="Orphan FK: child value not present in the synthetic output nor in "
                         "the target Oracle. Check FK remap / fecho / null_orphan_fks.",
                    message=f"FK {table}.{list(fk.child_cols)} -> "
                            f"{fk.parent_table}.{list(fk.parent_cols)}{note}",
                ))

            # Shared-key 1:1 cardinality (PK == FK).
            append_shared_key_check(table, df, fk, child_actual)
    return out, faltantes


def emit_faltantes(spark: SparkSession, path: str,
                   faltantes: List[Tuple[str, str, str]]) -> None:
    """Write the Oracle-verified orphan keys as a TABELA/COLUNA/VALOR Parquet —
    the input of engorda_instrumentos.py --faltantes-parquet, which prunes the
    sampling domain of instruments whose cluster references these keys.

    ACCUMULATES across runs: keys already at `path` stay and only new keys are
    appended. A key that leaves the list lets the instruments referencing it
    re-enter the generator's sampling domain, which made the prune/regenerate
    loop non-convergent (2026-07-25). Existing keys are never collected to the
    driver, so a large pre-enumerated file (scripts/enumerate_faltantes.py) at
    `path` is fine."""
    if not faltantes:
        logger.info("No new Oracle-verified orphan keys; faltantes parquet at %s "
                    "left untouched.", path)
        return
    new_df = spark.createDataFrame(faltantes, ["TABELA", "COLUNA", "VALOR"]).dropDuplicates()
    prev = None
    try:
        prev = spark.read.parquet(path).select(
            F.col("TABELA").cast("string").alias("TABELA"),
            F.col("COLUNA").cast("string").alias("COLUNA"),
            F.col("VALOR").cast("string").alias("VALOR"),
        )
        n_prev = prev.count()
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if not any(s in msg for s in ("Path does not exist", "PATH_NOT_FOUND",
                                      "Unable to infer schema", "FileNotFound")):
            raise
        n_prev = 0
    if prev is not None:
        # Keep only truly-new keys, materialized on the driver BEFORE writing:
        # appending output of a plan that reads `path` to `path` itself is unsafe.
        rows = [tuple(r) for r in
                new_df.join(prev, ["TABELA", "COLUNA", "VALOR"], "left_anti").collect()]
        if not rows:
            logger.info("All orphan key(s) already present in %s (%d total); "
                        "nothing to append.", path, n_prev)
            return
        new_df = spark.createDataFrame(rows, ["TABELA", "COLUNA", "VALOR"])
    n_new = new_df.count()
    new_df.coalesce(1).write.mode("append" if prev is not None else "overwrite").parquet(path)
    logger.info("Faltantes parquet at %s: %d new key(s) appended (%d total); rerun "
                "the generator with --faltantes-parquet %s",
                path, n_new, n_prev + n_new, path)


# ---------------------------------------------------------------------------
# Category 4 - NOT NULL incl. empty string (ORA-01400)
# ---------------------------------------------------------------------------
def check_not_null(tables: Dict[str, DataFrame], meta: Metadata, sample: int) -> List[Finding]:
    out: List[Finding] = []
    for table, df in tables.items():
        nn = meta.not_null.get(table) or set()
        if not nn:
            continue
        fk_cols = {c for fk in meta.fks.get(table, []) for c in fk.child_cols}
        pk_cols = _pk_cols_for(meta, table, df)
        types = meta.col_type.get(table, {})
        for col_upper in sorted(nn):
            actual = resolve(df, col_upper)
            if not actual:
                continue
            c = F.col(actual)
            if _is_string_type(types.get(col_upper, "")):
                eff_null = c.isNull() | (F.trim(c) == F.lit(""))
                note = " (NULL or empty string; Oracle stores '' as NULL)"
            else:
                eff_null = c.isNull()
                note = ""
            bad = df.where(eff_null)
            cnt = bad.count()
            is_fk = col_upper in fk_cols
            if cnt:
                hint = (
                    "NOT NULL FK column left null -> the rebind/fecho pass should have "
                    "resolved it (parent without synthetic key? cycle edge?)."
                    if is_fk else
                    "NOT NULL non-FK column left null -> no pipeline pass generates it; "
                    "it was born null in synthesis/bootstrap/postprocess or came null "
                    "from source. Populate it "
                    "in generation (this is the ORA-01400 class, e.g. COD_MOTIVO)."
                )
            else:
                hint = ""
            out.append(Finding(
                "4.not_null_fk" if is_fk else "4.not_null_nonfk",
                "NOT NULL (incl. empty string)",
                SEV_ERROR if cnt else SEV_INFO, table, cnt == 0, count=cnt, column=col_upper,
                sample=_sample_keys(bad, pk_cols, sample),
                hint=hint,
                message=f"{table}.{col_upper} NOT NULL violated{note}.",
            ))
    return out


# ---------------------------------------------------------------------------
# Category 5 - Date coherence
# ---------------------------------------------------------------------------
def check_dates(tables: Dict[str, DataFrame], meta: Metadata, sample: int) -> List[Finding]:
    out: List[Finding] = []
    for table, lcol, op, rcol in DATE_RULES:
        df = tables.get(table)
        if df is None:
            continue
        la, ra = resolve(df, lcol), resolve(df, rcol)
        if not la or not ra:
            continue
        left = F.to_date(F.col(la))
        right = F.to_date(F.col(ra))
        both = left.isNotNull() & right.isNotNull()
        violate = (left > right) if op == "<=" else (left >= right)
        bad = df.where(both & violate)
        c = bad.count()
        out.append(Finding(
            "5.date_order", "Date coherence",
            SEV_ERROR if c else SEV_INFO, table, c == 0, count=c, column=f"{lcol}{op}{rcol}",
            sample=_sample_keys(bad, _pk_cols_for(meta, table, df), sample),
            hint="Check the engorda date rules (_apply_engorda_business_rules) / "
                 "prazo de vencimento.",
            message=f"Rows where NOT ({lcol} {op} {rcol}).",
        ))
    return out


# ---------------------------------------------------------------------------
# Category 6 - Lookup combinations (SEM MODALIDADE)
# ---------------------------------------------------------------------------
def check_required_lookup_frames(
    tables: Dict[str, DataFrame],
    account_df: Optional[DataFrame],
    tos_df: Optional[DataFrame],
    tipo_df: Optional[DataFrame],
    cdb_object_df: Optional[DataFrame],
    sample: int,
    profile: Optional["ValidationProfile"] = None,
    lookup_errors: Optional[Dict[str, str]] = None,
    skip_prefixes: Optional[List[str]] = None,
) -> List[Finding]:
    """Enforce the mandatory target-backed registration lookups for the selected product.

    The operation-TOS check classifies registration operations through the target operation
    type and requires registration coverage per synthetic root. Historical operations remain
    subject to the generic FK checks. Account and platform checks are product-gated: when the
    profile disables them (unresolved target evidence, e.g. RDB), an explicit unsupported WARN
    is emitted instead of a CDB-shaped ERROR."""
    if profile is None:
        profile = CDB_SIMPLIFICADO_PROFILE
    skip_prefixes = skip_prefixes or []
    run_account = not _check_is_skipped("6.required.active_account", skip_prefixes)
    run_operation = not _check_is_skipped("6.required.operation_tos", skip_prefixes)
    run_platform = not _check_is_skipped("6.required.cdb_platform", skip_prefixes)
    cat = "Required lookup combinations"
    errors = dict(lookup_errors or {})
    lookup_hint = (
        "Check Oracle JDBC credentials/schema, SELECT grants, target columns, and target "
        "view/table availability, then rerun against QAB."
    )

    def target_columns(
        df: Optional[DataFrame], table: str, required: List[str]
    ) -> Optional[Dict[str, str]]:
        if df is None:
            errors.setdefault(table, "lookup was not loaded")
            return None
        resolved = {name: resolve(df, name) for name in required}
        missing = [name for name, actual in resolved.items() if not actual]
        if missing:
            errors[table] = f"missing column(s): {', '.join(missing)}"
            return None
        return {name: actual for name, actual in resolved.items() if actual}

    missing_refs = []
    account_parts = []
    for table, column in ACCOUNT_REFERENCES:
        source = tables.get(table)
        actual = resolve(source, column) if source is not None else None
        if source is None or actual is None:
            missing_refs.append(f"{table}.{column}")
            continue
        account_parts.append(
            source.select(
                F.lit(table).alias("source_table"),
                F.lit(column).alias("source_column"),
                F.col(actual).cast("string").alias("raw_account"),
                _canon_key_col(F.col(actual)).alias("account_id"),
            )
        )

    if not run_account:
        account_finding = None
    elif missing_refs:
        account_finding = Finding(
            "6.required.active_account", cat, SEV_ERROR, CONTA_PARTICIPANTE_TABLE, False,
            count=len(missing_refs), column=",".join(missing_refs), sample=missing_refs[:sample],
            hint="Export all four approved synthetic account reference columns; do not "
                 "substitute or broaden the check to other account columns.",
            message="Required synthetic account source table/column(s) are missing: "
                    f"{', '.join(missing_refs)}.",
        )
    else:
        accounts = reduce(lambda left, right: left.unionByName(right), account_parts)
        blank_accounts = accounts.where(
            F.col("raw_account").isNotNull() & (F.trim(F.col("raw_account")) == "")
        )
        nonblank_accounts = accounts.where(
            F.col("raw_account").isNotNull() & (F.trim(F.col("raw_account")) != "")
        )
        if account_df is None and nonblank_accounts.limit(1).count() == 0:
            account_df = accounts.sparkSession.createDataFrame(
                [],
                "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
                "COD_CONTA_PARTICIPANTE string, NUM_ID_AREA_ATUACAO string, "
                "COD_TIPO_ACESSO string",
            )
        account_cols = target_columns(
            account_df,
            CONTA_PARTICIPANTE_TABLE,
            [
                "NUM_CONTA_PARTICIPANTE",
                "NUM_ID_SITUACAO_CONTA",
                "COD_CONTA_PARTICIPANTE",
                "NUM_ID_AREA_ATUACAO",
                "COD_TIPO_ACESSO",
            ],
        )
        if account_cols is None:
            account_finding = Finding(
                "6.required.active_account", cat, SEV_ERROR,
                CONTA_PARTICIPANTE_TABLE, False,
                sample=_sample_keys(
                    accounts.where(F.col("raw_account").isNotNull()),
                    ["source_table", "source_column", "account_id"],
                    sample,
                ),
                hint=lookup_hint,
                message="Required active/local account check unavailable: "
                        f"{CONTA_PARTICIPANTE_TABLE} "
                        f"{errors[CONTA_PARTICIPANTE_TABLE]}.",
            )
        else:
            eligible_accounts = (
                account_df.select(
                    _canon_key_col(F.col(account_cols["NUM_CONTA_PARTICIPANTE"]))
                    .alias("account_id"),
                    _canon_key_col(F.col(account_cols["NUM_ID_SITUACAO_CONTA"]))
                    .alias("situation_id"),
                    F.trim(
                        F.col(account_cols["COD_CONTA_PARTICIPANTE"]).cast("string")
                    )
                    .alias("account_code"),
                    _canon_key_col(F.col(account_cols["NUM_ID_AREA_ATUACAO"]))
                    .alias("area_id"),
                    F.col(account_cols["COD_TIPO_ACESSO"]).cast("string")
                    .alias("access_type"),
                )
                .where(
                    (F.col("situation_id") == "1")
                    & F.col("account_code").rlike(r"^[0-9]{5}\.(40|10)-[0-9]$")
                    & (F.col("area_id") == "1")
                    & (F.col("access_type") == "L")
                )
                .select("account_id")
                .where(F.col("account_id").isNotNull())
                .dropDuplicates()
            )
            invalid_accounts = blank_accounts.select(
                "source_table", "source_column", "account_id"
            ).unionByName(
                nonblank_accounts.join(
                    F.broadcast(eligible_accounts), "account_id", "left_anti"
                ).select("source_table", "source_column", "account_id")
            )
            invalid_account_count = invalid_accounts.count()
            account_finding = Finding(
                "6.required.active_account", cat,
                SEV_ERROR if invalid_account_count else SEV_INFO,
                CONTA_PARTICIPANTE_TABLE, invalid_account_count == 0,
                count=invalid_account_count,
                column=",".join(f"{table}.{column}" for table, column in ACCOUNT_REFERENCES),
                sample=_sample_keys(
                    invalid_accounts,
                    ["source_table", "source_column", "account_id"],
                    sample,
                ),
                hint=(
                    "Use a nonblank target CONTA_PARTICIPANTE with "
                    "NUM_ID_SITUACAO_CONTA=1 whose COD_CONTA_PARTICIPANTE has a "
                    "V_FAMILIA_CONTAS row with NUM_ID_AREA_ATUACAO=1 and COD_TIPO_ACESSO='L'. "
                    "The trimmed account code must match ^[0-9]{5}\\.(40|10)-[0-9]$; "
                    "situation 2 is not eligible."
                    if invalid_account_count else ""
                ),
                message="Synthetic account references must resolve to an active local-access "
                        "target account whose trimmed code has the required .40/.10 shape.",
            )

    tos_semantics_supported = CAP_LOOKUP_TOS in profile.supported_capabilities
    # Do not evaluate RDB rows with CDB's identification='S'/operation-type='1' literals.
    op_df = tables.get(OPERACAO_TABLE) if tos_semantics_supported else None
    op_tos_col = resolve(op_df, "NUM_ID_TIPO_OPER_OBJETO_SERV") if op_df is not None else None
    op_num_if_col = resolve(op_df, "NUM_IF") if op_df is not None else None
    root_df = tables.get("INSTRUMENTO_FINANCEIRO") if tos_semantics_supported else None
    root_num_if_col = resolve(root_df, "NUM_IF") if root_df is not None else None
    if not run_operation:
        operation_finding = None
    elif (
        op_df is None
        or op_tos_col is None
        or op_num_if_col is None
        or root_df is None
        or root_num_if_col is None
    ):
        missing = []
        if op_df is None:
            missing.append(OPERACAO_TABLE)
        else:
            if op_tos_col is None:
                missing.append(f"{OPERACAO_TABLE}.NUM_ID_TIPO_OPER_OBJETO_SERV")
            if op_num_if_col is None:
                missing.append(f"{OPERACAO_TABLE}.NUM_IF")
        if root_df is None:
            missing.append("INSTRUMENTO_FINANCEIRO")
        elif root_num_if_col is None:
            missing.append("INSTRUMENTO_FINANCEIRO.NUM_IF")
        operation_finding = Finding(
            "6.required.operation_tos", cat, SEV_ERROR, OPERACAO_TABLE, False,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            sample=(
                _sample_keys(op_df, [resolve(op_df, "NUM_ID_OPERACAO")], sample)
                if op_df is not None and resolve(op_df, "NUM_ID_OPERACAO") else []
            ),
            hint="Export OPERACAO.NUM_IF and NUM_ID_TIPO_OPER_OBJETO_SERV together with "
                 "INSTRUMENTO_FINANCEIRO.NUM_IF so registration coverage can be checked.",
            message=f"Required registration-operation source is missing: {', '.join(missing)}.",
        )
    else:
        tos_cols = target_columns(
            tos_df,
            TIPO_OPER_OBJETO_SERV_TABLE,
            [
                "NUM_ID_TIPO_OPER_OBJETO_SERV",
                "NUM_ID_TIPO_OPERACAO",
                "NUM_ID_OBJETO_SERVICO",
                "IND_DISPONIVEL_IDENTIFICACAO",
            ],
        )
        tipo_cols = target_columns(
            tipo_df,
            TIPO_OPERACAO_TABLE,
            ["NUM_ID_TIPO_OPERACAO", "COD_TIPO_OPERACAO"],
        )
        unavailable = [
            f"{table} {errors[table]}"
            for table, columns in (
                (TIPO_OPER_OBJETO_SERV_TABLE, tos_cols),
                (TIPO_OPERACAO_TABLE, tipo_cols),
            )
            if columns is None
        ]
        if unavailable:
            operation_finding = Finding(
                "6.required.operation_tos", cat, SEV_ERROR, OPERACAO_TABLE, False,
                sample=_sample_keys(
                    op_df,
                    [resolve(op_df, "NUM_ID_OPERACAO")]
                    if resolve(op_df, "NUM_ID_OPERACAO") else [op_tos_col],
                    sample,
                ),
                hint=lookup_hint,
                message="Required operation TOS check unavailable: "
                        f"{'; '.join(unavailable)}.",
            )
        else:
            operations = op_df.select(
                _norm_code(F.col(op_num_if_col)).alias("num_if"),
                F.col(op_tos_col).cast("string").alias("raw_tos_id"),
                _canon_key_col(F.col(op_tos_col)).alias("tos_id"),
            )
            tos_semantics = (
                tos_df.select(
                    _canon_key_col(F.col(tos_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"]))
                    .alias("tos_id"),
                    _canon_key_col(F.col(tos_cols["NUM_ID_TIPO_OPERACAO"]))
                    .alias("tipo_operacao_id"),
                    _canon_key_col(F.col(tos_cols["NUM_ID_OBJETO_SERVICO"]))
                    .alias("objeto_servico_id"),
                    F.trim(
                        F.col(tos_cols["IND_DISPONIVEL_IDENTIFICACAO"]).cast("string")
                    ).alias("identification_flag"),
                )
                .join(
                    tipo_df.select(
                        _canon_key_col(F.col(tipo_cols["NUM_ID_TIPO_OPERACAO"]))
                        .alias("tipo_operacao_id"),
                        F.col(tipo_cols["COD_TIPO_OPERACAO"]).cast("string")
                        .alias("operation_type_code"),
                    ),
                    "tipo_operacao_id",
                    "inner",
                )
                .where(F.col("tos_id").isNotNull())
                .dropDuplicates(["tos_id"])
            )
            registration_operations = (
                operations.where(
                    F.col("raw_tos_id").isNotNull()
                    & (F.trim(F.col("raw_tos_id")) != "")
                )
                .join(F.broadcast(tos_semantics), "tos_id", "inner")
                .where(F.col("operation_type_code") == "1")
            )
            valid_registration_ifs = (
                registration_operations.where(
                    (F.col("objeto_servico_id") == str(profile.object_service_id))
                    & (F.col("identification_flag") == "S")
                )
                .select("num_if")
                .where(F.col("num_if").isNotNull())
                .dropDuplicates()
            )
            invalid_registration_ifs = (
                registration_operations.where(
                    (
                        F.coalesce(F.col("objeto_servico_id"), F.lit(""))
                        != str(profile.object_service_id)
                    )
                    | (F.coalesce(F.col("identification_flag"), F.lit("")) != "S")
                )
                .select("num_if")
                .where(F.col("num_if").isNotNull())
            )
            active_roots = (
                _active(root_df)
                .select(_norm_code(F.col(root_num_if_col)).alias("num_if"))
                .where(F.col("num_if").isNotNull())
                .dropDuplicates()
            )
            missing_registration_ifs = active_roots.join(
                F.broadcast(valid_registration_ifs), "num_if", "left_anti"
            )
            invalid_ifs = (
                invalid_registration_ifs.unionByName(missing_registration_ifs)
                .dropDuplicates(["num_if"])
            )
            invalid_if_count = invalid_ifs.count()
            operation_finding = Finding(
                "6.required.operation_tos", cat,
                SEV_ERROR if invalid_if_count else SEV_INFO,
                OPERACAO_TABLE, invalid_if_count == 0,
                count=invalid_if_count,
                column="NUM_IF,NUM_ID_TIPO_OPER_OBJETO_SERV",
                sample=_sample_keys(invalid_ifs, ["num_if"], sample),
                hint=(
                    "Ensure every active synthetic root has a registration operation whose "
                    "target TOS joins to TIPO_OPERACAO.COD_TIPO_OPERACAO='1', "
                    f"NUM_ID_OBJETO_SERVICO={profile.object_service_id}, and trimmed "
                    "IND_DISPONIVEL_IDENTIFICACAO='S'. Historical operation types are allowed."
                    if invalid_if_count else ""
                ),
                message="Every synthetic root must have an approved registration-operation "
                        "TOS; historical operation types are not constrained by this check.",
            )

    if run_operation and not tos_semantics_supported:
        operation_finding = Finding(
            "6.required.operation_tos", cat, SEV_WARN, OPERACAO_TABLE, False,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            hint="Capture RDB TIPO_OPER_OBJETO_SERV rows and their operation type and "
                 "identification flags before enabling this check; do not reuse CDB literals.",
            message=f"TOS operation type and identification semantics are not validated for "
                    f"product {profile.name} (unresolved target evidence).",
        )

    cdb_cols = (
        target_columns(
            cdb_object_df,
            V_OBJETOS_SERVICO_TABLE,
            ["COD_OBJETO_SERVICO", "IND_PLATAFORMA_BAIXA"],
        )
        if run_platform else None
    )
    if not run_platform:
        platform_finding = None
    elif cdb_cols is None:
        platform_finding = Finding(
            "6.required.cdb_platform", cat, SEV_ERROR, V_OBJETOS_SERVICO_TABLE, False,
            hint=lookup_hint,
            message="Required CDB platform check unavailable: "
                    f"{V_OBJETOS_SERVICO_TABLE} {errors[V_OBJETOS_SERVICO_TABLE]}.",
        )
    else:
        eligible_cdb_count = cdb_object_df.where(
            (F.col(cdb_cols["COD_OBJETO_SERVICO"]).cast("string") == "CDB")
            & (
                F.trim(F.col(cdb_cols["IND_PLATAFORMA_BAIXA"]).cast("string"))
                == "S"
            )
        ).limit(1).count()
        platform_finding = Finding(
            "6.required.cdb_platform", cat,
            SEV_INFO if eligible_cdb_count else SEV_ERROR,
            V_OBJETOS_SERVICO_TABLE, bool(eligible_cdb_count),
            count=0 if eligible_cdb_count else 1,
            column="COD_OBJETO_SERVICO,IND_PLATAFORMA_BAIXA",
            hint=(
                "Ensure target V_OBJETOS_SERVICO exposes COD_OBJETO_SERVICO='CDB' with "
                "trimmed IND_PLATAFORMA_BAIXA='S'."
                if not eligible_cdb_count else ""
            ),
            message="Target CDB object service must be enabled for the baixa platform.",
        )

    if run_account and not profile.account_check_enabled:
        account_finding = Finding(
            "6.required.active_account", cat, SEV_WARN, CONTA_PARTICIPANTE_TABLE, False,
            hint="Capture the RDB/target account-eligibility rule before enabling this check; "
                 "do not reuse the CDB situacao/access/area/code literals.",
            message=f"Account eligibility not validated for product {profile.name} "
                    "(unresolved evidence).",
        )
    if run_platform and not (profile.platform_check_enabled and profile.object_service_code):
        platform_finding = Finding(
            "6.required.cdb_platform", cat, SEV_WARN, V_OBJETOS_SERVICO_TABLE, False,
            hint="Capture the target object-service platform code/flag for this product "
                 "before enabling this check.",
            message=f"Object-service platform not validated for product {profile.name} "
                    "(unresolved COD_OBJETO_SERVICO/IND_PLATAFORMA_BAIXA).",
        )
    return [
        finding for finding in (account_finding, operation_finding, platform_finding)
        if finding is not None
    ]


def check_lookup_combo_frames(
    op_df: DataFrame,
    tos_df: Optional[DataFrame],
    sic_df: Optional[DataFrame],
    tipo_operacao_df: Optional[DataFrame],
    sample: int,
    profile: Optional["ValidationProfile"] = None,
    lookup_errors: Optional[Dict[str, str]] = None,
    skip_prefixes: Optional[List[str]] = None,
) -> List[Finding]:
    """Compare synthetic operations with already-loaded Oracle lookup rows."""
    if profile is None:
        profile = CDB_SIMPLIFICADO_PROFILE
    skip_prefixes = skip_prefixes or []
    run_tos = not _check_is_skipped("6.combo.tos_fk", skip_prefixes)
    run_compatibility = not _check_is_skipped("6.combo.cdb_compatibility", skip_prefixes)
    run_sem_modalidade = not _check_is_skipped("6.combo.sem_modalidade", skip_prefixes)
    run_identification = not _check_is_skipped(
        "6.combo.identification_availability", skip_prefixes
    )
    if not any((run_tos, run_compatibility, run_sem_modalidade, run_identification)):
        return []
    cat = "Lookup combinations"
    if CAP_LOOKUP_TOS not in profile.supported_capabilities:
        return [Finding(
            "6.combo.unsupported", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Capture RDB TOS, operation-type, identification, and modalidade rows before "
                 "enabling lookup-combination validation.",
            message=f"Lookup-combination semantics are unsupported for product {profile.name}; "
                    "CDB literals were not evaluated.",
        )]
    errors = dict(lookup_errors or {})
    tos_col = resolve(op_df, "NUM_ID_TIPO_OPER_OBJETO_SERV")
    mod_col = resolve(op_df, "NUM_ID_MODALIDADE_LIQUIDACAO")
    op_id_col = resolve(op_df, "NUM_ID_OPERACAO")
    missing_op_cols = [
        name
        for name, actual in (
            ("NUM_ID_TIPO_OPER_OBJETO_SERV", tos_col),
            ("NUM_ID_MODALIDADE_LIQUIDACAO", mod_col),
        )
        if not actual
    ]
    if missing_op_cols:
        return [Finding(
            "6.combo.required_columns", cat, SEV_WARN, OPERACAO_TABLE, False,
            column=",".join(missing_op_cols),
            hint="Regenerate/export OPERACAO with its physical "
                 "NUM_ID_TIPO_OPER_OBJETO_SERV and NUM_ID_MODALIDADE_LIQUIDACAO columns; "
                 "do not invent NUM_ID_TIPO_OPERACAO on OPERACAO.",
            message="OPERACAO lacks required Category 6 lookup column(s): "
                    f"{', '.join(missing_op_cols)}.",
        )]

    select_cols = [
        _norm_code(F.col(tos_col)).alias("tos_id"),
        _norm_code(F.col(mod_col)).alias("modalidade_id"),
    ]
    if op_id_col:
        select_cols.insert(0, _norm_code(F.col(op_id_col)).alias("operation_id"))
    operations = op_df.select(*select_cols).where(
        F.col("tos_id").isNotNull() & (F.col("tos_id") != "")
    )
    sample_cols = ["operation_id"] if op_id_col else ["tos_id", "modalidade_id"]

    def target_columns(
        df: Optional[DataFrame], table: str, required: List[str]
    ) -> Optional[Dict[str, str]]:
        if df is None:
            errors.setdefault(table, "lookup was not loaded")
            return None
        resolved = {name: resolve(df, name) for name in required}
        missing = [name for name, actual in resolved.items() if not actual]
        if missing:
            errors[table] = f"missing column(s): {', '.join(missing)}"
            return None
        return {name: actual for name, actual in resolved.items() if actual}

    tos_cols = target_columns(
        tos_df,
        TIPO_OPER_OBJETO_SERV_TABLE,
        [
            "NUM_ID_TIPO_OPER_OBJETO_SERV",
            "NUM_ID_TIPO_OPERACAO",
            "NUM_ID_OBJETO_SERVICO",
            "IND_DISPONIVEL_IDENTIFICACAO",
        ],
    )
    sic_cols = (
        target_columns(
            sic_df,
            V_PARAMETRO_SIC_TABLE,
            ["NUM_ID_TIPO_OPER_OBJETO_SERV", "NUM_TIPO_IF", "NUM_ID_OBJETO_SERVICO"],
        )
        if run_compatibility else None
    )
    tipo_cols = (
        target_columns(
            tipo_operacao_df,
            TIPO_OPERACAO_TABLE,
            ["NUM_ID_TIPO_OPERACAO", "IND_SEM_MODALIDADE_INFOHUB"],
        )
        if run_sem_modalidade else None
    )

    if tos_cols is None:
        reason = errors[TIPO_OPER_OBJETO_SERV_TABLE]
        lookup_hint = (
            "Check Oracle JDBC credentials/schema, SELECT grants, and target view/table "
            "availability, then rerun against QAB."
        )
        return [
            Finding(
                check_id, cat, SEV_WARN, OPERACAO_TABLE, False,
                hint=lookup_hint,
                message=f"{message} unavailable: {TIPO_OPER_OBJETO_SERV_TABLE} {reason}.",
            )
            for check_id, message in (
                ("6.combo.tos_fk", "TOS structural check"),
                ("6.combo.cdb_compatibility", "CDB compatibility check"),
                ("6.combo.sem_modalidade", "Sem-modalidade check"),
                ("6.combo.identification_availability", "Identification availability check"),
            )
            if not _check_is_skipped(check_id, skip_prefixes)
        ]

    tos = tos_df.select(
        _norm_code(F.col(tos_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"])).alias("tos_id"),
        _norm_code(F.col(tos_cols["NUM_ID_TIPO_OPERACAO"])).alias("tipo_operacao_id"),
        _norm_code(F.col(tos_cols["NUM_ID_OBJETO_SERVICO"])).alias("objeto_servico_id"),
        F.trim(F.col(tos_cols["IND_DISPONIVEL_IDENTIFICACAO"]).cast("string"))
        .alias("identificacao_flag"),
    ).where(F.col("tos_id").isNotNull()).dropDuplicates(["tos_id"])

    out: List[Finding] = []
    if run_tos:
        missing_tos = operations.join(F.broadcast(tos.select("tos_id")), "tos_id", "left_anti")
        missing_tos_count = missing_tos.count()
        out.append(Finding(
            "6.combo.tos_fk", cat, SEV_ERROR if missing_tos_count else SEV_INFO,
            OPERACAO_TABLE, missing_tos_count == 0, count=missing_tos_count,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            sample=_sample_keys(missing_tos, sample_cols, sample),
            hint="Preserve or recover the transaction's exact static TOS FK, or prune source "
                 "operations unsupported by the target; otherwise ask the QAB configuration "
                 "owner to seed that exact mapping. Do not bind to an arbitrary valid TOS row.",
            message=f"Synthetic non-null TOS IDs absent from target "
                    f"{TIPO_OPER_OBJETO_SERV_TABLE}.",
        ))

    resolved_ops = operations.join(F.broadcast(tos), "tos_id", "inner")

    if run_compatibility and sic_cols is None:
        out.append(Finding(
            "6.combo.cdb_compatibility", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Check Oracle JDBC credentials/schema, SELECT grants, and target view/table "
                 "availability, then rerun against QAB.",
            message=f"CDB compatibility check unavailable: {V_PARAMETRO_SIC_TABLE} "
                    f"{errors[V_PARAMETRO_SIC_TABLE]}.",
        ))
    elif run_compatibility:
        valid_cdb_tos = (
            sic_df.select(
                _norm_code(F.col(sic_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"]))
                .alias("tos_id"),
                _norm_code(F.col(sic_cols["NUM_TIPO_IF"])).alias("tipo_if"),
                _norm_code(F.col(sic_cols["NUM_ID_OBJETO_SERVICO"]))
                .alias("objeto_servico_id"),
            )
            .where(
                (F.col("tipo_if") == str(profile.num_tipo_if))
                & (F.col("objeto_servico_id") == str(profile.object_service_id))
            )
            .select("tos_id")
            .where(F.col("tos_id").isNotNull())
            .dropDuplicates()
        )
        incompatible = resolved_ops.join(F.broadcast(valid_cdb_tos), "tos_id", "left_anti")
        incompatible_count = incompatible.count()
        out.append(Finding(
            "6.combo.cdb_compatibility", cat,
            SEV_WARN if incompatible_count else SEV_INFO,
            OPERACAO_TABLE, incompatible_count == 0, count=incompatible_count,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            sample=_sample_keys(incompatible, sample_cols, sample),
            hint=f"Use or preserve a TOS mapping exposed by target {V_PARAMETRO_SIC_TABLE} "
                 f"for NUM_TIPO_IF={profile.num_tipo_if} and "
                 f"NUM_ID_OBJETO_SERVICO={profile.object_service_id}, or prune unsupported "
                 "source operations. Otherwise align the underlying QAB configuration; "
                 f"{V_PARAMETRO_SIC_TABLE} is a view and must not be updated directly.",
            message=f"Resolved operation TOS mappings incompatible with {profile.name} "
                    "service configuration.",
        ))

    if run_sem_modalidade and profile.sem_modalidade_ids is None:
        out.append(Finding(
            "6.combo.sem_modalidade", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Confirm the sem-modalidade IDs for this product before enabling the check; "
                 "do not reuse the CDB IDs.",
            message=f"Sem-modalidade check not validated for product {profile.name} "
                    "(unresolved modalidade IDs).",
        ))
    elif run_sem_modalidade and tipo_cols is None:
        out.append(Finding(
            "6.combo.sem_modalidade", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Check Oracle JDBC credentials/schema, SELECT grants, and target view/table "
                 "availability, then rerun against QAB.",
            message=f"Sem-modalidade check unavailable: {TIPO_OPERACAO_TABLE} "
                    f"{errors[TIPO_OPERACAO_TABLE]}.",
        ))
    elif run_sem_modalidade:
        tipo_operacao = tipo_operacao_df.select(
            _norm_code(F.col(tipo_cols["NUM_ID_TIPO_OPERACAO"]))
            .alias("tipo_operacao_id"),
            F.trim(F.col(tipo_cols["IND_SEM_MODALIDADE_INFOHUB"]).cast("string"))
            .alias("sem_modalidade_flag"),
        ).dropDuplicates(["tipo_operacao_id"])
        sem_modalidade = resolved_ops.where(
            F.col("modalidade_id").isin(*[str(value) for value in profile.sem_modalidade_ids])
        ).join(F.broadcast(tipo_operacao), "tipo_operacao_id", "left")
        invalid_sem_modalidade = sem_modalidade.where(
            F.coalesce(F.col("sem_modalidade_flag"), F.lit("")) != "S"
        )
        invalid_sem_count = invalid_sem_modalidade.count()
        out.append(Finding(
            "6.combo.sem_modalidade", cat,
            SEV_WARN if invalid_sem_count else SEV_INFO,
            OPERACAO_TABLE, invalid_sem_count == 0, count=invalid_sem_count,
            column="NUM_ID_MODALIDADE_LIQUIDACAO,IND_SEM_MODALIDADE_INFOHUB",
            sample=_sample_keys(invalid_sem_modalidade, sample_cols, sample),
            hint=f"Do not renumber fixed modalidade IDs {profile.sem_modalidade_ids}. "
                 "Preserve a source-compatible modalidade/type pair, or ask the QAB "
                 "configuration owner to correct TIPO_OPERACAO.IND_SEM_MODALIDADE_INFOHUB "
                 "only when valid.",
            message="Sem-modalidade operation types are not enabled in TIPO_OPERACAO.",
        ))

    if run_identification:
        unavailable = resolved_ops.where(
            F.coalesce(F.col("identificacao_flag"), F.lit("")) != "S"
        )
        unavailable_count = unavailable.count()
        out.append(Finding(
            "6.combo.identification_availability", cat,
            SEV_WARN if unavailable_count else SEV_INFO,
            OPERACAO_TABLE, unavailable_count == 0, count=unavailable_count,
            column="IND_DISPONIVEL_IDENTIFICACAO",
            sample=_sample_keys(unavailable, sample_cols, sample),
            hint="Use or preserve a target CDB TOS row with "
                 "IND_DISPONIVEL_IDENTIFICACAO='S', or ask the QAB configuration owner to "
                 "align target static configuration; do not arbitrarily rewrite transaction "
                 "FKs.",
            message="Resolved TOS mappings are unavailable for identification.",
        ))
    return out


def check_lookup_combos(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], meta: Metadata, sample: int,
    max_account_keys: int = 1_000_000, profile: Optional["ValidationProfile"] = None,
    skip_prefixes: Optional[List[str]] = None,
) -> List[Finding]:
    if profile is None:
        profile = CDB_SIMPLIFICADO_PROFILE
    skip_prefixes = skip_prefixes or []
    run_combo = not _check_is_skipped("6.combo", skip_prefixes)
    run_required = not _check_is_skipped("6.required", skip_prefixes)
    run_required_account = run_required and not _check_is_skipped(
        "6.required.active_account", skip_prefixes
    )
    run_required_operation = run_required and not _check_is_skipped(
        "6.required.operation_tos", skip_prefixes
    )
    run_required_platform = run_required and not _check_is_skipped(
        "6.required.cdb_platform", skip_prefixes
    )
    if not (run_combo or run_required):
        return []
    op_df = tables.get(OPERACAO_TABLE)
    if not run_combo:
        existing = []
    elif op_df is None:
        existing = [Finding(
            "6.combo", "Lookup combinations", SEV_INFO, OPERACAO_TABLE, True,
            message="OPERACAO not in output; combo check skipped.",
        )]
    elif not cfg.jdbc_url:
        existing = [Finding(
            "6.combo.no_jdbc", "Lookup combinations", SEV_WARN, OPERACAO_TABLE, False,
            hint="Configure and verify Oracle JDBC credentials/schema, SELECT grants, and "
                  "target view/table availability, then rerun against QAB.",
            message="No Oracle connection; cannot resolve valid "
                    f"(tipo_operacao, modalidade, servico) combinations for {profile.name}.",
        )]
    else:
        existing = []

    lookup_tos_supported = CAP_LOOKUP_TOS in profile.supported_capabilities
    queries = {}
    if lookup_tos_supported and (run_combo or run_required_operation):
        queries.update({
            TIPO_OPER_OBJETO_SERV_TABLE: (
                "SELECT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_ID_TIPO_OPERACAO, "
                "NUM_ID_OBJETO_SERVICO, IND_DISPONIVEL_IDENTIFICACAO "
                f"FROM {cfg.schema}.{TIPO_OPER_OBJETO_SERV_TABLE}"
            ),
            TIPO_OPERACAO_TABLE: (
                "SELECT NUM_ID_TIPO_OPERACAO, IND_SEM_MODALIDADE_INFOHUB, COD_TIPO_OPERACAO "
                f"FROM {cfg.schema}.{TIPO_OPERACAO_TABLE}"
            ),
        })
    if run_combo and profile.sic_enabled:
        queries[V_PARAMETRO_SIC_TABLE] = (
            "SELECT DISTINCT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_TIPO_IF, "
            f"NUM_ID_OBJETO_SERVICO FROM {cfg.schema}.{V_PARAMETRO_SIC_TABLE} "
            f"WHERE NUM_TIPO_IF = {profile.num_tipo_if} "
            f"AND NUM_ID_OBJETO_SERVICO = {profile.object_service_id}"
        )
    if run_required_platform and profile.platform_check_enabled and profile.object_service_code:
        queries[V_OBJETOS_SERVICO_TABLE] = (
            "SELECT COD_OBJETO_SERVICO, IND_PLATAFORMA_BAIXA "
            f"FROM {cfg.schema}.{V_OBJETOS_SERVICO_TABLE} "
            f"WHERE COD_OBJETO_SERVICO = {_sql_literal(profile.object_service_code)}"
        )
    lookups: Dict[str, DataFrame] = {}
    errors: Dict[str, str] = {}
    if cfg.jdbc_url:
        for table, query in queries.items():
            try:
                remote = _jdbc(spark, cfg, query)
                # Detach small config lookups so later actions cannot lazily re-read JDBC.
                rows = remote.collect()
                lookups[table] = spark.createDataFrame(rows, remote.schema)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Category 6 lookup load failed for %s: %s", table, exc)
                errors[table] = str(exc)
    else:
        errors.update({table: "No Oracle connection" for table in queries})

    account_sources_available = run_required_account and profile.account_check_enabled and all(
        tables.get(table) is not None and resolve(tables[table], column) is not None
        for table, column in ACCOUNT_REFERENCES
    )
    account_keys: List[str] = []
    if account_sources_available:
        key_frames = [
            tables[table].select(
                _canon_key_col(F.col(resolve(tables[table], column))).alias("key")
            )
            for table, column in ACCOUNT_REFERENCES
        ]
        distinct_keys = reduce(lambda left, right: left.unionByName(right), key_frames).where(
            F.col("key").isNotNull() & (F.trim(F.col("key")) != "")
        ).dropDuplicates()
        account_keys = [
            _canon_key(row["key"])
            for row in distinct_keys.limit(max_account_keys + 1).collect()
        ]

    if account_sources_available and not account_keys:
        lookups[CONTA_PARTICIPANTE_TABLE] = spark.createDataFrame(
            [],
            "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
            "COD_CONTA_PARTICIPANTE string, NUM_ID_AREA_ATUACAO string, "
            "COD_TIPO_ACESSO string",
        )
    elif len(account_keys) > max_account_keys:
        errors[CONTA_PARTICIPANTE_TABLE] = (
            f"more than {max_account_keys} distinct synthetic account keys; lookup skipped"
        )
    elif account_keys and cfg.jdbc_url:
        account_rows = []
        account_schema = None
        try:
            for offset in range(0, len(account_keys), 1000):
                literals = ", ".join(
                    _sql_literal(value) for value in account_keys[offset:offset + 1000]
                )
                query = (
                    "SELECT cp.NUM_CONTA_PARTICIPANTE, cp.NUM_ID_SITUACAO_CONTA, "
                    "cp.COD_CONTA_PARTICIPANTE, vf.NUM_ID_AREA_ATUACAO, "
                    "vf.COD_TIPO_ACESSO "
                    f"FROM {cfg.schema}.{CONTA_PARTICIPANTE_TABLE} cp "
                    f"LEFT JOIN {cfg.schema}.{V_FAMILIA_CONTAS_TABLE} vf "
                    "ON cp.COD_CONTA_PARTICIPANTE = vf.COD_CONTA_MEMBRO "
                    f"WHERE cp.NUM_CONTA_PARTICIPANTE IN ({literals})"
                )
                remote = _jdbc(spark, cfg, query)
                rows = remote.collect()
                account_schema = account_schema or remote.schema
                account_rows.extend(rows)
            lookups[CONTA_PARTICIPANTE_TABLE] = spark.createDataFrame(
                account_rows, account_schema
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Category 6 account lookup load failed: %s", exc)
            errors[CONTA_PARTICIPANTE_TABLE] = str(exc)
    elif account_keys:
        errors[CONTA_PARTICIPANTE_TABLE] = "No Oracle connection"

    if run_combo and op_df is not None and cfg.jdbc_url and lookup_tos_supported:
        existing = check_lookup_combo_frames(
            op_df,
            lookups.get(TIPO_OPER_OBJETO_SERV_TABLE),
            lookups.get(V_PARAMETRO_SIC_TABLE),
            lookups.get(TIPO_OPERACAO_TABLE),
            sample,
            profile,
            errors,
            skip_prefixes,
        )

    required = (
        check_required_lookup_frames(
            tables,
            lookups.get(CONTA_PARTICIPANTE_TABLE),
            lookups.get(TIPO_OPER_OBJETO_SERV_TABLE),
            lookups.get(TIPO_OPERACAO_TABLE),
            lookups.get(V_OBJETOS_SERVICO_TABLE),
            sample,
            profile,
            errors,
            skip_prefixes,
        )
        if run_required else []
    )
    return existing + required


# ---------------------------------------------------------------------------
# Category 0/2g/6g/8g - LCA registration-route evidence
# ---------------------------------------------------------------------------
LCA_OUTPUT_TABLES = (
    "ENTIDADE", "REPRESENTANTE_IF", "INSTRUMENTO_FINANCEIRO", "TITULO", "IF_LCA",
    "CREDITO", "GARANTIA", "CONDICAO_IF", "AMORTIZACAO", "JUROS_FLUTUANTE", "SPREAD",
    "RESGATE", "EVENTO", "DEPOSITO_AUTOMATICO_IF", "OPERACAO", "DADO_OPERACAO",
    "LANCAMENTO", "ESPECIFICACAO", "ESPECIFICACAO_COMITENTE", "CARTEIRA_COMITENTE",
    "CARTEIRA_PARTICIPANTE",
)
LCA_CONDITION_SUBTYPES = {
    "1": "AMORTIZACAO", "3": "JUROS_FLUTUANTE", "5": "SPREAD", "20": "RESGATE",
}
LCA_MEU_NUMERO_TOGGLE = "VALIDA_MEU_NUMERO_DEPOSITO"


def _lca_unavailable(check_id: str, missing: Sequence[str], severity: str = SEV_WARN) -> Finding:
    return Finding(
        check_id, "LCA", severity,
        ",".join(sorted({value.split(".")[0] for value in missing})), False,
        hint="Export the complete LCA aggregate or make its bounded target lookup available.",
        message=f"Check unavailable; missing required input: {', '.join(missing)}.",
    )


def check_lca_metadata(
    meta: Metadata, no_oracle: bool, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lca":
        return []
    if no_oracle:
        return [Finding(
            "0.lca_metadata", "Coverage", SEV_WARN, "Oracle metadata", False,
            hint="Rerun with Oracle access; specs.json omits IF_LCA/GARANTIA and marks "
                 "ENTIDADE/REPRESENTANTE_IF static.",
            message="Live Oracle table and PK metadata for all 21 LCA output tables is "
                    "unavailable under --no-oracle (forces PARTIAL).",
        )]
    missing = [table for table in LCA_OUTPUT_TABLES if table not in meta.tables]
    missing_pk = [
        table for table in LCA_OUTPUT_TABLES if table in meta.tables and not meta.pk.get(table)
    ]
    failed = bool(missing or missing_pk)
    return [Finding(
        "0.lca_metadata", "Coverage", SEV_ERROR if failed else SEV_INFO,
        ",".join(LCA_OUTPUT_TABLES), not failed, count=len(missing) + len(missing_pk),
        hint="Use live metadata for every LCA output table; specs.json is not authoritative."
             if failed else "",
        message=(f"Missing Oracle table metadata={missing}; missing PK metadata={missing_pk}."
                 if failed else "Live Oracle table and PK metadata cover all 21 LCA tables."),
    )]


def _lca_edge_findings(
    check_id: str, parents: DataFrame, child: DataFrame, child_table: str,
    parent_column: str, child_id_column: str, sample: int,
) -> List[Finding]:
    parent_counts = parents.groupBy("parent_id").count().withColumnRenamed(
        "count", "parent_count"
    )
    edges = child.select(
        _canon_key_col(F.col(parent_column)).alias("parent_id"),
        _canon_key_col(F.col(child_id_column)).alias("child_id"),
    )
    bad = edges.join(parent_counts, "parent_id", "left").where(
        F.coalesce(F.col("parent_count"), F.lit(0)) != 1
    )
    duplicate = edges.groupBy("parent_id", "child_id").count().where(F.col("count") > 1)
    count, duplicate_count = bad.count(), duplicate.count()
    return [
        Finding(
            f"{check_id}.edge", "LCA graph", SEV_ERROR if count else SEV_INFO,
            child_table, count == 0, count=count, column=parent_column,
            sample=_sample_keys(bad, ["child_id", "parent_id"], sample),
            hint="Remove the orphan/ambiguous edge or export its one parent." if count else "",
            message="LCA child rows must resolve to exactly one aggregate parent.",
        ),
        Finding(
            f"{check_id}.duplicate", "LCA graph",
            SEV_ERROR if duplicate_count else SEV_INFO, child_table,
            duplicate_count == 0, count=duplicate_count,
            column=f"{child_id_column},{parent_column}",
            sample=_sample_keys(duplicate, ["child_id", "parent_id"], sample),
            hint="Keep each physical child-to-parent edge unambiguous."
                 if duplicate_count else "",
            message="Duplicate LCA physical graph edges.",
        ),
    ]


def check_lca_graph(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lca":
        return []
    missing_tables = [table for table in LCA_OUTPUT_TABLES if table not in tables]
    if missing_tables:
        return [_lca_unavailable("2g.output_tables", missing_tables, SEV_ERROR)]
    requirements = {
        "ENTIDADE": ("NUM_ID_ENTIDADE",),
        "REPRESENTANTE_IF": ("NUM_ID_ENTIDADE",),
        "INSTRUMENTO_FINANCEIRO": ("NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "COD_IF"),
        "TITULO": ("NUM_IF",),
        "IF_LCA": ("NUM_IF", "NUM_ID_ENT_DEPOSITARIO_ORIG"),
        "CREDITO": ("NUM_IF",),
        "GARANTIA": ("NUM_ID_GARANTIA", "NUM_IF"),
        "CONDICAO_IF": ("NUM_CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF"),
        **{table: ("NUM_CONDICAO_IF",) for table in LCA_CONDITION_SUBTYPES.values()},
        "EVENTO": ("NUM_EVENTO", "NUM_IF"),
        "DEPOSITO_AUTOMATICO_IF": ("NUM_IF",),
        "OPERACAO": ("NUM_ID_OPERACAO", "NUM_IF"),
        "DADO_OPERACAO": ("NUM_ID_DADO_OPERACAO", "NUM_ID_OPERACAO"),
        "LANCAMENTO": ("NUM_ID_LANCAMENTO", "NUM_ID_OPERACAO"),
        "ESPECIFICACAO": ("NUM_ID_ESPECIFICACAO", "NUM_ID_OPERACAO"),
        "ESPECIFICACAO_COMITENTE": (
            "NUM_ID_ESPECIFICACAO_COMITENTE", "NUM_ID_ESPECIFICACAO",
        ),
        "CARTEIRA_COMITENTE": ("NUM_CARTEIRA_COMITENTE", "NUM_IF"),
        "CARTEIRA_PARTICIPANTE": ("NUM_CARTEIRA_PARTICIPANTE", "NUM_IF"),
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_lca_unavailable("2g.graph.availability", missing, SEV_ERROR)]
    root_cols = columns["INSTRUMENTO_FINANCEIRO"]
    roots = _active(tables["INSTRUMENTO_FINANCEIRO"]).where(
        _canon_key_col(F.col(root_cols["NUM_TIPO_IF"])) == "96"
    ).select(
        _canon_key_col(F.col(root_cols["NUM_IF"])).alias("parent_id"),
        _lci_text(F.col(root_cols["COD_IF"])).alias("business_code"),
    )
    coded = roots.withColumn(
        "code_count", F.count(F.lit(1)).over(Window.partitionBy("business_code"))
    )
    bad_codes = coded.where(
        F.col("business_code").isNull() | (F.col("business_code") == "")
        | (F.col("code_count") > 1)
    )
    count = bad_codes.count()
    out = [Finding(
        "2g.root_code", "LCA graph", SEV_ERROR if count else SEV_INFO,
        "INSTRUMENTO_FINANCEIRO", count == 0, count=count, column="COD_IF",
        sample=_sample_keys(bad_codes, ["parent_id", "business_code"], sample),
        hint="Generate nonblank unique exact-trimmed active LCA COD_IF values."
             if count else "",
        message="Active LCA roots with blank or duplicate case-sensitive COD_IF values.",
    )]
    for table in ("TITULO", "IF_LCA", "CREDITO", "GARANTIA"):
        parent_col = columns[table]["NUM_IF"]
        children = tables[table].select(_canon_key_col(F.col(parent_col)).alias("parent_id"))
        counts = children.groupBy("parent_id").count().withColumnRenamed("count", "child_count")
        bad = roots.select("parent_id").join(counts, "parent_id", "left").where(
            F.coalesce(F.col("child_count"), F.lit(0)) != 1
        )
        child_count = bad.count()
        out.append(Finding(
            f"2g.one_{table.lower()}", "LCA graph", SEV_ERROR if child_count else SEV_INFO,
            table, child_count == 0, count=child_count, column="NUM_IF",
            sample=_sample_keys(bad, ["parent_id"], sample),
            hint=f"Keep exactly one {table} row per active LCA root." if child_count else "",
            message=f"Active LCA roots without exactly one {table} row.",
        ))
        child_id = columns[table].get("NUM_ID_GARANTIA", parent_col)
        out.extend(_lca_edge_findings(
            f"2g.{table.lower()}", roots.select("parent_id"), tables[table], table,
            parent_col, child_id, sample,
        ))
    entities = tables["ENTIDADE"].select(
        _canon_key_col(F.col(columns["ENTIDADE"]["NUM_ID_ENTIDADE"])).alias("parent_id")
    )
    representatives = tables["REPRESENTANTE_IF"].select(
        _canon_key_col(F.col(columns["REPRESENTANTE_IF"]["NUM_ID_ENTIDADE"])).alias("parent_id")
    )
    out.extend(_lca_edge_findings(
        "2g.representative", entities, tables["REPRESENTANTE_IF"], "REPRESENTANTE_IF",
        columns["REPRESENTANTE_IF"]["NUM_ID_ENTIDADE"],
        columns["REPRESENTANTE_IF"]["NUM_ID_ENTIDADE"], sample,
    ))
    out.extend(_lca_edge_findings(
        "2g.depositor_origin", representatives, tables["IF_LCA"], "IF_LCA",
        columns["IF_LCA"]["NUM_ID_ENT_DEPOSITARIO_ORIG"], columns["IF_LCA"]["NUM_IF"], sample,
    ))
    direct_edges = (
        ("condition", "CONDICAO_IF", "NUM_IF", "NUM_CONDICAO_IF"),
        ("event", "EVENTO", "NUM_IF", "NUM_EVENTO"),
        ("deposit", "DEPOSITO_AUTOMATICO_IF", "NUM_IF", "NUM_IF"),
        ("operation", "OPERACAO", "NUM_IF", "NUM_ID_OPERACAO"),
        ("wallet_comitente", "CARTEIRA_COMITENTE", "NUM_IF", "NUM_CARTEIRA_COMITENTE"),
        ("wallet_participante", "CARTEIRA_PARTICIPANTE", "NUM_IF",
         "NUM_CARTEIRA_PARTICIPANTE"),
    )
    for name, table, parent_name, child_name in direct_edges:
        out.extend(_lca_edge_findings(
            f"2g.{name}", roots.select("parent_id"), tables[table], table,
            columns[table][parent_name], columns[table][child_name], sample,
        ))
    conditions = tables["CONDICAO_IF"].select(
        _canon_key_col(F.col(columns["CONDICAO_IF"]["NUM_CONDICAO_IF"])).alias("parent_id")
    )
    for table in LCA_CONDITION_SUBTYPES.values():
        out.extend(_lca_edge_findings(
            f"2g.{table.lower()}", conditions, tables[table], table,
            columns[table]["NUM_CONDICAO_IF"], columns[table]["NUM_CONDICAO_IF"], sample,
        ))
    operations = tables["OPERACAO"].select(
        _canon_key_col(F.col(columns["OPERACAO"]["NUM_ID_OPERACAO"])).alias("parent_id")
    )
    for name, table, child_name in (
        ("operation_data", "DADO_OPERACAO", "NUM_ID_DADO_OPERACAO"),
        ("launch", "LANCAMENTO", "NUM_ID_LANCAMENTO"),
        ("specification", "ESPECIFICACAO", "NUM_ID_ESPECIFICACAO"),
    ):
        out.extend(_lca_edge_findings(
            f"2g.{name}", operations, tables[table], table,
            columns[table]["NUM_ID_OPERACAO"], columns[table][child_name], sample,
        ))
    specifications = tables["ESPECIFICACAO"].select(
        _canon_key_col(F.col(columns["ESPECIFICACAO"]["NUM_ID_ESPECIFICACAO"]))
        .alias("parent_id")
    )
    out.extend(_lca_edge_findings(
        "2g.specification_holder", specifications, tables["ESPECIFICACAO_COMITENTE"],
        "ESPECIFICACAO_COMITENTE",
        columns["ESPECIFICACAO_COMITENTE"]["NUM_ID_ESPECIFICACAO"],
        columns["ESPECIFICACAO_COMITENTE"]["NUM_ID_ESPECIFICACAO_COMITENTE"], sample,
    ))
    return out


def check_lca_polymorphism(
    tables: Dict[str, DataFrame], sample: int, profile: ValidationProfile
) -> List[Finding]:
    if profile.pipeline != "lca":
        return []
    requirements = {
        "CONDICAO_IF": ("NUM_CONDICAO_IF", "COD_TIPO_CONDICAO_IF"),
        **{table: ("NUM_CONDICAO_IF",) for table in LCA_CONDITION_SUBTYPES.values()},
    }
    columns, missing = _credito_scr_columns(tables, requirements)
    if missing:
        return [_lca_unavailable("2g.condition.availability", missing, SEV_ERROR)]
    condition = _active(tables["CONDICAO_IF"]).select(
        _canon_key_col(F.col(columns["CONDICAO_IF"]["NUM_CONDICAO_IF"])).alias("condition_id"),
        _lci_text(F.col(columns["CONDICAO_IF"]["COD_TIPO_CONDICAO_IF"])).alias(
            "condition_type"
        ),
    )
    membership = None
    for table in LCA_CONDITION_SUBTYPES.values():
        frame = _active(tables[table]).select(
            _canon_key_col(F.col(columns[table]["NUM_CONDICAO_IF"])).alias("condition_id"),
            F.lit(table).alias("physical_table"),
        )
        membership = frame if membership is None else membership.unionByName(frame)
    subtype_tables = list(LCA_CONDITION_SUBTYPES.values())
    counts = membership.groupBy("condition_id").pivot("physical_table", subtype_tables).count()
    known = condition.where(F.col("condition_type").isin(*LCA_CONDITION_SUBTYPES))
    joined = known.join(counts, "condition_id", "left").fillna(0, subtype_tables)
    pairs = []
    for code, table in LCA_CONDITION_SUBTYPES.items():
        pairs.extend((F.lit(code), F.lit(table)))
    joined = joined.withColumn("expected_table", F.create_map(*pairs)[F.col("condition_type")])
    bad = joined.where(reduce(
        lambda left, right: left | right,
        [F.when(F.col("expected_table") == table, F.col(table) != 1)
         .otherwise(F.col(table) != 0) for table in subtype_tables],
    ))
    unknown = condition.where(
        F.col("condition_type").isNull() | (F.col("condition_type") == "")
        | ~F.col("condition_type").isin(*LCA_CONDITION_SUBTYPES)
    )
    orphan = membership.join(condition.select("condition_id").dropDuplicates(),
                             "condition_id", "left_anti")
    bad_count, unknown_count, orphan_count = bad.count(), unknown.count(), orphan.count()
    return [
        Finding(
            "2g.condition_polymorphism", "LCA condition polymorphism",
            SEV_ERROR if bad_count else SEV_INFO, "CONDICAO_IF", bad_count == 0,
            count=bad_count, column="COD_TIPO_CONDICAO_IF,NUM_CONDICAO_IF",
            sample=_sample_keys(bad, ["condition_id", "condition_type"], sample),
            hint="Emit exactly one expected known physical row and no wrong known row."
                 if bad_count else "",
            message="Known LCA conditions with missing, duplicate, or wrong physical subtype.",
        ),
        Finding(
            "2g.unknown_condition_type", "LCA condition polymorphism",
            SEV_WARN if unknown_count else SEV_INFO, "CONDICAO_IF", unknown_count == 0,
            count=unknown_count, column="COD_TIPO_CONDICAO_IF",
            sample=_sample_keys(unknown, ["condition_id", "condition_type"], sample),
            hint="Capture another successful LCA variant before assigning a mapping."
                 if unknown_count else "",
            message="LCA condition types outside the four log-proven mappings.",
        ),
        Finding(
            "2g.subtype_orphan", "LCA condition polymorphism",
            SEV_ERROR if orphan_count else SEV_INFO, "CONDICAO_IF", orphan_count == 0,
            count=orphan_count, column="NUM_CONDICAO_IF",
            sample=_sample_keys(orphan, ["condition_id", "physical_table"], sample),
            hint="Remove subtype rows without a CONDICAO_IF parent." if orphan_count else "",
            message="Known LCA physical subtype rows without a condition parent.",
        ),
    ]


# ---------------------------------------------------------------------------
def _lca_from_lci_finding(finding: Finding) -> Finding:
    adapted = replace(
        finding,
        check_id=finding.check_id.replace("6e.", "6g.", 1),
        category=finding.category.replace("LCI", "LCA"),
        table=finding.table.replace("LCI_", "LCA_"),
        message=finding.message.replace("LCI", "LCA").replace("type-1 lot", "type-2 lot"),
        hint=finding.hint.replace("LCI", "LCA").replace("type-1 lot", "type-2 lot")
        .replace("type 81", "type 96").replace("object-service 75", "object-service 843"),
    )
    if adapted.check_id == "6g.lookup.issuer_account":
        adapted = replace(
            adapted,
            column="NUM_ID_SITUACAO_CONTA",
            hint="Use an LCA issuer account with status 1 or 2." if not adapted.passed else "",
        )
    return adapted


def check_lca_target_frames(
    tables: Dict[str, DataFrame], frames: Dict[str, DataFrame], sample: int,
    profile: ValidationProfile, errors: Optional[Dict[str, str]] = None,
) -> List[Finding]:
    if profile.pipeline != "lca":
        return []
    errors = errors or {}
    mapping = {
        "LCA_TIPO_IF": "LCI_TIPO_IF", "LCA_LOTES": "LCI_LOTES",
        "LCA_ACCOUNTS": "LCI_ACCOUNTS", "LCA_OBJECT_SERVICE": "LCI_OBJECT_SERVICE",
        "LCA_ROUTES": "LCI_ROUTES", "LCA_ROOT_CODES": "LCI_ROOT_CODES",
        "LCA_TOGGLE": "LCI_TOGGLE", "LCA_CONTROLS": "LCI_CONTROLS",
        "LCA_OPERATION_CODES": "LCI_OPERATION_CODES",
        "LCA_WALLET_COMITENTE": "LCI_WALLET_COMITENTE",
        "LCA_WALLET_PARTICIPANTE": "LCI_WALLET_PARTICIPANTE",
    }
    adapted = {mapping[name]: frame for name, frame in frames.items() if name in mapping}
    accounts = adapted.get("LCI_ACCOUNTS")
    if accounts is not None:
        if resolve(accounts, "COD_TIPO_ACESSO") is None:
            accounts = accounts.withColumn("COD_TIPO_ACESSO", F.lit(None).cast("string"))
        if resolve(accounts, "NUM_ID_AREA_ATUACAO") is None:
            accounts = accounts.withColumn("NUM_ID_AREA_ATUACAO", F.lit(None).cast("long"))
        adapted["LCI_ACCOUNTS"] = accounts
    adapted_errors = {mapping[name]: value for name, value in errors.items() if name in mapping}
    out = [
        _lca_from_lci_finding(finding)
        for finding in check_lci_target_frames(tables, adapted, sample, profile, adapted_errors)
    ]
    lots = frames.get("LCA_LOTES")
    lot_id = resolve(lots, "NUM_ID_LOTE") if lots is not None else None
    lot_type_if = resolve(lots, "NUM_TIPO_IF") if lots is not None else None
    root = tables.get("INSTRUMENTO_FINANCEIRO")
    root_lot = resolve(root, "NUM_ID_LOTE") if root is not None else None
    root_type = resolve(root, "NUM_TIPO_IF") if root is not None else None
    if lots is not None and all((lot_id, lot_type_if, root is not None, root_lot, root_type)):
        active_lots, supported = _lci_active_target(lots)
        if supported:
            roots = _active(root).where(
                _canon_key_col(F.col(root_type)) == "96"
            ).select(_canon_key_col(F.col(root_lot)).alias("lot_id"))
            target = active_lots.select(
                _canon_key_col(F.col(lot_id)).alias("lot_id"),
                _canon_key_col(F.col(lot_type_if)).alias("lot_root_type"),
            )
            bad = roots.join(target, "lot_id", "left").where(
                F.col("lot_root_type").isNull() | (F.col("lot_root_type") != "96")
            )
            count = bad.count()
            out.append(Finding(
                "6g.lookup.lot_root_type", "LCA target eligibility",
                SEV_ERROR if count else SEV_INFO, "LOTE", count == 0, count=count,
                column="LOTE.NUM_TIPO_IF", sample=_sample_keys(bad, ["lot_id"], sample),
                hint="Use a persisted type-96 LCA lot root." if count else "",
                message="LCA target lots retain the persisted compatible root type.",
            ))
        else:
            out.append(_lca_unavailable(
                "6g.lookup.lot_root_type", ["LCA_LOTES.DAT_EXCLUSAO"]
            ))
    else:
        out.append(_lca_unavailable(
            "6g.lookup.lot_root_type", ["LCA_LOTES.NUM_TIPO_IF"]
        ))
    credit = tables.get("CREDITO")
    credit_if = resolve(credit, "NUM_IF") if credit is not None else None
    credit_municipality = resolve(credit, "NUM_ID_MUNICIPIO") if credit is not None else None
    municipalities = frames.get("LCA_MUNICIPALITIES")
    municipality_id = (
        resolve(municipalities, "NUM_ID_MUNICIPIO") if municipalities is not None else None
    )
    municipality_uf = resolve(municipalities, "NUM_ID_UF") if municipalities is not None else None
    municipality_active = (
        resolve(municipalities, "IND_EXCLUIDO") if municipalities is not None else None
    )
    if all((credit is not None, credit_if, credit_municipality, municipalities is not None,
            municipality_id, municipality_uf, municipality_active)):
        references = credit.select(
            _canon_key_col(F.col(credit_if)).alias("root_id"),
            _canon_key_col(F.col(credit_municipality)).alias("municipality_id"),
        ).where(F.col("municipality_id").isNotNull() & (F.col("municipality_id") != ""))
        active_municipalities = municipalities.where(
            _lci_text(F.col(municipality_active)) == "N"
        ).select(
            _canon_key_col(F.col(municipality_id)).alias("municipality_id"),
            _canon_key_col(F.col(municipality_uf)).alias("uf_id"),
        )
        bad = references.join(active_municipalities, "municipality_id", "left").where(
            F.col("uf_id").isNull()
        )
        count = bad.count()
        out.append(Finding(
            "6g.lookup.municipality", "LCA target eligibility",
            SEV_ERROR if count else SEV_INFO, "MUNICIPIO", count == 0, count=count,
            column="CREDITO.NUM_ID_MUNICIPIO,IND_EXCLUIDO",
            sample=_sample_keys(bad, ["root_id", "municipality_id"], sample),
            hint="Resolve each nonnull CREDITO municipality to active IND_EXCLUIDO='N'."
                 if count else "",
            message="LCA CREDITO municipality references are active.",
        ))
        ufs = frames.get("LCA_UFS")
        uf_id = resolve(ufs, "NUM_ID_UF") if ufs is not None else None
        uf_active = resolve(ufs, "IND_EXCLUIDO") if ufs is not None else None
        if ufs is not None and uf_id and uf_active:
            active_ufs = ufs.where(_lci_text(F.col(uf_active)) == "N").select(
                _canon_key_col(F.col(uf_id)).alias("uf_id")
            ).dropDuplicates()
            bad = references.join(active_municipalities, "municipality_id", "inner").join(
                F.broadcast(active_ufs), "uf_id", "left_anti"
            )
            count = bad.count()
            out.append(Finding(
                "6g.lookup.uf", "LCA target eligibility", SEV_ERROR if count else SEV_INFO,
                "UF", count == 0, count=count, column="MUNICIPIO.NUM_ID_UF,IND_EXCLUIDO",
                sample=_sample_keys(bad, ["root_id", "municipality_id", "uf_id"], sample),
                hint="Resolve each municipality to active UF IND_EXCLUIDO='N'." if count else "",
                message="LCA municipality UF references are active.",
            ))
        else:
            out.append(_lca_unavailable("6g.lookup.uf", ["LCA_UFS"]))
    else:
        out.append(_lca_unavailable("6g.lookup.municipality", ["LCA_MUNICIPALITIES"]))
    return out


def load_lca_target_frames(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], maximum: int = 100_000,
    skip_prefixes: Sequence[str] = (),
) -> Tuple[Dict[str, DataFrame], Dict[str, str]]:
    """Bounded LCA target setup; skipped 6g prefixes never issue JDBC."""
    lci_skips = tuple(prefix.replace("6g.", "6e.", 1) for prefix in skip_prefixes)
    frames, errors = load_lci_target_frames(
        spark, cfg, tables, maximum, lci_skips, VALIDATION_PROFILES["lca"]
    )
    reverse = {
        "LCI_TIPO_IF": "LCA_TIPO_IF", "LCI_LOTES": "LCA_LOTES",
        "LCI_ACCOUNTS": "LCA_ACCOUNTS", "LCI_OBJECT_SERVICE": "LCA_OBJECT_SERVICE",
        "LCI_ROUTES": "LCA_ROUTES", "LCI_ROOT_CODES": "LCA_ROOT_CODES",
        "LCI_TOGGLE": "LCA_TOGGLE", "LCI_CONTROLS": "LCA_CONTROLS",
        "LCI_OPERATION_CODES": "LCA_OPERATION_CODES",
        "LCI_WALLET_COMITENTE": "LCA_WALLET_COMITENTE",
        "LCI_WALLET_PARTICIPANTE": "LCA_WALLET_PARTICIPANTE",
    }
    out = {reverse[name]: frame for name, frame in frames.items() if name in reverse}
    out_errors = {reverse.get(name, name): value for name, value in errors.items()}
    want_municipality = not _check_is_skipped(
        "6g.lookup.municipality", list(skip_prefixes)
    )
    want_uf = not _check_is_skipped("6g.lookup.uf", list(skip_prefixes))
    if not want_municipality and not want_uf:
        return out, out_errors
    credit = tables.get("CREDITO")
    municipality = resolve(credit, "NUM_ID_MUNICIPIO") if credit is not None else None
    if municipality is None:
        out_errors["LCA_MUNICIPALITIES"] = "CREDITO.NUM_ID_MUNICIPIO unavailable"
        return out, out_errors
    values = [str(row[0]) for row in credit.select(
        _canon_key_col(F.col(municipality)).alias("id")
    ).where(F.col("id").isNotNull() & (F.col("id") != "")).dropDuplicates()
        .limit(maximum + 1).collect()]
    if len(values) > maximum:
        out_errors["LCA_MUNICIPALITIES"] = f"more than {maximum} municipality IDs"
        return out, out_errors
    municipality_rows, municipality_schema = [], None
    try:
        for offset in range(0, len(values), 1000):
            batch = values[offset:offset + 1000]
            frame = _jdbc(
                spark, cfg,
                "SELECT NUM_ID_MUNICIPIO, NUM_ID_UF, IND_EXCLUIDO "
                f"FROM {cfg.schema}.MUNICIPIO WHERE NUM_ID_MUNICIPIO IN ("
                + ", ".join(_sql_literal(value) for value in batch) + ")",
            )
            municipality_schema = municipality_schema or frame.schema
            municipality_rows.extend(frame.collect())
        if municipality_schema is None:
            out["LCA_MUNICIPALITIES"] = spark.createDataFrame(
                [], "NUM_ID_MUNICIPIO string, NUM_ID_UF string, IND_EXCLUIDO string"
            )
        else:
            out["LCA_MUNICIPALITIES"] = spark.createDataFrame(
                municipality_rows, municipality_schema
            )
        if not want_uf:
            return out, out_errors
        uf_values = [str(row[0]) for row in out["LCA_MUNICIPALITIES"].select(
            _canon_key_col(F.col(resolve(out["LCA_MUNICIPALITIES"], "NUM_ID_UF")))
            .alias("uf_id")
        ).where(F.col("uf_id").isNotNull()).dropDuplicates().limit(maximum + 1).collect()]
        if len(uf_values) > maximum:
            out_errors["LCA_UFS"] = f"more than {maximum} UF IDs"
        elif uf_values:
            out["LCA_UFS"] = _jdbc(
                spark, cfg, "SELECT NUM_ID_UF, IND_EXCLUIDO "
                f"FROM {cfg.schema}.UF WHERE NUM_ID_UF IN ("
                + ", ".join(_sql_literal(value) for value in uf_values) + ")",
            )
        else:
            out["LCA_UFS"] = spark.createDataFrame(
                [], "NUM_ID_UF string, IND_EXCLUIDO string"
            )
    except Exception as exc:  # noqa: BLE001
        out_errors["LCA_MUNICIPALITIES"] = str(exc)
    return out, out_errors


def check_lca_registration_profile(
    tables: Dict[str, DataFrame], sample: int, enabled: bool, profile: ValidationProfile,
) -> List[Finding]:
    if profile.pipeline != "lca" or not enabled:
        return []

    def constants(table: str, expected: Dict[str, object]) -> Finding:
        frame = tables.get(table)
        check_id = f"8g.profile.{table.lower()}_constants"
        if frame is None:
            return _lca_unavailable(check_id, [table])
        actual = {name: resolve(frame, name) for name in expected}
        if any(column is None for column in actual.values()):
            return _lca_unavailable(check_id, [f"{table}.{name}" for name, column in actual.items()
                                                if column is None])
        bad = frame.where(reduce(
            lambda left, right: left | right,
            [~F.coalesce(_canon_key_col(F.col(actual[name])) == str(value), F.lit(False))
             for name, value in expected.items()],
        ))
        count = bad.count()
        return Finding(
            check_id, "LCA observed registration profile", SEV_WARN if count else SEV_INFO,
            table, count == 0, count=count, column=",".join(expected),
            sample=_sample_keys(bad, frame.columns[:1], sample),
            hint="Treat lca_inclusao.log values as advisory." if count else "",
            message="Rows differing from observed LCA registration constants.",
        )

    out = [
        constants("INSTRUMENTO_FINANCEIRO", {
            "NUM_SISTEMA": 55, "NUM_TIPO_IF": 96, "NUM_ID_FORMA_PAGAMENTO": 267,
            "NUM_ID_MOTIVO_SITUACAO_IF": 7, "COD_SITUACAO_IF": 0,
            "VAL_NOMINAL_EMISSAO": 500, "VAL_NOMINAL_ATUAL": 500,
            "VAL_NOMINAL_EM": 500, "VAL_PU_CURVA": 500, "IND_AGENDA_CONSTANTE": "S",
        }),
        constants("TITULO", {
            "QTD_EMITIDA": 1, "NUM_ID_TIPO_REGIME_TITULO": 2,
            "NUM_ID_VEICULO_GARANTIDOR": 1, "IND_FRACIONAMENTO": "N",
            "NOM_FORMA_TITULO": "ESCRITURAL",
        }),
        constants("IF_LCA", {
            "IND_MANUT_UNILATERAL_GARANTIAS": "S", "IND_LIQUIDACAO_ANTECIPADA": "N",
        }),
        constants("CREDITO", {"NUM_ID_TIPO_CREDITO": 11, "NUM_ID_MUNICIPIO": 339}),
        constants("GARANTIA", {"NUM_ID_TIPO_GARANTIA": 16}),
        constants("AMORTIZACAO", {"VAL_TAXA_AMORTIZACAO": 10}),
        constants("JUROS_FLUTUANTE", {
            "NUM_INDICE_VALORIZACAO": 4, "VAL_PERCENTUAL_TAXA_JUROS": 100,
        }),
        constants("SPREAD", {"VAL_TAXA_SPREAD": 1.56}),
        constants("RESGATE", {"COD_COND_RESGATE": "SEM TABELA"}),
    ]
    root, condition, event = (tables.get(name) for name in (
        "INSTRUMENTO_FINANCEIRO", "CONDICAO_IF", "EVENTO"
    ))
    root_key = resolve(root, "NUM_IF") if root is not None else None
    root_type = resolve(root, "NUM_TIPO_IF") if root is not None else None
    condition_root = resolve(condition, "NUM_IF") if condition is not None else None
    condition_type = resolve(condition, "COD_TIPO_CONDICAO_IF") if condition is not None else None
    if all((root is not None, condition is not None, root_key, root_type,
            condition_root, condition_type)):
        roots = _active(root).where(_canon_key_col(F.col(root_type)) == "96").select(
            _canon_key_col(F.col(root_key)).alias("root_id")
        )
        topology = condition.select(
            _canon_key_col(F.col(condition_root)).alias("root_id"),
            _lci_text(F.col(condition_type)).alias("condition_type"),
        ).groupBy("root_id").agg(F.sort_array(F.collect_list("condition_type")).alias("topology"))
        bad = roots.join(topology, "root_id", "left").where(
            ~F.coalesce(F.col("topology") == F.array(
                F.lit("1"), F.lit("20"), F.lit("3"), F.lit("5")
            ), F.lit(False))
        )
        count = bad.count()
        out.append(Finding(
            "8g.profile.condition_topology", "LCA observed registration profile",
            SEV_WARN if count else SEV_INFO, "CONDICAO_IF", count == 0, count=count,
            column="COD_TIPO_CONDICAO_IF", sample=_sample_keys(bad, ["root_id"], sample),
            hint="Observed topology 1+3+5+20 is advisory." if count else "",
            message="LCA roots outside the observed condition topology.",
        ))
    else:
        out.append(_lca_unavailable(
            "8g.profile.condition_topology",
            ["INSTRUMENTO_FINANCEIRO/CONDICAO_IF required columns"],
        ))
    event_root = resolve(event, "NUM_IF") if event is not None else None
    event_type = resolve(event, "NUM_TIPO_EVENTO_LEGADO") if event is not None else None
    if all((root is not None, event is not None, root_key, root_type, event_root, event_type)):
        roots = _active(root).where(_canon_key_col(F.col(root_type)) == "96").select(
            _canon_key_col(F.col(root_key)).alias("root_id")
        )
        counts = event.select(
            _canon_key_col(F.col(event_root)).alias("root_id"),
            _canon_key_col(F.col(event_type)).alias("event_type"),
        ).groupBy("root_id").agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("event_type") == "83", 1).otherwise(0)).alias("type83"),
            F.sum(F.when(F.col("event_type") == "84", 1).otherwise(0)).alias("type84"),
            F.sum(F.when(F.col("event_type") == "85", 1).otherwise(0)).alias("type85"),
        )
        bad = roots.join(counts, "root_id", "left").fillna(0).where(
            (F.col("total") != 20) | (F.col("type83") != 10)
            | (F.col("type84") != 9) | (F.col("type85") != 1)
        )
        count = bad.count()
        out.append(Finding(
            "8g.profile.event_dml_counts", "LCA observed registration profile",
            SEV_WARN if count else SEV_INFO, "EVENTO", count == 0, count=count,
            column="NUM_TIPO_EVENTO_LEGADO",
            sample=_sample_keys(bad, ["root_id", "total", "type83", "type84", "type85"], sample),
            hint="Logger says 8 amortizations, but DML has 9 type-84 rows; never harden this."
                 if count else "",
            message="Observed event DML is total20/type83=10/type84=9/type85=1; logger says "
                    "8 amortizations while physical DML has 9.",
        ))
        closure = roots
        for table, root_column, alias in (
            ("DEPOSITO_AUTOMATICO_IF", "NUM_IF", "deposit_count"),
            ("OPERACAO", "NUM_IF", "operation_count"),
            ("CARTEIRA_COMITENTE", "NUM_IF", "wallet_holder_count"),
            ("CARTEIRA_PARTICIPANTE", "NUM_IF", "wallet_participant_count"),
        ):
            frame = tables.get(table)
            actual = resolve(frame, root_column) if frame is not None else None
            if actual:
                per_root = frame.select(
                    _canon_key_col(F.col(actual)).alias("root_id")
                ).groupBy("root_id").count().withColumnRenamed("count", alias)
                closure = closure.join(per_root, "root_id", "left")
            else:
                closure = closure.withColumn(alias, F.lit(0))
        operation = tables.get("OPERACAO")
        operation_id = resolve(operation, "NUM_ID_OPERACAO") if operation is not None else None
        operation_root = resolve(operation, "NUM_IF") if operation is not None else None
        operation_bridge = None
        if operation_id and operation_root:
            operation_bridge = operation.select(
                _canon_key_col(F.col(operation_id)).alias("operation_id"),
                _canon_key_col(F.col(operation_root)).alias("root_id"),
            )
            for table, alias in (
                ("DADO_OPERACAO", "data_count"),
                ("LANCAMENTO", "launch_count"),
                ("ESPECIFICACAO", "specification_count"),
            ):
                frame = tables.get(table)
                child_operation = resolve(frame, "NUM_ID_OPERACAO") \
                    if frame is not None else None
                if child_operation:
                    per_root = frame.select(
                        _canon_key_col(F.col(child_operation)).alias("operation_id")
                    ).join(operation_bridge, "operation_id", "inner").groupBy(
                        "root_id"
                    ).count().withColumnRenamed("count", alias)
                    closure = closure.join(per_root, "root_id", "left")
                else:
                    closure = closure.withColumn(alias, F.lit(0))
            specification = tables.get("ESPECIFICACAO")
            holder = tables.get("ESPECIFICACAO_COMITENTE")
            specification_id = resolve(specification, "NUM_ID_ESPECIFICACAO") \
                if specification is not None else None
            specification_operation = resolve(specification, "NUM_ID_OPERACAO") \
                if specification is not None else None
            holder_specification = resolve(holder, "NUM_ID_ESPECIFICACAO") \
                if holder is not None else None
            if specification_id and specification_operation and holder_specification:
                specification_roots = specification.select(
                    _canon_key_col(F.col(specification_id)).alias("specification_id"),
                    _canon_key_col(F.col(specification_operation))
                    .alias("operation_id"),
                ).join(operation_bridge, "operation_id", "inner")
                holder_counts = holder.select(
                    _canon_key_col(F.col(holder_specification)).alias("specification_id")
                ).join(specification_roots, "specification_id", "inner").groupBy(
                    "root_id"
                ).count().withColumnRenamed("count", "holder_count")
                closure = closure.join(holder_counts, "root_id", "left")
            else:
                closure = closure.withColumn("holder_count", F.lit(0))
        else:
            for column in (
                "data_count", "launch_count", "specification_count", "holder_count",
            ):
                closure = closure.withColumn(column, F.lit(0))
        count_columns = [
            "deposit_count", "operation_count", "wallet_holder_count",
            "wallet_participant_count", "launch_count", "specification_count", "holder_count",
        ]
        closure = closure.fillna(0, count_columns + ["data_count"])
        closure_bad = closure.where(
            reduce(
                lambda left, right: left | right,
                [F.col(column) != 1 for column in count_columns]
                + [F.col("data_count") != 2],
            )
        )
        closure_count = closure_bad.count()
        out.append(Finding(
            "8g.profile.async_closure", "LCA observed registration profile",
            SEV_WARN if closure_count else SEV_INFO, "OPERACAO", closure_count == 0,
            count=closure_count, column=",".join(count_columns + ["data_count"]),
            sample=_sample_keys(closure_bad, ["root_id"], sample),
            hint=("Observed one deposit/operation/lancamento/specification/wallet closure and "
                  "two DADO rows are advisory asynchronous state, never hard."
                  if closure_count else ""),
            message="LCA asynchronous closure differs from the observed 1/1/2/1/1/1/1 state.",
        ))
        data = tables.get("DADO_OPERACAO")
        data_operation = resolve(data, "NUM_ID_OPERACAO") if data is not None else None
        data_type = resolve(data, "NUM_ID_TIPO_DADO_OPERACAO") if data is not None else None
        if data_operation and data_type and operation_bridge is not None:
            data_types = data.select(
                _canon_key_col(F.col(data_operation)).alias("operation_id"),
                _canon_key_col(F.col(data_type)).alias("data_type"),
            ).groupBy("operation_id").agg(
                F.sort_array(F.collect_list("data_type")).alias("data_types")
            )
            bad = operation_bridge.join(data_types, "operation_id", "left").where(
                ~F.coalesce(
                    F.col("data_types") == F.array(F.lit("287"), F.lit("288")), F.lit(False)
                )
            )
            count = bad.count()
            out.append(Finding(
                "8g.profile.operation_data_types", "LCA observed registration profile",
                SEV_WARN if count else SEV_INFO, "DADO_OPERACAO", count == 0, count=count,
                column="NUM_ID_TIPO_DADO_OPERACAO",
                sample=_sample_keys(bad, ["root_id", "operation_id", "data_types"], sample),
                hint="The observed exact 287+288 pair is advisory." if count else "",
                message="LCA operation data differs from observed types 287+288.",
            ))
        else:
            out.append(_lca_unavailable(
                "8g.profile.operation_data_types", ["DADO_OPERACAO/OPERACAO required columns"]
            ))
    else:
        missing = ["INSTRUMENTO_FINANCEIRO/EVENTO required columns"]
        out.extend([
            _lca_unavailable("8g.profile.event_dml_counts", missing),
            _lca_unavailable("8g.profile.async_closure", missing),
            _lca_unavailable("8g.profile.operation_data_types", missing),
        ])
    return out


# Category 7 - Shape conformance (per-IF cardinalities)
# ---------------------------------------------------------------------------
# Counting core kept in sync with scripts/profile_cdb_shapes.py: same universe
# (NUM_TIPO_IF=49, DAT_EXCLUSAO IS NULL), same active-row rule, same shape
# signature format ("TABLE=n|TABLE=n|..."). Schema-v2 metric names and order are
# validated against the selected product's metric inventory before any shape action.
SHAPE_ROOT_TABLE = "INSTRUMENTO_FINANCEIRO"
SHAPE_ROOT_KEY = "NUM_IF"
SHAPE_TIPO_IF = 49
# metric/table -> (bridge table, bridge key) for children not keyed by NUM_IF.
SHAPE_VIA: Dict[str, Tuple[str, str]] = {
    "RESGATE": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "JUROS_FLUTUANTE": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "JUROS_FIXO": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "ATUALIZACAO_POS": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "ATUALIZACAO_PRE": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "SPREAD": ("CONDICAO_IF", "NUM_CONDICAO_IF"),
    "DADO_OPERACAO": ("OPERACAO", "NUM_ID_OPERACAO"),
    "LANCAMENTO": ("OPERACAO", "NUM_ID_OPERACAO"),
    "ESPECIFICACAO": ("OPERACAO", "NUM_ID_OPERACAO"),
    "ESPECIFICACAO_COMITENTE": ("ESPECIFICACAO", "NUM_ID_ESPECIFICACAO"),
}
# Filtered metrics: metric name -> (source table, filter column, normalized value).
# Mirrors profile_cdb_shapes.py METRICS entries with a `where`; every domain IF
# has an evento tipo 85 and ~96% a tipo 83, so counting them separately stops a
# generator from passing "EVENTO=2" with two same-tipo eventos.
SHAPE_FILTERED: Dict[str, Tuple[str, str, str]] = {
    "EVENTO_TIPO83": ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "83"),
    "EVENTO_TIPO85": ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "85"),
    "EVENTO_TIPO84": ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "84"),
    "CONDICAO_IF_TIPO1": ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF", "1"),
    "CONDICAO_IF_TIPO3": ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF", "3"),
    "CONDICAO_IF_TIPO5": ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF", "5"),
    "CONDICAO_IF_TIPO20": ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF", "20"),
}
DEFAULT_SHAPE_METRICS: List[str] = [
    "TITULO", "CREDITO", "CONDICAO_IF", "RESGATE", "JUROS_FLUTUANTE", "JUROS_FIXO",
    "ATUALIZACAO_POS", "ATUALIZACAO_PRE", "SPREAD",
    "EVENTO", "EVENTO_TIPO83", "EVENTO_TIPO85",
    "OPERACAO", "DADO_OPERACAO", "LANCAMENTO", "DEPOSITO_AUTOMATICO_IF",
    "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
]
LCI_SHAPE_METRICS = [
    metric for metric in DEFAULT_SHAPE_METRICS
    if metric not in {"ATUALIZACAO_PRE", "SPREAD"}
]
LCA_SHAPE_METRICS = [
    "ENTIDADE", "REPRESENTANTE_IF", "TITULO", "IF_LCA", "CREDITO", "GARANTIA",
    "CONDICAO_IF", "CONDICAO_IF_TIPO1", "CONDICAO_IF_TIPO3", "CONDICAO_IF_TIPO5",
    "CONDICAO_IF_TIPO20", "AMORTIZACAO", "JUROS_FLUTUANTE", "SPREAD", "RESGATE",
    "EVENTO", "EVENTO_TIPO83", "EVENTO_TIPO84", "EVENTO_TIPO85", "DEPOSITO",
    "OPERACAO", "DADO_OPERACAO", "LANCAMENTO", "ESPECIFICACAO",
    "ESPECIFICACAO_COMITENTE", "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
]
SHAPE_METRICS_BY_PIPELINE = {
    "instrumento_financeiro": DEFAULT_SHAPE_METRICS,
    "lci": LCI_SHAPE_METRICS,
    "lca": LCA_SHAPE_METRICS,
}
BASELINE_DOMAIN_VERSION = 1
BASELINE_METRIC_VERSION = 2


def _shape_active(df: DataFrame) -> DataFrame:
    col = resolve(df, "DAT_EXCLUSAO")
    return df.where(_oracle_null_equivalent(F.col(col))) if col else df


def _shape_universe(
    tables: Dict[str, DataFrame], num_tipo_if: int = SHAPE_TIPO_IF
) -> Optional[DataFrame]:
    root = tables.get(SHAPE_ROOT_TABLE)
    if root is None:
        return None
    tipo, key = resolve(root, "NUM_TIPO_IF"), resolve(root, SHAPE_ROOT_KEY)
    if not tipo or not key:
        return None
    df = _shape_active(root.where(F.col(tipo).cast("long") == num_tipo_if))
    return df.select(F.col(key).cast("long").alias(SHAPE_ROOT_KEY)).dropDuplicates()


def _shape_counts(
    universe: DataFrame, tables: Dict[str, DataFrame], metric_names: List[str]
) -> Tuple[DataFrame, List[str]]:
    """Left-join per-metric row counts onto the universe; returns (df, skipped)."""
    result, skipped = universe, []
    for name in metric_names:
        source_table, wcol_name, wval = name, None, None
        if name == "DEPOSITO":
            source_table = "DEPOSITO_AUTOMATICO_IF"
        if name in SHAPE_FILTERED:
            source_table, wcol_name, wval = SHAPE_FILTERED[name]
        df = tables.get(source_table)
        keyed = None
        if df is not None:
            df = _shape_active(df)
            if wcol_name is not None:
                wcol = resolve(df, wcol_name)
                df = df.where(_norm_code(F.col(wcol)) == wval) if wcol else None
        if df is not None:
            if name in {"ENTIDADE", "REPRESENTANTE_IF"}:
                if_lca = tables.get("IF_LCA")
                representative = tables.get("REPRESENTANTE_IF")
                entity_key = resolve(df, "NUM_ID_ENTIDADE")
                rep_key = (
                    resolve(representative, "NUM_ID_ENTIDADE")
                    if representative is not None else None
                )
                lca_entity = (
                    resolve(if_lca, "NUM_ID_ENT_DEPOSITARIO_ORIG")
                    if if_lca is not None else None
                )
                lca_if = resolve(if_lca, SHAPE_ROOT_KEY) if if_lca is not None else None
                if all((if_lca is not None, representative is not None, entity_key,
                        rep_key, lca_entity, lca_if)):
                    route = _shape_active(if_lca).select(
                        F.col(lca_entity).cast("long").alias("entity_id"),
                        F.col(lca_if).cast("long").alias(SHAPE_ROOT_KEY),
                    )
                    child = df.select(F.col(entity_key).cast("long").alias("entity_id"))
                    if name == "ENTIDADE":
                        reps = _shape_active(representative).select(
                            F.col(rep_key).cast("long").alias("entity_id")
                        ).dropDuplicates()
                        child = child.join(reps, "entity_id", "inner")
                    keyed = child.join(route, "entity_id", "inner").select(SHAPE_ROOT_KEY)
            elif name == "ESPECIFICACAO_COMITENTE":
                specification = tables.get("ESPECIFICACAO")
                operation = tables.get("OPERACAO")
                child_spec = resolve(df, "NUM_ID_ESPECIFICACAO")
                spec_key = resolve(specification, "NUM_ID_ESPECIFICACAO") \
                    if specification is not None else None
                spec_operation = resolve(specification, "NUM_ID_OPERACAO") \
                    if specification is not None else None
                operation_key = resolve(operation, "NUM_ID_OPERACAO") \
                    if operation is not None else None
                operation_if = resolve(operation, SHAPE_ROOT_KEY) \
                    if operation is not None else None
                if all((specification is not None, operation is not None, child_spec,
                        spec_key, spec_operation, operation_key, operation_if)):
                    specifications = _shape_active(specification).select(
                        F.col(spec_key).cast("long").alias("spec_id"),
                        F.col(spec_operation).cast("long").alias("operation_id"),
                    )
                    operations = _shape_active(operation).select(
                        F.col(operation_key).cast("long").alias("operation_id"),
                        F.col(operation_if).cast("long").alias(SHAPE_ROOT_KEY),
                    )
                    keyed = df.select(
                        F.col(child_spec).cast("long").alias("spec_id")
                    ).join(specifications, "spec_id", "inner").join(
                        operations, "operation_id", "inner"
                    ).select(SHAPE_ROOT_KEY)
            elif name in SHAPE_VIA:
                bridge_table, bridge_key = SHAPE_VIA[name]
                bridge = tables.get(bridge_table)
                ck = resolve(df, bridge_key)
                if bridge is not None and ck:
                    bk = resolve(bridge, bridge_key)
                    bif = resolve(bridge, SHAPE_ROOT_KEY)
                    if bk and bif:
                        bridge = _shape_active(bridge).select(
                            F.col(bk).cast("long").alias("bk"),
                            F.col(bif).cast("long").alias(SHAPE_ROOT_KEY),
                        )
                        keyed = (df.select(F.col(ck).cast("long").alias("bk"))
                                 .join(bridge, "bk", "inner").select(SHAPE_ROOT_KEY))
            else:
                key = resolve(df, SHAPE_ROOT_KEY)
                if key:
                    keyed = df.select(F.col(key).cast("long").alias(SHAPE_ROOT_KEY))
        if keyed is None:
            skipped.append(name)
            result = result.withColumn(name, F.lit(0).cast("long"))
            continue
        counts = keyed.groupBy(SHAPE_ROOT_KEY).agg(F.count(F.lit(1)).alias(name))
        result = result.join(counts, SHAPE_ROOT_KEY, "left")
        result = result.withColumn(name, F.coalesce(F.col(name), F.lit(0)))
    return result, skipped


def _load_shape_baseline(spark: SparkSession, path: str) -> dict:
    """Load a baseline without deriving metrics from an untrusted shape signature."""
    baseline = json.loads(read_text(spark, path))
    shapes = baseline.get("shapes") or []
    if not shapes:
        raise ValueError(f"Baseline {path} has no 'shapes' section.")
    if not baseline.get("filtros_fonte_applied"):
        logger.warning(
            "Shape baseline %s was built WITHOUT --apply-filtros-fonte; the comparison "
            "conflates filter effects with generation distortions.", path)
    return baseline


def _current_baseline_identity(tables: Dict[str, DataFrame]) -> dict:
    """Derive source-key provenance from the clone map using the profiler algorithm."""
    clone_map = tables.get(MAPA_CLONE_NUM_IF_TABLE)
    if clone_map is None:
        return {
            "map_mode": "population",
            "source_key_count": None,
            "source_key_fingerprint": None,
        }
    source_key = resolve(clone_map, "NUM_IF_ORIG")
    if not source_key:
        return {
            "map_mode": "invalid-clone-map",
            "source_key_count": None,
            "source_key_fingerprint": None,
        }
    keys = clone_map.select(
        F.col(source_key).cast("long").alias(SHAPE_ROOT_KEY)
    ).where(F.col(SHAPE_ROOT_KEY).isNotNull()).dropDuplicates()
    row = keys.agg(
        F.count(F.lit(1)).alias("n"),
        F.sha2(
            F.concat_ws(",", F.sort_array(F.collect_list(F.col(SHAPE_ROOT_KEY).cast("string")))),
            256,
        ).alias("fingerprint"),
    ).first()
    return {
        "map_mode": "exact-source-keys",
        "source_key_count": int(row["n"]),
        "source_key_fingerprint": row["fingerprint"],
    }


def _baseline_incompatibility(
    baseline: dict,
    profile: ValidationProfile,
    current_identity: Optional[dict] = None,
) -> Optional[str]:
    """Reject a baseline whose product, metric contract, or provenance is incompatible.

    A schema-v1 (untagged) baseline is only tolerated for cdb_simplificado, and only as a
    legacy bridge; every other case is an error so an RDB baseline can never be consumed as
    type 49 and a simplificado baseline can never validate full CDB/RDB.
    """
    version = baseline.get("schema_version")
    if version is None:
        if profile.name != "cdb_simplificado":
            return ("legacy untagged baseline (schema_version missing) is only allowed for "
                    "cdb_simplificado")
        return None  # tolerated bridge for the deployed simplificado app
    if int(version) != 2:
        return f"unsupported baseline schema_version={version} (expected 2)"
    b_product = baseline.get("product")
    if b_product is not None and str(b_product) != profile.name:
        return f"baseline product {b_product!r} != selected product {profile.name!r}"
    b_type = baseline.get("num_tipo_if")
    if b_type is not None and int(b_type) != profile.num_tipo_if:
        return f"baseline num_tipo_if={b_type} != profile num_tipo_if={profile.num_tipo_if}"
    required = (
        "product", "num_tipo_if", "domain_version", "metric_version", "metrics",
        "map_mode", "source_key_count", "source_key_fingerprint",
    )
    missing = [field for field in required if field not in baseline]
    if missing:
        return f"baseline identity field(s) missing: {missing}"
    if int(baseline["domain_version"]) != BASELINE_DOMAIN_VERSION:
        return (f"baseline domain_version={baseline['domain_version']} != validator "
                f"domain_version={BASELINE_DOMAIN_VERSION}")
    if int(baseline["metric_version"]) != BASELINE_METRIC_VERSION:
        return (f"baseline metric_version={baseline['metric_version']} != validator "
                f"metric_version={BASELINE_METRIC_VERSION}")
    expected_metrics = SHAPE_METRICS_BY_PIPELINE.get(profile.pipeline, DEFAULT_SHAPE_METRICS)
    if baseline["metrics"] != expected_metrics:
        return "baseline metrics do not match the validator metric contract"
    if current_identity is not None:
        for field in ("map_mode", "source_key_count", "source_key_fingerprint"):
            if baseline[field] != current_identity.get(field):
                return (f"baseline {field}={baseline[field]!r} != synthetic output "
                        f"{field}={current_identity.get(field)!r}")
    return None


def check_shapes(
    spark: SparkSession,
    tables: Dict[str, DataFrame],
    baseline_path: Optional[str],
    sample: int,
    unseen_tol_pct: float,
    drift_tol: float,
    op_ratio_tol_pct: float,
    profile: ValidationProfile,
    skip_prefixes: Optional[List[str]] = None,
) -> List[Finding]:
    skip_prefixes = skip_prefixes or []
    run_unseen = not _check_is_skipped("7a.unseen_shapes", skip_prefixes)
    run_drift = not _check_is_skipped("7b.distribution_drift", skip_prefixes)
    run_op_ratio = (
        SHAPE_RULE_OP_RATIO in profile.hard_shape_rules
        and not _check_is_skipped("7c.op_ratio", skip_prefixes)
    )
    run_resgate_max = (
        SHAPE_RULE_RESGATE_MAX in profile.hard_shape_rules
        and not _check_is_skipped("7d.resgate_multiplicity", skip_prefixes)
    )
    out: List[Finding] = []
    cat = "Shape conformance"
    if not profile.hard_shape_rules and not baseline_path:
        return [Finding("7.shapes", cat, SEV_INFO, SHAPE_ROOT_TABLE, True,
                        message=f"No shape rules enabled for product {profile.name}; "
                                "shape checks skipped (see capability ledger).")]
    universe = _shape_universe(tables, profile.num_tipo_if)
    if universe is None:
        return [Finding("7.shapes", cat, SEV_INFO, SHAPE_ROOT_TABLE, True,
                        message="INSTRUMENTO_FINANCEIRO not in output; shape checks skipped.")]

    baseline_pct: Optional[dict] = None
    metric_names = SHAPE_METRICS_BY_PIPELINE.get(profile.pipeline, DEFAULT_SHAPE_METRICS)
    if baseline_path:
        try:
            baseline_raw = _load_shape_baseline(spark, baseline_path)
            current_identity = _current_baseline_identity(tables)
            incompat = _baseline_incompatibility(baseline_raw, profile, current_identity)
            if incompat:
                out.append(Finding("7.baseline_incompatible", cat, SEV_ERROR,
                                   SHAPE_ROOT_TABLE, False,
                                   hint="Produce a baseline for THIS product with "
                                        "profile_cdb_shapes.py --product "
                                        f"{profile.name}.",
                                    message=f"Incompatible shape baseline: {incompat}."))
                return out
            shapes = baseline_raw["shapes"]
            baseline_pct = {shape["shape"]: float(shape["pct"]) for shape in shapes}
            if baseline_raw.get("schema_version") is None:
                metric_names = [
                    part.split("=", 1)[0] for part in shapes[0]["shape"].split("|")
                ]
                out.append(Finding(
                    "7.baseline_legacy", cat, SEV_WARN, SHAPE_ROOT_TABLE, False,
                    hint="Regenerate this baseline with schema v2 source-key provenance.",
                    message="Legacy untagged baseline accepted only for cdb_simplificado; "
                            "strict shape coverage is unavailable.",
                ))
            else:
                metric_names = list(baseline_raw["metrics"])
        except Exception as exc:  # noqa: BLE001
            out.append(Finding("7.baseline", cat, SEV_ERROR, SHAPE_ROOT_TABLE, False,
                               hint="Regenerate it with profile_cdb_shapes.py "
                                    "--apply-filtros-fonte --product for this product.",
                               message=f"Could not load shape baseline {baseline_path}: {exc}"))
            return out

    run_distribution = (
        SHAPE_RULE_DISTRIBUTION in profile.hard_shape_rules
        and (run_unseen or run_drift)
    )
    if not (run_op_ratio or run_resgate_max or run_distribution):
        return out
    if baseline_path is None:
        metric_names = []
        if run_op_ratio:
            metric_names.extend(("OPERACAO", "DADO_OPERACAO", "LANCAMENTO"))
        if run_resgate_max:
            metric_names.append("RESGATE")

    counts, skipped = _shape_counts(universe, tables, metric_names)
    if skipped:
        out.append(Finding("7.metrics_skipped", cat, SEV_WARN, ",".join(skipped), False,
                           hint="Missing tables count as 0 rows per IF, which distorts the "
                                "shape comparison. Include them in the output/tables list.",
                           message=f"Shape metrics counted as 0 (table/columns unavailable): "
                                   f"{skipped}"))
    counts = counts.cache()
    total = counts.count()
    if total == 0:
        counts.unpersist()
        out.append(Finding("7.shapes", cat, SEV_WARN, SHAPE_ROOT_TABLE, False,
                           message=f"No active IFs (NUM_TIPO_IF={profile.num_tipo_if}) in the "
                                   "output."))
        return out

    # 7c - operation ratio invariant: DADO_OPERACAO = 2*OPERACAO, LANCAMENTO = OPERACAO.
    # Holds for ~99% of production IFs that have operations; a synthetic output that
    # binds these tables independently violates it almost everywhere.
    if (run_op_ratio
            and all(name in metric_names for name in ("OPERACAO", "DADO_OPERACAO", "LANCAMENTO"))):
        with_ops = counts.where(F.col("OPERACAO") > 0)
        n_ops = with_ops.count()
        if n_ops:
            bad = with_ops.where(
                (F.col("DADO_OPERACAO") != 2 * F.col("OPERACAO"))
                | (F.col("LANCAMENTO") != F.col("OPERACAO"))
            )
            c = bad.count()
            pct = 100.0 * c / n_ops
            out.append(Finding(
                "7c.op_ratio", cat,
                SEV_ERROR if pct > op_ratio_tol_pct else SEV_INFO,
                "OPERACAO", pct <= op_ratio_tol_pct, count=c,
                column="OPERACAO,DADO_OPERACAO,LANCAMENTO",
                sample=_sample_keys(bad.select(SHAPE_ROOT_KEY), [SHAPE_ROOT_KEY], sample),
                hint="Every production operação carries exactly 2 DADO_OPERACAO and "
                     "1 LANCAMENTO. Generate/bind the three tables as one unit per operação.",
                message=f"IFs violating OPERACAO:DADO_OPERACAO:LANCAMENTO = 1:2:1 "
                        f"({pct:.1f}% of {n_ops} IFs with operações; tolerance "
                        f"{op_ratio_tol_pct}%).",
            ))

    # 7d - CDB simplificado RESGATE multiplicity: schedule rows belong below one parent.
    if run_resgate_max and "RESGATE" in metric_names:
        multi = counts.where(F.col("RESGATE") > 1)
        c = multi.count()
        out.append(Finding(
            "7d.resgate_multiplicity", cat,
            SEV_ERROR if c else SEV_INFO, "RESGATE", c == 0, count=c, column="RESGATE",
            sample=_sample_keys(multi.select(SHAPE_ROOT_KEY), [SHAPE_ROOT_KEY], sample),
            hint="CDB simplificado expects at most one RESGATE condition per IF.",
            message="IFs with more than one RESGATE row.",
        ))

    # 7a/7b - distribution checks against the baseline profile.
    if SHAPE_RULE_DISTRIBUTION not in profile.hard_shape_rules:
        counts.unpersist()
        return out
    if not (run_unseen or run_drift):
        counts.unpersist()
        return out
    if baseline_pct is None:
        out.append(Finding(
            "7.baseline", cat, SEV_WARN, SHAPE_ROOT_TABLE, False,
            hint="Produce it once with: spark-submit profile_cdb_shapes.py "
                 "--base-uri <raw> --apply-filtros-fonte --report-path <json> "
                 "and pass it via --shape-baseline.",
            message="No --shape-baseline given; shape distribution checks skipped.",
        ))
        counts.unpersist()
        return out

    sig = F.concat_ws("|", *[
        F.concat(F.lit(f"{name}="), F.col(name).cast("string")) for name in metric_names
    ])
    dist_rows = (counts.withColumn("shape", sig).groupBy("shape")
                 .agg(F.count(F.lit(1)).alias("n"),
                      F.slice(F.collect_list(F.col(SHAPE_ROOT_KEY)), 1, sample).alias("ids"))
                 .collect())
    syn_pct = {r["shape"]: 100.0 * r["n"] / total for r in dist_rows}
    syn_ids = {r["shape"]: [int(x) for x in (r["ids"] or [])] for r in dist_rows}

    if run_unseen:
        unseen = sorted(
            ((s, p) for s, p in syn_pct.items() if s not in baseline_pct),
            key=lambda kv: -kv[1],
        )
        unseen_mass = sum(p for _, p in unseen)
        out.append(Finding(
            "7a.unseen_shapes", cat,
            SEV_ERROR if unseen_mass > unseen_tol_pct else SEV_INFO,
            SHAPE_ROOT_TABLE, unseen_mass <= unseen_tol_pct,
            count=len(unseen), column="shape",
            sample=[{"shape": s, "pct": round(p, 3), "sample_num_if": syn_ids[s][:5]}
                    for s, p in unseen[:sample]],
            hint="These per-IF cardinality combinations never occur in the (filtered) "
                 "production data — the generator invented them. Per-IF cluster sampling "
                 "eliminates this class by construction.",
            message=f"{unseen_mass:.1f}% of synthetic IFs have a shape absent from the "
                    f"baseline (tolerance {unseen_tol_pct}%).",
        ))

    if run_drift:
        tvd = 0.5 * sum(
            abs(syn_pct.get(s, 0.0) - baseline_pct.get(s, 0.0))
            for s in set(syn_pct) | set(baseline_pct)
        ) / 100.0
        drifted = sorted(
            set(syn_pct) | set(baseline_pct),
            key=lambda s: -abs(syn_pct.get(s, 0.0) - baseline_pct.get(s, 0.0)),
        )
        out.append(Finding(
            "7b.distribution_drift", cat,
            SEV_ERROR if tvd > drift_tol else SEV_INFO,
            SHAPE_ROOT_TABLE, tvd <= drift_tol, column="shape",
            sample=[{"shape": s,
                     "synthetic_pct": round(syn_pct.get(s, 0.0), 3),
                     "baseline_pct": round(baseline_pct.get(s, 0.0), 3)}
                    for s in drifted[:sample]],
            hint="The synthetic shape distribution should converge to the filtered-raw "
                 "baseline. See docs/cdb-shapes-findings.md for the full analysis.",
            message=f"Total variation distance between synthetic and baseline shape "
                    f"distributions = {tvd:.3f} (tolerance {drift_tol}).",
        ))

    counts.unpersist()
    return out


# ---------------------------------------------------------------------------
# Category 8 - Log-derived registration invariants
# ---------------------------------------------------------------------------
REGISTRATION_CONSTANTS: Dict[str, Dict[str, object]] = {
    "INSTRUMENTO_FINANCEIRO": {
        "NUM_SISTEMA": 55,
        "IND_AGENDA_CONSTANTE": "S",
        "IND_ESPECIFICA_COMITENTE": "N",
        "NUM_ID_MOTIVO_SITUACAO_IF": 25,
        "IND_MANTEM_PREMIO": "N",
        "IND_EXCLUI_IOF": "N",
        "IND_ELEGIVEL_IOF": "N",
    },
    "TITULO": {"IND_FRACIONAMENTO": "N", "NUM_ID_TIPO_REGIME_TITULO": 2},
    "CONDICAO_IF": {
        "COD_TIPO_UNIDADE_TEMPO_PAGA": "F",
        "QTD_UNID_TEMPO_PAGAMENTO": 1,
        "NUM_ID_ORIG_DESL_LIQ": 0,
    },
    "JUROS_FLUTUANTE": {
        "IND_ANO_COMERCIAL": 2,
        "IND_DIAS_CORRIDOS": 1,
        "NUM_ID_TIPO_INDICADOR": 0,
        "NOM_AGENDA_PAGAMENTO": "CONSTANTE",
    },
    "RESGATE": {"COD_TIPO_EXERCICIO": "EUROPEIA"},
    "EVENTO": {"NUM_ID_ESTADO_EVENTO": 1, "IND_INCORPORA": "N"},
}

# Bind the empirical (cetip.out-derived) registration profile to CDB simplificado only.
# Full CDB and RDB deliberately leave registration_constants=None (simplificado-only).
VALIDATION_PROFILES["cdb_simplificado"] = replace(
    VALIDATION_PROFILES["cdb_simplificado"], registration_constants=REGISTRATION_CONSTANTS
)
CDB_SIMPLIFICADO_PROFILE = VALIDATION_PROFILES["cdb_simplificado"]


def _cat8_unavailable(check_id: str, table: str, missing: List[str]) -> Finding:
    return Finding(
        check_id,
        "Log-derived invariants",
        SEV_WARN,
        table,
        False,
        hint="Include the required table and columns in the synthetic validation input.",
        message=f"Check unavailable; missing required table/column(s): {', '.join(missing)}.",
    )


def _cat8_bad_rows(
    check_id: str,
    table: str,
    column: str,
    bad: DataFrame,
    key_cols: List[str],
    sample: int,
    severity: str,
    hint: str,
    message: str,
) -> Finding:
    count = bad.count()
    return Finding(
        check_id,
        "Log-derived invariants",
        severity if count else SEV_INFO,
        table,
        count == 0,
        count=count,
        column=column,
        sample=_sample_keys(bad, key_cols, sample) if count else [],
        hint=hint if count else "",
        message=message,
    )


def _cat8_active_cdb(tables: Dict[str, DataFrame], required: List[str], num_tipo_if: int = 49):
    table = "INSTRUMENTO_FINANCEIRO"
    df = tables.get(table)
    if df is None:
        return None, [table]
    columns = {name: resolve(df, name) for name in required}
    missing = [name for name, actual in columns.items() if not actual]
    if missing:
        return None, missing
    return df.where(
        (_norm_code(F.col(columns["NUM_TIPO_IF"])) == str(num_tipo_if))
        & _oracle_null_equivalent(F.col(columns["DAT_EXCLUSAO"]))
    ), []


def _cat8_type_mix(
    tables: Dict[str, DataFrame],
    check_id: str,
    parent_table: str,
    parent_key: str,
    child_table: str,
    child_key: str,
    type_column: str,
    expected_types: Tuple[str, str],
    sample: int,
    parent_df: Optional[DataFrame] = None,
) -> Finding:
    parent = parent_df if parent_df is not None else tables.get(parent_table)
    child = tables.get(child_table)
    missing_tables = [
        table for table, df in ((parent_table, parent), (child_table, child)) if df is None
    ]
    if missing_tables:
        return _cat8_unavailable(
            check_id, f"{parent_table},{child_table}", missing_tables
        )

    pkey = resolve(parent, parent_key)
    ckey = resolve(child, child_key)
    ctype = resolve(child, type_column)
    missing = [
        name
        for name, actual in ((parent_key, pkey), (child_key, ckey), (type_column, ctype))
        if not actual
    ]
    if missing:
        return _cat8_unavailable(check_id, f"{parent_table},{child_table}", missing)

    parents = parent.select(F.col(pkey).cast("string").alias("parent_key")).dropDuplicates()
    children = _shape_active(child).select(
        F.col(ckey).cast("string").alias("parent_key"),
        _norm_code(F.col(ctype)).alias("child_type"),
    )
    counts = children.groupBy("parent_key").agg(
        F.count(F.lit(1)).alias("total"),
        *[
            F.sum(F.when(F.col("child_type") == value, 1).otherwise(0)).alias(
                f"type_{value}"
            )
            for value in expected_types
        ],
    )
    joined = parents.join(counts, "parent_key", "left").fillna(0)
    bad = joined.where(
        (F.col("total") != len(expected_types))
        | reduce(
            lambda left, right: left | right,
            [F.col(f"type_{value}") != 1 for value in expected_types],
        )
    )
    return _cat8_bad_rows(
        check_id,
        child_table,
        type_column,
        bad,
        ["parent_key"],
        sample,
        SEV_WARN,
        f"Generate exactly one child of each type {expected_types} for every parent.",
        f"Parents must have exactly one child of each type {expected_types} and no extras.",
    )


def check_log_invariants(
    tables: Dict[str, DataFrame], sample: int, registration_profile: bool = False,
    profile: Optional["ValidationProfile"] = None,
) -> List[Finding]:
    if profile is None:
        profile = CDB_SIMPLIFICADO_PROFILE
    out: List[Finding] = []
    active, active_missing = _cat8_active_cdb(
        tables, ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "COD_IF"], profile.num_tipo_if
    )
    if active is None:
        out.append(
            _cat8_unavailable("8a.cod_if_unique", "INSTRUMENTO_FINANCEIRO", active_missing)
        )
    else:
        num_if, cod_if = resolve(active, "NUM_IF"), resolve(active, "COD_IF")
        rows = active.select(
            F.col(num_if),
            F.col(cod_if),
            _norm_code(F.col(cod_if)).alias("normalized_cod_if"),
        ).where(
            F.col(cod_if).isNotNull()
            & (_norm_code(F.col(cod_if)) != "")
        )
        duplicates = (
            rows.groupBy("normalized_cod_if")
            .count()
            .where(F.col("count") > 1)
            .select("normalized_cod_if")
        )
        out.append(
            _cat8_bad_rows(
                "8a.cod_if_unique",
                "INSTRUMENTO_FINANCEIRO",
                "COD_IF",
                rows.join(duplicates, "normalized_cod_if", "inner"),
                [num_if, cod_if],
                sample,
                SEV_ERROR,
                "Assign a distinct COD_IF to every active synthetic CDB.",
                "Active synthetic CDB rows participating in duplicate COD_IF values.",
            )
        )

    operation = tables.get(OPERACAO_TABLE)
    match_check_id = "8a.operacao_cod_if_match"
    if active is None or operation is None:
        missing_match = active_missing if active is None else []
        if operation is None:
            missing_match = [*missing_match, OPERACAO_TABLE]
        out.append(
            _cat8_unavailable(
                match_check_id,
                f"INSTRUMENTO_FINANCEIRO,{OPERACAO_TABLE}",
                missing_match,
            )
        )
    else:
        root_cols = {name: resolve(active, name) for name in ("NUM_IF", "COD_IF")}
        op_cols = {
            name: resolve(operation, name)
            for name in ("NUM_ID_OPERACAO", "NUM_IF", "COD_IF")
        }
        missing_match = [
            name
            for name, actual in {**root_cols, **op_cols}.items()
            if not actual
        ]
        if missing_match:
            out.append(
                _cat8_unavailable(
                    match_check_id,
                    f"INSTRUMENTO_FINANCEIRO,{OPERACAO_TABLE}",
                    missing_match,
                )
            )
        else:
            roots = (
                active.select(
                    _canon_key_col(F.col(root_cols["NUM_IF"])).alias("normalized_num_if"),
                    F.trim(F.col(root_cols["COD_IF"]).cast("string")).alias("root_cod_if"),
                )
                .where(
                    F.col("normalized_num_if").isNotNull()
                    & (F.col("normalized_num_if") != "")
                )
                .groupBy("normalized_num_if")
                .agg(
                    F.count(F.lit(1)).alias("root_count"),
                    F.min("root_cod_if").alias("root_cod_if"),
                )
            )
            operations = operation.select(
                F.col(op_cols["NUM_ID_OPERACAO"]).alias("operation_id"),
                F.col(op_cols["NUM_IF"]).alias("operation_num_if"),
                _canon_key_col(F.col(op_cols["NUM_IF"])).alias("normalized_num_if"),
                F.trim(F.col(op_cols["COD_IF"]).cast("string")).alias(
                    "operation_cod_if"
                ),
            )
            compared = operations.join(roots, "normalized_num_if", "left")
            bad = compared.where(
                F.col("normalized_num_if").isNull()
                | (F.col("normalized_num_if") == "")
                | F.col("root_count").isNull()
                | (F.col("root_count") != 1)
                | F.col("operation_cod_if").isNull()
                | (F.col("operation_cod_if") == "")
                | F.col("root_cod_if").isNull()
                | (F.col("root_cod_if") == "")
                | ~(F.col("operation_cod_if") == F.col("root_cod_if"))
            )
            out.append(
                _cat8_bad_rows(
                    match_check_id,
                    OPERACAO_TABLE,
                    "NUM_IF,COD_IF",
                    bad,
                    ["operation_id", "operation_num_if", "root_cod_if", "operation_cod_if"],
                    sample,
                    SEV_ERROR,
                    "Copy the active root COD_IF to every operation sharing its normalized NUM_IF.",
                    "Operations with blank, mismatching, or unmatched active-root COD_IF values.",
                )
            )

    if operation is None:
        out.append(
            _cat8_unavailable("8a.cod_operacao_unique", OPERACAO_TABLE, [OPERACAO_TABLE])
        )
    else:
        cod_operacao = resolve(operation, "COD_OPERACAO")
        if not cod_operacao:
            out.append(
                _cat8_unavailable(
                    "8a.cod_operacao_unique", OPERACAO_TABLE, ["COD_OPERACAO"]
                )
            )
        else:
            op_key = resolve(operation, "NUM_ID_OPERACAO")
            rows = operation.select(
                *([F.col(op_key)] if op_key else []),
                F.col(cod_operacao),
                _norm_code(F.col(cod_operacao)).alias("normalized_cod_operacao"),
            ).where(
                F.col(cod_operacao).isNotNull()
                & (_norm_code(F.col(cod_operacao)) != "")
            )
            duplicates = (
                rows.groupBy("normalized_cod_operacao")
                .count()
                .where(F.col("count") > 1)
                .select("normalized_cod_operacao")
            )
            out.append(
                _cat8_bad_rows(
                    "8a.cod_operacao_unique",
                    OPERACAO_TABLE,
                    "COD_OPERACAO",
                    rows.join(duplicates, "normalized_cod_operacao", "inner"),
                    [c for c in (op_key, cod_operacao) if c],
                    sample,
                    SEV_ERROR,
                    "Assign a distinct non-empty COD_OPERACAO to every synthetic operation.",
                    "Synthetic operation rows participating in duplicate COD_OPERACAO values.",
                )
            )

    tuple_columns = [
        "NUM_ID_OPERACAO",
        "DAT_OPERACAO",
        "NUM_CONTA_PARTICIPANTE_P1",
        "NUM_CONTA_PARTICIPANTE_P2",
        "NUM_CONTROLE_LANCAMENTO_P1",
        "NUM_CONTROLE_LANCAMENTO_P2",
        "NUM_ID_TIPO_OPER_OBJETO_SERV",
    ]
    if operation is None:
        out.append(_cat8_unavailable("8a.meu_numero_unique", OPERACAO_TABLE, [OPERACAO_TABLE]))
    else:
        op_cols = {name: resolve(operation, name) for name in tuple_columns}
        missing_tuple = [name for name, actual in op_cols.items() if not actual]
        if missing_tuple:
            out.append(_cat8_unavailable("8a.meu_numero_unique", OPERACAO_TABLE, missing_tuple))
        else:
            projections = [
                operation.select(
                    F.col(op_cols["NUM_ID_OPERACAO"]).alias("operation_id"),
                    F.lit(side).alias("side"),
                    F.col(op_cols["DAT_OPERACAO"]).alias("operation_date"),
                    F.col(op_cols[f"NUM_CONTA_PARTICIPANTE_{side}"]).alias("account"),
                    F.col(op_cols[f"NUM_CONTROLE_LANCAMENTO_{side}"]).alias("control"),
                    F.col(op_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"]).alias("tos"),
                )
                for side in ("P1", "P2")
            ]
            flattened = projections[0].unionByName(projections[1])
            tuple_names = ["operation_date", "account", "control", "tos"]
            complete = flattened.where(
                reduce(
                    lambda left, right: left & right,
                    [
                        F.col(column).isNotNull()
                        & (F.trim(F.col(column).cast("string")) != "")
                        for column in tuple_names
                    ],
                )
            )
            duplicates = (
                complete.groupBy(*tuple_names)
                .count()
                .where(F.col("count") > 1)
                .select(*tuple_names)
            )
            out.append(
                _cat8_bad_rows(
                    "8a.meu_numero_unique",
                    OPERACAO_TABLE,
                    "DAT_OPERACAO,NUM_CONTA_PARTICIPANTE_P1/P2,"
                    "NUM_CONTROLE_LANCAMENTO_P1/P2,NUM_ID_TIPO_OPER_OBJETO_SERV",
                    complete.join(duplicates, tuple_names, "inner"),
                    ["operation_id", "side"],
                    sample,
                    SEV_ERROR,
                    "Regenerate participant control numbers so the flattened P1/P2 tuple "
                    "is unique.",
                    "P1/P2 projections participating in duplicate f_testa_meunumero tuples.",
                )
            )

    if not registration_profile:
        return out

    if active is None:
        out.append(
            _cat8_unavailable("8b.cod_if_format", "INSTRUMENTO_FINANCEIRO", active_missing)
        )
    elif profile.cod_if_pattern is None:
        out.append(Finding(
            "8b.cod_if_format", "Log-derived invariants", SEV_WARN,
            "INSTRUMENTO_FINANCEIRO", False, column="COD_IF",
            hint="Capture the target COD_IF registration format for this product before "
                 "enabling the format check; do not promote an assumed pattern.",
            message=f"COD_IF format not validated for product {profile.name} "
                    "(unresolved registration format).",
        ))
    else:
        num_if, cod_if = resolve(active, "NUM_IF"), resolve(active, "COD_IF")
        valid = F.coalesce(
            F.col(cod_if).cast("string").rlike(profile.cod_if_pattern),
            F.lit(False),
        )
        out.append(
            _cat8_bad_rows(
                "8b.cod_if_format",
                "INSTRUMENTO_FINANCEIRO",
                "COD_IF",
                active.where(~valid),
                [num_if, cod_if],
                sample,
                SEV_WARN,
                f"Generate COD_IF with the {profile.name} registration format.",
                f"Active {profile.name} COD_IF values outside {profile.cod_if_pattern}.",
            )
        )

    if operation is None:
        out.append(
            _cat8_unavailable("8b.cod_operacao_format", OPERACAO_TABLE, [OPERACAO_TABLE])
        )
    else:
        cod_operacao = resolve(operation, "COD_OPERACAO")
        if not cod_operacao:
            out.append(
                _cat8_unavailable("8b.cod_operacao_format", OPERACAO_TABLE, ["COD_OPERACAO"])
            )
        else:
            op_key = resolve(operation, "NUM_ID_OPERACAO")
            valid = F.coalesce(
                F.col(cod_operacao).cast("string").rlike(r"^[0-9]{16}$"), F.lit(False)
            )
            out.append(
                _cat8_bad_rows(
                    "8b.cod_operacao_format",
                    OPERACAO_TABLE,
                    "COD_OPERACAO",
                    operation.where(~valid),
                    [c for c in (op_key, cod_operacao) if c],
                    sample,
                    SEV_WARN,
                    "Generate COD_OPERACAO as exactly 16 decimal digits.",
                    "COD_OPERACAO values outside ^[0-9]{16}$.",
                )
            )

    for table, constants in (profile.registration_constants or {}).items():
        df = tables.get(table)
        check_id = f"8c.registration_constants.{table.lower()}"
        if df is None:
            out.append(_cat8_unavailable(check_id, table, [table]))
            continue
        columns = {name: resolve(df, name) for name in constants}
        missing_constants = [name for name, actual in columns.items() if not actual]
        if missing_constants:
            out.append(_cat8_unavailable(check_id, table, missing_constants))
            continue
        mismatch = reduce(
            lambda left, right: left | right,
            [
                ~F.coalesce(_norm_code(F.col(columns[name])) == str(value), F.lit(False))
                for name, value in constants.items()
            ],
        )
        keys = [
            column
            for name in ("NUM_IF", "NUM_CONDICAO_IF", "NUM_EVENTO")
            if (column := resolve(df, name))
        ]
        out.append(
            _cat8_bad_rows(
                check_id,
                table,
                ",".join(constants),
                df.where(mismatch),
                keys or df.columns[:1],
                sample,
                SEV_WARN,
                "Persist the current registration values for the listed profile columns.",
                "Rows differing from the empirical current-registration persisted profile.",
            )
        )

    # Condição/evento exact type-mixes are simplificado-only empirical shapes; a full CDB or
    # RDB legitimately differs, so they run only when the profile carries the registration
    # constants (i.e. cdb_simplificado). DADO_OPERACAO (502,503) stays enabled as an
    # empirical WARN for every product.
    if profile.registration_constants is not None:
        profile_parents, profile_missing = _cat8_active_cdb(
            tables, ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"], profile.num_tipo_if
        )
        if profile_parents is None:
            out.extend(
                _cat8_unavailable(check_id, "INSTRUMENTO_FINANCEIRO", profile_missing)
                for check_id in ("8c.condicao_type_mix", "8c.evento_type_mix")
            )
        else:
            out.append(
                _cat8_type_mix(
                    tables, "8c.condicao_type_mix", "INSTRUMENTO_FINANCEIRO", "NUM_IF",
                    "CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF", ("3", "20"), sample,
                    profile_parents,
                )
            )
            out.append(
                _cat8_type_mix(
                    tables, "8c.evento_type_mix", "INSTRUMENTO_FINANCEIRO", "NUM_IF",
                    "EVENTO", "NUM_IF", "NUM_TIPO_EVENTO_LEGADO", ("83", "85"), sample,
                    profile_parents,
                )
            )
    out.append(
        _cat8_type_mix(
            tables, "8c.dado_operacao_type_mix", OPERACAO_TABLE, "NUM_ID_OPERACAO",
            DADO_OPERACAO_TABLE, "NUM_ID_OPERACAO", "NUM_ID_TIPO_DADO_OPERACAO",
            ("502", "503"), sample,
        )
    )
    return out


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def emit_report(spark: SparkSession, findings: List[Finding],
                 report_path: Optional[str], fail_severity: str,
                 profile: ValidationProfile, resolved_input: str,
                 table_inventory: List[str], partial_reasons: List[str],
                 baseline_identity: Optional[dict] = None,
                 runtime_identity: Optional[dict] = None) -> int:
    fail_level = _SEV_ORDER[fail_severity.upper()]
    failing = [f for f in findings if (not f.passed) and _SEV_ORDER[f.severity] >= fail_level]

    # Coverage verdict:
    #   FAIL    -> any finding at/above fail-severity, or an identity/contract error;
    #   PARTIAL -> no FAIL, but required coverage is incomplete (unsupported required
    #              capability, diagnostic --tables subset, or skipped required checks);
    #   PASS    -> every required check ran and passed.
    unsupported_caps = list(profile.unsupported_required())
    reasons = list(partial_reasons)
    reasons += [f"unsupported required capability: {c}" for c in unsupported_caps]
    reasons += [
        f"non-passing warning: {finding.check_id}"
        for finding in findings
        if not finding.passed and finding.severity == SEV_WARN
    ]
    reasons = list(dict.fromkeys(reasons))
    if failing:
        verdict = "FAIL"
    elif reasons:
        verdict = "PARTIAL"
    else:
        verdict = "PASS"

    by_cat: Dict[str, List[Finding]] = {}
    for f in findings:
        by_cat.setdefault(f.category, []).append(f)

    print("\n" + "=" * 78)
    roots = {"credito_scr": CREDITO_SCR_TABLE, "dicre": CREDITO_DC_TABLE}
    identity = (
        f"root={roots[profile.pipeline]}" if profile.pipeline in roots
        else f"NUM_TIPO_IF={profile.num_tipo_if}"
    )
    print(f"SYNTHETIC OUTPUT VALIDATION — product={profile.name} ({identity})")
    print("=" * 78)
    print(f"input: {resolved_input}")
    for cat in sorted(by_cat):
        print(f"\n### {cat}")
        for f in by_cat[cat]:
            status = "PASS" if f.passed else f.severity
            head = f"  [{status:5}] {f.check_id:26} {f.table}"
            if f.column:
                head += f".{f.column}"
            if f.count:
                head += f"  (count={f.count})"
            print(head)
            if f.message:
                print(f"           {f.message}")
            if not f.passed and f.sample:
                print(f"           sample: {f.sample}")
            if not f.passed and f.hint:
                print(f"           FIX: {f.hint}")

    n_err = sum(1 for f in findings if not f.passed and f.severity == SEV_ERROR)
    n_warn = sum(1 for f in findings if not f.passed and f.severity == SEV_WARN)
    print("\n" + "-" * 78)
    print(f"SUMMARY: {len(findings)} checks | {n_err} ERROR | {n_warn} WARN | "
          f"VERDICT={verdict} (fail-severity={fail_severity})")
    if reasons and verdict != "FAIL":
        print("PARTIAL because:")
        for r in reasons:
            print(f"  - {r}")
    print("-" * 78)

    if report_path:
        payload = {
            "schema_version": 2,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "product": profile.name,
            "num_tipo_if": profile.num_tipo_if,
            "evidence_version": profile.evidence_version,
            "resolved_input": resolved_input,
            "table_inventory": sorted(table_inventory),
            "baseline_identity": baseline_identity,
            "spark_version": (runtime_identity or {}).get("spark_version"),
            "aqe_enabled": (runtime_identity or {}).get("aqe_enabled"),
            "fail_severity": fail_severity,
            "verdict": verdict,
            "failed": verdict == "FAIL",
            "partial_reasons": reasons,
            "required_capabilities": list(profile.required_capabilities),
            "supported_capabilities": list(profile.supported_capabilities),
            "unsupported_required_capabilities": unsupported_caps,
            "counts": {"error": n_err, "warn": n_warn, "total": len(findings)},
            "findings": [f.to_dict() for f in findings],
        }
        try:
            write_text(spark, report_path,
                       json.dumps(payload, ensure_ascii=False, indent=2, default=str))
            logger.info("JSON report written to %s", report_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not write report to %s: %s", report_path, exc)

    # A PARTIAL run is not a pass: return non-zero so CI cannot treat incomplete
    # coverage as green.
    return 0 if verdict == "PASS" else 1


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate a synthetic financial product output against application rules.")
    p.add_argument("--product", required=True, choices=sorted(VALIDATION_PROFILES),
                   help="Product profile to validate against (selects identity type, object "
                        "service, domain, and which strict checks are supported).")
    p.add_argument("--input-base", default=None,
                   help="Explicit synthetic input tree (local or oci:// URI). Overrides "
                        "DATAGEN_SYNTHETIC_BASE_URI + DATAGEN_CLONE_PREFIX / profile prefix.")
    p.add_argument("--tables", default=None,
                   help="Comma-separated table list (DIAGNOSTIC ONLY: any explicit subset "
                        "marks coverage partial and can never yield strict PASS).")
    p.add_argument("--report-path", default=None,
                   help="Write a JSON report to this path (local or oci:// URI).")
    p.add_argument("--shape-baseline", default=None,
                   help="Shape-profile JSON from profile_cdb_shapes.py --apply-filtros-fonte "
                        "(local or oci:// URI). Enables Cat 1 subtype-map and Cat 7 "
                        "distribution checks.")
    p.add_argument("--application-capacity-contract",
                   default=os.environ.get("DATAGEN_APPLICATION_CAPACITY_CONTRACT") or None,
                   help="Optional application capacity-contract JSON (local or oci:// URI). "
                        "Defaults to DATAGEN_APPLICATION_CAPACITY_CONTRACT when set.")
    p.add_argument("--shape-unseen-tol", type=float, default=1.0,
                   help="Cat 7a: max %% of synthetic IFs whose shape is absent from the "
                        "baseline (default 1.0).")
    p.add_argument("--shape-drift-tol", type=float, default=0.15,
                   help="Cat 7b: max total variation distance between synthetic and "
                        "baseline shape distributions, 0-1 (default 0.15).")
    p.add_argument("--shape-op-ratio-tol", type=float, default=5.0,
                   help="Cat 7c: max %% of operation-bearing IFs violating "
                        "OPERACAO:DADO_OPERACAO:LANCAMENTO = 1:2:1 (default 5.0).")
    p.add_argument("--fail-severity", default="error", choices=["error", "warn"],
                   help="Minimum severity that makes the run exit non-zero.")
    p.add_argument("--sample-size", type=int, default=20, help="Offending keys sampled per check.")
    p.add_argument("--validate-against", default="union", choices=["synthetic", "union"],
                   help="Resolve FK parents only within the synthetic output, "
                        "or also against Oracle.")
    p.add_argument("--max-residual-keys", type=int, default=1_000_000,
                   help="Max distinct child keys left unresolved by the synthetic output "
                        "that will be looked up in Oracle via IN-lists; above this the "
                        "FK is reported unresolved (WARN). Sized for large clone runs: "
                        "reference FKs like COMITENTE have one residual per distinct "
                        "child value (~1 per cloned IF).")
    p.add_argument("--max-parent-keys", type=int, default=None,
                   help="Deprecated (parent key sets are no longer downloaded); ignored.")
    p.add_argument("--emit-faltantes", default=None,
                   help="Write the Oracle-verified orphan keys (Cat 3) as a "
                        "TABELA/COLUNA/VALOR Parquet at this path/URI — feed it to "
                        "engorda_instrumentos.py --faltantes-parquet to prune the "
                        "sampling domain on the next generation run. ACCUMULATES: "
                        "keys already at the path are kept; only new keys are "
                        "appended (a shrinking list made the loop non-convergent).")
    p.add_argument("--skip-check", action="append", default=[],
                   help="Check-id prefix(es) to skip (repeatable), e.g. 6.combo.")
    p.add_argument("--no-oracle", action="store_true",
                   help="Do not read Oracle metadata (limits Categories 3/4/6).")
    p.add_argument("--verify-subtype-map", action="store_true",
                   help="Run the expensive best-effort production audit of the static "
                        "CONDICAO_IF subtype map. Disabled by default; synthetic subtype "
                        "integrity is always validated by Category 1.")
    p.add_argument("--registration-profile", action="store_true",
                   help="Enable Cat 8 current-registration format, persisted-profile, and "
                        "exact type-mix WARN checks.")
    return p.parse_args()


def create_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("validate_products")
             .getOrCreate())
    # Spark 3.5.0 (OCI Data Flow) + AQE + cached DataFrames silently LOSES JOIN
    # ROWS (SPARK-45282, fixed in 3.5.1). This validator caches every synthetic
    # table and joins over them — with AQE on, a check can false-PASS by losing
    # the very rows it should flag. Keep AQE off until the apps run >= 3.5.1.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    return spark


def main() -> None:
    run_started = perf_counter()
    args = parse_args()
    profile = get_validation_profile(args.product)
    is_credito_scr = profile.pipeline == "credito_scr"
    is_dicre = profile.pipeline == "dicre"
    is_lci = profile.pipeline == "lci"
    is_lca = profile.pipeline == "lca"
    is_non_if = is_credito_scr or is_dicre
    skip_prefixes = [prefix.strip() for prefix in args.skip_check if prefix.strip()]
    if _check_is_skipped("0.identity", skip_prefixes):
        raise SystemExit("Product identity is non-skippable; remove its --skip-check prefix.")
    if args.shape_baseline and _check_is_skipped("7.baseline", skip_prefixes):
        raise SystemExit("A supplied baseline contract is non-skippable.")
    if is_non_if and args.shape_baseline:
        raise SystemExit(
            f"--shape-baseline is not supported for the non-IF {profile.name} pipeline."
        )
    cfg = read_config(args.no_oracle, profile, args.input_base)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # A supplied contract is an input to the validation run, so fail before
    # expensive synthetic reads if it cannot be read or parsed.
    if _check_group_is_skipped(("4.capacity",), skip_prefixes):
        application_capacities = {}
        logger.info("Skipped application capacity contract setup (--skip-check).")
    else:
        application_capacities = _timed(
            "setup application capacity contract",
            lambda: load_application_capacity_contract(
                spark, args.application_capacity_contract,
            ),
        )

    only = [t.strip().upper() for t in args.tables.split(",")] if args.tables else None
    tables = _timed(
        "setup synthetic table discovery",
        lambda: read_synthetic_tables(spark, cfg.synthetic_base, only),
    )

    if args.no_oracle:
        meta = Metadata(set(tables), {}, {}, {}, {})
        logger.warning("--no-oracle: Categories 3/4/6 are limited (no PK/FK/NOT NULL metadata).")
    else:
        meta = _timed(
            "setup Oracle metadata",
            lambda: load_oracle_metadata(spark, cfg),
        )

    findings: List[Finding] = []
    # Coverage ledger: unsupported required capabilities (forces PARTIAL) and any diagnostic
    # subset / skipped-required reasons.
    partial_reasons: List[str] = []
    findings += build_capability_findings(profile)
    if only:
        partial_reasons.append(
            "diagnostic --tables subset restricts coverage (no strict PASS)")
    if args.no_oracle:
        partial_reasons.append(
            "--no-oracle disables required Oracle-backed metadata checks")
    if skip_prefixes:
        partial_reasons.append(
            f"--skip-check restricts coverage: {sorted(skip_prefixes)}")

    # Product identity preflight — before any semantic check.
    findings += _timed(
        "category 0 identity",
        lambda: (
            check_credito_scr_identity(tables, profile, args.sample_size)
            if is_credito_scr else check_dicre_identity(tables, profile, args.sample_size)
            if is_dicre else check_product_identity(tables, profile, args.sample_size)
        ),
    )
    findings += _timed(
        "category 0 metadata coverage",
        lambda: (
            check_credito_scr_metadata(meta, args.no_oracle, profile)
            if is_credito_scr else check_dicre_metadata(meta, args.no_oracle, profile)
            if is_dicre else check_lci_metadata(meta, args.no_oracle, profile)
            if is_lci else check_lca_metadata(meta, args.no_oracle, profile)
            if is_lca else []
        ),
    )
    if is_credito_scr:
        findings += _run_check_group(
            "category 2d Credito SCR graph", ("2d.",), skip_prefixes,
            lambda: check_credito_scr_graph(tables, args.sample_size, profile),
        )
    elif is_dicre:
        findings += _run_check_group(
            "category 2f DICRE graph", ("2f.",), skip_prefixes,
            lambda: check_dicre_graph(tables, args.sample_size, profile)
            + check_dicre_irop_graph(tables, args.sample_size, profile),
        )
    else:
        if is_lci:
            findings += _run_check_group(
                "category 2e LCI graph", ("2e.",), skip_prefixes,
                lambda: check_lci_graph(tables, args.sample_size, profile),
            )
        elif is_lca:
            findings += _run_check_group(
                "category 2g LCA graph", ("2g.",), skip_prefixes,
                lambda: check_lca_graph(tables, args.sample_size, profile),
            )
        polymorphism_prefixes = (
            ("2e.condition_polymorphism", "2e.unknown_condition_type", "2e.subtype_orphan")
            if is_lci else
            ("2g.condition_polymorphism", "2g.unknown_condition_type", "2g.subtype_orphan")
            if is_lca else ("1.", "1a.", "1b.", "1c.")
        )
        findings += _run_check_group(
            "category 1 polymorphism", polymorphism_prefixes, skip_prefixes,
            lambda: (
                check_lci_polymorphism(tables, args.sample_size, profile)
                if is_lci else check_lca_polymorphism(tables, args.sample_size, profile)
                if is_lca else check_polymorphism(tables, meta, args.sample_size)
            ),
        )
        if args.shape_baseline:
            findings += _run_check_group(
                "category 1 baseline subtype verification", ("1.map_snapshot",), skip_prefixes,
                lambda: verify_subtype_map_from_baseline(spark, args.shape_baseline),
            )
        if not args.no_oracle and args.verify_subtype_map:
            findings += _run_check_group(
                "category 1 Oracle subtype verification", ("1.map_verify",), skip_prefixes,
                lambda: verify_subtype_map_against_production(spark, cfg),
            )
        elif not args.no_oracle:
            logger.info(
                "Production subtype-map audit skipped; use --verify-subtype-map to run it."
            )
        findings += _run_check_group(
            "category 2 domain", ("2.domain",), skip_prefixes,
            lambda: check_domain(tables, meta, args.sample_size, profile),
        )
        findings += _run_check_group(
            "category 2b CDB variants", ("2b.",), skip_prefixes,
            lambda: check_cdb_variant_rules(tables, args.sample_size, profile),
        )
        findings += _run_check_group(
            "category 2c RDB resgate schedules", ("2c.",), skip_prefixes,
            lambda: check_rdb_resgate_schedule_rules(tables, args.sample_size, profile),
        )
    if args.max_parent_keys is not None:
        logger.warning("--max-parent-keys is deprecated and ignored; "
                       "see --max-residual-keys.")
    referential_prefixes = ("3.fk_", "3.shared_key")
    if _check_group_is_skipped(referential_prefixes, skip_prefixes):
        ref_findings, faltantes = [], []
        logger.info("Skipped category 3 referential before execution (--skip-check).")
    else:
        ref_findings, faltantes = _timed(
            "category 3 referential",
            lambda: check_referential(
                spark, cfg, tables, meta, args.sample_size,
                args.validate_against, args.max_residual_keys, skip_prefixes,
            ),
        )
        ref_findings = [
            finding for finding in ref_findings
            if not _check_is_skipped(finding.check_id, skip_prefixes)
        ]
    findings += ref_findings
    findings += _run_check_group(
        "category 3b primary keys", ("3b.",), skip_prefixes,
        lambda: check_primary_keys(tables, meta, args.sample_size, args.no_oracle),
    )
    if not is_non_if:
        findings += _run_check_group(
            "category 3c clone map", ("3c.",), skip_prefixes,
            lambda: check_clone_map(tables, profile, args.sample_size),
        )
    if args.emit_faltantes and not _check_is_skipped("3.fk_orphan", skip_prefixes):
        _timed(
            "category 3 emit faltantes",
            lambda: emit_faltantes(spark, args.emit_faltantes, faltantes),
        )
    findings += _run_check_group(
        "category 4 not null", ("4.not_null",), skip_prefixes,
        lambda: check_not_null(tables, meta, args.sample_size),
    )
    findings += _run_check_group(
        "category 4 capacity", ("4.capacity",), skip_prefixes,
        lambda: check_capacity(tables, meta, application_capacities, args.sample_size),
    )
    findings += _run_check_group(
        "category 5 dates", ("5.",), skip_prefixes,
        lambda: check_dates(tables, meta, args.sample_size),
    )
    if is_credito_scr:
        if args.no_oracle:
            credito_lookups, credito_lookup_errors = {}, {
                name: "No Oracle connection"
                for name in (
                    "MODALIDADE_CREDITO", "PARAMETRO_BASE_CREDITO",
                    "TCTPFEATURE_TOGGLE", "CREDITO_SCR_TARGET",
                )
            }
        else:
            credito_lookups, credito_lookup_errors = _timed(
                "category 6d Credito SCR target lookup setup",
                lambda: load_credito_scr_target_frames(
                    spark, cfg, tables, args.registration_profile
                ),
            )
        findings += _run_check_group(
            "category 6d Credito SCR target lookups", ("6d.",), skip_prefixes,
            lambda: check_credito_scr_target_frames(
                tables, credito_lookups.get("MODALIDADE_CREDITO"),
                credito_lookups.get("PARAMETRO_BASE_CREDITO"),
                credito_lookups.get("TCTPFEATURE_TOGGLE"), args.sample_size, profile,
                credito_lookup_errors, credito_lookups.get("CONTA_PARTICIPANTE"),
                args.registration_profile, credito_lookups.get("CREDITO_SCR_TARGET"),
            ),
        )
        findings += _run_check_group(
            "category 8d Credito SCR insertion profile", ("8d.",), skip_prefixes,
            lambda: check_credito_scr_registration_profile(
                tables, args.sample_size, args.registration_profile, profile,
            ),
        )
    elif is_dicre:
        if _check_group_is_skipped(("6f.",), skip_prefixes):
            logger.info("Skipped category 6f DICRE target lookup setup (--skip-check).")
        else:
            if args.no_oracle:
                dicre_lookups, dicre_lookup_errors = {}, {
                    name: "No Oracle connection"
                    for name in (
                        "DICRE_ACCOUNTS", "DICRE_BASES", "DICRE_IF_COMPATIBILITY",
                        "DICRE_QUALIFICATIONS", "TCTPFEATURE_TOGGLE", "CREDITO_DC_TARGET",
                    )
                }
            else:
                dicre_lookups, dicre_lookup_errors = _timed(
                    "category 6f DICRE target lookup setup",
                    lambda: load_dicre_target_frames(spark, cfg, tables),
                )
            findings += _run_check_group(
                "category 6f DICRE target eligibility", ("6f.",), skip_prefixes,
                lambda: check_dicre_target_frames(
                    tables, dicre_lookups, args.sample_size, profile, dicre_lookup_errors,
                ),
            )
        findings += _run_check_group(
            "category 8f DICRE insertion profile", ("8f.",), skip_prefixes,
            lambda: check_dicre_registration_profile(
                tables, args.sample_size, args.registration_profile, profile,
            ),
        )
    elif is_lci:
        if _check_group_is_skipped(("6e.",), skip_prefixes):
            logger.info("Skipped category 6e LCI target lookup setup (--skip-check).")
        else:
            if args.no_oracle:
                lci_lookups, lci_lookup_errors = {}, {
                    name: "No Oracle connection"
                    for name in (
                        "LCI_TIPO_IF", "LCI_LOTES", "LCI_ACCOUNTS", "LCI_OBJECT_SERVICE",
                        "LCI_ROUTES", "LCI_ROOT_CODES", "LCI_TOGGLE", "LCI_CONTROLS",
                        "LCI_OPERATION_CODES", "LCI_WALLET_COMITENTE",
                        "LCI_WALLET_PARTICIPANTE",
                    )
                }
            else:
                lci_lookups, lci_lookup_errors = _timed(
                    "category 6e LCI target lookup setup",
                    lambda: load_lci_target_frames(
                        spark, cfg, tables, skip_prefixes=skip_prefixes
                    ),
                )
            findings += _run_check_group(
                "category 6e LCI target eligibility", ("6e.",), skip_prefixes,
                lambda: check_lci_target_frames(
                    tables, lci_lookups, args.sample_size, profile, lci_lookup_errors,
                ),
            )
        findings += _run_check_group(
            "category 7 LCI shapes", ("7.", "7a.", "7b."), skip_prefixes,
            lambda: check_shapes(
                spark, tables, args.shape_baseline, args.sample_size,
                args.shape_unseen_tol, args.shape_drift_tol, args.shape_op_ratio_tol, profile,
                skip_prefixes,
            ),
        )
        findings += _run_check_group(
            "category 8e LCI insertion profile", ("8e.",), skip_prefixes,
            lambda: check_lci_registration_profile(
                tables, args.sample_size, args.registration_profile, profile,
            ),
        )
    elif is_lca:
        if _check_group_is_skipped(("6g.",), skip_prefixes):
            logger.info("Skipped category 6g LCA target lookup setup (--skip-check).")
        else:
            if args.no_oracle:
                lca_lookups, lca_lookup_errors = {}, {
                    name: "No Oracle connection"
                    for name in (
                        "LCA_TIPO_IF", "LCA_LOTES", "LCA_ACCOUNTS", "LCA_OBJECT_SERVICE",
                        "LCA_ROUTES", "LCA_ROOT_CODES", "LCA_TOGGLE", "LCA_CONTROLS",
                        "LCA_OPERATION_CODES", "LCA_WALLET_COMITENTE",
                        "LCA_WALLET_PARTICIPANTE", "LCA_MUNICIPALITIES", "LCA_UFS",
                    )
                }
            else:
                lca_lookups, lca_lookup_errors = _timed(
                    "category 6g LCA target lookup setup",
                    lambda: load_lca_target_frames(
                        spark, cfg, tables, skip_prefixes=skip_prefixes
                    ),
                )
            findings += _run_check_group(
                "category 6g LCA target eligibility", ("6g.",), skip_prefixes,
                lambda: check_lca_target_frames(
                    tables, lca_lookups, args.sample_size, profile, lca_lookup_errors,
                ),
            )
        findings += _run_check_group(
            "category 7 LCA shapes", ("7.", "7a.", "7b."), skip_prefixes,
            lambda: check_shapes(
                spark, tables, args.shape_baseline, args.sample_size,
                args.shape_unseen_tol, args.shape_drift_tol, args.shape_op_ratio_tol, profile,
                skip_prefixes,
            ),
        )
        findings += _run_check_group(
            "category 8g LCA insertion profile", ("8g.",), skip_prefixes,
            lambda: check_lca_registration_profile(
                tables, args.sample_size, args.registration_profile, profile,
            ),
        )
    else:
        findings += _run_check_group(
            "category 6 lookup combinations", ("6.required", "6.combo"), skip_prefixes,
            lambda: check_lookup_combos(
                spark, cfg, tables, meta, args.sample_size, args.max_residual_keys, profile,
                skip_prefixes,
            ),
        )
        findings += _run_check_group(
            "category 7 shapes", ("7.", "7a.", "7b.", "7c.", "7d."), skip_prefixes,
            lambda: check_shapes(
                spark, tables, args.shape_baseline, args.sample_size,
                args.shape_unseen_tol, args.shape_drift_tol, args.shape_op_ratio_tol, profile,
                skip_prefixes,
            ),
        )
        findings += _run_check_group(
            "category 8 log invariants", ("8",), skip_prefixes,
            lambda: check_log_invariants(
                tables, args.sample_size, args.registration_profile, profile,
            ),
        )

    baseline_identity = None
    if args.shape_baseline:
        try:
            baseline = json.loads(read_text(spark, args.shape_baseline))
            identity_fields = (
                "schema_version", "product", "num_tipo_if", "domain_version",
                "metric_version", "map_mode", "source_key_count", "source_key_fingerprint",
            )
            baseline_identity = {
                "path": args.shape_baseline,
                **{field: baseline.get(field) for field in identity_fields},
            }
        except Exception as exc:  # noqa: BLE001
            baseline_identity = {"path": args.shape_baseline, "load_error": str(exc)}
    runtime_identity = {
        "spark_version": spark.version,
        "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled", "false") == "true",
    }

    code = _timed(
        "report emission",
        lambda: emit_report(
            spark, findings, args.report_path, args.fail_severity, profile,
            cfg.synthetic_base, list(tables), partial_reasons,
            baseline_identity, runtime_identity,
        ),
    )
    logger.info("[PERF] complete run elapsed=%.1fs", perf_counter() - run_started)
    spark.stop()
    sys.exit(code)


if __name__ == "__main__":
    main()
