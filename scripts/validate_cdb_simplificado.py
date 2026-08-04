#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
validate_cdb_simplificado.py
============================

Descriptive validator for the CDB-simplificado synthetic dataset produced by
`engorda_tables.py`. It runs on the ENGORDA OUTPUT (the synthetic Parquet under
DATAGEN_SYNTHETIC_BASE_URI) and checks it against the ACTUAL application rules of
the CETIP/NoMe platform, so structural/domain violations are caught BEFORE the
Oracle append and before the daily/operational batch runs on top of the data.

It is fully self-contained: it does NOT import from `engorda_tables.py`.

Authoritative rules (PK / FK graph / NOT NULL / column types and capacities) are read from the
Oracle data dictionary (ALL_* views) over JDBC. An optional application-capacity
contract supplies global code-level limits. A few semantic rules that are
NOT expressible in schema metadata (the CONDICAO_IF polymorphic subtype map and
the CDB-simplificado product predicates) are curated below and, where possible,
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

  spark-submit profile_cdb_shapes.py --base-uri <raw> --apply-filtros-fonte \
      --label raw_filtered --report-path oci://.../profile_raw_filtered.json

Pass that JSON via --shape-baseline. Without it, only the baseline-free hard
invariants run (OPERACAO:DADO_OPERACAO:LANCAMENTO = 1:2:1, RESGATE <= 1 per IF)
and the distribution checks are skipped with a WARN.

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
  spark-submit --jars ojdbc8.jar validate_cdb_simplificado.py \
      --report-path report.json --fail-severity error --validate-against union
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
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_cdb")


def _timed(label: str, operation):
    """Run an operation and log elapsed wall time without changing its result."""
    started = perf_counter()
    logger.info("[PERF] start %s", label)
    try:
        return operation()
    finally:
        logger.info("[PERF] end %s elapsed=%.1fs", label, perf_counter() - started)

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

# CDB-simplificado product predicates (image of engorda's FILTROS_FONTE). Each is
# a Spark SQL boolean expression that must be TRUE for every row of the table.
DOMAIN_RULES: Dict[str, List[str]] = {
    "INSTRUMENTO_FINANCEIRO": ["NUM_TIPO_IF = 49", "DAT_EXCLUSAO IS NULL"],
    "RESGATE": ["UPPER(TRIM(COD_COND_RESGATE)) = 'SEM TABELA'", "DAT_EXCLUSAO IS NULL"],
    "TITULO": ["COD_TIPO_ESCALONAMENTO IS NULL"],
    "CONDICAO_IF": ["DAT_EXCLUSAO IS NULL"],
    "CARTEIRA_COMITENTE": ["QTD_CARTEIRA_COMITENTE > 0"],
    "CARTEIRA_PARTICIPANTE": ["QTD_CARTEIRA_PARTICIPANTE > 0"],
}

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


def read_config(no_oracle: bool) -> Config:
    base = os.environ.get("DATAGEN_SYNTHETIC_BASE_URI", "").strip()
    if not base:
        raise SystemExit("DATAGEN_SYNTHETIC_BASE_URI is required.")
    prefix = os.environ.get("DATAGEN_SYNTHETIC_PREFIX", "").strip().strip("/")
    if prefix:
        base = f"{base.rstrip('/')}/{prefix}"
    jdbc_url = os.environ.get("DATAGEN_SOURCE_JDBC_URL", "").strip() or None
    jdbc_user = os.environ.get("DATAGEN_SOURCE_DB_USER", "").strip() or None
    jdbc_pwd = os.environ.get("DATAGEN_SOURCE_DB_PASSWORD", "")
    schema = os.environ.get("DATAGEN_SOURCE_SCHEMA", "CETIP").strip().upper()
    if not no_oracle and not (jdbc_url and jdbc_user):
        raise SystemExit(
            "Oracle metadata requires DATAGEN_SOURCE_JDBC_URL and "
            "DATAGEN_SOURCE_DB_USER (or run with --no-oracle)."
        )
    return Config(base.rstrip("/"), jdbc_url, jdbc_user, jdbc_pwd, schema)


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
        return [Finding("1.polymorphism", "CONDICAO_IF polymorphism", SEV_INFO,
                        CONDICAO_IF_TABLE, True, message="CONDICAO_IF not in output; skipped.")]

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
        logger.warning("Could not load baseline subtype-map snapshot: %s", exc)
        return []
    snapshot = baseline.get("subtype_map")
    if snapshot is None:
        logger.warning(
            "Shape baseline %s has no subtype_map; regenerate it with the current profiler.",
            baseline_path,
        )
        return []
    source = baseline.get("base_uri") or baseline_path
    return check_subtype_map_snapshot(snapshot, str(source))


# ---------------------------------------------------------------------------
# Category 2 - Domain conformance (FILTROS_FONTE image)
# ---------------------------------------------------------------------------
def check_domain(tables: Dict[str, DataFrame], meta: Metadata, sample: int) -> List[Finding]:
    out: List[Finding] = []
    for table, preds in DOMAIN_RULES.items():
        df = tables.get(table)
        if df is None:
            continue
        pk_cols = _pk_cols_for(meta, table, df)
        for pred in preds:
            try:
                bad = df.where(F.expr(f"NOT ({pred})"))
                c = bad.count()
                out.append(Finding(
                    "2.domain", "Domain conformance",
                    SEV_ERROR if c else SEV_INFO, table, c == 0, count=c,
                    sample=_sample_keys(bad, pk_cols, sample),
                    hint="Row outside the CDB-simplificado product (FILTROS_FONTE). A "
                         "fecho/rebind pass re-injected an out-of-product row, or the "
                         "source filter was not applied.",
                    message=f"Rows violating domain predicate: {pred}",
                ))
            except Exception as exc:  # noqa: BLE001
                out.append(Finding(
                    "2.domain", "Domain conformance", SEV_WARN, table, False,
                    hint="Check that the predicate columns exist in the output.",
                    message=f"Predicate skipped ({pred}): {exc}",
                ))
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

    out: List[Finding] = []
    faltantes: List[Tuple[str, str, str]] = []
    for table, df in tables.items():
        for fk in meta.fks.get(table, []):
            fk_label = (
                f"{table}.{','.join(fk.child_cols)}->"
                f"{fk.parent_table}.{','.join(fk.parent_cols)}"
            )
            child_actual = [resolve(df, c) for c in fk.child_cols]
            if any(a is None for a in child_actual):
                continue  # FK columns not all present in the output

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
                    if residual and len(fk.child_cols) == 1:
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
            if meta.is_shared_key_fk(fk):
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
    lookup_errors: Optional[Dict[str, str]] = None,
) -> List[Finding]:
    """Enforce the three mandatory target-backed CDB registration lookups."""
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

    if missing_refs:
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

    op_df = tables.get(OPERACAO_TABLE)
    op_tos_col = resolve(op_df, "NUM_ID_TIPO_OPER_OBJETO_SERV") if op_df is not None else None
    if op_df is None or op_tos_col is None:
        missing = (
            OPERACAO_TABLE
            if op_df is None
            else f"{OPERACAO_TABLE}.NUM_ID_TIPO_OPER_OBJETO_SERV"
        )
        operation_finding = Finding(
            "6.required.operation_tos", cat, SEV_ERROR, OPERACAO_TABLE, False,
            column="NUM_ID_TIPO_OPER_OBJETO_SERV",
            sample=(
                _sample_keys(op_df, [resolve(op_df, "NUM_ID_OPERACAO")], sample)
                if op_df is not None and resolve(op_df, "NUM_ID_OPERACAO") else []
            ),
            hint="Export OPERACAO with NUM_ID_TIPO_OPER_OBJETO_SERV; every row requires its "
                 "original nonblank target TOS reference.",
            message=f"Required operation TOS source is missing: {missing}.",
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
            op_id_col = resolve(op_df, "NUM_ID_OPERACAO")
            operations = op_df.select(
                *(
                    [_norm_code(F.col(op_id_col)).alias("operation_id")]
                    if op_id_col else []
                ),
                F.col(op_tos_col).cast("string").alias("raw_tos_id"),
                _canon_key_col(F.col(op_tos_col)).alias("tos_id"),
            )
            valid_tos = (
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
                .where(
                    (F.col("objeto_servico_id") == str(CDB_OBJETO_SERVICO))
                    & (F.col("identification_flag") == "S")
                    & (F.col("operation_type_code") == "1")
                )
                .select("tos_id")
                .where(F.col("tos_id").isNotNull())
                .dropDuplicates()
            )
            invalid_operations = operations.where(
                F.col("raw_tos_id").isNull() | (F.trim(F.col("raw_tos_id")) == "")
            ).unionByName(
                operations.where(
                    F.col("raw_tos_id").isNotNull()
                    & (F.trim(F.col("raw_tos_id")) != "")
                ).join(F.broadcast(valid_tos), "tos_id", "left_anti"),
                allowMissingColumns=True,
            )
            invalid_operation_count = invalid_operations.count()
            operation_sample_cols = (
                ["operation_id"] if "operation_id" in operations.columns else ["tos_id"]
            )
            operation_finding = Finding(
                "6.required.operation_tos", cat,
                SEV_ERROR if invalid_operation_count else SEV_INFO,
                OPERACAO_TABLE, invalid_operation_count == 0,
                count=invalid_operation_count,
                column="NUM_ID_TIPO_OPER_OBJETO_SERV",
                sample=_sample_keys(invalid_operations, operation_sample_cols, sample),
                hint=(
                    "Use a nonblank target TOS with NUM_ID_OBJETO_SERVICO=44, trimmed "
                    "IND_DISPONIVEL_IDENTIFICACAO='S', and joined "
                    "TIPO_OPERACAO.COD_TIPO_OPERACAO exactly '1' (not '2')."
                    if invalid_operation_count else ""
                ),
                message="Every synthetic operation must resolve to the approved CDB TOS.",
            )

    cdb_cols = target_columns(
        cdb_object_df,
        V_OBJETOS_SERVICO_TABLE,
        ["COD_OBJETO_SERVICO", "IND_PLATAFORMA_BAIXA"],
    )
    if cdb_cols is None:
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

    return [account_finding, operation_finding, platform_finding]


def check_lookup_combo_frames(
    op_df: DataFrame,
    tos_df: Optional[DataFrame],
    sic_df: Optional[DataFrame],
    tipo_operacao_df: Optional[DataFrame],
    sample: int,
    lookup_errors: Optional[Dict[str, str]] = None,
) -> List[Finding]:
    """Compare synthetic operations with already-loaded Oracle lookup rows."""
    cat = "Lookup combinations"
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
    sic_cols = target_columns(
        sic_df,
        V_PARAMETRO_SIC_TABLE,
        ["NUM_ID_TIPO_OPER_OBJETO_SERV", "NUM_TIPO_IF", "NUM_ID_OBJETO_SERVICO"],
    )
    tipo_cols = target_columns(
        tipo_operacao_df,
        TIPO_OPERACAO_TABLE,
        ["NUM_ID_TIPO_OPERACAO", "IND_SEM_MODALIDADE_INFOHUB"],
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
        ]

    tos = tos_df.select(
        _norm_code(F.col(tos_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"])).alias("tos_id"),
        _norm_code(F.col(tos_cols["NUM_ID_TIPO_OPERACAO"])).alias("tipo_operacao_id"),
        _norm_code(F.col(tos_cols["NUM_ID_OBJETO_SERVICO"])).alias("objeto_servico_id"),
        F.trim(F.col(tos_cols["IND_DISPONIVEL_IDENTIFICACAO"]).cast("string"))
        .alias("identificacao_flag"),
    ).where(F.col("tos_id").isNotNull()).dropDuplicates(["tos_id"])

    missing_tos = operations.join(F.broadcast(tos.select("tos_id")), "tos_id", "left_anti")
    missing_tos_count = missing_tos.count()
    out = [Finding(
        "6.combo.tos_fk", cat, SEV_ERROR if missing_tos_count else SEV_INFO,
        OPERACAO_TABLE, missing_tos_count == 0, count=missing_tos_count,
        column="NUM_ID_TIPO_OPER_OBJETO_SERV",
        sample=_sample_keys(missing_tos, sample_cols, sample),
        hint="Preserve or recover the transaction's exact static TOS FK, or prune source "
             "operations unsupported by the target; otherwise ask the QAB configuration "
             "owner to seed that exact mapping. Do not bind to an arbitrary valid TOS row.",
        message=f"Synthetic non-null TOS IDs absent from target {TIPO_OPER_OBJETO_SERV_TABLE}.",
    )]

    resolved_ops = operations.join(F.broadcast(tos), "tos_id", "inner")

    if sic_cols is None:
        out.append(Finding(
            "6.combo.cdb_compatibility", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Check Oracle JDBC credentials/schema, SELECT grants, and target view/table "
                 "availability, then rerun against QAB.",
            message=f"CDB compatibility check unavailable: {V_PARAMETRO_SIC_TABLE} "
                    f"{errors[V_PARAMETRO_SIC_TABLE]}.",
        ))
    else:
        valid_cdb_tos = (
            sic_df.select(
                _norm_code(F.col(sic_cols["NUM_ID_TIPO_OPER_OBJETO_SERV"]))
                .alias("tos_id"),
                _norm_code(F.col(sic_cols["NUM_TIPO_IF"])).alias("tipo_if"),
                _norm_code(F.col(sic_cols["NUM_ID_OBJETO_SERVICO"]))
                .alias("objeto_servico_id"),
            )
            .where(
                (F.col("tipo_if") == str(CDB_TIPO_IF))
                & (F.col("objeto_servico_id") == str(CDB_OBJETO_SERVICO))
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
                 f"for NUM_TIPO_IF={CDB_TIPO_IF} and "
                 f"NUM_ID_OBJETO_SERVICO={CDB_OBJETO_SERVICO}, or prune unsupported source "
                 "operations. Otherwise align the underlying QAB configuration; "
                 f"{V_PARAMETRO_SIC_TABLE} is a view and must not be updated directly.",
            message="Resolved operation TOS mappings incompatible with CDB service configuration.",
        ))

    if tipo_cols is None:
        out.append(Finding(
            "6.combo.sem_modalidade", cat, SEV_WARN, OPERACAO_TABLE, False,
            hint="Check Oracle JDBC credentials/schema, SELECT grants, and target view/table "
                 "availability, then rerun against QAB.",
            message=f"Sem-modalidade check unavailable: {TIPO_OPERACAO_TABLE} "
                    f"{errors[TIPO_OPERACAO_TABLE]}.",
        ))
    else:
        tipo_operacao = tipo_operacao_df.select(
            _norm_code(F.col(tipo_cols["NUM_ID_TIPO_OPERACAO"]))
            .alias("tipo_operacao_id"),
            F.trim(F.col(tipo_cols["IND_SEM_MODALIDADE_INFOHUB"]).cast("string"))
            .alias("sem_modalidade_flag"),
        ).dropDuplicates(["tipo_operacao_id"])
        sem_modalidade = resolved_ops.where(
            F.col("modalidade_id").isin(*[str(value) for value in SEM_MODALIDADE_IDS])
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
            hint=f"Do not renumber fixed modalidade IDs {SEM_MODALIDADE_IDS}. Preserve a "
                 "source-compatible modalidade/type pair, or ask the QAB configuration owner "
                 "to correct TIPO_OPERACAO.IND_SEM_MODALIDADE_INFOHUB only when valid.",
            message="Sem-modalidade operation types are not enabled in TIPO_OPERACAO.",
        ))

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
             "IND_DISPONIVEL_IDENTIFICACAO='S', or ask the QAB configuration owner to align "
             "target static configuration; do not arbitrarily rewrite transaction FKs.",
        message="Resolved TOS mappings are unavailable for identification.",
    ))
    return out


def check_lookup_combos(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], meta: Metadata, sample: int,
    max_account_keys: int = 1_000_000,
) -> List[Finding]:
    op_df = tables.get(OPERACAO_TABLE)
    if op_df is None:
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
                    "(tipo_operacao, modalidade, servico) combinations for CDB.",
        )]
    else:
        existing = []

    queries = {
        TIPO_OPER_OBJETO_SERV_TABLE: (
            "SELECT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_ID_TIPO_OPERACAO, "
            "NUM_ID_OBJETO_SERVICO, IND_DISPONIVEL_IDENTIFICACAO "
            f"FROM {cfg.schema}.{TIPO_OPER_OBJETO_SERV_TABLE}"
        ),
        V_PARAMETRO_SIC_TABLE: (
            "SELECT DISTINCT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_TIPO_IF, "
            f"NUM_ID_OBJETO_SERVICO FROM {cfg.schema}.{V_PARAMETRO_SIC_TABLE} "
            f"WHERE NUM_TIPO_IF = {CDB_TIPO_IF} "
            f"AND NUM_ID_OBJETO_SERVICO = {CDB_OBJETO_SERVICO}"
        ),
        TIPO_OPERACAO_TABLE: (
            "SELECT NUM_ID_TIPO_OPERACAO, IND_SEM_MODALIDADE_INFOHUB, COD_TIPO_OPERACAO "
            f"FROM {cfg.schema}.{TIPO_OPERACAO_TABLE}"
        ),
        V_OBJETOS_SERVICO_TABLE: (
            "SELECT COD_OBJETO_SERVICO, IND_PLATAFORMA_BAIXA "
            f"FROM {cfg.schema}.{V_OBJETOS_SERVICO_TABLE} "
            "WHERE COD_OBJETO_SERVICO = 'CDB'"
        ),
    }
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

    account_sources_available = all(
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

    if op_df is not None and cfg.jdbc_url:
        existing = check_lookup_combo_frames(
            op_df,
            lookups.get(TIPO_OPER_OBJETO_SERV_TABLE),
            lookups.get(V_PARAMETRO_SIC_TABLE),
            lookups.get(TIPO_OPERACAO_TABLE),
            sample,
            errors,
        )

    return existing + check_required_lookup_frames(
        tables,
        lookups.get(CONTA_PARTICIPANTE_TABLE),
        lookups.get(TIPO_OPER_OBJETO_SERV_TABLE),
        lookups.get(TIPO_OPERACAO_TABLE),
        lookups.get(V_OBJETOS_SERVICO_TABLE),
        sample,
        errors,
    )


# ---------------------------------------------------------------------------
# Category 7 - Shape conformance (per-IF cardinalities)
# ---------------------------------------------------------------------------
# Counting core kept in sync with scripts/profile_cdb_shapes.py: same universe
# (NUM_TIPO_IF=49, DAT_EXCLUSAO IS NULL), same active-row rule, same shape
# signature format ("TABLE=n|TABLE=n|..."). The metric list and its ORDER are
# parsed from the baseline JSON's own signatures, so the two scripts cannot
# silently drift apart: an incompatible baseline simply fails to match.
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
}
# Filtered metrics: metric name -> (source table, filter column, normalized value).
# Mirrors profile_cdb_shapes.py METRICS entries with a `where`; every domain IF
# has an evento tipo 85 and ~96% a tipo 83, so counting them separately stops a
# generator from passing "EVENTO=2" with two same-tipo eventos.
SHAPE_FILTERED: Dict[str, Tuple[str, str, str]] = {
    "EVENTO_TIPO83": ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "83"),
    "EVENTO_TIPO85": ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "85"),
}
DEFAULT_SHAPE_METRICS: List[str] = [
    "TITULO", "CREDITO", "CONDICAO_IF", "RESGATE", "JUROS_FLUTUANTE", "JUROS_FIXO",
    "ATUALIZACAO_POS", "ATUALIZACAO_PRE", "SPREAD",
    "EVENTO", "EVENTO_TIPO83", "EVENTO_TIPO85",
    "OPERACAO", "DADO_OPERACAO", "LANCAMENTO", "DEPOSITO_AUTOMATICO_IF",
    "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
]


def _shape_active(df: DataFrame) -> DataFrame:
    col = resolve(df, "DAT_EXCLUSAO")
    return df.where(F.col(col).isNull()) if col else df


def _shape_universe(tables: Dict[str, DataFrame]) -> Optional[DataFrame]:
    root = tables.get(SHAPE_ROOT_TABLE)
    if root is None:
        return None
    tipo, key = resolve(root, "NUM_TIPO_IF"), resolve(root, SHAPE_ROOT_KEY)
    if not tipo or not key:
        return None
    df = _shape_active(root.where(F.col(tipo).cast("long") == SHAPE_TIPO_IF))
    return df.select(F.col(key).cast("long").alias(SHAPE_ROOT_KEY)).dropDuplicates()


def _shape_counts(
    universe: DataFrame, tables: Dict[str, DataFrame], metric_names: List[str]
) -> Tuple[DataFrame, List[str]]:
    """Left-join per-metric row counts onto the universe; returns (df, skipped)."""
    result, skipped = universe, []
    for name in metric_names:
        source_table, wcol_name, wval = name, None, None
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
            if name in SHAPE_VIA:
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


def _load_shape_baseline(spark: SparkSession, path: str) -> Tuple[dict, List[str]]:
    """Return ({signature: pct}, ordered metric names parsed from the signatures)."""
    baseline = json.loads(read_text(spark, path))
    shapes = baseline.get("shapes") or []
    if not shapes:
        raise ValueError(f"Baseline {path} has no 'shapes' section.")
    if not baseline.get("filtros_fonte_applied"):
        logger.warning(
            "Shape baseline %s was built WITHOUT --apply-filtros-fonte; the comparison "
            "conflates filter effects with generation distortions.", path)
    metric_names = [part.split("=", 1)[0] for part in shapes[0]["shape"].split("|")]
    return {s["shape"]: float(s["pct"]) for s in shapes}, metric_names


def check_shapes(
    spark: SparkSession,
    tables: Dict[str, DataFrame],
    baseline_path: Optional[str],
    sample: int,
    unseen_tol_pct: float,
    drift_tol: float,
    op_ratio_tol_pct: float,
) -> List[Finding]:
    out: List[Finding] = []
    cat = "Shape conformance"
    universe = _shape_universe(tables)
    if universe is None:
        return [Finding("7.shapes", cat, SEV_INFO, SHAPE_ROOT_TABLE, True,
                        message="INSTRUMENTO_FINANCEIRO not in output; shape checks skipped.")]

    baseline_pct: Optional[dict] = None
    metric_names = DEFAULT_SHAPE_METRICS
    if baseline_path:
        try:
            baseline_pct, metric_names = _load_shape_baseline(spark, baseline_path)
        except Exception as exc:  # noqa: BLE001
            out.append(Finding("7.baseline", cat, SEV_WARN, SHAPE_ROOT_TABLE, False,
                               hint="Regenerate it with profile_cdb_shapes.py "
                                    "--apply-filtros-fonte on the raw data.",
                               message=f"Could not load shape baseline {baseline_path}: {exc}"))

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
                           message="No active CDB IFs (NUM_TIPO_IF=49) in the output."))
        return out

    # 7c - operation ratio invariant: DADO_OPERACAO = 2*OPERACAO, LANCAMENTO = OPERACAO.
    # Holds for ~99% of production IFs that have operations; a synthetic output that
    # binds these tables independently violates it almost everywhere.
    if all(name in metric_names for name in ("OPERACAO", "DADO_OPERACAO", "LANCAMENTO")):
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

    # 7d - RESGATE multiplicity: production has no IF with more than one resgate
    # condition (0 exceptions in 67.2M IFs profiled).
    if "RESGATE" in metric_names:
        multi = counts.where(F.col("RESGATE") > 1)
        c = multi.count()
        out.append(Finding(
            "7d.resgate_multiplicity", cat,
            SEV_ERROR if c else SEV_INFO, "RESGATE", c == 0, count=c, column="RESGATE",
            sample=_sample_keys(multi.select(SHAPE_ROOT_KEY), [SHAPE_ROOT_KEY], sample),
            hint="Production CDBs never have more than one RESGATE condition; bind at "
                 "most one resgate-condição per IF.",
            message="IFs with more than one RESGATE row.",
        ))

    # 7a/7b - distribution checks against the baseline profile.
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


def _cat8_active_cdb(tables: Dict[str, DataFrame], required: List[str]):
    table = "INSTRUMENTO_FINANCEIRO"
    df = tables.get(table)
    if df is None:
        return None, [table]
    columns = {name: resolve(df, name) for name in required}
    missing = [name for name, actual in columns.items() if not actual]
    if missing:
        return None, missing
    return df.where(
        (_norm_code(F.col(columns["NUM_TIPO_IF"])) == str(CDB_TIPO_IF))
        & F.col(columns["DAT_EXCLUSAO"]).isNull()
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
    tables: Dict[str, DataFrame], sample: int, registration_profile: bool = False
) -> List[Finding]:
    out: List[Finding] = []
    active, active_missing = _cat8_active_cdb(
        tables, ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO", "COD_IF"]
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
    else:
        num_if, cod_if = resolve(active, "NUM_IF"), resolve(active, "COD_IF")
        valid = F.coalesce(
            F.col(cod_if).cast("string").rlike(r"^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$"),
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
                "Generate COD_IF with the current CDB registration format.",
                "Active CDB COD_IF values outside ^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$.",
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

    for table, constants in REGISTRATION_CONSTANTS.items():
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

    profile_parents, profile_missing = _cat8_active_cdb(
        tables, ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
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
                report_path: Optional[str], fail_severity: str) -> int:
    fail_level = _SEV_ORDER[fail_severity.upper()]
    failing = [f for f in findings if (not f.passed) and _SEV_ORDER[f.severity] >= fail_level]

    by_cat: Dict[str, List[Finding]] = {}
    for f in findings:
        by_cat.setdefault(f.category, []).append(f)

    print("\n" + "=" * 78)
    print("CDB-SIMPLIFICADO SYNTHETIC OUTPUT VALIDATION")
    print("=" * 78)
    for cat in sorted(by_cat):
        print(f"\n### {cat}")
        for f in by_cat[cat]:
            status = "PASS" if f.passed else f.severity
            head = f"  [{status:5}] {f.check_id:22} {f.table}"
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
          f"{'FAIL' if failing else 'OK'} (fail-severity={fail_severity})")
    print("-" * 78)

    if report_path:
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "fail_severity": fail_severity,
            "failed": bool(failing),
            "counts": {"error": n_err, "warn": n_warn, "total": len(findings)},
            "findings": [f.to_dict() for f in findings],
        }
        try:
            write_text(spark, report_path,
                       json.dumps(payload, ensure_ascii=False, indent=2, default=str))
            logger.info("JSON report written to %s", report_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not write report to %s: %s", report_path, exc)

    return 1 if failing else 0


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate the CDB-simplificado synthetic output.")
    p.add_argument("--tables", default=None,
                   help="Comma-separated table list. Default: auto-discover under the base URI.")
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
             .appName("validate_cdb_simplificado")
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
    cfg = read_config(args.no_oracle)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # A supplied contract is an input to the validation run, so fail before
    # expensive synthetic reads if it cannot be read or parsed.
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
    findings += _timed(
        "category 1 polymorphism",
        lambda: check_polymorphism(tables, meta, args.sample_size),
    )
    if args.shape_baseline:
        findings += _timed(
            "category 1 baseline subtype verification",
            lambda: verify_subtype_map_from_baseline(spark, args.shape_baseline),
        )
    if not args.no_oracle and args.verify_subtype_map:
        findings += _timed(
            "category 1 Oracle subtype verification",
            lambda: verify_subtype_map_against_production(spark, cfg),
        )
    elif not args.no_oracle:
        logger.info(
            "Production subtype-map audit skipped; use --verify-subtype-map to run it."
        )
    findings += _timed(
        "category 2 domain",
        lambda: check_domain(tables, meta, args.sample_size),
    )
    if args.max_parent_keys is not None:
        logger.warning("--max-parent-keys is deprecated and ignored; "
                       "see --max-residual-keys.")
    ref_findings, faltantes = _timed(
        "category 3 referential",
        lambda: check_referential(
            spark, cfg, tables, meta, args.sample_size,
            args.validate_against, args.max_residual_keys,
        ),
    )
    findings += ref_findings
    if args.emit_faltantes:
        _timed(
            "category 3 emit faltantes",
            lambda: emit_faltantes(spark, args.emit_faltantes, faltantes),
        )
    findings += _timed(
        "category 4 not null",
        lambda: check_not_null(tables, meta, args.sample_size),
    )
    findings += _timed(
        "category 4 capacity",
        lambda: check_capacity(tables, meta, application_capacities, args.sample_size),
    )
    findings += _timed(
        "category 5 dates",
        lambda: check_dates(tables, meta, args.sample_size),
    )
    findings += _timed(
        "category 6 lookup combinations",
        lambda: check_lookup_combos(
            spark, cfg, tables, meta, args.sample_size, args.max_residual_keys,
        ),
    )
    findings += _timed(
        "category 7 shapes",
        lambda: check_shapes(
            spark, tables, args.shape_baseline, args.sample_size,
            args.shape_unseen_tol, args.shape_drift_tol, args.shape_op_ratio_tol,
        ),
    )
    findings += _timed(
        "category 8 log invariants",
        lambda: check_log_invariants(
            tables, args.sample_size, args.registration_profile,
        ),
    )

    if args.skip_check:
        findings = [f for f in findings
                    if not any(f.check_id.startswith(s) for s in args.skip_check)]

    code = _timed(
        "report emission",
        lambda: emit_report(spark, findings, args.report_path, args.fail_severity),
    )
    logger.info("[PERF] complete run elapsed=%.1fs", perf_counter() - run_started)
    spark.stop()
    sys.exit(code)


if __name__ == "__main__":
    main()
