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

Authoritative rules (PK / FK graph / NOT NULL / column types) are read from the
Oracle data dictionary (ALL_* views) over JDBC. A few semantic rules that are
NOT expressible in schema metadata (the CONDICAO_IF polymorphic subtype map and
the CDB-simplificado product predicates) are curated below and, where possible,
verified against production data.

The six check categories map directly to the three failures observed in the
batch validation log:

  Cat 1  CONDICAO_IF polymorphism  -> ClassCastException JurosFlutuanteDO -> JurosFixoDO
  Cat 2  Domain conformance        -> out-of-product rows (FILTROS_FONTE image)
  Cat 3  Referential integrity     -> FK orphans / broken remap
  Cat 4  NOT NULL (incl. '')       -> ORA-01400 (e.g. TCTPDETALHE_TRAN_SEM_FINA.COD_MOTIVO)
  Cat 5  Date coherence            -> bad revaluation input
  Cat 6  Lookup combinations       -> "SEM MODALIDADE / servico_ft nao encontrado"
  Cat 7  Shape conformance         -> per-IF cardinalities vs the production profile
                                      (see docs/cdb-shapes-findings.md)

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
from dataclasses import asdict, dataclass, field
from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("validate_cdb")

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

# Candidate reference tables for the operation/service combo check (Cat 6).
OPERACAO_TABLE = "OPERACAO"
DADO_OPERACAO_TABLE = "DADO_OPERACAO"
COMBO_TABLE_PATTERNS = ["%OBJETO_SERV%", "%TIPO_OPER_OBJETO%", "%TIPO_OPER_OBJ_SERV%"]


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
        f"SELECT table_name, column_name, data_type, nullable "
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

    logger.info("Loading Oracle metadata (owner=%s) ...", owner)
    cols_rows = _jdbc(spark, cfg, q_cols).collect()
    pk_rows = _jdbc(spark, cfg, q_pk).collect()
    fk_rows = _jdbc(spark, cfg, q_fk).collect()

    tables: set = set()
    not_null: Dict[str, set] = {}
    col_type: Dict[str, Dict[str, str]] = {}
    for r in cols_rows:
        t = (r["TABLE_NAME"] or "").upper()
        c = (r["COLUMN_NAME"] or "").upper()
        tables.add(t)
        col_type.setdefault(t, {})[c] = (r["DATA_TYPE"] or "").upper()
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
    return Metadata(tables, pk, not_null, col_type, fks)


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
            tables[name.upper()] = spark.read.parquet(path)
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
    batch_size: int = 500,
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
) -> List[Finding]:
    def canon(col):
        # Native-function version of _canon_key: numeric-looking values lose
        # trailing fractional zeros; anything else passes through unchanged.
        stripped = F.regexp_replace(
            F.regexp_replace(col, r"(\.\d*?)0+$", "$1"), r"\.$", "")
        return F.when(col.rlike(r"^-?\d+\.\d*0*$"), stripped).otherwise(col)

    out: List[Finding] = []
    for table, df in tables.items():
        for fk in meta.fks.get(table, []):
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

            residual = [tuple(r[f"k{i}"] for i in range(len(child_actual)))
                        for r in residual_df.limit(max_residual_keys + 1).collect()]

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
                    found = _residual_in_oracle(spark, cfg, fk, residual)
                    residual = [t for t in residual if t not in found]
                    note = " (checked against synthetic ∪ Oracle)"
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
    return out


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
# Category 6 - Lookup combinations (SEM MODALIDADE) - best effort
# ---------------------------------------------------------------------------
def check_lookup_combos(
    spark: SparkSession, cfg: Config, tables: Dict[str, DataFrame], meta: Metadata, sample: int,
) -> List[Finding]:
    out: List[Finding] = []
    op_df = tables.get(OPERACAO_TABLE)
    if op_df is None:
        return [Finding("6.combo", "Lookup combinations", SEV_INFO, OPERACAO_TABLE, True,
                        message="OPERACAO not in output; combo check skipped.")]
    if not cfg.jdbc_url:
        return [Finding("6.combo", "Lookup combinations", SEV_WARN, OPERACAO_TABLE, False,
                        message="No Oracle connection; cannot resolve valid "
                                "(tipo_operacao, modalidade, servico) combinations for CDB.")]

    # Discover a candidate service-mapping table.
    combo_table = None
    for pat in COMBO_TABLE_PATTERNS:
        try:
            rows = _jdbc(spark, cfg,
                         f"SELECT table_name FROM all_tables WHERE owner='{cfg.schema}' "
                         f"AND table_name LIKE '{pat}'").collect()
            if rows:
                combo_table = rows[0]["TABLE_NAME"].upper()
                break
        except Exception as exc:  # noqa: BLE001
            logger.warning("combo table discovery failed for %s: %s", pat, exc)

    if not combo_table:
        return [Finding("6.combo", "Lookup combinations", SEV_WARN, OPERACAO_TABLE, False,
                        hint="Identify the tipo_operacao x modalidade x servico mapping table "
                             "and add it to COMBO_TABLE_PATTERNS to enable a full combo "
                             "anti-join.",
                        message="Could not resolve the operation/service mapping table; "
                                "SEM MODALIDADE risk not fully validated. Verify "
                                "TIPO_OPERACAO/MODALIDADE_LIQUIDACAO refs "
                                "are seeded for CDB operations.")]

    to_col = resolve(op_df, "NUM_ID_TIPO_OPERACAO")
    mod_col = resolve(op_df, "NUM_ID_MODALIDADE_LIQUIDACAO")
    if not to_col or not mod_col:
        return [Finding("6.combo", "Lookup combinations", SEV_INFO, OPERACAO_TABLE, True,
                        message=f"OPERACAO lacks tipo_operacao/modalidade columns; "
                                f"using {combo_table} not possible. Skipped.")]

    try:
        combo_cols = meta.col_type.get(combo_table, {})
        c_to = "NUM_ID_TIPO_OPERACAO" if "NUM_ID_TIPO_OPERACAO" in combo_cols else None
        c_mod = ("NUM_ID_MODALIDADE_LIQUIDACAO"
                 if "NUM_ID_MODALIDADE_LIQUIDACAO" in combo_cols else None)
        if not c_to or not c_mod:
            return [Finding("6.combo", "Lookup combinations", SEV_WARN, OPERACAO_TABLE, False,
                            message=f"Mapping table {combo_table} lacks the expected combo "
                                    "columns; SEM MODALIDADE risk not validated.")]
        valid = _jdbc(spark, cfg,
                      f"SELECT DISTINCT {c_to} t, {c_mod} m FROM {cfg.schema}.{combo_table}")
        valid = valid.select(F.col("T").cast("string").alias("t"),
                             F.col("M").cast("string").alias("m"))
        used = op_df.select(
            F.col(to_col).cast("string").alias("t"),
            F.col(mod_col).cast("string").alias("m"),
        ).dropna().dropDuplicates()
        missing = used.join(valid, ["t", "m"], "left_anti")
        c = missing.count()
        out.append(Finding(
            "6.combo", "Lookup combinations",
            SEV_ERROR if c else SEV_INFO, OPERACAO_TABLE, c == 0, count=c,
            column="NUM_ID_TIPO_OPERACAO,NUM_ID_MODALIDADE_LIQUIDACAO",
            sample=_sample_keys(missing, ["t", "m"], sample),
            hint=f"Operation uses a (tipo_operacao, modalidade) with no mapping in "
                 f"{combo_table} for CDB -> 'SEM MODALIDADE / servico_ft nao encontrado'. "
                 "Seed/verify the mapping.",
            message=f"OPERACAO (tipo_operacao, modalidade) pairs absent from {combo_table}.",
        ))
    except Exception as exc:  # noqa: BLE001
        out.append(Finding("6.combo", "Lookup combinations", SEV_WARN, OPERACAO_TABLE, False,
                           message=f"Combo check errored: {exc}"))
    return out


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
                "OPERACAO", pct <= op_ratio_tol_pct, count=c, column="OPERACAO,DADO_OPERACAO,LANCAMENTO",
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
                        "(local or oci:// URI). Enables the Cat 7 distribution checks.")
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
    p.add_argument("--max-residual-keys", type=int, default=100_000,
                   help="Max distinct child keys left unresolved by the synthetic output "
                        "that will be looked up in Oracle via IN-lists; above this the "
                        "FK is reported unresolved (WARN).")
    p.add_argument("--max-parent-keys", type=int, default=None,
                   help="Deprecated (parent key sets are no longer downloaded); ignored.")
    p.add_argument("--skip-check", action="append", default=[],
                   help="Check-id prefix(es) to skip (repeatable), e.g. 6.combo.")
    p.add_argument("--no-oracle", action="store_true",
                   help="Do not read Oracle metadata (limits Categories 3/4/6).")
    return p.parse_args()


def create_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("validate_cdb_simplificado")
            .getOrCreate())


def main() -> None:
    args = parse_args()
    cfg = read_config(args.no_oracle)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    only = [t.strip().upper() for t in args.tables.split(",")] if args.tables else None
    tables = read_synthetic_tables(spark, cfg.synthetic_base, only)

    if args.no_oracle:
        meta = Metadata(set(tables), {}, {}, {}, {})
        logger.warning("--no-oracle: Categories 3/4/6 are limited (no PK/FK/NOT NULL metadata).")
    else:
        meta = load_oracle_metadata(spark, cfg)

    findings: List[Finding] = []
    findings += check_polymorphism(tables, meta, args.sample_size)
    if not args.no_oracle:
        findings += verify_subtype_map_against_production(spark, cfg)
    findings += check_domain(tables, meta, args.sample_size)
    if args.max_parent_keys is not None:
        logger.warning("--max-parent-keys is deprecated and ignored; "
                       "see --max-residual-keys.")
    findings += check_referential(spark, cfg, tables, meta, args.sample_size,
                                  args.validate_against, args.max_residual_keys)
    findings += check_not_null(tables, meta, args.sample_size)
    findings += check_dates(tables, meta, args.sample_size)
    findings += check_lookup_combos(spark, cfg, tables, meta, args.sample_size)
    findings += check_shapes(spark, tables, args.shape_baseline, args.sample_size,
                             args.shape_unseen_tol, args.shape_drift_tol,
                             args.shape_op_ratio_tol)

    if args.skip_check:
        findings = [f for f in findings
                    if not any(f.check_id.startswith(s) for s in args.skip_check)]

    code = emit_report(spark, findings, args.report_path, args.fail_severity)
    spark.stop()
    sys.exit(code)


if __name__ == "__main__":
    main()