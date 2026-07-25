#!/usr/bin/env python3
"""Enumerate the COMPLETE set of raw->target drift keys ("faltantes") for the
CDB Simplificado clone pipeline, in one pass.

Why: validate_cdb_simplificado.py only finds orphans among the keys that the
sampled batch happens to reference, and engorda_instrumentos.py redraws the
batch on every run (orderBy(rand(seed)) over a domain that the pruning itself
changes). The reactive prune/regenerate loop therefore never converges
(2026-07-25: 708 -> 646 -> 481 FRESH keys per run). This job instead anti-joins
EVERY key referenced by the product domain in the raw export against the
target Oracle's actual key set, once. Regenerating with the resulting parquet
makes ANY sample clean by construction.

Targets — the only FK targets that ever drifted; the validator's union check
still guards everything else:

    CARTEIRA_COMITENTE.NUM_ID_ENTIDADE -> COMITENTE.NUM_ID_ENTIDADE
    CARTEIRA_COMITENTE.NUM_CONTA       -> CONTA.NUM_CONTA

CARTEIRA_COMITENTE is the child on purpose: it carries NUM_IF, which is what
engorda_instrumentos._num_if_excluidos_por_faltantes needs to reach the
instrument (ESPECIFICACAO_COMITENTE shares the same comitente keys and is
covered transitively when the instrument is pruned).

Usage (Data Flow: reuse the VALIDATOR application — it has the ojdbc jar,
private endpoint and DATAGEN_SOURCE_* env; the profiler app has no JDBC):

  spark-submit --jars ojdbc8.jar enumerate_faltantes.py \\
      --base-uri oci://<bucket>@<ns>/onprem-export-full \\
      --output oci://<bucket>@<ns>/reports/cdb-shapes/faltantes_qab.parquet \\
      [--universe domain|all] [--jdbc-partitions 32] [--report-path out.json]

  enumerate_faltantes.py --self-test      # no data and no Oracle needed

Environment (same names as the validator):
  DATAGEN_SOURCE_JDBC_URL     jdbc:oracle:thin:@host:1521:sid   (required)
  DATAGEN_SOURCE_DB_USER      Oracle user                        (required)
  DATAGEN_SOURCE_DB_PASSWORD  Oracle password
  DATAGEN_SOURCE_SCHEMA       owner (default: CETIP)

Output semantics: rows for the two enumerated (TABELA, COLUNA) pairs are
REPLACED wholesale — this file is the complete truth for them as of the
enumeration; rows for any OTHER pair already present at --output (e.g. from
the validator's reactive --emit-faltantes) are preserved. VALOR is the
canonical key string that engorda_instrumentos._norm_key_col produces
('343237623', never '343237623.0000000000'). Drift grows with time: re-run
this job right before each regeneration.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enumerate_faltantes")

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"

ROOT_TABLE = "INSTRUMENTO_FINANCEIRO"
ROOT_KEY = "NUM_IF"
CONDICAO_IF_KEY = "NUM_CONDICAO_IF"
CDB_TIPO_IF = 49

CHILD_TABLE = "CARTEIRA_COMITENTE"
# (child column in CHILD_TABLE, Oracle parent table, Oracle parent column)
TARGETS: List[Tuple[str, str, str]] = [
    ("NUM_ID_ENTIDADE", "COMITENTE", "NUM_ID_ENTIDADE"),
    ("NUM_CONTA", "CONTA", "NUM_CONTA"),
]
DOMAIN_TABLES = [ROOT_TABLE, "TITULO", "CONDICAO_IF", "RESGATE", CHILD_TABLE]


# ---------------------------------------------------------------------------
# Small shared helpers (mirrors of profile_cdb_shapes.py / the generator)
# ---------------------------------------------------------------------------
def _is_uri(path: str) -> bool:
    return "://" in path


def write_text(spark: SparkSession, path: str, content: str) -> None:
    """Write a small text file locally or to any Spark-readable URI (oci://...)."""
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


def _ci(df: DataFrame, name: str) -> Optional[str]:
    lookup = {c.upper(): c for c in df.columns}
    return lookup.get(name.upper())


def active_rows(df: DataFrame, table: str) -> DataFrame:
    """Drop logically deleted rows (DAT_EXCLUSAO IS NOT NULL) when the column exists."""
    col = _ci(df, "DAT_EXCLUSAO")
    if col:
        return df.where(F.col(col).isNull())
    return df


def _norm_key(col):
    """Canonical comparable key string — the EXACT normalization the generator
    applies to both faltantes VALOR and the source column (_norm_key_col):
    trimmed string minus any all-zero fraction ('343.0000000000' -> '343')."""
    return F.regexp_replace(F.trim(col.cast("string")), r"\.0+$", "")


def _norm_pair(tabela: str, coluna: str) -> Tuple[str, str]:
    """(TABELA, COLUNA) as the generator normalizes them: upper, TABELA without
    a schema qualifier ('CETIP.CARTEIRA_COMITENTE' -> 'CARTEIRA_COMITENTE')."""
    return tabela.strip().upper().split(".")[-1], coluna.strip().upper()


# ---------------------------------------------------------------------------
# Config / IO
# ---------------------------------------------------------------------------
@dataclass
class Config:
    jdbc_url: str
    jdbc_user: str
    jdbc_password: str
    schema: str


def read_config() -> Config:
    url = os.environ.get("DATAGEN_SOURCE_JDBC_URL", "").strip()
    user = os.environ.get("DATAGEN_SOURCE_DB_USER", "").strip()
    pwd = os.environ.get("DATAGEN_SOURCE_DB_PASSWORD", "")
    schema = os.environ.get("DATAGEN_SOURCE_SCHEMA", "CETIP").strip().upper()
    if not (url and user):
        raise SystemExit(
            "DATAGEN_SOURCE_JDBC_URL and DATAGEN_SOURCE_DB_USER are required "
            "(this job exists to compare against the target Oracle)."
        )
    return Config(url, user, pwd, schema)


def read_tables(spark: SparkSession, base: str, names: List[str]) -> Dict[str, DataFrame]:
    out: Dict[str, DataFrame] = {}
    for name in names:
        path = f"{base.rstrip('/')}/{name}"
        try:
            out[name] = spark.read.parquet(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Table %s not readable at %s: %s", name, path, exc)
    return out


# ---------------------------------------------------------------------------
# Domain (image of profile_cdb_shapes.py build_universe/build_domain_keys)
# ---------------------------------------------------------------------------
def build_universe(tables: Dict[str, DataFrame]) -> DataFrame:
    """Active CDBs: NUM_TIPO_IF = 49, DAT_EXCLUSAO IS NULL."""
    root = tables.get(ROOT_TABLE)
    if root is None:
        raise SystemExit(f"{ROOT_TABLE} is required and was not readable.")
    tipo = _ci(root, "NUM_TIPO_IF")
    key = _ci(root, ROOT_KEY)
    if not tipo or not key:
        raise SystemExit(f"{ROOT_TABLE} lacks NUM_TIPO_IF/{ROOT_KEY}.")
    df = active_rows(root.where(F.col(tipo).cast("long") == CDB_TIPO_IF), ROOT_TABLE)
    return df.select(F.col(key).cast("long").alias(ROOT_KEY)).dropDuplicates()


def build_domain_keys(tables: Dict[str, DataFrame]) -> DataFrame:
    """IF-level product domain (team FILTRO_BASE): TITULO without escalonamento
    AND >=1 active CONDICAO_IF with an active RESGATE 'SEM TABELA'."""
    for t in ("TITULO", "CONDICAO_IF", "RESGATE"):
        if t not in tables:
            raise SystemExit(f"--universe domain requires table {t} in the input.")
    tit = tables["TITULO"]
    esc = _ci(tit, "COD_TIPO_ESCALONAMENTO")
    tit_ok = tit.where(F.col(esc).isNull()) if esc else tit
    tit_keys = tit_ok.select(F.col(_ci(tit, ROOT_KEY)).cast("long").alias(ROOT_KEY))

    cif = active_rows(tables["CONDICAO_IF"], "CONDICAO_IF")
    res = active_rows(tables["RESGATE"], "RESGATE")
    res_col = _ci(res, "COD_COND_RESGATE")
    res_ok = res.where(F.upper(F.trim(F.col(res_col).cast("string"))) == "SEM TABELA")
    res_keys = res_ok.select(
        F.col(_ci(res, CONDICAO_IF_KEY)).cast("long").alias(CONDICAO_IF_KEY)
    )
    cif_with_res = (
        cif.select(
            F.col(_ci(cif, CONDICAO_IF_KEY)).cast("long").alias(CONDICAO_IF_KEY),
            F.col(_ci(cif, ROOT_KEY)).cast("long").alias(ROOT_KEY),
        )
        .join(res_keys, CONDICAO_IF_KEY, "leftsemi")
        .select(ROOT_KEY)
    )
    return tit_keys.join(cif_with_res, ROOT_KEY, "leftsemi").dropDuplicates()


# ---------------------------------------------------------------------------
# Core logic (pure DataFrame functions — exercised by --self-test)
# ---------------------------------------------------------------------------
def referenced_keys(child: DataFrame, universe: DataFrame, column: str) -> DataFrame:
    """Distinct canonical key strings of `column` referenced by child rows whose
    NUM_IF is inside the universe. Column: VALOR (string)."""
    num_if = _ci(child, ROOT_KEY)
    col = _ci(child, column)
    if not num_if or not col:
        raise SystemExit(f"{CHILD_TABLE} lacks {ROOT_KEY}/{column}.")
    return (
        child.select(
            F.col(num_if).cast("long").alias(ROOT_KEY),
            _norm_key(F.col(col)).alias("VALOR"),
        )
        .where(F.col("VALOR").isNotNull() & (F.col("VALOR") != ""))
        .join(universe, ROOT_KEY, "leftsemi")
        .select("VALOR")
        .dropDuplicates()
    )


def missing_keys(referenced: DataFrame, existing: DataFrame) -> DataFrame:
    """Referenced keys with no match in the target's key set (both sides are
    canonical VALOR strings)."""
    return referenced.join(existing, "VALOR", "left_anti")


def read_oracle_keys(spark: SparkSession, cfg: Config, table: str, column: str,
                     partitions: int, fetch_size: int) -> DataFrame:
    """The target's full key set for one parent table, as canonical VALOR
    strings. Partitioned single-column JDBC read (bounds from a MIN/MAX probe);
    falls back to a single-partition read when bounds are unusable."""
    fq = f"{cfg.schema}.{table}"

    def _reader():
        return (
            spark.read.format("jdbc")
            .option("url", cfg.jdbc_url)
            .option("driver", ORACLE_DRIVER)
            .option("user", cfg.jdbc_user)
            .option("password", cfg.jdbc_password)
            .option("fetchsize", str(fetch_size))
        )

    bounds = (
        _reader()
        .option("dbtable", f"(SELECT MIN({column}) MN, MAX({column}) MX FROM {fq}) b")
        .load()
        .collect()[0]
    )
    reader = _reader().option(
        "dbtable", f"(SELECT {column} FROM {fq} WHERE {column} IS NOT NULL) t"
    )
    mn, mx = bounds["MN"], bounds["MX"]
    if mn is not None and mx is not None and int(mx) > int(mn) and partitions > 1:
        reader = (
            reader.option("partitionColumn", column)
            .option("lowerBound", str(int(mn)))
            .option("upperBound", str(int(mx)))
            .option("numPartitions", str(partitions))
        )
        logger.info("Oracle %s.%s: partitioned read, bounds [%s, %s] x %d partitions.",
                    fq, column, mn, mx, partitions)
    else:
        logger.info("Oracle %s.%s: single-partition read (bounds %s..%s).",
                    fq, column, mn, mx)
    df = reader.load()
    return df.select(_norm_key(F.col(df.columns[0])).alias("VALOR")).dropDuplicates()


def preserved_rows(existing: DataFrame,
                   replaced_pairs: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """Rows of an existing faltantes file whose (TABELA, COLUNA) — normalized
    the way the generator normalizes them — is NOT among the pairs this job
    replaces. Collected to the driver (materialized) so the same path can be
    overwritten safely afterwards."""
    cm = {c.upper(): c for c in existing.columns}
    need = ["TABELA", "COLUNA", "VALOR"]
    missing = [n for n in need if n not in cm]
    if missing:
        raise SystemExit(
            f"Existing --output has columns {existing.columns}; missing {missing}. "
            "Refusing to merge into a file that is not a faltantes parquet."
        )
    rows = existing.select(
        F.col(cm["TABELA"]).cast("string"),
        F.col(cm["COLUNA"]).cast("string"),
        F.col(cm["VALOR"]).cast("string"),
    ).collect()
    replaced = {(_norm_pair(t, c)) for t, c in replaced_pairs}
    return [
        (r[0], r[1], r[2]) for r in rows
        if _norm_pair(r[0] or "", r[1] or "") not in replaced
    ]


# ---------------------------------------------------------------------------
# Self-test (in-memory fixture; no raw data, no Oracle)
# ---------------------------------------------------------------------------
def run_selftest(spark: SparkSession) -> None:
    from decimal import Decimal as D  # DecimalType columns reject plain ints

    dec = "decimal(22,10)"
    tables = {
        ROOT_TABLE: spark.createDataFrame(
            [(D(1001), D(49), None), (D(1002), D(49), None), (D(1003), D(22), None),
             (D(1004), D(49), "2020-01-01")],
            f"NUM_IF {dec}, NUM_TIPO_IF {dec}, DAT_EXCLUSAO string",
        ),
        "TITULO": spark.createDataFrame(
            [(D(1001), None), (D(1002), None), (D(1003), None), (D(1004), None)],
            f"NUM_IF {dec}, COD_TIPO_ESCALONAMENTO string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(D(5001), D(1001), None), (D(5002), D(1002), None)],
            f"NUM_CONDICAO_IF {dec}, NUM_IF {dec}, DAT_EXCLUSAO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(D(5001), "SEM TABELA", None), (D(5002), "COM TABELA", None)],
            f"NUM_CONDICAO_IF {dec}, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        ),
        CHILD_TABLE: spark.createDataFrame(
            # 1001 in domain: ent 111 exists / 222 missing; conta 95 exists / 96 missing
            # 1002 out of domain (COM TABELA): ent 333 must NOT appear under 'domain'
            [(D(1001), D(111), D(95)), (D(1001), D(222), D(96)),
             (D(1002), D(333), D(95)), (D(1003), D(444), D(95))],
            f"NUM_IF {dec}, NUM_ID_ENTIDADE {dec}, NUM_CONTA {dec}",
        ),
    }
    oracle = {
        "COMITENTE": spark.createDataFrame([("111",)], "VALOR string"),
        "CONTA": spark.createDataFrame([("95",)], "VALOR string"),
    }

    universe = build_universe(tables)
    assert {r[0] for r in universe.collect()} == {1001, 1002}, "universe: tipo 49 + active"
    domain = universe.join(build_domain_keys(tables), ROOT_KEY, "leftsemi")
    assert {r[0] for r in domain.collect()} == {1001}, "domain: SEM TABELA exists-join"

    # Canonicalization: decimal(22,10) '222.0000000000' must anti-join as '222'.
    ref_ent = referenced_keys(tables[CHILD_TABLE], domain, "NUM_ID_ENTIDADE")
    assert {r[0] for r in ref_ent.collect()} == {"111", "222"}, "referenced canon"
    miss_ent = {r[0] for r in missing_keys(ref_ent, oracle["COMITENTE"]).collect()}
    assert miss_ent == {"222"}, f"missing comitente: {miss_ent}"

    ref_cta = referenced_keys(tables[CHILD_TABLE], domain, "NUM_CONTA")
    miss_cta = {r[0] for r in missing_keys(ref_cta, oracle["CONTA"]).collect()}
    assert miss_cta == {"96"}, f"missing conta: {miss_cta}"

    # --universe all widens to every active CDB (1002's ent 333 now counts).
    ref_all = referenced_keys(tables[CHILD_TABLE], universe, "NUM_ID_ENTIDADE")
    miss_all = {r[0] for r in missing_keys(ref_all, oracle["COMITENTE"]).collect()}
    assert miss_all == {"222", "333"}, f"missing (all): {miss_all}"

    # Merge: enumerated pairs are replaced, foreign pairs preserved (incl.
    # schema-qualified TABELA spellings, normalized like the generator does).
    existing = spark.createDataFrame(
        [("CETIP.CARTEIRA_COMITENTE", "NUM_ID_ENTIDADE", "999"),
         ("carteira_comitente", "num_conta", "888"),
         ("OPERACAO", "NUM_ID_TRANSF_ARQ_P1", "7")],
        "TABELA string, COLUNA string, VALOR string",
    )
    kept = preserved_rows(existing, [(CHILD_TABLE, c) for c, _, _ in TARGETS])
    assert kept == [("OPERACAO", "NUM_ID_TRANSF_ARQ_P1", "7")], f"preserved: {kept}"

    print("SELF-TEST OK: universe/domain, canonicalization, anti-join, merge.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-uri", help="Raw export root (folder-per-table parquet).")
    p.add_argument("--output", help="Faltantes parquet path/URI (TABELA/COLUNA/VALOR). "
                                    "Enumerated pairs replaced; other pairs preserved.")
    p.add_argument("--universe", choices=["domain", "all"], default="domain",
                   help="'domain' = team FILTRO_BASE (the generator's sampling "
                        "domain); 'all' = every active CDB (wider margin).")
    p.add_argument("--jdbc-partitions", type=int, default=32,
                   help="Partitions for the Oracle key-column reads (default 32).")
    p.add_argument("--fetch-size", type=int, default=10000,
                   help="JDBC fetch size (default 10000).")
    p.add_argument("--report-path", default=None,
                   help="Optional JSON summary path/URI.")
    p.add_argument("--self-test", action="store_true",
                   help="Run the embedded fixture test and exit (no data/Oracle).")
    args = p.parse_args()

    spark = SparkSession.builder.appName("enumerate_faltantes").getOrCreate()
    if args.self_test:
        run_selftest(spark)
        return
    if not args.base_uri or not args.output:
        raise SystemExit("--base-uri and --output are required (or use --self-test).")
    cfg = read_config()

    tables = read_tables(spark, args.base_uri, DOMAIN_TABLES)
    if CHILD_TABLE not in tables:
        raise SystemExit(f"{CHILD_TABLE} is required and was not readable.")
    universe = build_universe(tables)
    if args.universe == "domain":
        universe = universe.join(build_domain_keys(tables), ROOT_KEY, "leftsemi")
    universe = universe.cache()
    logger.info("Universe (%s): %d IF(s).", args.universe, universe.count())

    summary: dict = {"universe": args.universe, "targets": {}}
    result: Optional[DataFrame] = None
    for child_col, parent_table, parent_col in TARGETS:
        ref = referenced_keys(tables[CHILD_TABLE], universe, child_col).cache()
        n_ref = ref.count()
        ora = read_oracle_keys(spark, cfg, parent_table, parent_col,
                               args.jdbc_partitions, args.fetch_size)
        miss = missing_keys(ref, ora).cache()
        n_miss = miss.count()
        logger.info("%s.%s -> %s.%s: %d referenced, %d MISSING in target.",
                    CHILD_TABLE, child_col, parent_table, parent_col, n_ref, n_miss)
        summary["targets"][f"{CHILD_TABLE}.{child_col}"] = {
            "parent": f"{parent_table}.{parent_col}",
            "referenced_keys": n_ref,
            "missing_keys": n_miss,
        }
        piece = miss.select(
            F.lit(CHILD_TABLE).alias("TABELA"),
            F.lit(child_col).alias("COLUNA"),
            F.col("VALOR"),
        )
        result = piece if result is None else result.unionByName(piece)
        ref.unpersist()

    kept: List[Tuple[str, str, str]] = []
    try:
        existing = spark.read.parquet(args.output)
        kept = preserved_rows(existing, [(CHILD_TABLE, c) for c, _, _ in TARGETS])
        logger.info("Existing faltantes at %s: %d row(s) for other (TABELA, COLUNA) "
                    "pairs preserved.", args.output, len(kept))
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if not any(s in msg for s in ("Path does not exist", "PATH_NOT_FOUND",
                                      "Unable to infer schema", "FileNotFound")):
            raise
    if kept:
        result = result.unionByName(
            spark.createDataFrame(kept, "TABELA string, COLUNA string, VALOR string")
        )

    total = result.count()
    result.coalesce(4).write.mode("overwrite").parquet(args.output)
    summary["total_rows_written"] = total
    summary["output"] = args.output
    logger.info("Faltantes parquet written to %s (%d row(s) total). Regenerate with "
                "--faltantes-parquet %s — any sample is then clean by construction.",
                args.output, total, args.output)

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if args.report_path:
        write_text(spark, args.report_path, json.dumps(summary, indent=2, ensure_ascii=False))
        logger.info("JSON summary written to %s", args.report_path)


if __name__ == "__main__":
    main()
