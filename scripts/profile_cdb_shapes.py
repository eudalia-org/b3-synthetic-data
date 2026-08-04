#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
profile_cdb_shapes.py
=====================

Per-instrument cardinality ("shape") profiler for the CDB-simplificado domain.

For every CDB instrument (INSTRUMENTO_FINANCEIRO with NUM_TIPO_IF = 49 and
DAT_EXCLUSAO IS NULL) it counts the related rows in each table of the domain
and groups instruments by their count vector — the "shape". The output is the
distribution of shapes across the dataset, plus per-table marginal
distributions and attribute audits of the predicates used by engorda's
FILTROS_FONTE.

The reference shape below comes from a real registration traced in
docs/cetip.out (p6spy log of the NoMe application inserting one CDB
simplificado) and matches docs/query.sql:

    1 IF : 1 TITULO : 1 CREDITO : 2 CONDICAO_IF : 1 RESGATE :
    1 JUROS_FLUTUANTE : 2 EVENTO : 1 OPERACAO : 2 DADO_OPERACAO :
    1 LANCAMENTO : 1 DEPOSITO_AUTOMATICO_IF : 1 CARTEIRA_COMITENTE :
    1 CARTEIRA_PARTICIPANTE

Run it on the RAW production Parquet to learn the true population shape
distribution (run 1), again with --apply-filtros-fonte to get the ENGORDA-INPUT
image (run 2 — engorda only ever saw the filtered rows, so this is the fair
baseline), then on the synthetic output diffing against that baseline (run 3):

    spark-submit profile_cdb_shapes.py \
        --base-uri oci://bucket@ns/raw --label raw \
        --report-path oci://bucket@ns/reports/profile_raw.json

    spark-submit profile_cdb_shapes.py \
        --base-uri oci://bucket@ns/raw --label raw_filtered --apply-filtros-fonte \
        --report-path oci://bucket@ns/reports/profile_raw_filtered.json

    spark-submit profile_cdb_shapes.py \
        --base-uri oci://bucket@ns/synthetic --label synthetic \
        --report-path oci://bucket@ns/reports/profile_synthetic.json \
        --compare-with oci://bucket@ns/reports/profile_raw_filtered.json

Purely offline: reads only Parquet, no Oracle/JDBC required. Fully
self-contained: no imports from the rest of the repo, and it carries its own
verification — run `spark-submit profile_cdb_shapes.py --self-test` (no data
needed) to check the profiler against a built-in fixture before a real run.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("profile_cdb")

ROOT_TABLE = "INSTRUMENTO_FINANCEIRO"
ROOT_KEY = "NUM_IF"
CDB_TIPO_IF = 49

CONDICAO_IF_TABLE = "CONDICAO_IF"
CONDICAO_IF_KEY = "NUM_CONDICAO_IF"
CONDICAO_IF_TYPE = "COD_TIPO_CONDICAO_IF"
OPERACAO_TABLE = "OPERACAO"
OPERACAO_KEY = "NUM_ID_OPERACAO"

# Physical joined-subclass tables used by the application to resolve CONDICAO_IF.
# This app is deployed as a standalone file, so the list mirrors the validator.
SUBTYPE_TABLES = [
    "AMORTIZACAO",
    "ATUALIZACAO_POS",
    "ATUALIZACAO_PRE",
    "DESDOBRAMENTO",
    "JUROS_FIXO",
    "JUROS_FLUTUANTE",
    "OPCAO",
    "PARAMETRO_LIMITE",
    "PARTICIPACAO_LUCROS",
    "PREMIO",
    "PREMIO_CONTRATO",
    "PREMIO_OPCAO",
    "RESET",
    "RESGATE",
    "SPREAD",
    "TERMO",
]


@dataclass(frozen=True)
class Metric:
    """One count in the shape vector: rows of `table` per NUM_IF.

    via: None        -> table has NUM_IF, join directly;
         "CONDICAO_IF" -> table keys on NUM_CONDICAO_IF, resolve NUM_IF through
                          the (active) CONDICAO_IF rows of the universe;
         "OPERACAO"    -> table keys on NUM_ID_OPERACAO, resolve through the
                          OPERACAO rows of the universe.
    where: optional (column, normalized value) equality filter applied to the
         table's rows before counting (e.g. EVENTO by NUM_TIPO_EVENTO_LEGADO).
    """

    name: str
    table: str
    via: Optional[str] = None
    where: Optional[tuple] = None


# Order defines the shape-signature order. Names are the table names (or
# TABLE_QUALIFIER for filtered metrics) so the report needs no legend.
METRICS: List[Metric] = [
    Metric("TITULO", "TITULO"),
    Metric("CREDITO", "CREDITO"),
    Metric("CONDICAO_IF", "CONDICAO_IF"),
    Metric("RESGATE", "RESGATE", via="CONDICAO_IF"),
    Metric("JUROS_FLUTUANTE", "JUROS_FLUTUANTE", via="CONDICAO_IF"),
    Metric("JUROS_FIXO", "JUROS_FIXO", via="CONDICAO_IF"),
    Metric("ATUALIZACAO_POS", "ATUALIZACAO_POS", via="CONDICAO_IF"),
    Metric("ATUALIZACAO_PRE", "ATUALIZACAO_PRE", via="CONDICAO_IF"),
    Metric("SPREAD", "SPREAD", via="CONDICAO_IF"),
    Metric("EVENTO", "EVENTO"),
    # Every domain IF has an evento tipo 85 and ~96% also a tipo 83 (team
    # proportions query, 2026-07-21); the cetip.out registration inserts
    # exactly one of each. Counted separately so a generator emitting two
    # same-tipo eventos cannot pass as "EVENTO=2".
    Metric("EVENTO_TIPO83", "EVENTO", where=("NUM_TIPO_EVENTO_LEGADO", "83")),
    Metric("EVENTO_TIPO85", "EVENTO", where=("NUM_TIPO_EVENTO_LEGADO", "85")),
    Metric("OPERACAO", "OPERACAO"),
    Metric("DADO_OPERACAO", "DADO_OPERACAO", via="OPERACAO"),
    Metric("LANCAMENTO", "LANCAMENTO", via="OPERACAO"),
    Metric("DEPOSITO_AUTOMATICO_IF", "DEPOSITO_AUTOMATICO_IF"),
    Metric("CARTEIRA_COMITENTE", "CARTEIRA_COMITENTE"),
    Metric("CARTEIRA_PARTICIPANTE", "CARTEIRA_PARTICIPANTE"),
]

# Write-set of one real CDB-simplificado registration (docs/cetip.out).
REFERENCE_SHAPE: Dict[str, int] = {
    "TITULO": 1,
    "CREDITO": 1,
    "CONDICAO_IF": 2,
    "RESGATE": 1,
    "JUROS_FLUTUANTE": 1,
    "JUROS_FIXO": 0,
    "ATUALIZACAO_POS": 0,
    "ATUALIZACAO_PRE": 0,
    "SPREAD": 0,
    "EVENTO": 2,
    "EVENTO_TIPO83": 1,
    "EVENTO_TIPO85": 1,
    "OPERACAO": 1,
    "DADO_OPERACAO": 2,
    "LANCAMENTO": 1,
    "DEPOSITO_AUTOMATICO_IF": 1,
    "CARTEIRA_COMITENTE": 1,
    "CARTEIRA_PARTICIPANTE": 1,
}

# Tables the profiler needs beyond the metric tables.
EXTRA_TABLES = ["COMITENTE"]

# Engorda's FILTROS_FONTE row predicates (image of engorda_tables.py), minus the
# parts the profiler always applies anyway (the IF root filter and the
# DAT_EXCLUSAO IS NULL active-row rule). With --apply-filtros-fonte these are
# applied to the source tables before profiling, so the resulting profile is the
# ENGORDA-INPUT image of the data — the right baseline to diff the synthetic
# output against. Without the flag the profile is the unfiltered product truth.
# A predicate whose column is missing is ignored with a note (engorda does the
# same, defensively against schema variation).
FILTROS_FONTE = {
    "RESGATE": [("COD_COND_RESGATE", "ieq", "SEM TABELA")],
    "TITULO": [("COD_TIPO_ESCALONAMENTO", "isnull", None)],
    "CARTEIRA_COMITENTE": [("QTD_CARTEIRA_COMITENTE", ">", 0)],
    "CARTEIRA_PARTICIPANTE": [("QTD_CARTEIRA_PARTICIPANTE", ">", 0)],
}

# Attribute audits: value distributions (within the universe) of the columns
# engorda's FILTROS_FONTE filters on — measured, not assumed.
AUDITS = [
    ("RESGATE", "COD_COND_RESGATE", "upper_trim"),
    ("TITULO", "COD_TIPO_ESCALONAMENTO", "raw"),
    ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF", "code"),
    ("EVENTO", "NUM_TIPO_EVENTO_LEGADO", "code"),
]

MARGINAL_CAP = 5  # per-IF counts above this are bucketed as "5+"


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------
def read_tables(spark: SparkSession, base: str, names: List[str]) -> Dict[str, DataFrame]:
    out: Dict[str, DataFrame] = {}
    for name in names:
        path = f"{base}/{name}"
        try:
            out[name] = spark.read.parquet(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Table %s not readable at %s: %s", name, path, exc)
    return out


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


def _ci(df: DataFrame, name: str) -> Optional[str]:
    lookup = {c.upper(): c for c in df.columns}
    return lookup.get(name.upper())


def active_rows(df: DataFrame, notes: List[str], table: str) -> DataFrame:
    """Drop logically deleted rows (DAT_EXCLUSAO IS NOT NULL) when the column exists."""
    col = _ci(df, "DAT_EXCLUSAO")
    if col:
        notes.append(f"{table}: filtered DAT_EXCLUSAO IS NULL")
        return df.where(F.col(col).isNull())
    return df


def _norm_code(col):
    """Normalize an id/code column: trimmed string without a trailing .0."""
    return F.regexp_replace(F.trim(col.cast("string")), r"\.0$", "")


# ---------------------------------------------------------------------------
# Profile construction (pure DataFrame logic; testable without IO)
# ---------------------------------------------------------------------------
def build_domain_keys(tables: Dict[str, DataFrame], notes: List[str]) -> DataFrame:
    """IF-level product domain (team FILTRO_BASE query, 2026-07-21): IFs whose
    TITULO has no escalonamento AND that have >=1 active CONDICAO_IF with an
    active RESGATE 'SEM TABELA'. Root predicates (tipo 49, active) are applied
    by build_universe; this adds the exists-semi-joins."""
    for t in ("TITULO", "CONDICAO_IF", "RESGATE"):
        if t not in tables:
            raise SystemExit(f"--universe domain requires table {t} in the input.")
    tit = tables["TITULO"]
    esc = _ci(tit, "COD_TIPO_ESCALONAMENTO")
    tit_ok = tit.where(F.col(esc).isNull()) if esc else tit
    tit_keys = tit_ok.select(F.col(_ci(tit, ROOT_KEY)).cast("long").alias(ROOT_KEY))

    cif = active_rows(tables["CONDICAO_IF"], notes, "CONDICAO_IF")
    res = active_rows(tables["RESGATE"], notes, "RESGATE")
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
    notes.append(
        "universe=domain: exists(active CONDICAO_IF with active RESGATE 'SEM TABELA') "
        "AND TITULO.COD_TIPO_ESCALONAMENTO IS NULL"
    )
    return tit_keys.join(cif_with_res, ROOT_KEY, "leftsemi").dropDuplicates()


def build_universe(
    tables: Dict[str, DataFrame],
    notes: List[str],
    universe_keys: Optional[DataFrame] = None,
) -> DataFrame:
    root = tables.get(ROOT_TABLE)
    if root is None:
        raise SystemExit(f"{ROOT_TABLE} is required and was not readable.")
    tipo = _ci(root, "NUM_TIPO_IF")
    key = _ci(root, ROOT_KEY)
    if not tipo or not key:
        raise SystemExit(f"{ROOT_TABLE} lacks NUM_TIPO_IF/{ROOT_KEY}.")
    df = root.where(F.col(tipo).cast("long") == CDB_TIPO_IF)
    df = active_rows(df, notes, ROOT_TABLE)
    universe = df.select(F.col(key).cast("long").alias(ROOT_KEY)).dropDuplicates()
    if universe_keys is not None:
        universe = universe.join(universe_keys, ROOT_KEY, "leftsemi")
        notes.append("universe restricted to --universe-keys")
    return universe


def build_subtype_map_snapshot(
    tables: Dict[str, DataFrame], universe: DataFrame
) -> dict:
    """Observe joined-subclass discriminator values within the baseline universe."""
    snapshot = {
        "version": 1,
        "source": "raw_parquet",
        "condition_table": CONDICAO_IF_TABLE,
        "key_column": CONDICAO_IF_KEY,
        "type_column": CONDICAO_IF_TYPE,
        "observed_by_table": {},
        "missing_tables": [],
        "unobserved_tables": [],
        "invalid_tables": [],
    }
    membership = None
    readable_tables = []
    for table in SUBTYPE_TABLES:
        subtype = tables.get(table)
        if subtype is None:
            snapshot["missing_tables"].append(table)
            continue
        subtype_key = _ci(subtype, CONDICAO_IF_KEY)
        if not subtype_key:
            snapshot["invalid_tables"].append(table)
            continue
        readable_tables.append(table)
        projection = active_rows(subtype, [], table).select(
            F.col(subtype_key).cast("long").alias(CONDICAO_IF_KEY),
            F.lit(table).alias("subtype_table"),
        )
        membership = projection if membership is None else membership.unionByName(projection)

    condition = tables.get(CONDICAO_IF_TABLE)
    if condition is None:
        snapshot["missing_tables"].append(CONDICAO_IF_TABLE)
        snapshot["missing_tables"].sort()
        snapshot["invalid_tables"].sort()
        snapshot["unobserved_tables"] = sorted(readable_tables)
        return snapshot

    condition_key = _ci(condition, CONDICAO_IF_KEY)
    condition_if = _ci(condition, ROOT_KEY)
    condition_type = _ci(condition, CONDICAO_IF_TYPE)
    if not condition_key or not condition_if or not condition_type:
        snapshot["invalid_tables"].append(CONDICAO_IF_TABLE)
        snapshot["missing_tables"].sort()
        snapshot["invalid_tables"].sort()
        snapshot["unobserved_tables"] = sorted(readable_tables)
        return snapshot

    condition_scope = (
        active_rows(condition, [], CONDICAO_IF_TABLE)
        .select(
            F.col(condition_key).cast("long").alias(CONDICAO_IF_KEY),
            F.col(condition_if).cast("long").alias(ROOT_KEY),
            _norm_code(F.col(condition_type)).alias("condition_type"),
        )
        .join(universe, ROOT_KEY, "leftsemi")
        .select(CONDICAO_IF_KEY, "condition_type")
    )

    observed = {}
    if membership is not None:
        rows = (
            membership.join(condition_scope, CONDICAO_IF_KEY, "inner")
            .groupBy("subtype_table")
            .agg(F.sort_array(F.collect_set("condition_type")).alias("types"))
            .collect()
        )
        observed = {row["subtype_table"]: list(row["types"]) for row in rows}

    snapshot["observed_by_table"] = dict(sorted(observed.items()))
    snapshot["missing_tables"].sort()
    snapshot["invalid_tables"].sort()
    snapshot["unobserved_tables"] = sorted(set(readable_tables) - set(observed))
    return snapshot


def _keyed_by_num_if(
    tables: Dict[str, DataFrame], metric: Metric, notes: List[str]
) -> Optional[DataFrame]:
    """Return a DF with one row per counted child row, keyed by NUM_IF."""
    df = tables.get(metric.table)
    if df is None:
        return None
    df = active_rows(df, notes, metric.table)

    if metric.where is not None:
        wcol_name, wval = metric.where
        wcol = _ci(df, wcol_name)
        if not wcol:
            return None  # filter column absent -> metric skipped, not miscounted
        df = df.where(_norm_code(F.col(wcol)) == wval)

    if metric.via is None:
        key = _ci(df, ROOT_KEY)
        if not key:
            return None
        return df.select(F.col(key).cast("long").alias(ROOT_KEY))

    if metric.via == "CONDICAO_IF":
        bridge, bkey = tables.get(CONDICAO_IF_TABLE), CONDICAO_IF_KEY
    elif metric.via == "OPERACAO":
        bridge, bkey = tables.get(OPERACAO_TABLE), OPERACAO_KEY
    else:
        raise ValueError(f"Unknown via: {metric.via}")
    if bridge is None:
        return None
    child_key = _ci(df, bkey)
    bridge_key = _ci(bridge, bkey)
    bridge_if = _ci(bridge, ROOT_KEY)
    if not child_key or not bridge_key or not bridge_if:
        return None
    bridge = active_rows(bridge, [], metric.via)  # notes already recorded once
    bridge = bridge.select(
        F.col(bridge_key).cast("long").alias("bk"),
        F.col(bridge_if).cast("long").alias(ROOT_KEY),
    )
    child = df.select(F.col(child_key).cast("long").alias("bk"))
    return child.join(bridge, "bk", "inner").select(ROOT_KEY)


def build_counts(
    universe: DataFrame, tables: Dict[str, DataFrame], notes: List[str]
) -> tuple[DataFrame, List[str]]:
    """Left-join per-metric counts onto the universe. Returns (df, skipped)."""
    result = universe
    skipped: List[str] = []
    for metric in METRICS:
        keyed = _keyed_by_num_if(tables, metric, notes)
        if keyed is None:
            skipped.append(metric.name)
            result = result.withColumn(metric.name, F.lit(None).cast("long"))
            continue
        counts = keyed.groupBy(ROOT_KEY).agg(F.count(F.lit(1)).alias(metric.name))
        result = result.join(counts, ROOT_KEY, "left")
        result = result.withColumn(metric.name, F.coalesce(F.col(metric.name), F.lit(0)))
    return result, skipped


def add_simplificado_flag(
    counts: DataFrame, tables: Dict[str, DataFrame], notes: List[str]
) -> DataFrame:
    """SIMPLIFICADO per IF: does any holding comitente (via CARTEIRA_COMITENTE)
    have IND_COMITENTE_SIMPLIFICADO = 'S'? Values: yes / no / no_carteira / unknown."""
    cart = tables.get("CARTEIRA_COMITENTE")
    com = tables.get("COMITENTE")
    if cart is None or com is None:
        notes.append("SIMPLIFICADO flag skipped: CARTEIRA_COMITENTE/COMITENTE missing")
        return counts.withColumn("SIMPLIFICADO", F.lit("unknown"))
    c_if, c_ent = _ci(cart, ROOT_KEY), _ci(cart, "NUM_ID_ENTIDADE")
    m_ent, m_flag = _ci(com, "NUM_ID_ENTIDADE"), _ci(com, "IND_COMITENTE_SIMPLIFICADO")
    if not all([c_if, c_ent, m_ent, m_flag]):
        notes.append("SIMPLIFICADO flag skipped: join columns missing")
        return counts.withColumn("SIMPLIFICADO", F.lit("unknown"))

    holders = cart.select(
        F.col(c_if).cast("long").alias(ROOT_KEY),
        F.col(c_ent).cast("long").alias("ent"),
    )
    flags = com.select(
        F.col(m_ent).cast("long").alias("ent"),
        F.upper(F.trim(F.col(m_flag).cast("string"))).alias("flag"),
    )
    per_if = (
        holders.join(flags, "ent", "left")
        .groupBy(ROOT_KEY)
        .agg(F.max(F.when(F.col("flag") == "S", 1).otherwise(0)).alias("any_s"))
        .select(
            ROOT_KEY,
            F.when(F.col("any_s") == 1, "yes").otherwise("no").alias("SIMPLIFICADO"),
        )
    )
    out = counts.join(per_if, ROOT_KEY, "left")
    return out.withColumn(
        "SIMPLIFICADO", F.coalesce(F.col("SIMPLIFICADO"), F.lit("no_carteira"))
    )


def shape_signature_col(metric_names: List[str]):
    parts = [
        F.concat(F.lit(f"{name}="), F.col(name).cast("string")) for name in metric_names
    ]
    return F.concat_ws("|", *parts)


def shape_distribution(
    counts: DataFrame, metric_names: List[str], sample_size: int
) -> List[dict]:
    sig = shape_signature_col(metric_names)
    rows = (
        counts.withColumn("shape", sig)
        .groupBy("shape")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.slice(F.collect_list(F.col(ROOT_KEY)), 1, sample_size).alias("sample"),
            *[F.first(F.col(name)).alias(name) for name in metric_names],
        )
        .orderBy(F.desc("n"))
        .collect()
    )
    total = sum(r["n"] for r in rows) or 1
    out = []
    for r in rows:
        out.append(
            {
                "shape": r["shape"],
                "counts": {name: r[name] for name in metric_names},
                "n": r["n"],
                "pct": round(100.0 * r["n"] / total, 4),
                "sample_num_if": [int(x) for x in (r["sample"] or [])],
            }
        )
    return out


def marginals(counts: DataFrame, metric_names: List[str]) -> Dict[str, Dict[str, int]]:
    """Per metric: distribution of the per-IF count (0,1,...,cap+)."""
    out: Dict[str, Dict[str, int]] = {}
    aggs = []
    for name in metric_names:
        bucket = (
            F.when(F.col(name).isNull(), "missing")
            .when(F.col(name) >= MARGINAL_CAP, f"{MARGINAL_CAP}+")
            .otherwise(F.col(name).cast("string"))
        )
        aggs.append(bucket.alias(name))
    bucketed = counts.select(*aggs)
    for name in metric_names:
        rows = bucketed.groupBy(name).count().collect()
        out[name] = {r[name]: r["count"] for r in rows}
    return out


def reference_match(counts: DataFrame, metric_names: List[str]) -> dict:
    cond = None
    for name in metric_names:
        expected = REFERENCE_SHAPE.get(name)
        if expected is None:
            continue
        this = F.col(name) == expected
        cond = this if cond is None else (cond & this)
    total = counts.count()
    matching = counts.where(cond).count() if cond is not None else 0
    return {
        "reference_shape": REFERENCE_SHAPE,
        "total_ifs": total,
        "matching_ifs": matching,
        "pct": round(100.0 * matching / total, 4) if total else 0.0,
    }


def evento_path_crosscheck(
    universe: DataFrame, tables: Dict[str, DataFrame], sample_size: int
) -> dict:
    """EVENTO carries both NUM_IF and NUM_CONDICAO_IF; the counts through the
    two paths must agree. Disagreement means EVENTO rows whose NUM_IF and
    NUM_CONDICAO_IF point at different instruments — engorda rebinding the two
    FKs independently would produce exactly that."""
    eve = tables.get("EVENTO")
    cif = tables.get(CONDICAO_IF_TABLE)
    if eve is None or cif is None:
        return {"status": "skipped", "reason": "EVENTO or CONDICAO_IF missing"}
    e_if, e_cif = _ci(eve, ROOT_KEY), _ci(eve, CONDICAO_IF_KEY)
    c_key, c_if = _ci(cif, CONDICAO_IF_KEY), _ci(cif, ROOT_KEY)
    if not all([e_if, e_cif, c_key, c_if]):
        return {"status": "skipped", "reason": "join columns missing"}

    notes: List[str] = []
    eve_a = active_rows(eve, notes, "EVENTO").select(
        F.col(e_if).cast("long").alias("if_direct"),
        F.col(e_cif).cast("long").alias("bk"),
    )
    cif_a = active_rows(cif, notes, CONDICAO_IF_TABLE).select(
        F.col(c_key).cast("long").alias("bk"),
        F.col(c_if).cast("long").alias("if_via_cif"),
    )
    joined = eve_a.join(cif_a, "bk", "inner").where(
        F.col("if_direct").isNotNull() & F.col("if_via_cif").isNotNull()
    )
    in_universe = joined.join(
        universe.withColumnRenamed(ROOT_KEY, "if_direct"), "if_direct", "leftsemi"
    )
    mismatched = in_universe.where(F.col("if_direct") != F.col("if_via_cif"))
    n_bad = mismatched.count()
    return {
        "status": "ok" if n_bad == 0 else "mismatch",
        "checked": in_universe.count(),
        "mismatched": n_bad,
        "sample": [
            {"num_if_direct": int(r["if_direct"]), "num_if_via_condicao": int(r["if_via_cif"])}
            for r in mismatched.limit(sample_size).collect()
        ],
    }


def attribute_audits(
    universe: DataFrame, tables: Dict[str, DataFrame], notes: List[str]
) -> Dict[str, dict]:
    """Value distributions of the FILTROS_FONTE columns, inside the universe."""
    out: Dict[str, dict] = {}
    cif = tables.get(CONDICAO_IF_TABLE)
    for table, column, mode in AUDITS:
        df = tables.get(table)
        if df is None:
            continue
        col = _ci(df, column)
        if not col:
            out[f"{table}.{column}"] = {"status": "column missing"}
            continue
        df = active_rows(df, [], table)
        key = _ci(df, ROOT_KEY)
        if key:
            scoped = df.join(
                universe.withColumnRenamed(ROOT_KEY, "u"),
                F.col(key).cast("long") == F.col("u"),
                "leftsemi",
            )
        elif _ci(df, CONDICAO_IF_KEY) and cif is not None:
            bridge = active_rows(cif, [], CONDICAO_IF_TABLE).join(
                universe, _ci(cif, ROOT_KEY), "leftsemi"
            )
            scoped = df.join(
                bridge.select(F.col(_ci(cif, CONDICAO_IF_KEY)).cast("long").alias("bk")),
                F.col(_ci(df, CONDICAO_IF_KEY)).cast("long") == F.col("bk"),
                "leftsemi",
            )
        else:
            out[f"{table}.{column}"] = {"status": "cannot scope to universe"}
            continue

        c = F.col(col)
        if mode == "upper_trim":
            value = F.upper(F.trim(c.cast("string")))
        elif mode == "code":
            value = _norm_code(c)
        else:
            value = c.cast("string")
        rows = (
            scoped.select(F.coalesce(value, F.lit("<NULL>")).alias("v"))
            .groupBy("v")
            .count()
            .orderBy(F.desc("count"))
            .limit(50)
            .collect()
        )
        out[f"{table}.{column}"] = {r["v"]: r["count"] for r in rows}
    return out


def apply_filtros_fonte(
    tables: Dict[str, DataFrame], notes: List[str]
) -> Dict[str, DataFrame]:
    out = dict(tables)
    for table, preds in FILTROS_FONTE.items():
        df = out.get(table)
        if df is None:
            continue
        for column, op, value in preds:
            col = _ci(df, column)
            if not col:
                notes.append(
                    f"FILTROS_FONTE: {table}.{column} missing; predicate ignored"
                )
                continue
            c = F.col(col)
            if op == "ieq":
                cond = F.upper(F.trim(c.cast("string"))) == value
            elif op == ">":
                cond = c > value
            elif op == "isnull":
                cond = c.isNull()
            else:
                raise ValueError(f"Unknown FILTROS_FONTE op: {op}")
            df = df.where(cond)
            notes.append(f"FILTROS_FONTE applied: {table}.{column} {op} {value!r}")
        out[table] = df
    return out


def build_profile(
    tables: Dict[str, DataFrame],
    sample_size: int = 10,
    apply_filtros: bool = False,
    universe_keys: Optional[DataFrame] = None,
    universe_mode: str = "all",
) -> dict:
    """Full profile over an in-memory dict of DataFrames. Pure of IO."""
    notes: List[str] = []
    metric_names = [m.name for m in METRICS]
    if apply_filtros:
        tables = apply_filtros_fonte(tables, notes)

    if universe_mode == "domain":
        domain = build_domain_keys(tables, notes)
        universe_keys = (
            domain if universe_keys is None
            else universe_keys.join(domain, ROOT_KEY, "leftsemi")
        )
    universe = build_universe(tables, notes, universe_keys)
    counts, skipped = build_counts(universe, tables, notes)
    counts = add_simplificado_flag(counts, tables, notes)
    counts = counts.cache()

    profile = {
        "universe_size": counts.count(),
        "filtros_fonte_applied": apply_filtros,
        "metrics_skipped": skipped,
        "notes": sorted(set(notes)),
        "reference_match": reference_match(counts, metric_names),
        "shapes": shape_distribution(counts, metric_names, sample_size),
        "marginals": marginals(counts, metric_names),
        "by_simplificado": {},
        "evento_path_crosscheck": evento_path_crosscheck(universe, tables, sample_size),
        "attribute_audits": attribute_audits(universe, tables, notes),
        "subtype_map": build_subtype_map_snapshot(tables, universe),
    }

    for r in counts.groupBy("SIMPLIFICADO").count().collect():
        seg = counts.where(F.col("SIMPLIFICADO") == r["SIMPLIFICADO"])
        profile["by_simplificado"][r["SIMPLIFICADO"]] = {
            "n": r["count"],
            "top_shapes": shape_distribution(seg, metric_names, sample_size)[:5],
        }

    counts.unpersist()
    return profile


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def compare_profiles(current: dict, other: dict, other_label: str) -> dict:
    cur = {s["shape"]: s for s in current["shapes"]}
    oth = {s["shape"]: s for s in other.get("shapes", [])}
    only_current = [
        {"shape": k, "n": v["n"], "pct": v["pct"], "sample_num_if": v["sample_num_if"]}
        for k, v in cur.items() if k not in oth
    ]
    only_other = [
        {"shape": k, "n": v["n"], "pct": v["pct"]} for k, v in oth.items() if k not in cur
    ]
    drift = [
        {
            "shape": k,
            "pct_current": cur[k]["pct"],
            f"pct_{other_label}": oth[k]["pct"],
            "abs_diff": round(abs(cur[k]["pct"] - oth[k]["pct"]), 4),
        }
        for k in cur.keys() & oth.keys()
    ]
    drift.sort(key=lambda d: -d["abs_diff"])
    only_current.sort(key=lambda d: -d["n"])
    only_other.sort(key=lambda d: -d["n"])
    return {
        "compared_with": other_label,
        "shapes_only_in_current": only_current,
        f"shapes_only_in_{other_label}": only_other,
        "shared_shape_pct_drift": drift,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_report(profile: dict, label: str, top: int) -> None:
    print("\n" + "=" * 78)
    print(f"CDB-SIMPLIFICADO SHAPE PROFILE — {label}")
    print("=" * 78)
    print(f"Universe (NUM_TIPO_IF={CDB_TIPO_IF}, active): {profile['universe_size']} IFs")
    if profile.get("filtros_fonte_applied"):
        print("FILTROS_FONTE row predicates APPLIED — this is the engorda-input image.")
    if profile["metrics_skipped"]:
        print(f"Metrics skipped (table/columns unavailable): {profile['metrics_skipped']}")

    ref = profile["reference_match"]
    print(
        f"\nReference shape (cetip.out write-set): {ref['matching_ifs']}/{ref['total_ifs']} "
        f"IFs match exactly ({ref['pct']}%)"
    )

    print(f"\nTop {top} shapes:")
    for s in profile["shapes"][:top]:
        print(f"  {s['pct']:8.3f}%  n={s['n']:<10} {s['shape']}")
    remaining = profile["shapes"][top:]
    if remaining:
        rest_pct = round(sum(s["pct"] for s in remaining), 3)
        print(f"  ... {len(remaining)} more shapes totalling {rest_pct}%")

    print("\nPer-table marginal distribution of rows per IF:")
    for name, dist in profile["marginals"].items():
        ordered = sorted(dist.items(), key=lambda kv: kv[0])
        pretty = ", ".join(f"{k}:{v}" for k, v in ordered)
        print(f"  {name:24} {pretty}")

    print("\nBy comitente-simplificado flag:")
    for seg, data in sorted(profile["by_simplificado"].items()):
        print(f"  {seg}: n={data['n']}")
        for s in data["top_shapes"][:3]:
            print(f"      {s['pct']:8.3f}%  {s['shape']}")

    xc = profile["evento_path_crosscheck"]
    print(f"\nEVENTO NUM_IF vs NUM_CONDICAO_IF->NUM_IF crosscheck: {xc.get('status')}")
    if xc.get("status") == "mismatch":
        print(f"  {xc['mismatched']}/{xc['checked']} EVENTO rows point at two different IFs")
        print(f"  sample: {xc['sample']}")

    print("\nAttribute audits (FILTROS_FONTE columns, values inside the universe):")
    for key, dist in profile["attribute_audits"].items():
        print(f"  {key}: {dist}")

    comparison = profile.get("comparison")
    if comparison:
        other = comparison["compared_with"]
        print("\n" + "-" * 78)
        print(f"COMPARISON vs {other}")
        oc = comparison["shapes_only_in_current"]
        oo = comparison[f"shapes_only_in_{other}"]
        print(f"  Shapes only in current ({len(oc)}):")
        for s in oc[:top]:
            print(f"    n={s['n']:<8} {s['pct']:7.3f}%  {s['shape']}")
        print(f"  Shapes only in {other} ({len(oo)}):")
        for s in oo[:top]:
            print(f"    n={s['n']:<8} {s['pct']:7.3f}%  {s['shape']}")
        print("  Largest pct drift on shared shapes:")
        for d in comparison["shared_shape_pct_drift"][:top]:
            print(
                f"    cur={d['pct_current']:7.3f}%  {other}={d[f'pct_{other}']:7.3f}%  "
                f"diff={d['abs_diff']:6.3f}  {d['shape']}"
            )
    print()


# ---------------------------------------------------------------------------
# Self-test: built-in fixture with known shapes, no data needed
# ---------------------------------------------------------------------------
def _selftest_tables(spark: SparkSession) -> Dict[str, DataFrame]:
    """Three active CDBs: 1001 matches the cetip.out reference write-set exactly,
    1002 deviates (no CREDITO, 3 CONDICAO_IF, JUROS_FIXO, mis-pathed EVENTO),
    1003 is an empty shell. 1004 (excluded) and 2001 (non-CDB) must not count."""

    def df(rows, cols):
        types = {"DAT_EXCLUSAO": "string", "COD_TIPO_ESCALONAMENTO": "string",
                 "COD_COND_RESGATE": "string", "COD_TIPO_CONDICAO_IF": "string",
                 "IND_COMITENTE_SIMPLIFICADO": "string"}
        schema = ", ".join(f"{c} {types.get(c, 'long')}" for c in cols)
        return spark.createDataFrame(rows, schema)

    return {
        "INSTRUMENTO_FINANCEIRO": df(
            [(1001, 49, None), (1002, 49, None), (1003, 49, None),
             (1004, 49, "2024-01-01"), (2001, 50, None)],
            ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"],
        ),
        "TITULO": df(
            [(1001, None), (1002, "EMISSAO"), (1004, None), (2001, None)],
            ["NUM_IF", "COD_TIPO_ESCALONAMENTO"],
        ),
        "CREDITO": df([(1001,), (2001,)], ["NUM_IF"]),
        "CONDICAO_IF": df(
            [(11, 1001, "20", None), (12, 1001, "3", None), (13, 1002, "2", None),
             (14, 1002, "1", None), (15, 1002, "5", None), (16, 1004, "3", None)],
            ["NUM_CONDICAO_IF", "NUM_IF", "COD_TIPO_CONDICAO_IF", "DAT_EXCLUSAO"],
        ),
        "RESGATE": df([(11, "SEM TABELA", None), (14, "COM TABELA", None)],
                      ["NUM_CONDICAO_IF", "COD_COND_RESGATE", "DAT_EXCLUSAO"]),
        "JUROS_FLUTUANTE": df(
            [(12, None), (13, "2024-01-01")],
            ["NUM_CONDICAO_IF", "DAT_EXCLUSAO"],
        ),
        "JUROS_FIXO": df([(13,)], ["NUM_CONDICAO_IF"]),
        "ATUALIZACAO_POS": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "ATUALIZACAO_PRE": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "SPREAD": df([(15,)], ["NUM_CONDICAO_IF"]),
        # 1001: one tipo-83 + one tipo-85 event (as in the cetip.out registration).
        # 1002's tipo-85 event carries a NUM_CONDICAO_IF belonging to 1001 -> path mismatch.
        "EVENTO": df(
            [(91, 1001, 11, 83, None), (92, 1001, 12, 85, None),
             (93, 1002, 11, 85, None)],
            ["NUM_EVENTO", "NUM_IF", "NUM_CONDICAO_IF",
             "NUM_TIPO_EVENTO_LEGADO", "DAT_EXCLUSAO"],
        ),
        "OPERACAO": df([(501, 1001)], ["NUM_ID_OPERACAO", "NUM_IF"]),
        "DADO_OPERACAO": df([(1, 501), (2, 501)],
                            ["NUM_ID_DADO_OPERACAO", "NUM_ID_OPERACAO"]),
        "LANCAMENTO": df([(1, 501)], ["NUM_ID_LANCAMENTO", "NUM_ID_OPERACAO"]),
        "DEPOSITO_AUTOMATICO_IF": df([(1001,)], ["NUM_IF"]),
        "CARTEIRA_COMITENTE": df(
            [(1, 1001, 900), (2, 1002, 901)],
            ["NUM_CARTEIRA_COMITENTE", "NUM_IF", "NUM_ID_ENTIDADE"],
        ),
        "CARTEIRA_PARTICIPANTE": df([(1, 1001)],
                                    ["NUM_CARTEIRA_PARTICIPANTE", "NUM_IF"]),
        "COMITENTE": df([(900, "S"), (901, "N")],
                        ["NUM_ID_ENTIDADE", "IND_COMITENTE_SIMPLIFICADO"]),
    }


def run_selftest(spark: SparkSession) -> None:
    def shape_of(profile, num_if):
        for s in profile["shapes"]:
            if num_if in s["sample_num_if"]:
                return s
        raise AssertionError(f"NUM_IF {num_if} not found in any shape sample")

    profile = build_profile(_selftest_tables(spark), sample_size=10)

    assert profile["universe_size"] == 3, profile["universe_size"]
    assert profile["metrics_skipped"] == [], profile["metrics_skipped"]
    assert profile["subtype_map"]["observed_by_table"] == {
        "JUROS_FIXO": ["2"],
        "JUROS_FLUTUANTE": ["3"],
        "RESGATE": ["1", "20"],
        "SPREAD": ["5"],
    }
    tables_without_condition = _selftest_tables(spark)
    del tables_without_condition[CONDICAO_IF_TABLE]
    incomplete = build_subtype_map_snapshot(
        tables_without_condition,
        spark.createDataFrame([(1001,)], f"{ROOT_KEY} long"),
    )
    assert CONDICAO_IF_TABLE in incomplete["missing_tables"]
    assert "JUROS_FLUTUANTE" not in incomplete["missing_tables"]
    assert "JUROS_FLUTUANTE" in incomplete["unobserved_tables"]

    ref = profile["reference_match"]
    assert ref["matching_ifs"] == 1 and ref["total_ifs"] == 3, ref
    assert shape_of(profile, 1001)["counts"] == REFERENCE_SHAPE

    s1002 = shape_of(profile, 1002)["counts"]
    assert s1002["CREDITO"] == 0 and s1002["CONDICAO_IF"] == 3, s1002
    assert s1002["JUROS_FIXO"] == 1 and s1002["JUROS_FLUTUANTE"] == 0, s1002
    assert s1002["EVENTO"] == 1, s1002
    assert s1002["EVENTO_TIPO83"] == 0 and s1002["EVENTO_TIPO85"] == 1, s1002
    assert s1002["SPREAD"] == 1 and s1002["ATUALIZACAO_POS"] == 0, s1002
    assert s1002["RESGATE"] == 1 and s1002["TITULO"] == 1, s1002

    s1003 = shape_of(profile, 1003)["counts"]
    assert all(v == 0 for v in s1003.values()), s1003

    assert len(profile["shapes"]) == 3
    assert all(s["n"] == 1 for s in profile["shapes"])
    assert profile["marginals"]["CONDICAO_IF"] == {"0": 1, "2": 1, "3": 1}

    seg = {k: v["n"] for k, v in profile["by_simplificado"].items()}
    assert seg == {"yes": 1, "no": 1, "no_carteira": 1}, seg

    xc = profile["evento_path_crosscheck"]
    assert xc["status"] == "mismatch" and xc["mismatched"] == 1, xc
    assert xc["sample"][0] == {"num_if_direct": 1002, "num_if_via_condicao": 1001}

    audit = profile["attribute_audits"]
    assert audit["RESGATE.COD_COND_RESGATE"] == {"SEM TABELA": 1, "COM TABELA": 1}, audit
    assert audit["TITULO.COD_TIPO_ESCALONAMENTO"] == {"<NULL>": 1, "EMISSAO": 1}, audit
    assert audit["CONDICAO_IF.COD_TIPO_CONDICAO_IF"] == {
        "20": 1, "3": 1, "2": 1, "1": 1, "5": 1
    }, audit

    # --apply-filtros-fonte: 1002's COM TABELA resgate row and EMISSAO titulo row
    # are dropped; 1001 (SEM TABELA, escalonamento NULL) is untouched. The
    # CARTEIRA_* QTD predicates hit missing columns and must be ignored with a note.
    filtered = build_profile(_selftest_tables(spark), sample_size=10, apply_filtros=True)
    assert filtered["filtros_fonte_applied"] is True
    assert filtered["universe_size"] == 3, filtered["universe_size"]
    assert shape_of(filtered, 1001)["counts"] == REFERENCE_SHAPE
    f1002 = shape_of(filtered, 1002)["counts"]
    assert f1002["RESGATE"] == 0 and f1002["TITULO"] == 0, f1002
    assert filtered["attribute_audits"]["RESGATE.COD_COND_RESGATE"] == {"SEM TABELA": 1}
    assert any("QTD_CARTEIRA_COMITENTE missing" in n for n in filtered["notes"]), (
        filtered["notes"]
    )

    assert audit["EVENTO.NUM_TIPO_EVENTO_LEGADO"] == {"83": 1, "85": 2}, audit

    # --universe-keys: restricting to 1001 keeps only the reference IF.
    keys = spark.createDataFrame([(1001,)], "NUM_IF long")
    restricted = build_profile(_selftest_tables(spark), sample_size=10, universe_keys=keys)
    assert restricted["universe_size"] == 1, restricted["universe_size"]
    assert restricted["reference_match"]["matching_ifs"] == 1
    assert any("universe restricted" in n for n in restricted["notes"])

    # --universe domain (FILTRO_BASE): only 1001 qualifies — 1002 has no
    # 'SEM TABELA' resgate (and an escalonado titulo), 1003 has no condição.
    domain = build_profile(_selftest_tables(spark), sample_size=10, universe_mode="domain")
    assert domain["universe_size"] == 1, domain["universe_size"]
    assert domain["reference_match"]["matching_ifs"] == 1
    assert any("universe=domain" in n for n in domain["notes"])

    print("SELF-TEST PASSED: profiler verified against the built-in fixture.")


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Profile per-IF cardinalities of the CDB domain.")
    p.add_argument("--base-uri", default=None,
                   help="Parquet base URI holding one folder per table (raw or synthetic).")
    p.add_argument("--self-test", action="store_true",
                   help="Verify the profiler against a built-in in-memory fixture and exit.")
    p.add_argument("--apply-filtros-fonte", action="store_true",
                   help="Pre-filter the source tables with engorda's FILTROS_FONTE row "
                        "predicates, so the profile is the engorda-input image (use as "
                        "the --compare-with baseline for the synthetic run).")
    p.add_argument("--universe-keys", default=None,
                   help="Parquet path/URI with the NUM_IFs to restrict the universe to "
                        "(e.g. a clone run's MAPA_CLONE_NUM_IF) — builds a baseline over "
                        "exactly the sampled source instruments.")
    p.add_argument("--universe-keys-column", default="NUM_IF",
                   help="Column holding the NUM_IF in --universe-keys "
                        "(e.g. NUM_IF_ORIG for MAPA_CLONE_NUM_IF).")
    p.add_argument("--universe", default="all", choices=["all", "domain"],
                   help="'all' = every active CDB (NUM_TIPO_IF=49). 'domain' = the "
                        "IF-level product domain: non-escalonado TITULO and >=1 active "
                        "CONDICAO_IF with an active RESGATE 'SEM TABELA' (team "
                        "FILTRO_BASE query). Composes with --universe-keys (intersection).")
    p.add_argument("--prefix", default="", help="Optional sub-prefix under the base URI.")
    p.add_argument("--label", default="dataset", help="Label for the report (e.g. raw/synthetic).")
    p.add_argument("--report-path", default=None, help="Write the JSON profile here.")
    p.add_argument("--compare-with", default=None,
                   help="Path to a JSON profile produced by an earlier run; adds a diff section.")
    p.add_argument("--sample-size", type=int, default=10, help="Sample NUM_IFs kept per shape.")
    p.add_argument("--top", type=int, default=20, help="Shapes printed to the console.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("profile_cdb_shapes").getOrCreate()
    # Spark 3.5.0 (OCI Data Flow) + AQE + cached DataFrames silently LOSES JOIN
    # ROWS (SPARK-45282, fixed in 3.5.1). Baselines built with AQE on may drop
    # rows from joins. Keep AQE off until the apps run >= 3.5.1.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.sparkContext.setLogLevel("WARN")

    if args.self_test:
        try:
            run_selftest(spark)
        finally:
            spark.stop()
        return
    if not args.base_uri:
        spark.stop()
        raise SystemExit("--base-uri is required (or use --self-test).")

    base = args.base_uri.rstrip("/")
    if args.prefix.strip("/"):
        base = f"{base}/{args.prefix.strip('/')}"

    needed = sorted(
        {ROOT_TABLE, CONDICAO_IF_TABLE, OPERACAO_TABLE}
        | {m.table for m in METRICS}
        | set(EXTRA_TABLES)
        | set(SUBTYPE_TABLES)
    )
    tables = read_tables(spark, base, needed)
    logger.info("Read %d/%d tables from %s", len(tables), len(needed), base)

    universe_keys = None
    if args.universe_keys:
        kdf = spark.read.parquet(args.universe_keys)
        kcol = _ci(kdf, args.universe_keys_column)
        if not kcol:
            raise SystemExit(
                f"--universe-keys {args.universe_keys} lacks column "
                f"{args.universe_keys_column}"
            )
        universe_keys = (
            kdf.select(F.col(kcol).cast("long").alias(ROOT_KEY)).dropDuplicates()
        )
        logger.info(
            "Universe restricted to %d NUM_IF(s) from %s",
            universe_keys.count(), args.universe_keys,
        )

    profile = build_profile(
        tables, args.sample_size, args.apply_filtros_fonte, universe_keys,
        args.universe,
    )
    profile["label"] = args.label
    profile["universe_mode"] = args.universe
    if args.universe_keys:
        profile["universe_keys_source"] = args.universe_keys
    profile["base_uri"] = base
    profile["generated_at"] = datetime.now().isoformat(timespec="seconds")

    if args.compare_with:
        other = json.loads(read_text(spark, args.compare_with))
        profile["comparison"] = compare_profiles(
            profile, other, other.get("label", "other")
        )

    print_report(profile, args.label, args.top)

    if args.report_path:
        write_text(
            spark,
            args.report_path,
            json.dumps(profile, ensure_ascii=False, indent=2, default=str),
        )
        logger.info("JSON profile written to %s", args.report_path)

    spark.stop()


if __name__ == "__main__":
    main()
