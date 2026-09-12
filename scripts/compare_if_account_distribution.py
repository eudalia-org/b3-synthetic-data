"""Distributed OPERACAO P1/P2 account distributions using PySpark only.

The historical filename is retained because the deployed OCI Data Flow application
references it. The public API compares operation accounts, not IF accounts.

Source baselines describe the provided export, NOT the exact SQL eligibility pool:
full_export includes all operations, including null/unmatched NUM_IF; same_type
semi-joins root-type-matched IF keys; active_same_type additionally requires null
IF DAT_EXCLUSAO. No operation status or TOS filters apply. Every synthetic operation
is retained in every baseline. Marginal distributions are not per-operation
mutation proof; no operation correspondence is inferred.

Usage (standalone OCI Data Flow application; only this file is required)::

    compare_if_account_distribution.py --source-base-uri oci://bucket@namespace/export \
        --synthetic-run-base-uri oci://bucket@namespace/runs/run-id \
        --product cdb_simplificado --baseline all --top-n 30

Reads source OPERACAO and INSTRUMENTO_FINANCEIRO metadata, and only
products/<product>/synthetic/OPERACAO from the run. Repeat --product or omit it for
all five products. Output is bounded stdout only. Data Flow supplies Spark
configuration and the OCI connector/authentication.
"""

import argparse
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _identifier(name):
    value = F.regexp_replace(F.trim(F.col(name).cast("string")), r"\.0+$", "")
    return F.when(value != "", value)


def _require_columns(frame, columns, label):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _unique(frame, key, label):
    # Only a scalar aggregate crosses to the driver, never offending input rows.
    invalid = frame.groupBy(key).count().where(F.col(key).isNull() | (F.col("count") > 1))
    if invalid.agg(F.count(F.lit(1)).alias("n")).first()["n"]:
        raise ValueError(f"{label} {key} must be non-null, valid and unique")


def _account_counts(frame, count_name):
    return (
        frame.select(
            F.explode(
                F.array(
                    *[
                        F.struct(
                            F.lit(role).alias("ROLE"),
                            F.col(f"NUM_CONTA_PARTICIPANTE_{role}").alias("NUM_CONTA_PARTICIPANTE"),
                        )
                        for role in ("P1", "P2")
                    ]
                )
            ).alias("side")
        )
        .select("side.*")
        .groupBy("ROLE", "NUM_CONTA_PARTICIPANTE")
        .agg(F.count(F.lit(1)).alias(count_name))
    )


def compare_operation_accounts(
    source: DataFrame, synthetic: DataFrame, source_if: DataFrame, if_type: int
) -> dict:
    """Return lazy, uncached distribution and summary DataFrames.

    Validation actions reject blank/duplicate operation IDs on either side and
    blank/duplicate source IF IDs (after normalization). Identifiers/accounts are
    trimmed strings without zero decimal suffixes, never converted through floats.
    Blank accounts share the null bucket. IF metadata uses only NUM_IF, NUM_TIPO_IF,
    DAT_EXCLUSAO, never its account. NUM_IF on operations may be null or unmatched.
    Each role's denominator is its cohort's operation count, not twice that count.
    Shares use 0..100 units; zero totals give null shares/deltas/total variation.
    Callers own reads, caching and actions and must keep inputs stable.
    """
    if not isinstance(if_type, int) or isinstance(if_type, bool):
        raise ValueError("if_type must be an integer")
    columns = [
        "NUM_ID_OPERACAO",
        "NUM_IF",
        "NUM_CONTA_PARTICIPANTE_P1",
        "NUM_CONTA_PARTICIPANTE_P2",
    ]
    metadata = ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
    _require_columns(source, columns, "source")
    _require_columns(synthetic, columns, "synthetic")
    _require_columns(source_if, metadata, "source_if")
    source = source.select(*[_identifier(c).alias(c) for c in columns])
    synthetic = synthetic.select(*[_identifier(c).alias(c) for c in columns])
    source_if = source_if.select(
        _identifier("NUM_IF").alias("NUM_IF"),
        _identifier("NUM_TIPO_IF").alias("NUM_TIPO_IF"),
        "DAT_EXCLUSAO",
    )
    _unique(source, "NUM_ID_OPERACAO", "source")
    _unique(synthetic, "NUM_ID_OPERACAO", "synthetic")
    _unique(source_if, "NUM_IF", "source_if")
    same_type = source_if.where(F.col("NUM_TIPO_IF") == str(if_type))
    cohorts = {
        "full_export": source,
        "same_type": source.join(same_type.select("NUM_IF"), "NUM_IF", "left_semi"),
        "active_same_type": source.join(
            same_type.where(F.col("DAT_EXCLUSAO").isNull()).select("NUM_IF"),
            "NUM_IF",
            "left_semi",
        ),
    }
    baselines = source.sparkSession.createDataFrame(
        [(name,) for name in cohorts], "BASELINE string"
    )
    roles = source.sparkSession.createDataFrame([("P1",), ("P2",)], "ROLE string")
    keys = ["BASELINE", "ROLE"]
    account = "NUM_CONTA_PARTICIPANTE"
    source_counts = reduce(
        DataFrame.unionByName,
        (
            _account_counts(frame, "SOURCE_OPERATION_COUNT").withColumn("BASELINE", F.lit(name))
            for name, frame in cohorts.items()
        ),
    ).alias("s")
    synthetic_counts = (
        _account_counts(synthetic, "SYNTHETIC_OPERATION_COUNT").crossJoin(baselines).alias("y")
    )
    counts = source_counts.join(
        synthetic_counts,
        (F.col("s.BASELINE") == F.col("y.BASELINE"))
        & (F.col("s.ROLE") == F.col("y.ROLE"))
        & F.col(f"s.{account}").eqNullSafe(F.col(f"y.{account}")),
        "full_outer",
    ).select(
        *[F.coalesce(f"s.{key}", f"y.{key}").alias(key) for key in keys],
        F.coalesce(f"s.{account}", f"y.{account}").alias(account),
        F.coalesce("SOURCE_OPERATION_COUNT", F.lit(0)).alias("SOURCE_OPERATION_COUNT"),
        F.coalesce("SYNTHETIC_OPERATION_COUNT", F.lit(0)).alias("SYNTHETIC_OPERATION_COUNT"),
    )
    has_source = F.col("SOURCE_OPERATION_COUNT") > 0
    has_synthetic = F.col("SYNTHETIC_OPERATION_COUNT") > 0
    totals = (
        baselines.crossJoin(roles)
        .join(
            counts.groupBy(*keys).agg(
                F.sum("SOURCE_OPERATION_COUNT").alias("SOURCE_TOTAL"),
                F.sum("SYNTHETIC_OPERATION_COUNT").alias("SYNTHETIC_TOTAL"),
                F.sum(has_source.cast("long")).alias("SOURCE_ACCOUNT_BUCKETS"),
                F.sum(has_synthetic.cast("long")).alias("SYNTHETIC_ACCOUNT_BUCKETS"),
                F.sum((has_source & ~has_synthetic).cast("long")).alias("SOURCE_ONLY_BUCKETS"),
                F.sum((has_synthetic & ~has_source).cast("long")).alias("SYNTHETIC_ONLY_BUCKETS"),
            ),
            keys,
            "left",
        )
        .fillna(0)
    )
    distribution = (
        counts.join(totals.select(*keys, "SOURCE_TOTAL", "SYNTHETIC_TOTAL"), keys)
        .withColumn(
            "SOURCE_PCT",
            F.when(
                F.col("SOURCE_TOTAL") > 0,
                F.col("SOURCE_OPERATION_COUNT") * 100.0 / F.col("SOURCE_TOTAL"),
            ),
        )
        .withColumn(
            "SYNTHETIC_PCT",
            F.when(
                F.col("SYNTHETIC_TOTAL") > 0,
                F.col("SYNTHETIC_OPERATION_COUNT") * 100.0 / F.col("SYNTHETIC_TOTAL"),
            ),
        )
        .withColumn("DELTA_PP", F.col("SYNTHETIC_PCT") - F.col("SOURCE_PCT"))
        .withColumn(
            "PRESENCE",
            F.when(~has_source, "synthetic_only")
            .when(~has_synthetic, "source_only")
            .otherwise("both"),
        )
        .select(
            *keys,
            account,
            "SOURCE_OPERATION_COUNT",
            "SYNTHETIC_OPERATION_COUNT",
            "SOURCE_TOTAL",
            "SYNTHETIC_TOTAL",
            "SOURCE_PCT",
            "SYNTHETIC_PCT",
            "DELTA_PP",
            "PRESENCE",
        )
    )
    variation = distribution.groupBy(*keys).agg(
        (F.sum(F.abs(F.col("DELTA_PP"))) / 2.0).alias("TOTAL_VARIATION_PCT")
    )
    summary = totals.join(variation, keys, "left").select(
        *keys,
        "SOURCE_TOTAL",
        "SYNTHETIC_TOTAL",
        "SOURCE_ACCOUNT_BUCKETS",
        "SYNTHETIC_ACCOUNT_BUCKETS",
        "SOURCE_ONLY_BUCKETS",
        "SYNTHETIC_ONLY_BUCKETS",
        "TOTAL_VARIATION_PCT",
    )
    return {"distribution": distribution, "summary": summary}


def main(argv=None):
    product_types = {
        "cdb_simplificado": 49,
        "cdb_resgate": 49,
        "cdb_escalonamento": 49,
        "rdb_resgate": 50,
        "rdb_inclusao": 50,
    }
    baselines = ["full_export", "same_type", "active_same_type"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-base-uri", required=True)
    parser.add_argument("--synthetic-run-base-uri", required=True)
    parser.add_argument("--product", action="append", choices=product_types)
    parser.add_argument("--baseline", choices=baselines + ["all"], default="full_export")
    parser.add_argument("--top-n", type=int, default=30)
    args = parser.parse_args(argv)
    for name in ("source_base_uri", "synthetic_run_base_uri"):
        value = getattr(args, name).strip()
        if not value:
            parser.error(f"--{name.replace('_', '-')} must be nonempty")
        setattr(args, name, value)
    products = args.product or list(product_types)
    if len(products) != len(set(products)):
        parser.error("duplicate --product values are not allowed")
    if not 1 <= args.top_n <= 1000:
        parser.error("--top-n must be an integer in 1..1000")
    chosen = baselines if args.baseline == "all" else [args.baseline]
    source_path = args.source_base_uri.rstrip("/") + "/OPERACAO"
    metadata_path = args.source_base_uri.rstrip("/") + "/INSTRUMENTO_FINANCEIRO"
    print("Operation account distribution", flush=True)
    print(
        f"Source: {source_path}\nIF metadata: {metadata_path}\nRun: {args.synthetic_run_base_uri}",
        flush=True,
    )
    print(
        "Caveat: full_export includes all source operations, including null/unmatched IF. "
        "Other cohorts are root-type-matched, not the exact SQL eligibility pool; "
        "no operation status/TOS filters apply. Marginal distributions are not "
        "per-operation mutation proof. Differences are analytics, not job failures.",
        flush=True,
    )
    spark = SparkSession.builder.appName("compare-operation-account-distribution").getOrCreate()
    source_caches = []
    context = f"products={','.join(products)} source={source_path} metadata={metadata_path}"
    try:
        columns = [
            "NUM_ID_OPERACAO",
            "NUM_IF",
            "NUM_CONTA_PARTICIPANTE_P1",
            "NUM_CONTA_PARTICIPANTE_P2",
        ]
        source = spark.read.parquet(source_path).select(*columns)
        source_caches.append(source)
        source.persist(StorageLevel.MEMORY_AND_DISK)
        source_if = spark.read.parquet(metadata_path).select(
            "NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"
        )
        source_caches.append(source_if)
        source_if.persist(StorageLevel.MEMORY_AND_DISK)
        for product in products:
            base = f"{args.synthetic_run_base_uri.rstrip('/')}/products/{product}/synthetic"
            synthetic_path = base + "/OPERACAO"
            context = (
                f"product={product} source={source_path} metadata={metadata_path} "
                f"synthetic={synthetic_path}"
            )
            print(
                f"Product: {product} (source NUM_TIPO_IF={product_types[product]})\n"
                f"Synthetic: {synthetic_path}",
                flush=True,
            )
            caches = []
            try:
                synthetic = spark.read.parquet(synthetic_path).select(*columns)
                caches.append(synthetic)
                synthetic.persist(StorageLevel.MEMORY_AND_DISK)
                result = compare_operation_accounts(
                    source, synthetic, source_if, product_types[product]
                )
                distribution = result["distribution"]
                caches.append(distribution)
                distribution.persist(StorageLevel.MEMORY_AND_DISK)
                for role in ("P1", "P2"):
                    print(f"Role: {role} (OPERACAO.NUM_CONTA_PARTICIPANTE_{role})", flush=True)
                    print("Summary: all three baselines", flush=True)
                    result["summary"].where(F.col("ROLE") == role).withColumn(
                        "TOTAL_VARIATION_PCT", F.round("TOTAL_VARIATION_PCT", 6)
                    ).orderBy("BASELINE").show(n=3, truncate=100)
                    for baseline in chosen:
                        print(
                            f"Baseline: {baseline} (top {args.top_n} by absolute DELTA_PP)",
                            flush=True,
                        )
                        preview = distribution.where(
                            (F.col("BASELINE") == baseline) & (F.col("ROLE") == role)
                        ).orderBy(
                            F.abs(F.col("DELTA_PP")).desc_nulls_last(), "NUM_CONTA_PARTICIPANTE"
                        )
                        preview.select(
                            *[
                                F.round(c, 6).alias(c)
                                if c in {"SOURCE_PCT", "SYNTHETIC_PCT", "DELTA_PP"}
                                else F.col(c)
                                for c in preview.columns
                            ]
                        ).show(n=args.top_n, truncate=100)
            finally:
                for frame in reversed(caches):
                    frame.unpersist(blocking=True)
    except Exception as exc:
        raise RuntimeError(f"Operation account comparison failed ({context}): {exc}") from exc
    finally:
        try:
            for frame in reversed(source_caches):
                frame.unpersist(blocking=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
