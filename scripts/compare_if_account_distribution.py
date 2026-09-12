"""Distributed IF-account comparisons; requires only PySpark and the standard library.

The notebook owns reads, caching and output actions. Source baselines describe the
provided export, NOT the exact SQL eligibility pool: full_export includes every
type/status, same_type includes every status, and active_same_type requires a null
DAT_EXCLUSAO. Synthetic rows are never date-filtered or reclassified. With a map,
selected_sources counts distinct original IFs and clone_weighted counts each copy
under its new IF identity, exposing selection bias separately from clone weighting.
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _identifier(name):
    value = F.regexp_replace(F.trim(F.col(name).cast("string")), r"\.0+$", "")
    return F.when(value != "", value)


def _require_columns(frame, columns, label):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _reject(frame, message):
    # Only a scalar aggregate crosses to the driver, never offending input rows.
    if frame.agg(F.count(F.lit(1)).alias("n")).first()["n"]:
        raise ValueError(message)


def _unique(frame, keys, label):
    invalid = reduce(lambda a, b: a | b, (F.col(k).isNull() for k in keys))
    _reject(
        frame.groupBy(*keys).count().where(invalid | (F.col("count") > 1)),
        f"{label} {'/'.join(keys)} must be non-null, valid and unique",
    )


def compare_if_accounts(
    source: DataFrame,
    synthetic: DataFrame,
    clone_map: DataFrame | None,
    if_type: int,
) -> dict:
    """Return distribution, summary and account_changes (DataFrames, or None).

    Validation runs Spark actions and raises ValueError for invalid input. Returned
    frames are lazy and uncached; callers must keep inputs stable across actions.
    IDs/accounts are trimmed strings without zero decimal suffixes; blank accounts
    share the null bucket. Root IDs must be nonblank and unique even outside the
    requested source type. Every synthetic NUM_TIPO_IF must equal if_type.

    A supplied map must cover synthetic IDs exactly and reference existing raw
    source IDs. K is returned as a canonical positive-integer string, with no
    machine-integer range restriction. account_changes contains only null-safe
    mismatches, not every mapped IF. None explicitly means the map is unavailable.
    Percentages use 0..100 units, remain unrounded and are null for zero totals;
    DELTA_PP and total variation are null if either population is empty.
    """
    if not isinstance(if_type, int) or isinstance(if_type, bool):
        raise ValueError("if_type must be an integer")
    account = "NUM_CONTA_PARTICIPANTE"
    required = ["NUM_IF", account, "NUM_TIPO_IF"]
    _require_columns(source, required + ["DAT_EXCLUSAO"], "source")
    _require_columns(synthetic, required, "synthetic")
    if clone_map is not None:
        _require_columns(clone_map, ["NUM_IF_ORIG", "K", "NUM_IF_NOVO"], "clone_map")
    source = source.select(*[_identifier(c).alias(c) for c in required], "DAT_EXCLUSAO")
    synthetic = synthetic.select(*[_identifier(c).alias(c) for c in required])
    _unique(source, ["NUM_IF"], "source")
    _unique(synthetic, ["NUM_IF"], "synthetic")
    _reject(
        synthetic.where(~F.col("NUM_TIPO_IF").eqNullSafe(F.lit(str(if_type)))),
        "synthetic NUM_TIPO_IF must match if_type on every row",
    )
    same_type = source.where(F.col("NUM_TIPO_IF") == str(if_type))
    cohorts = {
        "full_export": source,
        "same_type": same_type,
        "active_same_type": same_type.where(F.col("DAT_EXCLUSAO").isNull()),
    }
    account_changes = None
    changed_count = None
    if clone_map is not None:
        k = F.regexp_replace(_identifier("K"), r"^\+?0*", "")
        clone_map = clone_map.select(
            _identifier("NUM_IF_ORIG").alias("NUM_IF_ORIG"),
            F.when(k.rlike(r"^[1-9][0-9]*$"), k).alias("K"),
            _identifier("NUM_IF_NOVO").alias("NUM_IF_NOVO"),
        )
        _unique(clone_map, ["NUM_IF_NOVO"], "clone_map")
        _unique(clone_map, ["NUM_IF_ORIG", "K"], "clone_map")
        originals = source.select(F.col("NUM_IF").alias("NUM_IF_ORIG"), account)
        _reject(
            clone_map.join(originals, "NUM_IF_ORIG", "left_anti"),
            "clone_map contains missing source NUM_IF_ORIG parents",
        )
        coverage = clone_map.select("NUM_IF_NOVO").join(
            synthetic.select("NUM_IF"),
            F.col("NUM_IF_NOVO") == F.col("NUM_IF"),
            "full_outer",
        )
        _reject(
            coverage.where(F.col("NUM_IF_NOVO").isNull() | F.col("NUM_IF").isNull()),
            "clone_map NUM_IF_NOVO coverage must exactly match synthetic NUM_IF "
            "(missing/extra IDs)",
        )
        expected = clone_map.join(originals, "NUM_IF_ORIG").select(
            F.col("NUM_IF_NOVO").alias("NUM_IF"), "NUM_IF_ORIG", "K", account
        )
        cohorts["selected_sources"] = source.join(
            clone_map.select(F.col("NUM_IF_ORIG").alias("NUM_IF")).distinct(),
            "NUM_IF",
            "left_semi",
        )
        cohorts["clone_weighted"] = expected
        source_account = "SOURCE_NUM_CONTA_PARTICIPANTE"
        synthetic_account = "SYNTHETIC_NUM_CONTA_PARTICIPANTE"
        account_changes = (
            expected.withColumnRenamed(account, source_account)
            .join(synthetic.select("NUM_IF", F.col(account).alias(synthetic_account)), "NUM_IF")
            .where(~F.col(source_account).eqNullSafe(F.col(synthetic_account)))
            .select("NUM_IF", "NUM_IF_ORIG", "K", source_account, synthetic_account)
        )
        changed_count = account_changes.agg(F.count(F.lit(1)).alias("n")).first()["n"]

    baselines = source.sparkSession.createDataFrame(
        [(name,) for name in cohorts], "BASELINE string"
    )
    source_counts = reduce(
        DataFrame.unionByName,
        (
            frame.groupBy(account)
            .agg(F.count(F.lit(1)).alias("SOURCE_IF_COUNT"))
            .withColumn("BASELINE", F.lit(name))
            for name, frame in cohorts.items()
        ),
    ).alias("s")
    synthetic_counts = (
        synthetic.groupBy(account)
        .agg(F.count(F.lit(1)).alias("SYNTHETIC_IF_COUNT"))
        .crossJoin(baselines)
        .alias("y")
    )
    counts = source_counts.join(
        synthetic_counts,
        (F.col("s.BASELINE") == F.col("y.BASELINE"))
        & F.col(f"s.{account}").eqNullSafe(F.col(f"y.{account}")),
        "full_outer",
    ).select(
        F.coalesce("s.BASELINE", "y.BASELINE").alias("BASELINE"),
        F.coalesce(f"s.{account}", f"y.{account}").alias(account),
        F.coalesce("SOURCE_IF_COUNT", F.lit(0)).alias("SOURCE_IF_COUNT"),
        F.coalesce("SYNTHETIC_IF_COUNT", F.lit(0)).alias("SYNTHETIC_IF_COUNT"),
    )
    has_source = F.col("SOURCE_IF_COUNT") > 0
    has_synthetic = F.col("SYNTHETIC_IF_COUNT") > 0
    totals = baselines.join(
        counts.groupBy("BASELINE").agg(
            F.sum("SOURCE_IF_COUNT").alias("SOURCE_TOTAL"),
            F.sum("SYNTHETIC_IF_COUNT").alias("SYNTHETIC_TOTAL"),
            F.sum(has_source.cast("long")).alias("SOURCE_ACCOUNT_BUCKETS"),
            F.sum(has_synthetic.cast("long")).alias("SYNTHETIC_ACCOUNT_BUCKETS"),
            F.sum((has_source & ~has_synthetic).cast("long")).alias("SOURCE_ONLY_BUCKETS"),
            F.sum((has_synthetic & ~has_source).cast("long")).alias("SYNTHETIC_ONLY_BUCKETS"),
        ),
        "BASELINE",
        "left",
    ).fillna(0)
    distribution = (
        counts.join(totals.select("BASELINE", "SOURCE_TOTAL", "SYNTHETIC_TOTAL"), "BASELINE")
        .withColumn(
            "SOURCE_PCT",
            F.when(
                F.col("SOURCE_TOTAL") > 0, F.col("SOURCE_IF_COUNT") * 100.0 / F.col("SOURCE_TOTAL")
            ),
        )
        .withColumn(
            "SYNTHETIC_PCT",
            F.when(
                F.col("SYNTHETIC_TOTAL") > 0,
                F.col("SYNTHETIC_IF_COUNT") * 100.0 / F.col("SYNTHETIC_TOTAL"),
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
            "BASELINE",
            account,
            "SOURCE_IF_COUNT",
            "SYNTHETIC_IF_COUNT",
            "SOURCE_TOTAL",
            "SYNTHETIC_TOTAL",
            "SOURCE_PCT",
            "SYNTHETIC_PCT",
            "DELTA_PP",
            "PRESENCE",
        )
    )
    variation = distribution.groupBy("BASELINE").agg(
        (F.sum(F.abs(F.col("DELTA_PP"))) / 2.0).alias("TOTAL_VARIATION_PCT")
    )
    summary = (
        totals.join(variation, "BASELINE", "left")
        .withColumn("CHANGED_ACCOUNT_IFS", F.lit(changed_count).cast("long"))
        .select(
            "BASELINE",
            "SOURCE_TOTAL",
            "SYNTHETIC_TOTAL",
            "SOURCE_ACCOUNT_BUCKETS",
            "SYNTHETIC_ACCOUNT_BUCKETS",
            "SOURCE_ONLY_BUCKETS",
            "SYNTHETIC_ONLY_BUCKETS",
            "TOTAL_VARIATION_PCT",
            "CHANGED_ACCOUNT_IFS",
        )
    )
    return {"distribution": distribution, "summary": summary, "account_changes": account_changes}
