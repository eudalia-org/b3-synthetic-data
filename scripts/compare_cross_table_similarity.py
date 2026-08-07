#!/usr/bin/env python
"""Profile and compare same-physical-type columns across Parquet tables.

The job scans each selected table into reusable column profiles, excludes keys
declared by the datagen specs, and compares cross-table columns only. Jaccard
uses exact distributed set operations or a coordinated SHA-256 bottom-k
sketch. PSI uses type-global cuts: each eligible PSI column contributes the
same fixed set of approximate deciles, the pooled deciles are reduced to
global deciles, and duplicate cuts are collapsed. Thus columns, not rows, have
equal influence on bin placement.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import posixpath
import sys
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import unquote, urlsplit

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("cross_table_similarity")

AUDIT_COLUMNS = {
    "DAT_INCLUSAO",
    "DAT_ALTERACAO",
    "DAT_EXCLUSAO",
    "DAT_INCLUSAO_REGISTRO",
}


@dataclass(frozen=True)
class Config:
    exact_distinct_threshold: int = 100_000
    sketch_size: int = 2_048
    numeric_categorical_threshold: int = 100
    psi_bins: int = 10
    min_nonnull: int = 100
    min_distinct: int = 2
    top: int = 100
    jaccard_match_threshold: float = 0.8
    psi_match_threshold: float = 0.05
    psi_accuracy: int = 10_000
    psi_representatives: int = 100
    timezone: str = "UTC"


@dataclass(frozen=True)
class ColumnPlan:
    table: str
    column: str
    type_json: str
    type_name: str
    kind: str
    supported: bool
    candidate: bool
    included: bool
    exclusion_reason: Optional[str]


@dataclass
class Analysis:
    columns: DataFrame
    jaccard_values: DataFrame
    psi_bins: DataFrame
    pairs: DataFrame
    integrity: dict
    skips: list[dict]
    tables_absent_specs: list[str]


@dataclass(frozen=True)
class NormalizedPath:
    scheme: str
    authority: str
    segments: tuple[str, ...]
    text: str


def _dtype_json(data_type: T.DataType) -> str:
    """Spark's canonical physical type JSON, including decimal precision/scale."""
    return data_type.json()


def _type_kind(data_type: T.DataType) -> tuple[str, bool]:
    if isinstance(data_type, T.StringType):
        return "string", True
    if isinstance(data_type, T.BooleanType):
        return "boolean", True
    if isinstance(data_type, T.NumericType):
        return "numeric", True
    if isinstance(data_type, T.DateType):
        return "date", True
    timestamp_types = (T.TimestampType,)
    timestamp_ntz = getattr(T, "TimestampNTZType", None)
    if timestamp_ntz is not None:
        timestamp_types += (timestamp_ntz,)
    if isinstance(data_type, timestamp_types):
        return "timestamp", True
    return "unsupported", False


def _table_name(name: str) -> str:
    return str(name).split(".", 1)[-1]


def _fk_list(spec: dict) -> list[dict]:
    fks = spec.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = spec.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def excluded_key_columns(specs: dict, table: str) -> tuple[set[str], set[str]]:
    """Return case-insensitive PK and child-side FK component names."""
    lookup = {_table_name(name).upper(): cfg for name, cfg in specs.items()}
    spec = lookup.get(_table_name(table).upper())
    if not isinstance(spec, dict):
        return set(), set()
    pk = {str(c).upper() for c in (spec.get("pk_cols") or [])}
    fk = {
        str(c).upper()
        for item in _fk_list(spec)
        for c in (item.get("columns") or [])
    }
    return pk, fk


def plan_columns(
    tables: dict[str, DataFrame], specs: dict, extra_excludes: Iterable[str] = ()
) -> list[ColumnPlan]:
    extra = {name.upper() for name in extra_excludes}
    plans: list[ColumnPlan] = []
    for table, df in tables.items():
        pk, fk = excluded_key_columns(specs, table)
        for field in df.schema.fields:
            upper = field.name.upper()
            kind, supported = _type_kind(field.dataType)
            reason = None
            if upper in pk:
                reason = "primary_key"
            elif upper in fk:
                reason = "foreign_key"
            elif upper in AUDIT_COLUMNS:
                reason = "audit_column"
            elif upper in extra:
                reason = "extra_exclude"
            candidate = reason is None
            if candidate and not supported:
                reason = "unsupported_type"
            plans.append(
                ColumnPlan(
                    table=table,
                    column=field.name,
                    type_json=_dtype_json(field.dataType),
                    type_name=field.dataType.simpleString(),
                    kind=kind,
                    supported=supported,
                    candidate=candidate,
                    included=candidate and supported,
                    exclusion_reason=reason,
                )
            )
    return plans


def _normalized_projection(df: DataFrame, plan: ColumnPlan) -> DataFrame:
    value = F.col(plan.column)
    invalid = F.lit(False)
    psi_value = F.lit(None).cast("string")

    if plan.kind == "string":
        trimmed = F.trim(value)
        normalized = F.when(F.length(trimmed) == 0, None).otherwise(F.upper(trimmed))
        value_key = normalized
    elif plan.kind == "boolean":
        value_key = value.cast("string")
    elif plan.kind == "numeric":
        if isinstance(df.schema[plan.column].dataType, (T.FloatType, T.DoubleType)):
            invalid = value.isNotNull() & (F.isnan(value) | (F.abs(value) == F.lit(float("inf"))))
        value_key = F.when(value == 0, F.lit("0")).otherwise(value.cast("string"))
        psi_value = value_key
    elif plan.kind == "date":
        value_key = value.cast("string")
        psi_value = F.datediff(value, F.lit("1970-01-01")).cast("string")
    elif plan.kind == "timestamp":
        value_key = F.date_format(value.cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        psi_value = F.unix_micros(value.cast("timestamp")).cast("string")
    else:  # guarded by plan.included
        raise ValueError(f"Unsupported projection for {plan.table}.{plan.column}")

    normalized_null = value_key.isNull()
    valid = ~normalized_null & ~invalid
    return df.select(
        F.lit(plan.table).alias("table"),
        F.lit(plan.column).alias("column"),
        F.lit(plan.type_json).alias("type_json"),
        F.lit(plan.kind).alias("kind"),
        value_key.alias("value"),
        psi_value.alias("psi_value"),
        valid.alias("is_valid"),
        invalid.alias("is_invalid"),
    )


def _union_all(frames: list[DataFrame], empty: DataFrame) -> DataFrame:
    if not frames:
        return empty
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def _empty_values(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [],
        "table string, column string, type_json string, kind string, value string, "
        "psi_value string, is_valid boolean, is_invalid boolean",
    )


def _profile_columns(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    plans: list[ColumnPlan],
    values: DataFrame,
    config: Config,
) -> DataFrame:
    row_counts = {table: df.count() for table, df in tables.items()}
    plan_schema = T.StructType(
        [
            T.StructField("table", T.StringType(), False),
            T.StructField("column", T.StringType(), False),
            T.StructField("type_json", T.StringType(), False),
            T.StructField("type_name", T.StringType(), False),
            T.StructField("kind", T.StringType(), False),
            T.StructField("supported", T.BooleanType(), False),
            T.StructField("candidate", T.BooleanType(), False),
            T.StructField("included", T.BooleanType(), False),
            T.StructField("exclusion_reason", T.StringType(), True),
            T.StructField("row_count", T.LongType(), False),
        ]
    )
    metadata = spark.createDataFrame(
        [(*asdict(plan).values(), row_counts[plan.table]) for plan in plans], plan_schema
    )
    aggregates = values.groupBy("table", "column", "type_json").agg(
        F.sum(F.col("is_valid").cast("long")).alias("valid_count"),
        F.sum(F.col("is_invalid").cast("long")).alias("invalid_count"),
        F.countDistinct(F.when(F.col("is_valid"), F.col("value"))).alias("distinct_count"),
    )
    profile = metadata.join(aggregates, ["table", "column", "type_json"], "left")
    profile = profile.withColumn(
        "non_null_count", F.col("valid_count") + F.col("invalid_count")
    ).withColumn("null_count", F.col("row_count") - F.col("non_null_count"))
    profile = profile.withColumn(
        "null_rate",
        F.when(F.col("row_count") > 0, F.col("null_count") / F.col("row_count")),
    )
    route = (
        F.when(F.col("kind").isin("string", "boolean"), F.lit("jaccard"))
        .when(
            (F.col("kind") == "numeric")
            & (F.col("distinct_count") <= config.numeric_categorical_threshold),
            F.lit("jaccard"),
        )
        .when(F.col("kind").isin("numeric", "date", "timestamp"), F.lit("psi"))
    )
    profile = profile.withColumn("route", F.when(F.col("included"), route))
    profile = profile.withColumn(
        "eligible",
        F.col("included")
        & (F.col("valid_count") >= config.min_nonnull)
        & (F.col("distinct_count") >= config.min_distinct),
    )
    profile = profile.withColumn(
        "eligibility_reason",
        F.when(~F.col("included"), F.col("exclusion_reason"))
        .when(F.col("valid_count") < config.min_nonnull, F.lit("insufficient_nonnull"))
        .when(F.col("distinct_count") < config.min_distinct, F.lit("insufficient_distinct")),
    )
    return profile


def _sketch_profiles(pairs: DataFrame, config: Config) -> DataFrame:
    sketch_pairs = pairs.where(
        (F.col("status") == "ok")
        & (F.col("metric") == "jaccard")
        & (
            (F.col("distinct_count_a") > config.exact_distinct_threshold)
            | (F.col("distinct_count_b") > config.exact_distinct_threshold)
        )
    )
    a = sketch_pairs.select(
        F.col("table_a").alias("table"),
        F.col("column_a").alias("column"),
        "type_json",
    )
    b = sketch_pairs.select(
        F.col("table_b").alias("table"),
        F.col("column_b").alias("column"),
        "type_json",
    )
    return a.unionByName(b).dropDuplicates(["table", "column", "type_json"])


def _jaccard_values(
    values: DataFrame, columns: DataFrame, pairs: DataFrame, config: Config
) -> DataFrame:
    selected = columns.where(F.col("route") == "jaccard").select(
        "table", "column", "type_json"
    )
    distinct = (
        values.where(F.col("is_valid"))
        .select("table", "column", "type_json", "value")
        .join(selected, ["table", "column", "type_json"], "inner")
        .dropDuplicates(["table", "column", "type_json", "value"])
    )
    sketch_values = distinct.join(
        _sketch_profiles(pairs, config), ["table", "column", "type_json"], "inner"
    ).withColumn("sha256_rank", F.sha2(F.col("value"), 256))
    window = Window.partitionBy("table", "column", "type_json").orderBy(
        "sha256_rank", "value"
    )
    ranked = sketch_values.withColumn("sketch_rank", F.row_number().over(window))
    return distinct.join(
        ranked,
        ["table", "column", "type_json", "value"],
        "left",
    ).select("table", "column", "type_json", "value", "sha256_rank", "sketch_rank")


def _psi_native_types(
    tables: dict[str, DataFrame], plans: list[ColumnPlan]
) -> dict[str, T.DataType]:
    native: dict[str, T.DataType] = {}
    for plan in plans:
        if not plan.included or plan.kind not in {"numeric", "date", "timestamp"}:
            continue
        physical = tables[plan.table].schema[plan.column].dataType
        native[plan.type_json] = (
            T.LongType() if plan.kind in {"date", "timestamp"} else physical
        )
    return native


def _empty_edges(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [],
        "type_json string, cuts array<string>, native_type string, "
        "representative_count int, accuracy int, approximation string",
    )


def _psi_edges(
    values: DataFrame,
    columns: DataFrame,
    native_types: dict[str, T.DataType],
    config: Config,
) -> DataFrame:
    selected = columns.where((F.col("route") == "psi") & F.col("eligible")).select(
        "table", "column", "type_json"
    )
    frames: list[DataFrame] = []
    for type_json, native_type in sorted(native_types.items()):
        typed = (
            values.where(F.col("is_valid") & (F.col("type_json") == type_json))
            .select(
                "table",
                "column",
                "type_json",
                F.col("psi_value").cast(native_type).alias("native_value"),
            )
            .join(selected, ["table", "column", "type_json"], "inner")
        )
        partition = Window.partitionBy("table", "column", "type_json")
        ordered = partition.orderBy("native_value")
        ranked = typed.withColumn("native_rank", F.row_number().over(ordered)).withColumn(
            "native_count", F.count(F.lit(1)).over(partition)
        )
        groups = ranked.select(
            "table", "column", "type_json", "native_count"
        ).dropDuplicates(["table", "column", "type_json"])
        representative_indexes = values.sparkSession.range(config.psi_representatives).select(
            F.col("id").alias("representative_index")
        )
        targets = groups.crossJoin(representative_indexes).withColumn(
            "target_rank",
            F.greatest(
                F.lit(1),
                F.ceil(
                    (F.col("representative_index") + F.lit(0.5))
                    * F.col("native_count")
                    / F.lit(config.psi_representatives)
                ),
            ).cast("int"),
        )
        target_alias, ranked_alias = targets.alias("target"), ranked.alias("ranked")
        representatives = target_alias.join(
            ranked_alias,
            (F.col("target.table") == F.col("ranked.table"))
            & (F.col("target.column") == F.col("ranked.column"))
            & (F.col("target.type_json") == F.col("ranked.type_json"))
            & (F.col("target.target_rank") == F.col("ranked.native_rank")),
        ).select(
            F.col("ranked.type_json").alias("type_json"),
            F.col("ranked.native_value").alias("point"),
        )

        pooled_partition = Window.partitionBy("type_json")
        pooled_order = pooled_partition.orderBy("point")
        pooled = representatives.withColumn(
            "point_rank", F.row_number().over(pooled_order)
        ).withColumn("point_count", F.count(F.lit(1)).over(pooled_partition))
        pooled_size = pooled.select("type_json", "point_count").dropDuplicates(["type_json"])
        cut_indexes = values.sparkSession.range(1, config.psi_bins).select(
            F.col("id").alias("cut_index")
        )
        cut_targets = pooled_size.crossJoin(cut_indexes).withColumn(
            "target_rank",
            F.ceil(
                F.col("cut_index") * F.col("point_count") / F.lit(config.psi_bins)
            ).cast("int"),
        )
        cut_alias, pooled_alias = cut_targets.alias("target"), pooled.alias("pooled")
        native_cuts = (
            cut_alias.join(
                pooled_alias,
                (F.col("target.type_json") == F.col("pooled.type_json"))
                & (F.col("target.target_rank") == F.col("pooled.point_rank")),
            )
            .groupBy(F.col("target.type_json").alias("type_json"))
            .agg(F.array_sort(F.collect_set("pooled.point")).alias("native_cuts"))
        )
        frames.append(
            native_cuts.select(
                "type_json",
                F.transform("native_cuts", lambda value: value.cast("string")).alias("cuts"),
                F.lit(native_type.simpleString()).alias("native_type"),
                F.lit(config.psi_representatives).alias("representative_count"),
                F.lit(config.psi_accuracy).alias("accuracy"),
                F.lit("native_nearest_rank").alias("approximation"),
            )
        )
    return _union_all(frames, _empty_edges(values.sparkSession))


def _psi_bins(
    values: DataFrame,
    columns: DataFrame,
    edges: DataFrame,
    native_types: dict[str, T.DataType],
    config: Config,
) -> DataFrame:
    selected = columns.where((F.col("route") == "psi") & F.col("eligible")).select(
        "table", "column", "type_json", "valid_count"
    )
    frames: list[DataFrame] = []
    for type_json, native_type in sorted(native_types.items()):
        type_edges = edges.where(F.col("type_json") == type_json)
        typed = (
            values.where(F.col("is_valid") & (F.col("type_json") == type_json))
            .select(
                "table",
                "column",
                "type_json",
                F.col("psi_value").cast(native_type).alias("native_value"),
            )
            .join(selected, ["table", "column", "type_json"], "inner")
            .join(type_edges, "type_json", "inner")
            .withColumn(
                "native_cuts", F.transform("cuts", lambda value: value.cast(native_type))
            )
            .withColumn(
                "bin_index",
                F.aggregate(
                    "native_cuts",
                    F.lit(0),
                    lambda total, cut: total
                    + F.when(F.col("native_value") > cut, 1).otherwise(0),
                ),
            )
        )
        counts = typed.groupBy("table", "column", "type_json", "bin_index").count()
        grid = (
            selected.where(F.col("type_json") == type_json)
            .join(type_edges, "type_json", "inner")
            .withColumn("effective_bins", F.size("cuts") + 1)
            .withColumn("bin_index", F.explode(F.sequence(F.lit(0), F.size("cuts"))))
        )
        frames.append(
            grid.join(counts, ["table", "column", "type_json", "bin_index"], "left")
            .select(
                "table",
                "column",
                "type_json",
                "bin_index",
                F.when(
                    F.col("bin_index") > 0, F.element_at("cuts", F.col("bin_index"))
                ).alias("lower_edge"),
                F.when(
                    F.col("bin_index") < F.size("cuts"),
                    F.element_at("cuts", F.col("bin_index") + 1),
                ).alias("upper_edge"),
                (F.col("bin_index") == 0).alias("lower_infinite"),
                (F.col("bin_index") == F.size("cuts")).alias("upper_infinite"),
                F.coalesce(F.col("count"), F.lit(0)).cast("long").alias("count"),
                F.col("valid_count").cast("long").alias("total"),
                "effective_bins",
                F.lit(config.psi_bins).alias("bins_requested"),
                "representative_count",
                "accuracy",
                "approximation",
                F.lit(config.timezone).alias("timezone"),
                F.to_json("cuts").alias("edges_json"),
                "native_type",
            )
        )
    empty = values.sparkSession.createDataFrame(
        [],
        "table string, column string, type_json string, bin_index int, lower_edge string, "
        "upper_edge string, lower_infinite boolean, upper_infinite boolean, count long, "
        "total long, effective_bins int, bins_requested int, representative_count int, "
        "accuracy int, approximation string, timezone string, edges_json string, "
        "native_type string",
    )
    return _union_all(frames, empty)


def _candidate_pairs(columns: DataFrame, config: Config) -> DataFrame:
    a, b = columns.alias("a"), columns.alias("b")
    pairs = a.crossJoin(b).where(
        F.col("a.candidate")
        & F.col("b.candidate")
        & (F.col("a.table") < F.col("b.table"))
        & (F.col("a.type_json") == F.col("b.type_json"))
    )
    pairs = pairs.select(
        F.col("a.table").alias("table_a"),
        F.col("a.column").alias("column_a"),
        F.col("b.table").alias("table_b"),
        F.col("b.column").alias("column_b"),
        F.col("a.type_json").alias("type_json"),
        F.col("a.type_name").alias("type_name"),
        F.col("a.supported").alias("supported_a"),
        F.col("b.supported").alias("supported_b"),
        F.col("a.route").alias("route_a"),
        F.col("b.route").alias("route_b"),
        F.col("a.eligible").alias("eligible_a"),
        F.col("b.eligible").alias("eligible_b"),
        F.col("a.row_count").alias("row_count_a"),
        F.col("b.row_count").alias("row_count_b"),
        F.col("a.non_null_count").alias("non_null_count_a"),
        F.col("b.non_null_count").alias("non_null_count_b"),
        F.col("a.valid_count").alias("valid_count_a"),
        F.col("b.valid_count").alias("valid_count_b"),
        F.col("a.invalid_count").alias("invalid_count_a"),
        F.col("b.invalid_count").alias("invalid_count_b"),
        F.col("a.null_count").alias("null_count_a"),
        F.col("b.null_count").alias("null_count_b"),
        F.col("a.null_rate").alias("null_rate_a"),
        F.col("b.null_rate").alias("null_rate_b"),
        F.col("a.distinct_count").alias("distinct_count_a"),
        F.col("b.distinct_count").alias("distinct_count_b"),
        F.col("a.eligibility_reason").alias("eligibility_reason_a"),
        F.col("b.eligibility_reason").alias("eligibility_reason_b"),
    )
    identity = F.to_json(F.struct("table_a", "column_a", "table_b", "column_b", "type_json"))
    pairs = pairs.withColumn("pair_id", F.sha2(identity, 256))
    pairs = pairs.withColumn(
        "metric", F.when(F.col("route_a") == F.col("route_b"), F.col("route_a"))
    )
    pairs = pairs.withColumn(
        "status",
        F.when(~F.col("supported_a") | ~F.col("supported_b"), F.lit("unsupported"))
        .when(~F.col("eligible_a") | ~F.col("eligible_b"), F.lit("ineligible"))
        .when(F.col("route_a") != F.col("route_b"), F.lit("metric_mismatch"))
        .otherwise(F.lit("ok")),
    )
    return pairs.withColumn(
        "threshold",
        F.when(F.col("metric") == "jaccard", F.lit(config.jaccard_match_threshold)).when(
            F.col("metric") == "psi", F.lit(config.psi_match_threshold)
        ),
    )


def _inverted_exact_intersections(
    jvalues: DataFrame, columns: DataFrame, config: Config
) -> DataFrame:
    """Count shared values by value-first self-join, independent of pair rows."""
    exact_profiles = columns.where(
        F.col("eligible")
        & (F.col("route") == "jaccard")
        & (F.col("distinct_count") <= config.exact_distinct_threshold)
    ).select("table", "column", "type_json")
    exact_values = jvalues.select("table", "column", "type_json", "value").join(
        exact_profiles, ["table", "column", "type_json"], "inner"
    )
    a, b = exact_values.alias("a"), exact_values.alias("b")
    intersections = (
        a.join(
            b,
            (F.col("a.type_json") == F.col("b.type_json"))
            & (F.col("a.value") == F.col("b.value"))
            & (F.col("a.table") < F.col("b.table")),
        )
        .groupBy(
            F.col("a.table").alias("table_a"),
            F.col("a.column").alias("column_a"),
            F.col("b.table").alias("table_b"),
            F.col("b.column").alias("column_b"),
            F.col("a.type_json").alias("type_json"),
        )
        .agg(F.count(F.lit(1)).alias("intersection_count"))
    )
    identity = F.to_json(F.struct("table_a", "column_a", "table_b", "column_b", "type_json"))
    return intersections.withColumn("pair_id", F.sha2(identity, 256)).select(
        "pair_id", "intersection_count"
    )


def _exact_jaccard(
    pairs: DataFrame, jvalues: DataFrame, columns: DataFrame, config: Config
) -> DataFrame:
    exact = pairs.where(
        (F.col("status") == "ok")
        & (F.col("metric") == "jaccard")
        & (F.col("distinct_count_a") <= config.exact_distinct_threshold)
        & (F.col("distinct_count_b") <= config.exact_distinct_threshold)
    )
    intersections = _inverted_exact_intersections(jvalues, columns, config)
    return (
        exact.join(intersections, "pair_id", "left")
        .select(
            "pair_id",
            F.lit("exact").alias("mode"),
            (
                F.coalesce(F.col("intersection_count"), F.lit(0))
                / (
                    F.col("distinct_count_a")
                    + F.col("distinct_count_b")
                    - F.coalesce(F.col("intersection_count"), F.lit(0))
                )
            ).alias("jaccard"),
            F.lit(None).cast("long").alias("sketch_sample_size"),
            F.lit(None).cast("double").alias("jaccard_interval_low"),
            F.lit(None).cast("double").alias("jaccard_interval_high"),
        )
    )


def wilson_interval_columns(successes, total):
    """Wilson 95% score interval expressions for a binomial proportion."""
    z = 1.959963984540054
    proportion = successes / total
    denominator = 1 + z * z / total
    center = (proportion + z * z / (2 * total)) / denominator
    margin = (
        z
        * F.sqrt(proportion * (1 - proportion) / total + z * z / (4 * total * total))
        / denominator
    )
    return F.greatest(F.lit(0.0), center - margin), F.least(F.lit(1.0), center + margin)


def _sketch_jaccard(pairs: DataFrame, jvalues: DataFrame, config: Config) -> DataFrame:
    sketch = pairs.where(
        (F.col("status") == "ok")
        & (F.col("metric") == "jaccard")
        & (
            (F.col("distinct_count_a") > config.exact_distinct_threshold)
            | (F.col("distinct_count_b") > config.exact_distinct_threshold)
        )
    )
    kept = jvalues.where(F.col("sketch_rank") <= config.sketch_size)
    a = (
        sketch.join(
            kept,
            (F.col("table_a") == F.col("table"))
            & (F.col("column_a") == F.col("column"))
            & (sketch.type_json == kept.type_json),
        )
        .select("pair_id", "value", "sha256_rank", F.lit(1).alias("in_a"), F.lit(0).alias("in_b"))
    )
    b = (
        sketch.join(
            kept,
            (F.col("table_b") == F.col("table"))
            & (F.col("column_b") == F.col("column"))
            & (sketch.type_json == kept.type_json),
        )
        .select("pair_id", "value", "sha256_rank", F.lit(0).alias("in_a"), F.lit(1).alias("in_b"))
    )
    union = a.unionByName(b).groupBy("pair_id", "value", "sha256_rank").agg(
        F.max("in_a").alias("in_a"), F.max("in_b").alias("in_b")
    )
    window = Window.partitionBy("pair_id").orderBy("sha256_rank", "value")
    sample = union.withColumn("union_rank", F.row_number().over(window)).where(
        F.col("union_rank") <= config.sketch_size
    )
    estimates = sample.groupBy("pair_id").agg(
        F.count(F.lit(1)).alias("sample_size"),
        F.sum((F.col("in_a") * F.col("in_b")).cast("long")).alias("shared"),
    )
    low, high = wilson_interval_columns(F.col("shared"), F.col("sample_size"))
    return estimates.select(
        "pair_id",
        F.lit("bottom_k").alias("mode"),
        (F.col("shared") / F.col("sample_size")).alias("jaccard"),
        F.col("sample_size").cast("long").alias("sketch_sample_size"),
        low.alias("jaccard_interval_low"),
        high.alias("jaccard_interval_high"),
    )


def _psi_scores(pairs: DataFrame, bins: DataFrame) -> DataFrame:
    selected = pairs.where((F.col("status") == "ok") & (F.col("metric") == "psi"))
    a = bins.select(
        F.col("table").alias("pa_table"),
        F.col("column").alias("pa_column"),
        F.col("type_json").alias("pa_type"),
        "bin_index",
        F.col("count").alias("count_a"),
        "effective_bins",
        "edges_json",
        "accuracy",
        "representative_count",
        "approximation",
        "timezone",
    )
    b = bins.select(
        F.col("table").alias("pb_table"),
        F.col("column").alias("pb_column"),
        F.col("type_json").alias("pb_type"),
        F.col("bin_index").alias("pb_bin_index"),
        F.col("count").alias("count_b"),
    )
    joined = (
        selected.join(
            a,
            (F.col("table_a") == F.col("pa_table"))
            & (F.col("column_a") == F.col("pa_column"))
            & (selected.type_json == F.col("pa_type")),
        )
        .join(
            b,
            (F.col("table_b") == F.col("pb_table"))
            & (F.col("column_b") == F.col("pb_column"))
            & (selected.type_json == F.col("pb_type"))
            & (F.col("bin_index") == F.col("pb_bin_index")),
        )
    )
    p = (F.col("count_a") + F.lit(0.5)) / (
        F.col("valid_count_a") + F.lit(0.5) * F.col("effective_bins")
    )
    q = (F.col("count_b") + F.lit(0.5)) / (
        F.col("valid_count_b") + F.lit(0.5) * F.col("effective_bins")
    )
    return joined.groupBy("pair_id").agg(
        F.sum((p - q) * F.log(p / q)).alias("psi"),
        F.first("effective_bins").alias("effective_bins"),
        F.first("edges_json").alias("psi_edges_json"),
        F.first("accuracy").alias("psi_accuracy"),
        F.first("representative_count").alias("psi_representative_count"),
        F.first("approximation").alias("psi_approximation"),
        F.first("timezone").alias("timezone"),
    )


def _score_pairs(
    pairs: DataFrame,
    jvalues: DataFrame,
    bins: DataFrame,
    columns: DataFrame,
    config: Config,
) -> DataFrame:
    working = pairs
    exact = _exact_jaccard(working, jvalues, columns, config)
    sketch = _sketch_jaccard(working, jvalues, config)
    j_scores = exact.unionByName(sketch)
    psi = _psi_scores(working, bins)
    scored = working.join(j_scores, "pair_id", "left").join(psi, "pair_id", "left")
    scored = scored.withColumn(
        "is_match",
        F.when(
            (F.col("status") == "ok") & (F.col("metric") == "jaccard"),
            F.col("jaccard") >= F.col("threshold"),
        ).when(
            (F.col("status") == "ok") & (F.col("metric") == "psi"),
            F.col("psi") <= F.col("threshold"),
        ),
    )
    return scored.select(
        "pair_id",
        "table_a",
        "column_a",
        "table_b",
        "column_b",
        "type_json",
        "type_name",
        "row_count_a",
        "row_count_b",
        "non_null_count_a",
        "non_null_count_b",
        "valid_count_a",
        "valid_count_b",
        "invalid_count_a",
        "invalid_count_b",
        "null_count_a",
        "null_count_b",
        "null_rate_a",
        "null_rate_b",
        "distinct_count_a",
        "distinct_count_b",
        "eligible_a",
        "eligible_b",
        "eligibility_reason_a",
        "eligibility_reason_b",
        "route_a",
        "route_b",
        "metric",
        "mode",
        "status",
        "jaccard",
        "psi",
        "threshold",
        "is_match",
        "sketch_sample_size",
        "jaccard_interval_low",
        "jaccard_interval_high",
        "effective_bins",
        "psi_edges_json",
        "psi_accuracy",
        "psi_representative_count",
        "psi_approximation",
        "timezone",
    )


def _integrity_checks(
    columns: DataFrame, jvalues: DataFrame, bins: DataFrame, pairs: DataFrame
) -> dict:
    bad_profiles = columns.where(
        F.col("included")
        & (
            (
                F.col("row_count")
                != F.col("null_count") + F.col("valid_count") + F.col("invalid_count")
            )
            | (F.col("distinct_count") > F.col("valid_count"))
            | (F.col("null_rate") < 0)
            | (F.col("null_rate") > 1)
        )
    ).count()
    j_counts = jvalues.groupBy("table", "column", "type_json").count()
    bad_jaccard_sets = (
        columns.where(F.col("route") == "jaccard")
        .join(j_counts, ["table", "column", "type_json"], "left")
        .where(F.coalesce(F.col("count"), F.lit(0)) != F.col("distinct_count"))
        .count()
    )
    bin_totals = bins.groupBy("table", "column", "type_json").agg(F.sum("count").alias("n"))
    bad_psi_bins = (
        columns.where((F.col("route") == "psi") & F.col("eligible"))
        .join(bin_totals, ["table", "column", "type_json"], "left")
        .where(F.coalesce(F.col("n"), F.lit(-1)) != F.col("valid_count"))
        .count()
    )
    bad_pairs = pairs.where(
        (F.col("table_a") == F.col("table_b"))
        | ((F.col("status") == "ok") & F.col("metric").isNull())
        | (
            (F.col("metric") == "jaccard")
            & F.col("jaccard").isNotNull()
            & ~F.col("jaccard").between(0, 1)
        )
        | ((F.col("metric") == "psi") & F.col("psi").isNotNull() & (F.col("psi") < -1e-12))
    ).count()
    result = {
        "bad_column_profiles": bad_profiles,
        "bad_jaccard_value_sets": bad_jaccard_sets,
        "bad_psi_bin_totals": bad_psi_bins,
        "bad_pairs": bad_pairs,
    }
    if any(result.values()):
        raise RuntimeError(f"Similarity data-integrity checks failed: {result}")
    return result


def analyze_tables(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    specs: Optional[dict] = None,
    extra_excludes: Iterable[str] = (),
    config: Config = Config(),
) -> Analysis:
    """Build all output DataFrames without collecting sets or pair-level data."""
    plans = plan_columns(tables, specs or {}, extra_excludes)
    for df in tables.values():
        df.persist(StorageLevel.MEMORY_AND_DISK)
    projections = [
        _normalized_projection(tables[plan.table], plan) for plan in plans if plan.included
    ]
    values = _union_all(projections, _empty_values(spark)).localCheckpoint(eager=True)
    columns = _profile_columns(spark, tables, plans, values, config).localCheckpoint(eager=True)
    candidates = _candidate_pairs(columns, config).localCheckpoint(eager=True)
    jvalues = _jaccard_values(values, columns, candidates, config).localCheckpoint(eager=True)
    native_types = _psi_native_types(tables, plans)
    edges = _psi_edges(values, columns, native_types, config).localCheckpoint(eager=True)
    bins = _psi_bins(values, columns, edges, native_types, config).localCheckpoint(eager=True)
    pairs = _score_pairs(candidates, jvalues, bins, columns, config).localCheckpoint(eager=True)
    integrity = _integrity_checks(columns, jvalues, bins, pairs)
    skips = [
        {
            "table": plan.table,
            "column": plan.column,
            "type_json": plan.type_json,
            "exclusion_reason": plan.exclusion_reason,
        }
        for plan in plans
        if not plan.included
    ]
    spec_tables = {_table_name(name).upper() for name in (specs or {})}
    absent = sorted(table for table in tables if _table_name(table).upper() not in spec_tables)
    return Analysis(columns, jvalues, bins, pairs, integrity, skips, absent)


def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def normalize_path(path: str) -> NormalizedPath:
    """Canonicalize local, file://, and object-store paths for overlap checks."""
    if not path or not path.strip():
        raise ValueError("Path must not be empty")
    raw = path.strip()
    parsed = urlsplit(raw)
    if parsed.scheme and parsed.scheme.lower() != "file":
        scheme = parsed.scheme.lower()
        authority = parsed.netloc
        normalized = posixpath.normpath("/" + unquote(parsed.path).lstrip("/"))
        segments = tuple(part for part in normalized.split("/") if part)
        suffix = "/".join(segments)
        text = f"{scheme}://{authority}" + (f"/{suffix}" if suffix else "")
        return NormalizedPath(scheme, authority, segments, text)

    local_raw = unquote(parsed.path) if parsed.scheme else raw
    local = os.path.abspath(os.path.expanduser(local_raw))
    local = os.path.normpath(local)
    segments = tuple(part for part in local.split(os.sep) if part)
    text = "file://" + local
    return NormalizedPath("file", "", segments, text)


def _same_or_ancestor(ancestor: NormalizedPath, child: NormalizedPath) -> bool:
    return (
        ancestor.scheme == child.scheme
        and ancestor.authority == child.authority
        and len(ancestor.segments) <= len(child.segments)
        and child.segments[: len(ancestor.segments)] == ancestor.segments
    )


def validate_output_base(output_base: str, input_paths: Iterable[str]) -> NormalizedPath:
    output = normalize_path(output_base)
    if not output.segments:
        raise ValueError(f"Unsafe output root is forbidden: {output.text}")
    for raw_input in input_paths:
        source = normalize_path(raw_input)
        if _same_or_ancestor(output, source):
            raise ValueError(
                f"Unsafe output {output.text} is equal to or an ancestor of input {source.text}"
            )
    return output


def read_text(spark: SparkSession, path: str) -> str:
    hpath = _hadoop_path(spark, path)
    stream = hpath.getFileSystem(spark._jsc.hadoopConfiguration()).open(hpath)
    try:
        return spark._jvm.org.apache.commons.io.IOUtils.toString(
            stream, spark._jvm.java.nio.charset.StandardCharsets.UTF_8
        )
    finally:
        stream.close()


def write_text(spark: SparkSession, path: str, content: str) -> None:
    hpath = _hadoop_path(spark, path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(hpath, True)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def _delete_path(spark: SparkSession, path: str) -> None:
    hpath = _hadoop_path(spark, path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(hpath) and not fs.delete(hpath, True):
        raise RuntimeError(f"Could not delete work path {path}")


def _promote_staging_paths(fs, staging, final, backup, verify_final=None) -> None:
    """Promote a complete tree and restore the verified prior tree on failure."""
    if not fs.exists(staging):
        raise ValueError(f"Staging output is absent: {staging}")
    had_previous = bool(fs.exists(final))
    if had_previous:
        try:
            preserved = bool(fs.rename(final, backup)) and bool(fs.exists(backup))
        except Exception:  # noqa: BLE001 - filesystem connectors vary
            preserved = False
        if not preserved:
            raise RuntimeError(
                f"CRITICAL: prior output preservation could not be verified at {backup}"
            )

    promotion_error = None
    try:
        promoted = bool(fs.rename(staging, final)) and bool(fs.exists(final))
        if not promoted:
            raise ValueError("Staging rename was not verified")
        if verify_final is not None:
            verify_final()
    except Exception as exc:  # noqa: BLE001 - rollback must handle readback errors too
        promotion_error = exc

    if promotion_error is not None:
        try:
            if fs.exists(final) and not fs.delete(final, True):
                raise RuntimeError("partial promoted output could not be removed")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"CRITICAL: promotion failed and partial output remains; recover {backup}"
            ) from exc
        if not had_previous:
            raise ValueError(
                "Promotion failed; there was no prior output to restore"
            ) from promotion_error
        try:
            restored = bool(fs.rename(backup, final)) and bool(fs.exists(final))
        except Exception:  # noqa: BLE001
            restored = False
        if not restored:
            raise RuntimeError(
                f"CRITICAL: promotion and restore failed; manually recover {backup}"
            ) from promotion_error
        raise ValueError(
            "Promotion failed; prior output restored and verified"
        ) from promotion_error

    if had_previous:
        try:
            deleted = bool(fs.delete(backup, True)) and not bool(fs.exists(backup))
        except Exception:  # noqa: BLE001
            deleted = False
        if not deleted:
            logger.warning("New output is valid, but backup cleanup failed: %s", backup)


def _stage_and_publish(spark: SparkSession, final_path: str, prepare, validate) -> None:
    staging_path = f"{final_path.rstrip('/')}.__staging_{uuid.uuid4().hex}"
    backup_path = f"{final_path.rstrip('/')}.__previous_{uuid.uuid4().hex}"
    _delete_path(spark, staging_path)
    try:
        prepare(staging_path)
        validate(staging_path)
    except Exception:
        _delete_path(spark, staging_path)
        raise
    final = _hadoop_path(spark, final_path.rstrip("/"))
    staging = _hadoop_path(spark, staging_path)
    backup = _hadoop_path(spark, backup_path)
    fs = final.getFileSystem(spark._jsc.hadoopConfiguration())
    _promote_staging_paths(fs, staging, final, backup, lambda: validate(final_path.rstrip("/")))


def _output_frames(analysis: Analysis) -> dict[str, DataFrame]:
    return {
        "pairs": analysis.pairs,
        "profiles/columns": analysis.columns,
        "profiles/jaccard_values": analysis.jaccard_values,
        "profiles/psi_bins": analysis.psi_bins,
    }


def _schema_signature(schema: T.StructType) -> list[tuple[str, str]]:
    # Parquet readback makes fields nullable, so validate ordered names and physical types.
    return [(field.name, field.dataType.json()) for field in schema.fields]


def _validate_output_readback(
    spark: SparkSession,
    root: str,
    expected: dict[str, tuple[list[tuple[str, str]], int]],
    integrity: dict,
) -> None:
    for relative, (schema_json, expected_count) in expected.items():
        actual = spark.read.parquet(f"{root}/{relative}")
        if _schema_signature(actual.schema) != schema_json:
            raise RuntimeError(f"Readback schema mismatch at {root}/{relative}")
        actual_count = actual.count()
        if actual_count != expected_count:
            raise RuntimeError(
                f"Readback count mismatch at {root}/{relative}: "
                f"{actual_count} != {expected_count}"
            )
    report = json.loads(read_text(spark, f"{root}/report.json"))
    if report.get("summary", {}).get("integrity") != integrity or any(integrity.values()):
        raise RuntimeError(f"Readback integrity mismatch at {root}/report.json")


def _row_dict(row) -> dict:
    return row.asDict(recursive=True)


def build_report(
    analysis: Analysis,
    tables: list[str],
    base_uri: Optional[str],
    prefix: str,
    specs_uri: Optional[str],
    output_base: str,
    config: Config,
) -> dict:
    status_rows = analysis.pairs.groupBy("status").count().collect()
    profile_rows = analysis.columns.groupBy("included", "exclusion_reason").count().collect()
    identity_order = ["table_a", "column_a", "table_b", "column_b"]
    top_jaccard = [
        _row_dict(row)
        for row in analysis.pairs.where(
            (F.col("status") == "ok") & (F.col("metric") == "jaccard") & F.col("is_match")
        )
        .orderBy(F.desc("jaccard"), *identity_order)
        .limit(config.top)
        .collect()
    ]
    top_psi = [
        _row_dict(row)
        for row in analysis.pairs.where(
            (F.col("status") == "ok") & (F.col("metric") == "psi") & F.col("is_match")
        )
        .orderBy(F.asc("psi"), *identity_order)
        .limit(config.top)
        .collect()
    ]
    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "table_selection": {
            "base_uri": base_uri,
            "prefix": prefix,
            "tables": tables,
            "specs_uri": specs_uri,
        },
        "output_base": output_base,
        "config": asdict(config),
        "normalization": {
            "strings": "trim, uppercase, trimmed empty to NULL",
            "nulls": "excluded from scores; null_rate uses normalized NULLs",
            "invalid": "NaN and +/-Infinity excluded; typed Parquet has no unparseable scalars",
            "timezone": config.timezone,
        },
        "psi_cut_procedure": (
            f"For each eligible PSI column, native-type nearest-rank ordering computes "
            f"{config.psi_representatives} fixed midpoint-quantile representatives. Every "
            "column therefore contributes equally many points. Those points are pooled by "
            "exact DataType JSON in the native physical numeric type, and nearest-rank "
            "ordering computes the "
            f"{config.psi_bins - 1} type-global interior cuts; duplicate cuts collapse. Bins "
            "have conceptual outer infinities and use Jeffreys 0.5 smoothing. The persisted "
            f"approximation is native_nearest_rank with audit accuracy {config.psi_accuracy}. "
            "Dates use epoch days and timestamps use UTC "
            "epoch microseconds."
        ),
        "key_assumptions": {
            "tables_absent_from_specs": analysis.tables_absent_specs,
            "warning": (
                "Tables absent from specs are assumed to have no primary or foreign keys."
                if analysis.tables_absent_specs
                else None
            ),
        },
        "summary": {
            "pairs_by_status": {row["status"]: row["count"] for row in status_rows},
            "column_profiles": [row.asDict() for row in profile_rows],
            "integrity": analysis.integrity,
        },
        "skips": sorted(analysis.skips, key=lambda row: (row["table"], row["column"])),
        "top_qualifying_matches": {"jaccard": top_jaccard, "psi": top_psi},
    }


def write_outputs(
    spark: SparkSession,
    analysis: Analysis,
    output_base: str,
    report: dict,
) -> None:
    validate_output_base(output_base, [])
    frames = _output_frames(analysis)
    expected = {
        relative: (_schema_signature(frame.schema), frame.count())
        for relative, frame in frames.items()
    }

    def prepare(root):
        for relative, frame in frames.items():
            frame.write.mode("append").parquet(f"{root}/{relative}")
        write_text(spark, f"{root}/report.json", json.dumps(report, indent=2, default=str))

    def validate(root):
        _validate_output_readback(spark, root, expected, analysis.integrity)

    _stage_and_publish(spark, output_base.rstrip("/"), prepare, validate)


def execute_from_tables(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    output_base: str,
    specs: Optional[dict] = None,
    extra_excludes: Iterable[str] = (),
    config: Config = Config(),
    base_uri: Optional[str] = None,
    prefix: str = "",
    specs_uri: Optional[str] = None,
    input_paths: Iterable[str] = (),
) -> Analysis:
    validate_output_base(output_base, input_paths)
    analysis = analyze_tables(spark, tables, specs, extra_excludes, config)
    report = build_report(
        analysis, list(tables), base_uri, prefix, specs_uri, output_base, config
    )
    write_outputs(spark, analysis, output_base, report)
    return analysis


def _parse_repeated_csv(values: Optional[list[str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        for item in raw.split(","):
            value = item.strip()
            if value and value.upper() not in seen:
                seen.add(value.upper())
                out.append(value)
    return out


def _validate_config(config: Config) -> None:
    integer_fields = (
        "exact_distinct_threshold",
        "sketch_size",
        "numeric_categorical_threshold",
        "min_nonnull",
        "min_distinct",
        "top",
        "psi_accuracy",
        "psi_representatives",
    )
    for name in integer_fields:
        if getattr(config, name) < 1:
            raise SystemExit(f"--{name.replace('_', '-')} must be >= 1")
    if config.psi_bins < 2:
        raise SystemExit("--psi-bins must be >= 2")
    if not 0 <= config.jaccard_match_threshold <= 1:
        raise SystemExit("--jaccard-match-threshold must be in [0, 1]")
    if config.psi_match_threshold < 0:
        raise SystemExit("--psi-match-threshold must be >= 0")


def configure_spark(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")


def run_selftest(spark: SparkSession) -> None:
    config = Config(
        exact_distinct_threshold=3,
        sketch_size=3,
        numeric_categorical_threshold=3,
        psi_bins=4,
        min_nonnull=3,
        min_distinct=2,
        top=10,
        psi_accuracy=10_000,
    )
    tables = {
        "A": spark.createDataFrame(
            [(i, value, float(i)) for i, value in enumerate([" a ", "b", "c", "", None])],
            "id long, code string, measure double",
        ),
        "B": spark.createDataFrame(
            [(i, value, float(i)) for i, value in enumerate(["B", "c", "d", None, " "])],
            "id long, code string, measure double",
        ),
    }
    specs = {"A": {"pk_cols": ["id"]}, "B": {"pk_cols": ["id"]}}
    with tempfile.TemporaryDirectory(prefix="cross-table-selftest-") as directory:
        output = f"file://{directory}/result"
        analysis = execute_from_tables(
            spark, tables, output, specs=specs, config=config, base_uri="in-memory"
        )
        pairs = {
            (row["column_a"], row["column_b"]): row
            for row in analysis.pairs.collect()
        }
        code = pairs[("code", "code")]
        assert code["mode"] == "exact" and math.isclose(code["jaccard"], 0.5), code
        measure = pairs[("measure", "measure")]
        assert measure["metric"] == "psi" and math.isclose(measure["psi"], 0.0), measure
        assert spark.read.parquet(f"{output}/profiles/columns").count() == 6
        report = json.loads(read_text(spark, f"{output}/report.json"))
        assert report["summary"]["integrity"] == {
            "bad_column_profiles": 0,
            "bad_jaccard_value_sets": 0,
            "bad_psi_bin_totals": 0,
            "bad_pairs": 0,
        }
    print("SELF-TEST PASSED: cross-table similarity metrics and outputs verified.")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-uri", help="Parquet root containing one path per table")
    parser.add_argument("--prefix", default="", help="Optional sub-prefix below --base-uri")
    parser.add_argument(
        "--tables", action="append", help="Required table names; comma-separated and/or repeatable"
    )
    parser.add_argument("--specs-uri", default=os.environ.get("DATAGEN_SPECS_URI"))
    parser.add_argument("--output-base", help="Single output root, replaced exactly once")
    parser.add_argument("--extra-exclude", action="append", default=[])
    parser.add_argument("--exact-distinct-threshold", type=int, default=100_000)
    parser.add_argument("--sketch-size", type=int, default=2_048)
    parser.add_argument("--numeric-categorical-threshold", type=int, default=100)
    parser.add_argument("--psi-bins", type=int, default=10)
    parser.add_argument("--min-nonnull", type=int, default=100)
    parser.add_argument("--min-distinct", type=int, default=2)
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--jaccard-match-threshold", type=float, default=0.8)
    parser.add_argument("--psi-match-threshold", type=float, default=0.05)
    parser.add_argument("--psi-accuracy", type=int, default=10_000)
    parser.add_argument("--psi-representatives", type=int, default=100)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    config = Config(
        exact_distinct_threshold=args.exact_distinct_threshold,
        sketch_size=args.sketch_size,
        numeric_categorical_threshold=args.numeric_categorical_threshold,
        psi_bins=args.psi_bins,
        min_nonnull=args.min_nonnull,
        min_distinct=args.min_distinct,
        top=args.top,
        jaccard_match_threshold=args.jaccard_match_threshold,
        psi_match_threshold=args.psi_match_threshold,
        psi_accuracy=args.psi_accuracy,
        psi_representatives=args.psi_representatives,
    )
    _validate_config(config)
    tables = _parse_repeated_csv(args.tables)
    extra_excludes = _parse_repeated_csv(args.extra_exclude)
    if not args.self_test:
        if not args.base_uri or not args.output_base or len(tables) < 2:
            raise SystemExit(
                "--base-uri, --output-base, and at least two explicit --tables are required"
            )
        input_base = args.base_uri.rstrip("/")
        if args.prefix.strip("/"):
            input_base = f"{input_base}/{args.prefix.strip('/')}"
        input_paths = [f"{input_base}/{table}" for table in tables]
        try:
            validate_output_base(args.output_base, input_paths)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    else:
        input_base, input_paths = None, []

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = SparkSession.builder.appName("compare_cross_table_similarity").getOrCreate()
    configure_spark(spark)
    try:
        if args.self_test:
            run_selftest(spark)
            return
        base = input_base
        loaded: dict[str, DataFrame] = {}
        for table in tables:
            path = f"{base}/{table}"
            try:
                loaded[table] = spark.read.parquet(path)
            except Exception as exc:  # noqa: BLE001 - explicit paths must fail with context
                raise SystemExit(f"Failed to read explicit table {table} at {path}: {exc}") from exc
        specs = json.loads(read_text(spark, args.specs_uri)) if args.specs_uri else {}
        if not isinstance(specs, dict):
            raise SystemExit(f"Specs at {args.specs_uri} must be a JSON object")
        execute_from_tables(
            spark,
            loaded,
            args.output_base,
            specs,
            extra_excludes,
            config,
            args.base_uri,
            args.prefix,
            args.specs_uri,
            input_paths,
        )
        logger.info("Similarity output written to %s", args.output_base)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
