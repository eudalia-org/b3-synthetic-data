#!/usr/bin/env python
"""Profile and compare same-physical-type columns across Parquet tables.

The job scans each selected table into reusable column profiles, excludes keys
declared by the datagen specs, and compares cross-table columns only. Jaccard
uses exact distributed set operations or a coordinated SHA-256 bottom-k
sketch. PSI uses type-global cuts: each eligible PSI column contributes the
same fixed set of approximate deciles, the pooled deciles are reduced to
global deciles, and duplicate cuts are collapsed. Thus columns, not rows, have
equal influence on bin placement.

Publication is immutable: each successful run is written under
``<output-base>/runs/<run-id>`` and ``LATEST.json`` is updated only after
readback validation. Prior runs are never renamed or deleted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import posixpath
import shutil
import sys
import tempfile
import uuid
import weakref
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import unquote, urlsplit

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
    psi_sample_rows: int = 100_000
    sample_seed: int = 42
    column_batch_size: int = 8
    exact_pair_batch_size: int = 100
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
    psi_sampling: list[dict]


@dataclass(frozen=True)
class Publication:
    run_id: str
    run_uri: str
    report_uri: str
    generated_at: str


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


def _normalized_expressions(df: DataFrame, plan: ColumnPlan):
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
    return value_key, psi_value, valid, invalid


def _normalized_projection(df: DataFrame, plan: ColumnPlan) -> DataFrame:
    value_key, psi_value, valid, invalid = _normalized_expressions(df, plan)
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
        F.coalesce(F.sum(F.col("is_valid").cast("long")), F.lit(0)).alias("valid_count"),
        F.coalesce(F.sum(F.col("is_invalid").cast("long")), F.lit(0)).alias(
            "invalid_count"
        ),
        F.countDistinct(F.when(F.col("is_valid"), F.col("value"))).alias("distinct_count"),
    )
    profile = metadata.join(aggregates, ["table", "column", "type_json"], "left")
    for count_name in ("valid_count", "invalid_count", "distinct_count"):
        profile = profile.withColumn(
            count_name,
            F.when(
                F.col("included"), F.coalesce(F.col(count_name), F.lit(0).cast("long"))
            ).otherwise(F.col(count_name)),
        )
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
    return _assemble_scores(working, j_scores, psi)


def _assemble_scores(pairs: DataFrame, j_scores: DataFrame, psi: DataFrame) -> DataFrame:
    scored = pairs.join(j_scores, "pair_id", "left").join(psi, "pair_id", "left")
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
    """Independent small-data reference API; main never calls this all-column path."""
    directory = tempfile.mkdtemp(prefix="cross-table-analysis-")
    workspace = f"file://{directory}"
    plans = plan_columns(tables, specs or {}, extra_excludes)
    projections = [
        _normalized_projection(tables[plan.table], plan) for plan in plans if plan.included
    ]
    _union_all(projections, _empty_values(spark)).write.mode("append").parquet(
        f"{workspace}/values"
    )
    values = spark.read.parquet(f"{workspace}/values")
    columns = _profile_columns(spark, tables, plans, values, config)
    columns = (
        columns.withColumn(
            "psi_sample_rows",
            F.when(F.col("route") == "psi", F.lit(config.psi_sample_rows).cast("long")),
        )
        .withColumn(
            "psi_sample_fraction",
            F.when(F.col("route") == "psi", F.lit(1.0)),
        )
        .withColumn(
            "psi_sample_seed",
            F.when(F.col("route") == "psi", F.lit(config.sample_seed).cast("long")),
        )
        .withColumn(
            "psi_sample_actual_count",
            F.when(F.col("route") == "psi", F.col("row_count")),
        )
    )
    columns.write.mode("append").parquet(f"{workspace}/profiles/columns")
    columns = spark.read.parquet(f"{workspace}/profiles/columns")
    candidates = _candidate_pairs(columns, config)
    candidates.write.mode("append").parquet(f"{workspace}/candidates")
    candidates = spark.read.parquet(f"{workspace}/candidates")
    jvalues = _jaccard_values(values, columns, candidates, config)
    jvalues.write.mode("append").parquet(f"{workspace}/profiles/jaccard_values")
    jvalues = spark.read.parquet(f"{workspace}/profiles/jaccard_values")
    native_types = _psi_native_types(tables, plans)
    edges = _psi_edges(values, columns, native_types, config)
    edges.write.mode("append").parquet(f"{workspace}/profiles/psi_edges")
    edges = spark.read.parquet(f"{workspace}/profiles/psi_edges")
    bins = _psi_bins(values, columns, edges, native_types, config)
    bins.write.mode("append").parquet(f"{workspace}/profiles/psi_bins")
    bins = spark.read.parquet(f"{workspace}/profiles/psi_bins")
    pairs = _score_pairs(candidates, jvalues, bins, columns, config)
    pairs.write.mode("append").parquet(f"{workspace}/pairs")
    pairs = spark.read.parquet(f"{workspace}/pairs")
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
    row_counts = {table: df.count() for table, df in tables.items()}
    psi_tables = {
        row["table"]
        for row in columns.where((F.col("route") == "psi") & F.col("eligible"))
        .select("table")
        .distinct()
        .collect()
    }
    sampling = [
        {
            "table": table,
            "procedure": "full_table_small_reference",
            "requested_max_rows": config.psi_sample_rows,
            "source_row_count": row_counts[table],
            "requested_fraction": 1.0,
            "actual_fraction": 1.0,
            "seed": config.sample_seed,
            "actual_sample_count": row_counts[table],
        }
        for table in sorted(psi_tables)
    ]
    analysis = Analysis(columns, jvalues, bins, pairs, integrity, skips, absent, sampling)
    weakref.finalize(analysis, shutil.rmtree, directory, True)
    return analysis


# ---------------------------------------------------------------------------
# Scalable production pipeline. Source rows are never unioned across columns;
# every source expansion is bounded by Config.column_batch_size.
# ---------------------------------------------------------------------------
def _perf_log(phase: str, **fields) -> None:
    details = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
    logger.info("PERF phase=%s %s", phase, details)


def _batches(items: list, size: int):
    for start in range(0, len(items), size):
        yield start // size, items[start : start + size]


def _column_profile_schema() -> T.StructType:
    return T.StructType(
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
            T.StructField("valid_count", T.LongType(), True),
            T.StructField("invalid_count", T.LongType(), True),
            T.StructField("distinct_count", T.LongType(), True),
            T.StructField("non_null_count", T.LongType(), True),
            T.StructField("null_count", T.LongType(), True),
            T.StructField("null_rate", T.DoubleType(), True),
            T.StructField("route", T.StringType(), True),
            T.StructField("eligible", T.BooleanType(), False),
            T.StructField("eligibility_reason", T.StringType(), True),
            T.StructField("psi_sample_rows", T.LongType(), True),
            T.StructField("psi_sample_fraction", T.DoubleType(), True),
            T.StructField("psi_sample_seed", T.LongType(), True),
            T.StructField("psi_sample_actual_count", T.LongType(), True),
        ]
    )


def _profile_record(
    plan: ColumnPlan,
    row_count: int,
    valid_count: Optional[int],
    invalid_count: Optional[int],
    distinct_count: Optional[int],
    config: Config,
) -> dict:
    non_null = None if valid_count is None else valid_count + (invalid_count or 0)
    null_count = None if non_null is None else row_count - non_null
    null_rate = None
    if null_count is not None and row_count:
        null_rate = null_count / row_count
    route = None
    if plan.included:
        if plan.kind in {"string", "boolean"}:
            route = "jaccard"
        elif plan.kind == "numeric" and distinct_count <= config.numeric_categorical_threshold:
            route = "jaccard"
        else:
            route = "psi"
    eligible = bool(
        plan.included
        and valid_count is not None
        and valid_count >= config.min_nonnull
        and distinct_count is not None
        and distinct_count >= config.min_distinct
    )
    if not plan.included:
        eligibility_reason = plan.exclusion_reason
    elif valid_count < config.min_nonnull:
        eligibility_reason = "insufficient_nonnull"
    elif distinct_count < config.min_distinct:
        eligibility_reason = "insufficient_distinct"
    else:
        eligibility_reason = None
    return {
        **asdict(plan),
        "row_count": row_count,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "distinct_count": distinct_count,
        "non_null_count": non_null,
        "null_count": null_count,
        "null_rate": null_rate,
        "route": route,
        "eligible": eligible,
        "eligibility_reason": eligibility_reason,
        "psi_sample_rows": None,
        "psi_sample_fraction": None,
        "psi_sample_seed": None,
        "psi_sample_actual_count": None,
    }


def _append_rows(
    spark: SparkSession, rows: list[dict], schema: T.StructType, path: str
) -> None:
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").parquet(path)


def _production_profiles(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    plans: list[ColumnPlan],
    workspace: str,
    config: Config,
) -> DataFrame:
    fragment_path = f"{workspace}/fragments/column_profiles"
    by_table = {table: [plan for plan in plans if plan.table == table] for table in tables}
    for table, df in tables.items():
        table_plans = by_table[table]
        included = [plan for plan in table_plans if plan.included]
        started = datetime.now(timezone.utc)
        row_count = df.count()
        excluded_rows = [
            _profile_record(plan, row_count, None, None, None, config)
            for plan in table_plans
            if not plan.included
        ]
        _append_rows(spark, excluded_rows, _column_profile_schema(), fragment_path)
        for batch_id, batch in _batches(included, config.column_batch_size):
            expressions = []
            for index, plan in enumerate(batch):
                value, _psi, valid, invalid = _normalized_expressions(df, plan)
                expressions.extend(
                    [
                        F.sum(F.when(valid, 1).otherwise(0)).cast("long").alias(f"v{index}"),
                        F.sum(F.when(invalid, 1).otherwise(0)).cast("long").alias(f"i{index}"),
                        F.countDistinct(F.when(valid, value)).cast("long").alias(f"d{index}"),
                    ]
                )
            result = df.agg(*expressions).first()
            records = [
                _profile_record(
                    plan,
                    row_count,
                    int(result[f"v{index}"] or 0),
                    int(result[f"i{index}"] or 0),
                    int(result[f"d{index}"] or 0),
                    config,
                )
                for index, plan in enumerate(batch)
            ]
            _append_rows(spark, records, _column_profile_schema(), fragment_path)
            _perf_log(
                "profile_batch",
                table=table,
                batch=batch_id,
                columns=len(batch),
                rows=row_count,
            )
        _perf_log(
            "profile_table",
            table=table,
            columns=len(table_plans),
            rows=row_count,
            seconds=(datetime.now(timezone.utc) - started).total_seconds(),
        )
    fragments = spark.read.parquet(fragment_path)
    final_path = f"{workspace}/profiles/columns"
    fragments.write.mode("append").parquet(final_path)
    return spark.read.parquet(final_path)


def _production_psi_samples(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    plans: list[ColumnPlan],
    profiles: DataFrame,
    workspace: str,
    config: Config,
) -> tuple[DataFrame, dict[str, DataFrame], list[dict]]:
    profile_rows = {
        (row["table"], row["column"]): row for row in profiles.collect()
    }
    samples: dict[str, DataFrame] = {}
    records: list[dict] = []
    for table, df in tables.items():
        psi_plans = [
            plan
            for plan in plans
            if plan.table == table
            and profile_rows[(plan.table, plan.column)]["route"] == "psi"
            and profile_rows[(plan.table, plan.column)]["eligible"]
        ]
        if not psi_plans:
            continue
        row_count = int(profile_rows[(table, plans_for_table(plans, table)[0].column)]["row_count"])
        fraction = min(1.0, config.psi_sample_rows / row_count) if row_count else 1.0
        projection = df.select(*[F.col(plan.column) for plan in psi_plans])
        sampled = (
            projection
            if fraction >= 1.0
            else projection.sample(False, fraction, config.sample_seed).limit(
                config.psi_sample_rows
            )
        )
        token = hashlib.sha256(table.encode("utf-8")).hexdigest()[:16]
        path = f"{workspace}/psi_samples/{token}"
        sampled.write.mode("append").parquet(path)
        durable = spark.read.parquet(path)
        actual = durable.count()
        actual_fraction = actual / row_count if row_count else 1.0
        samples[table] = durable
        record = {
            "table": table,
            "procedure": "deterministic_bernoulli_without_replacement_then_limit",
            "requested_max_rows": config.psi_sample_rows,
            "source_row_count": row_count,
            "requested_fraction": fraction,
            "actual_fraction": actual_fraction,
            "seed": config.sample_seed,
            "actual_sample_count": actual,
        }
        records.append(record)
        _perf_log(
            "psi_sample_table",
            table=table,
            source_rows=row_count,
            requested_fraction=fraction,
            actual_fraction=actual_fraction,
            seed=config.sample_seed,
            sample_rows=actual,
            columns=len(psi_plans),
        )

    metadata_schema = (
        "table string, psi_sample_rows long, psi_sample_fraction double, "
        "psi_sample_seed long, psi_sample_actual_count long"
    )
    metadata = spark.createDataFrame(
        [
            (
                record["table"],
                record["requested_max_rows"],
                record["actual_fraction"],
                record["seed"],
                record["actual_sample_count"],
            )
            for record in records
        ],
        metadata_schema,
    ) if records else spark.createDataFrame([], metadata_schema)
    base = profiles.drop(
        "psi_sample_rows",
        "psi_sample_fraction",
        "psi_sample_seed",
        "psi_sample_actual_count",
    ).join(metadata, "table", "left")
    for name, data_type in (
        ("psi_sample_rows", "long"),
        ("psi_sample_fraction", "double"),
        ("psi_sample_seed", "long"),
        ("psi_sample_actual_count", "long"),
    ):
        base = base.withColumn(
            name,
            F.when(F.col("route") == "psi", F.col(name)).otherwise(
                F.lit(None).cast(data_type)
            ),
        )
    path = f"{workspace}/profiles/columns_sampled"
    base.select(*[field.name for field in _column_profile_schema().fields]).write.mode(
        "append"
    ).parquet(path)
    return spark.read.parquet(path), samples, records


def plans_for_table(plans: list[ColumnPlan], table: str) -> list[ColumnPlan]:
    return [plan for plan in plans if plan.table == table]


def _batch_jaccard_values(df: DataFrame, batch: list[ColumnPlan]) -> DataFrame:
    entries = []
    for plan in batch:
        value, _psi, valid, _invalid = _normalized_expressions(df, plan)
        entries.append(
            F.struct(
                F.lit(plan.table).alias("table"),
                F.lit(plan.column).alias("column"),
                F.lit(plan.type_json).alias("type_json"),
                value.alias("value"),
                valid.alias("is_valid"),
            )
        )
    return (
        df.select(F.explode(F.array(*entries)).alias("cell"))
        .select("cell.*")
        .where(F.col("is_valid"))
        .select("table", "column", "type_json", "value")
        .dropDuplicates(["table", "column", "type_json", "value"])
    )


def _production_jaccard_values(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    plans: list[ColumnPlan],
    profiles: DataFrame,
    candidates: DataFrame,
    workspace: str,
    config: Config,
) -> tuple[DataFrame, str]:
    eligible = {
        (row["table"], row["column"])
        for row in profiles.where(F.col("route") == "jaccard")
        .select("table", "column")
        .collect()
    }
    raw_path = f"{workspace}/profiles/jaccard_raw"
    wrote = False
    for table, df in tables.items():
        selected = [
            plan
            for plan in plans
            if plan.table == table and (plan.table, plan.column) in eligible
        ]
        for batch_id, batch in _batches(selected, config.column_batch_size):
            values = _batch_jaccard_values(df, batch)
            values.write.mode("append").partitionBy("table", "column").parquet(raw_path)
            wrote = True
            _perf_log(
                "jaccard_extract_batch",
                table=table,
                batch=batch_id,
                columns=len(batch),
            )
    raw_schema = "table string, column string, type_json string, value string"
    has_partition_data = wrote and _path_has_parquet_parts(spark, raw_path)
    if not has_partition_data:
        spark.createDataFrame([], raw_schema).write.mode("append").parquet(raw_path)
    raw = spark.read.parquet(raw_path)
    needs_sketch = _sketch_profiles(candidates, config)
    ranked_source = raw.join(
        needs_sketch, ["table", "column", "type_json"], "inner"
    ).withColumn("sha256_rank", F.sha2("value", 256))
    window = Window.partitionBy("table", "column", "type_json").orderBy(
        "sha256_rank", "value"
    )
    ranked = ranked_source.withColumn("sketch_rank", F.row_number().over(window))
    output = raw.join(
        ranked,
        ["table", "column", "type_json", "value"],
        "left",
    ).select("table", "column", "type_json", "value", "sha256_rank", "sketch_rank")
    path = f"{workspace}/profiles/jaccard_values"
    if has_partition_data:
        output.write.mode("append").partitionBy("table", "column").parquet(path)
    else:
        output.write.mode("append").parquet(path)
    ordered = spark.read.parquet(path).select(
        "table", "column", "type_json", "value", "sha256_rank", "sketch_rank"
    )
    return ordered, path


def _representative_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("table", T.StringType(), False),
            T.StructField("column", T.StringType(), False),
            T.StructField("type_json", T.StringType(), False),
            T.StructField("representative_index", T.IntegerType(), False),
            T.StructField("point", T.StringType(), False),
        ]
    )


def _production_psi_representatives(
    spark: SparkSession,
    samples: dict[str, DataFrame],
    plan_by_identity: dict[tuple[str, str], ColumnPlan],
    profile_rows: dict[tuple[str, str], object],
    native_types: dict[str, T.DataType],
    workspace: str,
    config: Config,
) -> DataFrame:
    path = f"{workspace}/profiles/psi_representatives"
    wrote = False
    for identity, profile in sorted(profile_rows.items()):
        if profile["route"] != "psi" or not profile["eligible"]:
            continue
        plan = plan_by_identity[identity]
        df = samples[plan.table]
        _value, psi_value, valid, _invalid = _normalized_expressions(df, plan)
        native_type = native_types[plan.type_json]
        narrow = df.select(
            psi_value.cast(native_type).alias("native_value"), valid.alias("is_valid")
        ).where(F.col("is_valid"))
        window = Window.orderBy("native_value")
        ranked = narrow.select("native_value").withColumn(
            "native_rank", F.row_number().over(window)
        )
        count = narrow.count()
        if count == 0:
            continue
        targets = [
            (
                index,
                max(1, math.ceil((index + 0.5) * count / config.psi_representatives)),
            )
            for index in range(config.psi_representatives)
        ]
        target_df = spark.createDataFrame(targets, "representative_index int, native_rank int")
        representatives = target_df.join(ranked, "native_rank", "inner").select(
            F.lit(plan.table).alias("table"),
            F.lit(plan.column).alias("column"),
            F.lit(plan.type_json).alias("type_json"),
            "representative_index",
            F.col("native_value").cast("string").alias("point"),
        )
        representatives.write.mode("append").parquet(path)
        wrote = True
        _perf_log(
            "psi_representatives",
            table=plan.table,
            column=plan.column,
            sample_rows=count,
            source="workspace_sample",
        )
    if not wrote:
        spark.createDataFrame([], _representative_schema()).write.mode("append").parquet(path)
    return spark.read.parquet(path)


def _production_psi_edges(
    spark: SparkSession,
    representatives: DataFrame,
    native_types: dict[str, T.DataType],
    workspace: str,
    config: Config,
) -> tuple[DataFrame, dict[str, list[str]]]:
    records = []
    edge_lookup: dict[str, list[str]] = {}
    present_types = {
        row["type_json"] for row in representatives.select("type_json").distinct().collect()
    }
    for type_json in sorted(present_types):
        native_type = native_types[type_json]
        points = representatives.where(F.col("type_json") == type_json).select(
            F.col("point").cast(native_type).alias("point")
        )
        count = points.count()
        ranked = points.withColumn("point_rank", F.row_number().over(Window.orderBy("point")))
        targets = sorted(
            {
                max(1, math.ceil(index * count / config.psi_bins))
                for index in range(1, config.psi_bins)
            }
        )
        cuts = [
            row["point"]
            for row in ranked.where(F.col("point_rank").isin(targets))
            .select(F.col("point").cast("string").alias("point"))
            .dropDuplicates(["point"])
            .orderBy(F.col("point").cast(native_type))
            .collect()
        ]
        edge_lookup[type_json] = cuts
        records.append(
            {
                "type_json": type_json,
                "cuts": cuts,
                "native_type": native_type.simpleString(),
                "representative_count": config.psi_representatives,
                "accuracy": config.psi_accuracy,
                "approximation": "native_nearest_rank",
            }
        )
        _perf_log("psi_global_edges", type_json=type_json, points=count, cuts=len(cuts))
    edges = (
        spark.createDataFrame(records, _empty_edges(spark).schema)
        if records
        else _empty_edges(spark)
    )
    path = f"{workspace}/profiles/psi_edges"
    edges.write.mode("append").parquet(path)
    return spark.read.parquet(path), edge_lookup


def _psi_bin_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("table", T.StringType(), False),
            T.StructField("column", T.StringType(), False),
            T.StructField("type_json", T.StringType(), False),
            T.StructField("bin_index", T.IntegerType(), False),
            T.StructField("lower_edge", T.StringType(), True),
            T.StructField("upper_edge", T.StringType(), True),
            T.StructField("lower_infinite", T.BooleanType(), False),
            T.StructField("upper_infinite", T.BooleanType(), False),
            T.StructField("count", T.LongType(), False),
            T.StructField("total", T.LongType(), False),
            T.StructField("effective_bins", T.IntegerType(), False),
            T.StructField("bins_requested", T.IntegerType(), False),
            T.StructField("representative_count", T.IntegerType(), False),
            T.StructField("accuracy", T.IntegerType(), False),
            T.StructField("approximation", T.StringType(), False),
            T.StructField("timezone", T.StringType(), False),
            T.StructField("edges_json", T.StringType(), False),
            T.StructField("native_type", T.StringType(), False),
        ]
    )


def _production_psi_bins(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    plans: list[ColumnPlan],
    profile_rows: dict[tuple[str, str], object],
    native_types: dict[str, T.DataType],
    edge_lookup: dict[str, list[str]],
    workspace: str,
    config: Config,
) -> DataFrame:
    path = f"{workspace}/profiles/psi_bins"
    wrote = False
    for table, df in tables.items():
        selected = [
            plan
            for plan in plans
            if plan.table == table
            and profile_rows[(plan.table, plan.column)]["route"] == "psi"
            and profile_rows[(plan.table, plan.column)]["eligible"]
        ]
        for batch_id, batch in _batches(selected, config.column_batch_size):
            expressions = []
            metadata = []
            for column_index, plan in enumerate(batch):
                _value, psi_value, valid, _invalid = _normalized_expressions(df, plan)
                native_type = native_types[plan.type_json]
                native_value = psi_value.cast(native_type)
                cuts = edge_lookup[plan.type_json]
                effective_bins = len(cuts) + 1
                for bin_index in range(effective_bins):
                    condition = valid
                    if bin_index > 0:
                        condition = condition & (
                            native_value > F.lit(cuts[bin_index - 1]).cast(native_type)
                        )
                    if bin_index < len(cuts):
                        condition = condition & (
                            native_value <= F.lit(cuts[bin_index]).cast(native_type)
                        )
                    alias = f"c{column_index}_{bin_index}"
                    expressions.append(
                        F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(alias)
                    )
                    metadata.append((alias, plan, bin_index, cuts))
            result = df.agg(*expressions).first()
            records = []
            for alias, plan, bin_index, cuts in metadata:
                profile = profile_rows[(plan.table, plan.column)]
                records.append(
                    {
                        "table": plan.table,
                        "column": plan.column,
                        "type_json": plan.type_json,
                        "bin_index": bin_index,
                        "lower_edge": cuts[bin_index - 1] if bin_index > 0 else None,
                        "upper_edge": cuts[bin_index] if bin_index < len(cuts) else None,
                        "lower_infinite": bin_index == 0,
                        "upper_infinite": bin_index == len(cuts),
                        "count": int(result[alias]),
                        "total": int(profile["valid_count"]),
                        "effective_bins": len(cuts) + 1,
                        "bins_requested": config.psi_bins,
                        "representative_count": config.psi_representatives,
                        "accuracy": config.psi_accuracy,
                        "approximation": "native_nearest_rank",
                        "timezone": config.timezone,
                        "edges_json": json.dumps(cuts, separators=(",", ":")),
                        "native_type": native_types[plan.type_json].simpleString(),
                    }
                )
            _append_rows(spark, records, _psi_bin_schema(), path)
            wrote = True
            _perf_log(
                "psi_histogram_batch",
                table=table,
                batch=batch_id,
                columns=len(batch),
                bins=len(records),
            )
    if not wrote:
        spark.createDataFrame([], _psi_bin_schema()).write.mode("append").parquet(path)
    return spark.read.parquet(path)


def _jaccard_score_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("pair_id", T.StringType(), True),
            T.StructField("mode", T.StringType(), True),
            T.StructField("jaccard", T.DoubleType(), True),
            T.StructField("sketch_sample_size", T.LongType(), True),
            T.StructField("jaccard_interval_low", T.DoubleType(), True),
            T.StructField("jaccard_interval_high", T.DoubleType(), True),
        ]
    )


def _exact_jaccard_pair_batch(pair_batch: DataFrame, jvalues: DataFrame) -> DataFrame:
    values = jvalues.select("table", "column", "type_json", "value")
    left_requests = pair_batch.select(
        "pair_id",
        F.col("table_a").alias("table"),
        F.col("column_a").alias("column"),
        "type_json",
    )
    right_requests = pair_batch.select(
        "pair_id",
        F.col("table_b").alias("table"),
        F.col("column_b").alias("column"),
        "type_json",
    )
    left_values = left_requests.join(
        values, ["table", "column", "type_json"], "inner"
    ).select("pair_id", "value")
    right_values = right_requests.join(
        values, ["table", "column", "type_json"], "inner"
    ).select("pair_id", "value")
    intersections = left_values.join(
        right_values, ["pair_id", "value"], "inner"
    ).groupBy("pair_id").agg(
        F.count(F.lit(1)).alias("intersection_count")
    )
    return pair_batch.join(intersections, "pair_id", "left").select(
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


def _assign_exact_pair_batches(exact: DataFrame, batch_size: int) -> DataFrame:
    """Distributed deterministic ordering; avoids a global SQL window partition."""
    identity = lambda row: (  # noqa: E731 - required by RDD sortBy serialization
        row["table_a"],
        row["column_a"],
        row["table_b"],
        row["column_b"],
        row["type_json"],
    )
    partitions = max(
        exact.rdd.getNumPartitions(), exact.sparkSession.sparkContext.defaultParallelism
    )
    indexed = (
        exact.rdd.sortBy(identity, ascending=True, numPartitions=partitions)
        .zipWithIndex()
        .map(lambda item: tuple(item[0]) + (item[1] + 1, item[1] // batch_size))
    )
    schema = exact.schema.add("pair_ordinal", T.LongType(), False).add(
        "pair_batch_id", T.LongType(), False
    )
    return exact.sparkSession.createDataFrame(indexed, schema)


def _read_exact_batch_jvalues(
    spark: SparkSession,
    jvalues_path: str,
    pair_batch: DataFrame,
    batch_id: int,
) -> DataFrame:
    identities = (
        pair_batch.select(
            F.col("table_a").alias("table"), F.col("column_a").alias("column")
        )
        .unionByName(
            pair_batch.select(
                F.col("table_b").alias("table"), F.col("column_b").alias("column")
            )
        )
        .dropDuplicates(["table", "column"])
        .collect()
    )
    if len(identities) > 200:
        raise RuntimeError(f"Exact pair batch {batch_id} requested more than 200 profiles")
    predicate = None
    for identity in identities:
        term = (F.col("table") == identity["table"]) & (
            F.col("column") == identity["column"]
        )
        predicate = term if predicate is None else predicate | term
    values = spark.read.parquet(jvalues_path)
    filtered = values.where(predicate) if predicate is not None else values.limit(0)
    _perf_log(
        "exact_partition_read",
        batch=batch_id,
        profiles=len(identities),
        partition_filter=True,
    )
    return filtered.select(
        "table", "column", "type_json", "value", "sha256_rank", "sketch_rank"
    )


def _production_scores(
    spark: SparkSession,
    candidates: DataFrame,
    jvalues_path: str,
    bins: DataFrame,
    workspace: str,
    config: Config,
) -> DataFrame:
    exact = candidates.where(
        (F.col("status") == "ok")
        & (F.col("metric") == "jaccard")
        & (F.col("distinct_count_a") <= config.exact_distinct_threshold)
        & (F.col("distinct_count_b") <= config.exact_distinct_threshold)
    )
    assigned_path = f"{workspace}/exact_pairs_batched"
    _assign_exact_pair_batches(exact, config.exact_pair_batch_size).write.mode(
        "append"
    ).parquet(assigned_path)
    exact = spark.read.parquet(assigned_path)
    assignment = exact.agg(
        F.max("pair_batch_id").alias("maximum"), F.count(F.lit(1)).alias("pairs")
    ).first()
    maximum = assignment["maximum"]
    _perf_log(
        "exact_batches_materialized",
        pairs=assignment["pairs"],
        batches=(int(maximum) + 1 if maximum is not None else 0),
    )
    score_path = f"{workspace}/fragments/jaccard_scores"
    wrote = False
    for batch_id in range(int(maximum) + 1 if maximum is not None else 0):
        pair_batch = exact.where(F.col("pair_batch_id") == batch_id).drop(
            "pair_ordinal", "pair_batch_id"
        )
        pair_count = pair_batch.count()
        if pair_count > config.exact_pair_batch_size:
            raise RuntimeError(f"Exact pair batch {batch_id} exceeded configured bound")
        batch_jvalues = _read_exact_batch_jvalues(
            spark, jvalues_path, pair_batch, batch_id
        )
        scores = _exact_jaccard_pair_batch(pair_batch, batch_jvalues)
        scores.write.mode("append").parquet(score_path)
        wrote = True
        _perf_log("exact_score_batch", batch=batch_id, pairs=pair_count)
    all_jvalues = spark.read.parquet(jvalues_path).select(
        "table", "column", "type_json", "value", "sha256_rank", "sketch_rank"
    )
    sketch = _sketch_jaccard(candidates, all_jvalues, config)
    sketch.write.mode("append").parquet(score_path)
    sketch_count = (
        spark.read.parquet(score_path)
        .where(F.col("mode") == "bottom_k")
        .count()
    )
    _perf_log("sketch_scores", pairs=sketch_count)
    wrote = True
    if not wrote:
        spark.createDataFrame([], _jaccard_score_schema()).write.mode("append").parquet(score_path)
    j_scores = spark.read.parquet(score_path)
    psi_path = f"{workspace}/fragments/psi_scores"
    psi_scores = _psi_scores(candidates, bins)
    psi_scores.write.mode("append").parquet(psi_path)
    _perf_log("psi_scores", pairs=psi_scores.count())
    psi = spark.read.parquet(psi_path)
    pairs = _assemble_scores(candidates, j_scores, psi)
    path = f"{workspace}/pairs"
    pairs.write.mode("append").parquet(path)
    return spark.read.parquet(path)


def _build_scalable_analysis(
    spark: SparkSession,
    tables: dict[str, DataFrame],
    specs: Optional[dict],
    extra_excludes: Iterable[str],
    config: Config,
    workspace: str,
) -> Analysis:
    plans = plan_columns(tables, specs or {}, extra_excludes)
    profiles = _production_profiles(spark, tables, plans, workspace, config)
    profiles, psi_samples, psi_sampling = _production_psi_samples(
        spark, tables, plans, profiles, workspace, config
    )
    candidate_path = f"{workspace}/candidates"
    _candidate_pairs(profiles, config).write.mode("append").parquet(candidate_path)
    candidates = spark.read.parquet(candidate_path)
    _perf_log("candidates", pairs=candidates.count())

    profile_rows = {(row["table"], row["column"]): row for row in profiles.collect()}
    plan_by_identity = {(plan.table, plan.column): plan for plan in plans}
    native_types = _psi_native_types(tables, plans)
    jvalues, jvalues_path = _production_jaccard_values(
        spark, tables, plans, profiles, candidates, workspace, config
    )
    representatives = _production_psi_representatives(
        spark,
        psi_samples,
        plan_by_identity,
        profile_rows,
        native_types,
        workspace,
        config,
    )
    _edges, edge_lookup = _production_psi_edges(
        spark, representatives, native_types, workspace, config
    )
    bins = _production_psi_bins(
        spark,
        tables,
        plans,
        profile_rows,
        native_types,
        edge_lookup,
        workspace,
        config,
    )
    pairs = _production_scores(
        spark, candidates, jvalues_path, bins, workspace, config
    )
    integrity = _integrity_checks(profiles, jvalues, bins, pairs)
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
    return Analysis(
        profiles, jvalues, bins, pairs, integrity, skips, absent, psi_sampling
    )


def run_production(
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
    """Filesystem-truncated production pipeline used by main; never builds global tall rows."""
    validate_output_base(output_base, input_paths)
    workspace = f"{output_base.rstrip('/')}.__workspace_{uuid.uuid4().hex}"
    _delete_path(spark, workspace)
    started = datetime.now(timezone.utc)
    try:
        analysis = _build_scalable_analysis(
            spark, tables, specs, extra_excludes, config, workspace
        )
        report = build_report(
            analysis, list(tables), base_uri, prefix, specs_uri, output_base, config
        )
        publication = write_outputs(spark, analysis, output_base, report)
        published = Analysis(
            spark.read.parquet(f"{publication.run_uri}/profiles/columns"),
            spark.read.parquet(f"{publication.run_uri}/profiles/jaccard_values"),
            spark.read.parquet(f"{publication.run_uri}/profiles/psi_bins"),
            spark.read.parquet(f"{publication.run_uri}/pairs"),
            analysis.integrity,
            analysis.skips,
            analysis.tables_absent_specs,
            analysis.psi_sampling,
        )
        _perf_log(
            "production_complete",
            seconds=(datetime.now(timezone.utc) - started).total_seconds(),
        )
        return published
    finally:
        _delete_path(spark, workspace)


def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def _path_has_parquet_parts(spark: SparkSession, path: str) -> bool:
    hpath = _hadoop_path(spark, path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(hpath):
        return False
    files = fs.listFiles(hpath, True)
    while files.hasNext():
        if files.next().getPath().getName().startswith("part-"):
            return True
    return False


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
            f"Each table is deterministically sampled once to approximately at most "
            f"{config.psi_sample_rows} rows with seed {config.sample_seed}. From that "
            "durable bounded sample, native-type nearest-rank ordering computes "
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
        "psi_sampling": analysis.psi_sampling,
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
) -> Publication:
    validate_output_base(output_base, [])
    generated_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + f"-{uuid.uuid4().hex}"
    run_uri = f"{output_base.rstrip('/')}/runs/{run_id}"
    report_uri = f"{run_uri}/report.json"
    publication = Publication(run_id, run_uri, report_uri, generated_at)
    frames = _output_frames(analysis)
    expected = {
        relative: (_schema_signature(frame.schema), frame.count())
        for relative, frame in frames.items()
    }
    published_report = dict(report)
    published_report["publication"] = asdict(publication)
    try:
        for relative, frame in frames.items():
            frame.write.mode("append").parquet(f"{run_uri}/{relative}")
        write_text(spark, report_uri, json.dumps(published_report, indent=2, default=str))
        _validate_output_readback(spark, run_uri, expected, analysis.integrity)
    except Exception:
        try:
            _delete_path(spark, run_uri)
        except Exception:  # noqa: BLE001 - dead contexts may leave an immutable orphan
            logger.exception("Could not clean incomplete immutable run %s", run_uri)
        raise
    latest = asdict(publication)
    write_text(
        spark,
        f"{output_base.rstrip('/')}/LATEST.json",
        json.dumps(latest, indent=2),
    )
    return publication


def read_latest(spark: SparkSession, output_base: str) -> Publication:
    payload = json.loads(read_text(spark, f"{output_base.rstrip('/')}/LATEST.json"))
    return Publication(
        payload["run_id"],
        payload["run_uri"],
        payload["report_uri"],
        payload["generated_at"],
    )


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
        "psi_sample_rows",
        "column_batch_size",
        "exact_pair_batch_size",
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
        latest = json.loads(read_text(spark, f"{output}/LATEST.json"))
        assert spark.read.parquet(f"{latest['run_uri']}/profiles/columns").count() == 6
        report = json.loads(read_text(spark, latest["report_uri"]))
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
    parser.add_argument(
        "--output-base",
        help="Immutable run catalog root; only LATEST.json is overwritten after validation",
    )
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
    parser.add_argument("--psi-sample-rows", type=int, default=100_000)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--column-batch-size", type=int, default=8)
    parser.add_argument("--exact-pair-batch-size", type=int, default=100)
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
        psi_sample_rows=args.psi_sample_rows,
        sample_seed=args.sample_seed,
        column_batch_size=args.column_batch_size,
        exact_pair_batch_size=args.exact_pair_batch_size,
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
        run_production(
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
        publication = read_latest(spark, args.output_base)
        logger.info(
            "Similarity run published: run_id=%s run_uri=%s latest=%s/LATEST.json",
            publication.run_id,
            publication.run_uri,
            args.output_base.rstrip("/"),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
