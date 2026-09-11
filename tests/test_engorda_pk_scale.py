"""Opt-in allocation and FK workload matrix, not the full business pipeline.

Run with DATAGEN_SCALE_TESTS=1 and a Spark 3.5 runtime. The five logged
workloads use 16 shuffle partitions; the separate reproducer uses 512 and
Spark's default range-exchange sample size of 20. No source keys reach Python.
The reproducer allows 8 MiB for Spark's bounded internal range samples; the
five large workloads use a stricter 1 MiB driver-result limit.
"""

import json
import os
from decimal import Decimal
from time import perf_counter

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from datagen import engorda_tables as producer

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAGEN_SCALE_TESTS") != "1", reason="opt-in PK scale regression"
)

CASES = [
    ("cdb_simplificado", 1_000_000, 1, 2_000_000, 962_573, 16),
    ("cdb_resgate", 1_000_000, 1, 2_000_000, 1_000_000, 16),
    ("cdb_escalonamento", 376_201, 3, 2_132_224, 1_756_023, 16),
    ("rdb_inclusao", 777_834, 2, 1_555_693, 777_832, 16),
    ("rdb_resgate", 5_696, 176, 11_392, 5_696, 16),
    ("known_reproducer_100k_k2", 100_000, 2, 200_000, 100_000, 512),
]


@pytest.fixture
def scale_spark(request):
    result_limit = "8m" if request.node.callspec.params["partitions"] == 512 else "1m"
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("engorda-pk-allocation-fk-scale")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", result_limit)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.execution.rangeExchange.sampleSizePerPartition", "20")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


@pytest.mark.parametrize(
    "case,n,k,conditions,juros,partitions", CASES, ids=[case[0] for case in CASES]
)
def test_clona_tabela_allocation_fk_scale(
    scale_spark, tmp_path, monkeypatch, case, n, k, conditions, juros, partitions
):
    spark = scale_spark
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    started = perf_counter()
    original_collect, original_broadcast = DataFrame.collect, F.broadcast
    collected_sizes, broadcast_sizes, results = [], [], []
    mappings = {}
    baseline = spark.sparkContext._jsc.sc().getPersistentRDDs().size()
    metric_columns = {"rows", "unique", "pairs", "minimum", "maximum", "wrong_rank", "wrong_fk"}

    def bounded_collect(frame):
        columns = set(frame.columns)
        summary = columns == {"____pk_rid_part", "__sz"}
        metrics = columns == metric_columns or columns == {"bounded_count"}
        assert summary or metrics, f"Driver key collection forbidden: {frame.columns}"
        assert "Aggregate" in frame._jdf.queryExecution().optimizedPlan().toString()
        limit = max(512, partitions) if summary else 1
        rows = original_collect(frame.limit(limit + 1))
        assert len(rows) <= limit, "Driver summaries exceeded their bounded cardinality"
        collected_sizes.append(len(rows))
        return rows

    def bounded_broadcast(frame):
        columns = set(frame.columns)
        if columns == {producer.K_COL}:
            limit = k
        else:
            assert columns == {"____pk_rid_part", "____pk_rid_poff"}, (
                f"Forced full-map broadcast forbidden: {frame.columns}"
            )
            limit = partitions
        size = frame.agg(F.count("*").alias("bounded_count")).collect()[0][0]
        assert size <= limit
        broadcast_sizes.append(size)
        return original_broadcast(frame)

    monkeypatch.setattr(DataFrame, "collect", bounded_collect)
    monkeypatch.setattr(F, "broadcast", bounded_broadcast)
    root_start, condition_start = 9_000_000_000_000, 80_000_000_000_000
    root_base, condition_base = Decimal("2000000000.125"), Decimal("4000000000.125")
    root_name, condition_name = "INSTRUMENTO_FINANCEIRO", "CONDICAO_IF"
    root_fk = producer.FkRemap(("NUM_IF",), root_name, ("NUM_IF",), True)
    condition_fk = producer.FkRemap(
        ("NUM_CONDICAO_IF",), condition_name, ("NUM_CONDICAO_IF",), True
    )
    tables = [
        (
            producer.PlanoTabela(
                root_name, ("NUM_IF",), pk_regra="OFFSET_PROPRIO", pk_start=root_start
            ),
            n,
            root_base,
            root_start,
        ),
        (
            producer.PlanoTabela(
                condition_name,
                ("NUM_CONDICAO_IF",),
                [root_fk],
                pk_regra="OFFSET_PROPRIO",
                pk_start=condition_start,
            ),
            conditions,
            condition_base,
            condition_start,
        ),
        (
            producer.PlanoTabela(
                "JUROS_FLUTUANTE", ("NUM_CONDICAO_IF",), [condition_fk], pk_regra="VIA_PAI"
            ),
            juros,
            condition_base,
            condition_start,
        ),
    ]

    def check(frame, pk, rank, clone, wrong_fk, count, start, table, kind, pair):
        expected = F.lit(start) + rank * k + clone - 1
        valid = F.col(pk).eqNullSafe(expected) & rank.between(0, count - 1) & clone.between(1, k)
        stats = (
            frame.agg(
                F.count("*").alias("rows"),
                F.countDistinct(pk).alias("unique"),
                F.countDistinct(F.struct(*pair)).alias("pairs"),
                F.min(pk).alias("minimum"),
                F.max(pk).alias("maximum"),
                F.sum(F.when(valid, 0).otherwise(1)).alias("wrong_rank"),
                F.sum(F.when(wrong_fk, 1).otherwise(0)).alias("wrong_fk"),
            )
            .collect()[0]
            .asDict()
        )
        results.append({"table": table, "kind": kind, **stats})
        assert stats == {
            "rows": count * k,
            "unique": count * k,
            "pairs": count * k,
            "minimum": start,
            "maximum": start + count * k - 1,
            "wrong_rank": 0,
            "wrong_fk": 0,
        }, (case, table, kind, stats)

    try:
        for plan, count, base, start in tables:
            table_started = perf_counter()
            pk = plan.pk_cols[0]
            source = spark.range(count, numPartitions=16).select(
                (F.lit(base) + F.col("id") * 3).cast("decimal(38,10)").alias(pk),
                F.col("id").alias("source_rank"),
                (F.col("id") % n).alias("source_root_rank"),
            )
            if plan.name == condition_name:
                source = source.withColumn(
                    "NUM_IF",
                    (F.lit(root_base) + F.col("source_root_rank") * 3).cast("decimal(38,10)"),
                )
            source_path = str(tmp_path / plan.name / "source")
            source.write.parquet(source_path)
            source = spark.read.parquet(source_path)
            assert source.schema[pk].dataType == DecimalType(38, 10)
            output, mapping = producer.clona_tabela(spark, plan, source, k, mappings)
            mappings[plan.name] = mapping
            for column in (f"old_{pk}", f"new_{pk}"):
                assert mapping.schema[column].dataType == DecimalType(38, 10)
            assert mapping._jdf.logicalPlan().rdd().getStorageLevel().useMemory()
            # Fractional source keys test numeric ordering without lossy long casts.
            rank = (F.col(f"old_{pk}") - F.lit(base)) / 3
            check(
                mapping,
                f"new_{pk}",
                rank,
                F.col(producer.K_COL),
                F.lit(False),
                count,
                start,
                plan.name,
                "map",
                [F.col(f"old_{pk}"), F.col(producer.K_COL)],
            )
            output_path = str(tmp_path / plan.name / "output")
            output.write.parquet(output_path)
            output = spark.read.parquet(output_path)
            assert output.schema == source.schema
            clone = F.pmod(F.col(pk) - F.lit(start), F.lit(k)) + 1
            wrong_fk = ~F.col("source_root_rank").eqNullSafe(F.col("source_rank") % n)
            if plan.name == condition_name:
                wrong_fk = wrong_fk | ~F.col("NUM_IF").eqNullSafe(
                    F.lit(root_start) + F.col("source_root_rank") * k + clone - 1
                )
            # JUROS uses the first condition keys, so its exact interval and rank
            # oracle also prove the shared PK references the same parent's clone.
            check(
                output,
                pk,
                F.col("source_rank"),
                clone,
                wrong_fk,
                count,
                start,
                plan.name,
                "output",
                [F.col("source_rank"), clone],
            )
            results[-1]["table_elapsed_s"] = round(perf_counter() - table_started, 2)
            assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == (
                baseline + len(mappings)
            ), "Temporary allocation snapshots leaked"
        assert len(broadcast_sizes) == 5  # Three K ranges and two partition-offset maps.
        assert len(collected_sizes) == 13  # Five broadcast counts, two sizes, six metrics.
    finally:
        for mapping in mappings.values():
            mapping._jdf.logicalPlan().rdd().unpersist(False)
        remaining = spark.sparkContext._jsc.sc().getPersistentRDDs().size()
        print(
            "PK_SCALE "
            + json.dumps(
                {
                    "case": case,
                    "spark": spark.version,
                    "source_roots": n,
                    "k": k,
                    "shuffle_partitions": partitions,
                    "range_sample_size": 20,
                    "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
                    "driver_result_limit": spark.sparkContext.getConf().get(
                        "spark.driver.maxResultSize"
                    ),
                    "adaptive": False,
                    "auto_broadcast_threshold": -1,
                    "elapsed_s": round(perf_counter() - started, 2),
                    "max_collected_rows": max(collected_sizes, default=0),
                    "broadcast_rows": broadcast_sizes,
                    "persistent_rdds_before": baseline,
                    "persistent_rdds_after": remaining,
                    "results": results,
                },
                default=str,
                sort_keys=True,
            ),
            flush=True,
        )
    assert remaining == baseline, "Owned checkpoint maps leaked after child verification"
