"""Opt-in, real-Parquet million-row regression for the auxiliary evidence path."""

import os
from time import perf_counter

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameReader

from datagen import engorda_tables as producer
from scripts import validate_products as validator


@pytest.mark.skipif(os.environ.get("DATAGEN_SCALE_TESTS") != "1", reason="opt-in million-row test")
def test_one_million_ccb_evidence_roundtrip(tmp_path, monkeypatch):
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ccb-million-row-evidence")
        .config("spark.driver.maxResultSize", "1m")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    started = perf_counter()
    size = 1_000_000
    collected_sizes = []
    original_collect = DataFrame.collect
    original_parquet = DataFrameReader.parquet
    forbidden_reads = []
    mapping = None

    def bounded_collect(frame):
        assert frame.columns == ["bucket", "count", "s0", "s1", "s2", "s3"]
        rows = original_collect(frame)
        assert len(rows) <= 256
        collected_sizes.append(len(rows))
        return rows

    def frozen_reads_only(reader, *paths, **options):
        assert not any(
            str(path).startswith(forbidden) for path in paths for forbidden in forbidden_reads
        ), "Evidence replay must not reread RAW or the source snapshot during validation"
        return original_parquet(reader, *paths, **options)

    monkeypatch.setattr(DataFrame, "collect", bounded_collect)
    monkeypatch.setattr(DataFrameReader, "parquet", frozen_reads_only)
    try:
        config = {"DATAGEN_RAW_BASE_URI": str(tmp_path / "raw")}
        raw_uri = producer.raw_path(config, "ACTPCCB_CONDICAO_IF")
        source = spark.range(size + 10, numPartitions=16).select(
            (F.col("id") + 2_000_000_000).cast("decimal(38,10)").alias("NUM_IF"),
            F.lit("PREFIXADO").alias("RENT_INDEXADOR_TAXA_FLU"),
            F.lit("PAGAMENTO DE PARCELAS").alias("FORMA_PAGAMENTO"),
        )
        source.write.parquet(raw_uri)
        roots = spark.range(size, numPartitions=16).select(
            (F.col("id") + 2_000_000_000).cast("decimal(38,10)").alias("NUM_IF")
        )
        descriptor = producer._freeze_ccb_classification_source(
            spark, config, "ccb_pppre", roots, str(tmp_path / "plan.json")
        )
        assert descriptor["row_count"] == size
        forbidden_reads.append(raw_uri)
        mapping = roots.select(
            F.col("NUM_IF").alias("old_NUM_IF"),
            F.lit(1).cast("long").alias(producer.K_COL),
            (F.col("NUM_IF").cast("long") + 1_000_000_000).alias("new_NUM_IF"),
        ).cache()
        output = str(tmp_path / "synthetic")
        plan = {"plan_id": "f" * 64, "ccb_classification": descriptor}
        producer._write_ccb_classification_evidence(
            spark,
            config,
            "ccb_pppre",
            roots,
            mapping,
            fator_k=1,
            output_base=output,
            output_uri=output,
            plan=plan,
        )
        producer._write_offline_artifact_marker(spark, output, "ccb_pppre", plan)
        forbidden_reads.append(descriptor["uri"])
        instruments = mapping.select(
            F.col("new_NUM_IF").alias("NUM_IF"),
            F.lit(53).alias("NUM_TIPO_IF"),
            F.lit(None).cast("long").alias("NUM_IF_PERTENCE"),
            F.lit(None).cast("timestamp").alias("DAT_EXCLUSAO"),
        )
        tables = {
            "INSTRUMENTO_FINANCEIRO": instruments,
            "MAPA_CLONE_NUM_IF": mapping.select(
                F.col("old_NUM_IF").alias("NUM_IF_ORIG"),
                F.col(producer.K_COL).alias("K"),
                F.col("new_NUM_IF").alias("NUM_IF_NOVO"),
            ),
            "OPERACAO": instruments.select(
                "NUM_IF",
                F.lit(871).alias("NUM_ID_TIPO_OPER_OBJETO_SERV"),
                F.lit(43).alias("COD_SITUACAO_OPERACAO"),
            ),
            "HISTORICO_PU_CURVA": instruments.select("NUM_IF"),
        }
        classification = validator.load_ccb_classification_evidence(spark, output, tables)
        assert classification.count() == size
        findings = validator.check_osias(
            tables,
            5,
            validator.VALIDATION_PROFILES["ccb"],
            True,
            ccb_classification=classification,
        )
        assert findings and all(item.passed for item in findings)
        assert "ACTPCCB_CONDICAO_IF" not in tables
        assert "_CCB_CLASSIFICATION" not in validator.list_table_dirs(spark, output)
        assert (tmp_path / "synthetic" / "_CCB_CLASSIFICATION.json").stat().st_size < 8192
        assert collected_sizes and max(collected_sizes) == 256
        print(
            f"CCB evidence: {size} source and synthetic rows; "
            f"max collected summaries={max(collected_sizes)}; "
            f"driver result limit=1m; elapsed={perf_counter() - started:.2f}s"
        )
    finally:
        if mapping is not None:
            mapping.unpersist()
        spark.stop()
