import hashlib
import json
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datagen import engorda_tables as E


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ccb-producer-evidence-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def source_inputs(spark, tmp_path):
    config = {"DATAGEN_RAW_BASE_URI": str(tmp_path / "raw")}
    source = spark.createDataFrame(
        [
            (Decimal("10.00"), " PRE ", "PARCELADO"),
            (Decimal("10.00"), " PRE ", "PARCELADO"),
            (Decimal("20.00"), None, None),
        ],
        "NUM_IF decimal(20,2), RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string",
    )
    source.write.parquet(E.raw_path(config, "ACTPCCB_CONDICAO_IF"))
    return config, spark.createDataFrame([(10,)], "NUM_IF long")


@pytest.mark.parametrize(
    "product", ["ccb_pppre", "ccb_pfpre", "ccb_pgrpre", "ccb_favcp", "ccb_fapre"]
)
def test_materialize_old_ccb_plan_requires_replan_before_any_io(product):
    with pytest.raises(ValueError, match="CCB.*replan"):
        E.executa_clonagem(
            None,
            {},
            {},
            product_profile=E.get_product_profile(product),
            phase="materialize",
            planned_artifact={"product": product},
            reservation={},
            snapshot_lotes={},
            snapshot_lote_counts={},
        )


def test_fingerprint_matches_contract_and_is_partition_independent(spark):
    columns = ["NUM_IF", "RENT_INDEXADOR_TAXA_FLU", "FORMA_PAGAMENTO"]
    rows = [("10", "PR\u00c9", "P"), ("10", "PR\u00c9", "P"), ("20", None, "F")]
    buckets = {}
    for row in rows:
        encoded = json.dumps(dict(zip(columns, row)), ensure_ascii=False, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        bucket = int(digest[:8], 16) % 256
        summary = buckets.setdefault(
            bucket, {"bucket": bucket, "count": 0, **dict.fromkeys(["s0", "s1", "s2", "s3"], 0)}
        )
        summary["count"] += 1
        for index in range(4):
            summary[f"s{index}"] += int(digest[index * 16 : (index + 1) * 16], 16)
    summaries = [
        {**summary, **{f"s{i}": str(summary[f"s{i}"]) for i in range(4)}}
        for _, summary in sorted(buckets.items())
    ]
    expected = hashlib.sha256(
        json.dumps(summaries, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()
    frame = spark.createDataFrame(rows, ", ".join(f"{col} string" for col in columns))
    assert E._ccb_classification_fingerprint(frame, columns) == {"row_count": 3, "sha256": expected}
    assert E._ccb_classification_fingerprint(frame.repartition(3), columns)["sha256"] == expected
    assert E._ccb_classification_fingerprint(frame.limit(0), columns) == {
        "row_count": 0,
        "sha256": hashlib.sha256(b"[]").hexdigest(),
    }


def test_source_is_narrowed_canonical_and_cached_until_context_exit(spark, source_inputs):
    config, roots = source_inputs
    with E._load_ccb_classification_source(spark, config, "ccb_pppre", roots) as (source, desc):
        assert source.is_cached
        assert source.columns == list(E.CCB_CLASSIFICATION_COLUMNS)
        assert [tuple(row) for row in source.collect()] == [("10", " PRE ", "PARCELADO")]
        assert desc["row_count"] == 1
        assert desc["uri"] is None
    assert not source.is_cached


@pytest.mark.parametrize(
    "rows, message",
    [
        ([("10", "PRE", "P"), ("10", "PRE", "F")], "conflit"),
        ([("10", None, "P")], "null|vazi"),
        ([("10", "PRE", "  ")], "null|vazi"),
        ([("20", "PRE", "P")], "cobertura"),
    ],
)
def test_source_rejects_invalid_classification(spark, tmp_path, rows, message):
    config = {"DATAGEN_RAW_BASE_URI": str(tmp_path)}
    spark.createDataFrame(
        rows, ", ".join(f"{column} string" for column in E.CCB_CLASSIFICATION_COLUMNS)
    ).write.parquet(E.raw_path(config, "ACTPCCB_CONDICAO_IF"))
    roots = spark.createDataFrame([(10,)], "NUM_IF long")
    with pytest.raises(ValueError, match=message):
        with E._load_ccb_classification_source(spark, config, "ccb_pppre", roots):
            pytest.fail("invalid source accepted")


def test_unrelated_product_needs_no_source_or_spark():
    with E._load_ccb_classification_source(None, {}, "lci", None) as result:
        assert result == (None, None)
    assert E._freeze_ccb_classification_source(None, {}, "lci", None, "unused") is None
    E._write_ccb_classification_evidence(
        None, {}, "lci", None, None, fator_k=1, output_base=None, output_uri="unused"
    )


@pytest.mark.parametrize(
    "field, value",
    [
        ("artifact_type", "wrong"),
        ("schema_version", True),
        ("schema_version", 2),
        ("hash_algorithm", "sha256"),
        ("row_count", 0),
        ("row_count", True),
        ("uri", None),
        ("uri", "  "),
        ("sha256", "A" * 64),
        ("sha256", "bad"),
    ],
)
def test_invalid_frozen_descriptor_fails_before_io(field, value):
    descriptor = {
        "artifact_type": "ccb_classification_source",
        "schema_version": 1,
        "hash_algorithm": "sha256-bucket-sums-v1",
        "row_count": 1,
        "uri": "frozen/source",
        "sha256": "a" * 64,
    }
    descriptor[field] = value
    with pytest.raises(ValueError, match="CCB"):
        with E._load_ccb_classification_source(None, {}, "ccb_pppre", None, descriptor=descriptor):
            pytest.fail("invalid descriptor accepted")


def test_new_ccb_plan_requires_evidence():
    with pytest.raises(ValueError, match="CCB novo plano exige ccb_classification"):
        E._build_engorda_plan(
            config={},
            specs_uri="spec",
            spec_sha256="a" * 64,
            product_profile=E.get_product_profile("ccb_pppre"),
            valores=[10],
            fator_k=1,
            seed=42,
            engorda_ts=datetime(2026, 9, 11),
            controle_operacional_date=date(2026, 9, 11),
            tipo_derivado=54,
            planos={},
            lotes={},
            faltantes_uri=None,
            query_num_if_uri="query.sql",
            selected_lote={},
        )


@pytest.mark.parametrize("missing", ["table", "column"])
def test_missing_classification_source_fails_clearly(spark, tmp_path, missing):
    config = {"DATAGEN_RAW_BASE_URI": str(tmp_path)}
    if missing == "column":
        spark.createDataFrame([(10,)], "NUM_IF long").write.parquet(
            E.raw_path(config, "ACTPCCB_CONDICAO_IF")
        )
    with pytest.raises(ValueError, match="CCB classificacao"):
        with E._load_ccb_classification_source(
            spark, config, "ccb_pppre", spark.createDataFrame([(10,)], "NUM_IF long")
        ):
            pytest.fail("missing source accepted")


def test_snapshot_roundtrip_and_materialized_remap_never_read_raw(spark, source_inputs, tmp_path):
    config, roots = source_inputs
    descriptor = E._freeze_ccb_classification_source(
        spark, config, "ccb_pppre", roots, str(tmp_path / "plan.json")
    )
    assert descriptor["uri"].startswith(str(tmp_path / "plan.json.ccb-classification"))
    mapping = spark.createDataFrame(
        [(10, 1, 100), (10, 2, 101)], f"old_NUM_IF long, {E.K_COL} long, new_NUM_IF long"
    )
    output = str(tmp_path / "staged")
    final = str(tmp_path / "final")
    E._write_ccb_classification_evidence(
        spark,
        {"DATAGEN_RAW_BASE_URI": "missing://must-not-read"},
        "ccb_pppre",
        roots,
        mapping,
        fator_k=2,
        output_base=output,
        output_uri=final,
        plan={"ccb_classification": descriptor, "plan_id": "a" * 64},
    )
    manifest = json.loads((tmp_path / "staged" / E.CCB_CLASSIFICATION_MANIFEST).read_text())
    assert manifest == {
        "artifact_type": "ccb_classification_evidence",
        "schema_version": 1,
        "product": "ccb_pppre",
        "output_uri": final,
        "plan_id": "a" * 64,
        "fator_k": 2,
        "source": descriptor,
        "generated": E._ccb_classification_fingerprint(
            spark.read.parquet(f"{output}/{E.CCB_CLASSIFICATION_TABLE}"),
            E.CCB_CLASSIFICATION_OUTPUT_COLUMNS,
        ),
    }
    output_frame = spark.read.parquet(f"{output}/{E.CCB_CLASSIFICATION_TABLE}")
    assert output_frame.dtypes == [
        ("NUM_IF_ORIG", "string"),
        ("K", "bigint"),
        ("NUM_IF", "string"),
        ("RENT_INDEXADOR_TAXA_FLU", "string"),
        ("FORMA_PAGAMENTO", "string"),
    ]
    assert {tuple(row) for row in output_frame.collect()} == {
        ("10", 1, "100", " PRE ", "PARCELADO"),
        ("10", 2, "101", " PRE ", "PARCELADO"),
    }
    tampered = {**descriptor, "sha256": "0" * 64}
    with pytest.raises(ValueError, match="hash|fingerprint"):
        with E._load_ccb_classification_source(spark, {}, "ccb_pppre", roots, descriptor=tampered):
            pytest.fail("tampered snapshot accepted")
    with pytest.raises(ValueError, match="cobertura"):
        with E._load_ccb_classification_source(
            spark,
            {},
            "ccb_pppre",
            spark.createDataFrame([(20,)], "NUM_IF long"),
            descriptor=descriptor,
        ):
            pytest.fail("snapshot accepted different frozen roots")


@pytest.mark.parametrize("product", sorted(E.CCB_CLASSIFICATION_PRODUCTS))
def test_direct_all_evidence_and_dry_run(spark, source_inputs, tmp_path, product):
    config, roots = source_inputs
    mapping = spark.createDataFrame(
        [(10, 1, 100)], f"old_NUM_IF long, {E.K_COL} long, new_NUM_IF long"
    )
    for output in (None, str(tmp_path / "output")):
        E._write_ccb_classification_evidence(
            spark,
            config,
            product,
            roots,
            mapping,
            fator_k=1,
            output_base=output,
            output_uri=str(tmp_path / "final"),
        )
        if output is None:
            assert not (tmp_path / "output").exists()
    manifest = json.loads((tmp_path / "output" / E.CCB_CLASSIFICATION_MANIFEST).read_text())
    assert manifest["source"]["uri"] is None
    assert manifest["plan_id"] is None
    assert manifest["product"] == product


@pytest.mark.parametrize(
    "rows",
    [
        [(10, 1, 100), (10, 1, 101)],
        [(10, 1, 100), (10, 2, 100)],
        [(10, 1, 100), (10, 3, 101)],
        [(10, 1, 100)],
        [(10, 1, 100), (20, 2, 101)],
    ],
)
def test_invalid_mapping_blocks_output(spark, source_inputs, tmp_path, rows):
    config, roots = source_inputs
    mapping = spark.createDataFrame(rows, f"old_NUM_IF long, {E.K_COL} long, new_NUM_IF long")
    with pytest.raises(ValueError, match="CCB evidencia"):
        E._write_ccb_classification_evidence(
            spark,
            config,
            "ccb_pppre",
            roots,
            mapping,
            fator_k=2,
            output_base=str(tmp_path / "output"),
            output_uri=str(tmp_path / "final"),
        )
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize(
    "failure", ["parquet", "manifest", "manifest_readback", "hash_readback", "schema_readback"]
)
def test_staging_failures_block_publication_and_release_caches(
    spark, source_inputs, tmp_path, monkeypatch, failure
):
    config, roots = source_inputs
    mapping = spark.createDataFrame(
        [(10, 1, 100)], f"old_NUM_IF long, {E.K_COL} long, new_NUM_IF long"
    )
    published = []
    monkeypatch.setattr(E, "_publish_staging", lambda *args, **kwargs: published.append(args))
    baseline = spark.sparkContext._jsc.getPersistentRDDs().size()

    def fail(*args, **kwargs):
        raise ValueError("injected failure")

    if failure == "parquet":
        monkeypatch.setattr(E, "escreve_tabela", fail)
    elif failure == "manifest":
        monkeypatch.setattr(E, "_write_json_artifact", fail)
    elif failure == "manifest_readback":
        monkeypatch.setattr(E, "_read_json_artifact", lambda *args: {})
    else:
        write = E.escreve_tabela

        def corrupt(spark, frame, uri, expected_rows):
            value = 10 if failure == "schema_readback" else "TAMPERED"
            write(spark, frame.withColumn("FORMA_PAGAMENTO", F.lit(value)), uri, expected_rows)

        monkeypatch.setattr(E, "escreve_tabela", corrupt)
    with pytest.raises(ValueError):
        E._stage_and_publish(
            spark,
            str(tmp_path / "final"),
            lambda staging: E._write_ccb_classification_evidence(
                spark,
                config,
                "ccb_pppre",
                roots,
                mapping,
                fator_k=1,
                output_base=staging,
                output_uri=str(tmp_path / "final"),
            ),
        )
    assert published == []
    assert not (tmp_path / "final").exists()
    assert spark.sparkContext._jsc.getPersistentRDDs().size() == baseline


@pytest.fixture
def clone_job(spark, source_inputs, tmp_path, monkeypatch):
    config, _ = source_inputs
    config.update(
        DATAGEN_SPECS_URI="spec.json",
        DATAGEN_OUTPUT_URI=str(tmp_path / "output"),
        DATAGEN_SYNTHETIC_BASE_URI=str(tmp_path / "synthetic"),
    )
    root = spark.createDataFrame(
        [(10, 54, "ORIGINAL")], "NUM_IF long, NUM_TIPO_IF long, COD_IF string"
    )
    profile = E.get_product_profile("ccb_pppre")
    profile = replace(
        profile,
        integrity=E.IntegrityPolicy(),
        business_keys=replace(profile.business_keys, operation=None),
    )
    spec = {E.TABELA_RAIZ: {"pk_cols": ["NUM_IF"], "foreign_keys": [], "static": False}}
    monkeypatch.setitem(E.TABELAS_ENGORDA_POR_PRODUTO, profile.name, (E.TABELA_RAIZ,))
    monkeypatch.setattr(
        E,
        "monta_plano",
        lambda *args, **kwargs: {
            E.TABELA_RAIZ: E.PlanoTabela(
                E.TABELA_RAIZ, ("NUM_IF",), pk_regra="OFFSET_PROPRIO", pk_start=100
            )
        },
    )
    monkeypatch.setattr(E, "seleciona_instrumentos", lambda *args, **kwargs: [10])
    monkeypatch.setattr(E, "_deriva_tipo_oracle", lambda *args: 54)
    monkeypatch.setattr(E, "calcula_lotes", lambda *args, **kwargs: {E.TABELA_RAIZ: root})
    monkeypatch.setattr(E, "loga_chaves_amostra", lambda *args: None)
    monkeypatch.setattr(E, "_loga_contagens_dominio", lambda *args: None)
    return (
        config,
        spec,
        {
            "product_profile": profile,
            "num_ifs": [10],
            "fator_k": 2,
            "no_oracle": True,
            "engorda_ts": datetime(2026, 9, 11),
            "controle_operacional_date": date(2026, 9, 11),
        },
    )


def test_plan_roundtrip_preserves_descriptor_and_materializes_without_raw(
    spark, clone_job, tmp_path, monkeypatch
):
    config, spec, kwargs = clone_job
    plan_uri = str(tmp_path / "plan.json")
    plan = E.executa_clonagem(spark, config, spec, phase="plan", plan_uri=plan_uri, **kwargs)[
        "plan"
    ]
    assert E._validate_plan_artifact(plan) == plan
    assert plan["ccb_classification"]["uri"]
    assert plan["selected_lote"]["table_set"] == [E.TABELA_RAIZ]
    assert list(plan["tables"]) == [E.TABELA_RAIZ]
    tampered = {**plan, "ccb_classification": {**plan["ccb_classification"], "sha256": "f" * 64}}
    with pytest.raises(ValueError, match="plan_id"):
        E._validate_plan_artifact(tampered)
    old_body = {
        key: value for key, value in plan.items() if key not in {"plan_id", "ccb_classification"}
    }
    assert E._validate_plan_artifact({**old_body, "plan_id": E._plan_id(old_body)})
    lots, missing, counts = E._load_selected_lote_snapshot(
        spark,
        plan_uri,
        plan["selected_lote"],
        expected_tables={E.TABELA_RAIZ},
        selected_num_ifs=[10],
        selective_keys=frozenset(),
    )
    reservation = {
        "artifact_type": E.ENGORDA_RESERVATION_ARTIFACT,
        "schema_version": E.ENGORDA_RESERVATION_SCHEMA_VERSION,
        "product": plan["product"],
        "plan_id": plan["plan_id"],
        "table_pks": {E.TABELA_RAIZ: {"start": 100, "end": 101, "count": 2, "step": 1}},
        "cod_operacao": {"strategy": "oracle_allocator", "count": 0},
        "meu_numero": {
            "strategy": E.MEU_NUMERO_GROUPED_STRATEGY,
            "operational_date": "2026-09-11",
            "group_ids": [],
            "prefix": None,
            "count": 0,
            "start": None,
            "end": None,
        },
    }
    monkeypatch.setattr(E, "raw_path", lambda *args: pytest.fail("materialize read RAW"))
    kwargs.pop("num_ifs")
    stats = E.executa_clonagem(
        spark,
        config,
        spec,
        phase="materialize",
        planned_artifact=plan,
        reservation=reservation,
        snapshot_lotes=lots,
        snapshot_faltantes=missing,
        snapshot_lote_counts=counts,
        **kwargs,
    )
    assert list(stats) == [E.TABELA_RAIZ]
    manifest = json.loads((tmp_path / "output" / E.CCB_CLASSIFICATION_MANIFEST).read_text())
    assert manifest["source"] == plan["ccb_classification"]
    assert manifest["plan_id"] == plan["plan_id"]
    assert manifest["generated"]["row_count"] == 2
    assert (
        json.loads((tmp_path / "output" / E.OFFLINE_ARTIFACT_MARKER).read_text())["plan_id"]
        == plan["plan_id"]
    )


def test_source_readback_tampering_prevents_plan_commit(spark, clone_job, tmp_path, monkeypatch):
    config, spec, kwargs = clone_job
    write = E.escreve_tabela

    def corrupt(spark, frame, uri, expected_rows):
        write(spark, frame.withColumn("FORMA_PAGAMENTO", F.lit("TAMPERED")), uri, expected_rows)

    monkeypatch.setattr(E, "escreve_tabela", corrupt)
    with pytest.raises(ValueError, match="readback.*hash"):
        E.executa_clonagem(
            spark, config, spec, phase="plan", plan_uri=str(tmp_path / "plan.json"), **kwargs
        )
    assert not (tmp_path / "plan.json").exists()


@pytest.mark.parametrize("dry_run", [True, False])
def test_phase_all_orchestration_keeps_evidence_outside_inventory(
    spark, clone_job, tmp_path, dry_run
):
    config, spec, kwargs = clone_job
    kwargs["no_oracle"] = not dry_run
    stats = E.executa_clonagem(spark, config, spec, dry_run=dry_run, **kwargs)
    assert list(stats) == [E.TABELA_RAIZ]
    output = tmp_path / "output"
    if dry_run:
        assert not output.exists()
    else:
        manifest = json.loads((output / E.CCB_CLASSIFICATION_MANIFEST).read_text())
        assert manifest["plan_id"] is None
        assert manifest["source"]["uri"] is None
        assert manifest["generated"]["row_count"] == 2
