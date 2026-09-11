"""Consumer contract tests using real Parquet and small independently hashed fixtures."""

import ast
import hashlib
import inspect
import json
import shutil
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scripts import validate_products as validator

SOURCE = ("NUM_IF", "RENT_INDEXADOR_TAXA_FLU", "FORMA_PAGAMENTO")
OUTPUT = ("NUM_IF_ORIG", "K", "NUM_IF", "RENT_INDEXADOR_TAXA_FLU", "FORMA_PAGAMENTO")
SCHEMA = (
    "NUM_IF_ORIG string, K long, NUM_IF string, "
    "RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string"
)
ROWS = [
    ("1", 1, "101", "PREFIXADO", "PAGAMENTO DE PARCELAS"),
    ("1", 2, "102", "PREFIXADO", "PAGAMENTO DE PARCELAS"),
    ("2", 1, "201", "PREFIXADO", "PAGAMENTO DE PARCELAS FIXAS"),
    ("2", 2, "202", "PREFIXADO", "PAGAMENTO DE PARCELAS FIXAS"),
]
PROFILE = validator.VALIDATION_PROFILES["ccb"]


def prototype_fingerprint(rows, columns):
    buckets = {}
    for row in rows:
        payload = json.dumps(
            dict(zip(columns, (None if value is None else str(value) for value in row))),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % 256
        summary = buckets.setdefault(
            bucket, {"bucket": bucket, "count": 0, **{f"s{i}": 0 for i in range(4)}}
        )
        summary["count"] += 1
        for index in range(4):
            summary[f"s{index}"] += int(digest[index * 16 : (index + 1) * 16], 16)
    summaries = [
        {**buckets[bucket], **{f"s{i}": str(buckets[bucket][f"s{i}"]) for i in range(4)}}
        for bucket in sorted(buckets)
    ]
    payload = json.dumps(summaries, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return {"row_count": len(rows), "sha256": hashlib.sha256(payload.encode()).hexdigest()}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("validate-ccb-evidence-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    yield session
    session.stop()


def manifest_for(base, rows=ROWS):
    source_rows = sorted(set((row[0], row[3], row[4]) for row in rows))
    return {
        "artifact_type": "ccb_classification_evidence",
        "schema_version": 1,
        "product": "ccb_pppre",
        "output_uri": str(base),
        "plan_id": "a" * 64,
        "fator_k": 2,
        "source": {
            "artifact_type": "ccb_classification_source",
            "schema_version": 1,
            "hash_algorithm": "sha256-bucket-sums-v1",
            "uri": "/never-read-source-snapshot",
            **prototype_fingerprint(source_rows, SOURCE),
        },
        "generated": prototype_fingerprint(rows, OUTPUT),
    }


@pytest.fixture(scope="module")
def seed(spark, tmp_path_factory):
    base = tmp_path_factory.mktemp("ccb-seed")
    spark.createDataFrame(ROWS, SCHEMA).write.parquet(str(base / "_CCB_CLASSIFICATION"))
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(Decimal(row[2]), 53, None, None) for row in ROWS],
            "NUM_IF decimal(20,2), NUM_TIPO_IF long, NUM_IF_PERTENCE long, DAT_EXCLUSAO string",
        ),
        "MAPA_CLONE_NUM_IF": spark.createDataFrame(
            [(Decimal(row[0]), row[1], Decimal(row[2])) for row in ROWS],
            "NUM_IF_ORIG decimal(20,2), K long, NUM_IF_NOVO decimal(20,2)",
        ),
        "OPERACAO": spark.createDataFrame(
            [(row[2], "871", "43") for row in ROWS],
            "NUM_IF string, NUM_ID_TIPO_OPER_OBJETO_SERV string, COD_SITUACAO_OPERACAO string",
        ),
        "HISTORICO_PU_CURVA": spark.createDataFrame([(row[2],) for row in ROWS], "NUM_IF string"),
    }
    return base, tables


@pytest.fixture
def artifact(seed, tmp_path):
    base = tmp_path / "output"
    shutil.copytree(seed[0], base)
    manifest = manifest_for(base)
    (base / "_CCB_CLASSIFICATION.json").write_text(json.dumps(manifest))
    return base, dict(seed[1]), manifest


def load(spark, artifact):
    base, tables, manifest = artifact
    (base / "_CCB_CLASSIFICATION.json").write_text(json.dumps(manifest))
    return validator.load_ccb_classification_evidence(spark, str(base), tables)


def run(spark, artifact):
    base, tables, _ = artifact
    return {
        finding.check_id: finding
        for finding in validator.check_osias_with_ccb_evidence(
            spark, str(base), tables, 5, PROFILE, True
        )
    }


@pytest.mark.parametrize(
    "columns,rows,schema",
    [
        (OUTPUT, ROWS, SCHEMA),
        (
            SOURCE,
            [("1", None, 'quoted "\\\n'), ("2", " vcp ", "liquida\u00e7\u00e3o")],
            "NUM_IF string, RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string",
        ),
        (SOURCE, [], "NUM_IF string, RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string"),
    ],
)
def test_fingerprint_exact_prototype_and_bounded_collection(
    spark, monkeypatch, columns, rows, schema
):
    original_collect = DataFrame.collect

    def bounded_collect(frame):
        assert frame.columns == ["bucket", "count", "s0", "s1", "s2", "s3"]
        result = original_collect(frame)
        assert len(result) <= 256
        return result

    monkeypatch.setattr(DataFrame, "collect", bounded_collect)
    frame = spark.createDataFrame(rows, schema)
    assert validator._ccb_classification_fingerprint(frame, columns) == prototype_fingerprint(
        rows, columns
    )
    assert validator._ccb_classification_fingerprint(
        frame.repartition(3), columns
    ) == prototype_fingerprint(rows, columns)
    assert validator._ccb_classification_fingerprint(
        frame.unionByName(frame), columns
    ) == prototype_fingerprint(rows * 2, columns)


def test_fingerprint_producer_parity(spark):
    from datagen import engorda_tables as producer

    frame = spark.createDataFrame(ROWS, SCHEMA)
    assert producer._ccb_classification_fingerprint(
        frame, OUTPUT
    ) == validator._ccb_classification_fingerprint(frame, OUTPUT)
    source = frame.select(F.col("NUM_IF_ORIG").alias("NUM_IF"), *SOURCE[1:]).distinct()
    assert producer._ccb_classification_fingerprint(
        source, SOURCE
    ) == validator._ccb_classification_fingerprint(source, SOURCE)


def test_producer_written_parquet_roundtrip(spark, seed, tmp_path):
    from datagen import engorda_tables as producer

    source = (
        spark.createDataFrame(ROWS, SCHEMA)
        .select(F.col("NUM_IF_ORIG").alias("NUM_IF"), *SOURCE[1:])
        .distinct()
    )
    snapshot = tmp_path / "frozen-source"
    source.write.parquet(str(snapshot))
    base = tmp_path / "published"
    manifest = manifest_for(base)
    manifest["source"]["uri"] = str(snapshot)
    mapping = seed[1]["MAPA_CLONE_NUM_IF"].select(
        F.col("NUM_IF_ORIG").alias("old_NUM_IF"),
        F.col("K").alias(producer.K_COL),
        F.col("NUM_IF_NOVO").alias("new_NUM_IF"),
    )
    producer._write_ccb_classification_evidence(
        spark,
        {},
        "ccb_pppre",
        source.select("NUM_IF"),
        mapping,
        fator_k=2,
        output_base=str(base),
        output_uri=str(base),
        plan={"plan_id": manifest["plan_id"], "ccb_classification": manifest["source"]},
    )
    shutil.rmtree(snapshot)
    projection = validator.load_ccb_classification_evidence(spark, str(base), seed[1])
    assert projection.columns == list(SOURCE)
    assert projection.count() == 4
    assert all(
        finding.passed
        for finding in validator.check_osias_with_ccb_evidence(
            spark, str(base), seed[1], 5, PROFILE, True
        )
    )


def test_valid_evidence_returns_only_classifiers_without_reading_source(
    spark, artifact, monkeypatch
):
    original_read = validator.read_text
    reads = []

    def read_text(session, path):
        reads.append(path)
        return original_read(session, path)

    monkeypatch.setattr(validator, "read_text", read_text)
    projection = load(spark, artifact)
    assert projection.dtypes == [(name, "string") for name in SOURCE]
    assert projection.count() == 4
    assert reads == [str(artifact[0] / "_CCB_CLASSIFICATION.json")]
    assert "ACTPCCB_CONDICAO_IF" not in artifact[1]
    assert "_CCB_CLASSIFICATION" not in artifact[1]


def test_file_uri_input_reads_manifest_and_offline_marker(spark, artifact):
    base, tables, manifest = artifact
    (base / "_DATAGEN_OFFLINE.json").write_text(
        json.dumps({"product": manifest["product"], "plan_id": manifest["plan_id"]})
    )
    projection = validator.load_ccb_classification_evidence(spark, base.as_uri(), tables)
    assert projection.columns == list(SOURCE)
    assert projection.count() == 4


@pytest.mark.parametrize(
    "product", ["ccb_pppre", "ccb_pfpre", "ccb_pgrpre", "ccb_favcp", "ccb_fapre"]
)
def test_known_product_and_direct_all_manifest(spark, artifact, product):
    base, _, manifest = artifact
    manifest.update(product=product, plan_id=None, output_uri=base.as_uri() + "/")
    manifest["source"]["uri"] = None
    (base / "_DATAGEN_OFFLINE.json").write_text(json.dumps({"product": product, "plan_id": None}))
    assert load(spark, artifact).count() == 4


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("artifact_type", "other", "artifact_type"),
        ("schema_version", 2, "schema_version"),
        ("schema_version", True, "schema_version"),
        ("product", "ccb", "product"),
        ("product", "lci", "product"),
        ("output_uri", "/stale-output", "output_uri"),
        ("output_uri", "", "output_uri"),
        ("plan_id", "not-a-hash", "plan_id"),
        ("fator_k", 0, "fator_k"),
        ("fator_k", -1, "fator_k"),
        ("fator_k", True, "fator_k"),
        ("fator_k", 1.5, "fator_k"),
        ("source", None, "artifact_type"),
        ("source.artifact_type", "other", "artifact_type"),
        ("source.schema_version", 3, "schema_version"),
        ("source.hash_algorithm", "sha256", "hash_algorithm"),
        ("source.uri", None, "uri"),
        ("source.uri", " ", "uri"),
        ("source.row_count", 0, "row_count"),
        ("source.row_count", True, "row_count"),
        ("source.sha256", "A" * 64, "sha256"),
        ("generated", [], "row_count"),
        ("generated.row_count", -1, "row_count"),
        ("generated.row_count", 5, "count"),
        ("generated.sha256", "123", "sha256"),
    ],
)
def test_manifest_rejects_invalid_fields(spark, artifact, field, value, error):
    target = artifact[2]
    parts = field.split(".")
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = value
    with pytest.raises(ValueError, match=error):
        load(spark, artifact)


@pytest.mark.parametrize("field", ["plan_id", "source.uri"])
def test_missing_nullable_manifest_fields_are_not_direct_all(spark, artifact, field):
    if field == "plan_id":
        del artifact[2][field]
    else:
        del artifact[2]["source"]["uri"]
    with pytest.raises(ValueError, match=field.split(".")[-1]):
        load(spark, artifact)


@pytest.mark.parametrize("payload", ["not json", "[]", "null"])
def test_malformed_manifest(spark, artifact, payload):
    (artifact[0] / "_CCB_CLASSIFICATION.json").write_text(payload)
    findings = run(spark, artifact)
    assert not findings["9.osias.ccb.classification_evidence"].passed
    assert findings["9.osias.ccb.pu_curve_history"].passed


@pytest.mark.parametrize(
    "marker",
    [
        {"product": "ccb_pfpre", "plan_id": "a" * 64},
        {"product": "ccb_pppre", "plan_id": "b" * 64},
        {"product": "ccb_pppre"},
        [],
    ],
)
def test_offline_marker_product_and_plan_parity(spark, artifact, marker):
    (artifact[0] / "_DATAGEN_OFFLINE.json").write_text(json.dumps(marker))
    with pytest.raises(ValueError, match="offline marker"):
        load(spark, artifact)


@pytest.mark.parametrize("part", ["source", "generated"])
def test_digest_tampering(spark, artifact, part):
    artifact[2][part]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match=f"{part}.*fingerprint"):
        load(spark, artifact)


@pytest.mark.parametrize("change", ["missing", "extra", "order", "key_type", "k_type"])
def test_parquet_schema_contract(spark, artifact, change):
    frame = spark.createDataFrame(ROWS, SCHEMA)
    if change == "missing":
        frame = frame.drop("FORMA_PAGAMENTO")
    elif change == "extra":
        frame = frame.withColumn("EXTRA", F.lit("ignored"))
    elif change == "order":
        frame = frame.select(*reversed(OUTPUT))
    elif change == "key_type":
        frame = frame.withColumn("NUM_IF", F.col("NUM_IF").cast("long"))
    else:
        frame = frame.withColumn("K", F.col("K").cast("int"))
    frame.write.mode("overwrite").parquet(str(artifact[0] / "_CCB_CLASSIFICATION"))
    with pytest.raises(ValueError, match="columns/types"):
        load(spark, artifact)


@pytest.mark.parametrize(
    "column,value",
    [
        (0, None),
        (0, " "),
        (0, "1.00"),
        (1, None),
        (1, 0),
        (1, 3),
        (2, None),
        (2, " 101 "),
        (3, None),
        (3, " "),
        (4, None),
        (4, ""),
    ],
)
def test_invalid_evidence_values(spark, artifact, column, value):
    rows = [list(row) for row in ROWS]
    rows[0][column] = value
    spark.createDataFrame(rows, SCHEMA).write.mode("overwrite").parquet(
        str(artifact[0] / "_CCB_CLASSIFICATION")
    )
    with pytest.raises(ValueError, match="blank/null.*K out of bounds"):
        load(spark, artifact)


@pytest.mark.parametrize(
    "change,error",
    [
        ("classification_conflict", "conflicting classifications"),
        ("duplicate_id", "duplicate key"),
        ("duplicate_pair", "duplicate key"),
        ("missing_clone", "count"),
        ("stale_original", "source projection fingerprint"),
    ],
)
def test_integrity_checks_even_with_recomputed_generated_hash(spark, artifact, change, error):
    rows = [list(row) for row in ROWS]
    if change == "classification_conflict":
        rows[1][3] = "VCP"
    elif change == "duplicate_id":
        rows[1][2] = rows[0][2]
    elif change == "duplicate_pair":
        rows[1][1] = rows[0][1]
    elif change == "missing_clone":
        rows.pop()
    else:
        rows[0][0] = rows[1][0] = "99"
    artifact[2]["generated"] = prototype_fingerprint(rows, OUTPUT)
    spark.createDataFrame(rows, SCHEMA).write.mode("overwrite").parquet(
        str(artifact[0] / "_CCB_CLASSIFICATION")
    )
    with pytest.raises(ValueError, match=error):
        load(spark, artifact)


@pytest.mark.parametrize("change", ["missing", "extra", "null"])
def test_exact_output_root_coverage(spark, artifact, change):
    tables = artifact[1]
    root = tables["INSTRUMENTO_FINANCEIRO"]
    if change == "missing":
        root = root.where("NUM_IF != 101")
    else:
        root = root.unionByName(
            root.limit(1).withColumn("NUM_IF", F.lit(None if change == "null" else 999))
        )
    tables["INSTRUMENTO_FINANCEIRO"] = root
    with pytest.raises(ValueError, match="output roots"):
        load(spark, artifact)


@pytest.mark.parametrize(
    "change",
    ["missing", "extra", "duplicate", "original", "synthetic", "k", "fractional_k", "null"],
)
def test_exact_clone_map_coverage(spark, artifact, change):
    tables = artifact[1]
    mapa = tables["MAPA_CLONE_NUM_IF"]
    if change == "missing":
        mapa = mapa.where("NUM_IF_NOVO != 101")
    elif change in {"extra", "duplicate"}:
        extra = mapa.limit(1)
        if change == "extra":
            extra = extra.withColumn("NUM_IF_NOVO", F.lit(999))
        mapa = mapa.unionByName(extra)
    else:
        column = {"original": "NUM_IF_ORIG", "synthetic": "NUM_IF_NOVO"}.get(change, "K")
        value = None if change == "null" else 1.5 if change == "fractional_k" else 999
        mapa = mapa.withColumn(column, F.lit(value))
    tables["MAPA_CLONE_NUM_IF"] = mapa
    with pytest.raises(ValueError, match="MAPA_CLONE_NUM_IF"):
        load(spark, artifact)


@pytest.mark.parametrize(
    "table,column",
    [
        ("INSTRUMENTO_FINANCEIRO", None),
        ("INSTRUMENTO_FINANCEIRO", "NUM_IF"),
        ("MAPA_CLONE_NUM_IF", None),
        ("MAPA_CLONE_NUM_IF", "K"),
    ],
)
def test_required_physical_evidence(spark, artifact, table, column):
    if column:
        artifact[1][table] = artifact[1][table].drop(column)
    else:
        del artifact[1][table]
    with pytest.raises(ValueError, match="root/map columns"):
        load(spark, artifact)


def test_valid_evidence_enforces_osias_and_never_enters_inventory(spark, artifact):
    tables = artifact[1]
    originals = dict(tables)
    assert all(item.passed for item in run(spark, artifact).values())
    assert tables == originals
    tables["OPERACAO"] = (
        tables["OPERACAO"]
        .withColumn("NUM_ID_TIPO_OPER_OBJETO_SERV", F.lit("999"))
        .withColumn("COD_SITUACAO_OPERACAO", F.lit("42"))
    )
    findings = run(spark, artifact)
    assert findings["9.osias.ccb.pppre.route"].count == 2
    assert findings["9.osias.ccb.pfpre.route"].count == 2
    assert findings["9.osias.ccb.pppre.operation_status"].count == 2
    assert findings["9.osias.ccb.pu_curve_history"].passed


@pytest.mark.parametrize("present", ["neither", "parquet", "manifest", "corrupt_parquet"])
def test_fallback_only_when_both_parts_absent_and_history_is_independent(spark, artifact, present):
    base, tables, _ = artifact
    tables["ACTPCCB_CONDICAO_IF"] = spark.createDataFrame(
        [(row[2], row[3], row[4]) for row in ROWS],
        "NUM_IF string, RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string",
    )
    tables["HISTORICO_PU_CURVA"] = tables["HISTORICO_PU_CURVA"].limit(0)
    if present in {"neither", "manifest", "corrupt_parquet"}:
        shutil.rmtree(base / "_CCB_CLASSIFICATION")
    if present in {"neither", "parquet"}:
        (base / "_CCB_CLASSIFICATION.json").unlink()
    if present == "corrupt_parquet":
        (base / "_CCB_CLASSIFICATION").mkdir()
        (base / "_CCB_CLASSIFICATION" / "part-00000.parquet").write_text("corrupt parquet")
    findings = run(spark, artifact)
    assert findings["9.osias.ccb.pu_curve_history"].count == 4
    if present == "neither":
        assert findings["9.osias.ccb.scenario"].passed
    else:
        assert findings["9.osias.ccb.classification_evidence"].severity == "ERROR"
        assert "9.osias.ccb.scenario" not in findings


def test_no_sidecar_and_no_actual_table_gives_regeneration_guidance(spark, tmp_path, seed):
    findings = validator.check_osias_with_ccb_evidence(
        spark, str(tmp_path), seed[1], 5, PROFILE, True
    )
    availability = next(item for item in findings if item.check_id.endswith(".availability"))
    assert availability.severity == "ERROR"
    assert "Regenerate" in availability.hint


@pytest.mark.parametrize(
    "classification,conflict",
    [
        (("PREFIXADO", "PAGAMENTO DE PARCELAS"), False),
        ((" prefixado ", "PAGAMENTO DE PARCELAS"), True),
        (("VCP", "PAGAMENTO DE RENDIMENTO PREFIXADO"), True),
        ((None, "PAGAMENTO DE PARCELAS"), True),
    ],
)
def test_actual_table_conflicts_including_same_normalized_scenario(
    spark, seed, classification, conflict
):
    tables = dict(seed[1])
    rows = [(row[2], row[3], row[4]) for row in ROWS] + [("101", *classification)]
    tables["ACTPCCB_CONDICAO_IF"] = spark.createDataFrame(
        rows, "NUM_IF string, RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string"
    )
    findings = {item.check_id: item for item in validator.check_osias(tables, 5, PROFILE, True)}
    assert findings["9.osias.ccb.scenario"].count == int(conflict)
    assert findings["9.osias.ccb.pu_curve_history"].passed


@pytest.mark.parametrize("explicit", [False, True])
def test_reserved_directory_excluded_from_discovery_load_and_inventory(spark, artifact, explicit):
    base = artifact[0]
    for name in ("BUSINESS", "_CCB_CLASSIFICATION_EXTRA"):
        spark.range(1).write.parquet(str(base / name))
    assert validator.list_table_dirs(spark, str(base)) == ["BUSINESS", "_CCB_CLASSIFICATION_EXTRA"]
    tables = validator.read_synthetic_tables(
        spark,
        str(base),
        ["BUSINESS", "_CCB_CLASSIFICATION", "_CCB_CLASSIFICATION_EXTRA"] if explicit else None,
    )
    assert set(tables) == {"BUSINESS", "_CCB_CLASSIFICATION_EXTRA"}
    for frame in tables.values():
        frame.unpersist()


@pytest.mark.parametrize("product,enabled", [("ccb", False), ("lci", True), ("cdb", True)])
def test_disabled_and_nonccb_do_not_touch_sidecar(monkeypatch, product, enabled):
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected sidecar IO")

    monkeypatch.setattr(validator, "load_ccb_classification_evidence", forbidden)
    validator.check_osias_with_ccb_evidence(
        None, "/unread", {}, 5, validator.VALIDATION_PROFILES[product], enabled
    )


@pytest.mark.parametrize("skip", ["9", "9.osias.", "9.osias.ccb."])
def test_skipped_osias_group_does_not_touch_sidecar(monkeypatch, skip):
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected sidecar IO")

    monkeypatch.setattr(validator, "load_ccb_classification_evidence", forbidden)
    assert (
        validator._run_check_group(
            "Osias",
            ("9.osias.ccb.",),
            [skip],
            lambda: validator.check_osias_with_ccb_evidence(None, "/unread", {}, 5, PROFILE, True),
        )
        == []
    )


def test_standalone_runtime_has_no_producer_or_shared_import():
    tree = ast.parse(inspect.getsource(validator))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = (
                [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else [item.name for item in node.names]
            )
            assert not any("engorda" in name or "datagen" in name for name in names)
    fingerprint = inspect.getsource(validator._ccb_classification_fingerprint)
    assert "collect_list" not in fingerprint
    assert "toLocalIterator" not in inspect.getsource(validator.load_ccb_classification_evidence)
