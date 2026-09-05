import hashlib
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import profile_cdb_shapes as profiler  # noqa: E402
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-product-shapes-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def baseline_identity(**overrides):
    baseline = {
        "schema_version": 2,
        "product": "cdb_simplificado",
        "num_tipo_if": 49,
        "domain_version": validator.BASELINE_DOMAIN_VERSION,
        "metric_version": validator.BASELINE_METRIC_VERSION,
        "metrics": validator.DEFAULT_SHAPE_METRICS,
        "map_mode": "exact-source-keys",
        "source_key_count": 2,
        "source_key_fingerprint": "abc123",
    }
    baseline.update(overrides)
    return baseline


def operation_shape_tables(spark, num_tipo_if=49, dado_rows=2, lancamento_rows=1):
    operation_data = [
        (101, 502, "380.485.158-49"),
        (101, 503, "PF"),
    ][:dado_rows]
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, num_tipo_if, None)],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string",
        ),
        "OPERACAO": spark.createDataFrame([(101, 1)], "NUM_ID_OPERACAO long, NUM_IF long"),
        "DADO_OPERACAO": spark.createDataFrame(
            operation_data,
            "NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long, VAL_DADO_ATUAL string",
        ),
        "LANCAMENTO": spark.createDataFrame([(101,)] * lancamento_rows, "NUM_ID_OPERACAO long"),
        "CONDICAO_IF": spark.createDataFrame([(11, 1)], "NUM_CONDICAO_IF long, NUM_IF long"),
        "RESGATE": spark.createDataFrame([(11,)], "NUM_CONDICAO_IF long"),
    }


def test_profiler_cli_requires_explicit_product(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["profile_cdb_shapes.py"])
    with pytest.raises(SystemExit):
        profiler.parse_args()

    monkeypatch.setattr(sys, "argv", ["profile_cdb_shapes.py", "--product", "rdb"])
    assert profiler.parse_args().product == "rdb"


def test_profiler_emits_schema_v2_product_and_metric_tags(spark):
    root = spark.createDataFrame(
        [(1, 50, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
    )

    profile = profiler.build_profile(
        {"INSTRUMENTO_FINANCEIRO": root},
        product="rdb",
        num_tipo_if=50,
        simplified=False,
    )

    assert profile["schema_version"] == profiler.BASELINE_SCHEMA_VERSION == 2
    assert profile["product"] == "rdb"
    assert profile["num_tipo_if"] == 50
    assert profile["domain_version"] == profiler.DOMAIN_VERSION
    assert profile["metric_version"] == profiler.METRIC_VERSION
    assert profile["metrics"] == [metric.name for metric in profiler.METRICS]


def test_source_key_fingerprint_is_order_independent_and_deduplicated(spark):
    first = spark.createDataFrame([(3,), (1,), (2,), (2,)], "NUM_IF long").dropDuplicates()
    second = spark.createDataFrame([(2,), (3,), (1,)], "NUM_IF long")
    expected = hashlib.sha256(b"1,2,3").hexdigest()

    assert profiler.source_key_provenance(first) == (3, expected)
    assert profiler.source_key_provenance(second) == (3, expected)


@pytest.mark.parametrize(
    ("baseline", "profile_name", "message"),
    [
        ({}, "rdb", "legacy untagged baseline"),
        ({"schema_version": 1}, "cdb_simplificado", "schema_version=1"),
        ({"schema_version": 3}, "cdb_simplificado", "schema_version=3"),
        ({"schema_version": 2, "product": "cdb"}, "rdb", "product 'cdb'"),
        ({"schema_version": 2, "num_tipo_if": 49}, "rdb", "num_tipo_if=49"),
    ],
)
def test_baseline_identity_rejects_wrong_schema_product_or_type(baseline, profile_name, message):
    incompatibility = validator._baseline_incompatibility(
        baseline, validator.VALIDATION_PROFILES[profile_name]
    )

    assert message in incompatibility


def test_legacy_baseline_is_only_tolerated_for_simplificado():
    assert (
        validator._baseline_incompatibility({}, validator.VALIDATION_PROFILES["cdb_simplificado"])
        is None
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"domain_version": 999}, "domain_version=999"),
        ({"metric_version": 999}, "metric_version=999"),
        ({"metrics": ["OPERACAO"]}, "metrics do not match"),
        ({"map_mode": "population"}, "map_mode='population'"),
        ({"source_key_count": 3}, "source_key_count=3"),
        ({"source_key_fingerprint": "wrong"}, "source_key_fingerprint"),
    ],
)
def test_baseline_identity_rejects_version_metric_and_provenance_mismatches(overrides, message):
    current = {
        "map_mode": "exact-source-keys",
        "source_key_count": 2,
        "source_key_fingerprint": "abc123",
    }

    incompatibility = validator._baseline_incompatibility(
        baseline_identity(**overrides),
        validator.VALIDATION_PROFILES["cdb_simplificado"],
        current,
    )

    assert message in incompatibility


def test_rdb_without_shape_evidence_skips_before_touching_spark():
    findings = validator.check_shapes(
        None,
        {},
        None,
        sample=5,
        unseen_tol_pct=1.0,
        drift_tol=0.15,
        op_ratio_tol_pct=5.0,
        profile=validator.VALIDATION_PROFILES["rdb"],
    )

    assert len(findings) == 1
    assert findings[0].passed
    assert "No shape rules enabled" in findings[0].message


@pytest.mark.parametrize(("dado_rows", "expected_pass"), [(2, True), (1, False)])
def test_full_cdb_enforces_operation_closure_and_single_resgate_parent(
    spark, dado_rows, expected_pass
):
    findings = by_id(
        validator.check_shapes(
            spark,
            operation_shape_tables(spark, dado_rows=dado_rows),
            None,
            sample=5,
            unseen_tol_pct=1.0,
            drift_tol=0.15,
            op_ratio_tol_pct=5.0,
            profile=validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert findings["7c.op_ratio"].passed is expected_pass
    assert findings["7d.resgate_multiplicity"].passed
    assert "7.baseline" not in findings


def test_cdb_operation_closure_rejects_children_compensated_between_operations(spark):
    tables = operation_shape_tables(spark)
    tables["OPERACAO"] = spark.createDataFrame(
        [(101, 1), (102, 1)], "NUM_ID_OPERACAO long, NUM_IF long"
    )
    tables["DADO_OPERACAO"] = spark.createDataFrame(
        [
            (101, 502, "380.485.158-49"),
            (101, 503, "PF"),
            (101, 502, "380.485.158-49"),
            (101, 503, "PF"),
        ],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long, VAL_DADO_ATUAL string",
    )
    tables["LANCAMENTO"] = spark.createDataFrame([(101,), (102,)], "NUM_ID_OPERACAO long")

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert not findings["7c.op_ratio"].passed
    assert findings["7c.op_ratio"].count == 2


def test_cdb_operation_closure_accepts_multiple_complete_operations(spark):
    tables = operation_shape_tables(spark)
    tables["OPERACAO"] = spark.createDataFrame(
        [(101, 1), (102, 1)], "NUM_ID_OPERACAO long, NUM_IF long"
    )
    tables["DADO_OPERACAO"] = spark.createDataFrame(
        [
            (101, 502, "380.485.158-49"),
            (101, 503, "PF"),
            (102, 502, "158.521.262-84"),
            (102, 503, "PF"),
        ],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long, VAL_DADO_ATUAL string",
    )
    tables["LANCAMENTO"] = spark.createDataFrame([(101,), (102,)], "NUM_ID_OPERACAO long")

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert findings["7c.op_ratio"].passed
    assert findings["7c.op_ratio"].count == 0


def test_cdb_operation_data_rejects_invalid_observed_pf_profile(spark):
    tables = operation_shape_tables(spark)
    tables["DADO_OPERACAO"] = spark.createDataFrame(
        [(101, 501, "380.485.158-49"), (101, 503, "PF")],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long, VAL_DADO_ATUAL string",
    )

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert findings["7c.op_ratio"].passed
    assert not findings["7c.op_data_profile"].passed
    assert findings["7c.op_data_profile"].count == 1


def test_cdb_operation_data_leaves_pj_profile_unvalidated(spark):
    tables = operation_shape_tables(spark)
    tables["DADO_OPERACAO"] = spark.createDataFrame(
        [(101, 502, "12.345.678/0001-90"), (101, 503, "PJ")],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long, VAL_DADO_ATUAL string",
    )

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert findings["7c.op_ratio"].passed
    assert findings["7c.op_data_nature_unvalidated"].severity == validator.SEV_WARN
    assert findings["7c.op_data_nature_unvalidated"].count == 1


def test_full_cdb_rejects_multiple_resgate_parents(spark):
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 49, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1), (12, 1)], "NUM_CONDICAO_IF long, NUM_IF long"
        ),
        "RESGATE": spark.createDataFrame([(11,), (12,)], "NUM_CONDICAO_IF long"),
    }

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb"],
        )
    )

    assert findings["7d.resgate_multiplicity"].severity == validator.SEV_ERROR
    assert findings["7d.resgate_multiplicity"].count == 1


def test_simplificado_enforces_resgate_multiplicity_and_requires_baseline(spark):
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 49, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1), (12, 1)], "NUM_CONDICAO_IF long, NUM_IF long"
        ),
        "RESGATE": spark.createDataFrame([(11,), (12,)], "NUM_CONDICAO_IF long"),
    }

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb_simplificado"],
        )
    )

    assert findings["7d.resgate_multiplicity"].severity == validator.SEV_ERROR
    assert findings["7.baseline"].severity == validator.SEV_WARN


def test_supplied_cross_product_baseline_is_rejected(spark, tmp_path):
    baseline_path = tmp_path / "cdb-baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "product": "cdb",
                "num_tipo_if": 49,
                "filtros_fonte_applied": True,
                "shapes": [{"shape": "OPERACAO=0", "pct": 100.0}],
            }
        )
    )
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 50, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
        )
    }

    findings = by_id(
        validator.check_shapes(
            spark,
            tables,
            str(baseline_path),
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["rdb"],
        )
    )

    assert findings["7.baseline_incompatible"].severity == validator.SEV_ERROR


def test_unreadable_baseline_fails_before_shape_counts(spark, tmp_path):
    baseline_path = tmp_path / "broken.json"
    baseline_path.write_text("not-json")
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 49, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
        )
    }

    findings = validator.check_shapes(
        spark,
        tables,
        str(baseline_path),
        5,
        1.0,
        0.15,
        5.0,
        validator.VALIDATION_PROFILES["cdb_simplificado"],
    )

    assert len(findings) == 1
    assert findings[0].check_id == "7.baseline"
    assert findings[0].severity == validator.SEV_ERROR


def test_fine_grained_shape_skip_omits_resgate_check(spark):
    findings = by_id(
        validator.check_shapes(
            spark,
            operation_shape_tables(spark),
            None,
            5,
            1.0,
            0.15,
            5.0,
            validator.VALIDATION_PROFILES["cdb_simplificado"],
            ["7d.resgate_multiplicity"],
        )
    )

    assert "7d.resgate_multiplicity" not in findings


def test_fully_skipped_lookup_combo_avoids_frame_access():
    assert (
        validator.check_lookup_combo_frames(
            None,
            None,
            None,
            None,
            5,
            validator.VALIDATION_PROFILES["cdb_simplificado"],
            skip_prefixes=["6.combo"],
        )
        == []
    )
