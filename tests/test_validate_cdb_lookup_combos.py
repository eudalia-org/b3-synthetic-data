import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_cdb_simplificado as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-cdb-lookup-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def frames(spark, operations, tos, sic, tipos):
    op_df = spark.createDataFrame(
        operations,
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
        "NUM_ID_MODALIDADE_LIQUIDACAO long",
    )
    tos_df = spark.createDataFrame(
        tos,
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_TIPO_OPERACAO long, "
        "NUM_ID_OBJETO_SERVICO long, IND_DISPONIVEL_IDENTIFICACAO string",
    )
    sic_df = spark.createDataFrame(
        sic,
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )
    tipo_df = spark.createDataFrame(
        tipos,
        "NUM_ID_TIPO_OPERACAO long, IND_SEM_MODALIDADE_INFOHUB string",
    )
    return op_df, tos_df, sic_df, tipo_df


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def assert_failed_have_hints(findings):
    assert all(finding.hint.strip() for finding in findings if not finding.passed)


def test_all_valid_operation(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6)],
        tos=[(100, 10, 44, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )

    findings = validator.check_lookup_combo_frames(*inputs, sample=5)

    assert all(finding.passed for finding in findings)
    assert all(finding.severity == validator.SEV_INFO for finding in findings)


def test_missing_target_tos_is_structural_error_without_cascades(spark):
    inputs = frames(
        spark,
        operations=[(1, 999, 6)],
        tos=[],
        sic=[(100, 49, 44)],
        tipos=[(10, "N")],
    )

    findings = by_id(validator.check_lookup_combo_frames(*inputs, sample=5))

    assert findings["6.combo.tos_fk"].severity == validator.SEV_ERROR
    assert findings["6.combo.tos_fk"].count == 1
    assert_failed_have_hints(findings.values())
    for check_id in (
        "6.combo.cdb_compatibility",
        "6.combo.sem_modalidade",
        "6.combo.identification_availability",
    ):
        assert findings[check_id].passed
        assert findings[check_id].count == 0


def test_exact_mapping_is_required_even_when_operation_type_is_valid(spark):
    inputs = frames(
        spark,
        operations=[(1, 101, 1)],
        tos=[(100, 10, 44, "S"), (101, 10, 45, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )

    findings = by_id(validator.check_lookup_combo_frames(*inputs, sample=5))

    assert findings["6.combo.tos_fk"].passed
    assert findings["6.combo.cdb_compatibility"].severity == validator.SEV_WARN
    assert findings["6.combo.cdb_compatibility"].count == 1
    assert_failed_have_hints(findings.values())


def test_sem_modalidade_n_and_null_flags_warn(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6), (2, 101, 16)],
        tos=[(100, 10, 44, "S"), (101, 11, 44, "S")],
        sic=[(100, 49, 44), (101, 49, 44)],
        tipos=[(10, "N"), (11, None)],
    )

    finding = by_id(
        validator.check_lookup_combo_frames(*inputs, sample=5)
    )["6.combo.sem_modalidade"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 2
    assert_failed_have_hints([finding])


def test_unavailable_and_null_identification_flags_warn(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 1), (2, 101, 1)],
        tos=[(100, 10, 44, "N"), (101, 10, 44, None)],
        sic=[(100, 49, 44), (101, 49, 44)],
        tipos=[(10, "S")],
    )

    finding = by_id(
        validator.check_lookup_combo_frames(*inputs, sample=5)
    )["6.combo.identification_availability"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 2
    assert_failed_have_hints([finding])


def test_missing_lookup_frames_degrade_with_hints(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6)],
        tos=[(100, 10, 44, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )
    op_df, tos_df, _, _ = inputs

    missing_tos = validator.check_lookup_combo_frames(
        op_df, None, None, None, sample=5
    )
    missing_sic_and_tipo = validator.check_lookup_combo_frames(
        op_df, tos_df, None, None, sample=5
    )

    assert_failed_have_hints(missing_tos)
    assert_failed_have_hints(missing_sic_and_tipo)
    assert all(finding.check_id.startswith("6.combo") for finding in missing_tos)
    assert all(
        finding.check_id.startswith("6.combo")
        for finding in missing_sic_and_tipo
    )


def test_no_jdbc_warns(spark):
    op_df = spark.createDataFrame([(1, 100, 6)], "op long, tos long, modalidade long")
    cfg = validator.Config("unused", None, None, "", "CETIP")
    meta = validator.Metadata(set(), {}, {}, {}, {})

    finding = validator.check_lookup_combos(
        spark, cfg, {validator.OPERACAO_TABLE: op_df}, meta=meta, sample=5
    )[0]

    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert finding.check_id == "6.combo.no_jdbc"
    assert "No Oracle connection" in finding.message
    assert_failed_have_hints([finding])


def test_lazy_jdbc_failure_degrades_to_hinted_warnings(spark, monkeypatch):
    class FailsOnMaterialization:
        def collect(self):
            raise RuntimeError("Oracle read failed during action")

    sic_df = spark.createDataFrame(
        [(100, 49, 44)],
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )
    tipo_df = spark.createDataFrame(
        [(10, "S")],
        "NUM_ID_TIPO_OPERACAO long, IND_SEM_MODALIDADE_INFOHUB string",
    )

    def fake_jdbc(_spark, _cfg, query):
        if f"FROM {_cfg.schema}.{validator.TIPO_OPER_OBJETO_SERV_TABLE}" in query:
            return FailsOnMaterialization()
        if f"FROM {_cfg.schema}.{validator.V_PARAMETRO_SIC_TABLE}" in query:
            return sic_df
        return tipo_df

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    op_df = spark.createDataFrame(
        [(1, 100, 6)],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
        "NUM_ID_MODALIDADE_LIQUIDACAO long",
    )
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")
    meta = validator.Metadata(set(), {}, {}, {}, {})

    findings = validator.check_lookup_combos(
        spark, cfg, {validator.OPERACAO_TABLE: op_df}, meta=meta, sample=5
    )

    assert all(finding.check_id.startswith("6.combo.") for finding in findings)
    assert all(not finding.passed for finding in findings)
    assert all(finding.severity == validator.SEV_WARN for finding in findings)
    assert_failed_have_hints(findings)


def test_missing_required_operation_columns_warn(spark):
    op_df = spark.createDataFrame([(1, 100)], "NUM_ID_OPERACAO long, OTHER_ID long")

    finding = validator.check_lookup_combo_frames(
        op_df, None, None, None, sample=5
    )[0]

    assert finding.check_id == "6.combo.required_columns"
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert "NUM_ID_TIPO_OPER_OBJETO_SERV" in finding.message
    assert "NUM_ID_MODALIDADE_LIQUIDACAO" in finding.message
    assert_failed_have_hints([finding])
