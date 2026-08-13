import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-credito-scr-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def credito_scr_tables(spark):
    return {
        "LOTE": spark.createDataFrame(
            [(100, "LCI00010CAP", 10, 1, "N", None)],
            "NUM_ID_LOTE long, NOME_LOTE string, NUM_CONTA_PARTICIPANTE long, "
            "NUM_ID_TIPO_LOTE long, IND_REVOLVENCIA string, DAT_EXCLUSAO string",
        ),
        "CREDITO_SCR": spark.createDataFrame(
            [
                (
                    1000, "26H00032408", 100, 46, 505, "IPOC-1", "2026-07-17", None,
                    143, "PF", "N", 17, 2, 100.0, 200.0, "2026-07-17", "2028-07-06",
                )
            ],
            "NUM_ID_CREDITO_SCR long, COD_CREDITO_SCR string, NUM_ID_LOTE long, "
            "NUM_ID_MODALIDADE_CREDITO long, NUM_ID_BASE_CREDITO long, COD_IPOC string, "
            "DAT_SALDO_REMANESCENTE string, DAT_EXCLUSAO string, NUM_TIPO_IF long, "
            "COD_TIPO_PESSOA string, IND_MULTIPLO_IPOC string, NUM_ID_TIPO_CREDITO long, "
            "NUM_ID_INDEXADOR_CREDITO long, VAL_SALDO_REMANESCENTE double, "
            "VAL_CONTRATADO double, DAT_CONTRATACAO string, DAT_VENCIMENTO string",
        ),
        "HISTORICO_CREDITO_SCR": spark.createDataFrame(
            [
                (
                    5000, 1000, "26H00032408", 100, 10, "Inclus\u00e3o", 6, 143, "PF",
                    "N", 100.0, 200.0, "2026-07-17", "2028-07-06",
                )
            ],
            "NUM_ID_HISTORICO_CREDITO_SCR long, NUM_ID_CREDITO_SCR long, "
            "COD_CREDITO_SCR string, NUM_ID_LOTE long, NUM_CONTA_PARTICIPANTE long, "
            "TXT_DESCRICAO string, COD_ID_CANAL long, NUM_TIPO_IF long, "
            "COD_TIPO_PESSOA string, IND_MULTIPLO_IPOC string, "
            "VAL_SALDO_REMANESCENTE double, VAL_CONTRATADO double, "
            "DAT_CONTRATACAO string, DAT_VENCIMENTO string",
        ),
    }


def graph_findings(tables):
    return by_id(validator.check_credito_scr_graph(
        tables, sample=5, profile=validator.VALIDATION_PROFILES["credito_scr"]
    ))


def target_frames(spark):
    return (
        spark.createDataFrame([(46, "0901")],
                              "NUM_ID_MODALIDADE_CREDITO long, COD_MODALIDADE_CREDITO string"),
        spark.createDataFrame([(10, 505)],
                              "NUM_CONTA_PARTICIPANTE long, NUM_ID_BASE_CREDITO long"),
        spark.createDataFrame(
            [("S", "2026-01-01", "2026-12-31")],
            "IND_FTRE_HAB string, DATA_INIC_VIG_FTRE string, DATA_FIM_VIG_FTRE string",
        ),
        spark.createDataFrame([], "COD_IPOC string"),
    )


def add_registration_columns(tables):
    credit = tables["CREDITO_SCR"]
    history = tables["HISTORICO_CREDITO_SCR"]
    for name, value in {
        "NUM_IF": None,
        "QTD_CREDITO": None,
        "COD_CONTRATO_SCR": "CONTRACT-1",
        "COD_REFERENCIA_EXTERNA_DEVEDOR": "DEBTOR-1",
        "VAL_PERCENTUAL_INDEXADOR": None,
        "VAL_PERCENTUAL_TAXA_ANUAL": None,
    }.items():
        credit = credit.withColumn(name, validator.F.lit(value))
    for name, value in {
        "NUM_IF_CREDITO": None,
        "QTD_CREDITO": None,
        "DAT_SALDO_REMANESCENTE": "2026-07-17",
        "COD_CONTRATO_SCR": "CONTRACT-1",
        "NUM_ID_TIPO_CREDITO": 17,
        "NUM_ID_MODALIDADE_CREDITO": 46,
        "NUM_ID_INDEXADOR_CREDITO": 2,
        "COD_REFERENCIA_EXTERNA_DEVEDOR": "DEBTOR-1",
        "VAL_PERCENTUAL_INDEXADOR": None,
        "VAL_PERCENTUAL_TAXA_ANUAL": None,
        "COD_IPOC": "IPOC-1",
    }.items():
        history = history.withColumn(name, validator.F.lit(value))
    tables["CREDITO_SCR"] = credit
    tables["HISTORICO_CREDITO_SCR"] = history
    return tables


def test_credito_scr_profile_uses_non_if_pipeline():
    profile = validator.VALIDATION_PROFILES["credito_scr"]

    assert profile.pipeline == "credito_scr"
    assert profile.unsupported_required() == ()
    assert validator.check_credito_scr_graph({}, 5, validator.VALIDATION_PROFILES["cdb"]) == []


def test_credito_scr_accepts_complete_active_graph(spark):
    findings = graph_findings(credito_scr_tables(spark))

    assert all(finding.passed for finding in findings.values())
    assert set(findings) == {
        "2d.active_lot_natural_key",
        "2d.credit_active_lot",
        "2d.credit_code_unique",
        "2d.inclusion_history",
        "2d.inclusion_identity",
    }


def test_credito_scr_treats_oracle_empty_exclusion_as_active(spark):
    tables = credito_scr_tables(spark)
    tables["LOTE"] = tables["LOTE"].fillna({"DAT_EXCLUSAO": ""})
    tables["CREDITO_SCR"] = tables["CREDITO_SCR"].fillna({"DAT_EXCLUSAO": ""})

    assert all(finding.passed for finding in graph_findings(tables).values())


def test_credito_scr_rejects_duplicate_active_lot_and_orphan_credit(spark):
    tables = credito_scr_tables(spark)
    tables["LOTE"] = tables["LOTE"].union(
        spark.createDataFrame(
            [(101, "LCI00010CAP", 10, 1, "N", None)], tables["LOTE"].schema
        )
    )
    tables["CREDITO_SCR"] = tables["CREDITO_SCR"].withColumn(
        "NUM_ID_LOTE", validator.F.lit(999)
    )

    findings = graph_findings(tables)

    assert findings["2d.active_lot_natural_key"].count == 1
    assert findings["2d.credit_active_lot"].count == 1


def test_credito_scr_rejects_blank_or_duplicate_active_credit_codes(spark):
    tables = credito_scr_tables(spark)
    duplicate = spark.createDataFrame(
        [
            (
                1001, "26H00032408", 100, 46, 505, "IPOC-2", "2026-07-17", None,
                143, "PF", "N", 17, 2, 100.0, 200.0, "2026-07-17", "2028-07-06",
            ),
            (
                1002, " ", 100, 46, 505, "IPOC-3", "2026-07-17", None,
                143, "PF", "N", 17, 2, 100.0, 200.0, "2026-07-17", "2028-07-06",
            ),
        ],
        tables["CREDITO_SCR"].schema,
    )
    tables["CREDITO_SCR"] = tables["CREDITO_SCR"].union(duplicate)

    finding = graph_findings(tables)["2d.credit_code_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 3


def test_credito_scr_business_code_uniqueness_preserves_case_and_dot_zero(spark):
    tables = credito_scr_tables(spark)
    first = tables["CREDITO_SCR"].withColumn(
        "COD_CREDITO_SCR", validator.F.lit("credit.0")
    )
    second = first.withColumn("NUM_ID_CREDITO_SCR", validator.F.lit(1001)).withColumn(
        "COD_CREDITO_SCR", validator.F.lit("credit")
    )
    third = first.withColumn("NUM_ID_CREDITO_SCR", validator.F.lit(1002)).withColumn(
        "COD_CREDITO_SCR", validator.F.lit("CREDIT.0")
    )
    tables["CREDITO_SCR"] = first.union(second).union(third)

    assert graph_findings(tables)["2d.credit_code_unique"].passed


def test_credito_scr_requires_one_normalized_inclusion_history(spark):
    tables = credito_scr_tables(spark)
    tables["HISTORICO_CREDITO_SCR"] = tables["HISTORICO_CREDITO_SCR"].union(
        spark.createDataFrame(
            [
                (
                    5001, 1000, "26H00032408", 100, 10, "INCLUSAO", 6, 143, "PF",
                    "N", 100.0, 200.0, "2026-07-17", "2028-07-06",
                )
            ],
            tables["HISTORICO_CREDITO_SCR"].schema,
        )
    )

    finding = graph_findings(tables)["2d.inclusion_history"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_credito_scr_rejects_inclusion_identity_mismatch(spark):
    tables = credito_scr_tables(spark)
    tables["HISTORICO_CREDITO_SCR"] = tables["HISTORICO_CREDITO_SCR"].withColumn(
        "NUM_CONTA_PARTICIPANTE", validator.F.lit(999)
    )

    finding = graph_findings(tables)["2d.inclusion_identity"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_credito_scr_target_checks_accept_lookup_eligible_credit(spark):
    tables = credito_scr_tables(spark)
    modalidade, bases, toggle, target_ipocs = target_frames(spark)

    findings = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, toggle, 5,
        validator.VALIDATION_PROFILES["credito_scr"],
        existing_ipocs=target_ipocs,
    ))

    assert findings["6d.lookup.modalidade"].passed
    assert findings["6d.lookup.base_eligibility"].passed
    assert findings["6d.lookup.ipoc_unique"].passed


def test_credito_scr_target_checks_reject_excluded_mode_and_ineligible_base(spark):
    tables = credito_scr_tables(spark)
    _, _, toggle, target_ipocs = target_frames(spark)
    modalidade = spark.createDataFrame(
        [(46, "9999")], "NUM_ID_MODALIDADE_CREDITO long, COD_MODALIDADE_CREDITO string"
    )
    bases = spark.createDataFrame(
        [(999, 505)], "NUM_CONTA_PARTICIPANTE long, NUM_ID_BASE_CREDITO long"
    )

    findings = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, toggle, 5,
        validator.VALIDATION_PROFILES["credito_scr"],
        existing_ipocs=target_ipocs,
    ))

    assert findings["6d.lookup.modalidade"].severity == validator.SEV_ERROR
    assert findings["6d.lookup.base_eligibility"].severity == validator.SEV_ERROR


def test_credito_scr_ipoc_uniqueness_follows_feature_period(spark):
    tables = credito_scr_tables(spark)
    duplicate = spark.createDataFrame(
        [
            (
                1001, "26H00032409", 100, 46, 505, "IPOC-1", "2026-07-17", None,
                143, "PF", "N", 17, 2, 100.0, 200.0, "2026-07-17", "2028-07-06",
            )
        ],
        tables["CREDITO_SCR"].schema,
    )
    tables["CREDITO_SCR"] = tables["CREDITO_SCR"].union(duplicate)
    modalidade, bases, enabled, target_ipocs = target_frames(spark)
    disabled = spark.createDataFrame(
        [("S", "2025-01-01", "2025-12-31")], enabled.schema
    )

    enabled_finding = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, enabled, 5,
        validator.VALIDATION_PROFILES["credito_scr"],
        existing_ipocs=target_ipocs,
    ))["6d.lookup.ipoc_unique"]
    disabled_finding = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, disabled, 5,
        validator.VALIDATION_PROFILES["credito_scr"],
        existing_ipocs=target_ipocs,
    ))["6d.lookup.ipoc_unique"]

    assert enabled_finding.count == 2
    assert disabled_finding.passed


def test_credito_scr_ipoc_uniqueness_checks_existing_target(spark):
    tables = credito_scr_tables(spark)
    modalidade, bases, enabled, _ = target_frames(spark)
    target_ipocs = spark.createDataFrame([("IPOC-1",)], "COD_IPOC string")

    finding = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, enabled, 5,
        validator.VALIDATION_PROFILES["credito_scr"], existing_ipocs=target_ipocs,
    ))["6d.lookup.ipoc_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_credito_scr_does_not_require_target_ipocs_when_toggle_is_disabled(spark):
    tables = credito_scr_tables(spark)
    modalidade, bases, enabled, _ = target_frames(spark)
    disabled = spark.createDataFrame(
        [("S", "2025-01-01", "2025-12-31")], enabled.schema
    )

    finding = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, disabled, 5,
        validator.VALIDATION_PROFILES["credito_scr"], existing_ipocs=None,
    ))["6d.lookup.ipoc_unique"]

    assert finding.passed
    assert finding.severity == validator.SEV_INFO


def test_credito_scr_observed_profile_is_opt_in(spark):
    tables = add_registration_columns(credito_scr_tables(spark))
    profile = validator.VALIDATION_PROFILES["credito_scr"]

    assert validator.check_credito_scr_registration_profile(tables, 5, False, profile) == []
    tables["CREDITO_SCR"] = tables["CREDITO_SCR"].withColumn(
        "NUM_TIPO_IF", validator.F.lit(999)
    )
    findings = by_id(validator.check_credito_scr_registration_profile(
        tables, 5, True, profile
    ))

    assert findings["8d.profile.credit_constants"].severity == validator.SEV_WARN
    assert findings["8d.profile.credit_constants"].count == 1


def test_credito_scr_account_route_profile_is_advisory(spark):
    tables = credito_scr_tables(spark)
    modalidade, bases, toggle, target_ipocs = target_frames(spark)
    account = spark.createDataFrame(
        [(10, 3, "00010.10-9", 2, "R")],
        "NUM_CONTA_PARTICIPANTE long, NUM_ID_SITUACAO_CONTA long, "
        "COD_CONTA_PARTICIPANTE string, NUM_ID_AREA_ATUACAO long, COD_TIPO_ACESSO string",
    )

    finding = by_id(validator.check_credito_scr_target_frames(
        tables, modalidade, bases, toggle, 5,
        validator.VALIDATION_PROFILES["credito_scr"], account_profile=account,
        registration_profile=True, existing_ipocs=target_ipocs,
    ))["8d.profile.account_eligibility"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 1


def test_credito_scr_metadata_requires_live_table_and_pk_inventory():
    profile = validator.VALIDATION_PROFILES["credito_scr"]
    meta = validator.Metadata(
        {"LOTE", "CREDITO_SCR", "HISTORICO_CREDITO_SCR"},
        {"LOTE": ["NUM_ID_LOTE"], "CREDITO_SCR": ["NUM_ID_CREDITO_SCR"]},
        {}, {}, {},
    )

    finding = validator.check_credito_scr_metadata(meta, False, profile)[0]

    assert finding.severity == validator.SEV_ERROR
    assert "HISTORICO_CREDITO_SCR" in finding.message
