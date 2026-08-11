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
        SparkSession.builder.appName("validate-cdb-variants-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def valid_resgate_tables(spark, mode="COM TABELA"):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 49, None, "2026-07-17", "2028-07-06", 0)],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string, DAT_EMISSAO string, "
            "DAT_VENCIMENTO string, COD_SITUACAO_IF long",
        ),
        "TITULO": spark.createDataFrame(
            [(1, None)], "NUM_IF long, COD_TIPO_ESCALONAMENTO string"
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, 3, None, None), (12, 1, 20, None, None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, "
            "DAT_EXCLUSAO string, DAT_INICIO_CONDICAO_IF string",
        ),
        "JUROS_FLUTUANTE": spark.createDataFrame(
            [(11, 4, 90.0, 90.0, 2, 1, "N", 0, "CONSTANTE")],
            "NUM_CONDICAO_IF long, NUM_INDICE_VALORIZACAO long, "
            "VAL_TAXA_JUROS_FLUTUANTE double, VAL_PERCENTUAL_TAXA_JUROS double, "
            "IND_ANO_COMERCIAL long, IND_DIAS_CORRIDOS long, IND_INCORPORA_JUROS string, "
            "NUM_ID_TIPO_INDICADOR long, NOM_AGENDA_PAGAMENTO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(12, mode, "2028-07-06")],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_RESGATE string",
        ),
        "CONDICAO_RESGATE": spark.createDataFrame(
            [(101, 12, None, "2026-08-16", 0.05)],
            "NUM_ID_CONDICAO_RESGATE long, NUM_CONDICAO_IF long, IND_EXCLUIDO string, "
            "DAT_RESGATE string, VAL_PERCENTUAL double",
        ),
    }


def valid_escalonamento_tables(spark):
    tables = valid_resgate_tables(spark, mode="SEM TABELA")
    tables["TITULO"] = spark.createDataFrame(
        [(1, "EMISSAO")], "NUM_IF long, COD_TIPO_ESCALONAMENTO string"
    )
    tables["CONDICAO_IF"] = spark.createDataFrame(
        [
            (11, 1, 3, None, "2026-07-17"),
            (13, 1, 3, None, "2027-01-13"),
            (12, 1, 20, None, None),
        ],
        "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, "
        "DAT_EXCLUSAO string, DAT_INICIO_CONDICAO_IF string",
    )
    tables["JUROS_FLUTUANTE"] = spark.createDataFrame(
        [
            (11, 4, 90.0, 0.05, 2, 1, "N", 0, "CONSTANTE"),
            (13, 4, 90.0, 0.30, 2, 1, "N", 0, "CONSTANTE"),
        ],
        "NUM_CONDICAO_IF long, NUM_INDICE_VALORIZACAO long, "
        "VAL_TAXA_JUROS_FLUTUANTE double, VAL_PERCENTUAL_TAXA_JUROS double, "
        "IND_ANO_COMERCIAL long, IND_DIAS_CORRIDOS long, IND_INCORPORA_JUROS string, "
        "NUM_ID_TIPO_INDICADOR long, NOM_AGENDA_PAGAMENTO string",
    )
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].limit(0)
    return tables


def test_variant_rules_are_full_cdb_only(spark):
    tables = valid_resgate_tables(spark)

    assert validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb_simplificado"]
    ) == []
    assert validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["rdb"]
    ) == []


def test_valid_com_tabela_resgate_passes(spark):
    tables = valid_resgate_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].withColumn(
        "VAL_PERCENTUAL", pyspark.sql.functions.lit(113.33)
    )
    findings = validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    )

    assert findings
    assert all(finding.passed for finding in findings)


def test_resgate_schedule_requires_table_rows_and_rejects_sem_tabela_children(spark):
    missing = valid_resgate_tables(spark)
    missing.pop("CONDICAO_RESGATE")
    missing_findings = by_id(validator.check_cdb_variant_rules(
        missing, 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert missing_findings["2b.resgate_schedule_coverage"].severity == validator.SEV_ERROR

    sem_tabela = valid_resgate_tables(spark, mode="SEM TABELA")
    sem_findings = by_id(validator.check_cdb_variant_rules(
        sem_tabela, 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert sem_findings["2b.resgate_schedule_parent"].severity == validator.SEV_ERROR

    excluded = valid_resgate_tables(spark, mode="SEM TABELA")
    excluded["CONDICAO_RESGATE"] = excluded["CONDICAO_RESGATE"].withColumn(
        "IND_EXCLUIDO", pyspark.sql.functions.lit("S")
    )
    excluded_findings = by_id(validator.check_cdb_variant_rules(
        excluded, 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert excluded_findings["2b.resgate_schedule_parent"].passed

    partial = valid_resgate_tables(spark, mode="SEM TABELA")
    partial["CONDICAO_RESGATE"] = partial["CONDICAO_RESGATE"].drop("VAL_PERCENTUAL")
    partial_findings = by_id(validator.check_cdb_variant_rules(
        partial, 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert partial_findings["2b.resgate_schedule_coverage"].severity == validator.SEV_ERROR


@pytest.mark.parametrize("percentage", [float("nan"), float("inf"), float("-inf")])
def test_resgate_schedule_rejects_non_finite_percentages(spark, percentage):
    tables = valid_resgate_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].withColumn(
        "VAL_PERCENTUAL", pyspark.sql.functions.lit(percentage)
    )

    findings = by_id(validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    ))

    assert findings["2b.resgate_schedule_values"].severity == validator.SEV_ERROR


def test_resgate_schedule_rejects_invalid_parent_values_dates_and_duplicates(spark):
    tables = valid_resgate_tables(spark)
    tables["CONDICAO_RESGATE"] = spark.createDataFrame(
        [
            (101, 11, None, "2026-07-01", None),
            (102, 12, None, "2029-01-01", 1.0),
            (103, 12, None, "2029-01-01", 2.0),
            (104, 99, None, "2026-08-01", 3.0),
        ],
        tables["CONDICAO_RESGATE"].schema,
    )

    findings = by_id(validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    ))

    assert findings["2b.resgate_schedule_parent"].severity == validator.SEV_ERROR
    assert findings["2b.resgate_schedule_values"].severity == validator.SEV_ERROR
    assert findings["2b.resgate_schedule_dates"].severity == validator.SEV_ERROR
    assert findings["2b.resgate_schedule_unique_dates"].severity == validator.SEV_ERROR


def test_escalonamento_requires_valid_consistent_segments(spark):
    valid = by_id(validator.check_cdb_variant_rules(
        valid_escalonamento_tables(spark), 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert valid["2b.escalonamento_coverage"].passed
    assert valid["2b.escalonamento_dates"].passed
    assert valid["2b.escalonamento_consistency"].passed

    tables = valid_escalonamento_tables(spark)
    tables["CONDICAO_IF"] = spark.createDataFrame(
        [
            (11, 1, 3, None, "2026-08-01"),
            (13, 1, 3, None, "2026-08-01"),
            (12, 1, 20, None, None),
        ],
        tables["CONDICAO_IF"].schema,
    )
    tables["JUROS_FLUTUANTE"] = tables["JUROS_FLUTUANTE"].withColumn(
        "VAL_TAXA_JUROS_FLUTUANTE",
        pyspark.sql.functions.when(
            pyspark.sql.functions.col("NUM_CONDICAO_IF") == 13, 91.0
        ).otherwise(90.0),
    )
    invalid = by_id(validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    ))

    assert invalid["2b.escalonamento_dates"].severity == validator.SEV_ERROR
    assert invalid["2b.escalonamento_unique_dates"].severity == validator.SEV_ERROR
    assert invalid["2b.escalonamento_consistency"].severity == validator.SEV_ERROR

    no_segments = valid_escalonamento_tables(spark)
    no_segments["CONDICAO_IF"] = no_segments["CONDICAO_IF"].where(
        pyspark.sql.functions.col("COD_TIPO_CONDICAO_IF") == 20
    )
    no_segment_findings = by_id(validator.check_cdb_variant_rules(
        no_segments, 5, validator.VALIDATION_PROFILES["cdb"]
    ))
    assert no_segment_findings["2b.escalonamento_coverage"].severity == validator.SEV_ERROR


def test_non_escalonado_does_not_require_escalonamento_columns(spark):
    tables = valid_resgate_tables(spark)
    tables["JUROS_FLUTUANTE"] = tables["JUROS_FLUTUANTE"].select("NUM_CONDICAO_IF")

    findings = by_id(validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    ))

    assert "2b.escalonamento_consistency" not in findings


def test_pending_rules_are_warning_only(spark):
    tables = valid_resgate_tables(spark)
    tables["PENDENCIA_IF"] = spark.createDataFrame(
        [
            (201, 1, 1, "2026-08-01", None),
            (202, 1, 29, "2026-08-02", "2026-08-01"),
            (203, 1, 29, "2026-08-01", None),
            (204, 1, 1, "2026-08-01", "not-a-date"),
        ],
        "NUM_ID_PENDENCIA_IF long, NUM_IF long, NUM_ID_TIPO_PENDENCIA long, "
        "DAT_INICIO_PENDENCIA string, DAT_FIM_PENDENCIA string",
    )

    findings = by_id(validator.check_cdb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["cdb"]
    ))

    assert findings["2b.pendencia_dates"].severity == validator.SEV_WARN
    assert findings["2b.pendencia_dates"].count == 2
    assert findings["2b.pendencia_open_final"].severity == validator.SEV_WARN
