import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-rdb-variants-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def rdb_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [
                (1, 50, "2026-07-17", "2028-07-06", None),
                (2, 50, "2026-07-17", "2028-07-06", None),
            ],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EMISSAO string, DAT_VENCIMENTO string, "
            "DAT_EXCLUSAO string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, 20, None), (12, 2, 20, None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, "
            "DAT_EXCLUSAO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(11, "SEM TABELA", "2028-07-06"), (12, "COM TABELA", "2028-07-06")],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_RESGATE string",
        ),
        "CONDICAO_RESGATE": spark.createDataFrame(
            [
                (101, 12, None, "2026-08-16", 0.05),
                (102, 12, None, "2028-05-27", 113.33),
            ],
            "NUM_ID_CONDICAO_RESGATE long, NUM_CONDICAO_IF long, IND_EXCLUIDO string, "
            "DAT_RESGATE string, VAL_PERCENTUAL double",
        ),
    }


def check(tables):
    return by_id(validator.check_rdb_resgate_schedule_rules(
        tables, sample=5, profile=validator.VALIDATION_PROFILES["rdb"]
    ))


def test_rdb_accepts_sem_tabela_and_observed_com_tabela_schedule(spark):
    findings = check(rdb_tables(spark))

    assert findings["2c.rdb_resgate_schedule_coverage"].passed
    assert findings["2c.rdb_resgate_schedule_parent"].passed
    assert findings["2c.rdb_resgate_schedule_values"].passed
    assert findings["2c.rdb_resgate_schedule_unique_dates"].passed
    assert findings["2c.rdb_resgate_schedule_dates"].passed
    assert findings["2c.rdb_resgate_schedule_percentages"].passed


def test_rdb_com_tabela_requires_an_active_schedule(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].limit(0)

    finding = check(tables)["2c.rdb_resgate_schedule_coverage"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_rdb_sem_tabela_rejects_active_schedule_rows(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].union(
        spark.createDataFrame(
            [(103, 11, None, "2026-09-15", 0.10)],
            tables["CONDICAO_RESGATE"].schema,
        )
    )

    finding = check(tables)["2c.rdb_resgate_schedule_parent"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_rdb_ignores_excluded_schedule_rows(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].union(
        spark.createDataFrame(
            [(103, 11, "S", "2026-09-15", 0.10)],
            tables["CONDICAO_RESGATE"].schema,
        )
    )

    assert check(tables)["2c.rdb_resgate_schedule_parent"].passed


def test_rdb_excluded_schedule_does_not_satisfy_com_tabela_coverage(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].withColumn(
        "IND_EXCLUIDO", F.lit("S")
    )

    finding = check(tables)["2c.rdb_resgate_schedule_coverage"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_rdb_rejects_schedule_with_missing_or_inactive_parent(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].union(
        spark.createDataFrame(
            [(103, 999, None, "2026-09-15", 0.10)],
            tables["CONDICAO_RESGATE"].schema,
        )
    )

    finding = check(tables)["2c.rdb_resgate_schedule_parent"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_rdb_rejects_null_or_non_finite_schedule_values(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = spark.createDataFrame(
        [
            (101, 12, None, None, 0.05),
            (102, 12, None, "2028-05-27", float("nan")),
        ],
        tables["CONDICAO_RESGATE"].schema,
    )

    finding = check(tables)["2c.rdb_resgate_schedule_values"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 2


def test_rdb_warns_when_percentages_decrease_by_schedule_date(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = spark.createDataFrame(
        [
            (101, 12, None, "2026-08-16", 10.0),
            (102, 12, None, "2027-08-16", 5.0),
        ],
        tables["CONDICAO_RESGATE"].schema,
    )

    finding = check(tables)["2c.rdb_resgate_schedule_percentages"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 1


def test_rdb_warns_for_duplicate_or_out_of_bounds_dates(spark):
    tables = rdb_tables(spark)
    tables["CONDICAO_RESGATE"] = spark.createDataFrame(
        [
            (101, 12, None, "2026-07-16", 0.05),
            (102, 12, None, "2026-07-16", 0.10),
        ],
        tables["CONDICAO_RESGATE"].schema,
    )
    findings = check(tables)

    assert findings["2c.rdb_resgate_schedule_unique_dates"].severity == validator.SEV_WARN
    assert findings["2c.rdb_resgate_schedule_dates"].severity == validator.SEV_WARN


def test_rdb_schedule_rules_do_not_run_for_cdb(spark):
    assert validator.check_rdb_resgate_schedule_rules(
        rdb_tables(spark), 5, validator.VALIDATION_PROFILES["cdb"]
    ) == []
