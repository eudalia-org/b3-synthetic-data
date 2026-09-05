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
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, DAT_EXCLUSAO string",
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
    return by_id(
        validator.check_rdb_resgate_schedule_rules(
            tables, sample=5, profile=validator.VALIDATION_PROFILES["rdb"]
        )
    )


def check_profile(tables, profile_name):
    return by_id(
        validator.check_rdb_resgate_schedule_rules(
            tables, sample=5, profile=validator.VALIDATION_PROFILES[profile_name]
        )
    )


def rdb_inclusao_tables(spark):
    tables = rdb_tables(spark)
    tables["RESGATE"] = spark.createDataFrame(
        [(11, "SEM TABELA", "2028-07-06"), (12, "SEM TABELA", "2028-07-06")],
        tables["RESGATE"].schema,
    )
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].limit(0)
    tables["TITULO"] = spark.createDataFrame(
        [(1, 0.0), (2, 0.0)], "NUM_IF long, QTD_RESGATADA double"
    )
    return tables


def rdb_resgate_tables(spark):
    tables = rdb_tables(spark)
    tables["RESGATE"] = spark.createDataFrame(
        [(11, "COM TABELA", "2028-07-06"), (12, "COM TABELA", "2028-07-06")],
        tables["RESGATE"].schema,
    )
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].union(
        spark.createDataFrame(
            [(103, 11, None, "2027-05-01", 10.0)],
            tables["CONDICAO_RESGATE"].schema,
        )
    )
    return tables


def test_rdb_profiles_are_distinct():
    assert validator.VALIDATION_PROFILES["rdb_inclusao"].name == "rdb_inclusao"
    assert validator.VALIDATION_PROFILES["rdb_resgate"].name == "rdb_resgate"
    assert validator.VALIDATION_PROFILES["rdb_inclusao"].pipeline == "rdb"


def test_rdb_inclusao_requires_sem_tabela_and_zero_redeemed_quantity(spark):
    findings = check_profile(rdb_inclusao_tables(spark), "rdb_inclusao")

    assert findings["2c.rdb_variant_resgate_mode"].passed
    assert findings["2c.rdb_inclusao_redeemed_quantity"].passed
    assert findings["2c.rdb_resgate_schedule_parent"].passed


def test_rdb_inclusao_rejects_nonzero_redeemed_quantity(spark):
    tables = rdb_inclusao_tables(spark)
    tables["TITULO"] = spark.createDataFrame([(1, 0.0), (2, 1.0)], tables["TITULO"].schema)

    finding = check_profile(tables, "rdb_inclusao")["2c.rdb_inclusao_redeemed_quantity"]
    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_rdb_variant_profiles_reject_the_other_resgate_mode(spark):
    inclusion = rdb_inclusao_tables(spark)
    inclusion["RESGATE"] = inclusion["RESGATE"].withColumn("COD_COND_RESGATE", F.lit("COM TABELA"))
    resgate = rdb_resgate_tables(spark)
    resgate["RESGATE"] = resgate["RESGATE"].withColumn("COD_COND_RESGATE", F.lit("SEM TABELA"))

    assert (
        check_profile(inclusion, "rdb_inclusao")["2c.rdb_variant_resgate_mode"].severity
        == validator.SEV_ERROR
    )
    assert (
        check_profile(resgate, "rdb_resgate")["2c.rdb_variant_resgate_mode"].severity
        == validator.SEV_ERROR
    )


def test_rdb_resgate_profile_requires_com_tabela_schedule(spark):
    findings = check_profile(rdb_resgate_tables(spark), "rdb_resgate")

    assert findings["2c.rdb_variant_resgate_mode"].passed
    assert findings["2c.rdb_resgate_schedule_coverage"].passed
    assert "2c.rdb_inclusao_redeemed_quantity" not in findings


@pytest.mark.parametrize("profile_name", ["rdb_inclusao", "rdb_resgate"])
def test_specific_rdb_profiles_fail_when_core_contract_is_unavailable(spark, profile_name):
    tables = rdb_inclusao_tables(spark)
    tables.pop("RESGATE")

    finding = check_profile(tables, profile_name)["2c.rdb_resgate_schedule_availability"]
    assert finding.severity == validator.SEV_ERROR


@pytest.mark.parametrize("row_count", [0, 2])
def test_rdb_inclusao_requires_exactly_one_sem_tabela_resgate(spark, row_count):
    tables = rdb_inclusao_tables(spark)
    if row_count == 0:
        tables["RESGATE"] = tables["RESGATE"].limit(0)
    else:
        tables["RESGATE"] = tables["RESGATE"].union(
            tables["RESGATE"].where(F.col("NUM_CONDICAO_IF") == 11)
        )

    finding = check_profile(tables, "rdb_inclusao")["2c.rdb_variant_resgate_mode"]
    assert finding.severity == validator.SEV_ERROR


def test_rdb_inclusao_rejects_active_schedule_rows(spark):
    tables = rdb_inclusao_tables(spark)
    tables["CONDICAO_RESGATE"] = spark.createDataFrame(
        [(103, 11, None, "2026-09-15", 0.10)],
        rdb_tables(spark)["CONDICAO_RESGATE"].schema,
    )

    finding = check_profile(tables, "rdb_inclusao")["2c.rdb_resgate_schedule_parent"]
    assert finding.severity == validator.SEV_ERROR


def test_incomplete_schedule_does_not_discard_inclusion_quantity_error(spark):
    tables = rdb_inclusao_tables(spark)
    tables["TITULO"] = spark.createDataFrame([(1, 1.0), (2, 0.0)], tables["TITULO"].schema)
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].drop("VAL_PERCENTUAL")

    findings = check_profile(tables, "rdb_inclusao")
    assert findings["2c.rdb_inclusao_redeemed_quantity"].severity == validator.SEV_ERROR


def test_rdb_resgate_profile_rejects_missing_schedule_table(spark):
    tables = rdb_resgate_tables(spark)
    tables.pop("CONDICAO_RESGATE")

    finding = check_profile(tables, "rdb_resgate")["2c.rdb_resgate_schedule_coverage"]
    assert finding.severity == validator.SEV_ERROR


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
    tables["CONDICAO_RESGATE"] = tables["CONDICAO_RESGATE"].withColumn("IND_EXCLUIDO", F.lit("S"))

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
    assert (
        validator.check_rdb_resgate_schedule_rules(
            rdb_tables(spark), 5, validator.VALIDATION_PROFILES["cdb"]
        )
        == []
    )
