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
        SparkSession.builder.appName("validate-cdb-log-invariants-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def valid_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [
                (1, 49, None, "CDB101ABCDE", 55, "S", "N", 25, "N", "N", "N"),
                (2, 49, None, "CDB202FGHIJ", 55, "S", "N", 25, "N", "N", "N"),
            ],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string, COD_IF string, "
            "NUM_SISTEMA long, IND_AGENDA_CONSTANTE string, IND_ESPECIFICA_COMITENTE string, "
            "NUM_ID_MOTIVO_SITUACAO_IF long, IND_MANTEM_PREMIO string, IND_EXCLUI_IOF string, "
            "IND_ELEGIVEL_IOF string",
        ),
        "TITULO": spark.createDataFrame(
            [(1, "N", 2), (2, "N", 2)],
            "NUM_IF long, IND_FRACIONAMENTO string, NUM_ID_TIPO_REGIME_TITULO long",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [
                (11, 1, 3, None, "F", 1, 0),
                (12, 1, 20, None, "F", 1, 0),
                (21, 2, 3, None, "F", 1, 0),
                (22, 2, 20, None, "F", 1, 0),
            ],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, DAT_EXCLUSAO string, "
            "COD_TIPO_UNIDADE_TEMPO_PAGA string, QTD_UNID_TEMPO_PAGAMENTO long, "
            "NUM_ID_ORIG_DESL_LIQ long",
        ),
        "JUROS_FLUTUANTE": spark.createDataFrame(
            [(11, 2, 1, 0, "CONSTANTE"), (21, 2, 1, 0, "CONSTANTE")],
            "NUM_CONDICAO_IF long, IND_ANO_COMERCIAL long, IND_DIAS_CORRIDOS long, "
            "NUM_ID_TIPO_INDICADOR long, NOM_AGENDA_PAGAMENTO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(12, "EUROPEIA"), (22, "EUROPEIA")],
            "NUM_CONDICAO_IF long, COD_TIPO_EXERCICIO string",
        ),
        "EVENTO": spark.createDataFrame(
            [
                (101, 1, 83, None, 1, "N"),
                (102, 1, 85, None, 1, "N"),
                (201, 2, 83, None, 1, "N"),
                (202, 2, 85, None, 1, "N"),
            ],
            "NUM_EVENTO long, NUM_IF long, NUM_TIPO_EVENTO_LEGADO long, DAT_EXCLUSAO string, "
            "NUM_ID_ESTADO_EVENTO long, IND_INCORPORA string",
        ),
        "OPERACAO": spark.createDataFrame(
            [
                (1001, "20260101", "0000000000000001", 10, 11, "A1", "B1", 100),
                (1002, "20260101", "0000000000000002", 20, 21, "A2", "B2", 100),
            ],
            "NUM_ID_OPERACAO long, DAT_OPERACAO string, COD_OPERACAO string, "
            "NUM_CONTA_PARTICIPANTE_P1 long, NUM_CONTA_PARTICIPANTE_P2 long, "
            "NUM_CONTROLE_LANCAMENTO_P1 string, NUM_CONTROLE_LANCAMENTO_P2 string, "
            "NUM_ID_TIPO_OPER_OBJETO_SERV long",
        ),
        "DADO_OPERACAO": spark.createDataFrame(
            [(1, 1001, 502), (2, 1001, 503), (3, 1002, 502), (4, 1002, 503)],
            "NUM_ID_DADO_OPERACAO long, NUM_ID_OPERACAO long, "
            "NUM_ID_TIPO_DADO_OPERACAO long",
        ),
    }


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def assert_hinted_warn(finding):
    assert not finding.passed
    assert finding.severity == validator.SEV_WARN
    assert finding.hint.strip()


def test_valid_global_and_registration_profile(spark):
    findings = validator.check_log_invariants(
        valid_tables(spark), sample=5, registration_profile=True
    )

    assert all(finding.passed for finding in findings)
    assert all(finding.severity == validator.SEV_INFO for finding in findings)


def test_duplicate_active_cdb_cod_if_is_error_and_samples_keys(spark):
    tables = valid_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "COD_IF", pyspark.sql.functions.lit("CDB101ABCDE")
    )

    finding = by_id(validator.check_log_invariants(tables, sample=5))["8a.cod_if_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 2
    assert {row[0] for row in finding.sample} == {1, 2}


def test_empty_cod_if_is_left_to_nullability_and_format_checks(spark):
    tables = valid_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "COD_IF", pyspark.sql.functions.lit("   ")
    )

    global_finding = by_id(
        validator.check_log_invariants(tables, sample=5)
    )["8a.cod_if_unique"]
    profile_finding = by_id(
        validator.check_log_invariants(tables, sample=5, registration_profile=True)
    )["8b.cod_if_format"]

    assert global_finding.passed
    assert_hinted_warn(profile_finding)


def test_duplicate_cod_operacao_is_error_and_samples_operations(spark):
    tables = valid_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO", pyspark.sql.functions.lit("0000000000000001")
    )

    finding = by_id(validator.check_log_invariants(tables, sample=5))["8a.cod_operacao_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 2
    assert {row[0] for row in finding.sample} == {1001, 1002}


def test_null_and_empty_cod_operacao_are_left_to_nullability_checks(spark):
    tables = valid_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO",
        pyspark.sql.functions.when(
            pyspark.sql.functions.col("NUM_ID_OPERACAO") == 1001,
            pyspark.sql.functions.lit(None).cast("string"),
        ).otherwise(pyspark.sql.functions.lit("")),
    )

    finding = by_id(validator.check_log_invariants(tables, sample=5))["8a.cod_operacao_unique"]

    assert finding.passed
    assert finding.count == 0


def test_duplicate_flattened_meunumero_tuple_is_error_and_samples_operations(spark):
    tables = valid_tables(spark)
    operation = tables["OPERACAO"]
    tables["OPERACAO"] = operation.withColumn(
        "NUM_CONTA_PARTICIPANTE_P1", pyspark.sql.functions.lit(10)
    ).withColumn("NUM_CONTROLE_LANCAMENTO_P1", pyspark.sql.functions.lit("A1"))

    finding = by_id(validator.check_log_invariants(tables, sample=5))["8a.meu_numero_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 2
    assert {row[0] for row in finding.sample} == {1001, 1002}


def test_incomplete_meunumero_tuples_are_ignored(spark):
    operation = spark.createDataFrame(
        [
            (1, "20260101", "0000000000000001", 10, None, "A", None, 100),
            (2, "20260101", "0000000000000002", 10, None, None, None, 100),
        ],
        "NUM_ID_OPERACAO long, DAT_OPERACAO string, COD_OPERACAO string, "
        "NUM_CONTA_PARTICIPANTE_P1 long, NUM_CONTA_PARTICIPANTE_P2 long, "
        "NUM_CONTROLE_LANCAMENTO_P1 string, NUM_CONTROLE_LANCAMENTO_P2 string, "
        "NUM_ID_TIPO_OPER_OBJETO_SERV long",
    )

    finding = by_id(
        validator.check_log_invariants({"OPERACAO": operation}, sample=5)
    )["8a.meu_numero_unique"]

    assert finding.passed
    assert finding.count == 0


def test_empty_meunumero_tuple_components_are_ignored(spark):
    operation = spark.createDataFrame(
        [
            (1, "20260101", "0000000000000001", 10, 20, "", "B", 100),
            (2, "20260101", "0000000000000002", 10, 21, "", "C", 100),
        ],
        "NUM_ID_OPERACAO long, DAT_OPERACAO string, COD_OPERACAO string, "
        "NUM_CONTA_PARTICIPANTE_P1 long, NUM_CONTA_PARTICIPANTE_P2 long, "
        "NUM_CONTROLE_LANCAMENTO_P1 string, NUM_CONTROLE_LANCAMENTO_P2 string, "
        "NUM_ID_TIPO_OPER_OBJETO_SERV long",
    )

    finding = by_id(
        validator.check_log_invariants({"OPERACAO": operation}, sample=5)
    )["8a.meu_numero_unique"]

    assert finding.passed
    assert finding.count == 0


def test_missing_tables_and_columns_return_hinted_warnings(spark):
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame([(1,)], "NUM_IF long"),
        "OPERACAO": spark.createDataFrame([(1,)], "NUM_ID_OPERACAO long"),
    }

    findings = validator.check_log_invariants(tables, sample=5, registration_profile=True)

    unavailable = [finding for finding in findings if not finding.passed]
    assert unavailable
    assert all(finding.severity == validator.SEV_WARN for finding in unavailable)
    assert all(finding.hint.strip() for finding in unavailable)
    assert "COD_IF" in by_id(findings)["8a.cod_if_unique"].message
    assert "COD_OPERACAO" in by_id(findings)["8a.cod_operacao_unique"].message


def test_registration_profile_disabled_skips_formats_constants_and_type_mixes(spark):
    tables = valid_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "NUM_SISTEMA", pyspark.sql.functions.lit(99)
    )
    tables.pop("DADO_OPERACAO")

    findings = validator.check_log_invariants(tables, sample=5, registration_profile=False)

    assert {finding.check_id for finding in findings} == {
        "8a.cod_if_unique",
        "8a.cod_operacao_unique",
        "8a.meu_numero_unique",
    }
    assert all(finding.passed for finding in findings)


def test_registration_profile_bad_formats_and_constants_warn(spark):
    tables = valid_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = (
        tables["INSTRUMENTO_FINANCEIRO"]
        .withColumn("COD_IF", pyspark.sql.functions.lit("BAD"))
        .withColumn("NUM_SISTEMA", pyspark.sql.functions.lit(None).cast("long"))
    )
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO", pyspark.sql.functions.lit("123")
    )
    tables["RESGATE"] = tables["RESGATE"].withColumn(
        "COD_TIPO_EXERCICIO", pyspark.sql.functions.lit("AMERICANA")
    )

    findings = by_id(
        validator.check_log_invariants(tables, sample=5, registration_profile=True)
    )

    for check_id in (
        "8b.cod_if_format",
        "8b.cod_operacao_format",
        "8c.registration_constants.instrumento_financeiro",
        "8c.registration_constants.resgate",
    ):
        assert_hinted_warn(findings[check_id])


def test_profile_type_mixes_include_zero_child_parents_and_require_exact_mix(spark):
    tables = valid_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].where(
        pyspark.sql.functions.col("NUM_IF") == 1
    )
    tables["EVENTO"] = tables["EVENTO"].unionByName(
        spark.createDataFrame(
            [(203, 2, 85, None, 1, "N")],
            tables["EVENTO"].schema,
        )
    )
    tables["DADO_OPERACAO"] = tables["DADO_OPERACAO"].withColumn(
        "NUM_ID_TIPO_DADO_OPERACAO",
        pyspark.sql.functions.when(
            pyspark.sql.functions.col("NUM_ID_DADO_OPERACAO") == 4, 502
        ).otherwise(pyspark.sql.functions.col("NUM_ID_TIPO_DADO_OPERACAO")),
    )

    findings = by_id(
        validator.check_log_invariants(tables, sample=5, registration_profile=True)
    )

    assert_hinted_warn(findings["8c.condicao_type_mix"])
    assert findings["8c.condicao_type_mix"].sample == ["2"]
    assert_hinted_warn(findings["8c.evento_type_mix"])
    assert_hinted_warn(findings["8c.dado_operacao_type_mix"])


def test_profile_type_mix_unavailable_columns_warn(spark):
    tables = valid_tables(spark)
    tables["EVENTO"] = tables["EVENTO"].drop("NUM_TIPO_EVENTO_LEGADO")

    finding = by_id(
        validator.check_log_invariants(tables, sample=5, registration_profile=True)
    )["8c.evento_type_mix"]

    assert_hinted_warn(finding)
    assert "NUM_TIPO_EVENTO_LEGADO" in finding.message


def test_columns_are_resolved_case_insensitively(spark):
    tables = {
        table: df.toDF(*[column.lower() for column in df.columns])
        for table, df in valid_tables(spark).items()
    }

    findings = validator.check_log_invariants(tables, sample=5, registration_profile=True)

    assert all(finding.passed for finding in findings)


def test_registration_profile_cli_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["validate_cdb_simplificado.py", "--registration-profile"])

    assert validator.parse_args().registration_profile is True
