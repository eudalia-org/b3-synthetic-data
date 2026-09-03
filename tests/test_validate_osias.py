import json
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
        SparkSession.builder.appName("validate-osias-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def test_osias_is_opt_in_and_does_not_apply_to_sibling_profiles():
    assert validator.check_osias(
        {}, 5, validator.VALIDATION_PROFILES["cdb"], False
    ) == []
    assert validator.check_osias(
        {}, 5, validator.VALIDATION_PROFILES["cdb_simplificado"], True
    ) == []
    assert validator.check_osias(
        {}, 5, validator.VALIDATION_PROFILES["lca"], True
    ) == []


@pytest.mark.parametrize("product", ["cdb", "ccb", "gravame", "lci"])
def test_osias_fails_closed_when_required_evidence_is_missing(product):
    finding = validator.check_osias(
        {}, 5, validator.VALIDATION_PROFILES[product], True
    )[0]

    assert finding.check_id == f"9.osias.{product}.availability"
    assert finding.severity == validator.SEV_ERROR
    assert not finding.passed


def cdb_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 49, None), (2, 49, None), (3, 49, "2026-01-01")],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string",
        ),
        "TITULO": spark.createDataFrame(
            [(1, None, "8"), (2, "EMISSAO", "0.00"), (3, None, "bad")],
            "NUM_IF long, COD_TIPO_ESCALONAMENTO string, QTD_RESGATADA string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, 20, None), (12, 2, 20, None), (13, 3, 20, None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, "
            "DAT_EXCLUSAO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(11, "COM TABELA", None), (12, "SEM TABELA", None),
             (13, "SEM TABELA", None)],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        ),
        "OPERACAO": spark.createDataFrame(
            [(1, "4509.00"), (2, "4509"), (3, "999")],
            "NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV string",
        ),
    }


def test_osias_cdb_checks_only_resgate_and_escalonamento_roots(spark):
    profile = validator.VALIDATION_PROFILES["cdb"]
    findings = by_id(validator.check_osias(cdb_tables(spark), 5, profile, True))

    assert all(finding.passed for finding in findings.values())

    tables = cdb_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "NUM_ID_TIPO_OPER_OBJETO_SERV",
        validator.F.when(validator.F.col("NUM_IF") == 1, None)
        .when(validator.F.col("NUM_IF") == 2, "4508")
        .otherwise(validator.F.col("NUM_ID_TIPO_OPER_OBJETO_SERV")),
    )
    tables["TITULO"] = tables["TITULO"].withColumn(
        "QTD_RESGATADA",
        validator.F.when(validator.F.col("NUM_IF") == 2, "not-zero")
        .otherwise(validator.F.col("QTD_RESGATADA")),
    )
    findings = by_id(validator.check_osias(tables, 5, profile, True))

    assert findings["9.osias.cdb.resgate.route"].count == 1
    assert findings["9.osias.cdb.escalonamento.route"].count == 1
    assert findings["9.osias.cdb.escalonamento.redeemed_quantity"].count == 1


def test_osias_missing_required_column_is_error(spark):
    tables = cdb_tables(spark)
    tables["TITULO"] = tables["TITULO"].drop("QTD_RESGATADA")

    finding = validator.check_osias(
        tables, 5, validator.VALIDATION_PROFILES["cdb"], True
    )[0]

    assert finding.check_id == "9.osias.cdb.availability"
    assert finding.severity == validator.SEV_ERROR
    assert "TITULO.QTD_RESGATADA" in finding.message


def ccb_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 53, None, None), (2, 53, None, None), (3, 53, None, None),
             (4, 53, None, None), (5, 53, None, None)],
            "NUM_IF long, NUM_TIPO_IF long, NUM_IF_PERTENCE long, DAT_EXCLUSAO string",
        ),
        "ACTPCCB_CONDICAO_IF": spark.createDataFrame(
            [
                (1, " vcp ", " liquidação fora do âmbito b3 "),
                (2, "PREFIXADO", "PAGAMENTO DE PARCELAS FIXAS"),
                (3, "PREFIXADO", "PAGAMENTO DE PARCELAS"),
                (4, "PREFIXADO", "LIQUIDAÇÃO FORA DO ÂMBITO B3"),
                (5, "VCP", "PAGAMENTO DE RENDIMENTO PREFIXADO"),
            ],
            "NUM_IF long, RENT_INDEXADOR_TAXA_FLU string, FORMA_PAGAMENTO string",
        ),
        "OPERACAO": spark.createDataFrame(
            [(1, "871.0", 99), (2, "871", 99), (3, "871", 43), (4, "999", 99),
             (5, "999", 99)],
            "NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV string, "
            "COD_SITUACAO_OPERACAO long",
        ),
    }


def test_osias_ccb_uses_exact_variant_discriminators_and_pppre_status_only(spark):
    profile = validator.VALIDATION_PROFILES["ccb"]
    findings = by_id(validator.check_osias(ccb_tables(spark), 5, profile, True))

    assert all(finding.passed for finding in findings.values())

    tables = ccb_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "NUM_ID_TIPO_OPER_OBJETO_SERV",
        validator.F.when(validator.F.col("NUM_IF") == 1, 872)
        .otherwise(validator.F.col("NUM_ID_TIPO_OPER_OBJETO_SERV")),
    ).withColumn(
        "COD_SITUACAO_OPERACAO",
        validator.F.when(validator.F.col("NUM_IF") == 3, 42)
        .otherwise(validator.F.col("COD_SITUACAO_OPERACAO")),
    )
    findings = by_id(validator.check_osias(tables, 5, profile, True))

    assert findings["9.osias.ccb.favcp.route"].count == 1
    assert findings["9.osias.ccb.pfpre.route"].passed
    assert findings["9.osias.ccb.pppre.route"].passed
    assert findings["9.osias.ccb.pppre.operation_status"].count == 1


def test_osias_gravame_checks_only_owned_routes(spark):
    tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 175, None), (2, 49, None)],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string",
        ),
        "OPERACAO": spark.createDataFrame(
            [(1, "15394.0"), (1, "15512"), (2, "999")],
            "NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV string",
        ),
    }
    profile = validator.VALIDATION_PROFILES["gravame"]
    finding = validator.check_osias(tables, 5, profile, True)[0]
    assert finding.passed

    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "NUM_ID_TIPO_OPER_OBJETO_SERV",
        validator.F.when(validator.F.col("NUM_IF") == 1, 15395)
        .otherwise(validator.F.col("NUM_ID_TIPO_OPER_OBJETO_SERV")),
    )
    assert validator.check_osias(tables, 5, profile, True)[0].count == 2


def lci_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 81, "100.0", None), (2, 81, "200", "2026-01-01")],
            "NUM_IF long, NUM_TIPO_IF long, NUM_ID_LOTE string, DAT_EXCLUSAO string",
        ),
        "CREDITO_SCR": spark.createDataFrame(
            [(10, "100", None), (20, "999", None)],
            "NUM_ID_CREDITO_SCR long, NUM_ID_LOTE string, DAT_EXCLUSAO string",
        ),
        "HISTORICO_CREDITO_SCR": spark.createDataFrame(
            [("10.00",), ("20",)], "NUM_ID_CREDITO_SCR string"
        ),
    }


def test_osias_lci_requires_complete_lot_backing_pairs(spark):
    profile = validator.VALIDATION_PROFILES["lci"]
    findings = by_id(validator.check_osias(lci_tables(spark), 5, profile, True))
    assert findings["9.osias.lci.credit_backing"].passed
    assert findings["9.osias.lci.credit_history"].passed

    tables = lci_tables(spark)
    tables["HISTORICO_CREDITO_SCR"] = tables["HISTORICO_CREDITO_SCR"].where(
        validator.F.col("NUM_ID_CREDITO_SCR") != "20"
    )
    findings = by_id(validator.check_osias(tables, 5, profile, True))
    assert findings["9.osias.lci.credit_backing"].passed
    assert findings["9.osias.lci.credit_history"].count == 1

    tables = lci_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "DAT_EXCLUSAO", validator.F.lit(None).cast("string")
    )
    tables["HISTORICO_CREDITO_SCR"] = tables["HISTORICO_CREDITO_SCR"].limit(0)
    findings = by_id(validator.check_osias(tables, 5, profile, True))

    assert findings["9.osias.lci.credit_backing"].count == 1
    assert findings["9.osias.lci.credit_history"].count == 2


def test_osias_fails_unclassifiable_cdb_and_ccb_roots(spark):
    cdb = cdb_tables(spark)
    cdb["RESGATE"] = cdb["RESGATE"].withColumn(
        "COD_COND_RESGATE",
        validator.F.when(validator.F.col("NUM_CONDICAO_IF") == 12, "UNKNOWN")
        .otherwise(validator.F.col("COD_COND_RESGATE")),
    )
    cdb_findings = by_id(validator.check_osias(
        cdb, 5, validator.VALIDATION_PROFILES["cdb"], True
    ))
    assert cdb_findings["9.osias.cdb.scenario"].count == 1

    ccb = ccb_tables(spark)
    ccb["ACTPCCB_CONDICAO_IF"] = ccb["ACTPCCB_CONDICAO_IF"].withColumn(
        "FORMA_PAGAMENTO",
        validator.F.when(validator.F.col("NUM_IF") == 3, "UNKNOWN")
        .otherwise(validator.F.col("FORMA_PAGAMENTO")),
    )
    ccb_findings = by_id(validator.check_osias(
        ccb, 5, validator.VALIDATION_PROFILES["ccb"], True
    ))
    assert ccb_findings["9.osias.ccb.scenario"].count == 1


def test_osias_cli_and_report_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["validate_products.py", "--product", "cdb", "--osias"])
    assert validator.parse_args().osias

    report_path = tmp_path / "report.json"
    validator.emit_report(
        None, [], str(report_path), "error", validator.VALIDATION_PROFILES["cdb_simplificado"],
        "/input", [], [], osias=True,
    )
    assert json.loads(report_path.read_text())["osias"] is True
