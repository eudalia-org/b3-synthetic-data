import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-products-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def domain_tables(spark, product="cdb_simplificado"):
    tipo = 50 if product == "rdb" else 49
    simplified = product == "cdb_simplificado"
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, tipo, None)], "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string"
        ),
        "TITULO": spark.createDataFrame(
            [(1, None if simplified else "ESCALONADO")],
            "NUM_IF long, COD_TIPO_ESCALONAMENTO string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, 20, None), (12, 1, 3, None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF long, "
            "DAT_EXCLUSAO string",
        ),
        "RESGATE": spark.createDataFrame(
            [(11, "SEM TABELA" if simplified else "OUTRA", None)],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        ),
        "DEPOSITO_AUTOMATICO_IF": spark.createDataFrame([(1,)], "NUM_IF long"),
        "OPERACAO": spark.createDataFrame(
            [(101, 1)], "NUM_ID_OPERACAO long, NUM_IF long"
        ),
        "DADO_OPERACAO": spark.createDataFrame([(101,)], "NUM_ID_OPERACAO long"),
        "LANCAMENTO": spark.createDataFrame([(101,)], "NUM_ID_OPERACAO long"),
        "ESPECIFICACAO": spark.createDataFrame(
            [(101, 1001)], "NUM_ID_OPERACAO long, NUM_ID_ESPECIFICACAO long"
        ),
        "ESPECIFICACAO_COMITENTE": spark.createDataFrame(
            [(1001,)], "NUM_ID_ESPECIFICACAO long"
        ),
    }


def test_profiles_are_explicit_and_rdb_does_not_inherit_cdb_defaults():
    simplificado = validator.VALIDATION_PROFILES["cdb_simplificado"]
    cdb = validator.VALIDATION_PROFILES["cdb"]
    rdb = validator.VALIDATION_PROFILES["rdb"]

    assert (simplificado.num_tipo_if, cdb.num_tipo_if, rdb.num_tipo_if) == (49, 49, 50)
    assert (simplificado.object_service_id, cdb.object_service_id, rdb.object_service_id) == (
        44, 44, 45,
    )
    assert rdb.object_service_code is None
    assert rdb.cod_if_pattern == r"^[A-Z0-9 -]{1,14}$"
    assert rdb.sem_modalidade_ids is None
    assert cdb.unsupported_required() == (validator.CAP_POLYMORPHISM,)
    assert set(rdb.unsupported_required()) == {
        validator.CAP_POLYMORPHISM,
        validator.CAP_LOOKUP_TOS,
        validator.CAP_PLATFORM,
        validator.CAP_ACCOUNT,
        validator.CAP_MODALIDADE,
        validator.CAP_COD_IF_FORMAT,
        validator.CAP_SHAPE,
        validator.CAP_REGISTRATION_PROFILE,
    }


def test_condition_type_inventory_matches_application_constants():
    assert set(validator.EXPECTED_CONDICAO_TYPE_CODES) == {
        "1", "2", "3", "4", "5", "6", "7", "8", "14", "15", "16", "17",
        "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
        "29", "30",
    }
    assert set(validator.UNMAPPED_CONDICAO_TYPE_CODES) == {
        "8", "18", "19", "25", "26", "27", "28", "29", "30",
    }


def test_input_resolution_is_product_aware(monkeypatch):
    monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://bucket/root/")
    monkeypatch.delenv("DATAGEN_CLONE_PREFIX", raising=False)
    monkeypatch.delenv("DATAGEN_SYNTHETIC_PREFIX", raising=False)

    assert validator.resolve_input_base(
        validator.VALIDATION_PROFILES["rdb"], None
    ) == "oci://bucket/root/sintetizacao_multiproduto/rdb_completo"
    monkeypatch.setenv("DATAGEN_CLONE_PREFIX", "/custom/run/")
    assert validator.resolve_input_base(
        validator.VALIDATION_PROFILES["cdb"], None
    ) == "oci://bucket/root/custom/run"
    assert validator.resolve_input_base(
        validator.VALIDATION_PROFILES["cdb"], " /explicit/input/ "
    ) == "/explicit/input"


def test_legacy_input_prefix_is_rejected_for_rdb(monkeypatch):
    monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://bucket/root")
    monkeypatch.delenv("DATAGEN_CLONE_PREFIX", raising=False)
    monkeypatch.setenv("DATAGEN_SYNTHETIC_PREFIX", "legacy")

    with pytest.raises(SystemExit, match="not supported"):
        validator.resolve_input_base(validator.VALIDATION_PROFILES["rdb"], None)


def test_validator_cli_requires_product(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["validate_products.py"])
    with pytest.raises(SystemExit):
        validator.parse_args()

    monkeypatch.setattr(
        sys, "argv", ["validate_products.py", "--product", "rdb"]
    )
    assert validator.parse_args().product == "rdb"


def test_identity_rejects_foreign_and_mixed_root_types(spark):
    root = spark.createDataFrame(
        [(1, 50, None), (2, 49, None)],
        "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string",
    )
    findings = by_id(validator.check_product_identity(
        {"INSTRUMENTO_FINANCEIRO": root}, validator.VALIDATION_PROFILES["rdb"], 5
    ))

    assert findings["0.identity.type"].passed
    assert findings["0.identity.mixed"].severity == validator.SEV_ERROR
    assert findings["0.identity.mixed"].sample == ["49"]


@pytest.mark.parametrize("product", ["cdb_simplificado", "cdb", "rdb"])
def test_domain_accepts_each_product_profile(spark, product):
    finding = validator.check_domain(
        domain_tables(spark, product),
        validator.Metadata(set(), {}, {}, {}, {}),
        5,
        validator.VALIDATION_PROFILES[product],
    )[0]

    assert finding.passed


def test_simplificado_domain_uses_exists_semantics_for_closure_rows(spark):
    tables = domain_tables(spark)
    tables["TITULO"] = tables["TITULO"].union(
        spark.createDataFrame([(1, "ESCALONADO")], tables["TITULO"].schema)
    )
    tables["RESGATE"] = tables["RESGATE"].union(
        spark.createDataFrame([(11, "OUTRA", None)], tables["RESGATE"].schema)
    )

    eligible, missing = validator.build_eligible_num_ifs(
        tables, validator.VALIDATION_PROFILES["cdb_simplificado"]
    )

    assert missing == []
    assert [row.NUM_IF for row in eligible.collect()] == [1]


@pytest.mark.parametrize(
    "table",
    [
        "TITULO",
        "RESGATE",
        "DEPOSITO_AUTOMATICO_IF",
        "DADO_OPERACAO",
        "LANCAMENTO",
        "ESPECIFICACAO_COMITENTE",
    ],
)
def test_domain_rejects_an_if_missing_any_required_exists_cluster(spark, table):
    tables = domain_tables(spark)
    tables[table] = tables[table].limit(0)

    finding = validator.check_domain(
        tables,
        validator.Metadata(set(), {}, {}, {}, {}),
        5,
        validator.VALIDATION_PROFILES["cdb_simplificado"],
    )[0]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_domain_reports_missing_mandatory_columns_instead_of_crashing(spark):
    tables = domain_tables(spark)
    tables["TITULO"] = tables["TITULO"].drop("COD_TIPO_ESCALONAMENTO")

    finding = validator.check_domain(
        tables,
        validator.Metadata(set(), {}, {}, {}, {}),
        5,
        validator.VALIDATION_PROFILES["cdb_simplificado"],
    )[0]

    assert finding.check_id == "2.domain.availability"
    assert finding.severity == validator.SEV_WARN
    assert "TITULO.COD_TIPO_ESCALONAMENTO" in finding.message


def test_primary_key_and_clone_map_checks_detect_duplicates_and_dangling_rows(spark):
    tables = {
        "TITULO": spark.createDataFrame([(1,), (1,)], "NUM_IF long"),
    }
    meta = validator.Metadata({"TITULO"}, {"TITULO": ["NUM_IF"]}, {}, {}, {})
    pk = by_id(validator.check_primary_keys(tables, meta, sample=5, no_oracle=False))
    assert pk["3b.pk_unique"].severity == validator.SEV_ERROR
    assert pk["3b.pk_unique"].count == 1

    clone_tables = {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(101, 49), (102, 49)], "NUM_IF long, NUM_TIPO_IF long"
        ),
        "MAPA_CLONE_NUM_IF": spark.createDataFrame(
            [(1, 101, 1), (2, 103, 1)], "NUM_IF_ORIG long, NUM_IF_NOVO long, K long"
        ),
    }
    clone = by_id(validator.check_clone_map(
        clone_tables, validator.VALIDATION_PROFILES["cdb_simplificado"], 5
    ))
    assert clone["3c.clone_map_covers_roots"].count == 1
    assert clone["3c.clone_map_no_dangling"].count == 1


def test_report_exit_codes_distinguish_pass_partial_and_fail(capsys):
    simplificado = validator.VALIDATION_PROFILES["cdb_simplificado"]
    rdb = validator.VALIDATION_PROFILES["rdb"]
    failure = validator.Finding(
        "0.identity", "Product identity", validator.SEV_ERROR, "ROOT", False
    )
    unavailable = validator.Finding(
        "2.domain.availability", "Domain conformance", validator.SEV_WARN, "TITULO", False,
        message="Domain eligibility unavailable.",
    )

    assert validator.emit_report(None, [], None, "error", simplificado, "/input", [], []) == 0
    assert validator.emit_report(None, [], None, "error", rdb, "/input", [], []) == 1
    assert validator.emit_report(
        None, [unavailable], None, "error", simplificado, "/input", [], []
    ) == 1
    assert validator.emit_report(
        None, [failure], None, "error", simplificado, "/input", [], []
    ) == 1
    output = capsys.readouterr().out
    assert "VERDICT=PASS" in output
    assert "VERDICT=PARTIAL" in output
    assert "VERDICT=FAIL" in output


def test_fully_skipped_group_is_not_executed():
    executed = False

    def operation():
        nonlocal executed
        executed = True
        return []

    assert validator._run_check_group("category 7", ("7.",), ["7."], operation) == []
    assert not executed


def test_required_lookup_group_skip_avoids_frame_access():
    assert validator.check_required_lookup_frames(
        {}, None, None, None, None, 5,
        validator.VALIDATION_PROFILES["cdb_simplificado"],
        skip_prefixes=["6.required"],
    ) == []


def test_supplied_baseline_contract_is_non_skippable(monkeypatch):
    monkeypatch.setattr(
        validator,
        "parse_args",
        lambda: SimpleNamespace(
            product="cdb_simplificado",
            skip_check=["7"],
            shape_baseline="baseline.json",
        ),
    )

    with pytest.raises(SystemExit, match="baseline contract is non-skippable"):
        validator.main()


def test_fk_group_skip_avoids_referential_actions(spark):
    child = spark.createDataFrame([(1,)], "PARENT_ID long")
    fk = validator.ForeignKey("FK_CHILD", "CHILD", ("PARENT_ID",), "PARENT", ("ID",))
    meta = validator.Metadata({"CHILD"}, {}, {}, {}, {"CHILD": [fk]})
    cfg = validator.Config("/input", None, None, "", "CETIP")

    findings, faltantes = validator.check_referential(
        spark, cfg, {"CHILD": child}, meta, 5, "union", 100, ["3.fk_"]
    )

    assert findings == []
    assert faltantes == []
