import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import profile_cdb_shapes as profiler  # noqa: E402
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-lci-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def lci_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 81, None, "26G00281662", 100, "2026-07-17", 55, 19, 22, 0,
              1.0, 1.0, 1.0, 1.0)],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string, COD_IF string, "
            "NUM_ID_LOTE long, DAT_REGISTRO string, NUM_SISTEMA long, "
            "NUM_ID_FORMA_PAGAMENTO long, NUM_ID_MOTIVO_SITUACAO_IF long, "
            "COD_SITUACAO_IF long, VAL_NOMINAL_EMISSAO double, VAL_NOMINAL_ATUAL double, "
            "VAL_NOMINAL_EM double, VAL_PU_CURVA double",
        ),
        "TITULO": spark.createDataFrame(
            [(1, 10.0, 2, "N", "ESCRITURAL", None)],
            "NUM_IF long, QTD_EMITIDA double, NUM_ID_TIPO_REGIME_TITULO long, "
            "IND_FRACIONAMENTO string, NOM_FORMA_TITULO string, "
            "COD_TIPO_ESCALONAMENTO string",
        ),
        "CREDITO": spark.createDataFrame([(1,)], "NUM_IF long"),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, "2", None), (12, 1, "20", None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF string, "
            "DAT_EXCLUSAO string",
        ),
        "JUROS_FIXO": spark.createDataFrame(
            [(11, 6.7)], "NUM_CONDICAO_IF long, VAL_TAXA_JUROS_FIXO double"
        ),
        "JUROS_FLUTUANTE": spark.createDataFrame(
            [], "NUM_CONDICAO_IF long, VAL_TAXA_JUROS_FLUTUANTE double"
        ),
        "ATUALIZACAO_POS": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "RESGATE": spark.createDataFrame(
            [(12, "SEM TABELA", None)],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        ),
        "HISTORICO_PU_CURVA": spark.createDataFrame(
            [(21, 1)], "NUM_HISTORICO_PU_CURVA long, NUM_IF long"
        ),
        "EVENTO": spark.createDataFrame(
            [(31, 1, "83"), (32, 1, "85")],
            "NUM_EVENTO long, NUM_IF long, NUM_TIPO_EVENTO_LEGADO string",
        ),
        "DEPOSITO_AUTOMATICO_IF": spark.createDataFrame(
            [(1, "control.0")], "NUM_IF long, NUM_CONTROLE_LANCAMENTO string"
        ),
        "OPERACAO": spark.createDataFrame(
            [(41, 1, 3949, "operation.0", 10.0, 1.0, 10.0)],
            "NUM_ID_OPERACAO long, NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
            "COD_OPERACAO string, QTD_OPERACAO double, VAL_PRECO_UNITARIO double, "
            "VAL_FINANCEIRO double",
        ),
        "DADO_OPERACAO": spark.createDataFrame(
            [(51, 41, 265), (52, 41, 269)],
            "NUM_ID_DADO_OPERACAO long, NUM_ID_OPERACAO long, "
            "NUM_ID_TIPO_DADO_OPERACAO long",
        ),
        "LANCAMENTO": spark.createDataFrame(
            [(61, 41)], "NUM_ID_LANCAMENTO long, NUM_ID_OPERACAO long"
        ),
        "ESPECIFICACAO": spark.createDataFrame(
            [(71, 41)], "NUM_ID_ESPECIFICACAO long, NUM_ID_OPERACAO long"
        ),
        "ESPECIFICACAO_COMITENTE": spark.createDataFrame(
            [(81, 71)],
            "NUM_ID_ESPECIFICACAO_COMITENTE long, NUM_ID_ESPECIFICACAO long",
        ),
        "CARTEIRA_COMITENTE": spark.createDataFrame(
            [(91, 501, 1, 55, 1, 8)],
            "NUM_CARTEIRA_COMITENTE long, NUM_ID_ENTIDADE long, "
            "COD_TIPO_POSICAO_CARTEIRA long, NUM_SISTEMA long, NUM_IF long, "
            "NUM_CONTA_PARTICIPANTE long",
        ),
        "CARTEIRA_PARTICIPANTE": spark.createDataFrame(
            [(101, 1, 55, 1, 8)],
            "NUM_CARTEIRA_PARTICIPANTE long, COD_TIPO_POSICAO_CARTEIRA long, "
            "NUM_SISTEMA long, NUM_IF long, NUM_CONTA_PARTICIPANTE long",
        ),
    }


def target_frames(spark, toggle_period=("2026-01-01", "2026-12-31")):
    return {
        "LCI_TIPO_IF": spark.createDataFrame(
            [(81, "LCI", None)], "NUM_TIPO_IF long, COD_TIPO_IF string, DAT_EXCLUSAO string"
        ),
        "LCI_LOTES": spark.createDataFrame(
            [(100, 1, 10, None)],
            "NUM_ID_LOTE long, NUM_ID_TIPO_LOTE long, NUM_CONTA_PARTICIPANTE long, "
            "DAT_EXCLUSAO string",
        ),
        "LCI_ACCOUNTS": spark.createDataFrame(
            [(10, 2, "L", 1)],
            "NUM_CONTA_PARTICIPANTE long, NUM_ID_SITUACAO_CONTA long, "
            "COD_TIPO_ACESSO string, NUM_ID_AREA_ATUACAO long",
        ),
        "LCI_OBJECT_SERVICE": spark.createDataFrame(
            [("LCI", "S")], "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string"
        ),
        "LCI_ROUTES": spark.createDataFrame(
            [(3949, 75, "1", "S")],
            "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_OBJETO_SERVICO long, "
            "COD_TIPO_OPERACAO string, IND_DISPONIVEL_IDENTIFICACAO string",
        ),
        "LCI_ROOT_CODES": spark.createDataFrame([], "COD_IF string, DAT_EXCLUSAO string"),
        "LCI_TOGGLE": spark.createDataFrame(
            [(validator.LCI_MEU_NUMERO_TOGGLE, "S", *toggle_period)],
            "COD_FTRE_TOG string, IND_FTRE_HAB string, DATA_INIC_VIG_FTRE string, "
            "DATA_FIM_VIG_FTRE string",
        ),
        "LCI_CONTROLS": spark.createDataFrame(
            [], "NUM_CONTROLE_LANCAMENTO string, DAT_EXCLUSAO string"
        ),
        "LCI_OPERATION_CODES": spark.createDataFrame(
            [], "COD_OPERACAO string, DAT_EXCLUSAO string"
        ),
        "LCI_WALLET_COMITENTE": spark.createDataFrame(
            [], "NUM_ID_ENTIDADE long, COD_TIPO_POSICAO_CARTEIRA long, NUM_SISTEMA long, "
            "NUM_IF long, NUM_CONTA_PARTICIPANTE long, DAT_EXCLUSAO string",
        ),
        "LCI_WALLET_PARTICIPANTE": spark.createDataFrame(
            [], "COD_TIPO_POSICAO_CARTEIRA long, NUM_SISTEMA long, NUM_IF long, "
            "NUM_CONTA_PARTICIPANTE long, DAT_EXCLUSAO string",
        ),
    }


def graph_findings(tables):
    return by_id(validator.check_lci_graph(
        tables, 5, validator.VALIDATION_PROFILES["lci"]
    ))


def lookup_findings(tables, frames):
    return by_id(validator.check_lci_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["lci"]
    ))


def test_lci_profile_is_generic_if_but_isolated_from_cdb_and_other_aggregates(capsys):
    profile = validator.VALIDATION_PROFILES["lci"]

    assert profile.pipeline == "lci"
    assert profile.num_tipo_if == 81
    assert profile.object_service_id == 75
    assert profile.object_service_code == "LCI"
    assert profile.default_clone_prefix == "sintetizacao_multiproduto/lci"
    assert profile.simplified_domain
    assert profile.registration_constants is None
    assert profile.unsupported_required() == ()
    assert validator.LCI_MEU_NUMERO_TOGGLE == "VALIDA_MEU_NUMERO_DEPOSITO"
    assert validator.LCI_OUTPUT_TABLES == (
        "INSTRUMENTO_FINANCEIRO", "TITULO", "CREDITO", "CONDICAO_IF",
        "JUROS_FIXO", "JUROS_FLUTUANTE", "ATUALIZACAO_POS", "RESGATE",
        "HISTORICO_PU_CURVA", "EVENTO", "DEPOSITO_AUTOMATICO_IF", "OPERACAO",
        "DADO_OPERACAO", "LANCAMENTO", "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE", "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
    )
    assert profiler.PRODUCTS["lci"] == {"num_tipo_if": 81, "simplified": True}
    assert [metric.name for metric in profiler.metrics_for_product("lci")] == (
        validator.LCI_SHAPE_METRICS
    )
    assert validator.check_credito_scr_graph({}, 5, profile) == []
    assert validator.check_dicre_graph({}, 5, profile) == []

    validator.emit_report(None, [], None, "error", profile, "/input", [], [])
    assert "product=lci (NUM_TIPO_IF=81)" in capsys.readouterr().out


def test_lci_runs_generic_if_identity_and_simplified_domain(spark):
    tables = lci_tables(spark)
    profile = validator.VALIDATION_PROFILES["lci"]

    assert all(finding.passed for finding in validator.check_product_identity(
        tables, profile, 5
    ))
    assert validator.check_domain(
        tables, validator.Metadata(set(), {}, {}, {}, {}), 5, profile
    )[0].passed


def test_lci_requires_all_output_tables_and_live_pk_metadata(spark):
    tables = lci_tables(spark)
    tables.pop("HISTORICO_PU_CURVA")
    assert graph_findings(tables)["2e.output_tables"].severity == validator.SEV_ERROR

    metadata_tables = set(validator.LCI_OUTPUT_TABLES)
    pks = {table: ["ID"] for table in metadata_tables}
    pks.pop("HISTORICO_PU_CURVA")
    finding = validator.check_lci_metadata(
        validator.Metadata(metadata_tables, pks, {}, {}, {}), False,
        validator.VALIDATION_PROFILES["lci"],
    )[0]
    assert finding.severity == validator.SEV_ERROR
    assert "HISTORICO_PU_CURVA" in finding.message


def test_lci_no_oracle_metadata_is_explicit_partial():
    finding = validator.check_lci_metadata(
        validator.Metadata(set(), {}, {}, {}, {}), True,
        validator.VALIDATION_PROFILES["lci"],
    )[0]
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed


def test_lci_root_code_is_exact_trimmed_case_sensitive_and_preserves_dot_zero(spark):
    tables = lci_tables(spark)
    root = tables["INSTRUMENTO_FINANCEIRO"]
    second = root.withColumn("NUM_IF", validator.F.lit(2)).withColumn(
        "COD_IF", validator.F.lit("code")
    )
    third = root.withColumn("NUM_IF", validator.F.lit(3)).withColumn(
        "COD_IF", validator.F.lit("CODE.0")
    )
    tables["INSTRUMENTO_FINANCEIRO"] = root.withColumn(
        "COD_IF", validator.F.lit("code.0")
    ).union(second).union(third)
    assert graph_findings(tables)["2e.root_code"].passed

    tables["INSTRUMENTO_FINANCEIRO"] = root.union(
        root.withColumn("NUM_IF", validator.F.lit(2)).withColumn(
            "COD_IF", validator.F.lit(" 26G00281662 ")
        ).withColumn("DAT_EXCLUSAO", validator.F.lit(""))
    )
    assert graph_findings(tables)["2e.root_code"].count == 2


@pytest.mark.parametrize("table", ["TITULO", "CREDITO"])
def test_lci_requires_exactly_one_title_and_credit(spark, table):
    tables = lci_tables(spark)
    tables[table] = tables[table].limit(0)
    assert graph_findings(tables)[f"2e.one_{table.lower()}"].count == 1

    tables = lci_tables(spark)
    tables[table] = tables[table].union(tables[table])
    assert graph_findings(tables)[f"2e.one_{table.lower()}"].count == 1


@pytest.mark.parametrize(
    ("table", "column", "check_id"),
    [
        ("CONDICAO_IF", "NUM_IF", "2e.condition.edge"),
        ("HISTORICO_PU_CURVA", "NUM_IF", "2e.history.edge"),
        ("EVENTO", "NUM_IF", "2e.event.edge"),
        ("DEPOSITO_AUTOMATICO_IF", "NUM_IF", "2e.deposit.edge"),
        ("OPERACAO", "NUM_IF", "2e.operation.edge"),
        ("CARTEIRA_COMITENTE", "NUM_IF", "2e.wallet_comitente.edge"),
        ("CARTEIRA_PARTICIPANTE", "NUM_IF", "2e.wallet_participante.edge"),
        ("DADO_OPERACAO", "NUM_ID_OPERACAO", "2e.operation_data.edge"),
        ("LANCAMENTO", "NUM_ID_OPERACAO", "2e.launch.edge"),
        ("ESPECIFICACAO", "NUM_ID_OPERACAO", "2e.specification.edge"),
        ("ESPECIFICACAO_COMITENTE", "NUM_ID_ESPECIFICACAO",
         "2e.specification_holder.edge"),
    ],
)
def test_lci_rejects_each_log_proven_graph_orphan(spark, table, column, check_id):
    tables = lci_tables(spark)
    tables[table] = tables[table].withColumn(column, validator.F.lit(999))
    assert graph_findings(tables)[check_id].severity == validator.SEV_ERROR


def test_lci_rejects_duplicate_physical_graph_edges(spark):
    tables = lci_tables(spark)
    tables["LANCAMENTO"] = tables["LANCAMENTO"].union(tables["LANCAMENTO"])
    assert graph_findings(tables)["2e.launch.duplicate"].count == 1


def test_lci_known_condition_polymorphism_accepts_expected_rows(spark):
    findings = by_id(validator.check_lci_polymorphism(
        lci_tables(spark), 5, validator.VALIDATION_PROFILES["lci"]
    ))
    assert findings["2e.condition_polymorphism"].passed
    assert findings["2e.subtype_orphan"].passed


@pytest.mark.parametrize("mode", ["wrong", "missing"])
def test_lci_known_condition_polymorphism_rejects_wrong_or_missing_rows(spark, mode):
    tables = lci_tables(spark)
    tables["JUROS_FIXO"] = tables["JUROS_FIXO"].limit(0)
    if mode == "wrong":
        tables["JUROS_FLUTUANTE"] = spark.createDataFrame(
            [(11, 98.0)], tables["JUROS_FLUTUANTE"].schema
        )
    finding = by_id(validator.check_lci_polymorphism(
        tables, 5, validator.VALIDATION_PROFILES["lci"]
    ))["2e.condition_polymorphism"]
    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_lci_unknown_condition_type_warns_without_hard_failure(spark):
    tables = lci_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].withColumn(
        "COD_TIPO_CONDICAO_IF",
        validator.F.when(validator.F.col("NUM_CONDICAO_IF") == 11, "99").otherwise(
            validator.F.col("COD_TIPO_CONDICAO_IF")
        ),
    )
    findings = by_id(validator.check_lci_polymorphism(
        tables, 5, validator.VALIDATION_PROFILES["lci"]
    ))
    assert findings["2e.condition_polymorphism"].passed
    assert findings["2e.unknown_condition_type"].severity == validator.SEV_WARN


@pytest.mark.parametrize(
    ("frame", "column", "value", "check_id"),
    [
        ("LCI_TIPO_IF", "COD_TIPO_IF", "CDB", "6e.lookup.tipo_if"),
        ("LCI_LOTES", "NUM_ID_TIPO_LOTE", 2, "6e.lookup.lot"),
        ("LCI_ACCOUNTS", "NUM_ID_SITUACAO_CONTA", 3, "6e.lookup.issuer_account"),
        ("LCI_OBJECT_SERVICE", "IND_PLATAFORMA_BAIXA", "N",
         "6e.lookup.object_service"),
        ("LCI_ROUTES", "NUM_ID_OBJETO_SERVICO", 44, "6e.lookup.route"),
        ("LCI_ROUTES", "IND_DISPONIVEL_IDENTIFICACAO", "N", "6e.lookup.route"),
    ],
)
def test_lci_rejects_ineligible_target_lot_account_and_route(
    spark, frame, column, value, check_id
):
    frames = target_frames(spark)
    frames[frame] = frames[frame].withColumn(column, validator.F.lit(value))
    assert lookup_findings(lci_tables(spark), frames)[check_id].severity == validator.SEV_ERROR


def test_lci_detects_exact_active_target_cod_if_collision(spark):
    frames = target_frames(spark)
    frames["LCI_ROOT_CODES"] = spark.createDataFrame(
        [(" 26G00281662 ", None)], frames["LCI_ROOT_CODES"].schema
    )
    assert lookup_findings(lci_tables(spark), frames)[
        "6e.collision.cod_if"
    ].severity == validator.SEV_ERROR


def test_lci_meu_numero_uses_dat_registro_and_detects_target_collision(spark):
    tables = lci_tables(spark)
    frames = target_frames(spark)
    frames["LCI_CONTROLS"] = spark.createDataFrame(
        [("control.0", None)], frames["LCI_CONTROLS"].schema
    )
    finding = lookup_findings(tables, frames)["6e.meu_numero"]
    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_lci_disabled_meu_numero_has_no_target_control_dependency(spark):
    frames = target_frames(spark, ("2025-01-01", "2025-12-31"))
    del frames["LCI_CONTROLS"]
    finding = lookup_findings(lci_tables(spark), frames)["6e.meu_numero"]
    assert finding.passed
    assert "DAT_REGISTRO" in finding.message


def test_lci_operation_code_local_and_target_collisions(spark):
    tables = lci_tables(spark)
    duplicate = tables["OPERACAO"].withColumn("NUM_ID_OPERACAO", validator.F.lit(42))
    tables["OPERACAO"] = tables["OPERACAO"].union(duplicate)
    assert lookup_findings(tables, target_frames(spark))[
        "6e.operation_code.local"
    ].count == 2

    tables = lci_tables(spark)
    frames = target_frames(spark)
    frames["LCI_OPERATION_CODES"] = spark.createDataFrame(
        [("operation.0", None)], frames["LCI_OPERATION_CODES"].schema
    )
    assert lookup_findings(tables, frames)[
        "6e.operation_code.target"
    ].severity == validator.SEV_ERROR


@pytest.mark.parametrize(
    ("target_name", "source_table", "check_id"),
    [
        ("LCI_WALLET_COMITENTE", "CARTEIRA_COMITENTE", "6e.wallet.comitente"),
        ("LCI_WALLET_PARTICIPANTE", "CARTEIRA_PARTICIPANTE",
         "6e.wallet.participante"),
    ],
)
def test_lci_wallet_natural_key_collisions(spark, target_name, source_table, check_id):
    tables = lci_tables(spark)
    frames = target_frames(spark)
    target = frames[target_name]
    source = tables[source_table]
    values = tuple(source.select(*[column for column in target.columns if column != "DAT_EXCLUSAO"])
                   .first()) + (None,)
    frames[target_name] = spark.createDataFrame([values], target.schema)
    assert lookup_findings(tables, frames)[check_id].severity == validator.SEV_ERROR


@pytest.mark.parametrize(
    ("source_table", "check_id"),
    [
        ("CARTEIRA_COMITENTE", "6e.wallet.comitente.local"),
        ("CARTEIRA_PARTICIPANTE", "6e.wallet.participante.local"),
    ],
)
def test_lci_rejects_duplicate_synthetic_wallet_keys(spark, source_table, check_id):
    tables = lci_tables(spark)
    duplicate = tables[source_table].withColumn(
        tables[source_table].columns[0], validator.F.lit(999)
    )
    tables[source_table] = tables[source_table].union(duplicate)

    assert lookup_findings(tables, target_frames(spark))[check_id].severity == validator.SEV_ERROR


def test_lci_wallet_without_active_semantics_is_partial_not_false_pass(spark):
    frames = target_frames(spark)
    frames["LCI_WALLET_PARTICIPANTE"] = frames["LCI_WALLET_PARTICIPANTE"].drop(
        "DAT_EXCLUSAO"
    )
    finding = lookup_findings(lci_tables(spark), frames)["6e.wallet.participante"]
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed


def test_lci_registration_profile_is_opt_in_and_drift_is_advisory(spark):
    tables = lci_tables(spark)
    profile = validator.VALIDATION_PROFILES["lci"]
    assert validator.check_lci_registration_profile(tables, 5, False, profile) == []

    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "NUM_SISTEMA", validator.F.lit(99)
    )
    findings = by_id(validator.check_lci_registration_profile(tables, 5, True, profile))
    assert findings["8e.profile.instrumento_financeiro_constants"].severity == validator.SEV_WARN
    assert findings["8e.profile.condition_topology"].passed
    assert findings["8e.profile.operation_financial_identity"].passed


def test_lci_shape_dispatch_is_supported_without_cdb_hard_ratios(spark):
    findings = by_id(validator.check_shapes(
        spark, lci_tables(spark), None, 5, 1.0, 0.15, 5.0,
        validator.VALIDATION_PROFILES["lci"],
    ))
    assert findings["7.baseline"].severity == validator.SEV_WARN
    assert "7c.op_ratio" not in findings


def test_lci_target_loader_does_not_read_skipped_prefixes(spark, monkeypatch):
    def fail_jdbc(*_args, **_kwargs):
        raise AssertionError("skipped LCI target check attempted JDBC")

    monkeypatch.setattr(validator, "_jdbc", fail_jdbc)
    frames, errors = validator.load_lci_target_frames(
        spark, SimpleNamespace(schema="CETIP"), lci_tables(spark),
        skip_prefixes=(
            "6e.lookup", "6e.collision", "6e.operation_code", "6e.meu_numero",
            "6e.wallet",
        ),
    )

    assert frames == {}
    assert errors == {}
