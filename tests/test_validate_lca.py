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
        SparkSession.builder.appName("validate-lca-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def lca_tables(spark):
    events = (
        [(100 + index, 1, "83") for index in range(10)]
        + [(200 + index, 1, "84") for index in range(9)]
        + [(300, 1, "85")]
    )
    return {
        "ENTIDADE": spark.createDataFrame(
            [(500, "N")], "NUM_ID_ENTIDADE long, IND_EXCLUIDO string"
        ),
        "REPRESENTANTE_IF": spark.createDataFrame(
            [(500, None)], "NUM_ID_ENTIDADE long, DAT_EXCLUSAO string"
        ),
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 96, None, "code.0", 1000, "2026-07-17", 55, 267, 7, 0,
              500.0, 500.0, 500.0, 500.0, "S")],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string, COD_IF string, "
            "NUM_ID_LOTE long, DAT_REGISTRO string, NUM_SISTEMA long, "
            "NUM_ID_FORMA_PAGAMENTO long, NUM_ID_MOTIVO_SITUACAO_IF long, "
            "COD_SITUACAO_IF long, VAL_NOMINAL_EMISSAO double, VAL_NOMINAL_ATUAL double, "
            "VAL_NOMINAL_EM double, VAL_PU_CURVA double, IND_AGENDA_CONSTANTE string",
        ),
        "TITULO": spark.createDataFrame(
            [(1, 1.0, 2, 1, "N", "ESCRITURAL")],
            "NUM_IF long, QTD_EMITIDA double, NUM_ID_TIPO_REGIME_TITULO long, "
            "NUM_ID_VEICULO_GARANTIDOR long, IND_FRACIONAMENTO string, NOM_FORMA_TITULO string",
        ),
        "IF_LCA": spark.createDataFrame(
            [(1, 500, "S", "N")],
            "NUM_IF long, NUM_ID_ENT_DEPOSITARIO_ORIG long, "
            "IND_MANUT_UNILATERAL_GARANTIAS string, IND_LIQUIDACAO_ANTECIPADA string",
        ),
        "CREDITO": spark.createDataFrame(
            [(1, 339, 11)], "NUM_IF long, NUM_ID_MUNICIPIO long, NUM_ID_TIPO_CREDITO long"
        ),
        "GARANTIA": spark.createDataFrame(
            [(900, 1, 16)], "NUM_ID_GARANTIA long, NUM_IF long, NUM_ID_TIPO_GARANTIA long"
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, "1"), (12, 1, "3"), (13, 1, "5"), (14, 1, "20")],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF string",
        ),
        "AMORTIZACAO": spark.createDataFrame(
            [(11, 10.0)], "NUM_CONDICAO_IF long, VAL_TAXA_AMORTIZACAO double"
        ),
        "JUROS_FLUTUANTE": spark.createDataFrame(
            [(12, 4, 100.0)],
            "NUM_CONDICAO_IF long, NUM_INDICE_VALORIZACAO long, "
            "VAL_PERCENTUAL_TAXA_JUROS double",
        ),
        "SPREAD": spark.createDataFrame(
            [(13, 1.56)], "NUM_CONDICAO_IF long, VAL_TAXA_SPREAD double"
        ),
        "RESGATE": spark.createDataFrame(
            [(14, "SEM TABELA")], "NUM_CONDICAO_IF long, COD_COND_RESGATE string"
        ),
        "EVENTO": spark.createDataFrame(
            events, "NUM_EVENTO long, NUM_IF long, NUM_TIPO_EVENTO_LEGADO string"
        ),
        "DEPOSITO_AUTOMATICO_IF": spark.createDataFrame(
            [(1, "control.0")], "NUM_IF long, NUM_CONTROLE_LANCAMENTO string"
        ),
        "OPERACAO": spark.createDataFrame(
            [(41, 1, 8430, "operation.0", "control.0", "control.0")],
            "NUM_ID_OPERACAO long, NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
            "COD_OPERACAO string, NUM_CONTROLE_LANCAMENTO_P1 string, "
            "NUM_CONTROLE_LANCAMENTO_P2 string",
        ),
        "DADO_OPERACAO": spark.createDataFrame(
            [(51, 41, 287), (52, 41, 288)],
            "NUM_ID_DADO_OPERACAO long, NUM_ID_OPERACAO long, NUM_ID_TIPO_DADO_OPERACAO long",
        ),
        "LANCAMENTO": spark.createDataFrame(
            [(61, 41)], "NUM_ID_LANCAMENTO long, NUM_ID_OPERACAO long"
        ),
        "ESPECIFICACAO": spark.createDataFrame(
            [(71, 41)], "NUM_ID_ESPECIFICACAO long, NUM_ID_OPERACAO long"
        ),
        "ESPECIFICACAO_COMITENTE": spark.createDataFrame(
            [(81, 71)], "NUM_ID_ESPECIFICACAO_COMITENTE long, NUM_ID_ESPECIFICACAO long"
        ),
        "CARTEIRA_COMITENTE": spark.createDataFrame(
            [(91, 700, 1, 55, 1, 8)],
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
        "LCA_TIPO_IF": spark.createDataFrame(
            [(96, "LCA", None)], "NUM_TIPO_IF long, COD_TIPO_IF string, DAT_EXCLUSAO string"
        ),
        "LCA_LOTES": spark.createDataFrame(
            [(1000, 2, 10, 96, None)],
            "NUM_ID_LOTE long, NUM_ID_TIPO_LOTE long, NUM_CONTA_PARTICIPANTE long, "
            "NUM_TIPO_IF long, DAT_EXCLUSAO string",
        ),
        # Deliberately no LCI COD_TIPO_ACESSO/NUM_ID_AREA_ATUACAO columns.
        "LCA_ACCOUNTS": spark.createDataFrame(
            [(10, 2)], "NUM_CONTA_PARTICIPANTE long, NUM_ID_SITUACAO_CONTA long"
        ),
        "LCA_MUNICIPALITIES": spark.createDataFrame(
            [(339, 26, "N")], "NUM_ID_MUNICIPIO long, NUM_ID_UF long, IND_EXCLUIDO string"
        ),
        "LCA_UFS": spark.createDataFrame([(26, "N")], "NUM_ID_UF long, IND_EXCLUIDO string"),
        "LCA_OBJECT_SERVICE": spark.createDataFrame(
            [("LCA", "S")], "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string"
        ),
        "LCA_ROUTES": spark.createDataFrame(
            [(8430, 843, "1", "S")],
            "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_OBJETO_SERVICO long, "
            "COD_TIPO_OPERACAO string, IND_DISPONIVEL_IDENTIFICACAO string",
        ),
        "LCA_ROOT_CODES": spark.createDataFrame([], "COD_IF string, DAT_EXCLUSAO string"),
        "LCA_TOGGLE": spark.createDataFrame(
            [(validator.LCA_MEU_NUMERO_TOGGLE, "S", *toggle_period)],
            "COD_FTRE_TOG string, IND_FTRE_HAB string, DATA_INIC_VIG_FTRE string, "
            "DATA_FIM_VIG_FTRE string",
        ),
        "LCA_CONTROLS": spark.createDataFrame(
            [], "NUM_CONTROLE_LANCAMENTO string, DAT_EXCLUSAO string"
        ),
        "LCA_OPERATION_CODES": spark.createDataFrame(
            [], "COD_OPERACAO string, DAT_EXCLUSAO string"
        ),
        "LCA_WALLET_COMITENTE": spark.createDataFrame(
            [], "NUM_ID_ENTIDADE long, COD_TIPO_POSICAO_CARTEIRA long, NUM_SISTEMA long, "
            "NUM_IF long, NUM_CONTA_PARTICIPANTE long"
        ),
        "LCA_WALLET_PARTICIPANTE": spark.createDataFrame(
            [], "COD_TIPO_POSICAO_CARTEIRA long, NUM_SISTEMA long, NUM_IF long, "
            "NUM_CONTA_PARTICIPANTE long, DAT_EXCLUSAO string"
        ),
    }


def graph_findings(tables):
    return by_id(validator.check_lca_graph(tables, 5, validator.VALIDATION_PROFILES["lca"]))


def target_findings(tables, frames):
    return by_id(validator.check_lca_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["lca"]
    ))


def test_lca_profile_inventory_shape_and_isolation(capsys):
    profile = validator.VALIDATION_PROFILES["lca"]
    assert (profile.pipeline, profile.num_tipo_if, profile.object_service_id) == ("lca", 96, 843)
    assert profile.object_service_code == "LCA"
    assert profile.default_clone_prefix == "sintetizacao_multiproduto/lca"
    assert profile.simplified_domain and profile.registration_constants is None
    assert profile.unsupported_required() == ()
    assert len(validator.LCA_OUTPUT_TABLES) == 21
    assert validator.LCA_OUTPUT_TABLES == (
        "ENTIDADE", "REPRESENTANTE_IF", "INSTRUMENTO_FINANCEIRO", "TITULO", "IF_LCA",
        "CREDITO", "GARANTIA", "CONDICAO_IF", "AMORTIZACAO", "JUROS_FLUTUANTE", "SPREAD",
        "RESGATE", "EVENTO", "DEPOSITO_AUTOMATICO_IF", "OPERACAO", "DADO_OPERACAO",
        "LANCAMENTO", "ESPECIFICACAO", "ESPECIFICACAO_COMITENTE", "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
    )
    assert profiler.PRODUCTS["lca"] == {"num_tipo_if": 96, "simplified": True}
    assert [metric.name for metric in profiler.metrics_for_product("lca")] == (
        validator.LCA_SHAPE_METRICS
    )
    assert validator.LCA_SHAPE_METRICS == [
        "ENTIDADE", "REPRESENTANTE_IF", "TITULO", "IF_LCA", "CREDITO", "GARANTIA",
        "CONDICAO_IF", "CONDICAO_IF_TIPO1", "CONDICAO_IF_TIPO3",
        "CONDICAO_IF_TIPO5", "CONDICAO_IF_TIPO20", "AMORTIZACAO",
        "JUROS_FLUTUANTE", "SPREAD", "RESGATE", "EVENTO", "EVENTO_TIPO83",
        "EVENTO_TIPO84", "EVENTO_TIPO85", "DEPOSITO", "OPERACAO", "DADO_OPERACAO",
        "LANCAMENTO", "ESPECIFICACAO", "ESPECIFICACAO_COMITENTE",
        "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE",
    ]
    assert [metric.name for metric in profiler.metrics_for_product("lci")] == (
        validator.LCI_SHAPE_METRICS
    )
    assert validator.check_lci_graph({}, 5, profile) == []
    validator.emit_report(None, [], None, "error", profile, "/input", [], [])
    assert "product=lca (NUM_TIPO_IF=96)" in capsys.readouterr().out


def test_lca_metadata_requires_every_live_table_and_pk():
    tables = set(validator.LCA_OUTPUT_TABLES)
    pks = {table: ["ID"] for table in tables}
    pks.pop("IF_LCA")
    finding = validator.check_lca_metadata(
        validator.Metadata(tables, pks, {}, {}, {}), False,
        validator.VALIDATION_PROFILES["lca"],
    )[0]
    assert finding.severity == validator.SEV_ERROR
    assert "IF_LCA" in finding.message
    assert validator.check_lca_metadata(
        validator.Metadata(set(), {}, {}, {}, {}), True,
        validator.VALIDATION_PROFILES["lca"],
    )[0].severity == validator.SEV_WARN


def test_lca_exact_root_and_one_each_joined_child(spark):
    tables = lca_tables(spark)
    assert graph_findings(tables)["2g.root_code"].passed
    for table in ("TITULO", "IF_LCA", "CREDITO", "GARANTIA"):
        changed = lca_tables(spark)
        changed[table] = changed[table].limit(0)
        assert graph_findings(changed)[f"2g.one_{table.lower()}"].count == 1
    root = tables["INSTRUMENTO_FINANCEIRO"]
    tables["INSTRUMENTO_FINANCEIRO"] = root.union(
        root.withColumn("NUM_IF", validator.F.lit(2)).withColumn(
            "COD_IF", validator.F.lit(" code.0 ")
        )
    )
    assert graph_findings(tables)["2g.root_code"].severity == validator.SEV_ERROR


@pytest.mark.parametrize(
    ("table", "column", "check_id"),
    [
        ("REPRESENTANTE_IF", "NUM_ID_ENTIDADE", "2g.representative.edge"),
        ("IF_LCA", "NUM_ID_ENT_DEPOSITARIO_ORIG", "2g.depositor_origin.edge"),
        ("CONDICAO_IF", "NUM_IF", "2g.condition.edge"),
        ("EVENTO", "NUM_IF", "2g.event.edge"),
        ("DEPOSITO_AUTOMATICO_IF", "NUM_IF", "2g.deposit.edge"),
        ("OPERACAO", "NUM_IF", "2g.operation.edge"),
        ("CARTEIRA_COMITENTE", "NUM_IF", "2g.wallet_comitente.edge"),
        ("CARTEIRA_PARTICIPANTE", "NUM_IF", "2g.wallet_participante.edge"),
        ("DADO_OPERACAO", "NUM_ID_OPERACAO", "2g.operation_data.edge"),
        ("LANCAMENTO", "NUM_ID_OPERACAO", "2g.launch.edge"),
        ("ESPECIFICACAO", "NUM_ID_OPERACAO", "2g.specification.edge"),
        ("ESPECIFICACAO_COMITENTE", "NUM_ID_ESPECIFICACAO",
         "2g.specification_holder.edge"),
    ],
)
def test_lca_rejects_every_graph_orphan(spark, table, column, check_id):
    tables = lca_tables(spark)
    tables[table] = tables[table].withColumn(column, validator.F.lit(999))
    assert graph_findings(tables)[check_id].severity == validator.SEV_ERROR


def test_lca_rejects_condition_subtype_orphan_and_duplicate_edge(spark):
    tables = lca_tables(spark)
    tables["AMORTIZACAO"] = tables["AMORTIZACAO"].withColumn(
        "NUM_CONDICAO_IF", validator.F.lit(999)
    )
    assert graph_findings(tables)["2g.amortizacao.edge"].severity == validator.SEV_ERROR
    tables = lca_tables(spark)
    tables["LANCAMENTO"] = tables["LANCAMENTO"].union(tables["LANCAMENTO"])
    assert graph_findings(tables)["2g.launch.duplicate"].severity == validator.SEV_ERROR


def test_lca_known_and_unknown_polymorphism(spark):
    profile = validator.VALIDATION_PROFILES["lca"]
    tables = lca_tables(spark)
    assert by_id(validator.check_lca_polymorphism(tables, 5, profile))[
        "2g.condition_polymorphism"
    ].passed
    tables["SPREAD"] = tables["SPREAD"].limit(0)
    tables["RESGATE"] = spark.createDataFrame(
        [(13, "SEM TABELA"), (14, "SEM TABELA")], tables["RESGATE"].schema
    )
    assert by_id(validator.check_lca_polymorphism(tables, 5, profile))[
        "2g.condition_polymorphism"
    ].severity == validator.SEV_ERROR
    tables = lca_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].withColumn(
        "COD_TIPO_CONDICAO_IF",
        validator.F.when(validator.F.col("NUM_CONDICAO_IF") == 11, "99").otherwise(
            validator.F.col("COD_TIPO_CONDICAO_IF")
        ),
    )
    findings = by_id(validator.check_lca_polymorphism(tables, 5, profile))
    assert findings["2g.unknown_condition_type"].severity == validator.SEV_WARN


@pytest.mark.parametrize(
    ("frame", "column", "value", "check_id"),
    [
        ("LCA_TIPO_IF", "COD_TIPO_IF", "LCI", "6g.lookup.tipo_if"),
        ("LCA_LOTES", "NUM_ID_TIPO_LOTE", 1, "6g.lookup.lot"),
        ("LCA_LOTES", "NUM_ID_TIPO_LOTE", None, "6g.lookup.lot"),
        ("LCA_LOTES", "NUM_TIPO_IF", 81, "6g.lookup.lot_root_type"),
        ("LCA_LOTES", "NUM_TIPO_IF", None, "6g.lookup.lot_root_type"),
        ("LCA_ACCOUNTS", "NUM_ID_SITUACAO_CONTA", 3, "6g.lookup.issuer_account"),
        ("LCA_MUNICIPALITIES", "IND_EXCLUIDO", "S", "6g.lookup.municipality"),
        ("LCA_UFS", "IND_EXCLUIDO", "S", "6g.lookup.uf"),
        ("LCA_OBJECT_SERVICE", "IND_PLATAFORMA_BAIXA", "N", "6g.lookup.object_service"),
        ("LCA_ROUTES", "NUM_ID_OBJETO_SERVICO", 75, "6g.lookup.route"),
    ],
)
def test_lca_target_eligibility_negative_frames(spark, frame, column, value, check_id):
    frames = target_frames(spark)
    frames[frame] = frames[frame].withColumn(column, validator.F.lit(value))
    assert target_findings(lca_tables(spark), frames)[check_id].severity == validator.SEV_ERROR


def test_lca_target_positive_has_no_lci_access_area_assumption(spark):
    findings = target_findings(lca_tables(spark), target_frames(spark))
    assert all(not finding.table.startswith("LCI_") for finding in findings.values())
    for check_id in (
        "6g.lookup.tipo_if", "6g.lookup.lot", "6g.lookup.lot_root_type",
        "6g.lookup.issuer_account", "6g.lookup.municipality", "6g.lookup.uf",
        "6g.lookup.object_service", "6g.lookup.route",
    ):
        assert findings[check_id].passed, findings[check_id]


def test_lca_missing_lot_root_type_is_unavailable_not_false_pass(spark):
    frames = target_frames(spark)
    frames["LCA_LOTES"] = frames["LCA_LOTES"].drop("NUM_TIPO_IF")

    finding = target_findings(lca_tables(spark), frames)["6g.lookup.lot_root_type"]

    assert finding.severity == validator.SEV_WARN
    assert not finding.passed


def test_lca_code_collisions_and_toggle_dat_registro(spark):
    tables = lca_tables(spark)
    frames = target_frames(spark)
    frames["LCA_ROOT_CODES"] = spark.createDataFrame(
        [(" code.0 ", None)], frames["LCA_ROOT_CODES"].schema
    )
    frames["LCA_OPERATION_CODES"] = spark.createDataFrame(
        [("operation.0", None)], frames["LCA_OPERATION_CODES"].schema
    )
    frames["LCA_CONTROLS"] = spark.createDataFrame(
        [("control.0", None)], frames["LCA_CONTROLS"].schema
    )
    findings = target_findings(tables, frames)
    assert findings["6g.collision.cod_if"].severity == validator.SEV_ERROR
    assert findings["6g.operation_code.target"].severity == validator.SEV_ERROR
    assert findings["6g.meu_numero"].severity == validator.SEV_ERROR
    frames = target_frames(spark, ("2025-01-01", "2025-12-31"))
    del frames["LCA_CONTROLS"]
    finding = target_findings(tables, frames)["6g.meu_numero"]
    assert finding.passed and "DAT_REGISTRO" in finding.message


@pytest.mark.parametrize(
    ("target_name", "source_table", "check_id"),
    [
        ("LCA_WALLET_COMITENTE", "CARTEIRA_COMITENTE", "6g.wallet.comitente"),
        ("LCA_WALLET_PARTICIPANTE", "CARTEIRA_PARTICIPANTE", "6g.wallet.participante"),
    ],
)
def test_lca_wallet_local_and_target_collisions(spark, target_name, source_table, check_id):
    tables = lca_tables(spark)
    frames = target_frames(spark)
    target = frames[target_name]
    source = tables[source_table]
    values = tuple(source.select(*[column for column in target.columns
                                   if column != "DAT_EXCLUSAO"]).first())
    if "DAT_EXCLUSAO" in target.columns:
        values += (None,)
    frames[target_name] = spark.createDataFrame([values], target.schema)
    assert target_findings(tables, frames)[check_id].severity == validator.SEV_ERROR
    duplicate = source.withColumn(source.columns[0], validator.F.lit(999))
    tables[source_table] = source.union(duplicate)
    assert target_findings(tables, target_frames(spark))[
        f"{check_id}.local"
    ].severity == validator.SEV_ERROR


def test_lca_registration_profile_is_opt_in_and_event_counts_are_advisory(spark):
    tables = lca_tables(spark)
    profile = validator.VALIDATION_PROFILES["lca"]
    assert validator.check_lca_registration_profile(tables, 5, False, profile) == []
    findings = by_id(validator.check_lca_registration_profile(tables, 5, True, profile))
    assert findings["8g.profile.event_dml_counts"].passed
    assert "logger says 8" in findings["8g.profile.event_dml_counts"].message
    tables["EVENTO"] = tables["EVENTO"].limit(19)
    finding = by_id(validator.check_lca_registration_profile(tables, 5, True, profile))[
        "8g.profile.event_dml_counts"
    ]
    assert finding.severity == validator.SEV_WARN


def test_lca_registration_profile_reports_missing_columns_without_crashing(spark):
    tables = lca_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].drop("COD_TIPO_CONDICAO_IF")
    tables["OPERACAO"] = tables["OPERACAO"].drop("NUM_ID_OPERACAO")

    findings = by_id(validator.check_lca_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["lca"]
    ))

    assert findings["8g.profile.condition_topology"].severity == validator.SEV_WARN
    assert findings["8g.profile.async_closure"].severity == validator.SEV_WARN
    assert findings["8g.profile.operation_data_types"].severity == validator.SEV_WARN


def test_lca_registration_profile_reports_event_coverage_independently(spark):
    tables = lca_tables(spark)
    tables["EVENTO"] = tables["EVENTO"].drop("NUM_TIPO_EVENTO_LEGADO")

    findings = by_id(validator.check_lca_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["lca"]
    ))

    for check_id in (
        "8g.profile.event_dml_counts", "8g.profile.async_closure",
        "8g.profile.operation_data_types",
    ):
        assert findings[check_id].severity == validator.SEV_WARN


def test_lca_shape_profiler_prints_without_cdb_reference_sections(spark, capsys):
    profile = profiler.build_profile(
        lca_tables(spark), product="lca", num_tipo_if=96, simplified=True
    )

    profiler.print_report(profile, "lca-test", 3)
    output = capsys.readouterr().out

    assert "Reference shape (cetip.out write-set)" not in output
    assert "By comitente-simplificado flag" not in output


def test_lca_shape_dispatch_and_prefix_aware_loader_no_jdbc(spark, monkeypatch):
    findings = by_id(validator.check_shapes(
        spark, lca_tables(spark), None, 5, 1.0, 0.15, 5.0,
        validator.VALIDATION_PROFILES["lca"],
    ))
    assert findings["7.baseline"].severity == validator.SEV_WARN
    assert "7c.op_ratio" not in findings

    def fail_jdbc(*_args, **_kwargs):
        raise AssertionError("skipped LCA target check attempted JDBC")

    monkeypatch.setattr(validator, "_jdbc", fail_jdbc)
    frames, errors = validator.load_lca_target_frames(
        spark, SimpleNamespace(schema="CETIP"), lca_tables(spark), skip_prefixes=("6g.",)
    )
    assert frames == {}
    assert errors == {}
