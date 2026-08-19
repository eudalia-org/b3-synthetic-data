import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import profile_cdb_shapes as profiler  # noqa: E402
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-gravame-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def gravame_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 175, "26F00000000010", "2026-06-02", "2026-06-02", "2030-07-12", None)],
            "NUM_IF long, NUM_TIPO_IF long, COD_IF string, DAT_EMISSAO string, "
            "DAT_REGISTRO string, DAT_VENCIMENTO string, DAT_EXCLUSAO string",
        ),
        "COMPLEMENTO_CONTRATO": spark.createDataFrame(
            [(1, 31, "2026-06-02", None)],
            "NUM_IF long, COD_TIPO_CONTRATO long, DAT_INCLUSAO string, DAT_EXCLUSAO string",
        ),
        "IF_GRVM": spark.createDataFrame(
            [(1, 1, 3, None)],
            "NUM_IF long, NUM_ID_TIPO_CONST_GRAVAME long, "
            "NUM_ID_SITUACAO_IF_GRVM long, DAT_EXCLUSAO string",
        ),
        "PARAMETRO_PONTA": spark.createDataFrame(
            [(11, 1, 21, "S", 74, None), (12, 1, 22, "N", 74, None)],
            "NUM_ID_PARAMETRO_PONTA long, NUM_IF long, NUM_CONTA long, "
            "IND_PARTICIPANTE_TITULAR string, NUM_ID_PAPEL long, DAT_EXCLUSAO string",
        ),
        "CONTA": spark.createDataFrame(
            [(21, "26F00000000010P1", "N"), (22, "26F00000000010P2", "N")],
            "NUM_CONTA long, COD_CONTA string, IND_EXCLUIDO string",
        ),
        "OPERACAO": spark.createDataFrame(
            [
                (31, 1, "op520", None, 15394, 6, 11, 12, None),
                (32, 1, "op527", "op520", 15512, 6, 11, 12, None),
            ],
            "NUM_ID_OPERACAO long, NUM_IF long, COD_OPERACAO string, "
            "COD_OPERACAO_ORIGINAL string, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
            "NUM_ID_MODALIDADE_LIQUIDACAO long, NUM_ID_PARAMETRO_PONTA_P1 long, "
            "NUM_ID_PARAMETRO_PONTA_P2 long, DAT_EXCLUSAO string",
        ),
        "LANCAMENTO": spark.createDataFrame(
            [(41, 31, "P1"), (42, 32, "P2")],
            "NUM_ID_LANCAMENTO long, NUM_ID_OPERACAO long, COD_PAPEL_PARTICIPANTE string",
        ),
        "DADO_OPERACAO": spark.createDataFrame(
            [], "NUM_ID_DADO_OPERACAO long, NUM_ID_OPERACAO long"
        ),
        "ARQUIVO_TRANSF": spark.createDataFrame(
            [(51, "Contrato de Garantia.pdf", "application/pdf")],
            "NUM_ID_ARQUIVO_TRANSF long, NOM_ARQUIVO string, NOM_TIPO_MIME string",
        ),
        "ARQUIVO_TRANSF_CONTEUDO": spark.createDataFrame(
            [(51, "blob")], "NUM_ID_ARQUIVO_TRANSF_CONT long, CONTEUDO_ARQUIVO string"
        ),
        "ARQUIVO_IF": spark.createDataFrame(
            [(61, 1, 51, None)],
            "NUM_ID_ARQUIVO_IF long, NUM_IF long, NUM_ID_ARQUIVO_TRANSF long, "
            "DAT_EXCLUSAO string",
        ),
        "PROTOCOLO": spark.createDataFrame(
            [(71, 1, "26FREG00000010", None)],
            "NUM_ID_PROTOCOLO long, NUM_IF long, COD_PROTOCOLO string, DAT_EXCLUSAO string",
        ),
        "GRAVAME_GRAU_PENHOR": spark.createDataFrame(
            [],
            "NUM_ID_GRAVAME_GRAU_PENHOR long, NUM_IF_GRAVAME long, "
            "NUM_IF_GARANTIA long, NUM_GRAU_PENHOR long, DAT_EXCLUSAO string",
        ),
    }


def instrument_backed_tables(spark):
    tables = gravame_tables(spark)
    op = tables["OPERACAO"]
    tables["OPERACAO"] = op.union(
        spark.createDataFrame(
            [
                (33, 900, "op541", "op527", 15464, 6, 11, 12, None),
                (34, 900, "op529", "op541", 15035, 6, 11, 12, None),
            ],
            op.schema,
        )
    )
    launch = tables["LANCAMENTO"]
    tables["LANCAMENTO"] = launch.union(
        spark.createDataFrame([(43, 33, "P1"), (44, 34, "P2")], launch.schema)
    )
    data = tables["DADO_OPERACAO"]
    tables["DADO_OPERACAO"] = spark.createDataFrame(
        [(100 + index, 33 if index < 6 else 34) for index in range(12)], data.schema
    )
    tables["IF_GRVM"] = tables["IF_GRVM"].withColumn(
        "NUM_ID_TIPO_CONST_GRAVAME", validator.F.lit(2)
    )
    tables["GRAVAME_GRAU_PENHOR"] = spark.createDataFrame(
        [(81, 1, 900, 1, None)], tables["GRAVAME_GRAU_PENHOR"].schema
    )
    return tables


def target_frames(spark):
    return {
        "GRAVAME_TIPO_IF": spark.createDataFrame(
            [(175, "GRVM", None)],
            "NUM_TIPO_IF long, COD_TIPO_IF string, DAT_EXCLUSAO string",
        ),
        "GRAVAME_OBJECT_SERVICE": spark.createDataFrame(
            [("GRVM", "S")], "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string"
        ),
        "GRAVAME_ROUTES": spark.createDataFrame(
            [(15394, 1132, "520", "S"), (15512, 1132, "527", "S")],
            "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_OBJETO_SERVICO long, "
            "COD_TIPO_OPERACAO string, IND_DISPONIVEL_IDENTIFICACAO string",
        ),
        "GRAVAME_GUARANTEES": spark.createDataFrame(
            [(900, None, "2030-07-12")],
            "NUM_IF long, DAT_EXCLUSAO string, DAT_VENCIMENTO string",
        ),
    }


def test_gravame_profile_is_isolated(capsys):
    profile = validator.VALIDATION_PROFILES["gravame"]

    assert (profile.pipeline, profile.num_tipo_if) == ("gravame", 175)
    assert (profile.object_service_id, profile.object_service_code) == (1132, "GRVM")
    assert profile.cod_if_pattern is None
    assert not profile.account_check_enabled
    assert profile.unsupported_required() == ()
    assert profiler.PRODUCTS["gravame"] == {"num_tipo_if": 175, "simplified": False}
    assert validator.check_ccb_graph({}, 5, profile) == []
    assert validator.check_lci_graph({}, 5, profile) == []

    validator.emit_report(None, [], None, "error", profile, "/input", [], [])
    assert "product=gravame (NUM_TIPO_IF=175)" in capsys.readouterr().out


def test_gravame_graph_accepts_contract_only_registration(spark):
    findings = by_id(validator.check_gravame_graph(
        gravame_tables(spark), 5, validator.VALIDATION_PROFILES["gravame"]
    ))

    assert all(finding.passed for finding in findings.values()), findings


def test_gravame_metadata_is_partial_without_oracle_and_requires_union_metadata():
    profile = validator.VALIDATION_PROFILES["gravame"]
    assert validator.check_gravame_metadata(
        validator.Metadata(set(), {}, {}, {}, {}), True, profile
    )[0].severity == validator.SEV_WARN

    tables = set(validator.GRAVAME_OUTPUT_TABLES)
    pks = {table: ["ID"] for table in tables}
    pks.pop("IF_GRVM")
    finding = validator.check_gravame_metadata(
        validator.Metadata(tables, pks, {}, {}, {}), False, profile
    )[0]
    assert finding.severity == validator.SEV_ERROR
    assert "IF_GRVM" in finding.message


@pytest.mark.parametrize(
    ("table", "column", "check_id"),
    [
        ("COMPLEMENTO_CONTRATO", "NUM_IF", "2i.contract.edge"),
        ("IF_GRVM", "NUM_IF", "2i.extension.edge"),
        ("PARAMETRO_PONTA", "NUM_IF", "2i.endpoint.edge"),
        ("ARQUIVO_IF", "NUM_IF", "2i.document.edge"),
        ("PROTOCOLO", "NUM_IF", "2i.protocol.edge"),
        ("LANCAMENTO", "NUM_ID_OPERACAO", "2i.launch.edge"),
        ("DADO_OPERACAO", "NUM_ID_OPERACAO", "2i.operation_data.edge"),
    ],
)
def test_gravame_graph_rejects_orphans(spark, table, column, check_id):
    tables = gravame_tables(spark)
    if table == "DADO_OPERACAO":
        tables[table] = spark.createDataFrame([(91, 999)], tables[table].schema)
    else:
        tables[table] = tables[table].withColumn(column, validator.F.lit(999))

    finding = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))[check_id]
    assert finding.severity == validator.SEV_ERROR


def test_gravame_graph_rejects_account_parameter_and_document_breaks(spark):
    tables = gravame_tables(spark)
    tables["PARAMETRO_PONTA"] = tables["PARAMETRO_PONTA"].withColumn(
        "NUM_CONTA", validator.F.lit(999)
    )
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "NUM_ID_PARAMETRO_PONTA_P2", validator.F.lit(999)
    )
    tables["ARQUIVO_IF"] = tables["ARQUIVO_IF"].withColumn(
        "NUM_ID_ARQUIVO_TRANSF", validator.F.lit(999)
    )
    findings = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))

    assert findings["2i.endpoint_account"].severity == validator.SEV_ERROR
    assert findings["2i.operation_endpoint"].severity == validator.SEV_ERROR
    assert findings["2i.document_transfer"].severity == validator.SEV_ERROR


def test_gravame_operation_chain_rejects_missing_original_and_duplicate_code(spark):
    tables = gravame_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO_ORIGINAL",
        validator.F.when(validator.F.col("NUM_ID_OPERACAO") == 32, "missing").otherwise(
            validator.F.col("COD_OPERACAO_ORIGINAL")
        ),
    )
    finding = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.operation_chain"]
    assert finding.severity == validator.SEV_ERROR

    tables = gravame_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO_ORIGINAL",
        validator.F.when(validator.F.col("NUM_ID_OPERACAO") == 31, "op527").otherwise(
            validator.F.col("COD_OPERACAO_ORIGINAL")
        ),
    )
    finding = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.operation_chain"]
    assert finding.severity == validator.SEV_ERROR

    tables = gravame_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "COD_OPERACAO", validator.F.lit("duplicate")
    )
    finding = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.operation_code"]
    assert finding.severity == validator.SEV_ERROR


def test_gravame_pledge_root_is_local_but_guarantee_may_be_external(spark):
    tables = instrument_backed_tables(spark)
    findings = by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))
    assert findings["2i.pledge.edge"].passed
    assert findings["2i.external_operation_target"].passed

    wrong_target = tables["OPERACAO"].withColumn(
        "NUM_IF",
        validator.F.when(validator.F.col("NUM_IF") == 900, 901).otherwise(
            validator.F.col("NUM_IF")
        ),
    )
    tables["OPERACAO"] = wrong_target
    assert by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.external_operation_target"].severity == validator.SEV_ERROR

    tables = instrument_backed_tables(spark)
    tables["OPERACAO"] = tables["OPERACAO"].withColumn(
        "NUM_IF",
        validator.F.when(validator.F.col("NUM_IF") == 900, 1).otherwise(
            validator.F.col("NUM_IF")
        ),
    )
    assert by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.external_operation_target"].severity == validator.SEV_ERROR

    tables = gravame_tables(spark)
    operations = tables["OPERACAO"]
    tables["OPERACAO"] = operations.union(spark.createDataFrame(
        [(35, 900, "orphan541", None, 15464, 6, 11, 12, None)], operations.schema
    ))
    assert by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.operation_route_membership"].severity == validator.SEV_ERROR

    tables = instrument_backed_tables(spark)
    tables["GRAVAME_GRAU_PENHOR"] = tables["GRAVAME_GRAU_PENHOR"].withColumn(
        "NUM_IF_GRAVAME", validator.F.lit(999)
    )
    assert by_id(validator.check_gravame_graph(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["2i.pledge.edge"].severity == validator.SEV_ERROR


def test_gravame_dates_reject_malformed_and_reversed_values(spark):
    tables = gravame_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "DAT_REGISTRO", validator.F.lit("not-a-date")
    ).withColumn("DAT_EMISSAO", validator.F.lit("2031-01-01"))
    tables["COMPLEMENTO_CONTRATO"] = tables["COMPLEMENTO_CONTRATO"].withColumn(
        "DAT_INCLUSAO", validator.F.lit("bad-contract-date")
    )
    findings = by_id(validator.check_gravame_dates(
        tables, 5, validator.VALIDATION_PROFILES["gravame"]
    ))

    assert findings["5i.date_parse"].severity == validator.SEV_ERROR
    assert findings["5i.date_order"].severity == validator.SEV_ERROR


def test_gravame_registration_profile_is_advisory_and_opt_in(spark):
    profile = validator.VALIDATION_PROFILES["gravame"]
    assert validator.check_gravame_registration_profile(
        gravame_tables(spark), 5, False, profile
    ) == []

    tables = gravame_tables(spark)
    tables["GRAVAME_GRAU_PENHOR"] = spark.createDataFrame(
        [(81, 1, 900, 1, None)], tables["GRAVAME_GRAU_PENHOR"].schema
    )
    finding = by_id(validator.check_gravame_registration_profile(
        tables, 5, True, profile
    ))["8i.profile.variant_shape"]
    assert finding.severity == validator.SEV_WARN

    tables = gravame_tables(spark)
    tables["IF_GRVM"] = tables["IF_GRVM"].limit(0)
    findings = by_id(validator.check_gravame_registration_profile(
        tables, 5, True, profile
    ))
    assert findings["8i.profile.common_shape"].severity == validator.SEV_WARN
    assert findings["8i.profile.variant_shape"].severity == validator.SEV_WARN

    tables = gravame_tables(spark)
    tables["DADO_OPERACAO"] = tables["DADO_OPERACAO"].drop("NUM_ID_OPERACAO")
    finding = validator.check_gravame_registration_profile(tables, 5, True, profile)[0]
    assert finding.check_id == "8i.profile.availability"
    assert finding.severity == validator.SEV_WARN


@pytest.mark.parametrize(
    ("frame", "column", "value", "check_id"),
    [
        ("GRAVAME_TIPO_IF", "COD_TIPO_IF", "CCB", "6i.lookup.tipo_if"),
        ("GRAVAME_OBJECT_SERVICE", "IND_PLATAFORMA_BAIXA", "N", "6i.lookup.platform"),
        ("GRAVAME_ROUTES", "NUM_ID_OBJETO_SERVICO", 47,
         "6i.lookup.registration_route"),
    ],
)
def test_gravame_target_rejects_wrong_type_platform_and_route(
    spark, frame, column, value, check_id
):
    frames = target_frames(spark)
    frames[frame] = frames[frame].withColumn(column, validator.F.lit(value))
    findings = by_id(validator.check_gravame_target_frames(
        gravame_tables(spark), frames, 5, validator.VALIDATION_PROFILES["gravame"]
    ))

    assert findings[check_id].severity == validator.SEV_ERROR


def test_gravame_target_ignores_nonregistration_routes(spark):
    tables = instrument_backed_tables(spark)
    frames = target_frames(spark)
    frames["GRAVAME_ROUTES"] = frames["GRAVAME_ROUTES"].union(
        spark.createDataFrame([(15464, 44, "520", "S")], frames["GRAVAME_ROUTES"].schema)
    )

    finding = by_id(validator.check_gravame_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["6i.lookup.registration_route"]
    assert finding.passed


def test_gravame_target_requires_active_non_matured_guarantee(spark):
    tables = instrument_backed_tables(spark)
    frames = target_frames(spark)
    findings = by_id(validator.check_gravame_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["gravame"]
    ))
    assert findings["6i.lookup.guarantee"].passed

    frames["GRAVAME_GUARANTEES"] = frames["GRAVAME_GUARANTEES"].withColumn(
        "DAT_EXCLUSAO", validator.F.lit("2026-06-01")
    )
    finding = by_id(validator.check_gravame_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["gravame"]
    ))["6i.lookup.guarantee"]
    assert finding.severity == validator.SEV_ERROR


def test_gravame_shape_profiler_counts_operation_chain(spark):
    profile = profiler.build_profile(
        instrument_backed_tables(spark), product="gravame", num_tipo_if=175,
        simplified=False,
    )

    assert profile["metrics"] == validator.GRAVAME_SHAPE_METRICS
    assert [metric.name for metric in profiler.metrics_for_product("gravame")] == (
        validator.GRAVAME_SHAPE_METRICS
    )
    assert profile["metrics_skipped"] == []
    assert profile["subtype_map"] == {"status": "not applicable to Gravame"}
    counts = profile["shapes"][0]["counts"]
    assert counts["OPERACAO_GRAVAME_CHAIN"] == 4
    assert counts["LANCAMENTO_GRAVAME_CHAIN"] == 4
    assert counts["DADO_OPERACAO_GRAVAME_CHAIN"] == 12
    assert counts["GRAVAME_GRAU_PENHOR"] == 1


def test_gravame_shape_dispatch_needs_baseline_without_cdb_ratio(spark):
    findings = by_id(validator.check_shapes(
        spark, gravame_tables(spark), None, 5, 1.0, 0.15, 5.0,
        validator.VALIDATION_PROFILES["gravame"],
    ))

    assert findings["7.baseline"].severity == validator.SEV_WARN
    assert "7c.op_ratio" not in findings
