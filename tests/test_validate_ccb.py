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
        SparkSession.builder.appName("validate-ccb-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def ccb_tables(spark):
    return {
        "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
            [(1, 53, None, "25K00003006", "2026-08-14", "2026-08-14",
              "2035-08-17", 8, "N", 55)],
            "NUM_IF long, NUM_TIPO_IF long, DAT_EXCLUSAO string, COD_IF string, "
            "DAT_EMISSAO string, DAT_REGISTRO string, DAT_VENCIMENTO string, "
            "NUM_ID_FORMA_PAGAMENTO long, IND_AGENDA_CONSTANTE string, NUM_SISTEMA long",
        ),
        "TITULO": spark.createDataFrame([(1,)], "NUM_IF long"),
        "CREDITO": spark.createDataFrame(
            [(1, "2026-08-14", "2035-08-17")],
            "NUM_IF long, DAT_INICIO_RENTABILIDADE string, DAT_VENCIMENTO_CREDITO string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [(11, 1, "4", None), (12, 1, "2", None)],
            "NUM_CONDICAO_IF long, NUM_IF long, COD_TIPO_CONDICAO_IF string, "
            "DAT_EXCLUSAO string",
        ),
        "AMORTIZACAO": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "ATUALIZACAO_POS": spark.createDataFrame([(11,)], "NUM_CONDICAO_IF long"),
        "ATUALIZACAO_PRE": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "JUROS_FIXO": spark.createDataFrame([(12, 17.479)],
                                             "NUM_CONDICAO_IF long, VAL_TAXA_JUROS_FIXO double"),
        "SPREAD": spark.createDataFrame([], "NUM_CONDICAO_IF long"),
        "RESGATE": spark.createDataFrame(
            [], "NUM_CONDICAO_IF long, DAT_RESGATE string, COD_TIPO_EXERCICIO string"
        ),
        "HISTORICO_PU_CURVA": spark.createDataFrame(
            [(21, 1)], "NUM_HISTORICO_PU_CURVA long, NUM_IF long"
        ),
        "HISTORICO_IF_TITULO": spark.createDataFrame(
            [(31, "25K00003006")], "NUM_ID_HISTORICO_IF_TITULO long, COD_IF string"
        ),
        "ALTERACAO_IF": spark.createDataFrame(
            [(41, 1, "R")], "NUM_ID_ALTERACAO_IF long, NUM_IF long, COD_TIPO_ALTERACAO string"
        ),
        "TCTPIF_CCB": spark.createDataFrame(
            [(51, 1, "N", None)],
            "NUM_ID_IF_CCB long, NUM_IF long, IND_BAIXA_VENCIMENTO string, "
            "DTHR_EXCLUSAO string",
        ),
        "TCTPCRONOGRAMA_CCB": spark.createDataFrame(
            [(61, 1, "90", "2027-08-17", "2027-08-17", "2027-08-17", None,
              "parcel.1", 19289.0, 2143.0)],
            "NUM_EVENTO_CCB long, NUM_IF long, NUM_TIPO_EVENTO_LEGADO string, "
            "DATA_ORIGINAL_EVENTO string, DATA_OCORRENCIA_EVENTO string, "
            "DATA_LIQUIDACAO string, DATA_EXCLUSAO string, COD_PARCELA string, "
            "VAL_EVENTO double, VAL_PU_EVENTO double",
        ),
        "OPERACAO": spark.createDataFrame(
            [(71, 1, 871, 6, "operation.0", 1, 2, 1.0)],
            "NUM_ID_OPERACAO long, NUM_IF long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
            "NUM_ID_MODALIDADE_LIQUIDACAO long, COD_OPERACAO string, "
            "COD_TIPO_DEBITO_P1 long, COD_TIPO_DEBITO_P2 long, QTD_OPERACAO double",
        ),
        "LANCAMENTO": spark.createDataFrame(
            [(81, 71)], "NUM_ID_LANCAMENTO long, NUM_ID_OPERACAO long"
        ),
        "GARANTIA": spark.createDataFrame([], "NUM_ID_GARANTIA long, NUM_IF long"),
        "TCTPCADEIA_IPOC": spark.createDataFrame(
            [],
            "NUM_CADEIA_IPOC long, NUM_IPOC_ANTERIOR long, NUM_IF long, "
            "DATA_EXCLUSAO string",
        ),
    }


def target_frames(spark):
    return {
        "CCB_TIPO_IF": spark.createDataFrame(
            [(53, "CCB", None)], "NUM_TIPO_IF long, COD_TIPO_IF string, DAT_EXCLUSAO string"
        ),
        "CCB_OBJECT_SERVICE": spark.createDataFrame(
            [("CCB", "S")], "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string"
        ),
        "CCB_ROUTES": spark.createDataFrame(
            [(871, 47, "1", "S")],
            "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_OBJETO_SERVICO long, "
            "COD_TIPO_OPERACAO string, IND_DISPONIVEL_IDENTIFICACAO string",
        ),
    }


def test_ccb_profile_is_isolated_and_explicit(capsys):
    profile = validator.VALIDATION_PROFILES["ccb"]

    assert (profile.pipeline, profile.num_tipo_if) == ("ccb", 53)
    assert (profile.object_service_id, profile.object_service_code) == (47, "CCB")
    assert profile.cod_if_pattern is None
    assert not profile.sic_enabled
    assert not profile.account_check_enabled
    assert profile.unsupported_required() == ()
    assert profiler.PRODUCTS["ccb"] == {"num_tipo_if": 53, "simplified": False}
    assert validator.check_lci_graph({}, 5, profile) == []
    assert validator.check_lca_graph({}, 5, profile) == []

    validator.emit_report(None, [], None, "error", profile, "/input", [], [])
    assert "product=ccb (NUM_TIPO_IF=53)" in capsys.readouterr().out


def test_ccb_graph_accepts_observed_no_resgate_registration(spark):
    findings = by_id(validator.check_ccb_graph(
        ccb_tables(spark), 5, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert all(finding.passed for finding in findings.values()), findings


def test_ccb_metadata_is_partial_without_oracle_and_requires_union_metadata():
    profile = validator.VALIDATION_PROFILES["ccb"]
    assert validator.check_ccb_metadata(
        validator.Metadata(set(), {}, {}, {}, {}), True, profile
    )[0].severity == validator.SEV_WARN

    tables = set(validator.CCB_OUTPUT_TABLES)
    pks = {table: ["ID"] for table in tables if table != "HISTORICO_PU_CURVA"}
    pks.pop("TCTPIF_CCB")
    finding = validator.check_ccb_metadata(
        validator.Metadata(tables, pks, {}, {}, {}), False, profile
    )[0]
    assert finding.severity == validator.SEV_ERROR
    assert "TCTPIF_CCB" in finding.message


@pytest.mark.parametrize(
    ("table", "column", "check_id"),
    [
        ("TITULO", "NUM_IF", "2h.title.edge"),
        ("CREDITO", "NUM_IF", "2h.credit.edge"),
        ("CONDICAO_IF", "NUM_IF", "2h.condition.edge"),
        ("TCTPIF_CCB", "NUM_IF", "2h.ccb_extension.edge"),
        ("TCTPCRONOGRAMA_CCB", "NUM_IF", "2h.schedule.edge"),
        ("OPERACAO", "NUM_IF", "2h.operation.edge"),
        ("LANCAMENTO", "NUM_ID_OPERACAO", "2h.launch.edge"),
    ],
)
def test_ccb_graph_rejects_orphans(spark, table, column, check_id):
    tables = ccb_tables(spark)
    tables[table] = tables[table].withColumn(column, validator.F.lit(999))

    assert by_id(validator.check_ccb_graph(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))[check_id].severity == validator.SEV_ERROR


def test_ccb_history_code_and_ipoc_parent_stay_inside_root(spark):
    tables = ccb_tables(spark)
    tables["HISTORICO_IF_TITULO"] = tables["HISTORICO_IF_TITULO"].withColumn(
        "COD_IF", validator.F.lit("OTHER")
    )
    assert by_id(validator.check_ccb_graph(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))["2h.title_history.edge"].severity == validator.SEV_ERROR

    tables = ccb_tables(spark)
    tables["TCTPCADEIA_IPOC"] = spark.createDataFrame(
        [(91, 999, 1, None)], tables["TCTPCADEIA_IPOC"].schema
    )
    assert by_id(validator.check_ccb_graph(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))["2h.ipoc_parent"].severity == validator.SEV_ERROR


def test_ccb_blank_or_duplicate_root_codes_are_advisory(spark):
    tables = ccb_tables(spark)
    root = tables["INSTRUMENTO_FINANCEIRO"]
    tables["INSTRUMENTO_FINANCEIRO"] = root.union(
        root.withColumn("NUM_IF", validator.F.lit(2))
    )

    finding = by_id(validator.check_ccb_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["ccb"]
    ))["8h.profile.root_code"]
    assert finding.severity == validator.SEV_WARN
    assert finding.count == 2


def test_ccb_deleted_schedule_rows_do_not_create_graph_or_variant_failures(spark):
    tables = ccb_tables(spark)
    tables["TCTPCRONOGRAMA_CCB"] = tables["TCTPCRONOGRAMA_CCB"].withColumn(
        "NUM_IF", validator.F.lit(999)
    ).withColumn("DATA_EXCLUSAO", validator.F.lit("2026-08-15"))

    graph = by_id(validator.check_ccb_graph(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))
    variants = by_id(validator.check_ccb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))
    assert graph["2h.schedule.edge"].passed
    assert variants["2h.resgate_baixa_event"].passed


def test_ccb_blank_schedule_exclusion_is_oracle_active(spark):
    tables = ccb_tables(spark)
    tables["TCTPCRONOGRAMA_CCB"] = tables["TCTPCRONOGRAMA_CCB"].withColumn(
        "DATA_EXCLUSAO", validator.F.lit("")
    )

    profile = profiler.build_profile(
        tables, product="ccb", num_tipo_if=53, simplified=False
    )
    assert profile["shapes"][0]["counts"]["TCTPCRONOGRAMA_CCB"] == 1


def test_ccb_polymorphism_accepts_known_graph_and_rejects_wrong_subtype(spark):
    profile = validator.VALIDATION_PROFILES["ccb"]
    findings = by_id(validator.check_ccb_polymorphism(ccb_tables(spark), 5, profile))
    assert findings["2h.condition_polymorphism"].passed
    assert findings["2h.subtype_orphan"].passed

    tables = ccb_tables(spark)
    tables["JUROS_FIXO"] = tables["JUROS_FIXO"].limit(0)
    tables["SPREAD"] = spark.createDataFrame([(12,)], tables["SPREAD"].schema)
    finding = by_id(validator.check_ccb_polymorphism(tables, 5, profile))[
        "2h.condition_polymorphism"
    ]
    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_ccb_unknown_condition_type_is_advisory(spark):
    tables = ccb_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].withColumn(
        "COD_TIPO_CONDICAO_IF",
        validator.F.when(validator.F.col("NUM_CONDICAO_IF") == 11, "99").otherwise(
            validator.F.col("COD_TIPO_CONDICAO_IF")
        ),
    )
    findings = by_id(validator.check_ccb_polymorphism(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings["2h.unknown_condition_type"].severity == validator.SEV_WARN


def test_ccb_resgate_baixa_event_correlation_is_advisory(spark):
    tables = ccb_tables(spark)
    tables["TCTPIF_CCB"] = tables["TCTPIF_CCB"].withColumn(
        "IND_BAIXA_VENCIMENTO", validator.F.lit("S")
    )
    finding = by_id(validator.check_ccb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))["2h.resgate_baixa_event"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 1


@pytest.mark.parametrize("exercise", ["AMERICANA", None])
def test_ccb_resgate_maturity_and_exercise_drift_is_advisory(spark, exercise):
    tables = ccb_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].union(
        spark.createDataFrame([(13, 1, "20", None)], tables["CONDICAO_IF"].schema)
    )
    tables["RESGATE"] = spark.createDataFrame(
        [(13, "2034-08-17", exercise)], tables["RESGATE"].schema
    )
    tables["TCTPIF_CCB"] = tables["TCTPIF_CCB"].withColumn(
        "IND_BAIXA_VENCIMENTO", validator.F.lit("S")
    )
    tables["TCTPCRONOGRAMA_CCB"] = tables["TCTPCRONOGRAMA_CCB"].withColumn(
        "NUM_TIPO_EVENTO_LEGADO", validator.F.lit("85")
    ).withColumn("DATA_ORIGINAL_EVENTO", validator.F.lit("2034-08-17"))

    findings = by_id(validator.check_ccb_variant_rules(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))
    assert findings["2h.resgate_baixa_event"].passed
    assert findings["2h.resgate_details"].severity == validator.SEV_WARN


def test_ccb_registration_profile_is_opt_in_and_variant_drift_warns(spark):
    profile = validator.VALIDATION_PROFILES["ccb"]
    assert validator.check_ccb_registration_profile(ccb_tables(spark), 5, False, profile) == []

    tables = ccb_tables(spark)
    tables["CONDICAO_IF"] = tables["CONDICAO_IF"].where(
        validator.F.col("COD_TIPO_CONDICAO_IF") != "4"
    )
    findings = by_id(validator.check_ccb_registration_profile(tables, 5, True, profile))

    assert findings["8h.profile.variant_condition_mix"].severity == validator.SEV_WARN
    assert findings["8h.profile.core_counts"].passed


def test_ccb_registration_profile_checks_history_alteration_agenda_and_events(spark):
    profile = validator.VALIDATION_PROFILES["ccb"]
    tables = ccb_tables(spark)
    tables["HISTORICO_IF_TITULO"] = tables["HISTORICO_IF_TITULO"].limit(0)
    tables["ALTERACAO_IF"] = tables["ALTERACAO_IF"].withColumn(
        "COD_TIPO_ALTERACAO", validator.F.lit("X")
    )
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "IND_AGENDA_CONSTANTE", validator.F.lit("S")
    )
    tables["TCTPCRONOGRAMA_CCB"] = tables["TCTPCRONOGRAMA_CCB"].withColumn(
        "NUM_TIPO_EVENTO_LEGADO", validator.F.lit("83")
    )
    findings = by_id(validator.check_ccb_registration_profile(
        tables, 5, True, profile
    ))

    assert findings["8h.profile.core_counts"].severity == validator.SEV_WARN
    assert findings["8h.profile.variant_condition_mix"].severity == validator.SEV_WARN
    assert findings["8h.profile.variant_event_mix"].severity == validator.SEV_WARN


def test_ccb_unknown_payment_form_is_not_forced_into_observed_matrix(spark):
    tables = ccb_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "NUM_ID_FORMA_PAGAMENTO", validator.F.lit(999)
    )
    findings = by_id(validator.check_ccb_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings["8h.profile.variant_condition_mix"].passed
    assert findings["8h.profile.variant_event_mix"].passed
    assert findings["8h.profile.unknown_payment_form"].severity == validator.SEV_WARN


def test_ccb_schedule_profile_checks_parcels_values_and_original_date(spark):
    tables = ccb_tables(spark)
    schedule = tables["TCTPCRONOGRAMA_CCB"].withColumn(
        "COD_PARCELA", validator.F.lit("parcel.1")
    ).withColumn("VAL_EVENTO", validator.F.lit(-1.0)).withColumn(
        "DATA_ORIGINAL_EVENTO", validator.F.lit("2040-01-01")
    )
    tables["TCTPCRONOGRAMA_CCB"] = schedule.union(
        schedule.withColumn("NUM_EVENTO_CCB", validator.F.lit(62))
    )
    findings = by_id(validator.check_ccb_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings["8h.profile.schedule_dates"].severity == validator.SEV_WARN
    assert findings["8h.profile.schedule_parcels"].severity == validator.SEV_WARN
    assert findings["8h.profile.schedule_values"].severity == validator.SEV_WARN


def test_ccb_schedule_profile_reports_optional_column_coverage(spark):
    tables = ccb_tables(spark)
    tables["TCTPCRONOGRAMA_CCB"] = tables["TCTPCRONOGRAMA_CCB"].drop(
        "COD_PARCELA", "VAL_EVENTO"
    )
    findings = by_id(validator.check_ccb_registration_profile(
        tables, 5, True, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings["8h.profile.schedule_parcels"].severity == validator.SEV_WARN
    assert findings["8h.profile.schedule_values"].severity == validator.SEV_WARN


def test_ccb_dates_reject_malformed_and_reversed_values(spark):
    tables = ccb_tables(spark)
    tables["INSTRUMENTO_FINANCEIRO"] = tables["INSTRUMENTO_FINANCEIRO"].withColumn(
        "DAT_EMISSAO", validator.F.lit("not-a-date")
    )
    tables["CREDITO"] = tables["CREDITO"].withColumn(
        "DAT_INICIO_RENTABILIDADE", validator.F.lit("2036-01-01")
    )
    findings = by_id(validator.check_ccb_dates(
        tables, 5, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings["5h.date_parse"].severity == validator.SEV_ERROR
    assert findings["5h.date_order"].severity == validator.SEV_ERROR


@pytest.mark.parametrize(
    ("frame", "column", "value", "check_id"),
    [
        ("CCB_TIPO_IF", "COD_TIPO_IF", "CDB", "6h.lookup.tipo_if"),
        ("CCB_OBJECT_SERVICE", "IND_PLATAFORMA_BAIXA", "N", "6h.lookup.platform"),
        ("CCB_ROUTES", "NUM_ID_OBJETO_SERVICO", 44, "6h.lookup.registration_route"),
        ("CCB_ROUTES", "IND_DISPONIVEL_IDENTIFICACAO", "N",
         "6h.lookup.registration_route"),
    ],
)
def test_ccb_target_eligibility_rejects_wrong_type_platform_and_route(
    spark, frame, column, value, check_id
):
    frames = target_frames(spark)
    frames[frame] = frames[frame].withColumn(column, validator.F.lit(value))
    findings = by_id(validator.check_ccb_target_frames(
        ccb_tables(spark), frames, 5, validator.VALIDATION_PROFILES["ccb"]
    ))

    assert findings[check_id].severity == validator.SEV_ERROR


def test_ccb_target_check_ignores_historical_nonregistration_operation(spark):
    tables = ccb_tables(spark)
    historical = tables["OPERACAO"].withColumn("NUM_ID_OPERACAO", validator.F.lit(72)).withColumn(
        "NUM_ID_TIPO_OPER_OBJETO_SERV", validator.F.lit(999)
    )
    tables["OPERACAO"] = tables["OPERACAO"].union(historical)
    frames = target_frames(spark)
    frames["CCB_ROUTES"] = frames["CCB_ROUTES"].union(
        spark.createDataFrame([(999, 999, "599", "N")], frames["CCB_ROUTES"].schema)
    )

    finding = by_id(validator.check_ccb_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["ccb"]
    ))["6h.lookup.registration_route"]
    assert finding.passed


def test_ccb_target_loader_respects_skip_prefix(spark, monkeypatch):
    def fail_jdbc(*_args, **_kwargs):
        raise AssertionError("skipped CCB target check attempted JDBC")

    monkeypatch.setattr(validator, "_jdbc", fail_jdbc)
    frames, errors = validator.load_ccb_target_frames(
        spark, SimpleNamespace(schema="CETIP"), ccb_tables(spark),
        skip_prefixes=("6h.lookup",),
    )
    assert frames == {}
    assert errors == {}


def test_ccb_shape_profiler_and_validator_use_same_metrics(spark):
    profile = profiler.build_profile(
        ccb_tables(spark), product="ccb", num_tipo_if=53, simplified=False
    )

    assert profile["metrics"] == validator.CCB_SHAPE_METRICS
    assert [metric.name for metric in profiler.metrics_for_product("ccb")] == (
        validator.CCB_SHAPE_METRICS
    )
    assert profile["metrics_skipped"] == []
    counts = profile["shapes"][0]["counts"]
    assert counts["TCTPCRONOGRAMA_CCB_TIPO90"] == 1
    assert counts["CONDICAO_IF_TIPO4"] == 1
    assert counts["HISTORICO_IF_TITULO"] == 1


def test_ccb_shape_profile_rejects_cross_product_comparison(spark):
    ccb = profiler.build_profile(
        ccb_tables(spark), product="ccb", num_tipo_if=53, simplified=False
    )
    other = dict(ccb, product="cdb", num_tipo_if=49)

    with pytest.raises(ValueError, match="incompatible"):
        profiler.compare_profiles(ccb, other, "other")


def test_ccb_shape_dispatch_needs_baseline_but_no_cdb_ratio(spark):
    findings = by_id(validator.check_shapes(
        spark, ccb_tables(spark), None, 5, 1.0, 0.15, 5.0,
        validator.VALIDATION_PROFILES["ccb"],
    ))

    assert findings["7.baseline"].severity == validator.SEV_WARN
    assert "7c.op_ratio" not in findings
