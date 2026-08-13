import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-dicre-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def dicre_tables(spark):
    return {
        "LOTE": spark.createDataFrame(
            [(100, "LCA00010CAP", "10", "2", "96", "S", None)],
            "NUM_ID_LOTE long, NOME_LOTE string, NUM_CONTA_PARTICIPANTE string, "
            "NUM_ID_TIPO_LOTE string, NUM_TIPO_IF string, IND_REVOLVENCIA string, "
            "DAT_EXCLUSAO string",
        ),
        "CREDITO_DC": spark.createDataFrame(
            [(
                1000, "credit.0", 100, None, 53, 7, 4199, "2026-07-17", "IPOC-1",
                6099, None, None, 6119, 1.0, 1.0, "2026-07-17", "2026-07-17",
                "2030-04-19", "PJ", 46, 16, 2, 268, 6, "N", "N", "N", 26,
            )],
            "NUM_ID_CREDITO_DC long, COD_CREDITO_DC string, NUM_ID_LOTE long, "
            "DAT_EXCLUSAO string, NUM_TIPO_IF long, NUM_CONTA_CUSTODIANTE long, "
            "NUM_ID_BASE_CREDITO long, DAT_INCLUSAO string, COD_IPOC string, "
            "NUM_ID_QUALIF_FINALIDADE long, NUM_ID_QUALIF_JUROS_A_CADA long, "
            "NUM_ID_QUALIF_AMORT_A_CADA long, NUM_ID_QUALIF_GARANTIA_ESPEC long, "
            "VAL_PU double, VAL_PU_EMISSAO double, DAT_VAL_PU string, "
            "DAT_CONTRATACAO string, DAT_VENCIMENTO string, COD_TIPO_PESSOA string, "
            "NUM_ID_MODALIDADE_CREDITO long, NUM_ID_TIPO_GARANTIA long, "
            "NUM_ID_INDEXADOR_CREDITO long, NUM_ID_FORMA_PAGAMENTO long, "
            "NUM_ID_TIPO_AMORTIZACAO long, IND_INADIMPLENTE string, "
            "IND_MULTIPLO_IPOC string, IND_BAIXA_AUTOMATICA_VENC string, NUM_ID_UF long",
        ),
        "HISTORICO_CREDITO_DC": spark.createDataFrame(
            [(5000, "credit.0", 100, 1, 1.0, "2026-07-17", "N", "2026-07-17")],
            "NUM_ID_HISTORICO_CREDITO_DC long, COD_CREDITO_DC string, NUM_ID_LOTE long, "
            "NUM_ID_TIPO_ACAO_HIST_CREDITO long, VAL_PU double, DAT_VAL_PU string, "
            "IND_INADIMPLENTE string, DAT_IND_INADIMPLENTE string",
        ),
        "TCTPCHAV_IROP_ATIV": spark.createDataFrame(
            [(700, 53)], "NUM_CHAV_IROP long, NUM_TIPO_IF long"
        ),
        "TCTPDET_CHAV_IROP_CCB": spark.createDataFrame(
            [(700, "IPOC-1")], "NUM_CHAV_IROP long, COD_IPOC string"
        ),
        "TCTPDET_CHAV_IROP_CMER": spark.createDataFrame(
            [], "NUM_CHAV_IROP long, COD_COTR string"
        ),
        "TCTPIROP_ATIV": spark.createDataFrame(
            [(800, 1000, 700)],
            "NUM_IROP_ATIV long, NUM_IDT_CRE_DC long, NUM_CHAV_IROP long",
        ),
        "TCTPSOLI_IROP_ATIV": spark.createDataFrame(
            [(900, 800)], "NUM_SOLI_IROP_ATIV long, NUM_IROP_ATIV long"
        ),
    }


def target_frames(spark, target_ipocs=()):
    return {
        "DICRE_ACCOUNTS": spark.createDataFrame(
            [
                (10, 1, "L", 1, "BANK"),
                (7, 2, "R", 2, "BANK"),
            ],
            "NUM_CONTA_PARTICIPANTE long, NUM_ID_SITUACAO_CONTA long, "
            "COD_TIPO_ACESSO string, NUM_ID_AREA_ATUACAO long, NOM_SIMPLIFICADO string",
        ),
        "DICRE_BASES": spark.createDataFrame(
            [(10, 4199)], "NUM_CONTA_PARTICIPANTE long, NUM_ID_BASE_CREDITO long"
        ),
        "DICRE_IF_COMPATIBILITY": spark.createDataFrame(
            [(53, 96, 2)],
            "NUM_TIPO_IF long, NUM_TIPO_IF_GARANTIDO long, NUM_ID_TIPO_LOTE long",
        ),
        "DICRE_QUALIFICATIONS": spark.createDataFrame(
            [(6099, 20), (6119, 23)],
            "NUM_ID_QUALIFICACAO long, NUM_ID_QUALIFICACAO_SUBGRUPO long",
        ),
        "TCTPFEATURE_TOGGLE": spark.createDataFrame(
            [(validator.DICRE_IPOC_TOGGLE, "S", "2026-01-01", "2026-12-31")],
            "COD_FTRE_TOG string, IND_FTRE_HAB string, DATA_INIC_VIG_FTRE string, "
            "DATA_FIM_VIG_FTRE string",
        ),
        "CREDITO_DC_TARGET": spark.createDataFrame(
            [(value, None) for value in target_ipocs],
            "COD_IPOC string, DAT_EXCLUSAO string",
        ),
    }


def graph_findings(tables):
    return by_id(validator.check_dicre_graph(
        tables, 5, validator.VALIDATION_PROFILES["dicre"]
    ))


def irop_findings(tables):
    return by_id(validator.check_dicre_irop_graph(
        tables, 5, validator.VALIDATION_PROFILES["dicre"]
    ))


def lookup_findings(tables, frames):
    return by_id(validator.check_dicre_target_frames(
        tables, frames, 5, validator.VALIDATION_PROFILES["dicre"]
    ))


def test_dicre_profile_is_isolated_non_if_pipeline():
    profile = validator.VALIDATION_PROFILES["dicre"]

    assert profile.pipeline == "dicre"
    assert validator.DICRE_IPOC_TOGGLE == "VALIDADOR_UNICIDADE_IPOC_LCA"
    assert profile.unsupported_required() == ()
    assert validator.check_dicre_graph({}, 5, validator.VALIDATION_PROFILES["cdb"]) == []
    assert validator.check_credito_scr_graph({}, 5, profile) == []


def test_dicre_accepts_complete_graph_and_all_root_rows(spark):
    tables = dicre_tables(spark)
    tables["CREDITO_DC"] = tables["CREDITO_DC"].withColumn(
        "DAT_EXCLUSAO", validator.F.lit("2026-08-01")
    )

    identity = validator.check_dicre_identity(
        tables, validator.VALIDATION_PROFILES["dicre"], 5
    )[0]

    assert identity.count == 1
    assert all(finding.passed for finding in graph_findings(tables).values())
    assert all(finding.passed for finding in irop_findings(tables).values())


def test_dicre_lot_key_is_exact_trimmed_and_includes_num_tipo_if(spark):
    tables = dicre_tables(spark)
    duplicate = tables["LOTE"].withColumn("NUM_ID_LOTE", validator.F.lit(101))
    tables["LOTE"] = tables["LOTE"].union(duplicate)

    finding = graph_findings(tables)["2f.active_lot_natural_key"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1

    tables["LOTE"] = tables["LOTE"].withColumn(
        "NUM_TIPO_IF",
        validator.F.when(validator.F.col("NUM_ID_LOTE") == 101, "96.0").otherwise("96"),
    )
    assert graph_findings(tables)["2f.active_lot_natural_key"].passed


def test_dicre_credit_code_preserves_case_and_dot_zero(spark):
    tables = dicre_tables(spark)
    first = tables["CREDITO_DC"]
    second = first.withColumn("NUM_ID_CREDITO_DC", validator.F.lit(1001)).withColumn(
        "COD_CREDITO_DC", validator.F.lit("credit")
    )
    third = first.withColumn("NUM_ID_CREDITO_DC", validator.F.lit(1002)).withColumn(
        "COD_CREDITO_DC", validator.F.lit("CREDIT.0")
    )
    tables["CREDITO_DC"] = first.union(second).union(third)

    assert graph_findings(tables)["2f.credit_code_unique"].passed


def test_dicre_history_uses_action_one_and_code_plus_canonical_lot(spark):
    tables = dicre_tables(spark)
    other_action = tables["HISTORICO_CREDITO_DC"].withColumn(
        "NUM_ID_HISTORICO_CREDITO_DC", validator.F.lit(5001)
    ).withColumn("NUM_ID_TIPO_ACAO_HIST_CREDITO", validator.F.lit(2))
    tables["HISTORICO_CREDITO_DC"] = tables["HISTORICO_CREDITO_DC"].union(other_action)

    assert graph_findings(tables)["2f.inclusion_history"].passed

    duplicate = tables["HISTORICO_CREDITO_DC"].where(
        validator.F.col("NUM_ID_TIPO_ACAO_HIST_CREDITO") == 1
    ).withColumn("NUM_ID_HISTORICO_CREDITO_DC", validator.F.lit(5002))
    tables["HISTORICO_CREDITO_DC"] = tables["HISTORICO_CREDITO_DC"].union(duplicate)
    assert graph_findings(tables)["2f.inclusion_history"].count == 1

    tables = dicre_tables(spark)
    tables["HISTORICO_CREDITO_DC"] = tables["HISTORICO_CREDITO_DC"].withColumn(
        "COD_CREDITO_DC", validator.F.lit("CREDIT.0")
    )
    findings = graph_findings(tables)
    assert findings["2f.inclusion_history"].count == 1
    assert findings["2f.inclusion_history_orphan"].count == 1


@pytest.mark.parametrize(
    ("table", "column", "check_id"),
    [
        ("TCTPIROP_ATIV", "NUM_IDT_CRE_DC", "2f.irop.credit_edge"),
        ("TCTPIROP_ATIV", "NUM_CHAV_IROP", "2f.irop.key_edge"),
        ("TCTPDET_CHAV_IROP_CCB", "NUM_CHAV_IROP", "2f.irop.ccb_key_edge"),
        ("TCTPSOLI_IROP_ATIV", "NUM_IROP_ATIV", "2f.irop.request_edge"),
    ],
)
def test_dicre_rejects_each_present_irop_orphan(spark, table, column, check_id):
    tables = dicre_tables(spark)
    tables[table] = tables[table].withColumn(column, validator.F.lit(999))

    assert irop_findings(tables)[check_id].severity == validator.SEV_ERROR


def test_dicre_rejects_cmer_orphan_and_duplicate_business_edge(spark):
    tables = dicre_tables(spark)
    tables["TCTPDET_CHAV_IROP_CMER"] = spark.createDataFrame(
        [(999, "contract")], tables["TCTPDET_CHAV_IROP_CMER"].schema
    )
    assert irop_findings(tables)["2f.irop.cmer_key_edge"].count == 1

    tables = dicre_tables(spark)
    tables["TCTPIROP_ATIV"] = tables["TCTPIROP_ATIV"].union(
        tables["TCTPIROP_ATIV"].withColumn("NUM_IROP_ATIV", validator.F.lit(801))
    )
    assert irop_findings(tables)["2f.irop.credit_key_unique"].count == 1


def test_dicre_accepts_hard_target_eligibility(spark):
    findings = lookup_findings(dicre_tables(spark), target_frames(spark))

    assert all(finding.passed for finding in findings.values())


@pytest.mark.parametrize(
    ("frame", "replacement", "check_id"),
    [
        (
            "DICRE_ACCOUNTS",
            [(10, 3, "R", 2, "OTHER"), (7, 2, "R", 2, "BANK")],
            "6f.lookup.accounts",
        ),
        ("DICRE_BASES", [(10, 999)], "6f.lookup.base"),
        ("DICRE_IF_COMPATIBILITY", [(139, 96, 2)], "6f.lookup.if_compatibility"),
        ("DICRE_QUALIFICATIONS", [(999, 20), (6119, 23)], "6f.lookup.qualifications"),
    ],
)
def test_dicre_rejects_hard_target_ineligibility(
    spark, frame, replacement, check_id
):
    frames = target_frames(spark)
    frames[frame] = spark.createDataFrame(replacement, frames[frame].schema)

    finding = lookup_findings(dicre_tables(spark), frames)[check_id]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_dicre_disabled_ipoc_toggle_has_no_target_dependency(spark):
    frames = target_frames(spark)
    frames["TCTPFEATURE_TOGGLE"] = spark.createDataFrame(
        [(validator.DICRE_IPOC_TOGGLE, "S", "2025-01-01", "2025-12-31")],
        frames["TCTPFEATURE_TOGGLE"].schema,
    )
    del frames["CREDITO_DC_TARGET"]

    finding = lookup_findings(dicre_tables(spark), frames)["6f.lookup.ipoc_unique"]

    assert finding.passed
    assert finding.severity == validator.SEV_INFO


def test_dicre_detects_synthetic_and_active_target_ipoc_collisions(spark):
    tables = dicre_tables(spark)
    second = tables["CREDITO_DC"].withColumn(
        "NUM_ID_CREDITO_DC", validator.F.lit(1001)
    ).withColumn("COD_CREDITO_DC", validator.F.lit("credit-2"))
    tables["CREDITO_DC"] = tables["CREDITO_DC"].union(second)

    synthetic = lookup_findings(tables, target_frames(spark))["6f.lookup.ipoc_unique"]
    target = lookup_findings(
        dicre_tables(spark), target_frames(spark, ("IPOC-1",))
    )["6f.lookup.ipoc_unique"]

    assert synthetic.count == 2
    assert target.count == 1


def test_dicre_enabled_ipoc_collides_with_synthetic_outside_toggle_period(spark):
    tables = dicre_tables(spark)
    outside_period = tables["CREDITO_DC"].withColumn(
        "NUM_ID_CREDITO_DC", validator.F.lit(1001)
    ).withColumn("COD_CREDITO_DC", validator.F.lit("credit-2")).withColumn(
        "DAT_INCLUSAO", validator.F.lit("2025-07-17")
    )
    tables["CREDITO_DC"] = tables["CREDITO_DC"].union(outside_period)

    finding = lookup_findings(tables, target_frames(spark))["6f.lookup.ipoc_unique"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_dicre_registration_observations_are_opt_in_warnings(spark):
    tables = dicre_tables(spark)
    profile = validator.VALIDATION_PROFILES["dicre"]

    assert validator.check_dicre_registration_profile(tables, 5, False, profile) == []
    tables["CREDITO_DC"] = tables["CREDITO_DC"].withColumn(
        "VAL_PU", validator.F.lit(2.0)
    )
    findings = by_id(validator.check_dicre_registration_profile(
        tables, 5, True, profile
    ))

    assert findings["8f.profile.root_constants"].severity == validator.SEV_WARN
    assert findings["8f.profile.history_copied_values"].severity == validator.SEV_WARN
    assert findings["8f.profile.financial_dates"].severity == validator.SEV_WARN


def test_dicre_metadata_requires_all_eight_tables_and_primary_keys():
    profile = validator.VALIDATION_PROFILES["dicre"]
    tables = set(validator.DICRE_GRAPH_TABLES)
    pks = {table: ["ID"] for table in tables}
    pks.pop(validator.TCTPDET_CHAV_IROP_CMER_TABLE)

    finding = validator.check_dicre_metadata(
        validator.Metadata(tables, pks, {}, {}, {}), False, profile
    )[0]
    no_oracle = validator.check_dicre_metadata(
        validator.Metadata(set(), {}, {}, {}, {}), True, profile
    )[0]

    assert finding.severity == validator.SEV_ERROR
    assert validator.TCTPDET_CHAV_IROP_CMER_TABLE in finding.message
    assert no_oracle.passed


def test_dicre_report_displays_credito_dc_root(capsys):
    validator.emit_report(
        None, [], None, "error", validator.VALIDATION_PROFILES["dicre"],
        "/input", [], ["test partial"],
    )

    assert "product=dicre (root=CREDITO_DC)" in capsys.readouterr().out


def test_dicre_rejects_shape_baseline_before_spark_setup(monkeypatch):
    monkeypatch.setattr(
        validator,
        "parse_args",
        lambda: SimpleNamespace(
            product="dicre", skip_check=[], shape_baseline="baseline.json"
        ),
    )

    with pytest.raises(SystemExit, match="non-IF dicre pipeline"):
        validator.main()
