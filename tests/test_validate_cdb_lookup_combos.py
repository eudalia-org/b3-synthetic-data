import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-cdb-lookup-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def frames(spark, operations, tos, sic, tipos):
    op_df = spark.createDataFrame(
        operations,
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
        "NUM_ID_MODALIDADE_LIQUIDACAO long",
    )
    tos_df = spark.createDataFrame(
        tos,
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_ID_TIPO_OPERACAO long, "
        "NUM_ID_OBJETO_SERVICO long, IND_DISPONIVEL_IDENTIFICACAO string",
    )
    sic_df = spark.createDataFrame(
        sic,
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )
    tipo_df = spark.createDataFrame(
        tipos,
        "NUM_ID_TIPO_OPERACAO long, IND_SEM_MODALIDADE_INFOHUB string",
    )
    return op_df, tos_df, sic_df, tipo_df


def by_id(findings):
    return {finding.check_id: finding for finding in findings}


def assert_failed_have_hints(findings):
    assert all(finding.hint.strip() for finding in findings if not finding.passed)


def required_frames(
    spark,
    *,
    title_account=11,
    deposit_account=12,
    p1_account=13,
    p2_account=14,
    operation_tos=100,
    accounts=None,
    tos=None,
    tipos=None,
    cdb_objects=None,
):
    tables = {
        "TITULO": spark.createDataFrame(
            [(title_account,)], "NUM_CONTA_PARTICIPANTE string"
        ),
        "DEPOSITO_AUTOMATICO_IF": spark.createDataFrame(
            [(deposit_account,)], "NUM_CONTA_PARTICIPANTE string"
        ),
        "OPERACAO": spark.createDataFrame(
            [(1, operation_tos, 6, p1_account, p2_account)],
            "NUM_ID_OPERACAO long, NUM_ID_TIPO_OPER_OBJETO_SERV string, "
            "NUM_ID_MODALIDADE_LIQUIDACAO long, NUM_CONTA_PARTICIPANTE_P1 string, "
            "NUM_CONTA_PARTICIPANTE_P2 string",
        ),
    }
    account_df = spark.createDataFrame(
        accounts
        if accounts is not None
        else [
            ("11", "1", "00011.40-1", "1", "L"),
            ("12", "1", "00012.10-2", "1", "L"),
            ("13", "1", "00013.40-3", "1", "L"),
            ("14", "1", "00014.10-4", "1", "L"),
        ],
        "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
        "COD_CONTA_PARTICIPANTE string, NUM_ID_AREA_ATUACAO string, "
        "COD_TIPO_ACESSO string",
    )
    tos_df = spark.createDataFrame(
        tos if tos is not None else [("100", "10", "44", "S")],
        "NUM_ID_TIPO_OPER_OBJETO_SERV string, NUM_ID_TIPO_OPERACAO string, "
        "NUM_ID_OBJETO_SERVICO string, IND_DISPONIVEL_IDENTIFICACAO string",
    )
    tipo_df = spark.createDataFrame(
        tipos if tipos is not None else [("10", "S", "1")],
        "NUM_ID_TIPO_OPERACAO string, IND_SEM_MODALIDADE_INFOHUB string, "
        "COD_TIPO_OPERACAO string",
    )
    cdb_object_df = spark.createDataFrame(
        cdb_objects if cdb_objects is not None else [("CDB", "S")],
        "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string",
    )
    return tables, account_df, tos_df, tipo_df, cdb_object_df


def required_findings(spark, **kwargs):
    profile = kwargs.pop("profile", None)
    tables, account_df, tos_df, tipo_df, cdb_object_df = required_frames(spark, **kwargs)
    return validator.check_required_lookup_frames(
        tables, account_df, tos_df, tipo_df, cdb_object_df, sample=5, profile=profile
    )


def test_all_valid_operation(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6)],
        tos=[(100, 10, 44, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )

    findings = validator.check_lookup_combo_frames(*inputs, sample=5)

    assert all(finding.passed for finding in findings)
    assert all(finding.severity == validator.SEV_INFO for finding in findings)


def test_missing_target_tos_is_structural_error_without_cascades(spark):
    inputs = frames(
        spark,
        operations=[(1, 999, 6)],
        tos=[],
        sic=[(100, 49, 44)],
        tipos=[(10, "N")],
    )

    findings = by_id(validator.check_lookup_combo_frames(*inputs, sample=5))

    assert findings["6.combo.tos_fk"].severity == validator.SEV_ERROR
    assert findings["6.combo.tos_fk"].count == 1
    assert_failed_have_hints(findings.values())
    for check_id in (
        "6.combo.cdb_compatibility",
        "6.combo.sem_modalidade",
        "6.combo.identification_availability",
    ):
        assert findings[check_id].passed
        assert findings[check_id].count == 0


def test_exact_mapping_is_required_even_when_operation_type_is_valid(spark):
    inputs = frames(
        spark,
        operations=[(1, 101, 1)],
        tos=[(100, 10, 44, "S"), (101, 10, 45, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )

    findings = by_id(validator.check_lookup_combo_frames(*inputs, sample=5))

    assert findings["6.combo.tos_fk"].passed
    assert findings["6.combo.cdb_compatibility"].severity == validator.SEV_WARN
    assert findings["6.combo.cdb_compatibility"].count == 1
    assert_failed_have_hints(findings.values())


def test_sem_modalidade_n_and_null_flags_warn(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6), (2, 101, 16)],
        tos=[(100, 10, 44, "S"), (101, 11, 44, "S")],
        sic=[(100, 49, 44), (101, 49, 44)],
        tipos=[(10, "N"), (11, None)],
    )

    finding = by_id(
        validator.check_lookup_combo_frames(*inputs, sample=5)
    )["6.combo.sem_modalidade"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 2
    assert_failed_have_hints([finding])


def test_unavailable_and_null_identification_flags_warn(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 1), (2, 101, 1)],
        tos=[(100, 10, 44, "N"), (101, 10, 44, None)],
        sic=[(100, 49, 44), (101, 49, 44)],
        tipos=[(10, "S")],
    )

    finding = by_id(
        validator.check_lookup_combo_frames(*inputs, sample=5)
    )["6.combo.identification_availability"]

    assert finding.severity == validator.SEV_WARN
    assert finding.count == 2
    assert_failed_have_hints([finding])


def test_missing_lookup_frames_degrade_with_hints(spark):
    inputs = frames(
        spark,
        operations=[(1, 100, 6)],
        tos=[(100, 10, 44, "S")],
        sic=[(100, 49, 44)],
        tipos=[(10, "S")],
    )
    op_df, tos_df, _, _ = inputs

    missing_tos = validator.check_lookup_combo_frames(
        op_df, None, None, None, sample=5
    )
    missing_sic_and_tipo = validator.check_lookup_combo_frames(
        op_df, tos_df, None, None, sample=5
    )

    assert_failed_have_hints(missing_tos)
    assert_failed_have_hints(missing_sic_and_tipo)
    assert all(finding.check_id.startswith("6.combo") for finding in missing_tos)
    assert all(
        finding.check_id.startswith("6.combo")
        for finding in missing_sic_and_tipo
    )


def test_no_jdbc_warns(spark):
    op_df = spark.createDataFrame([(1, 100, 6)], "op long, tos long, modalidade long")
    cfg = validator.Config("unused", None, None, "", "CETIP")
    meta = validator.Metadata(set(), {}, {}, {}, {})

    findings = validator.check_lookup_combos(
        spark, cfg, {validator.OPERACAO_TABLE: op_df}, meta=meta, sample=5
    )
    finding = findings[0]

    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert finding.check_id == "6.combo.no_jdbc"
    assert "No Oracle connection" in finding.message
    required = [item for item in findings if item.check_id.startswith("6.required.")]
    assert len(required) == 3
    assert all(item.severity == validator.SEV_ERROR for item in required)
    assert_failed_have_hints(findings)


def test_lazy_jdbc_failure_is_caught_and_required_check_errors(spark, monkeypatch):
    class FailsOnMaterialization:
        def collect(self):
            raise RuntimeError("Oracle read failed during action")

    sic_df = spark.createDataFrame(
        [(100, 49, 44)],
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )
    tipo_df = spark.createDataFrame(
        [(10, "S", "1")],
        "NUM_ID_TIPO_OPERACAO long, IND_SEM_MODALIDADE_INFOHUB string, "
        "COD_TIPO_OPERACAO string",
    )
    cdb_df = spark.createDataFrame(
        [("CDB", "S")], "COD_OBJETO_SERVICO string, IND_PLATAFORMA_BAIXA string"
    )

    def fake_jdbc(_spark, _cfg, query):
        if f"FROM {_cfg.schema}.{validator.TIPO_OPER_OBJETO_SERV_TABLE}" in query:
            return FailsOnMaterialization()
        if f"FROM {_cfg.schema}.{validator.V_PARAMETRO_SIC_TABLE}" in query:
            return sic_df
        if f"FROM {_cfg.schema}.{validator.V_OBJETOS_SERVICO_TABLE}" in query:
            return cdb_df
        return tipo_df

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    op_df = spark.createDataFrame(
        [(1, 100, 6)],
        "NUM_ID_OPERACAO long, NUM_ID_TIPO_OPER_OBJETO_SERV long, "
        "NUM_ID_MODALIDADE_LIQUIDACAO long",
    )
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")
    meta = validator.Metadata(set(), {}, {}, {}, {})

    findings = validator.check_lookup_combos(
        spark, cfg, {validator.OPERACAO_TABLE: op_df}, meta=meta, sample=5
    )

    combo = [finding for finding in findings if finding.check_id.startswith("6.combo.")]
    required = by_id(findings)
    assert all(not finding.passed for finding in combo)
    assert all(finding.severity == validator.SEV_WARN for finding in combo)
    assert required["6.required.operation_tos"].severity == validator.SEV_ERROR
    assert_failed_have_hints(findings)


def test_missing_required_operation_columns_warn(spark):
    op_df = spark.createDataFrame([(1, 100)], "NUM_ID_OPERACAO long, OTHER_ID long")

    finding = validator.check_lookup_combo_frames(
        op_df, None, None, None, sample=5
    )[0]

    assert finding.check_id == "6.combo.required_columns"
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert "NUM_ID_TIPO_OPER_OBJETO_SERV" in finding.message
    assert "NUM_ID_MODALIDADE_LIQUIDACAO" in finding.message
    assert_failed_have_hints([finding])


def test_all_three_required_lookups_pass(spark):
    findings = required_findings(spark)

    assert [finding.check_id for finding in findings] == [
        "6.required.active_account",
        "6.required.operation_tos",
        "6.required.cdb_platform",
    ]
    assert all(finding.passed for finding in findings)
    assert all(finding.severity == validator.SEV_INFO for finding in findings)


def test_operation_type_code_two_fails_required_tos(spark):
    finding = by_id(required_findings(spark, tipos=[("10", "S", "2")]))[
        "6.required.operation_tos"
    ]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1
    assert_failed_have_hints([finding])


@pytest.mark.parametrize(
    "accounts",
    [
        [("11", "2", "00011.40-1", "1", "L")],
        [("11", "1", "00011.40-1", "2", "L")],
        [("11", "1", "00011.40-1", "1", "X")],
        [("11", "1", "00011.40-1", "1", "L ")],
        [],
    ],
    ids=["status-2", "wrong-area", "wrong-access", "nonexact-access", "missing-target"],
)
def test_ineligible_or_missing_account_fails(spark, accounts):
    accounts += [
        ("12", "1", "00012.10-2", "1", "L"),
        ("13", "1", "00013.40-3", "1", "L"),
        ("14", "1", "00014.10-4", "1", "L"),
    ]

    finding = by_id(required_findings(spark, accounts=accounts))[
        "6.required.active_account"
    ]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1
    assert_failed_have_hints([finding])


@pytest.mark.parametrize("missing_table", ["TITULO", "DEPOSITO_AUTOMATICO_IF", "OPERACAO"])
def test_each_required_account_source_table_missing_errors(spark, missing_table):
    inputs = required_frames(spark)
    tables = inputs[0]
    del tables[missing_table]

    findings = by_id(
        validator.check_required_lookup_frames(tables, *inputs[1:], sample=5)
    )

    assert findings["6.required.active_account"].severity == validator.SEV_ERROR
    assert_failed_have_hints(findings.values())


@pytest.mark.parametrize("table,column", validator.ACCOUNT_REFERENCES)
def test_each_required_account_source_column_missing_errors(spark, table, column):
    inputs = required_frames(spark)
    tables = inputs[0]
    tables[table] = tables[table].drop(column)

    finding = by_id(
        validator.check_required_lookup_frames(tables, *inputs[1:], sample=5)
    )["6.required.active_account"]

    assert finding.severity == validator.SEV_ERROR
    assert f"{table}.{column}" in finding.message
    assert_failed_have_hints([finding])


def test_unrelated_account_columns_are_ignored(spark):
    inputs = required_frames(spark)
    tables = inputs[0]
    tables["CREDITO"] = spark.createDataFrame(
        [(999,)], "NUM_CONTA_PARTICIPANTE long"
    )
    tables["TITULO"] = tables["TITULO"].withColumn("OTHER_ACCOUNT", pyspark.sql.functions.lit(999))

    finding = by_id(
        validator.check_required_lookup_frames(tables, *inputs[1:], sample=5)
    )["6.required.active_account"]

    assert finding.passed


def test_null_account_is_ignored_but_blank_account_fails(spark):
    null_finding = by_id(required_findings(spark, title_account=None))[
        "6.required.active_account"
    ]
    blank_finding = by_id(required_findings(spark, title_account="  "))[
        "6.required.active_account"
    ]

    assert null_finding.passed
    assert blank_finding.severity == validator.SEV_ERROR
    assert blank_finding.count == 1
    assert_failed_have_hints([blank_finding])


def test_required_lookup_ids_use_canonical_numeric_keys(spark):
    finding = by_id(required_findings(spark, title_account="11.000"))[
        "6.required.active_account"
    ]

    assert finding.passed


@pytest.mark.parametrize("account_id", ["11", "12", "13", "14"])
@pytest.mark.parametrize("group", ["40", "10"])
def test_account_code_groups_pass_for_every_required_reference(spark, account_id, group):
    codes = {
        "11": "00011.40-1",
        "12": "00012.10-2",
        "13": "00013.40-3",
        "14": "00014.10-4",
    }
    codes[account_id] = f"54321.{group}-7"
    accounts = [
        (value, "1", codes[value], "1", "L")
        for value in ("11", "12", "13", "14")
    ]

    finding = by_id(required_findings(spark, accounts=accounts))[
        "6.required.active_account"
    ]

    assert finding.passed


@pytest.mark.parametrize(
    "code",
    ["1234.40-1", "12345.20-1", "12345.40-12"],
    ids=["prefix", "group", "check-digit-shape"],
)
def test_malformed_account_code_shape_fails(spark, code):
    accounts = [
        ("11", "1", code, "1", "L"),
        ("12", "1", "00012.10-2", "1", "L"),
        ("13", "1", "00013.40-3", "1", "L"),
        ("14", "1", "00014.10-4", "1", "L"),
    ]

    finding = by_id(required_findings(spark, accounts=accounts))[
        "6.required.active_account"
    ]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1
    assert_failed_have_hints([finding])


def test_account_code_trailing_oracle_padding_is_trimmed(spark):
    accounts = [
        ("11", "1", "00011.40-1   ", "1", "L"),
        ("12", "1", "00012.10-2", "1", "L"),
        ("13", "1", "00013.40-3", "1", "L"),
        ("14", "1", "00014.10-4", "1", "L"),
    ]

    finding = by_id(required_findings(spark, accounts=accounts))[
        "6.required.active_account"
    ]

    assert finding.passed


@pytest.mark.parametrize(
    "kwargs",
    [
        {"operation_tos": None},
        {"operation_tos": "  "},
        {"operation_tos": "999"},
        {"tos": [("100", "10", "45", "S")]},
        {"tos": [("100", "10", "44", "N")]},
        {"tipos": [("10", "S", "2")]},
    ],
    ids=["null", "blank", "missing", "wrong-object", "identification", "type"],
)
def test_invalid_required_operation_tos_fails(spark, kwargs):
    finding = by_id(required_findings(spark, **kwargs))["6.required.operation_tos"]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1
    assert_failed_have_hints([finding])


@pytest.mark.parametrize("objects", [[("CDB", "N")], []], ids=["disabled", "absent"])
def test_cdb_platform_must_be_enabled(spark, objects):
    finding = by_id(required_findings(spark, cdb_objects=objects))[
        "6.required.cdb_platform"
    ]

    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1
    assert_failed_have_hints([finding])


def test_required_lookup_unavailability_errors_with_hints(spark):
    tables = required_frames(spark)[0]
    findings = validator.check_required_lookup_frames(
        tables,
        None,
        None,
        None,
        None,
        sample=5,
        lookup_errors={
            validator.CONTA_PARTICIPANTE_TABLE: "account unavailable",
            validator.TIPO_OPER_OBJETO_SERV_TABLE: "tos unavailable",
            validator.TIPO_OPERACAO_TABLE: "type unavailable",
            validator.V_OBJETOS_SERVICO_TABLE: "object unavailable",
        },
    )

    assert len(findings) == 3
    assert all(finding.severity == validator.SEV_ERROR for finding in findings)
    assert_failed_have_hints(findings)


def test_required_target_columns_missing_error(spark):
    inputs = required_frames(spark)
    _, account_df, tos_df, tipo_df, cdb_df = inputs

    findings = validator.check_required_lookup_frames(
        inputs[0],
        account_df.drop("NUM_ID_SITUACAO_CONTA"),
        tos_df.drop("NUM_ID_OBJETO_SERVICO"),
        tipo_df.drop("COD_TIPO_OPERACAO"),
        cdb_df.drop("IND_PLATAFORMA_BAIXA"),
        sample=5,
    )

    assert all(finding.severity == validator.SEV_ERROR for finding in findings)
    assert_failed_have_hints(findings)


@pytest.mark.parametrize("missing", ["table", "column"])
def test_required_operation_source_missing_errors(spark, missing):
    inputs = required_frames(spark)
    tables = inputs[0]
    if missing == "table":
        del tables["OPERACAO"]
    else:
        tables["OPERACAO"] = tables["OPERACAO"].drop(
            "NUM_ID_TIPO_OPER_OBJETO_SERV"
        )

    finding = by_id(
        validator.check_required_lookup_frames(tables, *inputs[1:], sample=5)
    )["6.required.operation_tos"]

    assert finding.severity == validator.SEV_ERROR
    assert_failed_have_hints([finding])


def test_account_lookup_sql_is_bounded_batched_and_strictly_checked(spark, monkeypatch):
    account_values = [str(value) for value in range(1, 1002)]
    inputs = required_frames(
        spark,
        title_account="1",
        deposit_account="2",
        p1_account="3",
        p2_account="4",
        accounts=[
            (
                value,
                "2" if value == "1" else "1",
                f"{int(value):05d}.40-{int(value) % 10}",
                "1",
                "L",
            )
            for value in account_values
        ],
    )
    tables = inputs[0]
    tables["TITULO"] = spark.createDataFrame(
        [(value,) for value in account_values], "NUM_CONTA_PARTICIPANTE string"
    )
    _, account_df, tos_df, tipo_df, cdb_df = inputs
    sic_df = spark.createDataFrame(
        [(100, 49, 44)],
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )
    account_queries = []

    def fake_jdbc(_spark, _cfg, query):
        if f"FROM {_cfg.schema}.{validator.CONTA_PARTICIPANTE_TABLE} cp" in query:
            account_queries.append(query)
            return account_df
        if f"FROM {_cfg.schema}.{validator.TIPO_OPER_OBJETO_SERV_TABLE}" in query:
            return tos_df
        if f"FROM {_cfg.schema}.{validator.V_PARAMETRO_SIC_TABLE}" in query:
            return sic_df
        if f"FROM {_cfg.schema}.{validator.V_OBJETOS_SERVICO_TABLE}" in query:
            return cdb_df
        return tipo_df

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")

    findings = by_id(
        validator.check_lookup_combos(
            spark, cfg, tables, validator.Metadata(set(), {}, {}, {}, {}), 5, 2_000
        )
    )

    assert len(account_queries) == 2
    assert all("LEFT JOIN CETIP.V_FAMILIA_CONTAS" in query for query in account_queries)
    assert all("cp.NUM_CONTA_PARTICIPANTE IN (" in query for query in account_queries)
    assert all(query.count(",") < 1010 for query in account_queries)
    assert findings["6.required.active_account"].severity == validator.SEV_ERROR
    assert findings["6.required.active_account"].count == 1


def test_account_lookup_over_limit_is_error_without_target_query(spark, monkeypatch):
    inputs = required_frames(spark)
    tables = inputs[0]
    tables["TITULO"] = spark.createDataFrame(
        [("11",), ("15",)], "NUM_CONTA_PARTICIPANTE string"
    )
    queries = []

    def fake_jdbc(_spark, _cfg, query):
        queries.append(query)
        if validator.TIPO_OPER_OBJETO_SERV_TABLE in query:
            return inputs[2]
        if validator.V_PARAMETRO_SIC_TABLE in query:
            return spark.createDataFrame(
                [(100, 49, 44)],
                "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, "
                "NUM_ID_OBJETO_SERVICO long",
            )
        if validator.V_OBJETOS_SERVICO_TABLE in query:
            return inputs[4]
        return inputs[3]

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")

    finding = by_id(
        validator.check_lookup_combos(
            spark, cfg, tables, validator.Metadata(set(), {}, {}, {}, {}), 5, 1
        )
    )["6.required.active_account"]

    assert finding.severity == validator.SEV_ERROR
    assert "more than 1" in finding.message
    assert not any(f"{validator.CONTA_PARTICIPANTE_TABLE} cp" in query for query in queries)
    assert_failed_have_hints([finding])


def test_account_lazy_jdbc_failure_becomes_required_error(spark, monkeypatch):
    class FailsOnMaterialization:
        def collect(self):
            raise RuntimeError("account lookup failed during action")

    inputs = required_frames(spark)
    _, _, tos_df, tipo_df, cdb_df = inputs
    sic_df = spark.createDataFrame(
        [(100, 49, 44)],
        "NUM_ID_TIPO_OPER_OBJETO_SERV long, NUM_TIPO_IF long, NUM_ID_OBJETO_SERVICO long",
    )

    def fake_jdbc(_spark, _cfg, query):
        if f"FROM {_cfg.schema}.{validator.CONTA_PARTICIPANTE_TABLE} cp" in query:
            return FailsOnMaterialization()
        if f"FROM {_cfg.schema}.{validator.TIPO_OPER_OBJETO_SERV_TABLE}" in query:
            return tos_df
        if f"FROM {_cfg.schema}.{validator.V_PARAMETRO_SIC_TABLE}" in query:
            return sic_df
        if f"FROM {_cfg.schema}.{validator.V_OBJETOS_SERVICO_TABLE}" in query:
            return cdb_df
        return tipo_df

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")

    finding = by_id(
        validator.check_lookup_combos(
            spark, cfg, inputs[0], validator.Metadata(set(), {}, {}, {}, {}), 5
        )
    )["6.required.active_account"]

    assert finding.severity == validator.SEV_ERROR
    assert "account lookup failed during action" in finding.message
    assert_failed_have_hints([finding])


def test_empty_account_references_pass_without_account_jdbc_rows(spark, monkeypatch):
    inputs = required_frames(
        spark,
        title_account=None,
        deposit_account=None,
        p1_account=None,
        p2_account=None,
    )
    calls = []

    def fake_jdbc(_spark, _cfg, query):
        calls.append(query)
        raise AssertionError("JDBC should not be called without a configured connection")

    monkeypatch.setattr(validator, "_jdbc", fake_jdbc)
    cfg = validator.Config("unused", None, None, "", "CETIP")

    findings = by_id(
        validator.check_lookup_combos(
            spark, cfg, inputs[0], validator.Metadata(set(), {}, {}, {}, {}), 5
        )
    )

    assert findings["6.required.active_account"].passed
    assert calls == []

    direct_findings = by_id(
        validator.check_required_lookup_frames(
            inputs[0], None, inputs[2], inputs[3], inputs[4], sample=5
        )
    )
    assert direct_findings["6.required.active_account"].passed


def test_rdb_tos_semantics_remain_unsupported_without_cdb_assumptions(spark):
    profile = validator.VALIDATION_PROFILES["rdb"]
    findings = by_id(required_findings(
        spark,
        profile=profile,
        tos=[("100", "10", "45", "S")],
    ))

    assert findings["6.required.operation_tos"].severity == validator.SEV_WARN
    assert "operation type" in findings["6.required.operation_tos"].message
    assert findings["6.required.active_account"].severity == validator.SEV_WARN
    assert findings["6.required.cdb_platform"].severity == validator.SEV_WARN

    wrong_service = by_id(required_findings(
        spark,
        profile=profile,
        tos=[("100", "10", "44", "S")],
    ))
    assert wrong_service["6.required.operation_tos"].severity == validator.SEV_WARN


def test_rdb_lookup_does_not_query_or_evaluate_unproven_tos(spark, monkeypatch):
    profile = validator.VALIDATION_PROFILES["rdb"]
    inputs = frames(
        spark,
        operations=[(1, 100, 6)],
        tos=[(100, 10, 45, "S")],
        sic=[(100, 50, 45)],
        tipos=[(10, "S")],
    )
    direct = validator.check_lookup_combo_frames(*inputs, sample=5, profile=profile)
    assert [finding.check_id for finding in direct] == ["6.combo.unsupported"]

    calls = []
    monkeypatch.setattr(
        validator,
        "_jdbc",
        lambda *_args: calls.append(_args) or pytest.fail("RDB lookup must not query CDB rules"),
    )
    cfg = validator.Config("unused", "jdbc:oracle:test", "user", "password", "CETIP")
    findings = by_id(validator.check_lookup_combos(
        spark,
        cfg,
        {"OPERACAO": inputs[0]},
        validator.Metadata(set(), {}, {}, {}, {}),
        sample=5,
        profile=profile,
    ))

    assert calls == []
    assert findings["6.required.operation_tos"].severity == validator.SEV_WARN
