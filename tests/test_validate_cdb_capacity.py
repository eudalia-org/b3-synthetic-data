import sys
from decimal import Decimal
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import validate_products as validator  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("validate-cdb-capacity-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def contract_row(
    table,
    column,
    declared,
    *,
    automatic=True,
    caller_dependent=False,
    confidence="high",
    routes=None,
    java_type=None,
    java_property=None,
    mapping_source=None,
):
    row = {
        "table": table,
        "column": column,
        "declared_capacity": declared,
        "enforcement": {
            "automatic": automatic,
            "caller_dependent": caller_dependent,
        },
        "confidence": confidence,
        "serializer_routes": routes or [],
    }
    for key, value in (
        ("java_type", java_type),
        ("java_property", java_property),
        ("mapping_source", mapping_source),
    ):
        if value is not None:
            row[key] = value
    return row


def findings_by_column(findings, check_id):
    return {finding.column: finding for finding in findings if finding.check_id == check_id}


def meta_for(table, capacity=None, *, nls="WE8ISO8859P1"):
    return validator.Metadata(
        {table}, {table: ["ID"]}, {}, {}, {}, capacity or {}, nls, "AL16UTF16"
    )


def test_contract_parser_rejects_malformed_rows_and_deduplicates_identical_rows(tmp_path):
    with pytest.raises(ValueError, match="rows list"):
        validator.parse_application_capacity_contract({})

    row = contract_row("T", "CODE", {"kind": "text", "value": 14, "unit": "utf16_code_units"})
    capacities = validator.parse_application_capacity_contract({"rows": [row, dict(row)]})
    assert capacities[("T", "CODE")].kind == "text"
    assert capacities[("T", "CODE")].ambiguous_sources == ("unidentified mapping",)

    malformed = tmp_path / "bad-contract.json"
    malformed.write_text("{not json", encoding="utf-8")
    with pytest.raises(SystemExit, match="application-capacity-contract"):
        validator.load_application_capacity_contract(None, str(malformed))
    with pytest.raises(SystemExit, match="application-capacity-contract"):
        validator.load_application_capacity_contract(None, str(tmp_path / "missing.json"))


def test_representative_duplicate_pattern_degrades_to_ambiguous_markers():
    duplicate_keys = [
        ("OPERACAO", "QTD_OPERACAO"),
        ("OPERACAO", "QTD_OPERACAO_ORIGINAL"),
        ("TITULO", "QTD_BLOQUEADA"),
        ("TITULO", "QTD_DEPOSITADA"),
        ("TITULO", "QTD_EMITIDA"),
        ("TITULO", "QTD_RESGATADA"),
    ]
    rows = [
        row
        for table, column in duplicate_keys
        for row in (
            contract_row(
                table,
                column,
                {"kind": "numeric", "integer_digits": 10, "decimal_digits": 2},
                java_type="NumeroReal",
                java_property="value",
            ),
            contract_row(
                table,
                column,
                {"kind": "numeric", "integer_digits": 12, "decimal_digits": 8},
                java_type="NumeroInteiro",
                java_property="value",
            ),
        )
    ]
    capacities = validator.parse_application_capacity_contract({"rows": rows})
    expected = {
        ("OPERACAO", "QTD_OPERACAO"),
        ("OPERACAO", "QTD_OPERACAO_ORIGINAL"),
        ("TITULO", "QTD_BLOQUEADA"),
        ("TITULO", "QTD_DEPOSITADA"),
        ("TITULO", "QTD_EMITIDA"),
        ("TITULO", "QTD_RESGATADA"),
    }
    assert {key for key, capacity in capacities.items() if capacity.kind == "ambiguous"} == expected
    assert capacities[("TITULO", "QTD_BLOQUEADA")].ambiguous_sources == (
        "NumeroInteiro.value",
        "NumeroReal.value",
    )


def test_duplicate_pattern_ambiguity_accumulates_sources_and_has_zero_count():
    rows = [
        contract_row(
            "T",
            "VALUE",
            {"kind": "numeric", "integer_digits": 2, "decimal_digits": 0},
            java_type=source,
            java_property="value",
        )
        for source in ("A", "B")
    ]
    rows.extend(
        [
            contract_row(
                "T",
                "VALUE",
                {"kind": "numeric", "integer_digits": digits, "decimal_digits": 0},
                java_type=source,
                java_property="value",
            )
            for source, digits in (("C", 3), ("D", 4))
        ]
    )
    contract = validator.parse_application_capacity_contract({"rows": rows})
    class ColumnsOnly:
        columns = ["ID", "VALUE"]

    finding = next(
        finding
        for finding in validator.check_capacity(
            {"T": ColumnsOnly()}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5
        )
        if finding.check_id == "4.capacity.application_ambiguous"
    )
    assert not finding.passed
    assert finding.count == 0
    assert finding.sample == []
    assert all(source in finding.message for source in ("A.value", "B.value", "C.value", "D.value"))


def test_application_text_capacity_cod_if_and_utf16_units(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "T", "COD_IF", {"kind": "text", "value": 14, "unit": "utf16_code_units"}
                ),
                contract_row(
                    "T", "UTF16", {"kind": "text", "value": 2, "unit": "utf16_code_units"}
                ),
                contract_row("T", "CHARS", {"kind": "text", "value": 2, "unit": "characters"}),
            ]
        }
    )
    df = spark.createDataFrame(
        [(1, "1" * 14, "a😀", "a😀"), (2, "1" * 15, "ab", "ab")],
        "ID long, COD_IF string, UTF16 string, CHARS string",
    )
    findings = findings_by_column(
        validator.check_capacity({"T": df}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5),
        "4.capacity.application_text",
    )

    assert findings["COD_IF"].severity == validator.SEV_ERROR
    assert findings["COD_IF"].count == 1
    assert findings["UTF16"].count == 1  # a + supplementary code point = 3 UTF-16 units
    assert findings["CHARS"].passed
    assert findings["COD_IF"].sample == [2]


def test_serializer_route_width_is_not_a_global_application_rule(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "T",
                    "COD_IF",
                    {"kind": "text", "value": 14, "unit": "utf16_code_units"},
                    routes=[{"route": "ASEL023", "width": 6, "overflow_behavior": "truncate"}],
                ),
            ]
        }
    )
    df = spark.createDataFrame([(1, "1234567")], "ID long, COD_IF string")
    finding = findings_by_column(
        validator.check_capacity({"T": df}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5),
        "4.capacity.application_text",
    )["COD_IF"]
    assert finding.passed


def test_application_numeric_enforcement_distinguishes_warn_and_error(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "T",
                    "CALLER",
                    {"kind": "numeric", "integer_digits": 2, "decimal_digits": 1},
                    automatic=False,
                    caller_dependent=True,
                ),
                contract_row(
                    "T",
                    "AUTO",
                    {"kind": "numeric", "integer_digits": 2, "decimal_digits": 1},
                ),
            ]
        }
    )
    df = spark.createDataFrame(
        [(1, Decimal("100.0"), Decimal("12.34"))],
        "ID long, CALLER decimal(10,2), AUTO decimal(10,2)",
    )
    findings = findings_by_column(
        validator.check_capacity({"T": df}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5),
        "4.capacity.application_number",
    )
    assert findings["CALLER"].severity == validator.SEV_WARN
    assert findings["CALLER"].count == 1
    assert findings["AUTO"].severity == validator.SEV_ERROR
    assert findings["AUTO"].count == 1


def test_application_numeric_scale_uses_original_decimal_precision(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "T",
                    "VALUE",
                    {"kind": "numeric", "integer_digits": 2, "decimal_digits": 2},
                )
            ]
        }
    )
    df = spark.createDataFrame([(1, Decimal("1.2301"))], "ID long, VALUE decimal(10,4)")
    finding = findings_by_column(
        validator.check_capacity({"T": df}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5),
        "4.capacity.application_number",
    )["VALUE"]
    assert finding.severity == validator.SEV_ERROR
    assert finding.count == 1


def test_ambiguous_application_capacity_warns_without_enforcement(spark):
    row = contract_row(
        "T",
        "VALUE",
        {"kind": "numeric", "integer_digits": 1, "decimal_digits": 0},
        java_type="FirstType",
        java_property="value",
    )
    alternate = contract_row(
        "T",
        "VALUE",
        {"kind": "numeric", "integer_digits": 99, "decimal_digits": 0},
        java_type="SecondType",
        java_property="value",
    )
    contract = validator.parse_application_capacity_contract({"rows": [row, alternate]})
    df = spark.createDataFrame([(1, Decimal("999"))], "ID long, VALUE decimal(10,0)")
    finding = findings_by_column(
        validator.check_capacity(
            {"T": df},
            meta_for("T", {"VALUE": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": 2, "DATA_SCALE": 0}}),
            contract,
            5,
        ),
        "4.capacity.application_ambiguous",
    )["VALUE"]
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert finding.count == 0
    assert finding.sample == []
    assert "FirstType.value" in finding.message
    assert "SecondType.value" in finding.message


def test_representative_ambiguous_key_is_warn_not_error(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "TITULO",
                    "QTD_BLOQUEADA",
                    {"kind": "numeric", "integer_digits": 10, "decimal_digits": 2},
                ),
                contract_row(
                    "TITULO",
                    "QTD_BLOQUEADA",
                    {"kind": "numeric", "integer_digits": 12, "decimal_digits": 8},
                ),
            ]
        }
    )
    df = spark.createDataFrame(
        [(1, Decimal("999999999999999"))], "ID long, QTD_BLOQUEADA decimal(30,0)"
    )
    finding = findings_by_column(
        validator.check_capacity(
            {"TITULO": df}, validator.Metadata(set(), {}, {}, {}, {}), contract, 5
        ),
        "4.capacity.application_ambiguous",
    )["QTD_BLOQUEADA"]
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert finding.count == 0
    assert finding.sample == []


def test_invalid_numeric_casts_are_capacity_violations(spark):
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row(
                    "T", "APP", {"kind": "numeric", "integer_digits": 2, "decimal_digits": 0}
                ),
            ]
        }
    )
    oracle_capacity = {
        "T": {
            "ORACLE": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": 3, "DATA_SCALE": 0},
        }
    }
    df = spark.createDataFrame(
        [(1, "not-a-number", "not-a-number")], "ID long, APP string, ORACLE string"
    )
    findings = validator.check_capacity({"T": df}, meta_for("T", oracle_capacity), contract, 5)
    app = findings_by_column(findings, "4.capacity.application_number")["APP"]
    oracle = findings_by_column(findings, "4.capacity.oracle_number")["ORACLE"]
    assert app.severity == validator.SEV_ERROR and app.count == 1
    assert oracle.severity == validator.SEV_ERROR and oracle.count == 1


def test_oracle_char_and_latin1_byte_capacity(spark):
    capacity = {
        "T": {
            "CHAR_COL": {"DATA_TYPE": "VARCHAR2", "CHAR_USED": "C", "CHAR_LENGTH": 2},
            "BYTE_COL": {"DATA_TYPE": "VARCHAR2", "CHAR_USED": "B", "DATA_LENGTH": 2},
        }
    }
    df = spark.createDataFrame(
        [(1, "abc", "é"), (2, "€", "€"), (3, "ab", "ééé")],
        "ID long, CHAR_COL string, BYTE_COL string",
    )
    findings = findings_by_column(
        validator.check_capacity({"T": df}, meta_for("T", capacity), {}, 5),
        "4.capacity.oracle_string",
    )
    # One value is too long and one is not representable in WE8ISO8859P1.
    assert findings["CHAR_COL"].count == 2
    # € cannot round-trip through WE8ISO8859P1; é is one ISO-8859-1 byte.
    assert findings["BYTE_COL"].count == 2
    assert all(finding.severity == validator.SEV_ERROR for finding in findings.values())


def test_oracle_number_precision_scale_and_unconstrained_number(spark):
    capacity = {
        "T": {
            "TIGHT": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": 3, "DATA_SCALE": 0},
            "NEG_SCALE": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": 3, "DATA_SCALE": -2},
            "WIDE": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": 30, "DATA_SCALE": 0},
            "FREE": {"DATA_TYPE": "NUMBER", "DATA_PRECISION": None, "DATA_SCALE": None},
        }
    }
    df = spark.createDataFrame(
        [
            (1, Decimal("999"), Decimal("99949"), Decimal("1E25"), Decimal("999999999999")),
            (2, Decimal("1000"), Decimal("99950"), Decimal("1E30"), Decimal("999999999999")),
        ],
        (
            "ID long, TIGHT decimal(10,0), NEG_SCALE decimal(10,0), "
            "WIDE decimal(38,0), FREE decimal(20,0)"
        ),
    )
    findings = findings_by_column(
        validator.check_capacity({"T": df}, meta_for("T", capacity), {}, 5),
        "4.capacity.oracle_number",
    )
    assert findings["TIGHT"].count == 1
    assert findings["NEG_SCALE"].count == 1
    assert findings["WIDE"].count == 1  # 10^25 fits NUMBER(30,0); 10^30 does not.
    assert "FREE" not in findings


def test_oracle_unknown_charset_warns_without_false_error(spark):
    capacity = {
        "T": {
            "BYTE_COL": {"DATA_TYPE": "VARCHAR2", "CHAR_USED": "B", "DATA_LENGTH": 2},
        }
    }
    df = spark.createDataFrame([(1, "€")], "ID long, BYTE_COL string")
    findings = validator.check_capacity({"T": df}, meta_for("T", capacity, nls=None), {}, 5)
    finding = next(f for f in findings if f.check_id == "4.capacity.oracle_string_unverified")
    assert finding.severity == validator.SEV_WARN
    assert not finding.passed
    assert finding.hint


def test_no_oracle_application_only_and_old_metadata_constructor(spark):
    old_meta = validator.Metadata(set(), {}, {}, {}, {})
    contract = validator.parse_application_capacity_contract(
        {
            "rows": [
                contract_row("T", "COD_IF", {"kind": "text", "value": 1, "unit": "characters"}),
            ]
        }
    )
    df = spark.createDataFrame([(1, "xx")], "ID long, COD_IF string")
    findings = validator.check_capacity({"T": df}, old_meta, contract, 5)
    assert len(findings) == 1
    assert findings[0].check_id == "4.capacity.application_text"
    assert findings[0].severity == validator.SEV_ERROR
