import hashlib
import json
import math
import os
import sys
from datetime import date, datetime
from decimal import Decimal

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from scripts import compare_cross_table_similarity as similarity  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("cross-table-similarity-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    similarity.configure_spark(session)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def analyzed(spark):
    rows_a = [
        (1, 10, 11, "2024-01-01", "x", " a ", "a", 1, 0, date(2024, 1, 1),
         datetime(2024, 1, 1, 0), 1.0),
        (2, 20, 21, "2024-01-02", "x", "b", "b", 2, 1, date(2024, 1, 2),
         datetime(2024, 1, 2, 0), float("nan")),
        (3, 30, 31, "2024-01-03", "x", "c", "c", 2, 2, date(2024, 1, 3),
         datetime(2024, 1, 3, 0), float("inf")),
        (4, 40, 41, "2024-01-04", "x", "", "d", 3, 3, date(2024, 1, 4),
         datetime(2024, 1, 4, 0), float("-inf")),
        (5, 50, 51, "2024-01-05", "x", None, "e", None, 4, date(2024, 1, 5),
         datetime(2024, 1, 5, 0), None),
    ]
    rows_b = [
        (1, "b", "a", 1, 0, date(2024, 1, 1), datetime(2024, 1, 1, 0)),
        (2, " C ", "b", 2, 0, date(2024, 1, 2), datetime(2024, 1, 2, 0)),
        (3, "d", "c", 3, 1, date(2024, 1, 4), datetime(2024, 1, 4, 0)),
        (4, " ", "x", 3, 3, date(2024, 1, 5), datetime(2024, 1, 5, 0)),
        (5, None, "y", None, 4, date(2024, 1, 6), datetime(2024, 1, 6, 0)),
    ]
    tables = {
        "A": spark.createDataFrame(
            rows_a,
            "id long, fk_a long, fk_b long, DAT_INCLUSAO string, extra_skip string, "
            "code string, sketch string, low long, high long, day date, moment timestamp, "
            "floating double",
        ),
        "B": spark.createDataFrame(
            rows_b,
            "id long, code string, sketch string, low long, high long, day date, moment timestamp",
        ),
        "C": spark.createDataFrame(
            [(1, "z"), (2, "z"), (3, "w"), (4, None), (5, None)], "extra_id long, extra string"
        ),
    }
    specs = {
        "A": {
            "pk_cols": ["id"],
            "foreign_keys": [{"columns": ["fk_a", "fk_b"], "parent_table": "P"}],
        },
        "B": {"pk_cols": ["id"]},
    }
    config = similarity.Config(
        exact_distinct_threshold=3,
        sketch_size=3,
        numeric_categorical_threshold=3,
        psi_bins=4,
        min_nonnull=3,
        min_distinct=2,
        top=100,
        jaccard_match_threshold=0.5,
        psi_match_threshold=0.0,
        psi_accuracy=10_000,
    )
    return similarity.analyze_tables(
        spark, tables, specs=specs, extra_excludes=["extra_skip"], config=config
    ), config


def _profiles(analysis):
    return {(r["table"], r["column"]): r for r in analysis.columns.collect()}


def _pair(analysis, table_a, column_a, table_b, column_b):
    return analysis.pairs.where(
        (F.col("table_a") == table_a)
        & (F.col("column_a") == column_a)
        & (F.col("table_b") == table_b)
        & (F.col("column_b") == column_b)
    ).first()


def test_planning_excludes_composite_keys_audits_and_extra_names(spark):
    tables = {
        "KNOWN": spark.createDataFrame(
            [],
            "pk1 long, pk2 long, fk1 long, fk2 long, DAT_ALTERACAO timestamp, "
            "custom string, payload string, parquet_extra string",
        ),
        "ABSENT": spark.createDataFrame([], "id long, payload string"),
    }
    specs = {
        "schema.KNOWN": {
            "pk_cols": ["PK1", "pk2"],
            "fks": [{"columns": ["FK1", "fk2"]}],
        }
    }
    plans = {
        (p.table, p.column): p
        for p in similarity.plan_columns(tables, specs, extra_excludes=["CUSTOM"])
    }
    assert plans[("KNOWN", "pk1")].exclusion_reason == "primary_key"
    assert plans[("KNOWN", "pk2")].exclusion_reason == "primary_key"
    assert plans[("KNOWN", "fk1")].exclusion_reason == "foreign_key"
    assert plans[("KNOWN", "fk2")].exclusion_reason == "foreign_key"
    assert plans[("KNOWN", "DAT_ALTERACAO")].exclusion_reason == "audit_column"
    assert plans[("KNOWN", "custom")].exclusion_reason == "extra_exclude"
    assert plans[("KNOWN", "parquet_extra")].included
    assert plans[("ABSENT", "id")].included  # table absent from specs means no keys


def test_physical_type_json_is_exact_and_unsupported_types_are_reported(spark):
    schema = T.StructType(
        [
            T.StructField("i", T.IntegerType()),
            T.StructField("l", T.LongType()),
            T.StructField("d1", T.DecimalType(10, 2)),
            T.StructField("d2", T.DecimalType(12, 2)),
            T.StructField("ts", T.TimestampType()),
            T.StructField("binary", T.BinaryType()),
            T.StructField("array", T.ArrayType(T.StringType())),
        ]
    )
    plans = {
        p.column: p
        for p in similarity.plan_columns({"T": spark.createDataFrame([], schema)}, {})
    }
    assert plans["i"].type_json != plans["l"].type_json
    assert plans["d1"].type_json != plans["d2"].type_json
    assert plans["d1"].type_json == T.DecimalType(10, 2).json()
    assert plans["ts"].supported
    assert plans["binary"].exclusion_reason == "unsupported_type"
    assert plans["array"].exclusion_reason == "unsupported_type"


def test_candidate_matrix_is_cross_table_same_type_only(analyzed):
    analysis, _ = analyzed
    assert analysis.pairs.where(F.col("table_a") == F.col("table_b")).count() == 0
    assert analysis.pairs.where(F.col("type_json").isNull()).count() == 0
    profiles = _profiles(analysis)
    assert ("A", "id") in profiles and not profiles[("A", "id")]["candidate"]
    assert _pair(analysis, "A", "code", "B", "code") is not None


def test_normalization_null_empty_nan_and_infinity(analyzed):
    analysis, _ = analyzed
    profiles = _profiles(analysis)
    code = profiles[("A", "code")]
    assert (code["row_count"], code["valid_count"], code["null_count"]) == (5, 3, 2)
    assert code["invalid_count"] == 0 and code["null_rate"] == 0.4
    assert {
        row["value"]
        for row in analysis.jaccard_values.where(
            (F.col("table") == "A") & (F.col("column") == "code")
        ).collect()
    } == {"A", "B", "C"}
    floating = profiles[("A", "floating")]
    assert floating["valid_count"] == 1
    assert floating["invalid_count"] == 3
    assert floating["null_count"] == 1
    assert not floating["eligible"]


def test_eligibility_boundaries_and_numeric_routing(analyzed):
    analysis, _ = analyzed
    profiles = _profiles(analysis)
    assert profiles[("A", "code")]["eligible"]  # exactly min_nonnull=3
    assert profiles[("C", "extra")]["eligible"]  # exactly two distinct
    assert profiles[("A", "low")]["route"] == "jaccard"
    assert profiles[("A", "high")]["route"] == "psi"
    mismatch = _pair(analysis, "A", "low", "B", "high")
    assert mismatch["status"] == "metric_mismatch"
    assert mismatch["metric"] is None and mismatch["jaccard"] is None and mismatch["psi"] is None


def test_exact_jaccard_hand_case_and_configurable_boundary(analyzed):
    analysis, _ = analyzed
    pair = _pair(analysis, "A", "code", "B", "code")
    assert pair["mode"] == "exact"  # distinct==3, threshold==3 is exact
    assert pair["jaccard"] == pytest.approx(2 / 4)
    assert pair["is_match"] is True  # inclusive threshold boundary
    assert pair["sketch_sample_size"] is None


def _expected_bottom_k(left, right, k):
    def rank(value):
        return hashlib.sha256(value.encode()).hexdigest()

    ka = set(sorted(left, key=lambda value: (rank(value), value))[:k])
    kb = set(sorted(right, key=lambda value: (rank(value), value))[:k])
    union_sample = sorted(ka | kb, key=lambda value: (rank(value), value))[:k]
    shared = sum(value in ka and value in kb for value in union_sample)
    return shared / len(union_sample), len(union_sample), shared


def _wilson(successes, total):
    z = 1.959963984540054
    p = successes / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total**2)) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def test_bottom_k_union_estimator_determinism_interval_and_mixed_mode(analyzed):
    analysis, _ = analyzed
    pair = _pair(analysis, "A", "sketch", "B", "sketch")
    score, sample_size, shared = _expected_bottom_k(
        {"A", "B", "C", "D", "E"}, {"A", "B", "C", "X", "Y"}, 3
    )
    assert pair["mode"] == "bottom_k"
    assert pair["jaccard"] == score
    assert pair["sketch_sample_size"] == sample_size
    assert (pair["jaccard_interval_low"], pair["jaccard_interval_high"]) == pytest.approx(
        _wilson(shared, sample_size)
    )
    again = _pair(analysis, "A", "sketch", "B", "sketch")
    assert again["jaccard"] == pair["jaccard"]
    mixed = _pair(analysis, "A", "sketch", "B", "code")
    assert mixed["mode"] == "bottom_k"  # one > threshold, one exactly at threshold


def test_exact_scorer_uses_value_inverted_index_for_wide_profiles(spark):
    values = [
        (table, f"c{column}", '"string"', f"V{value}", None, None)
        for table in ("A", "B")
        for column in range(20)
        for value in range(25)
    ]
    jvalues = spark.createDataFrame(
        values,
        "table string, column string, type_json string, value string, "
        "sha256_rank string, sketch_rank int",
    )
    columns = spark.createDataFrame(
        [
            (table, f"c{column}", '"string"', True, "jaccard", 25)
            for table in ("A", "B")
            for column in range(20)
        ],
        "table string, column string, type_json string, eligible boolean, route string, "
        "distinct_count long",
    )
    intersections = similarity._inverted_exact_intersections(
        jvalues, columns, similarity.Config(exact_distinct_threshold=25)
    )
    plan = intersections._jdf.queryExecution().logical().toString()
    assert "distinct_count_a" not in plan and "valid_count_a" not in plan
    assert "value#" in plan and "type_json#" in plan and "Join Inner" in plan
    rows = intersections.collect()
    assert len(rows) == 400 and {row["intersection_count"] for row in rows} == {25}


def test_exact_only_profiles_do_not_pay_for_hash_sort(spark):
    values = spark.createDataFrame(
        [
            (table, "code", '"string"', "string", value, None, True, False)
            for table in ("A", "B")
            for value in ("A", "B", "C")
        ],
        "table string, column string, type_json string, kind string, value string, "
        "psi_value string, is_valid boolean, is_invalid boolean",
    )
    columns = spark.createDataFrame(
        [(table, "code", '"string"', "jaccard") for table in ("A", "B")],
        "table string, column string, type_json string, route string",
    )
    pairs = spark.createDataFrame(
        [("ok", "jaccard", 3, 3, "A", "code", "B", "code", '"string"')],
        "status string, metric string, distinct_count_a long, distinct_count_b long, "
        "table_a string, column_a string, table_b string, column_b string, type_json string",
    )
    result = similarity._jaccard_values(
        values, columns, pairs, similarity.Config(exact_distinct_threshold=3)
    )
    assert result.where(F.col("sha256_rank").isNotNull()).count() == 0


def test_psi_matches_hand_calculation_with_jeffreys_smoothing(analyzed):
    analysis, _ = analyzed
    pair = _pair(analysis, "A", "high", "B", "high")
    bins = analysis.psi_bins.where(
        (F.col("column") == "high") & F.col("table").isin("A", "B")
    ).collect()
    by_table = {}
    for row in bins:
        by_table.setdefault(row["table"], {})[row["bin_index"]] = row["count"]
    effective = bins[0]["effective_bins"]
    expected = 0.0
    for index in range(effective):
        p = (by_table["A"][index] + 0.5) / (5 + 0.5 * effective)
        q = (by_table["B"][index] + 0.5) / (5 + 0.5 * effective)
        expected += (p - q) * math.log(p / q)
    assert pair["metric"] == "psi" and pair["status"] == "ok"
    assert pair["psi"] == pytest.approx(expected)
    assert json.loads(pair["psi_edges_json"]) == sorted(set(json.loads(pair["psi_edges_json"])))


def test_balanced_global_cuts_ignore_row_weight_and_collapse_duplicates(spark):
    def cuts(repetitions):
        rows = [
            ("T1", "x", '"double"', "numeric", str(value), float(value), True, False)
            for value in range(10)
        ]
        rows += [
            ("T2", "y", '"double"', "numeric", str(value), float(value), True, False)
            for _ in range(repetitions)
            for value in range(100, 110)
        ]
        values = spark.createDataFrame(
            rows,
            "table string, column string, type_json string, kind string, value string, "
            "psi_value string, is_valid boolean, is_invalid boolean",
        )
        columns = spark.createDataFrame(
            [("T1", "x", '"double"', "psi", True), ("T2", "y", '"double"', "psi", True)],
            "table string, column string, type_json string, route string, eligible boolean",
        )
        return similarity._psi_edges(
            values,
            columns,
            {'"double"': T.DoubleType()},
            similarity.Config(psi_bins=4, psi_accuracy=100_000),
        ).first()["cuts"]

    assert cuts(1) == cuts(50)

    repeated = spark.createDataFrame(
        [("T", "x", '"double"', "numeric", str(v), float(v), True, False)
         for v in [0, 0, 0, 0, 1, 1, 1, 1]],
        "table string, column string, type_json string, kind string, value string, "
        "psi_value string, is_valid boolean, is_invalid boolean",
    )
    columns = spark.createDataFrame(
        [("T", "x", '"double"', "psi", True)],
        "table string, column string, type_json string, route string, eligible boolean",
    )
    collapsed = similarity._psi_edges(
        repeated,
        columns,
        {'"double"': T.DoubleType()},
        similarity.Config(psi_bins=4),
    ).first()["cuts"]
    assert collapsed == sorted(set(collapsed)) and len(collapsed) < 3


def test_balanced_representatives_match_deterministic_equal_column_mixture(spark):
    values = spark.createDataFrame(
        [
            (table, "x", '"long"', "numeric", str(value), str(value), True, False)
            for table, population in (
                ("A", [0, 0, 0, 100]),
                ("B", [1000, 1000, 1000, 1100]),
            )
            for value in population
        ],
        "table string, column string, type_json string, kind string, value string, "
        "psi_value string, is_valid boolean, is_invalid boolean",
    )
    columns = spark.createDataFrame(
        [(table, "x", '"long"', "psi", True) for table in ("A", "B")],
        "table string, column string, type_json string, route string, eligible boolean",
    )
    edge = similarity._psi_edges(
        values,
        columns,
        {'"long"': T.LongType()},
        similarity.Config(psi_bins=4, psi_representatives=4, psi_accuracy=100_000),
    ).first()
    assert edge["cuts"] == ["0", "100", "1000"]
    assert edge["representative_count"] == 4


def test_native_psi_preserves_large_longs_decimals_and_timestamp_microseconds(spark):
    base = 2**62
    rows_a = [
        (
            base + i,
            Decimal(f"12345678901234567890.{i:010d}"),
            datetime(2024, 1, 1, 0, 0, 0, i),
        )
        for i in range(8)
    ]
    rows_b = [
        (
            base + i + 4,
            Decimal(f"12345678901234567890.{i + 4:010d}"),
            datetime(2024, 1, 1, 0, 0, 0, i + 4),
        )
        for i in range(8)
    ]
    schema = "big long, precise decimal(38,10), moment timestamp"
    config = similarity.Config(
        numeric_categorical_threshold=2,
        min_nonnull=3,
        min_distinct=2,
        psi_bins=4,
        psi_representatives=20,
        psi_accuracy=100_000,
    )
    analysis = similarity.analyze_tables(
        spark,
        {"A": spark.createDataFrame(rows_a, schema), "B": spark.createDataFrame(rows_b, schema)},
        config=config,
    )
    for column in ("big", "precise", "moment"):
        pair = _pair(analysis, "A", column, "B", column)
        assert pair["status"] == "ok" and pair["psi"] > 0, pair
        bins = analysis.psi_bins.where(F.col("column") == column).collect()
        assert bins[0]["effective_bins"] > 1
    long_edges = json.loads(_pair(analysis, "A", "big", "B", "big")["psi_edges_json"])
    assert all(int(edge) >= base for edge in long_edges)
    precise_edges = json.loads(
        _pair(analysis, "A", "precise", "B", "precise")["psi_edges_json"]
    )
    assert all(edge.startswith("12345678901234567890.") for edge in precise_edges)
    assert any(not edge.endswith("0000000000") for edge in precise_edges)
    moment_edges = json.loads(
        _pair(analysis, "A", "moment", "B", "moment")["psi_edges_json"]
    )
    assert len(set(moment_edges)) > 1 and all(edge.isdigit() for edge in moment_edges)


def test_dates_timestamps_and_timezone_metadata(analyzed, spark):
    analysis, _ = analyzed
    profiles = _profiles(analysis)
    assert profiles[("A", "day")]["route"] == "psi"
    assert profiles[("A", "moment")]["route"] == "psi"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert analysis.psi_bins.where(F.col("column").isin("day", "moment")).where(
        F.col("timezone") != "UTC"
    ).count() == 0


def test_decimal_values_retain_precision_and_scale_in_identity(spark):
    tables = {
        "A": spark.createDataFrame([(Decimal("1.20"),)], "v decimal(10,2)"),
        "B": spark.createDataFrame([(Decimal("1.20"),)], "v decimal(12,2)"),
    }
    plans = similarity.plan_columns(tables, {})
    assert plans[0].type_json != plans[1].type_json


def test_rankings_are_separate_deterministic_and_thresholds_are_inclusive(analyzed, tmp_path):
    analysis, config = analyzed
    report = similarity.build_report(
        analysis, ["A", "B", "C"], "memory", "", None, str(tmp_path), config
    )
    jaccard = report["top_qualifying_matches"]["jaccard"]
    psi = report["top_qualifying_matches"]["psi"]
    assert all(row["metric"] == "jaccard" and row["jaccard"] >= 0.5 for row in jaccard)
    assert all(row["metric"] == "psi" and row["psi"] <= 0.0 for row in psi)
    assert report["key_assumptions"]["tables_absent_from_specs"] == ["C"]
    assert "assumed to have no" in report["key_assumptions"]["warning"]
    j_keys = [(r["table_a"], r["column_a"], r["table_b"], r["column_b"]) for r in jaccard]
    assert j_keys == sorted(j_keys, key=lambda key: (-next(
        r["jaccard"] for r in jaccard
        if (r["table_a"], r["column_a"], r["table_b"], r["column_b"]) == key
    ), key))


@pytest.mark.parametrize(
    ("output", "inputs"),
    [
        ("/", ["/tmp/raw/T"]),
        ("file:///", ["file:///tmp/raw/T"]),
        ("oci://bucket@namespace", ["oci://bucket@namespace/raw/T"]),
        ("/tmp/raw", ["file:///tmp/raw/T"]),
        ("file:///tmp/raw/T/", ["/tmp/raw/T"]),
        ("oci://bucket@namespace/raw", ["oci://bucket@namespace/raw/T"]),
        ("oci://bucket@namespace/raw/T", ["oci://bucket@namespace/raw/T/"]),
    ],
)
def test_output_path_rejects_roots_equal_inputs_and_input_ancestors(output, inputs):
    with pytest.raises(ValueError, match="Unsafe output"):
        similarity.validate_output_base(output, inputs)


def test_path_normalization_is_segment_aware_for_file_and_oci():
    assert similarity.normalize_path("file:///tmp/a/../b//").text == "file:///tmp/b"
    assert similarity.normalize_path("/tmp/b").text == "file:///tmp/b"
    assert (
        similarity.normalize_path("oci://bucket@namespace/raw/a/../T//").text
        == "oci://bucket@namespace/raw/T"
    )
    allowed = similarity.validate_output_base(
        "oci://bucket@namespace/raw2", ["oci://bucket@namespace/raw/T"]
    )
    assert allowed.text.endswith("/raw2")


def test_interrupted_staging_preserves_previous_complete_output(spark, tmp_path):
    final = tmp_path / "final"
    final.mkdir()
    marker = final / "previous.marker"
    marker.write_text("old", encoding="utf-8")

    def prepare(staging):
        staging_path = type(final)(staging)
        staging_path.mkdir()
        (staging_path / "partial").write_text("partial", encoding="utf-8")
        raise RuntimeError("interrupted")

    with pytest.raises(RuntimeError, match="interrupted"):
        similarity._stage_and_publish(spark, str(final), prepare, lambda _root: None)
    assert marker.read_text(encoding="utf-8") == "old"
    assert not list(tmp_path.glob("final.__staging_*"))


class FakePublicationFs:
    def __init__(self, entries, failed_renames=()):
        self.entries = dict(entries)
        self.failed_renames = set(failed_renames)

    def exists(self, path):
        return str(path) in self.entries

    def rename(self, source, target):
        pair = (str(source), str(target))
        if pair in self.failed_renames or pair[0] not in self.entries:
            return False
        self.entries[pair[1]] = self.entries.pop(pair[0])
        return True

    def delete(self, path, recursive):
        return self.entries.pop(str(path), None) is not None


def test_promotion_failure_restores_previous_output():
    fs = FakePublicationFs(
        {"staging": "new", "final": "old"}, failed_renames={("staging", "final")}
    )
    with pytest.raises(ValueError, match="restored and verified"):
        similarity._promote_staging_paths(fs, "staging", "final", "backup")
    assert fs.entries == {"final": "old", "staging": "new"}


def test_output_schema_and_scoped_root_overwrite_preserve_adjacent_prefix(
    analyzed, spark, tmp_path
):
    analysis, _ = analyzed
    root = tmp_path / "similarity"
    adjacent = tmp_path / "similarity-neighbor"
    adjacent.mkdir()
    marker = adjacent / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    output = root.as_uri()

    report1 = {"run": 1, "summary": {"integrity": analysis.integrity}}
    report2 = {"run": 2, "summary": {"integrity": analysis.integrity}}
    similarity.write_outputs(spark, analysis, output, report1)
    stale = root / "stale.txt"
    stale.write_text("remove", encoding="utf-8")
    similarity.write_outputs(spark, analysis, output, report2)

    assert marker.read_text(encoding="utf-8") == "keep"
    assert not stale.exists()
    assert json.loads(similarity.read_text(spark, f"{output}/report.json")) == report2
    pairs = spark.read.parquet(f"{output}/pairs")
    required_pair_fields = {
        "table_a", "column_a", "table_b", "column_b", "type_json", "metric", "mode",
        "status", "jaccard", "psi", "threshold", "is_match", "sketch_sample_size",
        "jaccard_interval_low", "jaccard_interval_high",
    }
    assert required_pair_fields <= set(pairs.columns)
    columns = spark.read.parquet(f"{output}/profiles/columns")
    assert {
        "table", "column", "type_json", "included", "exclusion_reason", "row_count",
        "non_null_count", "valid_count", "invalid_count", "null_count", "distinct_count",
        "null_rate", "route", "eligible",
    } <= set(columns.columns)


def test_cli_parses_comma_and_repeatable_tables_and_defaults(monkeypatch):
    monkeypatch.setenv("DATAGEN_SPECS_URI", "file:///specs.json")
    args = similarity.parse_args(
        ["--tables", "A,B", "--tables", "C", "--base-uri", "/x", "--output-base", "/y"]
    )
    assert similarity._parse_repeated_csv(args.tables) == ["A", "B", "C"]
    assert args.specs_uri == "file:///specs.json"
    assert similarity.Config() == similarity.Config(
        exact_distinct_threshold=100_000,
        sketch_size=2_048,
        numeric_categorical_threshold=100,
        psi_bins=10,
        min_nonnull=100,
        min_distinct=2,
        top=100,
        jaccard_match_threshold=0.8,
        psi_match_threshold=0.05,
        psi_accuracy=10_000,
        psi_representatives=100,
        timezone="UTC",
    )


def test_builtin_selftest_is_end_to_end(spark, capsys):
    similarity.run_selftest(spark)
    assert "SELF-TEST PASSED" in capsys.readouterr().out
