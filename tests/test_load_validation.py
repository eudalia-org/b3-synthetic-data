import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import load_tables as L  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("load-val-test").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


class TestCapacity:
    def test_capacity_from_precision_scale(self):
        # True max magnitude (10^p - 1) / 10^s, not just the integer part.
        assert L.capacity_from_precision_scale(2, 0) == 99
        assert L.capacity_from_precision_scale(4, 2) == Decimal("99.99")
        assert L.capacity_from_precision_scale(2, 2) == Decimal("0.99")
        assert L.capacity_from_precision_scale(None, None) is None

    def test_scaled_value_within_domain_not_flagged(self):
        # NUMBER(4,2) holds 99.99; a value of 99.5 is valid and must NOT flag.
        profile = {"K": {"max": Decimal("99.5"), "min": Decimal("-99.5")}}
        target = {"K": {"precision": 4, "scale": 2}}
        assert L.numeric_domain_violations("T", profile, target) == []

    def test_scaled_value_overflow_flagged(self):
        # 100.0 exceeds NUMBER(4,2)'s 99.99 magnitude.
        profile = {"K": {"max": Decimal("100.0"), "min": Decimal("0")}}
        target = {"K": {"precision": 4, "scale": 2}}
        out = L.numeric_domain_violations("T", profile, target)
        assert len(out) == 1 and out[0].columns == "K"


class TestColumnAlignment:
    def test_extra_synthetic_column_flagged(self):
        target = {"A": {"nullable": True, "has_default": False}}
        out = L.column_alignment_violations("T", {"A", "B"}, target)
        assert [(v.check, v.columns) for v in out] == [("column_alignment", "B")]

    def test_missing_required_column_flagged(self):
        target = {
            "A": {"nullable": True, "has_default": False},
            "B": {"nullable": False, "has_default": False},  # required
            "C": {"nullable": False, "has_default": True},   # not required (default)
        }
        out = L.column_alignment_violations("T", {"A"}, target)
        cols = sorted(v.columns for v in out)
        assert cols == ["B"]  # C has a default; A present


class TestNumericDomain:
    def test_overflow_over_and_under(self):
        profile = {"K": {"max": 150, "min": -150}}
        target = {"K": {"precision": 2, "scale": 0}}  # cap 99
        out = L.numeric_domain_violations("T", profile, target)
        # max overflow + min underflow → two separate violations for column K
        assert len(out) == 2 and all(v.columns == "K" for v in out)

    def test_within_domain_ok(self):
        profile = {"K": {"max": 99, "min": 0}}
        target = {"K": {"precision": 2, "scale": 0}}
        assert L.numeric_domain_violations("T", profile, target) == []

    def test_unconstrained_number_skipped(self):
        profile = {"K": {"max": 10**30, "min": 0}}
        target = {"K": {"precision": None, "scale": None}}
        assert L.numeric_domain_violations("T", profile, target) == []


class TestStringLength:
    def test_too_long_flagged(self):
        profile = {"S": {"max_octet": 12}}
        target = {"S": {"data_length": 10}}
        out = L.string_length_violations("T", profile, target)
        assert len(out) == 1 and out[0].columns == "S"

    def test_fits_ok(self):
        assert L.string_length_violations(
            "T", {"S": {"max_octet": 10}}, {"S": {"data_length": 10}}) == []


class TestNotNull:
    def test_null_in_not_null_flagged(self):
        profile = {"A": {"null_count": 3}, "B": {"null_count": 0}}
        target = {"A": {"nullable": False}, "B": {"nullable": False}}
        out = L.not_null_violations("T", profile, target)
        assert [v.columns for v in out] == ["A"]


class TestUniqueness:
    def test_internal_dup_flagged(self):
        constraints = [("PK_T", ("A",)), ("UK_T", ("B", "C"))]
        out = L.uniqueness_violations(
            "T", constraints, total_count=100,
            distinct_counts={("A",): 100, ("B", "C"): 90},  # UK has dups
            prod_collision_counts={},
            nonnull_counts={("A",): 100, ("B", "C"): 100})
        assert [(v.check, v.columns) for v in out] == [("uniqueness_internal", "B,C")]

    def test_production_collision_flagged(self):
        out = L.uniqueness_violations(
            "T", [("PK_T", ("A",))], total_count=100,
            distinct_counts={("A",): 100},
            prod_collision_counts={("A",): 5},
            nonnull_counts={("A",): 100})
        assert [(v.check, v.columns) for v in out] == [("uniqueness_vs_production", "A")]

    def test_clean_no_violations(self):
        out = L.uniqueness_violations(
            "T", [("PK_T", ("A",))], total_count=100,
            distinct_counts={("A",): 100}, prod_collision_counts={("A",): 0},
            nonnull_counts={("A",): 100})
        assert out == []

    def test_nullable_uk_with_nulls_not_flagged(self):
        # 100 rows, 50 carry a NULL in the UK column. countDistinct drops those,
        # so distinct=50 must be compared against the 50 non-null rows, NOT 100.
        out = L.uniqueness_violations(
            "T", [("UK_T", ("B",))], total_count=100,
            distinct_counts={("B",): 50},
            prod_collision_counts={},
            nonnull_counts={("B",): 50})
        assert out == []

    def test_falls_back_to_total_when_nonnull_absent(self):
        # Without a non-null count (NOT NULL PK), compare against total_count.
        out = L.uniqueness_violations(
            "T", [("PK_T", ("A",))], total_count=100,
            distinct_counts={("A",): 90}, prod_collision_counts={})
        assert [(v.check, v.columns) for v in out] == [("uniqueness_internal", "A")]


class TestFkToStatic:
    def test_orphans_flagged(self):
        out = L.fk_to_static_violations(
            "T", {(("NUM_TIPO_IF",), "TIPO_IF"): 7, (("X",), "Y"): 0})
        assert [(v.columns, v.detail.startswith("7")) for v in out] == [("NUM_TIPO_IF", True)]


class TestValidateTable:
    def test_runs_all_checks_and_concatenates(self):
        # one violation from numeric domain + one from not-null
        out = L.validate_table(
            table="T",
            synthetic_cols={"K", "S"},
            profile={"K": {"max": 150, "min": 0, "null_count": 0},
                     "S": {"null_count": 2, "max_octet": 5}},
            target_cols={
                "K": {"precision": 2, "scale": 0, "nullable": True, "has_default": False},
                "S": {"data_length": 10, "nullable": False, "has_default": False},
            },
            constraints=[],
            total_count=10,
            distinct_counts={},
            prod_collision_counts={},
            fk_orphan_counts={},
        )
        checks = sorted(v.check for v in out)
        assert checks == ["not_null", "numeric_domain"]


class TestReport:
    def test_groups_by_table(self):
        vs = [L.Violation("A", "not_null", "X", "1 NULL"),
              L.Violation("A", "numeric_domain", "Y", "max>cap"),
              L.Violation("B", "fk_to_static", "Z", "orphans")]
        report = L.format_violation_report(vs)
        assert "A" in report and "B" in report and "not_null" in report and "Z" in report

    def test_empty_report(self):
        assert L.format_violation_report([]) == "No violations."


class TestDryRunArg:
    def test_dry_run_flag_parses(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["load_tables", "--dry-run"])
        args = L.parse_arguments()
        assert args.dry_run is True

    def test_dry_run_defaults_false(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["load_tables"])
        args = L.parse_arguments()
        assert args.dry_run is False


class TestProfile:
    def test_profile_numeric_string_null_distinct(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("K", T.LongType()),
            T.StructField("S", T.StringType()),
        ])
        df = spark.createDataFrame([(1, "ab"), (2, "abcd"), (2, None)], schema)
        # target_cols marks K numeric, S string; constraint PK(K)
        target_cols = {
            "K": {"is_numeric": True, "is_string": False, "nullable": False},
            "S": {"is_numeric": False, "is_string": True, "nullable": False},
        }
        prof = L.profile_synthetic_table(df, target_cols, [("PK", ("K",))])
        assert prof["total_count"] == 3
        assert prof["columns"]["K"]["max"] == 2 and prof["columns"]["K"]["min"] == 1
        assert prof["columns"]["K"]["null_count"] == 0
        assert prof["columns"]["S"]["max_octet"] == 4
        assert prof["columns"]["S"]["null_count"] == 1
        assert prof["distinct_counts"][("K",)] == 2  # values 1,2 (dup 2)
        assert prof["nonnull_counts"][("K",)] == 3   # K never null

    def test_nullable_uk_nonnull_count_excludes_nulls(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("B", T.StringType())])
        # 4 rows: two distinct non-null ("a","b") + two NULLs
        df = spark.createDataFrame([("a",), ("b",), (None,), (None,)], schema)
        target_cols = {"B": {"is_numeric": False, "is_string": True, "nullable": True}}
        prof = L.profile_synthetic_table(df, target_cols, [("UK", ("B",))])
        assert prof["total_count"] == 4
        assert prof["distinct_counts"][("B",)] == 2   # countDistinct drops NULLs
        assert prof["nonnull_counts"][("B",)] == 2    # only the 2 non-null rows
        # distinct == nonnull -> no internal-dup violation despite distinct < total
        out = L.uniqueness_violations(
            "T", [("UK", ("B",))], total_count=prof["total_count"],
            distinct_counts=prof["distinct_counts"], prod_collision_counts={},
            nonnull_counts=prof["nonnull_counts"])
        assert out == []


class TestJoinCores:
    def test_single_key_collision(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.LongType())])
        syn = spark.createDataFrame([(1,), (2,), (3,)], schema)
        existing = spark.createDataFrame([(2,), (3,), (4,)], schema)
        assert L._count_key_collisions(syn, existing, ["K"]) == 2  # 2 and 3

    def test_composite_key_collision(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("K", T.LongType()),
            T.StructField("S", T.StringType()),
        ])
        syn = spark.createDataFrame([(1, "a"), (2, "b")], schema)
        existing = spark.createDataFrame([(2, "b"), (3, "c")], schema)
        assert L._count_key_collisions(syn, existing, ["K", "S"]) == 1  # (2,"b")

    def test_no_collision_clean(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.LongType())])
        syn = spark.createDataFrame([(1,), (2,)], schema)
        existing = spark.createDataFrame([(8,), (9,)], schema)
        assert L._count_key_collisions(syn, existing, ["K"]) == 0

    def test_composite_orphans_exclude_null_fk_rows(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("A", T.LongType()),
            T.StructField("B", T.StringType()),
        ])
        # (1,"x") present in parent; (2,"y") orphan; (None,"z") NULL FK -> excluded
        syn = spark.createDataFrame([(1, "x"), (2, "y"), (None, "z")], schema)
        parent = spark.createDataFrame([(1, "x"), (5, "w")], schema)
        assert L._count_orphans(syn, parent, ["A", "B"]) == 1  # only (2,"y")

    def test_orphans_clean(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("A", T.LongType())])
        syn = spark.createDataFrame([(1,), (2,)], schema)
        parent = spark.createDataFrame([(1,), (2,), (3,)], schema)
        assert L._count_orphans(syn, parent, ["A"]) == 0
