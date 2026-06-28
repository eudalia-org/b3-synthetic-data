import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import load_tables as L  # noqa: E402


class TestCapacity:
    def test_capacity_from_precision_scale(self):
        assert L.capacity_from_precision_scale(2, 0) == 99
        assert L.capacity_from_precision_scale(10, 2) == 10**8 - 1
        assert L.capacity_from_precision_scale(2, 2) == 0
        assert L.capacity_from_precision_scale(None, None) is None


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
