import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import validate_tables as vt  # noqa: E402


@pytest.mark.skip(reason="requires Spark (JDK 17-21); runs on OCI Data Flow")
class TestChecksIntegration:
    def test_checks_against_small_frames(self, spark):
        # not_null: a null in a NOT NULL column is one violation
        # decimal_domain: value >= 10**(p-s) is a violation
        # varchar_domain: len(str) > char_length is a violation
        # pk_unique: duplicate pk rows are violations; pk_collision: synth pk in raw
        # fk: child FK not in (raw union synthetic) parent pk is a violation
        # unique: duplicate non-null unique tuples are violations
        ...


class TestReport:
    def test_has_violations_and_summary(self):
        report = vt.Report(findings=[
            vt.Finding(table="T", check="not_null", target="A",
                       violation_count=3, sample=[{"A": None}], ok=False),
            vt.Finding(table="T", check="pk_unique", target="ID",
                       violation_count=0, sample=[], ok=True),
        ])
        assert report.has_violations is True
        assert report.summary_counts == {"ok": 1, "violations": 1}

    def test_clean_report_has_no_violations(self):
        report = vt.Report(findings=[
            vt.Finding(table="T", check="not_null", target="A",
                       violation_count=0, sample=[], ok=True)])
        assert report.has_violations is False

    def test_report_to_json_roundtrips(self):
        report = vt.Report(findings=[
            vt.Finding(table="T", check="fk", target="FK1",
                       violation_count=2, sample=[{"FK": 9}], ok=False)])
        blob = vt.report_to_json(report)
        assert blob["has_violations"] is True
        assert blob["findings"][0]["table"] == "T"
        assert blob["findings"][0]["violation_count"] == 2

    def test_render_summary_lists_violations_first(self):
        report = vt.Report(findings=[
            vt.Finding(table="A", check="not_null", target="X",
                       violation_count=0, sample=[], ok=True),
            vt.Finding(table="B", check="fk", target="Y",
                       violation_count=5, sample=[], ok=False)])
        text = vt.render_summary(report)
        assert text.index("B") < text.index("A")  # violations first
        assert "5" in text


class TestPureHelpers:
    def test_decimal_max_abs(self):
        assert vt.decimal_max_abs(3, 0) == 999
        assert vt.decimal_max_abs(5, 2) == 999      # 3 integer digits
        assert vt.decimal_max_abs(2, 0) == 99
        assert vt.decimal_max_abs(1, 1) == 0        # no integer digits

    def test_normalize_schema_strips_owner(self):
        schema = {"CETIP.T": {"columns": {"A": {"type": "NUMBER", "nullable": False}}}}
        out = vt.normalize_schema(schema)
        assert "T" in out and "CETIP.T" not in out

    def test_normalize_specs_strips_owner_and_parent(self):
        specs = {"CETIP.CHILD": {"pk_cols": ["ID"], "foreign_keys": [
            {"columns": ["PID"], "parent_table": "CETIP.PARENT",
             "parent_columns": ["ID"]}]}}
        out = vt.normalize_specs(specs)
        assert "CHILD" in out
        assert out["CHILD"]["foreign_keys"][0]["parent_table"] == "PARENT"


class TestPlanChecks:
    def test_lists_checks_and_fk_parents(self):
        specs = {
            "PARENT": {"pk_cols": ["ID"]},
            "CHILD": {"pk_cols": ["CID"], "foreign_keys": [
                {"columns": ["PID"], "parent_table": "PARENT", "parent_columns": ["ID"]}]},
        }
        schema = {
            "PARENT": {"columns": {"ID": {"type": "NUMBER", "precision": 5,
                                          "scale": 0, "nullable": False}}},
            "CHILD": {"columns": {"CID": {"type": "NUMBER", "precision": 5,
                                          "scale": 0, "nullable": False},
                                  "PID": {"type": "NUMBER", "precision": 5,
                                          "scale": 0, "nullable": True}},
                      "unique": [["PID"]]},
        }
        plan = vt.plan_checks(specs, schema, tables=None)
        child = next(p for p in plan if p["table"] == "CHILD")
        assert child["not_null"] == ["CID"]          # PID nullable -> not enforced
        assert ["PID"] in child["unique"]
        assert child["fks"][0]["parent_table"] == "PARENT"

    def test_tables_subset_filters(self):
        specs = {"A": {"pk_cols": ["X"]}, "B": {"pk_cols": ["Y"]}}
        schema = {"A": {"columns": {"X": {"type": "NUMBER", "precision": 5,
                                          "scale": 0, "nullable": False}}},
                  "B": {"columns": {"Y": {"type": "NUMBER", "precision": 5,
                                          "scale": 0, "nullable": False}}}}
        plan = vt.plan_checks(specs, schema, tables=["A"])
        assert {p["table"] for p in plan} == {"A"}
