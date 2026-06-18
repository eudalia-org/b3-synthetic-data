import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import validate_tables as vt  # noqa: E402


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
