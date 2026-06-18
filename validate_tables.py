"""Offline DB-constraint validator for engorda's synthetic Parquet.

Checks the synthetic output against the same constraints Oracle enforces (PK,
FK, UNIQUE, NOT NULL, datatype precision/scale) WITHOUT running a load. The
core `validate(spark, ...) -> Report` is importable from a Data Science
notebook (pass your own SparkSession); `main()` is the Data Flow CLI wrapper.

Design: docs/plans/2026-06-18-validate-tables-design.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field

logger = logging.getLogger("validate_tables")

REQUIRED_ENV_VARS = (
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
    "DATAGEN_SPECS_URI",
    "DATAGEN_SCHEMA_URI",
)

SAMPLE_LIMIT = 10  # offending rows captured per finding


@dataclass
class Finding:
    table: str
    check: str            # not_null | decimal_domain | varchar_domain | pk_unique | pk_collision | fk | unique
    target: str           # column or constraint label
    violation_count: int
    sample: list
    ok: bool


@dataclass
class Report:
    findings: list = field(default_factory=list)

    @property
    def has_violations(self) -> bool:
        return any(not f.ok for f in self.findings)

    @property
    def summary_counts(self) -> dict:
        ok = sum(1 for f in self.findings if f.ok)
        return {"ok": ok, "violations": len(self.findings) - ok}


def report_to_json(report: Report) -> dict:
    return {
        "has_violations": report.has_violations,
        "summary": report.summary_counts,
        "findings": [
            {
                "table": f.table, "check": f.check, "target": f.target,
                "violation_count": f.violation_count, "sample": f.sample, "ok": f.ok,
            }
            for f in report.findings
        ],
    }


def render_summary(report: Report) -> str:
    rows = sorted(report.findings, key=lambda f: (f.ok, f.table, f.check))
    lines = [f"Validation: {report.summary_counts['violations']} violation(s), "
             f"{report.summary_counts['ok']} ok"]
    for f in rows:
        mark = "ok" if f.ok else "!!"
        lines.append(f"  [{mark}] {f.table}.{f.check}({f.target}) "
                     f"-> {f.violation_count} bad")
    return "\n".join(lines)
