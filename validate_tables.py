"""Offline DB-constraint validator for engorda's synthetic Parquet.

Checks the synthetic output against the same constraints Oracle enforces (PK,
FK, UNIQUE, NOT NULL, datatype precision/scale) WITHOUT running a load. The
core `validate(spark, ...) -> Report` is importable from a Data Science
notebook (pass your own SparkSession); `main()` is the Data Flow CLI wrapper.

Design: docs/plans/2026-06-18-validate-tables-design.md
"""
from __future__ import annotations

import argparse
import copy
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


def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def decimal_max_abs(precision: int, scale: int) -> int:
    """Largest absolute value a Decimal(precision, scale) can hold (int part)."""
    int_digits = precision - scale
    return (10 ** int_digits) - 1 if int_digits > 0 else 0


def normalize_schema(schema: dict) -> dict:
    return {table_path_name(str(name)): cfg for name, cfg in schema.items()}


def normalize_specs(specs: dict) -> dict:
    out: dict = {}
    for raw_name, cfg in specs.items():
        new_cfg = copy.deepcopy(dict(cfg))
        for fk in new_cfg.get("foreign_keys") or []:
            if isinstance(fk, dict) and fk.get("parent_table"):
                fk["parent_table"] = table_path_name(str(fk["parent_table"]))
        out[table_path_name(str(raw_name))] = new_cfg
    return out


def _sample(df, cols, limit=SAMPLE_LIMIT) -> list:
    rows = df.select(*cols).limit(limit).collect()
    return [row.asDict() for row in rows]


def check_not_null(df, table, not_null_cols) -> list:
    from pyspark.sql import functions as F
    findings = []
    for col in not_null_cols:
        if col not in df.columns:
            continue
        bad = df.filter(F.col(col).isNull())
        count = bad.count()
        findings.append(Finding(table, "not_null", col, count,
                                _sample(bad, [col]) if count else [], count == 0))
    return findings


def check_decimal_domain(df, table, col, precision, scale) -> Finding:
    from pyspark.sql import functions as F
    limit = decimal_max_abs(precision, scale)
    bad = df.filter(F.col(col).isNotNull() & (F.abs(F.col(col)) > F.lit(limit)))
    count = bad.count()
    return Finding(table, "decimal_domain", col, count,
                   _sample(bad, [col]) if count else [], count == 0)


def check_varchar_domain(df, table, col, length) -> Finding:
    from pyspark.sql import functions as F
    bad = df.filter(F.col(col).isNotNull() & (F.length(F.col(col)) > F.lit(length)))
    count = bad.count()
    return Finding(table, "varchar_domain", col, count,
                   _sample(bad, [col]) if count else [], count == 0)


def check_pk(synth_df, raw_df, table, pk_cols) -> list:
    """PK not-null + internal uniqueness + no collision with existing (raw) keys."""
    from pyspark.sql import functions as F
    findings = []
    # not-null: any pk column null
    null_cond = None
    for col in pk_cols:
        c = F.col(col).isNull()
        null_cond = c if null_cond is None else (null_cond | c)
    bad_null = synth_df.filter(null_cond)
    n_null = bad_null.count()
    findings.append(Finding(table, "pk_not_null", ",".join(pk_cols), n_null,
                            _sample(bad_null, pk_cols) if n_null else [], n_null == 0))
    # internal uniqueness
    dups = (synth_df.groupBy(*pk_cols).count().filter(F.col("count") > 1))
    n_dup = dups.count()
    findings.append(Finding(table, "pk_unique", ",".join(pk_cols), n_dup,
                            _sample(dups, pk_cols) if n_dup else [], n_dup == 0))
    # collision with existing real keys (raw)
    if raw_df is not None:
        synth_keys = synth_df.select(*pk_cols).distinct()
        raw_keys = raw_df.select(*pk_cols).distinct()
        collide = synth_keys.join(raw_keys, on=list(pk_cols), how="inner")
        n_col = collide.count()
        findings.append(Finding(table, "pk_collision", ",".join(pk_cols), n_col,
                                _sample(collide, pk_cols) if n_col else [], n_col == 0))
    return findings


def check_fk(child_df, parent_universe_df, table, child_cols, parent_cols, label) -> Finding:
    """Non-null child FK tuples must exist in (raw union synthetic) parent keys."""
    from pyspark.sql import functions as F
    cond = None
    for col in child_cols:  # only rows where every FK col is non-null are enforced
        c = F.col(col).isNotNull()
        cond = c if cond is None else (cond & c)
    child = child_df.filter(cond).select(*child_cols).distinct()
    parent = parent_universe_df.select(
        *[F.col(p).alias(c) for p, c in zip(parent_cols, child_cols)]).distinct()
    orphans = child.join(parent, on=list(child_cols), how="left_anti")
    count = orphans.count()
    return Finding(table, "fk", label, count,
                   _sample(orphans, list(child_cols)) if count else [], count == 0)


def check_unique(df, table, cols) -> Finding:
    """Duplicate non-null unique tuples (Oracle ignores rows with any null)."""
    from pyspark.sql import functions as F
    cond = None
    for col in cols:
        c = F.col(col).isNotNull()
        cond = c if cond is None else (cond & c)
    dups = df.filter(cond).groupBy(*cols).count().filter(F.col("count") > 1)
    count = dups.count()
    return Finding(table, "unique", ",".join(cols), count,
                   _sample(dups, list(cols)) if count else [], count == 0)
