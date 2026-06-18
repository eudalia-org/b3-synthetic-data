# validate_tables.py Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an offline validator that checks engorda's synthetic Parquet against the same constraints Oracle enforces (PK, FK, UNIQUE, NOT NULL, datatype precision/scale), without running a load.

**Architecture:** A new metadata dump (`extract_schema.sql`) + pure build script (`build_schema_from_dump.py`) produce `schema.json` (per-column domains + UNIQUE). A self-contained Data Flow app (`validate_tables.py`) exposes an importable `validate(spark, specs, schema, raw_base, synth_base, tables=None) -> Report` core plus a thin `main()` CLI. Five column-pruned checks run against the **raw ∪ synthetic** key universe; results become a JSON report (object storage) + a stdout summary, with a non-zero exit on any violation.

**Tech Stack:** Python 3, PySpark (OCI Data Flow), pytest. Mirrors the existing `extract_constraints.sql` / `build_specs_from_constraints.py` / `engorda_tables.py` patterns.

**Reference design:** `docs/plans/2026-06-18-validate-tables-design.md`

**Conventions for this repo:**
- Run tests with `.venv/bin/python -m pytest` (NOT `uv run` — broken via pyproject's missing `eudalia` dist).
- Spark cannot run locally (JDK 17–21 needed; machine has 11 & 25). Spark-touching tests are marked `@pytest.mark.skip` like engorda's integration test; they run on Data Flow. All *pure* logic is TDD'd and runs locally.
- Each Data Flow app is a single uploaded file → `validate_tables.py` is **self-contained**: copy the small helpers it needs (`table_path_name`, decimal-domain math, path/env builders) rather than importing from `engorda_tables.py`.

---

## File Structure

- **Create `scripts/extract_schema.sql`** — dumps `all_tab_columns` (data_type, data_precision, data_scale, char_length, nullable) for owner `CETIP` → `columns.csv`. UNIQUE constraints are NOT re-dumped here — they already appear as `U` rows in the existing `constraints.csv` from `extract_constraints.sql`, which `build_schema_from_dump.py` reuses.
- **Create `scripts/build_schema_from_dump.py`** — pure/testable. Reads `columns.csv` (domains) + `constraints.csv` (U rows) → `schema.json`. Mirrors `build_specs_from_constraints.py`.
- **Create `tests/test_build_schema_from_dump.py`** — pure unit tests (no Spark).
- **Create `validate_tables.py`** — the app: dataclasses + formatters + pure helpers + Spark check functions + `validate()` core + `main()` CLI.
- **Create `tests/test_validate_tables.py`** — pure unit tests (dataclasses, formatters, domain math, manifest normalization); Spark check functions covered by a skipped integration test.

`schema.json` shape:
```json
{
  "TABLE": {
    "columns": {
      "NUM_IF":  {"type": "NUMBER",   "precision": 38, "scale": 0, "nullable": false},
      "COD_X":   {"type": "VARCHAR2", "length": 20, "nullable": true}
    },
    "unique": [["COL_A", "COL_B"]]
  }
}
```

---

## Task 1: `build_schema_from_dump.py` — parse column CSV

**Files:**
- Create: `scripts/build_schema_from_dump.py`
- Test: `tests/test_build_schema_from_dump.py`

- [ ] **Step 1: Write the failing test for `parse_columns_csv`**

```python
# tests/test_build_schema_from_dump.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import build_schema_from_dump as bsd  # noqa: E402


def _write_csv(tmp_path, name, header, rows):
    p = tmp_path / name
    lines = [",".join(header)] + [",".join(str(c) for c in r) for r in rows]
    p.write_text("\n".join(lines) + "\n")
    return str(p)


class TestParseColumnsCsv:
    def test_parses_and_normalizes_header_case(self, tmp_path):
        path = _write_csv(
            tmp_path, "columns.csv",
            ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_PRECISION",
             "DATA_SCALE", "CHAR_LENGTH", "NULLABLE"],
            [["CETIP.JUROS_FLUTUANTE", "NUM_CONDICAO_IF", "NUMBER", 38, 0, 0, "N"],
             ["CETIP.JUROS_FLUTUANTE", "COD_X", "VARCHAR2", "", "", 20, "Y"]],
        )
        rows = bsd.parse_columns_csv(path)
        assert len(rows) == 2
        # NOTE: parse_columns_csv does NOT strip the OWNER. prefix from
        # TABLE_NAME; owner-stripping is deferred to build_schema (Task 2).
        assert rows[0]["COLUMN_NAME"] == "NUM_CONDICAO_IF"
        assert rows[0]["NULLABLE"] == "N"
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py -v`
Expected: FAIL — `module 'build_schema_from_dump' has no attribute 'parse_columns_csv'` (or import error because the file doesn't exist yet).

- [ ] **Step 3: Write `parse_columns_csv` (and the module docstring/imports)**

```python
# scripts/build_schema_from_dump.py
"""Generate schema.json from Oracle column + constraint dumps.

Inputs:
  - columns.csv     from scripts/extract_schema.sql (all_tab_columns)
  - constraints.csv from scripts/extract_constraints.sql (reused; U rows only)

Output: schema.json in the validate_tables format:

    {
      "TABLE": {
        "columns": {
          "COL": {"type": "NUMBER", "precision": 38, "scale": 0, "nullable": false},
          ...
        },
        "unique": [["COL_A", "COL_B"]]
      }
    }

Usage:
    python scripts/build_schema_from_dump.py \
        --columns columns.csv --constraints constraints.csv --out schema.json
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict


def _norm_row(raw: dict) -> dict:
    return {
        (k or "").strip().upper(): (v.strip() if isinstance(v, str) else v)
        for k, v in raw.items()
    }


def parse_columns_csv(path: str) -> list[dict]:
    """Read the all_tab_columns dump, normalizing header case/whitespace."""
    rows: list[dict] = []
    with open(path, newline="") as handle:
        for raw in csv.DictReader(handle):
            row = _norm_row(raw)
            if not row.get("TABLE_NAME") or not row.get("COLUMN_NAME"):
                continue
            rows.append(row)
    return rows
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_schema_from_dump.py tests/test_build_schema_from_dump.py
git commit -m "feat: parse Oracle column dump for schema.json"
```

---

## Task 2: `build_schema` — column domains → schema.json

**Files:**
- Modify: `scripts/build_schema_from_dump.py`
- Test: `tests/test_build_schema_from_dump.py`

- [ ] **Step 1: Write the failing test**

Note the table-name stripping (`CETIP.JUROS_FLUTUANTE` → `JUROS_FLUTUANTE`), int coercion of precision/scale/length (empty string → `None`), and `nullable` mapping (`"N"` → `False`, `"Y"` → `True`).

```python
class TestBuildSchemaColumns:
    def test_columns_typed_and_table_stripped(self):
        col_rows = [
            {"TABLE_NAME": "CETIP.T", "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "38", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
            {"TABLE_NAME": "CETIP.T", "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR2",
             "DATA_PRECISION": "", "DATA_SCALE": "", "CHAR_LENGTH": "20", "NULLABLE": "Y"},
        ]
        schema = bsd.build_schema(col_rows, constraint_rows=[])
        assert set(schema.keys()) == {"T"}
        cols = schema["T"]["columns"]
        assert cols["ID"] == {"type": "NUMBER", "precision": 38, "scale": 0, "nullable": False}
        assert cols["NAME"] == {"type": "VARCHAR2", "length": 20, "nullable": True}
        assert "precision" not in cols["NAME"]  # VARCHAR carries length, not precision
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py::TestBuildSchemaColumns -v`
Expected: FAIL — `build_schema` not defined.

- [ ] **Step 3: Implement `build_schema` (columns half only for now)**

```python
def _table_name(raw: str) -> str:
    return raw.split(".", 1)[1] if "." in raw else raw


def _int_or_none(value) -> int | None:
    try:
        text = str(value).strip()
        return int(float(text)) if text else None
    except (TypeError, ValueError):
        return None


def _column_entry(row: dict) -> dict:
    data_type = (row.get("DATA_TYPE") or "").strip().upper()
    nullable = (row.get("NULLABLE") or "Y").strip().upper() != "N"
    entry: dict = {"type": data_type, "nullable": nullable}
    precision = _int_or_none(row.get("DATA_PRECISION"))
    scale = _int_or_none(row.get("DATA_SCALE"))
    length = _int_or_none(row.get("CHAR_LENGTH"))
    if precision is not None:
        entry["precision"] = precision
        entry["scale"] = scale or 0
    elif length:  # character columns: length, no precision (0/empty -> omit)
        entry["length"] = length
    return entry


def build_schema(column_rows: list[dict], constraint_rows: list[dict]) -> dict:
    """Build schema.json dict from column rows (+ constraint rows for UNIQUE)."""
    schema: dict = {}
    for row in column_rows:
        table = _table_name(row["TABLE_NAME"])
        schema.setdefault(table, {"columns": {}})
        schema[table]["columns"][row["COLUMN_NAME"]] = _column_entry(row)
    # UNIQUE constraints added in Task 3.
    return schema
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py -v`
Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add scripts/build_schema_from_dump.py tests/test_build_schema_from_dump.py
git commit -m "feat: build per-column domains for schema.json"
```

---

## Task 3: UNIQUE constraints (composite, position-paired) + `main()`

**Files:**
- Modify: `scripts/build_schema_from_dump.py`
- Test: `tests/test_build_schema_from_dump.py`

- [ ] **Step 1: Write the failing test for UNIQUE pairing**

Reuses the same constraint-row shape as `build_specs_from_constraints` (`CONSTRAINT_TYPE`, `CONSTRAINT_NAME`, `TABLE_NAME`, `COLUMN_NAME`, `COL_POSITION`). Only `U` rows are consumed; `P`/`R` ignored. Composite uniques are ordered by position; output is sorted for determinism.

```python
class TestBuildSchemaUnique:
    def test_composite_unique_paired_by_position(self):
        col_rows = [
            {"TABLE_NAME": "T", "COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
            {"TABLE_NAME": "T", "COLUMN_NAME": "B", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
        ]
        constraint_rows = [
            {"CONSTRAINT_TYPE": "U", "CONSTRAINT_NAME": "T_UK", "TABLE_NAME": "T",
             "COLUMN_NAME": "B", "COL_POSITION": "2"},
            {"CONSTRAINT_TYPE": "U", "CONSTRAINT_NAME": "T_UK", "TABLE_NAME": "T",
             "COLUMN_NAME": "A", "COL_POSITION": "1"},
            {"CONSTRAINT_TYPE": "P", "CONSTRAINT_NAME": "T_PK", "TABLE_NAME": "T",
             "COLUMN_NAME": "A", "COL_POSITION": "1"},  # ignored
        ]
        schema = bsd.build_schema(col_rows, constraint_rows)
        assert schema["T"]["unique"] == [["A", "B"]]  # ordered by position

    def test_no_unique_key_omits_field(self):
        col_rows = [{"TABLE_NAME": "T", "COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
                     "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0",
                     "NULLABLE": "N"}]
        schema = bsd.build_schema(col_rows, constraint_rows=[])
        assert "unique" not in schema["T"]
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py::TestBuildSchemaUnique -v`
Expected: FAIL — `unique` key missing / `KeyError`.

- [ ] **Step 3: Implement UNIQUE handling in `build_schema`**

Replace the `# UNIQUE constraints added in Task 3.` comment with:

```python
    # UNIQUE constraints: position-ordered column lists, only for known tables.
    uniques: dict[str, list[tuple[int, str]]] = defaultdict(list)
    unique_table: dict[str, str] = {}
    for row in constraint_rows:
        norm = _norm_row(row)
        if (norm.get("CONSTRAINT_TYPE") or "").upper() != "U":
            continue
        name = norm.get("CONSTRAINT_NAME")
        if not name or not norm.get("COLUMN_NAME"):
            continue
        position = _int_or_none(norm.get("COL_POSITION")) or 1
        uniques[name].append((position, norm["COLUMN_NAME"]))
        unique_table[name] = _table_name(norm["TABLE_NAME"])
    per_table: dict[str, list[list[str]]] = defaultdict(list)
    for name, cols in uniques.items():
        table = unique_table[name]
        if table not in schema:
            continue
        per_table[table].append([c for _, c in sorted(cols)])
    for table, keys in per_table.items():
        schema[table]["unique"] = sorted(keys)
```

Also add `parse_constraints_csv` (reuse the normalizer) so `main()` can read `constraints.csv`:

```python
def parse_constraints_csv(path: str) -> list[dict]:
    rows: list[dict] = []
    with open(path, newline="") as handle:
        for raw in csv.DictReader(handle):
            row = _norm_row(raw)
            if not row.get("CONSTRAINT_NAME") or not row.get("COLUMN_NAME"):
                continue
            rows.append(row)
    return rows
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py -v`
Expected: PASS.

- [ ] **Step 5: Add `main()` and the emit-with-sorted-keys path**

```python
def _emit(schema: dict) -> dict:
    out: dict = {}
    for table in sorted(schema):
        entry: dict = {"columns": schema[table]["columns"]}
        if schema[table].get("unique"):
            entry["unique"] = schema[table]["unique"]
        out[table] = entry
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate schema.json from Oracle column + constraint dumps.")
    parser.add_argument("--columns", required=True, help="CSV from extract_schema.sql")
    parser.add_argument("--constraints", required=True,
                        help="CSV from extract_constraints.sql (U rows reused)")
    parser.add_argument("--out", default="schema.json", help="Output schema.json path")
    args = parser.parse_args()

    column_rows = parse_columns_csv(args.columns)
    constraint_rows = parse_constraints_csv(args.constraints)
    schema = _emit(build_schema(column_rows, constraint_rows))
    with open(args.out, "w") as handle:
        json.dump(schema, handle, indent=2, ensure_ascii=False)
    n_unique = sum(1 for v in schema.values() if v.get("unique"))
    print(f"Wrote {args.out}: {len(schema)} tables, {n_unique} with UNIQUE keys.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the full file's tests, verify PASS**

Run: `.venv/bin/python -m pytest tests/test_build_schema_from_dump.py -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/build_schema_from_dump.py tests/test_build_schema_from_dump.py
git commit -m "feat: UNIQUE constraints + main() for schema.json builder"
```

---

## Task 4: `extract_schema.sql`

**Files:**
- Create: `scripts/extract_schema.sql`

No automated test (SQL). Mirror the header/export instructions of `extract_constraints.sql`.

- [ ] **Step 1: Write the SQL**

```sql
-- Dump per-column metadata (type + domain + nullability) for a schema, one row
-- per column. Export to columns.csv (header row included), then feed it +
-- the constraints.csv from extract_constraints.sql to
-- scripts/build_schema_from_dump.py to (re)generate schema.json.
--
-- SQL Developer: run, then right-click the grid -> Export -> CSV (include header).
-- sqlplus (Oracle 12.2+):
--   SET PAGESIZE 0 FEEDBACK OFF
--   SET MARKUP CSV ON QUOTE OFF
--   SPOOL columns.csv
--   @scripts/extract_schema.sql
--   SPOOL OFF
--
-- Change the owner below to your target schema.

SELECT tc.table_name     AS table_name,
       tc.column_name    AS column_name,
       tc.data_type      AS data_type,
       tc.data_precision AS data_precision,  -- NUMBER precision (null if unconstrained)
       tc.data_scale     AS data_scale,      -- NUMBER scale
       tc.char_length    AS char_length,     -- CHAR/VARCHAR length in chars
       tc.nullable       AS nullable         -- 'Y' nullable, 'N' NOT NULL
FROM   all_tab_columns tc
WHERE  tc.owner = 'CETIP'
ORDER  BY tc.table_name, tc.column_id;
```

- [ ] **Step 2: Commit**

```bash
git add scripts/extract_schema.sql
git commit -m "feat: SQL dump of column metadata for schema.json"
```

---

## Task 5: `validate_tables.py` — Report/Finding dataclasses + formatters

**Files:**
- Create: `validate_tables.py`
- Test: `tests/test_validate_tables.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_validate_tables.py
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
```

- [ ] **Step 2: Run, verify FAIL**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: FAIL — module/attributes not defined.

- [ ] **Step 3: Implement the dataclasses + formatters (top of `validate_tables.py`)**

```python
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
        mark = "OK " if f.ok else "FAIL"
        lines.append(f"  [{mark}] {f.table}.{f.check}({f.target}) "
                     f"-> {f.violation_count} bad")
    return "\n".join(lines)
```

- [ ] **Step 4: Run, verify PASS**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add validate_tables.py tests/test_validate_tables.py
git commit -m "feat: Report/Finding dataclasses + formatters for validator"
```

---

## Task 6: Pure helpers — table-name normalization + decimal domain math

**Files:**
- Modify: `validate_tables.py`
- Test: `tests/test_validate_tables.py`

- [ ] **Step 1: Write the failing test**

`decimal_max_abs` mirrors engorda's `_pk_capacity` integer-digit math; `normalize_schema`/`normalize_specs` strip `OWNER.` from table keys (and FK `parent_table`).

```python
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
```

- [ ] **Step 2: Run, verify FAIL**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py::TestPureHelpers -v`
Expected: FAIL — helpers not defined.

- [ ] **Step 3: Implement (copied/adapted from engorda — kept self-contained)**

```python
import copy


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
```

- [ ] **Step 4: Run, verify PASS**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add validate_tables.py tests/test_validate_tables.py
git commit -m "feat: domain math + manifest normalization for validator"
```

---

## Task 7: Spark check functions

**Files:**
- Modify: `validate_tables.py`
- Test: `tests/test_validate_tables.py` (skipped integration test documenting expected behavior)

Each check is a small function over DataFrame(s) returning `Finding`(s). They run on Data Flow; locally Spark is unavailable, so the test is a single `@pytest.mark.skip` integration test that documents intent (mirrors engorda's `TestEngordaIntegration`).

- [ ] **Step 1: Write the skipped integration test**

```python
import pytest


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
```

- [ ] **Step 2: Implement the check functions**

Use `pyspark.sql.functions as F` and `pyspark.sql.types as T` imported lazily inside functions (so the module imports without Spark for the pure tests). Each captures up to `SAMPLE_LIMIT` offending rows.

```python
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
```

- [ ] **Step 3: Run the (pure) tests — confirm nothing broke and the module still imports without Spark**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: PASS for pure tests, `TestChecksIntegration` SKIPPED.

- [ ] **Step 4: Commit**

```bash
git add validate_tables.py tests/test_validate_tables.py
git commit -m "feat: column-pruned Spark check functions for validator"
```

---

## Task 8: `validate()` orchestrator + path/IO helpers

**Files:**
- Modify: `validate_tables.py`
- Test: `tests/test_validate_tables.py`

The orchestrator wires manifests + parquet into the checks. Reading parquet needs Spark, so the orchestrator itself is exercised by the skipped integration test; but the **table-selection / parent-universe planning** is pure and TDD'd via a thin `plan_checks` helper.

- [ ] **Step 1: Write the failing test for `plan_checks`**

`plan_checks` decides, per table, which checks to run and (for FK) which parent tables form the universe — independent of Spark. It also honors the `tables` subset.

```python
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
```

- [ ] **Step 2: Run, verify FAIL**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py::TestPlanChecks -v`
Expected: FAIL — `plan_checks` not defined.

- [ ] **Step 3: Implement `plan_checks`, path helpers, and `validate()`**

```python
def plan_checks(specs: dict, schema: dict, tables=None) -> list:
    """Per-table check plan (pure). FK universe = parent table key columns."""
    chosen = set(tables) if tables else set(schema)
    plan = []
    for table in sorted(t for t in schema if t in chosen):
        cols = schema[table].get("columns", {})
        not_null = [c for c, meta in cols.items() if not meta.get("nullable", True)]
        decimals = [(c, m["precision"], m.get("scale", 0))
                    for c, m in cols.items() if "precision" in m]
        varchars = [(c, m["length"]) for c, m in cols.items() if "length" in m]
        spec = specs.get(table, {})
        plan.append({
            "table": table,
            "pk_cols": spec.get("pk_cols") or [],
            "not_null": sorted(not_null),
            "decimals": decimals,
            "varchars": varchars,
            "unique": schema[table].get("unique", []),
            "fks": [fk for fk in (spec.get("foreign_keys") or []) if isinstance(fk, dict)],
        })
    return plan


def _raw_path(raw_base: str, table: str) -> str:
    return f"{raw_base}/{table}"


def _synth_path(synth_base: str, table: str) -> str:
    return f"{synth_base}/{table}"


def _read_parquet_opt(spark, path):
    """Read a parquet path; return None if it doesn't exist (e.g. static parent
    not in synthetic output)."""
    try:
        return spark.read.parquet(path)
    except Exception:  # AnalysisException: path does not exist
        return None


def validate(spark, specs, schema, raw_base, synth_base, tables=None) -> Report:
    specs = normalize_specs(specs)
    schema = normalize_schema(schema)
    plan = plan_checks(specs, schema, tables)
    findings = []
    for item in plan:
        table = item["table"]
        synth = _read_parquet_opt(spark, _synth_path(synth_base, table))
        if synth is None:
            logger.warning("No synthetic parquet for %s; skipping", table)
            continue
        raw = _read_parquet_opt(spark, _raw_path(raw_base, table))
        # column/domain checks
        present = set(synth.columns)
        findings += check_not_null(synth, table,
                                   [c for c in item["not_null"] if c in present])
        for col, p, s in item["decimals"]:
            if col in present:
                findings.append(check_decimal_domain(synth, table, col, p, s))
        for col, length in item["varchars"]:
            if col in present:
                findings.append(check_varchar_domain(synth, table, col, length))
        # pk
        if item["pk_cols"] and all(c in present for c in item["pk_cols"]):
            findings += check_pk(synth, raw, table, item["pk_cols"])
        # unique
        for cols in item["unique"]:
            if all(c in present for c in cols):
                findings.append(check_unique(synth, table, cols))
        # fk: universe = raw(parent) union synthetic(parent)
        for fk in item["fks"]:
            parent = fk.get("parent_table")
            child_cols, parent_cols = fk.get("columns"), fk.get("parent_columns")
            if not (parent and child_cols and parent_cols):
                continue
            if not all(c in present for c in child_cols):
                continue
            universe = _parent_universe(spark, raw_base, synth_base, parent, parent_cols)
            if universe is None:
                logger.warning("No parent data for %s.%s -> %s; skipping FK",
                               table, child_cols, parent)
                continue
            label = f"{','.join(child_cols)}->{parent}"
            findings.append(check_fk(synth, universe, table,
                                     child_cols, parent_cols, label))
    return Report(findings=findings)


def _parent_universe(spark, raw_base, synth_base, parent, parent_cols):
    """Union of parent key columns across raw (existing) and synthetic (incoming)."""
    frames = []
    for base in (_raw_path(raw_base, parent), _synth_path(synth_base, parent)):
        df = _read_parquet_opt(spark, base)
        if df is not None and all(c in df.columns for c in parent_cols):
            frames.append(df.select(*parent_cols))
    if not frames:
        return None
    universe = frames[0]
    for extra in frames[1:]:
        universe = universe.unionByName(extra)
    return universe
```

- [ ] **Step 4: Run, verify PASS (pure tests) + integration SKIPPED**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: PASS for pure, SKIP for integration.

- [ ] **Step 5: Commit**

```bash
git add validate_tables.py tests/test_validate_tables.py
git commit -m "feat: validate() orchestrator + check planning for validator"
```

---

## Task 9: `main()` CLI — env, manifest IO, report write, exit code

**Files:**
- Modify: `validate_tables.py`
- Test: `tests/test_validate_tables.py`

- [ ] **Step 1: Write the failing test for env + arg helpers**

```python
class TestEnvAndArgs:
    def test_get_env_collects_required(self, monkeypatch):
        for name in vt.REQUIRED_ENV_VARS:
            monkeypatch.setenv(name, f"oci://bucket@ns/{name}/")
        cfg = vt.get_validate_env()
        assert cfg["DATAGEN_SCHEMA_URI"] == "oci://bucket@ns/DATAGEN_SCHEMA_URI"  # rstripped

    def test_get_env_exits_when_missing(self, monkeypatch):
        for name in vt.REQUIRED_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(SystemExit):
            vt.get_validate_env()

    def test_parse_arguments_defaults(self):
        args = vt.parse_arguments([])
        assert args.tables is None
        assert args.specs is None
```

- [ ] **Step 2: Run, verify FAIL**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py::TestEnvAndArgs -v`
Expected: FAIL — helpers not defined.

- [ ] **Step 3: Implement env/args/manifest-IO/main**

```python
def get_validate_env() -> dict:
    config = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Missing required env var(s): %s", ", ".join(missing))
        sys.exit(1)
    return config


def parse_arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate synthetic Parquet vs DB constraints.")
    parser.add_argument("--specs", default=None, help="Override DATAGEN_SPECS_URI.")
    parser.add_argument("--schema", default=None, help="Override DATAGEN_SCHEMA_URI.")
    parser.add_argument("--report-uri", default=None,
                        help="Where to write the JSON report (object storage).")
    parser.add_argument("--tables", default=None,
                        help="Comma-separated subset of tables to validate.")
    return parser.parse_args(argv)


def _read_json_object(spark, uri: str) -> dict:
    records = spark.sparkContext.wholeTextFiles(uri).collect()
    if len(records) != 1:
        raise ValueError(f"Expected exactly one JSON object at `{uri}`, found {len(records)}.")
    return json.loads(records[0][1])


def load_manifests(spark, specs_uri, schema_uri):
    specs = specs_uri if isinstance(specs_uri, dict) else _read_json_object(spark, specs_uri)
    schema = schema_uri if isinstance(schema_uri, dict) else _read_json_object(spark, schema_uri)
    return normalize_specs(specs), normalize_schema(schema)


def _write_report(spark, report: Report, uri: str) -> None:
    blob = json.dumps(report_to_json(report), indent=2, ensure_ascii=False)
    # one-object write via Hadoop FS (small file)
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop.fs
    conf = sc._jsc.hadoopConfiguration()
    jpath = sc._jvm.org.apache.hadoop.fs.Path(uri)
    fs = jpath.getFileSystem(conf)
    out = fs.create(jpath, True)
    try:
        out.write(bytearray(blob, "utf-8"))
    finally:
        out.close()


def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.sql.parquet.aggregatePushdown", "true")
    return builder.getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    config = get_validate_env()
    specs_uri = args.specs or config["DATAGEN_SPECS_URI"]
    schema_uri = args.schema or config["DATAGEN_SCHEMA_URI"]
    tables = [t.strip() for t in args.tables.split(",")] if args.tables else None

    spark = create_spark_session("validate_tables")
    specs, schema = load_manifests(spark, specs_uri, schema_uri)
    report = validate(spark, specs, schema,
                      config["DATAGEN_RAW_BASE_URI"],
                      config["DATAGEN_SYNTHETIC_BASE_URI"], tables=tables)
    summary = render_summary(report)
    print(summary)
    logger.info("%s", summary)
    if args.report_uri:
        _write_report(spark, report, args.report_uri)
        logger.info("Report written to %s", args.report_uri)
    sys.exit(1 if report.has_violations else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run, verify PASS**

Run: `.venv/bin/python -m pytest tests/test_validate_tables.py -v`
Expected: PASS (pure), SKIP (integration).

- [ ] **Step 5: Run the full suite + lint**

Run: `.venv/bin/python -m pytest tests/ -q`
Run: `.venv/bin/python -m ruff check validate_tables.py scripts/build_schema_from_dump.py` (or the repo's configured linter — match what engorda used; wrap any E501 long help strings).
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add validate_tables.py tests/test_validate_tables.py
git commit -m "feat: main() CLI + manifest/report IO for validate_tables"
```

---

## Task 10: Docs — README/usage note + Data Flow config

**Files:**
- Modify: the repo doc that lists the Data Flow apps (e.g. `README.md` or the engorda deployment doc), if present.

- [ ] **Step 1: Document the new app**

Add a short section: the `validate_tables.py` app, its env vars (incl. new `DATAGEN_SCHEMA_URI`), CLI flags (`--tables`, `--report-uri`), how to (re)generate `schema.json` (`extract_schema.sql` + `extract_constraints.sql` → `build_schema_from_dump.py`), and the notebook usage snippet from the design doc. Note the non-zero exit = pre-load gate.

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "docs: document validate_tables app + schema.json regeneration"
```

---

## Definition of Done

- [ ] `.venv/bin/python -m pytest tests/ -q` green (new pure tests pass; Spark integration tests skipped).
- [ ] `build_schema_from_dump.py` produces a `schema.json` from real `columns.csv` + `constraints.csv` (smoke-run once with real dumps if available).
- [ ] `validate_tables.py` imports without Spark installed (pure tests prove it).
- [ ] Five checks implemented against the raw ∪ synthetic universe; report writes JSON + prints summary; exits non-zero on violations.
- [ ] Notebook entrypoint (`validate`, `load_manifests`, `render_summary`) usable with a caller-supplied SparkSession.
- [ ] Docs updated.
- [ ] Manual Data Flow run validated against a real engorda output (out-of-band; local Spark gap).
