# Shift Synthetic Keys — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `datagen/shift_keys.py`, a standalone OCI Data Flow app that adds a uniform `+N` to every generated (non-static) PK/FK value in the synthetic Parquet output, in place, preserving FK integrity.

**Architecture:** Two phases over the synthetic tables. Phase 1 (read-only) pre-flight: for each shiftable column, read `max(col)` (footer-fast via `aggregatePushdown`) and abort if `max+N` would exceed the column's numeric domain (`_pk_capacity`). Phase 2 (mutate, per table): read → `(col+N).cast(dtype)` on each shiftable column → `localCheckpoint(eager)` to sever lineage from the source files → `write_synthetic_table` (scoped-delete the table's own prefix + `append`) back to the same path. The shift-column set is derived from `specs.json` (no joins, no shuffle — pure parallel map).

**Tech Stack:** Python, PySpark 4.1 (needs Java 17–21), OCI HDFS connector. Reuses helpers from `datagen/engorda_tables.py`: `write_synthetic_table`, `_pk_capacity`, `read_parquet`, `create_spark_session`, `synthetic_base_path`, `table_path_name`, `load_specs`.

**Spec:** `docs/plans/2026-06-28-shift-synthetic-keys-design.md`

---

## File Structure

- **Create:** `datagen/shift_keys.py` — the app. Responsibilities, top to bottom:
  - `compute_shift_columns(specs) -> dict[str, list[str]]` — pure: which columns shift, per table.
  - `shift_table(df, cols, offset) -> DataFrame` — pure transform: `(col+N).cast(dtype)`.
  - `capacity_from_precision_scale(precision, scale) -> int | None` — pure: `NUMBER(p,s) -> 10^(p-s)-1`.
  - `oracle_column_capacities(rows, shift) -> dict[(table,col), int]` — pure: from `ALL_TAB_COLUMNS` rows.
  - `find_collisions(prod_max, synth_min, offset) -> list[tuple]` — pure: `synth_min+N <= prod_max` flagged.
  - `read_oracle_capacities(spark, props, owner) -> rows` / `read_oracle_max(spark, props, owner, table, col)` — thin JDBC wrappers reusing `save_tables` helpers.
  - `check_overflow(spark, base, shift, offset, capacity_override=None) -> list[tuple]` — read-only; `capacity_override` (from Oracle) wins over `_pk_capacity`.
  - `apply_shift(spark, base, shift, offset, continue_on_error, reliable_checkpoint) -> list[str]` — Phase 2.
  - `get_shift_env() -> dict` — env validation (SYNTHETIC + SPECS required; CHECKPOINT + Oracle JDBC + owner optional).
  - `print_deployment_summary(config)` — env vars + Data Flow config block.
  - `parse_arguments()`, `main()`.
- **Create:** `tests/test_shift_keys.py` — unit (`compute_shift_columns`) + integration (transform, overflow, end-to-end).

## Test command

PySpark 4.1 needs Java 17–21. Run tests with:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  .venv/bin/python -m pytest tests/test_shift_keys.py -v
```

(If `.venv` is absent, use the project's configured interpreter with pyspark + a JDK 17–21 `JAVA_HOME`.)

---

### Task 1: `compute_shift_columns` — the shift rule (pure, unit-tested)

**Files:**
- Create: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_shift_keys.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import shift_keys  # noqa: E402


class TestComputeShiftColumns:
    def test_nonstatic_pk_shifts(self):
        specs = {"OPERACAO": {"pk_cols": ["NUM_OPER"]}}
        assert shift_keys.compute_shift_columns(specs) == {"OPERACAO": ["NUM_OPER"]}

    def test_static_pk_not_shifted(self):
        specs = {"TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True}}
        assert shift_keys.compute_shift_columns(specs) == {}

    def test_fk_to_nonstatic_parent_shifts(self):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_IF"],
                                           "parent_table": "INSTRUMENTO_FINANCEIRO"}]},
        }
        out = shift_keys.compute_shift_columns(specs)
        assert sorted(out["OPERACAO"]) == ["NUM_IF", "NUM_OPER"]
        assert out["INSTRUMENTO_FINANCEIRO"] == ["NUM_IF"]

    def test_fk_to_static_parent_not_shifted(self):
        specs = {
            "TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_TIPO_IF"],
                                           "parent_table": "TIPO_IF"}]},
        }
        # NUM_TIPO_IF references a static parent -> not shifted; only NUM_OPER shifts
        assert shift_keys.compute_shift_columns(specs) == {"OPERACAO": ["NUM_OPER"]}

    def test_shared_key_child_of_static_parent_pk_not_shifted(self):
        # PK == FK to a static parent: FK-to-static wins, PK kept matched
        specs = {
            "CODE": {"pk_cols": ["COD"], "static": True},
            "EXT": {"pk_cols": ["COD"],
                    "foreign_keys": [{"columns": ["COD"], "parent_table": "CODE"}]},
        }
        assert shift_keys.compute_shift_columns(specs) == {}

    def test_shared_key_child_of_nonstatic_parent_shifts_once(self):
        # PK == FK to a non-static parent: shifts (deduped to one column)
        specs = {
            "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
            "RESGATE": {"pk_cols": ["NUM_CONDICAO_IF"],
                        "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                          "parent_table": "CONDICAO_IF"}]},
        }
        out = shift_keys.compute_shift_columns(specs)
        assert out["RESGATE"] == ["NUM_CONDICAO_IF"]

    def test_real_specs_yields_31_columns(self):
        import json
        specs = json.load(open(Path(__file__).resolve().parent.parent / "specs.json"))
        out = shift_keys.compute_shift_columns(specs)
        total = sum(len(v) for v in out.values())
        assert total == 31
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_shift_keys.py::TestComputeShiftColumns -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'datagen.shift_keys'`.

- [ ] **Step 3: Write the module + minimal implementation**

```python
# datagen/shift_keys.py
"""Post-hoc uniform shift of generated PK/FK values in the synthetic output.

Adds a uniform +N to every generated (non-static) key, in place, preserving FK
integrity. See docs/plans/2026-06-28-shift-synthetic-keys-design.md.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import warnings
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datagen.engorda_tables import (
    _pk_capacity,
    create_spark_session,
    load_specs,
    read_parquet,
    synthetic_base_path,
    table_path_name,
    write_synthetic_table,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def compute_shift_columns(specs: dict) -> Dict[str, List[str]]:
    """Per-table list of key columns to shift by +N.

    A column (table, col) shifts iff it's an FK column whose parent is non-static,
    OR it's the PK of a non-static table and not an FK to a static parent
    (FK-to-static wins, keeping shared-key children matched to reference data).
    """
    static = {t for t, e in specs.items() if e.get("static")}

    fk_to_static = set()  # (table, col)
    for t, e in specs.items():
        for fk in e.get("foreign_keys", []) or []:
            if fk.get("parent_table") in static:
                for c in fk.get("columns", []) or []:
                    fk_to_static.add((t, c))

    shift: Dict[str, set] = {}
    for t, e in specs.items():
        cols: set = set()
        for fk in e.get("foreign_keys", []) or []:
            parent = fk.get("parent_table")
            if parent in specs and parent not in static:
                for c in fk.get("columns", []) or []:
                    cols.add(c)
        if t not in static:
            for pk in e.get("pk_cols", []) or []:
                if (t, pk) in fk_to_static:
                    warnings.warn(
                        f"{t}.{pk}: non-static PK that is also an FK to a static "
                        "parent; NOT shifting (kept matched to reference data).",
                        UserWarning, stacklevel=2,
                    )
                else:
                    cols.add(pk)
        if cols:
            shift[t] = sorted(cols)
    return shift
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_shift_keys.py::TestComputeShiftColumns -v`
Expected: PASS (7 tests). The `test_real_specs_yields_31_columns` confirms the rule against the live `specs.json`.

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): compute shift-column set from specs"
```

---

### Task 2: `shift_table` — the `(col+N).cast(dtype)` transform

**Files:**
- Modify: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

- [ ] **Step 1: Add the Spark fixture and failing tests**

```python
# add near the top of tests/test_shift_keys.py
import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.appName("shift-keys-test")
               .master("local[2]").config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    yield session
    session.stop()


class TestShiftTable:
    def test_shifts_listed_columns_and_preserves_others(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("NUM_OPER", T.LongType()),
            T.StructField("NUM_IF", T.LongType()),
            T.StructField("DESC", T.StringType()),
        ])
        df = spark.createDataFrame([(1, 10, "a"), (2, 20, "b")], schema)
        out = shift_keys.shift_table(df, ["NUM_OPER", "NUM_IF"], 1000)
        rows = {r["DESC"]: (r["NUM_OPER"], r["NUM_IF"]) for r in out.collect()}
        assert rows == {"a": (1001, 1010), "b": (1002, 1020)}

    def test_preserves_dtype(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 9))])
        df = spark.createDataFrame([(1,)], schema)
        out = shift_keys.shift_table(df, ["K"], 5)
        assert out.schema["K"].dataType == T.DecimalType(38, 9)
        assert int(out.collect()[0]["K"]) == 6

    def test_null_fk_stays_null(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("FK", T.LongType())])
        df = spark.createDataFrame([(5,), (None,)], schema)
        out = shift_keys.shift_table(df, ["FK"], 100)
        vals = sorted([r["FK"] for r in out.collect()], key=lambda x: (x is None, x))
        assert vals == [105, None]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_shift_keys.py::TestShiftTable -v`
Expected: FAIL — `AttributeError: module 'datagen.shift_keys' has no attribute 'shift_table'`.

- [ ] **Step 3: Implement `shift_table`**

```python
# datagen/shift_keys.py
def shift_table(df: DataFrame, cols: List[str], offset: int) -> DataFrame:
    """Add `offset` to each column in `cols`, preserving its dtype (so the output
    schema is byte-identical). NULLs stay NULL."""
    dtypes = {f.name: f.dataType for f in df.schema.fields}
    for c in cols:
        df = df.withColumn(c, (F.col(c) + F.lit(offset)).cast(dtypes[c]))
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_shift_keys.py::TestShiftTable -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): add dtype-preserving shift_table transform"
```

---

### Task 3: `check_overflow` — read-only pre-flight

**Files:**
- Modify: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

- [ ] **Step 1: Write failing tests (writes small parquet, reads max + capacity)**

```python
class TestCheckOverflow:
    def _write(self, spark, tmp_path, name, schema, rows):
        df = spark.createDataFrame(rows, schema)
        df.write.parquet(str(tmp_path / name))

    def test_no_overflow_returns_empty(self, spark, tmp_path):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 0))])
        self._write(spark, tmp_path, "T", schema, [(10,), (20,)])
        shift = {"T": ["K"]}
        assert shift_keys.check_overflow(spark, str(tmp_path), shift, 1000) == []

    def test_overflow_detected_for_tight_domain(self, spark, tmp_path):
        from pyspark.sql import types as T
        # Decimal(2,0) capacity = 99; max is 90, +20 = 110 > 99 -> overflow
        schema = T.StructType([T.StructField("K", T.DecimalType(2, 0))])
        self._write(spark, tmp_path, "T", schema, [(90,)])
        shift = {"T": ["K"]}
        out = shift_keys.check_overflow(spark, str(tmp_path), shift, 20)
        assert len(out) == 1
        table, col, mx, shifted, cap = out[0]
        assert (table, col, mx, shifted, cap) == ("T", "K", 90, 110, 99)

    def test_capacity_override_wins_over_parquet(self, spark, tmp_path):
        from pyspark.sql import types as T
        # Parquet dtype Decimal(38,0) is huge, but the live Oracle capacity is 200;
        # max 150 + 100 = 250 > 200 -> overflow detected only via the override.
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 0))])
        self._write(spark, tmp_path, "T", schema, [(150,)])
        out = shift_keys.check_overflow(spark, str(tmp_path), {"T": ["K"]}, 100,
                                        capacity_override={("T", "K"): 200})
        assert out == [("T", "K", 150, 250, 200)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_shift_keys.py::TestCheckOverflow -v`
Expected: FAIL — no `check_overflow`.

- [ ] **Step 3: Implement `check_overflow`**

```python
# datagen/shift_keys.py
def check_overflow(
    spark: SparkSession,
    base: str,
    shift: Dict[str, List[str]],
    offset: int,
    capacity_override: Dict[Tuple[str, str], int] | None = None,
) -> List[Tuple[str, str, int, int, int]]:
    """Read-only. For each shiftable column, read max(col) (footer-fast via
    aggregatePushdown) and flag (table, col, max, max+offset, capacity) when
    max+offset exceeds the column's numeric domain. `capacity_override` (the
    authoritative live Oracle capacity) wins over the Parquet-schema
    `_pk_capacity`. Non-numeric / empty columns are skipped."""
    capacity_override = capacity_override or {}
    overflows: List[Tuple[str, str, int, int, int]] = []
    for table, cols in shift.items():
        path = f"{base}/{table_path_name(table)}"
        df = read_parquet(spark, path)
        for c in cols:
            cap = capacity_override.get((table, c))
            if cap is None:
                cap = _pk_capacity(spark, path, c)
            if cap is None:
                continue
            row = df.agg(F.max(F.col(c)).alias("m")).first()
            if row is None or row["m"] is None:
                continue
            mx = int(row["m"])
            if mx + offset > cap:
                overflows.append((table, c, mx, mx + offset, cap))
    return overflows
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_shift_keys.py::TestCheckOverflow -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): add read-only overflow pre-flight"
```

---

### Task 4: Oracle live pre-flight — authoritative capacity + collision (pure logic)

**Files:**
- Modify: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

The pure functions are fully unit-tested; the two thin JDBC wrappers
(`read_oracle_capacities`, `read_oracle_max`) just run a query via the proven
`save_tables.read_rows`/`read_single_value` and are not exercised locally (no
Oracle in the test env).

- [ ] **Step 1: Write failing unit tests for the pure functions**

```python
class TestOraclePreflight:
    def test_capacity_from_precision_scale(self):
        assert shift_keys.capacity_from_precision_scale(2, 0) == 99
        assert shift_keys.capacity_from_precision_scale(5, 0) == 99999
        assert shift_keys.capacity_from_precision_scale(10, 2) == 10**8 - 1
        # NULL precision (unconstrained NUMBER) -> no limit
        assert shift_keys.capacity_from_precision_scale(None, None) is None

    def test_oracle_column_capacities_filters_to_shift_set(self):
        # rows mimic ALL_TAB_COLUMNS: (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE)
        rows = [
            ("OPERACAO", "NUM_OPER", 12, 0),
            ("OPERACAO", "IGNORED", 5, 0),      # not in shift set -> dropped
            ("INSTRUMENTO_FINANCEIRO", "NUM_IF", None, None),  # unconstrained -> skipped
        ]
        shift = {"OPERACAO": ["NUM_OPER"], "INSTRUMENTO_FINANCEIRO": ["NUM_IF"]}
        out = shift_keys.oracle_column_capacities(rows, shift)
        assert out == {("OPERACAO", "NUM_OPER"): 10**12 - 1}

    def test_find_collisions(self):
        prod_max = {"OPERACAO": 5000, "CONDICAO_IF": 100}
        synth_min = {"OPERACAO": 1, "CONDICAO_IF": 1}
        # offset 4000: OPERACAO 1+4000=4001 <= 5000 -> collision; CONDICAO_IF 4001 > 100 -> ok
        out = shift_keys.find_collisions(prod_max, synth_min, 4000)
        assert out == [("OPERACAO", 5000, 4001)]

    def test_find_collisions_none_when_offset_clears(self):
        assert shift_keys.find_collisions({"T": 100}, {"T": 1}, 1000) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_shift_keys.py::TestOraclePreflight -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement the pure functions + thin JDBC wrappers**

```python
# datagen/shift_keys.py  (add this import near the top)
from datagen.save_tables import build_connection_properties, read_rows, read_single_value


def capacity_from_precision_scale(precision, scale):
    """Largest integer value for an Oracle NUMBER(precision, scale).
    NULL precision (unconstrained NUMBER) -> None (no limit)."""
    if precision is None:
        return None
    int_digits = int(precision) - int(scale or 0)
    return (10 ** int_digits) - 1 if int_digits > 0 else 0


def oracle_column_capacities(rows, shift: Dict[str, List[str]]) -> Dict[Tuple[str, str], int]:
    """Map (table, col) -> capacity from ALL_TAB_COLUMNS rows
    (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE), restricted to the
    shift set; columns with no precision (unconstrained) are omitted."""
    wanted = {(t, c) for t, cols in shift.items() for c in cols}
    out: Dict[Tuple[str, str], int] = {}
    for table, col, precision, scale in rows:
        if (table, col) not in wanted:
            continue
        cap = capacity_from_precision_scale(precision, scale)
        if cap is not None:
            out[(table, col)] = cap
    return out


def find_collisions(prod_max: Dict[str, int], synth_min: Dict[str, int],
                    offset: int) -> List[Tuple[str, int, int]]:
    """Flag (table, prod_max, synth_min+offset) where the shifted synthetic key
    range does NOT clear current production (synth_min + offset <= prod_max)."""
    flagged: List[Tuple[str, int, int]] = []
    for table, pmax in prod_max.items():
        smin = synth_min.get(table)
        if smin is None or pmax is None:
            continue
        if smin + offset <= pmax:
            flagged.append((table, int(pmax), int(smin) + offset))
    return flagged


def read_oracle_capacities(spark, props: dict, owner: str):
    """Thin: ALL_TAB_COLUMNS rows for `owner`. Returns list of
    (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE)."""
    query = (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE "
        f"FROM ALL_TAB_COLUMNS WHERE OWNER = '{owner}'"
    )
    return [(r["TABLE_NAME"], r["COLUMN_NAME"], r["DATA_PRECISION"], r["DATA_SCALE"])
            for r in read_rows(spark, props, query)]


def read_oracle_max(spark, props: dict, owner: str, table: str, col: str):
    """Thin: live MAX(col) from owner.table (index-fast). None if empty."""
    row = read_single_value(spark, props, f"SELECT MAX({col}) AS M FROM {owner}.{table}")
    return None if row is None or row["M"] is None else int(row["M"])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_shift_keys.py::TestOraclePreflight -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): Oracle capacity + collision pre-flight logic"
```

---

### Task 5: `apply_shift` — Phase 2 in-place mutate (end-to-end)

**Files:**
- Modify: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

- [ ] **Step 1: Write the failing end-to-end test (parent + child + shared-key)**

```python
class TestApplyShift:
    def test_in_place_shift_preserves_fk_integrity(self, spark, tmp_path):
        from pyspark.sql import types as T
        base = str(tmp_path / "syn")
        # CONDICAO_IF (non-static parent), RESGATE (shared-key child),
        # OPERACAO (child with FK to CONDICAO_IF), TIPO_IF (static)
        spark.createDataFrame([(1,), (2,), (3,)],
            T.StructType([T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/CONDICAO_IF")
        spark.createDataFrame([(1,), (2,)],
            T.StructType([T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/RESGATE")
        spark.createDataFrame([(10, 1), (11, 2)],
            T.StructType([T.StructField("NUM_OPER", T.LongType()),
                          T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/OPERACAO")
        spark.createDataFrame([(46,)],
            T.StructType([T.StructField("NUM_TIPO_IF", T.LongType())])
        ).write.parquet(f"{base}/TIPO_IF")

        specs = {
            "TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True},
            "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
            "RESGATE": {"pk_cols": ["NUM_CONDICAO_IF"],
                        "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                          "parent_table": "CONDICAO_IF"}]},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                           "parent_table": "CONDICAO_IF"}]},
        }
        shift = shift_keys.compute_shift_columns(specs)
        failures = shift_keys.apply_shift(spark, base, shift, 1000,
                                          continue_on_error=False,
                                          reliable_checkpoint=False)
        assert failures == []

        cond = spark.read.parquet(f"{base}/CONDICAO_IF")
        oper = spark.read.parquet(f"{base}/OPERACAO")
        resg = spark.read.parquet(f"{base}/RESGATE")
        tipo = spark.read.parquet(f"{base}/TIPO_IF")

        # parent PK shifted
        assert sorted(r["NUM_CONDICAO_IF"] for r in cond.collect()) == [1001, 1002, 1003]
        # child FK shifted by same N -> still joins parent
        assert oper.join(cond, "NUM_CONDICAO_IF", "left_anti").count() == 0
        assert sorted(r["NUM_OPER"] for r in oper.collect()) == [1010, 1011]
        # shared-key child shifted, still matches parent
        assert resg.join(cond, "NUM_CONDICAO_IF", "left_anti").count() == 0
        # static table untouched
        assert [r["NUM_TIPO_IF"] for r in tipo.collect()] == [46]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `… -m pytest tests/test_shift_keys.py::TestApplyShift -v`
Expected: FAIL — no `apply_shift`.

- [ ] **Step 3: Implement `apply_shift`**

```python
# datagen/shift_keys.py
def apply_shift(
    spark: SparkSession,
    base: str,
    shift: Dict[str, List[str]],
    offset: int,
    *,
    continue_on_error: bool,
    reliable_checkpoint: bool,
) -> List[str]:
    """Phase 2: mutate each table in place. Per table: read -> shift -> checkpoint
    (sever lineage from the source files) -> scoped-delete + append to the same
    path. Returns the list of tables that failed."""
    tables = sorted(shift)
    total = len(tables)
    failures: List[str] = []
    for i, table in enumerate(tables, 1):
        path = f"{base}/{table_path_name(table)}"
        try:
            df = shift_table(read_parquet(spark, path), shift[table], offset)
            # Sever lineage: the next step deletes `path`, so a lazy read of the
            # source files would corrupt the output. Checkpoint replaces the plan
            # with a materialized RDD leaf.
            df = df.checkpoint(eager=True) if reliable_checkpoint else df.localCheckpoint(eager=True)
            write_synthetic_table(spark, df, path)
            logger.info("[%d/%d] shifted %s (%s)", i, total, table, ",".join(shift[table]))
        except Exception as exc:  # noqa: BLE001
            logger.exception("[%d/%d] FAILED shifting %s: %s", i, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise
    return failures
```

- [ ] **Step 4: Run test to verify it passes**

Run: `… -m pytest tests/test_shift_keys.py::TestApplyShift -v`
Expected: PASS — FK integrity holds after the in-place shift; static table untouched.

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): add in-place per-table apply_shift"
```

---

### Task 6: Env loading, CLI, deployment summary, `main` (with Oracle wiring)

**Files:**
- Modify: `datagen/shift_keys.py`
- Test: `tests/test_shift_keys.py`

- [ ] **Step 1: Write failing tests for env + arg parsing + summary**

```python
class TestEnvAndCli:
    def test_get_shift_env_requires_synthetic_and_specs(self, monkeypatch):
        monkeypatch.delenv("DATAGEN_SYNTHETIC_BASE_URI", raising=False)
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        with pytest.raises(SystemExit):
            shift_keys.get_shift_env()

    def test_get_shift_env_ok(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://b@n/syn/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        monkeypatch.delenv("DATAGEN_CHECKPOINT_URI", raising=False)
        cfg = shift_keys.get_shift_env()
        assert cfg["DATAGEN_SYNTHETIC_BASE_URI"] == "oci://b@n/syn"  # trailing / stripped
        assert cfg["DATAGEN_SPECS_URI"] == "oci://b@n/specs.json"
        assert cfg.get("DATAGEN_CHECKPOINT_URI") in (None, "")

    def test_parse_arguments_offset_required(self):
        with pytest.raises(SystemExit):
            shift_keys.parse_arguments([])

    def test_parse_arguments_values(self):
        args = shift_keys.parse_arguments(["--offset", "1000000", "--dry-run"])
        assert args.offset == 1000000 and args.dry_run is True
        assert args.continue_on_error is False

    def test_oracle_props_none_without_env(self, monkeypatch):
        for k in ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
                  "DATAGEN_SOURCE_DB_PASSWORD"):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://b@n/syn/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        cfg = shift_keys.get_shift_env()
        assert cfg["DATAGEN_ORACLE_OWNER"] == "CETIP"
        assert shift_keys.oracle_props_or_none(cfg) is None

    def test_deployment_summary_lists_env_and_config(self, capsys):
        shift_keys.print_deployment_summary({"DATAGEN_SYNTHETIC_BASE_URI": "oci://b@n/syn"})
        out = capsys.readouterr().out
        assert "DATAGEN_SYNTHETIC_BASE_URI" in out
        assert "DATAGEN_SPECS_URI" in out
        assert "DATAGEN_CHECKPOINT_URI" in out
        assert "DATAGEN_SOURCE_JDBC_URL" in out
        assert "DATAGEN_ORACLE_OWNER" in out
        assert "datagen/shift_keys.py" in out
        assert "--offset" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_shift_keys.py::TestEnvAndCli -v`
Expected: FAIL — missing `get_shift_env` / `parse_arguments` / `print_deployment_summary`.

- [ ] **Step 3: Implement env, CLI, summary, and `main`**

```python
# datagen/shift_keys.py
SYNTHETIC_ENV = "DATAGEN_SYNTHETIC_BASE_URI"
SPECS_ENV = "DATAGEN_SPECS_URI"
CHECKPOINT_ENV = "DATAGEN_CHECKPOINT_URI"


def get_shift_env() -> dict:
    config: dict = {}
    missing = []
    for name in (SYNTHETIC_ENV, SPECS_ENV):
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Missing required env var(s): %s", ", ".join(missing))
        sys.exit(1)
    # Optional prefix — must match what engorda used so we hit the same paths.
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", "").strip("/")
    chk = os.environ.get(CHECKPOINT_ENV)
    if chk:
        config[CHECKPOINT_ENV] = chk.rstrip("/")
    # Optional Oracle pre-flight: URL + user + password travel as a set.
    for name in ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
                 "DATAGEN_SOURCE_DB_PASSWORD"):
        val = os.environ.get(name)
        if val:
            config[name] = val
    config["DATAGEN_ORACLE_OWNER"] = os.environ.get("DATAGEN_ORACLE_OWNER", "CETIP")
    # JDBC tuning defaults required by build_connection_properties.
    config.setdefault("DATAGEN_JDBC_FETCH_SIZE", os.environ.get("DATAGEN_JDBC_FETCH_SIZE", "1000"))
    config.setdefault("DATAGEN_JDBC_READ_TIMEOUT_MS",
                      os.environ.get("DATAGEN_JDBC_READ_TIMEOUT_MS", "60000"))
    config.setdefault("DATAGEN_JDBC_LOB_PREFETCH",
                      os.environ.get("DATAGEN_JDBC_LOB_PREFETCH", "262144"))
    return config


def oracle_props_or_none(config: dict):
    """Build JDBC connection properties iff the full Oracle env set is present."""
    if all(config.get(k) for k in
           ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
            "DATAGEN_SOURCE_DB_PASSWORD")):
        return build_connection_properties(config)
    return None


def parse_arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a uniform +N to generated PK/FK values in the synthetic output.")
    parser.add_argument("--offset", type=int, required=True,
                        help="Uniform amount added to every shifted key.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Pre-flight only: report shift columns + overflow, write nothing.")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Continue to remaining tables if one fails (default: stop).")
    return parser.parse_args(argv)


def print_deployment_summary(config: dict) -> None:
    base = config.get(SYNTHETIC_ENV, "oci://<bucket>@<namespace>/<prefix>")
    print(
        "\n=== Deployment ===\n"
        "Required env vars:\n"
        f"  {SYNTHETIC_ENV}   {base}\n"
        f"  {SPECS_ENV}            oci://<bucket>@<namespace>/specs.json\n"
        f"  {CHECKPOINT_ENV}       (optional) oci://<bucket>@<namespace>/_chk\n"
        "Optional (enables live Oracle datatype + collision pre-flight):\n"
        "  DATAGEN_SOURCE_JDBC_URL      jdbc:oracle:thin:@//host:port/service\n"
        "  DATAGEN_SOURCE_DB_USER       <user>\n"
        "  DATAGEN_SOURCE_DB_PASSWORD   <password>\n"
        "  DATAGEN_ORACLE_OWNER         CETIP   (default)\n"
        "\nData Flow application:\n"
        "  Main:       datagen/shift_keys.py\n"
        "  Arguments:  --offset <N> [--dry-run] [--continue-on-error]\n"
        "  Spark:      create_spark_session workload conf (aggregatePushdown, Kryo,\n"
        "              memoryOverheadFactor=0.2). No shuffle -> shuffle.partitions irrelevant.\n"
        "  Shape:      Driver    8 OCPU / 64 GB\n"
        "              Executors 4 x (16-32 OCPU / 128 GB)   # I/O-bound; scale OCPU\n"
        "  Network:    Oracle checks need Data Flow -> Oracle connectivity (ADB networking)\n"
    )


def main() -> None:
    args = parse_arguments()
    config = get_shift_env()
    spark = create_spark_session("DataGenShiftKeys")
    try:
        reliable = bool(config.get(CHECKPOINT_ENV))
        if reliable:
            spark.sparkContext.setCheckpointDir(config[CHECKPOINT_ENV])

        specs = load_specs(spark, config[SPECS_ENV])
        shift = compute_shift_columns(specs)
        base = synthetic_base_path(config)  # base URI + optional prefix
        owner = config["DATAGEN_ORACLE_OWNER"]
        total_cols = sum(len(v) for v in shift.values())
        logger.info("Shifting %d column(s) across %d table(s) by +%d",
                    total_cols, len(shift), args.offset)

        # Live Oracle pre-flight when DB env is configured; else Parquet fallback.
        props = oracle_props_or_none(config)
        capacity_override: Dict[Tuple[str, str], int] = {}
        collisions: List[Tuple[str, int, int]] = []
        if props is not None:
            logger.info("Oracle pre-flight against OWNER=%s", owner)
            capacity_override = oracle_column_capacities(
                read_oracle_capacities(spark, props, owner), shift)
            prod_max: Dict[str, int] = {}
            synth_min: Dict[str, int] = {}
            for table in shift:
                pk_cols = specs[table].get("pk_cols", [])
                if specs[table].get("static") or not pk_cols:
                    continue
                pk = pk_cols[-1]
                if pk not in shift[table]:
                    continue  # PK kept fixed (FK-to-static) — no collision risk
                pmax = read_oracle_max(spark, props, owner, table, pk)
                if pmax is not None:
                    prod_max[table] = pmax
                path = f"{base}/{table_path_name(table)}"
                row = read_parquet(spark, path).agg(F.min(F.col(pk)).alias("m")).first()
                synth_min[table] = None if row is None or row["m"] is None else int(row["m"])
            collisions = find_collisions(prod_max, synth_min, args.offset)
        else:
            logger.warning("No Oracle env -> Parquet-schema capacity; production "
                           "COLLISION was NOT verified for offset %d.", args.offset)

        overflows = check_overflow(spark, base, shift, args.offset, capacity_override)
        if overflows or collisions:
            logger.error("Pre-flight FAILED — aborting, nothing written.")
            for table, col, mx, shifted, cap in overflows:
                logger.error("  overflow %s.%s: max=%d +%d=%d > capacity %d",
                             table, col, mx, args.offset, shifted, cap)
            for table, pmax, shifted_min in collisions:
                logger.error("  collision %s: synthetic min+%d=%d <= production max %d",
                             table, args.offset, shifted_min, pmax)
            print_deployment_summary(config)
            sys.exit(1)

        if args.dry_run:
            logger.info("Dry run: pre-flight OK. Writing nothing.")
            print_deployment_summary(config)
            return

        logger.warning("In-place, non-idempotent mutation — re-running double-shifts.")
        failures = apply_shift(spark, base, shift, args.offset,
                               continue_on_error=args.continue_on_error,
                               reliable_checkpoint=reliable)
        if failures:
            logger.error("Failed table(s): %s", ", ".join(failures))
            print_deployment_summary(config)
            sys.exit(1)
        logger.info("Done: shifted %d table(s) by +%d.", len(shift), args.offset)
        print_deployment_summary(config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_shift_keys.py::TestEnvAndCli -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/shift_keys.py tests/test_shift_keys.py
git commit -m "feat(shift_keys): env, CLI, deployment summary, main"
```

---

### Task 7: Full-suite check + lint

**Files:** none (verification only)

- [ ] **Step 1: Run the whole shift_keys suite**

Run: `… -m pytest tests/test_shift_keys.py -v`
Expected: PASS (all tasks' tests).

- [ ] **Step 2: Run the full project suite to confirm no regressions**

Run: `… -m pytest tests/ -q`
Expected: **no new failures** vs the baseline — only the pre-existing `FakeDF`-mock /
arg-default failures in `test_engorda_tables.py` remain (their exact count is
environment/time-specific; confirm none are newly introduced by shift_keys).

- [ ] **Step 3: Lint**

Run: `.venv/bin/ruff check datagen/shift_keys.py tests/test_shift_keys.py`
Expected: `All checks passed!` (fix any findings; keep line length ≤ 100 per `pyproject.toml`).

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -A
git commit -m "chore(shift_keys): lint"
```

---

## Notes for the implementer

- **Do not** use `df.write.mode("overwrite")` anywhere — on the OCI HDFS connector it deletes the shared parent prefix and clobbers sibling tables. Always go through `write_synthetic_table` (scoped delete + append).
- The checkpoint in `apply_shift` is **mandatory**, not an optimization: `write_synthetic_table` deletes the table's prefix before appending, so a lazily-read `df` still pointing at those files would be corrupted. `localCheckpoint`/`checkpoint` replace the plan with a materialized leaf.
- `_pk_capacity` returns `None` for non-numeric/unknown dtypes — `check_overflow` skips those (they shouldn't appear among key columns, but the guard is cheap).
- Keep the output schema identical via `cast(dtype)` — the downstream JDBC load depends on it.
- The Oracle pre-flight is **optional**: with the full `DATAGEN_SOURCE_JDBC_URL`/`_DB_USER`/`_DB_PASSWORD` set it uses live `ALL_TAB_COLUMNS` capacity + `MAX(pk)` collision; without it, it falls back to Parquet-schema capacity and **loudly warns** that collision wasn't verified. Enabling it needs Data Flow → Oracle networking.
- Put the `from datagen.save_tables import build_connection_properties, read_rows, read_single_value` line in the **top** import block (with the engorda import) so `ruff` doesn't flag a non-top-level import.
- Oracle pure functions (`capacity_from_precision_scale`, `oracle_column_capacities`, `find_collisions`) are unit-tested; the thin JDBC wrappers aren't run locally (no Oracle) — keep them thin so there's nothing to test beyond the query string.
