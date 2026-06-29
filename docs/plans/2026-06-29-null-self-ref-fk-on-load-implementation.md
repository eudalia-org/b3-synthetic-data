# Null Self-Referential FK Columns on Load — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Before inserting `INSTRUMENTO_FINANCEIRO`, set its self-referential FK columns (`NUM_IF_ORIGEM`, `NUM_IF_PERTENCE`) to NULL so the parallel `append` can't hit `ORA-02291` (a row referencing a not-yet-inserted row in the same table); left NULL, not back-filled.

**Architecture:** A `NULL_ON_INSERT` constant maps a table to the columns to null, and a `null_self_ref_columns(df, table, null_map)` helper nulls them (dtype-preserved) — called in `load_table` between `apply_pk_guard` and the write. No load-order change; still only the 15 non-static tables load.

**Tech Stack:** Python, PySpark 4.1 (Java 17). `load_tables.py` is a self-contained Data Flow app — no `datagen.*` imports; `F` is imported locally per-function as elsewhere in the file.

**Spec:** `docs/plans/2026-06-29-null-self-ref-fk-on-load-design.md`

---

## File Structure

- **Modify:** `datagen/load_tables.py` — add `NULL_ON_INSERT` constant + `null_self_ref_columns` helper; call it in `load_table` after `apply_pk_guard`.
- **Create:** `tests/test_load_self_ref.py` — unit tests for `null_self_ref_columns` (local Spark fixture).

## Test command

```
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  /private/tmp/claude-502/-Users-mateus-projects-eudalia-b3-synthetic-data/448b6c8e-627f-4aba-8e7c-d3c89ca352f6/scratchpad/venv/bin/python \
  -m pytest tests/test_load_self_ref.py -v
```

Lint: `…/scratchpad/venv/bin/ruff check datagen/load_tables.py tests/test_load_self_ref.py` (line length ≤ 100).

---

### Task 1: `NULL_ON_INSERT` + `null_self_ref_columns` helper

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_self_ref.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_load_self_ref.py
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import load_tables as L  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("load-selfref-test").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


class TestNullSelfRefColumns:
    def _if_df(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("NUM_IF", T.LongType()),
            T.StructField("NUM_IF_ORIGEM", T.LongType()),
            T.StructField("NUM_IF_PERTENCE", T.LongType()),
            T.StructField("DESC", T.StringType()),
        ])
        return spark.createDataFrame([(1, 7, 8, "a"), (2, 9, 10, "b")], schema)

    def test_nulls_listed_columns_keeps_others(self, spark):
        out = L.null_self_ref_columns(
            self._if_df(spark), "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        rows = {r["DESC"]: (r["NUM_IF"], r["NUM_IF_ORIGEM"], r["NUM_IF_PERTENCE"])
                for r in out.collect()}
        assert rows == {"a": (1, None, None), "b": (2, None, None)}

    def test_dtype_preserved(self, spark):
        out = L.null_self_ref_columns(
            self._if_df(spark), "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        from pyspark.sql import types as T
        assert out.schema["NUM_IF_ORIGEM"].dataType == T.LongType()

    def test_other_table_unchanged(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1, 7)], T.StructType([T.StructField("NUM_IF", T.LongType()),
                                    T.StructField("NUM_IF_ORIGEM", T.LongType())]))
        out = L.null_self_ref_columns(df, "OPERACAO", L.NULL_ON_INSERT)
        assert out.collect()[0]["NUM_IF_ORIGEM"] == 7  # not in map -> untouched

    def test_missing_column_skipped(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1,)], T.StructType([T.StructField("NUM_IF", T.LongType())]))
        # listed cols absent -> no error, df unchanged
        out = L.null_self_ref_columns(df, "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        assert out.collect()[0]["NUM_IF"] == 1

    def test_case_insensitive_match(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1, 7)], T.StructType([T.StructField("num_if", T.LongType()),
                                    T.StructField("num_if_origem", T.LongType())]))
        out = L.null_self_ref_columns(df, "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        assert out.collect()[0]["num_if_origem"] is None

    def test_constant_contents(self):
        assert L.NULL_ON_INSERT == {
            "INSTRUMENTO_FINANCEIRO": ["NUM_IF_ORIGEM", "NUM_IF_PERTENCE"]}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_load_self_ref.py -v`
Expected: FAIL — `AttributeError`/`ModuleNotFoundError` (`NULL_ON_INSERT` / `null_self_ref_columns` not defined).

- [ ] **Step 3: Implement in `datagen/load_tables.py`**

Add the constant near the other module constants (e.g. after `IDENTIFIER_PATTERN`):

```python
# Self-referential FK columns set to NULL on insert: a synthetic row can point at
# another not-yet-inserted row in the same table (ORA-02291), and the values are
# stale post-PK-shift anyway. The columns are nullable; left NULL, not back-filled.
NULL_ON_INSERT = {
    "INSTRUMENTO_FINANCEIRO": ["NUM_IF_ORIGEM", "NUM_IF_PERTENCE"],
}
```

Add the helper (near `apply_pk_guard` / `load_table`):

```python
def null_self_ref_columns(df, table, null_map):
    """Set a table's self-referential FK columns to NULL before insert.

    They are nullable and left NULL (not back-filled). A synthetic row may
    reference another not-yet-inserted row in the same table (ORA-02291). dtype is
    preserved so the insert schema is unchanged. No-op for tables/columns absent
    from `null_map` or from `df`. Case-insensitive column match.
    """
    from pyspark.sql import functions as F

    cols = null_map.get(table_path_name(table).upper(), [])
    actual = {c.upper(): c for c in df.columns}
    nulled = []
    for c in cols:
        real = actual.get(c.upper())
        if real is not None:
            df = df.withColumn(real, F.lit(None).cast(df.schema[real].dataType))
            nulled.append(real)
    if nulled:
        logger.info("%s: nulled self-ref FK column(s) on insert: %s",
                    table_path_name(table).upper(), ", ".join(nulled))
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_load_self_ref.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_self_ref.py
git commit -m "feat(load): null self-ref FK columns helper + NULL_ON_INSERT"
```

---

### Task 2: Wire into `load_table`

**Files:**
- Modify: `datagen/load_tables.py` (`load_table`, between `apply_pk_guard` and `df.count()`)

No new unit test — the helper is covered by Task 1; this task is the one-line wiring + a whole-suite check that nothing regressed (the load path can't be exercised locally without Oracle).

- [ ] **Step 1: Add the call in `load_table`**

Find this block in `load_table`:

```python
    df, _ = apply_pk_guard(
        spark, properties, config, df, specs, owner, table_name, table, index, total
    )

    appended = df.count()
```

Insert the null step between them:

```python
    df, _ = apply_pk_guard(
        spark, properties, config, df, specs, owner, table_name, table, index, total
    )

    df = null_self_ref_columns(df, table, NULL_ON_INSERT)

    appended = df.count()
```

- [ ] **Step 2: Confirm the module imports and the existing suites are unaffected**

Run: `… -m pytest tests/test_load_self_ref.py tests/test_load_tables.py tests/test_load_validation.py -q`
Expected: all pass (the `load_tables`/`load_validation` suites unaffected; the new self-ref suite green).

- [ ] **Step 3: Lint**

Run: `… ruff check datagen/load_tables.py` → All checks passed.

- [ ] **Step 4: Commit**

```bash
git add datagen/load_tables.py
git commit -m "feat(load): apply null_self_ref_columns before insert in load_table"
```

---

### Task 3: Whole-suite check

**Files:** none (verification only)

- [ ] **Step 1:** `… -m pytest tests/ -q` — expect **no new failures** vs baseline (only the pre-existing `FakeDF`-mock / arg-default failures in `test_engorda_tables.py`; their count is environment-specific — confirm none are new).
- [ ] **Step 2:** `… ruff check datagen/load_tables.py tests/test_load_self_ref.py` → All checks passed. Commit any lint fixes (`chore(load): lint`).

---

## Notes for the implementer

- `load_tables.py` is a self-contained single-file Data Flow app — do NOT add `from datagen.* import`. `F` is imported locally inside the helper (the file has no module-level `F`).
- `null_self_ref_columns` is keyed by `table_path_name(table).upper()`, so it's a no-op for the other 14 tables and for any listed column not present in the DataFrame.
- Do NOT change `resolve_load_tables` / `topo_sort_for_load` — scope stays at the 15 non-static tables in their existing order.
- The change is row-count-neutral, so its exact position in the `apply_pk_guard`→`df.count()`→`write` window is immaterial; place it right after `apply_pk_guard` as shown.
