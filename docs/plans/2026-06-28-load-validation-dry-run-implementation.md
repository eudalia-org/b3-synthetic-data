# Load Validation Dry-Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only `validate_load` pre-flight + `--dry-run` to `datagen/load_tables.py` that validates the synthetic Parquet against the live target Oracle schema and aborts (inserting nothing) on any of six constraint-violation classes.

**Architecture:** Pure per-check functions (target metadata + synthetic profile → `Violation`s, fully unit-tested) wrapped by thin Spark/Oracle I/O (synthetic profiling aggregates; `ALL_TAB_COLUMNS`/`ALL_CONSTRAINTS` reads; bounded production anti-joins). A `validate_load` orchestrator runs before the insert loop in `main`; violations abort with a report. `--dry-run` runs the validation and stops.

**Tech Stack:** Python, PySpark 4.1 (Java 17–21), Oracle JDBC. `load_tables.py` is a self-contained single-file Data Flow app — no `datagen.*` imports; reuses its own helpers (`read_rows`, `build_connection_properties`, `validate_identifier`, `read_existing_keys`, `build_existing_keys_query`, `table_owner_and_name`, `pk_cols_for`, `is_static`, `build_load_path`) and vendors one small helper.

**Spec:** `docs/plans/2026-06-28-load-validation-dry-run-design.md`

---

## File Structure

- **Modify:** `datagen/load_tables.py` — add: `Violation` namedtuple; `capacity_from_precision_scale` (vendored); six pure check functions; `validate_table` (pure per-table orchestrator); `profile_synthetic_table` (Spark); `read_target_columns`/`read_target_constraints`/`count_prod_collisions`/`count_fk_static_orphans` (thin Oracle I/O); `validate_load` (I/O orchestrator); `format_violation_report` (pure); `--dry-run` arg + `main` wiring.
- **Create:** `tests/test_load_validation.py` — unit tests for the pure functions + an integration test for `profile_synthetic_table`.

## Test command

PySpark 4.1 needs Java 17. Run tests with:

```
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  /private/tmp/claude-502/-Users-mateus-projects-eudalia-b3-synthetic-data/448b6c8e-627f-4aba-8e7c-d3c89ca352f6/scratchpad/venv/bin/python \
  -m pytest tests/test_load_validation.py -v
```

Lint: `…/scratchpad/venv/bin/ruff check datagen/load_tables.py tests/test_load_validation.py` (line length ≤ 100).

---

### Task 1: `Violation`, vendored capacity, and the four cheap pure checks

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_validation.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_load_validation.py
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
        assert len(out) == 1 and out[0].columns == "K"

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_load_validation.py -v`
Expected: FAIL — `AttributeError`/`ModuleNotFoundError` (functions not defined).

- [ ] **Step 3: Implement in `datagen/load_tables.py`**

Add near the top (after imports / constants):

```python
from collections import namedtuple

Violation = namedtuple("Violation", ["table", "check", "columns", "detail"])


def capacity_from_precision_scale(precision, scale):
    """Largest integer an Oracle NUMBER(precision, scale) holds.
    NULL precision (unconstrained NUMBER) -> None (no limit)."""
    if precision is None:
        return None
    int_digits = int(precision) - int(scale or 0)
    return (10 ** int_digits) - 1 if int_digits > 0 else 0


def column_alignment_violations(table, synthetic_cols, target_cols):
    """synthetic_cols: set of UPPER names. target_cols: {COL: {nullable, has_default}}."""
    out = []
    for col in sorted(synthetic_cols - set(target_cols)):
        out.append(Violation(table, "column_alignment", col, "column not in target table"))
    required = {
        c for c, m in target_cols.items() if not m["nullable"] and not m["has_default"]
    }
    for col in sorted(required - synthetic_cols):
        out.append(Violation(
            table, "column_alignment", col, "required NOT NULL column missing from synthetic"))
    return out


def numeric_domain_violations(table, profile, target_cols):
    """profile: {COL: {max, min}} (numeric cols). target_cols: {COL: {precision, scale}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None:
            continue
        cap = capacity_from_precision_scale(meta.get("precision"), meta.get("scale"))
        if cap is None:
            continue
        if prof["max"] is not None and prof["max"] > cap:
            out.append(Violation(table, "numeric_domain", col,
                                 f"max {prof['max']} > capacity {cap}"))
        if prof["min"] is not None and prof["min"] < -cap:
            out.append(Violation(table, "numeric_domain", col,
                                 f"min {prof['min']} < -capacity {-cap}"))
    return out


def string_length_violations(table, profile, target_cols):
    """profile: {COL: {max_octet}}. target_cols: {COL: {data_length}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None or meta.get("data_length") is None:
            continue
        if prof.get("max_octet") is not None and prof["max_octet"] > meta["data_length"]:
            out.append(Violation(table, "string_length", col,
                                 f"max byte length {prof['max_octet']} > {meta['data_length']}"))
    return out


def not_null_violations(table, profile, target_cols):
    """profile: {COL: {null_count}}. target_cols: {COL: {nullable}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None or meta.get("nullable", True):
            continue
        if prof.get("null_count", 0) > 0:
            out.append(Violation(table, "not_null", col,
                                 f"{prof['null_count']} NULL(s) in NOT NULL column"))
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_load_validation.py -v`
Expected: PASS (capacity 1, column-alignment 2, numeric 3, string 2, not-null 1 = 9 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_validation.py
git commit -m "feat(load): cheap pre-flight checks (alignment, domain, length, not-null)"
```

---

### Task 2: uniqueness + FK-to-static pure checks

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_validation.py`

- [ ] **Step 1: Write the failing tests**

```python
class TestUniqueness:
    def test_internal_dup_flagged(self):
        constraints = [("PK_T", ("A",)), ("UK_T", ("B", "C"))]
        out = L.uniqueness_violations(
            "T", constraints, total_count=100,
            distinct_counts={("A",): 100, ("B", "C"): 90},  # UK has dups
            prod_collision_counts={})
        assert [(v.check, v.columns) for v in out] == [("uniqueness_internal", "B,C")]

    def test_production_collision_flagged(self):
        out = L.uniqueness_violations(
            "T", [("PK_T", ("A",))], total_count=100,
            distinct_counts={("A",): 100},
            prod_collision_counts={("A",): 5})
        assert [(v.check, v.columns) for v in out] == [("uniqueness_vs_production", "A")]

    def test_clean_no_violations(self):
        out = L.uniqueness_violations(
            "T", [("PK_T", ("A",))], total_count=100,
            distinct_counts={("A",): 100}, prod_collision_counts={("A",): 0})
        assert out == []


class TestFkToStatic:
    def test_orphans_flagged(self):
        out = L.fk_to_static_violations(
            "T", {(("NUM_TIPO_IF",), "TIPO_IF"): 7, (("X",), "Y"): 0})
        assert [(v.columns, v.detail.startswith("7")) for v in out] == [("NUM_TIPO_IF", True)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_load_validation.py::TestUniqueness tests/test_load_validation.py::TestFkToStatic -v`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement**

```python
def uniqueness_violations(table, constraints, total_count, distinct_counts,
                          prod_collision_counts):
    """constraints: list of (name, tuple(cols)). distinct_counts/prod_collision_counts
    keyed by tuple(cols). Flags internal dups (distinct < total) and production
    collisions (>0 synthetic keys already in production)."""
    out = []
    for _name, cols in constraints:
        label = ",".join(cols)
        distinct = distinct_counts.get(cols)
        if distinct is not None and distinct < total_count:
            out.append(Violation(table, "uniqueness_internal", label,
                                 f"{total_count - distinct} duplicate key(s) within synthetic"))
        collisions = prod_collision_counts.get(cols, 0)
        if collisions > 0:
            out.append(Violation(table, "uniqueness_vs_production", label,
                                 f"{collisions} synthetic key(s) already in production"))
    return out


def fk_to_static_violations(table, orphan_counts):
    """orphan_counts: {(tuple(cols), parent_table): count}."""
    out = []
    for (cols, parent), count in orphan_counts.items():
        if count > 0:
            out.append(Violation(table, "fk_to_static", ",".join(cols),
                                 f"{count} value(s) not present in static parent {parent}"))
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `… -m pytest tests/test_load_validation.py::TestUniqueness tests/test_load_validation.py::TestFkToStatic -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_validation.py
git commit -m "feat(load): uniqueness + fk-to-static pre-flight checks"
```

---

### Task 3: `validate_table` pure orchestrator + `format_violation_report`

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_validation.py`

- [ ] **Step 1: Write the failing tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `… -m pytest tests/test_load_validation.py::TestValidateTable tests/test_load_validation.py::TestReport -v`
Expected: FAIL — not defined.

- [ ] **Step 3: Implement**

```python
def validate_table(table, synthetic_cols, profile, target_cols, constraints,
                   total_count, distinct_counts, prod_collision_counts, fk_orphan_counts):
    """Run all six checks for one table; return the concatenated violations.
    `profile` is the per-column dict (max/min/max_octet/null_count); the numeric
    and string checks read the columns relevant to them."""
    violations = []
    violations += column_alignment_violations(table, synthetic_cols, target_cols)
    violations += numeric_domain_violations(table, profile, target_cols)
    violations += string_length_violations(table, profile, target_cols)
    violations += not_null_violations(table, profile, target_cols)
    violations += uniqueness_violations(
        table, constraints, total_count, distinct_counts, prod_collision_counts)
    violations += fk_to_static_violations(table, fk_orphan_counts)
    return violations


def format_violation_report(violations):
    """Group violations by table into a human-readable multi-line report."""
    if not violations:
        return "No violations."
    by_table = {}
    for v in violations:
        by_table.setdefault(v.table, []).append(v)
    lines = []
    for table in sorted(by_table):
        lines.append(f"{table}:")
        for v in by_table[table]:
            lines.append(f"  [{v.check}] {v.columns} — {v.detail}")
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass** — Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_validation.py
git commit -m "feat(load): validate_table orchestrator + violation report"
```

---

### Task 4: `profile_synthetic_table` (Spark aggregates)

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_validation.py`

- [ ] **Step 1: Write the failing integration test**

```python
import pytest


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("load-val-test").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


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
```

- [ ] **Step 2: Run test to verify it fails** — Expected: FAIL (no `profile_synthetic_table`).

- [ ] **Step 3: Implement**

```python
def profile_synthetic_table(df, target_cols, constraints):
    """One-pass profile of a synthetic table for the checks.
    target_cols: {COL: {is_numeric, is_string, nullable, ...}} (UPPER keys).
    Returns {total_count, columns: {COL: {max, min, max_octet, null_count}},
    distinct_counts: {tuple(cols): n}}."""
    from pyspark.sql import functions as F

    col_map = {c.upper(): c for c in df.columns}
    aggs = [F.count(F.lit(1)).alias("__total")]
    present = {}  # upper -> actual
    for up, actual in col_map.items():
        meta = target_cols.get(up)
        if meta is None:
            continue
        present[up] = actual
        if meta.get("is_numeric"):
            aggs.append(F.max(actual).alias(f"__max__{up}"))
            aggs.append(F.min(actual).alias(f"__min__{up}"))
        if meta.get("is_string"):
            aggs.append(F.max(F.octet_length(F.col(actual))).alias(f"__oct__{up}"))
        aggs.append(
            F.count(F.when(F.col(actual).isNull(), F.lit(1))).alias(f"__null__{up}"))
    # distinct per constraint whose columns are all present
    constraint_keys = []
    for _name, cols in constraints:
        if all(c.upper() in present for c in cols):
            actuals = [present[c.upper()] for c in cols]
            alias = "__dist__" + "_".join(c.upper() for c in cols)
            aggs.append(F.countDistinct(*[F.col(a) for a in actuals]).alias(alias))
            constraint_keys.append((tuple(c.upper() for c in cols), alias))

    row = df.agg(*aggs).first()
    columns = {}
    for up in present:
        meta = target_cols[up]
        columns[up] = {
            "max": row[f"__max__{up}"] if meta.get("is_numeric") else None,
            "min": row[f"__min__{up}"] if meta.get("is_numeric") else None,
            "max_octet": row[f"__oct__{up}"] if meta.get("is_string") else None,
            "null_count": row[f"__null__{up}"],
        }
    distinct_counts = {cols: row[alias] for cols, alias in constraint_keys}
    return {"total_count": row["__total"], "columns": columns,
            "distinct_counts": distinct_counts}
```

- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_validation.py
git commit -m "feat(load): profile_synthetic_table aggregates"
```

---

### Task 5: thin Oracle I/O readers + `validate_load` orchestrator

**Files:**
- Modify: `datagen/load_tables.py`

These wrap proven helpers (`read_rows`, `read_existing_keys`) and aren't run
locally (no Oracle in the test env). Keep them thin. No new tests — the pure
logic they feed is already covered (Tasks 1–4).

- [ ] **Step 1: Implement the metadata + production readers and the orchestrator**

```python
def read_target_columns(spark, properties, owner, tables):
    """{TABLE: {COL: {data_type, precision, scale, data_length, char_length,
    nullable(bool), has_default(bool), is_numeric, is_string}}} from ALL_TAB_COLUMNS."""
    owner = validate_identifier(owner)
    names = ",".join(f"'{validate_identifier(table_path_name(t))}'" for t in tables)
    rows = read_rows(spark, properties,
                     "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_PRECISION, "
                     "DATA_SCALE, DATA_LENGTH, CHAR_LENGTH, NULLABLE, DATA_DEFAULT "
                     f"FROM ALL_TAB_COLUMNS WHERE OWNER='{owner}' "
                     f"AND TABLE_NAME IN ({names})")
    out = {}
    numeric = {"NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "INTEGER"}
    string = {"VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"}
    for r in rows:
        dt = r["DATA_TYPE"]
        out.setdefault(r["TABLE_NAME"], {})[r["COLUMN_NAME"]] = {
            "data_type": dt,
            "precision": r["DATA_PRECISION"],
            "scale": r["DATA_SCALE"],
            "data_length": r["DATA_LENGTH"],
            "char_length": r["CHAR_LENGTH"],
            "nullable": r["NULLABLE"] == "Y",
            "has_default": r["DATA_DEFAULT"] is not None,
            "is_numeric": dt in numeric,
            "is_string": dt in string,
        }
    return out


def read_target_constraints(spark, properties, owner, tables):
    """{TABLE: [(constraint_name, (COL,...)), ...]} for P and U constraints,
    columns ordered by POSITION."""
    owner = validate_identifier(owner)
    names = ",".join(f"'{validate_identifier(table_path_name(t))}'" for t in tables)
    rows = read_rows(spark, properties,
                     "SELECT c.TABLE_NAME, c.CONSTRAINT_NAME, acc.COLUMN_NAME, acc.POSITION "
                     "FROM ALL_CONSTRAINTS c JOIN ALL_CONS_COLUMNS acc "
                     "ON c.OWNER=acc.OWNER AND c.CONSTRAINT_NAME=acc.CONSTRAINT_NAME "
                     f"WHERE c.OWNER='{owner}' AND c.CONSTRAINT_TYPE IN ('P','U') "
                     f"AND c.TABLE_NAME IN ({names})")
    grouped = {}
    for r in rows:
        grouped.setdefault((r["TABLE_NAME"], r["CONSTRAINT_NAME"]), []).append(
            (int(r["POSITION"]), r["COLUMN_NAME"]))
    out = {}
    for (table, name), cols in grouped.items():
        ordered = tuple(c for _pos, c in sorted(cols))
        out.setdefault(table, []).append((name, ordered))
    return out


def count_prod_collisions(spark, properties, config, owner, table_name, df, constraints):
    """{tuple(cols): count of synthetic keys already in production}. Range-bounds
    the production read on a single numeric key (reusing read_existing_keys);
    composite/non-numeric keys read distinct production columns directly."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    col_map = {c.upper(): c for c in df.columns}
    out = {}
    for _name, cols in constraints:
        actuals = [col_map.get(c.upper()) for c in cols]
        if any(a is None for a in actuals):
            continue
        syn_keys = df.select(*actuals).dropna().dropDuplicates()
        if len(cols) == 1 and isinstance(df.schema[actuals[0]].dataType, NumericType):
            bounds = df.agg(F.min(actuals[0]), F.max(actuals[0])).first()
            lo, hi = bounds[0], bounds[1]
            if lo is None:
                out[tuple(c.upper() for c in cols)] = 0
                continue
            lo, hi = normalize_pk_bound(lo), normalize_pk_bound(hi)
            existing = read_existing_keys(
                spark, properties, resolve_num_partitions(config),
                owner, table_name, actuals[0], lo, hi)
            existing = existing.withColumnRenamed(existing.columns[0], actuals[0])
        else:
            col_list = ",".join(validate_identifier(c) for c in cols)
            q = (f"(SELECT {col_list} FROM {validate_identifier(owner)}."
                 f"{validate_identifier(table_name)}) DATAGEN_UK")
            existing = (spark.read.format("jdbc").options(**properties)
                        .option("dbtable", q).load())
            for syn_col, prod_col in zip(actuals, existing.columns):
                existing = existing.withColumnRenamed(prod_col, syn_col)
        out[tuple(c.upper() for c in cols)] = syn_keys.join(
            existing, on=actuals, how="inner").count()
    return out


def count_fk_static_orphans(spark, properties, config, specs, df, table, owner_for):
    """{(tuple(cols), parent): count of synthetic FK values absent from the static
    parent's key}. Only FKs whose parent is static (is_static) are checked."""
    norm = table_path_name(table).upper()
    entry = specs.get(norm, {})
    col_map = {c.upper(): c for c in df.columns}
    out = {}
    for fk in _fk_list(entry):
        parent = (fk.get("parent_table") or "").upper()
        if not parent or not is_static(specs, parent):
            continue
        cols = [c.upper() for c in fk.get("columns", [])]
        pcols = [c.upper() for c in (fk.get("parent_columns") or [])]
        actuals = [col_map.get(c) for c in cols]
        if not cols or len(cols) != len(pcols) or any(a is None for a in actuals):
            continue
        p_owner, p_name = owner_for(parent)
        col_list = ",".join(validate_identifier(c) for c in pcols)
        q = (f"(SELECT {col_list} FROM {validate_identifier(p_owner)}."
             f"{validate_identifier(p_name)}) DATAGEN_FK")
        parent_keys = spark.read.format("jdbc").options(**properties).option(
            "dbtable", q).load()
        for a, pc in zip(actuals, parent_keys.columns):
            parent_keys = parent_keys.withColumnRenamed(pc, a)
        syn = df.select(*actuals).dropna()
        orphans = syn.join(parent_keys, on=actuals, how="left_anti").count()
        out[(tuple(cols), parent)] = orphans
    return out


def validate_load(spark, properties, config, specs, target_schema, tables, limit):
    """Read-only pre-flight. Returns a flat list of Violations across all tables."""
    owner_for = lambda t: table_owner_and_name(target_schema, t)  # noqa: E731
    target_columns = read_target_columns(spark, properties, target_schema, tables)
    target_constraints = read_target_constraints(spark, properties, target_schema, tables)
    violations = []
    for table in tables:
        owner, table_name = owner_for(table)
        tcols = target_columns.get(table_name)
        if tcols is None:
            violations.append(Violation(table, "column_alignment", "*",
                                        f"target table {owner}.{table_name} not found"))
            continue
        df = spark.read.parquet(build_load_path(config, table_path_name(table)))
        if limit is not None:
            df = df.limit(limit)
        constraints = target_constraints.get(table_name, [])
        prof = profile_synthetic_table(df, tcols, constraints)
        prod_collisions = count_prod_collisions(
            spark, properties, config, owner, table_name, df, constraints)
        fk_orphans = count_fk_static_orphans(
            spark, properties, config, specs, df, table, owner_for)
        violations += validate_table(
            table=table,
            synthetic_cols={c.upper() for c in df.columns},
            profile=prof["columns"],
            target_cols=tcols,
            constraints=constraints,
            total_count=prof["total_count"],
            distinct_counts=prof["distinct_counts"],
            prod_collision_counts=prod_collisions,
            fk_orphan_counts=fk_orphans,
        )
    return violations
```

- [ ] **Step 2: Verify the module imports and the existing suite is unaffected**

Run: `… -m pytest tests/test_load_validation.py tests/test_load_tables.py -q`
Expected: the Task 1–4 tests still pass; `test_load_tables.py` unaffected.

- [ ] **Step 3: Lint** — `… ruff check datagen/load_tables.py` → All checks passed.

- [ ] **Step 4: Commit**

```bash
git add datagen/load_tables.py
git commit -m "feat(load): Oracle metadata/collision readers + validate_load orchestrator"
```

---

### Task 6: `--dry-run` arg + `main` wiring

**Files:**
- Modify: `datagen/load_tables.py`
- Test: `tests/test_load_validation.py`

- [ ] **Step 1: Write the failing test (arg parsing)**

```python
class TestDryRunArg:
    def test_dry_run_flag_parses(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["load_tables", "--dry-run"])
        args = L.parse_arguments()
        assert args.dry_run is True

    def test_dry_run_defaults_false(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["load_tables"])
        args = L.parse_arguments()
        assert args.dry_run is False
```

- [ ] **Step 2: Run test to verify it fails** — Expected: FAIL (`dry_run` attr missing).

- [ ] **Step 3: Implement — add the flag and wire `main`**

In `parse_arguments`, add:

```python
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the synthetic data against the target schema and exit; insert nothing.",
    )
```

In `main`, replace the body after `tables = resolve_load_tables(...)` so validation
runs before any insert:

```python
        tables = resolve_load_tables(specs, requested)
        target_schema = config["DATAGEN_TARGET_SCHEMA"]
        properties = build_connection_properties(config)

        logger.info("Pre-flight validation against %s ...", target_schema)
        violations = validate_load(
            spark, properties, config, specs, target_schema, tables, args.limit)
        if violations:
            logger.error("Pre-flight FAILED (%d violation(s)) — nothing inserted:\n%s",
                         len(violations), format_violation_report(violations))
            sys.exit(1)
        logger.info("Pre-flight validation passed.")
        if args.dry_run:
            logger.info("Dry run: validation only, nothing loaded.")
            return

        run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        entries = capture_manifest_entries(
            spark, properties, config, specs, target_schema, tables)
        manifest = build_manifest(
            run_id, datetime.now(timezone.utc).isoformat(), target_schema, entries)
        path = write_manifest(spark, config, run_id, manifest)
        logger.info("Load run_id=%s; manifest written to %s", run_id, path)

        load_tables(spark, config, specs, tables,
                    continue_on_error=args.continue_on_error, limit=args.limit)
```

(Note: `--continue-on-error` does not apply to validation — a violation aborts the
whole run; it only governs the insert loop, unchanged.)

- [ ] **Step 4: Run tests to verify they pass** — Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add datagen/load_tables.py tests/test_load_validation.py
git commit -m "feat(load): --dry-run flag + mandatory pre-flight in main"
```

---

### Task 7: Full-suite check + lint

**Files:** none (verification only)

- [ ] **Step 1: Run the new validation suite** — `… -m pytest tests/test_load_validation.py -v` → all PASS.

- [ ] **Step 2: Run the whole project suite** — `… -m pytest tests/ -q`
Expected: **no new failures** vs baseline — only the pre-existing `FakeDF`-mock /
arg-default failures in `test_engorda_tables.py` (count is environment-specific;
confirm none are newly introduced here).

- [ ] **Step 3: Lint** — `… ruff check datagen/load_tables.py tests/test_load_validation.py` → All checks passed.

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -A && git commit -m "chore(load): lint"
```

---

## Notes for the implementer

- `load_tables.py` is a **self-contained single-file Data Flow app** — do NOT import from `datagen.*`. Everything needed is either already in the file or added by this plan (`capacity_from_precision_scale` is vendored, not imported).
- Reuse the existing helpers as-is: `read_rows`, `build_connection_properties`, `validate_identifier`, `read_existing_keys`, `build_existing_keys_query`, `normalize_pk_bound`, `resolve_num_partitions`, `table_owner_and_name`, `table_path_name`, `pk_cols_for`, `is_static`, `_fk_list`, `build_load_path`.
- Validation is **read-only**: only `SELECT`s and Parquet reads. Never insert or run DDL in the validation path.
- The pure check functions (Tasks 1–3) are the tested core; the Oracle I/O (Task 5) is thin glue not exercised locally — keep it minimal so there's little untested surface.
- All identifiers passed into SQL go through `validate_identifier` (the file's existing injection guard).
