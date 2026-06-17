# Load Append + Static Filter + PK Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework `load_tables.py` to append (not overwrite), load only non-`static` tables from `specs.json`, skip synthetic rows whose PK already exists (PK-range-bounded anti-join), support `--limit` sample loads, and log clearly — per `docs/plans/2026-06-17-load-append-static-filter-design.md`.

**Architecture:** Modify the existing self-contained `load_tables.py`. Remove the FK/DDL constraint machinery (overwrite is gone). Add pure, unit-tested helpers (table selection, guard predicates, SQL builder); the Spark-touching pieces (specs read, existing-key read, anti-join, append) are covered by real-DB validation. Overwrite is replaced by plain `mode("append")`; the duplicate guard is a Spark anti-join whose existing-key read is bounded to the synthetic `[min,max]` PK range.

**Tech Stack:** Python 3.11, PySpark JDBC (Oracle `ojdbc8`), pytest.

## Global Constraints

- Tests/lint run WITHOUT `uv sync` (it is broken on this repo — missing local `eudalia` path dep):
  - Tests: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -v`
  - Lint: `uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py`
- `load_tables.py` top-level imports stay stdlib-only; `SparkSession` and any `pyspark.sql.functions`/`pyspark.sql.types` imports are lazy (inside functions) so unit tests run without pyspark.
- Trunk-based: commit directly to `main` after each task with the given message.
- Reuse existing helpers unchanged where the design doesn't touch them: `validate_identifier`, `parse_tables`, `get_load_env`, `create_spark_session`, `table_path_name`, `table_owner_and_name`, `dbtable_name`, `build_connection_properties`, `resolve_num_partitions`, `build_load_path`.

---

### Task 1: Remove FK/DDL machinery; make load_table a plain append

**Files:**
- Modify: `load_tables.py`
- Modify: `tests/test_load_tables.py`

**Interfaces:**
- Produces: `load_table(spark, properties, config, target_user, table)` (plain append, no truncate/constraints); `load_tables(spark, config, tables, continue_on_error)`.

- [ ] **Step 1: Delete the constraint/DDL functions and the contextmanager import**

In `load_tables.py` remove these functions entirely: `truncate_sql`,
`disable_constraint_sql`, `enable_constraint_sql`,
`build_constraint_discovery_query`, `constraints_disabled`, `read_rows`,
`execute_statement`, `discover_constraints`. Also remove the import line
`from contextlib import contextmanager`.

- [ ] **Step 2: Remove the two constraint CLI flags**

In `parse_arguments`, delete the `--no-manage-constraints` and
`--validate-constraints` arguments (leave the rest).

- [ ] **Step 3: Replace `load_table` with a plain-append version**

Replace the whole `load_table` function with:

```python
def load_table(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    target_user: str,
    table: str,
) -> None:
    owner, table_name = table_owner_and_name(target_user, table)
    validate_identifier(owner)
    validate_identifier(table_name)
    dbtable = dbtable_name(target_user, table)
    input_path = build_load_path(config, table_path_name(table))
    num_partitions = resolve_num_partitions(config)
    batch_size = config["DATAGEN_JDBC_BATCH_SIZE"]

    df = spark.read.parquet(input_path).repartition(num_partitions)
    logger.info("Appending %s to %s in %d partitions", input_path, dbtable, num_partitions)
    (
        df.write.format("jdbc")
        .options(**properties)
        .option("dbtable", dbtable)
        .option("batchsize", batch_size)
        .option("isolationLevel", DEFAULT_ISOLATION_LEVEL)
        .mode("append")
        .save()
    )
```

- [ ] **Step 4: Drop the removed params from `load_tables` and `main`**

In `load_tables`, change the signature to
`def load_tables(spark, config, tables, continue_on_error):` (remove
`manage_constraints` and `validate`), update the opening log line to drop
`manage_constraints=%s`, and change the `load_table(...)` call to:

```python
            load_table(
                spark=spark,
                properties=properties,
                config=config,
                target_user=target_user,
                table=table,
            )
```

In `main`, change the call to:

```python
        load_tables(
            spark,
            config,
            tables,
            continue_on_error=args.continue_on_error,
        )
```

- [ ] **Step 5: Remove the obsolete test classes**

In `tests/test_load_tables.py` delete the entire classes `TestSqlBuilders`,
`TestConstraintsDisabled`, and `TestDiscoverConstraints`.

- [ ] **Step 6: Verify compile, tests, lint**

```bash
uv run --no-project python -c "import load_tables"
uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -q
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
grep -n "constraint\|truncate\|execute_statement\|read_rows\|contextmanager" load_tables.py || true
```
Expected: import OK; 20 passed; lint clean; the grep prints nothing.

- [ ] **Step 7: Commit**

```bash
git add load_tables.py tests/test_load_tables.py
git commit -m "refactor: drop fk/ddl machinery, make load_table plain append"
```

---

### Task 2: Selection helpers — positive_int, pk_cols_for, is_static, resolve_load_tables

**Files:**
- Modify: `load_tables.py`
- Modify: `tests/test_load_tables.py`

**Interfaces:**
- Produces: `positive_int(str) -> int`; `pk_cols_for(specs: dict, table: str) -> list[str]`;
  `is_static(specs: dict, table: str) -> bool`;
  `resolve_load_tables(specs: dict, requested: list[str] | None) -> list[str]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestPositiveInt:
    def test_accepts_positive(self):
        assert load_tables.positive_int("100") == 100

    def test_rejects_non_integer(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("abc")

    def test_rejects_zero_and_negative(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("0")
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("-5")


SPECS = {
    "ENTIDADE": {"pk_cols": ["NUM_ID_ENTIDADE"]},
    "TIPO_DEBITO": {"pk_cols": ["COD_TIPO_DEBITO"], "static": True},
    "LANCAMENTO": {"pk_cols": ["NUM_ID_LANCAMENTO"]},
}


class TestPkColsFor:
    def test_returns_pk_cols(self):
        assert load_tables.pk_cols_for(SPECS, "LANCAMENTO") == ["NUM_ID_LANCAMENTO"]

    def test_matches_schema_qualified_and_case(self):
        assert load_tables.pk_cols_for(SPECS, "cetip.lancamento") == ["NUM_ID_LANCAMENTO"]

    def test_empty_when_absent(self):
        assert load_tables.pk_cols_for(SPECS, "NOPE") == []


class TestIsStatic:
    def test_true_for_static(self):
        assert load_tables.is_static(SPECS, "TIPO_DEBITO") is True

    def test_false_for_non_static(self):
        assert load_tables.is_static(SPECS, "ENTIDADE") is False

    def test_false_when_absent(self):
        assert load_tables.is_static(SPECS, "NOPE") is False


class TestResolveLoadTables:
    def test_requested_drops_static_keeps_order(self):
        assert load_tables.resolve_load_tables(
            SPECS, ["LANCAMENTO", "TIPO_DEBITO", "ENTIDADE"]
        ) == ["LANCAMENTO", "ENTIDADE"]

    def test_requested_table_absent_is_kept(self):
        assert load_tables.resolve_load_tables(SPECS, ["OTHER"]) == ["OTHER"]

    def test_none_returns_all_non_static_in_order(self):
        assert load_tables.resolve_load_tables(SPECS, None) == ["ENTIDADE", "LANCAMENTO"]

    def test_empty_result_exits(self):
        with pytest.raises(SystemExit):
            load_tables.resolve_load_tables(SPECS, ["TIPO_DEBITO"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k "PositiveInt or PkColsFor or IsStatic or ResolveLoadTables" -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'positive_int'`

- [ ] **Step 3: Implement the helpers**

Add to `load_tables.py` (place `positive_int` next to `parse_tables`; the
others after `build_load_path`):

```python
def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def pk_cols_for(specs: dict, table: str) -> list[str]:
    entry = specs.get(table_path_name(table).upper(), {})
    return list(entry.get("pk_cols", []))


def is_static(specs: dict, table: str) -> bool:
    return bool(specs.get(table_path_name(table).upper(), {}).get("static"))


def resolve_load_tables(specs: dict, requested: list[str] | None) -> list[str]:
    if requested:
        result = []
        for table in requested:
            if is_static(specs, table):
                logger.info("Skipping static table %s", table)
                continue
            if table_path_name(table).upper() not in specs:
                logger.info("Table %s not in specs; treating as non-static", table)
            result.append(table)
    else:
        result = [name for name, entry in specs.items() if not entry.get("static")]

    if not result:
        logger.error("No tables to load")
        sys.exit(1)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k "PositiveInt or PkColsFor or IsStatic or ResolveLoadTables" -v`
Expected: 13 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add table-selection and pk helpers for load"
```

---

### Task 3: Guard pure helpers — guard_applies, build_existing_keys_query

**Files:**
- Modify: `load_tables.py` (add `from decimal import Decimal` to imports)
- Modify: `tests/test_load_tables.py`

**Interfaces:**
- Produces: `guard_applies(pk_cols: list[str], pk_is_numeric: bool) -> bool`;
  `build_existing_keys_query(owner, table_name, pk_col, lo, hi) -> str` returning a
  parenthesized `(SELECT <pk> FROM <owner>.<table> WHERE <pk> BETWEEN <lo> AND <hi>) DATAGEN_KEYS`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
from decimal import Decimal


class TestGuardApplies:
    def test_single_numeric_true(self):
        assert load_tables.guard_applies(["NUM_ID"], True) is True

    def test_single_non_numeric_false(self):
        assert load_tables.guard_applies(["COD_X"], False) is False

    def test_composite_false(self):
        assert load_tables.guard_applies(["A", "B"], True) is False

    def test_empty_false(self):
        assert load_tables.guard_applies([], True) is False


class TestBuildExistingKeysQuery:
    def test_builds_bounded_subquery(self):
        q = load_tables.build_existing_keys_query("ADMIN", "LANCAMENTO", "NUM_ID", 10, 99)
        assert q == (
            "(SELECT NUM_ID FROM ADMIN.LANCAMENTO "
            "WHERE NUM_ID BETWEEN 10 AND 99) DATAGEN_KEYS"
        )

    def test_accepts_decimal_bounds(self):
        q = load_tables.build_existing_keys_query(
            "ADMIN", "T", "PK", Decimal("5"), Decimal("9")
        )
        assert "BETWEEN 5 AND 9" in q

    def test_rejects_non_numeric_bounds(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "PK", "5", "9")

    def test_rejects_boolean_bounds(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "PK", True, False)

    def test_rejects_bad_identifiers(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T; DROP", "PK", 1, 2)
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "P K", 1, 2)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k "GuardApplies or BuildExistingKeysQuery" -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'guard_applies'`

- [ ] **Step 3: Implement the helpers**

Add `from decimal import Decimal` to the stdlib imports block. Add the
functions after `resolve_load_tables`:

```python
def guard_applies(pk_cols: list[str], pk_is_numeric: bool) -> bool:
    return len(pk_cols) == 1 and pk_is_numeric


def build_existing_keys_query(
    owner: str, table_name: str, pk_col: str, lo, hi
) -> str:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    pk_col = validate_identifier(pk_col)
    for bound in (lo, hi):
        if isinstance(bound, bool) or not isinstance(bound, (int, float, Decimal)):
            raise ValueError(f"PK bound must be numeric: {bound!r}")
    return (
        f"(SELECT {pk_col} FROM {owner}.{table_name} "
        f"WHERE {pk_col} BETWEEN {lo} AND {hi}) DATAGEN_KEYS"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k "GuardApplies or BuildExistingKeysQuery" -v`
Expected: 9 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add pk duplicate-guard pure helpers"
```

---

### Task 4: CLI + specs wiring + --limit + structured logging

**Files:**
- Modify: `load_tables.py` (add `import json` to imports)

No new unit tests: this wires Spark-dependent flow (`load_specs`, parquet read,
limit) covered by real-DB validation; the pure helpers it calls are already
tested. Verify via compile + existing suite.

- [ ] **Step 1: Add `--specs` and `--limit`, make `--tables` optional**

In `parse_arguments`, change the mutually exclusive group to optional and add
the two arguments. The group block becomes:

```python
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument(
        "--tables",
        help="Comma-separated table list. If omitted, all non-static tables in --specs load.",
    )
    source.add_argument(
        "--tables-file",
        help="Local text file with one table per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--specs",
        default="specs.json",
        help="Path to specs JSON (static tables are skipped; pk_cols drive the dup guard).",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        help="Append at most this many rows per table (sample load into the real target).",
    )
```

Keep the existing `--continue-on-error` argument.

- [ ] **Step 2: Add `import json` and `load_specs`**

Add `import json` to the stdlib imports. Add this function (near
`create_spark_session`):

```python
def load_specs(spark: SparkSession, path: str) -> dict:
    try:
        text = "\n".join(spark.sparkContext.textFile(path).collect())
        return json.loads(text)
    except Exception as exc:
        logger.error("Failed to read specs %s: %s", path, exc)
        sys.exit(1)
```

- [ ] **Step 3: Thread `specs` and `limit` through `load_tables` and add structured logging**

Replace `load_tables` with:

```python
def load_tables(
    spark: SparkSession,
    config: dict[str, str],
    specs: dict,
    tables: list[str],
    continue_on_error: bool,
    limit: int | None,
) -> None:
    target_user = config["DATAGEN_TARGET_DB_USER"]
    properties = build_connection_properties(config)
    failures = []
    appended_total = 0
    total = len(tables)
    run_started_at = time.perf_counter()
    logger.info(
        "Load run: mode=APPEND, partitions=%s, batchsize=%s, limit=%s",
        config["DATAGEN_JDBC_NUM_PARTITIONS"],
        config["DATAGEN_JDBC_BATCH_SIZE"],
        limit if limit is not None else "none",
    )
    logger.info("Resolved %d table(s) to load", total)

    for index, table in enumerate(tables, start=1):
        try:
            started_at = time.perf_counter()
            appended = load_table(
                spark=spark,
                properties=properties,
                config=config,
                specs=specs,
                target_user=target_user,
                table=table,
                index=index,
                total=total,
                limit=limit,
            )
            appended_total += appended
            logger.info(
                "[%d/%d] %s: appended %s rows in %.1fs",
                index,
                total,
                table,
                f"{appended:,}",
                time.perf_counter() - started_at,
            )
        except Exception as exc:
            logger.exception("[%d/%d] %s: FAILED: %s", index, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise

    run_elapsed = time.perf_counter() - run_started_at
    logger.info(
        "Finished: loaded %d/%d table(s), %s rows in %.1fs",
        total - len(failures),
        total,
        f"{appended_total:,}",
        run_elapsed,
    )
    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)
```

- [ ] **Step 4: Update `load_table` for `specs`/`limit`/logging (guard added in Task 5)**

Replace `load_table` with this version (returns appended row count; applies
`--limit`; logs per the design; the guard is a no-op placeholder filled in
Task 5):

```python
def load_table(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    specs: dict,
    target_user: str,
    table: str,
    index: int,
    total: int,
    limit: int | None,
) -> int:
    owner, table_name = table_owner_and_name(target_user, table)
    validate_identifier(owner)
    validate_identifier(table_name)
    dbtable = dbtable_name(target_user, table)
    input_path = build_load_path(config, table_path_name(table))
    num_partitions = resolve_num_partitions(config)
    batch_size = config["DATAGEN_JDBC_BATCH_SIZE"]

    logger.info("[%d/%d] %s: reading %s", index, total, table, input_path)
    df = spark.read.parquet(input_path)
    if limit is not None:
        df = df.limit(limit)
    df = df.repartition(num_partitions)

    appended = df.count()
    limit_note = f" (limit {limit})" if limit is not None else ""
    logger.info(
        "[%d/%d] %s: %s rows%s -> appending to %s in %d partitions",
        index,
        total,
        table,
        f"{appended:,}",
        limit_note,
        dbtable,
        num_partitions,
    )
    (
        df.write.format("jdbc")
        .options(**properties)
        .option("dbtable", dbtable)
        .option("batchsize", batch_size)
        .option("isolationLevel", DEFAULT_ISOLATION_LEVEL)
        .mode("append")
        .save()
    )
    return appended
```

- [ ] **Step 5: Update `main` to read specs and resolve tables**

Replace `main` with:

```python
def main() -> None:
    args = parse_arguments()
    config = get_load_env()
    spark = create_spark_session("DataGenLoadTables")
    try:
        specs = load_specs(spark, args.specs)
        requested = (
            parse_tables(args.tables, args.tables_file)
            if (args.tables or args.tables_file)
            else None
        )
        tables = resolve_load_tables(specs, requested)
        load_tables(
            spark,
            config,
            specs,
            tables,
            continue_on_error=args.continue_on_error,
            limit=args.limit,
        )
    finally:
        spark.stop()
```

- [ ] **Step 6: Verify compile, tests, lint**

```bash
uv run --no-project python -c "import load_tables"
uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -q
uv run --no-project --with ruff ruff check load_tables.py
```
Expected: import OK; 42 passed; lint clean.

- [ ] **Step 7: Commit**

```bash
git add load_tables.py
git commit -m "feat: specs-driven selection, --limit, structured load logging"
```

---

### Task 5: Wire the PK-bounded anti-join guard into load_table

**Files:**
- Modify: `load_tables.py`

No new unit tests: the guard read/anti-join need a live Spark session and are
covered by real-DB validation (Task 7); the guard's pure helpers are already
tested in Tasks 2–3.

**Interfaces:**
- Consumes: `pk_cols_for`, `guard_applies`, `build_existing_keys_query`,
  `resolve_num_partitions`, `build_connection_properties` output (`properties`).
- Produces: `read_existing_keys(spark, properties, num_partitions, owner, table_name, pk_col, lo, hi) -> DataFrame`;
  `apply_pk_guard(spark, properties, config, df, specs, owner, table_name, table, index, total) -> (DataFrame, int)`
  returning `(rows_to_append_df, skipped_count)`.

- [ ] **Step 1: Add `read_existing_keys` and `apply_pk_guard`**

Add to `load_tables.py` (above `load_table`):

```python
def read_existing_keys(
    spark: SparkSession,
    properties: dict[str, str],
    num_partitions: int,
    owner: str,
    table_name: str,
    pk_col: str,
    lo,
    hi,
):
    query = build_existing_keys_query(owner, table_name, pk_col, lo, hi)
    return (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", query)
        .option("partitionColumn", validate_identifier(pk_col))
        .option("lowerBound", str(lo))
        .option("upperBound", str(hi))
        .option("numPartitions", num_partitions)
        .load()
    )


def apply_pk_guard(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    df,
    specs: dict,
    owner: str,
    table_name: str,
    table: str,
    index: int,
    total: int,
):
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    pk_cols = pk_cols_for(specs, table)
    col_map = {c.upper(): c for c in df.columns}
    pk_actual = col_map.get(pk_cols[0].upper()) if len(pk_cols) == 1 else None
    pk_is_numeric = bool(
        pk_actual is not None
        and isinstance(df.schema[pk_actual].dataType, NumericType)
    )

    if not guard_applies(pk_cols, pk_is_numeric) or pk_actual is None:
        logger.info(
            "[%d/%d] %s: no PK guard (pk_cols=%s) -> appending all rows",
            index, total, table, pk_cols,
        )
        return df, 0

    bounds = df.agg(F.min(pk_actual), F.max(pk_actual)).first()
    lo, hi = bounds[0], bounds[1]
    if lo is None:  # empty DataFrame
        return df, 0

    existing = read_existing_keys(
        spark, properties, resolve_num_partitions(config),
        owner, table_name, pk_actual, lo, hi,
    )
    existing = existing.withColumnRenamed(existing.columns[0], pk_actual)
    if not existing.take(1):
        logger.info(
            "[%d/%d] %s: 0 existing keys in PK range [%s, %s] -> appending all rows",
            index, total, table, lo, hi,
        )
        return df, 0

    to_append = df.join(existing, on=pk_actual, how="left_anti")
    appended = to_append.count()
    skipped = df.count() - appended
    logger.info(
        "[%d/%d] %s: %s existing keys in PK range [%s, %s] -> skipping %s already-loaded",
        index, total, table, f"{skipped:,}", lo, hi, f"{skipped:,}",
    )
    return to_append, skipped
```

- [ ] **Step 2: Call the guard from `load_table`**

In `load_table`, replace the block from `appended = df.count()` through the
end of the `df.write...save()` call with:

```python
    df, _ = apply_pk_guard(
        spark, properties, config, df, specs, owner, table_name, table, index, total
    )

    appended = df.count()
    limit_note = f" (limit {limit})" if limit is not None else ""
    logger.info(
        "[%d/%d] %s: appending %s rows%s to %s in %d partitions",
        index,
        total,
        table,
        f"{appended:,}",
        limit_note,
        dbtable,
        num_partitions,
    )
    (
        df.write.format("jdbc")
        .options(**properties)
        .option("dbtable", dbtable)
        .option("batchsize", batch_size)
        .option("isolationLevel", DEFAULT_ISOLATION_LEVEL)
        .mode("append")
        .save()
    )
    return appended
```

(The earlier `df = df.repartition(num_partitions)` line stays; the guard
operates on the repartitioned df and the anti-join result is what gets
written.)

- [ ] **Step 3: Verify compile, tests, lint**

```bash
uv run --no-project python -c "import load_tables"
uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -q
uv run --no-project --with ruff ruff check load_tables.py
```
Expected: import OK; 42 passed; lint clean.

- [ ] **Step 4: Commit**

```bash
git add load_tables.py
git commit -m "feat: skip already-loaded keys via pk-bounded anti-join"
```

---

### Task 6: README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Rewrite the "Fast Parallel Load" section**

Replace the body of the `## Fast Parallel Load` section (everything from the
paragraph after the heading down to, but not including, the next `##` heading)
with:

```markdown
`load_tables.py` loads per-table Parquet into the target Oracle database through
many short-lived parallel JDBC partitions (each partition commits in seconds, so a
load survives the Data Flow→ADB connection killer; Spark retries any killed
partition). It **appends** to existing target tables. Run one Data Flow job per big
table, or omit `--tables` to load every non-static table from the specs.

```bash
python load_tables.py --tables LANCAMENTO            # one table
python load_tables.py                                # all non-static tables in specs.json
python load_tables.py --tables LANCAMENTO --limit 100000   # sample load
```

Reads `{DATAGEN_LOAD_BASE_URI}/{DATAGEN_LOAD_PREFIX}/<TABLE>`. Tables marked
`"static": true` in `--specs` (default `specs.json`) are skipped — they are
pre-loaded reference data. `--limit N` appends at most N rows per table into the
real target (no separate sample target).

Duplicate guard: before appending, synthetic rows whose primary key already exists
in the target are skipped. The check is bounded to the synthetic batch's
`[min, max]` PK range (synthetic PKs are minted above the current max), so it reads
only that range — never the full target — and is skipped entirely when the range is
empty (the common first-load case). This makes rerunning failed tables
duplicate-free. The guard applies to single-column numeric PKs (from specs
`pk_cols`); other tables append without it and log a warning.

Partial failures are handled gracefully: with `--continue-on-error` the run attempts
every table, lists failed ones, and exits non-zero — rerun the failed tables (the PK
guard keeps the rerun duplicate-free).

Configuration: `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`,
`DATAGEN_TARGET_DB_USER` (default `ADMIN`), `DATAGEN_LOAD_BASE_URI`,
`DATAGEN_LOAD_PREFIX`, `DATAGEN_JDBC_NUM_PARTITIONS` (default 256),
`DATAGEN_JDBC_BATCH_SIZE` (default 10000), `DATAGEN_JDBC_READ_TIMEOUT_MS`
(default 600000). Set `spark.task.maxFailures` high (e.g. 8) in the Data Flow job.

Note: the guard makes reruns duplicate-free, but parallel JDBC append is
at-least-once within a single run (a partition that commits then is reported failed
is retried and re-inserts its rows). Closing that fully would need a server-side
staging+MERGE (CREATE TABLE on the target), which is out of scope.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: describe append, static filter, and dup guard for load"
```

---

### Task 7: Real-DB validation (run where the target Oracle is reachable)

**Files:** none (operational verification)

- [ ] **Step 1: Sample load + static skip**

Set `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`,
`DATAGEN_LOAD_BASE_URI`, then:

```bash
python load_tables.py --limit 100000
```
Expected log: `Load run: mode=APPEND ...`, `Resolved N table(s)`, `Skipping static
table TIPO_DEBITO` (and the other static ones), per-table `appending ... (limit
100000)`, and a `Finished: loaded N/N` summary. Confirm static tables are absent
from the per-table lines.

- [ ] **Step 2: Row count appended**

For one table, compare the target row delta to the Parquet/limit count:
`SELECT COUNT(*)` before and after, or the logged `appended` count.

- [ ] **Step 3: Idempotent rerun (the guard)**

Run a single table fully, then run it **again**:

```bash
python load_tables.py --tables <NUMERIC_PK_TABLE>
python load_tables.py --tables <NUMERIC_PK_TABLE>
```
Expected on the second run: `<N> existing keys in PK range [..] -> skipping <N>
already-loaded` and `appended 0 rows`. Target `COUNT(*)` unchanged after the second
run.

- [ ] **Step 4: Guard fallback / no-PK table**

If a table has a non-numeric or composite PK (or is absent from specs), confirm the
log shows `no PK guard (pk_cols=...) -> appending all rows` and it still loads.
