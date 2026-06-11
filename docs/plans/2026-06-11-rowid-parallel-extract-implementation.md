# ROWID Parallel Extract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `save_tables.py` read full Oracle tables through ~32 parallel ROWID-range JDBC partitions by default, per `docs/plans/2026-06-11-rowid-parallel-extract-design.md`.

**Architecture:** All code stays in `save_tables.py` (the script is submitted to OCI Data Flow as a single file, matching `scripts/migrate_rowid_to_oci.py`'s self-contained style). Pure helpers (identifier validation, extent merging, predicate building) are unit-tested without Spark; Oracle metadata lookups go through Spark JDBC using the existing `read_single_value` pattern plus a new multi-row `read_rows` helper. `build_jdbc_reader` is replaced by `load_source_dataframe`, which returns a DataFrame and picks the read mode: `--limit` → single partition; partition-column override → existing numeric path; otherwise ROWID predicates; any failure → single-partition fallback.

**Tech Stack:** Python 3.11, PySpark JDBC (Oracle `ojdbc8`), pytest (dev extra), Oracle `DBMS_ROWID` + `DBA/ALL/USER_EXTENTS`.

**Setup before Task 1:** `uv sync --extra dev` (installs pytest). Tests import `save_tables` directly; its top-level imports are stdlib-only, so no pyspark is needed to run unit tests.

---

### Task 1: Identifier validation and ROWID predicate building (pure functions)

**Files:**
- Modify: `save_tables.py` (add `re` import, `IDENTIFIER_PATTERN`, `ROWID_PATTERN`, `validate_identifier`, `build_rowid_predicates`)
- Create: `tests/test_save_tables.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_save_tables.py`:

```python
import pytest

import save_tables

# Realistic 18-character Oracle ROWIDs (base64 alphabet).
ROWID_A = "AAAS5MAAEAAAACXAAA"
ROWID_B = "AAAS5MAAEAAAACX9zz"
ROWID_C = "AAAS5MAAFAAAB2BAAA"
ROWID_D = "AAAS5MAAFAAAB2B9zz"


class TestValidateIdentifier:
    def test_uppercases_valid_identifier(self):
        assert save_tables.validate_identifier("orders") == "ORDERS"

    def test_accepts_oracle_special_characters(self):
        assert save_tables.validate_identifier("TAB_1$#") == "TAB_1$#"

    def test_rejects_injection_attempt(self):
        with pytest.raises(ValueError):
            save_tables.validate_identifier("T; DROP TABLE X")

    def test_rejects_quoted_identifier(self):
        with pytest.raises(ValueError):
            save_tables.validate_identifier('"MixedCase"')


class TestBuildRowidPredicates:
    def test_formats_between_predicates(self):
        predicates = save_tables.build_rowid_predicates(
            [(ROWID_A, ROWID_B), (ROWID_C, ROWID_D)]
        )
        assert predicates == [
            f"ROWID BETWEEN '{ROWID_A}' AND '{ROWID_B}'",
            f"ROWID BETWEEN '{ROWID_C}' AND '{ROWID_D}'",
        ]

    def test_rejects_malformed_rowid(self):
        with pytest.raises(ValueError):
            save_tables.build_rowid_predicates([("not-a-rowid", ROWID_B)])

    def test_empty_chunks_give_empty_predicates(self):
        assert save_tables.build_rowid_predicates([]) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_save_tables.py -v`
Expected: FAIL with `AttributeError: module 'save_tables' has no attribute 'validate_identifier'`

- [ ] **Step 3: Implement the functions**

In `save_tables.py`, add `import re` to the stdlib imports (between `os` and `sys` to keep them sorted), and add after the `REQUIRED_ENV_VARS` constant:

```python
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")
ROWID_PATTERN = re.compile(r"^[A-Za-z0-9/+]{18}$")


def validate_identifier(name: str) -> str:
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Unsupported Oracle identifier: {name!r}")
    return upper


def build_rowid_predicates(chunks: list[tuple[str, str]]) -> list[str]:
    predicates = []
    for start_rowid, end_rowid in chunks:
        for value in (start_rowid, end_rowid):
            if not ROWID_PATTERN.match(str(value)):
                raise ValueError(f"Unexpected ROWID value: {value!r}")
        predicates.append(f"ROWID BETWEEN '{start_rowid}' AND '{end_rowid}'")
    return predicates
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_save_tables.py -v`
Expected: 7 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check save_tables.py tests/test_save_tables.py
git add save_tables.py tests/test_save_tables.py
git commit -m "feat: add rowid predicate helpers for parallel extract"
```

---

### Task 2: Extent-to-chunk merging (pure function)

**Files:**
- Modify: `save_tables.py` (add `math` import and `merge_extents_into_chunks`)
- Test: `tests/test_save_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_save_tables.py`:

```python
def extent(index: int, blocks: int) -> tuple[str, str, int]:
    # Synthetic but pattern-valid 18-char rowids; index keeps them ordered/unique.
    start = f"AAAS5MAAEAAA{index:04d}AA"
    end = f"AAAS5MAAEAAA{index:04d}zz"
    return (start, end, blocks)


class TestMergeExtentsIntoChunks:
    def test_merges_small_extents_to_target_chunk_count(self):
        extents = [extent(i, 10) for i in range(8)]  # 80 blocks total
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=4)
        assert len(chunks) == 4
        # Coverage: first chunk starts at first extent, last chunk ends at last extent.
        assert chunks[0][0] == extents[0][0]
        assert chunks[-1][1] == extents[-1][1]

    def test_chunk_boundaries_follow_extent_order(self):
        extents = [extent(i, 10) for i in range(6)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=3)
        # Each chunk's start must be some extent's start and end some extent's end,
        # and chunks must appear in input order with no overlap or gap.
        starts = [e[0] for e in extents]
        ends = [e[1] for e in extents]
        covered = []
        for chunk_start, chunk_end in chunks:
            covered.append((starts.index(chunk_start), ends.index(chunk_end)))
        flattened = [i for pair in covered for i in range(pair[0], pair[1] + 1)]
        assert flattened == list(range(len(extents)))

    def test_fewer_extents_than_chunks(self):
        extents = [extent(0, 100), extent(1, 100)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=32)
        assert len(chunks) == 2

    def test_single_extent(self):
        extents = [extent(0, 5000)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=32)
        assert chunks == [(extents[0][0], extents[0][1])]

    def test_empty_extents(self):
        assert save_tables.merge_extents_into_chunks([], num_chunks=32) == []

    def test_invalid_chunk_count(self):
        assert save_tables.merge_extents_into_chunks([extent(0, 10)], num_chunks=0) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_save_tables.py -k Merge -v`
Expected: FAIL with `AttributeError: module 'save_tables' has no attribute 'merge_extents_into_chunks'`

- [ ] **Step 3: Implement the function**

In `save_tables.py`, add `import math` to the stdlib imports, and add below `build_rowid_predicates`:

```python
def merge_extents_into_chunks(
    extents: list[tuple[str, str, int]], num_chunks: int
) -> list[tuple[str, str]]:
    """Merge ordered (start_rowid, end_rowid, blocks) extents into ~num_chunks ranges."""
    if not extents or num_chunks <= 0:
        return []
    total_blocks = sum(int(blocks) for _, _, blocks in extents)
    target_blocks = math.ceil(total_blocks / num_chunks)
    chunks: list[tuple[str, str]] = []
    current_start, current_end, current_blocks = None, None, 0
    for start_rowid, end_rowid, blocks in extents:
        blocks = int(blocks)
        if current_start is None:
            current_start, current_end, current_blocks = start_rowid, end_rowid, blocks
        elif current_blocks + blocks <= target_blocks:
            current_end = end_rowid
            current_blocks += blocks
        else:
            chunks.append((current_start, current_end))
            current_start, current_end, current_blocks = start_rowid, end_rowid, blocks
    if current_start is not None:
        chunks.append((current_start, current_end))
    return chunks
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `uv run pytest tests/test_save_tables.py -v`
Expected: 13 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check save_tables.py tests/test_save_tables.py
git add save_tables.py tests/test_save_tables.py
git commit -m "feat: merge oracle extents into balanced rowid chunks"
```

---

### Task 3: Oracle metadata lookups and predicate assembly

**Files:**
- Modify: `save_tables.py` (add `read_rows`, `get_data_object_id`, `fetch_extents`, `fetch_rowid_predicates`)
- Test: `tests/test_save_tables.py`

These functions wrap Spark JDBC, so unit tests stub the query helpers with `monkeypatch` and only assert the orchestration logic in `fetch_rowid_predicates`. The thin JDBC wrappers themselves are exercised by the real-DB validation in Task 6.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_save_tables.py`:

```python
class TestFetchRowidPredicates:
    def test_builds_predicates_from_extents(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: 12345)
        monkeypatch.setattr(
            save_tables,
            "fetch_extents",
            lambda *a: [(ROWID_A, ROWID_B, 64), (ROWID_C, ROWID_D, 64)],
        )
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "admin", "orders", num_partitions=2
        )
        assert predicates == [
            f"ROWID BETWEEN '{ROWID_A}' AND '{ROWID_B}'",
            f"ROWID BETWEEN '{ROWID_C}' AND '{ROWID_D}'",
        ]

    def test_returns_empty_when_object_id_missing(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: None)
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "ADMIN", "ORDERS", num_partitions=4
        )
        assert predicates == []

    def test_returns_empty_when_no_extents(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: 12345)
        monkeypatch.setattr(save_tables, "fetch_extents", lambda *a: [])
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "ADMIN", "ORDERS", num_partitions=4
        )
        assert predicates == []

    def test_rejects_bad_identifier(self):
        with pytest.raises(ValueError):
            save_tables.fetch_rowid_predicates(
                None, {}, "ADMIN", "ORDERS; DROP", num_partitions=4
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_save_tables.py -k Fetch -v`
Expected: FAIL with `AttributeError: module 'save_tables' has no attribute 'fetch_rowid_predicates'`

- [ ] **Step 3: Implement the functions**

In `save_tables.py`, add directly below the existing `read_single_value`:

```python
def read_rows(spark: SparkSession, properties: dict[str, str], query: str) -> list:
    return (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", f"({query}) DATAGEN_Q")
        .load()
        .collect()
    )


def get_data_object_id(
    spark: SparkSession,
    properties: dict[str, str],
    owner: str,
    table_name: str,
) -> int | None:
    queries = (
        f"SELECT dbms_rowid.rowid_object(ROWID) AS DATA_OBJECT_ID "
        f"FROM {owner}.{table_name} WHERE ROWNUM = 1",
        f"SELECT data_object_id FROM all_objects "
        f"WHERE owner = '{owner}' AND object_name = '{table_name}' "
        f"AND object_type = 'TABLE'",
        f"SELECT data_object_id FROM user_objects "
        f"WHERE object_name = '{table_name}' AND object_type = 'TABLE'",
    )
    for query in queries:
        try:
            row = read_single_value(spark, properties, query)
        except Exception as exc:
            logger.debug("Object id lookup failed for %s.%s: %s", owner, table_name, exc)
            continue
        if row and row[0] is not None:
            return int(row[0])
    return None


def fetch_extents(
    spark: SparkSession,
    properties: dict[str, str],
    owner: str,
    table_name: str,
    data_object_id: int,
) -> list[tuple[str, str, int]]:
    attempts = (
        ("dba_extents", f"e.owner = '{owner}' AND "),
        ("all_extents", f"e.owner = '{owner}' AND "),
        ("user_extents", ""),
    )
    for view, owner_filter in attempts:
        query = (
            f"SELECT "
            f"dbms_rowid.rowid_create(1, {int(data_object_id)}, e.relative_fno, "
            f"e.block_id, 0) AS START_ROWID, "
            f"dbms_rowid.rowid_create(1, {int(data_object_id)}, e.relative_fno, "
            f"e.block_id + e.blocks - 1, 32767) AS END_ROWID, "
            f"e.blocks AS BLOCKS "
            f"FROM {view} e "
            f"WHERE {owner_filter}e.segment_name = '{table_name}' "
            f"AND e.segment_type = 'TABLE' "
            f"ORDER BY e.relative_fno, e.block_id"
        )
        try:
            rows = read_rows(spark, properties, query)
        except Exception as exc:
            logger.debug(
                "Extent query via %s failed for %s.%s: %s", view, owner, table_name, exc
            )
            continue
        if rows:
            return [(row[0], row[1], int(row[2])) for row in rows]
    return []


def fetch_rowid_predicates(
    spark: SparkSession,
    properties: dict[str, str],
    owner: str,
    table_name: str,
    num_partitions: int,
) -> list[str]:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    data_object_id = get_data_object_id(spark, properties, owner, table_name)
    if data_object_id is None:
        logger.warning("Could not resolve data object id for %s.%s", owner, table_name)
        return []
    extents = fetch_extents(spark, properties, owner, table_name, data_object_id)
    if not extents:
        logger.warning("No extents found for %s.%s", owner, table_name)
        return []
    chunks = merge_extents_into_chunks(extents, num_partitions)
    return build_rowid_predicates(chunks)
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `uv run pytest tests/test_save_tables.py -v`
Expected: 17 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check save_tables.py tests/test_save_tables.py
git add save_tables.py tests/test_save_tables.py
git commit -m "feat: fetch rowid range predicates via spark jdbc"
```

---

### Task 4: Replace `build_jdbc_reader` with `load_source_dataframe`

**Files:**
- Modify: `save_tables.py:219-267` (replace `build_jdbc_reader`) and `save_tables.py:287-305` (the loop in `save_tables`)

No new unit test: this function needs a live SparkSession; it is covered by the real-DB validation in Task 6. The existing tests guard against regressions in the helpers it calls.

- [ ] **Step 1: Replace `build_jdbc_reader` with `load_source_dataframe`**

Delete the entire `build_jdbc_reader` function and put in its place:

```python
def load_source_dataframe(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    source_user: str,
    table: str,
    source_table: str,
    limit: int | None,
):
    reader = (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", source_table)
        .option("fetchsize", config["DATAGEN_JDBC_FETCH_SIZE"])
    )
    if limit is not None:
        logger.info("Reading %s with one JDBC partition", source_table)
        return reader.load()

    owner, table_name = table_owner_and_name(source_user, table)
    overrides = parse_partition_column_overrides(
        config["DATAGEN_JDBC_PARTITION_COLUMNS"]
    )
    partition_column = overrides.get(f"{owner}.{table_name}") or overrides.get(table_name)

    if partition_column:
        bounds = get_numeric_bounds(spark, properties, source_table, partition_column)
        if bounds:
            lower_bound, upper_bound = bounds
            num_partitions = config["DATAGEN_JDBC_NUM_PARTITIONS"]
            logger.info(
                "Reading %s in %s JDBC partitions on %s [%s, %s]",
                source_table,
                num_partitions,
                partition_column,
                lower_bound,
                upper_bound,
            )
            return (
                reader.option("partitionColumn", partition_column)
                .option("lowerBound", lower_bound)
                .option("upperBound", upper_bound)
                .option("numPartitions", num_partitions)
                .load()
            )
        logger.warning(
            "No bounds found for %s.%s; trying ROWID partitioning",
            source_table,
            partition_column,
        )

    try:
        predicates = fetch_rowid_predicates(
            spark,
            properties,
            owner,
            table_name,
            int(config["DATAGEN_JDBC_NUM_PARTITIONS"]),
        )
    except Exception as exc:
        logger.warning("ROWID partitioning failed for %s: %s", source_table, exc)
        predicates = []

    if predicates:
        logger.info(
            "Reading %s in %d ROWID-range partitions", source_table, len(predicates)
        )
        jdbc_properties = {
            key: value for key, value in properties.items() if key != "url"
        }
        jdbc_properties["fetchsize"] = config["DATAGEN_JDBC_FETCH_SIZE"]
        return spark.read.jdbc(
            url=properties["url"],
            table=source_table,
            predicates=predicates,
            properties=jdbc_properties,
        )

    logger.info("Reading %s with one JDBC partition", source_table)
    return reader.load()
```

Behavior notes locked in by the design:
- `--limit` always reads with one partition (the `FETCH FIRST` subquery has no ROWID). This intentionally supersedes the override path for limited runs.
- A configured override whose bounds query returns nothing now falls through to ROWID partitioning instead of a single partition (strict improvement; logged).
- `spark.read.jdbc(...)` with `predicates` creates one partition per predicate; `fetchsize` rides along in `properties`.

- [ ] **Step 2: Update the `save_tables` loop to use it**

In `save_tables` (currently `save_tables.py:298-305`), replace:

```python
            df = build_jdbc_reader(
                spark=spark,
                properties=properties,
                config=config,
                source_user=source_user,
                table=table,
                source_table=read_table,
            ).load()
```

with:

```python
            df = load_source_dataframe(
                spark=spark,
                properties=properties,
                config=config,
                source_user=source_user,
                table=table,
                source_table=read_table,
                limit=limit,
            )
```

- [ ] **Step 3: Verify the module compiles, tests pass, and lint is clean**

```bash
uv run python -c "import save_tables"
uv run pytest tests/test_save_tables.py -v
uv run ruff check save_tables.py
```
Expected: import succeeds, 17 PASSED, no lint errors. Also confirm `build_jdbc_reader` no longer appears: `grep -n build_jdbc_reader save_tables.py` → no output.

- [ ] **Step 4: Commit**

```bash
git add save_tables.py
git commit -m "feat: default to rowid-range parallel jdbc reads"
```

---

### Task 5: Update README

**Files:**
- Modify: `README.md` ("Fast Raw Table Extract" section)

- [ ] **Step 1: Document the new default**

In `README.md`, replace the paragraph:

```markdown
`save_tables.py` extracts source Oracle tables directly to raw Parquet. It avoids a
pre-write `count()` and reads each table as one snapshot by default.
```

with:

```markdown
`save_tables.py` extracts source Oracle tables directly to raw Parquet. It avoids a
pre-write `count()`. Full-table reads are parallelized across
`DATAGEN_JDBC_NUM_PARTITIONS` (default 32) ROWID-range JDBC partitions computed from
the table's extent map; no numeric partition column is required. Set
`DATAGEN_JDBC_PARTITION_COLUMNS="OWNER.TABLE=COLUMN"` to use numeric-column
partitioning for specific tables instead. If extent metadata is unavailable (missing
privileges, empty table, or a view), the read falls back to a single JDBC partition.
Note: parallel partitions read in separate Oracle sessions, so the extract is not a
single consistent snapshot if the source changes mid-run.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: describe rowid parallel extract default"
```

---

### Task 6: Real-DB validation (run on the Data Flow / VDI environment)

**Files:** none (operational verification)

This task runs where the Oracle source is reachable (the environment with `DATAGEN_SOURCE_JDBC_URL` etc. configured) — it cannot run on a dev laptop without DB access.

- [ ] **Step 1: Pick a mid-size table and capture the current baseline**

Use `scripts/oracle_table_sizes.py --tables <TABLE>` to confirm size, then run the
extract and note the log lines and elapsed time:

```bash
python save_tables.py --tables <TABLE>
```

Expected log: `Reading ADMIN.<TABLE> in N ROWID-range partitions` (N ≤ 32), then
`Saved ADMIN.<TABLE> in <seconds>s`.

- [ ] **Step 2: Verify correctness against the source**

Compare the Parquet row count with the source count:

```python
# pyspark shell or notebook in the same environment
df = spark.read.parquet("<DATAGEN_RAW_BASE_URI>/<prefix>/<TABLE>")
print(df.count())
```

versus `SELECT COUNT(*) FROM ADMIN.<TABLE>` on Oracle. Counts must match (assuming a
quiesced source).

- [ ] **Step 3: Confirm the fallback path still works**

Run against a table the user lacks extent privileges for, or an empty table, and
confirm the log shows the warning followed by
`Reading ... with one JDBC partition` and a successful save.

- [ ] **Step 4: Confirm the limit path is unchanged**

```bash
python save_tables.py --tables <TABLE> --limit 100000
```

Expected log: `Reading up to 100000 rows from ADMIN.<TABLE>` followed by
`Reading ... with one JDBC partition`, with output written to `<TABLE>_limit_100000`.
