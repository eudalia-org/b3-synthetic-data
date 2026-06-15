# Parallel Load Implementation Plan (load_tables.py)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build standalone `load_tables.py` that loads per-table Parquet into target Oracle through many short-lived parallel JDBC partitions, managing FK constraints in-script, per `docs/plans/2026-06-12-parallel-load-design.md`.

**Architecture:** All code in `load_tables.py` (self-contained, submitted to OCI Data Flow as one file, like `save_tables.py`). Pure helpers (CLI/env parsing, path & dbtable building, connection properties, SQL builders, the constraint-disable context manager) are unit-tested without Spark. DDL (`TRUNCATE`, `ALTER ... DISABLE/ENABLE CONSTRAINT`) runs via the JVM JDBC `DriverManager` on the driver; constraint discovery reuses a Spark `read_rows` SELECT. Overwrite = explicit truncate + `mode("append")`.

**Tech Stack:** Python 3.11, PySpark JDBC (Oracle `ojdbc8`), pytest (run via `uv run --no-project --with pytest python -m pytest`, since `uv sync` is broken on this repo), Oracle `all_constraints`.

**Setup:** Tests import `load_tables` directly; its top-level imports must stay stdlib-only (`SparkSession` under `TYPE_CHECKING`) so pytest needs no pyspark.

---

### Task 1: Module scaffold — constants and `validate_identifier`

**Files:**
- Create: `load_tables.py`
- Create: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_load_tables.py`:

```python
import pytest

import load_tables


class TestValidateIdentifier:
    def test_uppercases_valid_identifier(self):
        assert load_tables.validate_identifier("orders") == "ORDERS"

    def test_accepts_oracle_special_characters(self):
        assert load_tables.validate_identifier("TAB_1$#") == "TAB_1$#"

    def test_rejects_injection_attempt(self):
        with pytest.raises(ValueError):
            load_tables.validate_identifier("T; DROP TABLE X")

    def test_rejects_quoted_identifier(self):
        with pytest.raises(ValueError):
            load_tables.validate_identifier('"MixedCase"')
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'load_tables'`

- [ ] **Step 3: Write the scaffold**

Create `load_tables.py`:

```python
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_TARGET_DB_USER = "ADMIN"
DEFAULT_NUM_PARTITIONS = "256"
DEFAULT_BATCH_SIZE = "10000"
DEFAULT_READ_TIMEOUT_MS = "600000"
DEFAULT_ISOLATION_LEVEL = "READ_COMMITTED"
PARQUET_REBASE_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
}
REQUIRED_ENV_VARS = (
    "DATAGEN_TARGET_JDBC_URL",
    "DATAGEN_TARGET_DB_PASSWORD",
    "DATAGEN_LOAD_BASE_URI",
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def validate_identifier(name: str) -> str:
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Unsupported Oracle identifier: {name!r}")
    return upper
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -v`
Expected: 4 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: scaffold load_tables with identifier validation"
```

---

### Task 2: CLI argument parsing and table-list parsing

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestParseTables:
    def test_parses_comma_list(self):
        assert load_tables.parse_tables("A,B , C", None) == ["A", "B", "C"]

    def test_dedupes_preserving_order(self):
        assert load_tables.parse_tables("A,B,A", None) == ["A", "B"]

    def test_reads_file_ignoring_blanks_and_comments(self, tmp_path):
        f = tmp_path / "tables.txt"
        f.write_text("ORDERS\n# comment\n\nCUSTOMERS\n")
        assert load_tables.parse_tables(None, str(f)) == ["ORDERS", "CUSTOMERS"]

    def test_exits_when_empty(self):
        with pytest.raises(SystemExit):
            load_tables.parse_tables("", None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ParseTables -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'parse_tables'`

- [ ] **Step 3: Implement parsing**

Add to `load_tables.py`:

```python
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load per-table Parquet into target Oracle with parallel JDBC writes."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--tables",
        help="Comma-separated table list, for example CUSTOMERS,ORDERS,ORDER_ITEMS.",
    )
    source.add_argument(
        "--tables-file",
        help="Local text file with one table per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Try remaining tables after a failure, then exit non-zero if any failed.",
    )
    parser.add_argument(
        "--no-manage-constraints",
        action="store_true",
        help="Do not disable/re-enable foreign keys; assume constraints are handled externally.",
    )
    parser.add_argument(
        "--validate-constraints",
        action="store_true",
        help="Re-enable foreign keys with ENABLE VALIDATE instead of ENABLE NOVALIDATE.",
    )
    return parser.parse_args()


def parse_tables(tables: str | None, tables_file: str | None) -> list[str]:
    if tables:
        parsed = [table.strip() for table in tables.split(",")]
    else:
        path = Path(tables_file or "")
        try:
            lines = path.read_text().splitlines()
        except OSError as exc:
            logger.error("Failed to read table list %s: %s", path, exc)
            sys.exit(1)
        parsed = []
        for line in lines:
            table = line.strip()
            if table and not table.startswith("#"):
                parsed.append(table)

    deduped = list(dict.fromkeys(table for table in parsed if table))
    if not deduped:
        logger.error("No tables provided")
        sys.exit(1)
    return deduped
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ParseTables -v`
Expected: 4 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: parse load_tables cli arguments and table list"
```

---

### Task 3: Environment config and Spark session

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestGetLoadEnv:
    BASE = {
        "DATAGEN_TARGET_JDBC_URL": "jdbc:oracle:thin:@host",
        "DATAGEN_TARGET_DB_PASSWORD": "secret",
        "DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load/",
    }

    def test_applies_defaults(self, monkeypatch):
        for key in list(os.environ):
            if key.startswith("DATAGEN_"):
                monkeypatch.delenv(key, raising=False)
        for key, value in self.BASE.items():
            monkeypatch.setenv(key, value)
        config = load_tables.get_load_env()
        assert config["DATAGEN_TARGET_DB_USER"] == "ADMIN"
        assert config["DATAGEN_JDBC_NUM_PARTITIONS"] == "256"
        assert config["DATAGEN_JDBC_BATCH_SIZE"] == "10000"
        assert config["DATAGEN_JDBC_READ_TIMEOUT_MS"] == "600000"
        assert config["DATAGEN_LOAD_PREFIX"] == ""
        assert config["DATAGEN_TARGET_JDBC_URL"] == "jdbc:oracle:thin:@host"

    def test_strips_trailing_slash_and_prefix_slashes(self, monkeypatch):
        for key, value in self.BASE.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("DATAGEN_LOAD_PREFIX", "/synthetic/")
        config = load_tables.get_load_env()
        assert config["DATAGEN_LOAD_BASE_URI"] == "oci://bucket@ns/load"
        assert config["DATAGEN_LOAD_PREFIX"] == "synthetic"

    def test_exits_when_required_missing(self, monkeypatch):
        for key in REQUIRED:
            monkeypatch.delenv(key, raising=False)
        with pytest.raises(SystemExit):
            load_tables.get_load_env()


REQUIRED = (
    "DATAGEN_TARGET_JDBC_URL",
    "DATAGEN_TARGET_DB_PASSWORD",
    "DATAGEN_LOAD_BASE_URI",
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k GetLoadEnv -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'get_load_env'`

- [ ] **Step 3: Implement env config and Spark session**

Add to `load_tables.py`:

```python
def get_load_env() -> dict[str, str]:
    config = {}
    missing = []

    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")

    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        sys.exit(1)

    config["DATAGEN_TARGET_DB_USER"] = os.environ.get(
        "DATAGEN_TARGET_DB_USER", DEFAULT_TARGET_DB_USER
    )
    config["DATAGEN_LOAD_PREFIX"] = os.environ.get("DATAGEN_LOAD_PREFIX", "").strip("/")
    config["DATAGEN_JDBC_NUM_PARTITIONS"] = os.environ.get(
        "DATAGEN_JDBC_NUM_PARTITIONS", DEFAULT_NUM_PARTITIONS
    )
    config["DATAGEN_JDBC_BATCH_SIZE"] = os.environ.get(
        "DATAGEN_JDBC_BATCH_SIZE", DEFAULT_BATCH_SIZE
    )
    config["DATAGEN_JDBC_READ_TIMEOUT_MS"] = os.environ.get(
        "DATAGEN_JDBC_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS
    )
    return config


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in PARQUET_REBASE_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k GetLoadEnv -v`
Expected: 3 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: resolve load_tables environment config"
```

---

### Task 4: Path, owner and dbtable helpers

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestNameAndPathHelpers:
    def test_table_path_name_strips_schema(self):
        assert load_tables.table_path_name("CETIP.LANCAMENTO") == "LANCAMENTO"
        assert load_tables.table_path_name("ORDERS") == "ORDERS"

    def test_table_owner_and_name_with_schema(self):
        assert load_tables.table_owner_and_name("ADMIN", "cetip.lancamento") == (
            "CETIP",
            "LANCAMENTO",
        )

    def test_table_owner_and_name_defaults_to_user(self):
        assert load_tables.table_owner_and_name("admin", "orders") == ("ADMIN", "ORDERS")

    def test_dbtable_name_qualifies_unqualified(self):
        assert load_tables.dbtable_name("ADMIN", "ORDERS") == "ADMIN.ORDERS"
        assert load_tables.dbtable_name("ADMIN", "CETIP.X") == "CETIP.X"

    def test_build_load_path_with_prefix(self):
        config = {
            "DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load",
            "DATAGEN_LOAD_PREFIX": "synthetic",
        }
        assert (
            load_tables.build_load_path(config, "ORDERS")
            == "oci://bucket@ns/load/synthetic/ORDERS"
        )

    def test_build_load_path_without_prefix(self):
        config = {"DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load", "DATAGEN_LOAD_PREFIX": ""}
        assert load_tables.build_load_path(config, "ORDERS") == "oci://bucket@ns/load/ORDERS"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k NameAndPath -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'table_path_name'`

- [ ] **Step 3: Implement helpers**

Add to `load_tables.py`:

```python
def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def table_owner_and_name(target_user: str, table: str) -> tuple[str, str]:
    if "." in table:
        owner, table_name = table.split(".", 1)
        return owner.upper(), table_name.upper()
    return target_user.upper(), table.upper()


def dbtable_name(target_user: str, table: str) -> str:
    return table if "." in table else f"{target_user}.{table}"


def build_load_path(config: dict[str, str], table: str) -> str:
    path_parts = [config["DATAGEN_LOAD_BASE_URI"]]
    if config["DATAGEN_LOAD_PREFIX"]:
        path_parts.append(config["DATAGEN_LOAD_PREFIX"])
    path_parts.append(table)
    return "/".join(path_parts)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k NameAndPath -v`
Expected: 6 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add load path and dbtable helpers"
```

---

### Task 5: Connection properties and partition count

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestConnectionProperties:
    CONFIG = {
        "DATAGEN_TARGET_JDBC_URL": "jdbc:oracle:thin:@host",
        "DATAGEN_TARGET_DB_USER": "ADMIN",
        "DATAGEN_TARGET_DB_PASSWORD": "secret",
        "DATAGEN_JDBC_READ_TIMEOUT_MS": "600000",
        "DATAGEN_JDBC_NUM_PARTITIONS": "256",
    }

    def test_base_connection_properties(self):
        props = load_tables.build_connection_properties(self.CONFIG)
        assert props["url"] == "jdbc:oracle:thin:@host"
        assert props["user"] == "ADMIN"
        assert props["password"] == "secret"
        assert props["driver"] == "oracle.jdbc.OracleDriver"
        assert props["oracle.jdbc.ReadTimeout"] == "600000"

    def test_omits_write_only_options(self):
        # batchsize / isolationLevel are applied at the write call, not in the
        # base properties (which are also reused for metadata SELECTs).
        props = load_tables.build_connection_properties(self.CONFIG)
        assert "batchsize" not in props
        assert "isolationLevel" not in props

    def test_resolve_num_partitions(self):
        assert load_tables.resolve_num_partitions(self.CONFIG) == 256
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ConnectionProperties -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'build_connection_properties'`

- [ ] **Step 3: Implement**

Add to `load_tables.py`:

```python
def build_connection_properties(config: dict[str, str]) -> dict[str, str]:
    return {
        "url": config["DATAGEN_TARGET_JDBC_URL"],
        "user": config["DATAGEN_TARGET_DB_USER"],
        "password": config["DATAGEN_TARGET_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
        "oracle.jdbc.ReadTimeout": config["DATAGEN_JDBC_READ_TIMEOUT_MS"],
    }


def resolve_num_partitions(config: dict[str, str]) -> int:
    return int(config["DATAGEN_JDBC_NUM_PARTITIONS"])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ConnectionProperties -v`
Expected: 3 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: build load connection properties and partition count"
```

---

### Task 6: SQL builders (discovery, truncate, disable/enable)

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestSqlBuilders:
    def test_truncate_sql(self):
        assert load_tables.truncate_sql("admin", "orders") == "TRUNCATE TABLE ADMIN.ORDERS"

    def test_disable_constraint_sql(self):
        assert (
            load_tables.disable_constraint_sql("ADMIN", "ORDERS", "FK_CUST")
            == "ALTER TABLE ADMIN.ORDERS DISABLE CONSTRAINT FK_CUST"
        )

    def test_enable_constraint_sql_novalidate(self):
        assert (
            load_tables.enable_constraint_sql("ADMIN", "ORDERS", "FK_CUST", validate=False)
            == "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST"
        )

    def test_enable_constraint_sql_validate(self):
        assert (
            load_tables.enable_constraint_sql("ADMIN", "ORDERS", "FK_CUST", validate=True)
            == "ALTER TABLE ADMIN.ORDERS ENABLE VALIDATE CONSTRAINT FK_CUST"
        )

    def test_discovery_query_includes_incoming_and_outgoing(self):
        query = load_tables.build_constraint_discovery_query("ADMIN", "ORDERS")
        assert "all_constraints" in query
        assert "p.owner = 'ADMIN' AND p.table_name = 'ORDERS'" in query  # incoming
        assert "owner = 'ADMIN' AND table_name = 'ORDERS'" in query      # outgoing
        assert "UNION" in query

    def test_builders_reject_bad_identifiers(self):
        with pytest.raises(ValueError):
            load_tables.truncate_sql("ADMIN", "ORDERS; DROP")
        with pytest.raises(ValueError):
            load_tables.disable_constraint_sql("ADMIN", "ORDERS", "X'); DROP")
        with pytest.raises(ValueError):
            load_tables.build_constraint_discovery_query("ADMIN", "O'R")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k SqlBuilders -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'truncate_sql'`

- [ ] **Step 3: Implement the SQL builders**

Add to `load_tables.py`:

```python
def truncate_sql(owner: str, table_name: str) -> str:
    return f"TRUNCATE TABLE {validate_identifier(owner)}.{validate_identifier(table_name)}"


def disable_constraint_sql(owner: str, table_name: str, name: str) -> str:
    return (
        f"ALTER TABLE {validate_identifier(owner)}.{validate_identifier(table_name)} "
        f"DISABLE CONSTRAINT {validate_identifier(name)}"
    )


def enable_constraint_sql(owner: str, table_name: str, name: str, validate: bool) -> str:
    mode = "ENABLE VALIDATE" if validate else "ENABLE NOVALIDATE"
    return (
        f"ALTER TABLE {validate_identifier(owner)}.{validate_identifier(table_name)} "
        f"{mode} CONSTRAINT {validate_identifier(name)}"
    )


def build_constraint_discovery_query(owner: str, table_name: str) -> str:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    return (
        "SELECT c.owner, c.table_name, c.constraint_name "
        "FROM all_constraints c "
        "JOIN all_constraints p "
        "ON c.r_owner = p.owner AND c.r_constraint_name = p.constraint_name "
        "WHERE c.constraint_type = 'R' AND c.status = 'ENABLED' "
        f"AND p.owner = '{owner}' AND p.table_name = '{table_name}' "
        "UNION "
        "SELECT owner, table_name, constraint_name "
        "FROM all_constraints "
        "WHERE constraint_type = 'R' AND status = 'ENABLED' "
        f"AND owner = '{owner}' AND table_name = '{table_name}'"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k SqlBuilders -v`
Expected: 6 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add load_tables ddl sql builders"
```

---

### Task 7: `constraints_disabled` context manager (the retry-safety property)

**Files:**
- Modify: `load_tables.py` (add `from contextlib import contextmanager` to imports)
- Test: `tests/test_load_tables.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_tables.py`:

```python
class TestConstraintsDisabled:
    CONSTRAINTS = [("ADMIN", "ORDERS", "FK_CUST"), ("SALES", "INVOICES", "FK_ORD")]

    def test_disables_then_reenables_in_order(self):
        calls = []
        with load_tables.constraints_disabled(calls.append, self.CONSTRAINTS, validate=False):
            calls.append("BODY")
        assert calls == [
            "ALTER TABLE ADMIN.ORDERS DISABLE CONSTRAINT FK_CUST",
            "ALTER TABLE SALES.INVOICES DISABLE CONSTRAINT FK_ORD",
            "BODY",
            "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST",
            "ALTER TABLE SALES.INVOICES ENABLE NOVALIDATE CONSTRAINT FK_ORD",
        ]

    def test_reenables_even_when_body_raises(self):
        calls = []
        with pytest.raises(RuntimeError):
            with load_tables.constraints_disabled(calls.append, self.CONSTRAINTS, validate=False):
                raise RuntimeError("load failed")
        # Both disabled constraints must still be re-enabled.
        assert "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST" in calls
        assert "ALTER TABLE SALES.INVOICES ENABLE NOVALIDATE CONSTRAINT FK_ORD" in calls

    def test_empty_constraints_is_noop(self):
        calls = []
        with load_tables.constraints_disabled(calls.append, [], validate=False):
            calls.append("BODY")
        assert calls == ["BODY"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ConstraintsDisabled -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'constraints_disabled'`

- [ ] **Step 3: Implement**

Add `from contextlib import contextmanager` to the imports block (after `import argparse`), and add:

```python
@contextmanager
def constraints_disabled(execute, constraints: list[tuple[str, str, str]], validate: bool):
    for owner, table_name, name in constraints:
        execute(disable_constraint_sql(owner, table_name, name))
    try:
        yield
    finally:
        for owner, table_name, name in constraints:
            execute(enable_constraint_sql(owner, table_name, name, validate))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k ConstraintsDisabled -v`
Expected: 3 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add constraints_disabled context manager"
```

---

### Task 8: Spark glue — `read_rows`, `execute_statement`, `discover_constraints`

**Files:**
- Modify: `load_tables.py`
- Test: `tests/test_load_tables.py`

`read_rows` and `execute_statement` need a live SparkSession/JVM and are covered by the real-DB validation in Task 11. `discover_constraints` is unit-tested by stubbing `read_rows`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_load_tables.py`:

```python
class TestDiscoverConstraints:
    def test_maps_rows_to_tuples(self, monkeypatch):
        monkeypatch.setattr(
            load_tables,
            "read_rows",
            lambda spark, props, query: [
                ("ADMIN", "ORDERS", "FK_CUST"),
                ("SALES", "INVOICES", "FK_ORD"),
            ],
        )
        result = load_tables.discover_constraints(None, {}, "ADMIN", "ORDERS")
        assert result == [
            ("ADMIN", "ORDERS", "FK_CUST"),
            ("SALES", "INVOICES", "FK_ORD"),
        ]

    def test_empty_when_no_constraints(self, monkeypatch):
        monkeypatch.setattr(load_tables, "read_rows", lambda *a: [])
        assert load_tables.discover_constraints(None, {}, "ADMIN", "ORDERS") == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k DiscoverConstraints -v`
Expected: FAIL with `AttributeError: module 'load_tables' has no attribute 'discover_constraints'`

- [ ] **Step 3: Implement**

Add to `load_tables.py`:

```python
def read_rows(spark: SparkSession, properties: dict[str, str], query: str) -> list:
    return (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", f"({query}) DATAGEN_Q")
        .load()
        .collect()
    )


def execute_statement(spark: SparkSession, properties: dict[str, str], sql: str) -> None:
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
        properties["url"], properties["user"], properties["password"]
    )
    try:
        stmt = conn.prepareStatement(sql)
        try:
            stmt.execute()
        finally:
            stmt.close()
    finally:
        conn.close()


def discover_constraints(
    spark: SparkSession, properties: dict[str, str], owner: str, table_name: str
) -> list[tuple[str, str, str]]:
    query = build_constraint_discovery_query(owner, table_name)
    rows = read_rows(spark, properties, query)
    return [(row[0], row[1], row[2]) for row in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -k DiscoverConstraints -v`
Expected: 2 PASSED

- [ ] **Step 5: Lint and commit**

```bash
uv run --no-project --with ruff ruff check load_tables.py tests/test_load_tables.py
git add load_tables.py tests/test_load_tables.py
git commit -m "feat: add spark jdbc glue and constraint discovery"
```

---

### Task 9: `load_table`, `load_tables` loop, and `main`

**Files:**
- Modify: `load_tables.py`

No new unit test: these require a live SparkSession; covered by import/compile check here and real-DB validation in Task 11. The pure helpers they call are already tested.

- [ ] **Step 1: Implement `load_table`**

Add to `load_tables.py`:

```python
def load_table(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    target_user: str,
    table: str,
    manage_constraints: bool,
    validate: bool,
) -> None:
    owner, table_name = table_owner_and_name(target_user, table)
    dbtable = dbtable_name(target_user, table)
    input_path = build_load_path(config, table_path_name(table))
    num_partitions = resolve_num_partitions(config)
    batch_size = config["DATAGEN_JDBC_BATCH_SIZE"]

    def execute(sql: str) -> None:
        execute_statement(spark, properties, sql)

    constraints: list[tuple[str, str, str]] = []
    if manage_constraints:
        constraints = discover_constraints(spark, properties, owner, table_name)
        logger.info("Disabling %d FK constraint(s) for %s", len(constraints), dbtable)

    with constraints_disabled(execute, constraints, validate):
        logger.info("Truncating %s", dbtable)
        execute(truncate_sql(owner, table_name))

        df = spark.read.parquet(input_path).repartition(num_partitions)
        logger.info("Writing %s in %d partitions", dbtable, num_partitions)
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

- [ ] **Step 2: Implement the `load_tables` loop**

Add to `load_tables.py`:

```python
def load_tables(
    spark: SparkSession,
    config: dict[str, str],
    tables: list[str],
    continue_on_error: bool,
    manage_constraints: bool,
    validate: bool,
) -> None:
    target_user = config["DATAGEN_TARGET_DB_USER"]
    properties = build_connection_properties(config)
    failures = []
    total = len(tables)
    run_started_at = time.perf_counter()
    logger.info(
        "Loading %d table(s): num_partitions=%s, batchsize=%s, manage_constraints=%s",
        total,
        config["DATAGEN_JDBC_NUM_PARTITIONS"],
        config["DATAGEN_JDBC_BATCH_SIZE"],
        manage_constraints,
    )

    for index, table in enumerate(tables, start=1):
        try:
            started_at = time.perf_counter()
            logger.info("[%d/%d] Loading %s", index, total, table)
            load_table(
                spark=spark,
                properties=properties,
                config=config,
                target_user=target_user,
                table=table,
                manage_constraints=manage_constraints,
                validate=validate,
            )
            logger.info(
                "[%d/%d] Loaded %s in %.1fs",
                index,
                total,
                table,
                time.perf_counter() - started_at,
            )
        except Exception as exc:
            logger.exception("[%d/%d] Failed to load %s: %s", index, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise

    run_elapsed = time.perf_counter() - run_started_at
    logger.info(
        "Finished: %d/%d table(s) loaded in %.1fs", total - len(failures), total, run_elapsed
    )
    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)
```

- [ ] **Step 3: Implement `main`**

Add to `load_tables.py`:

```python
def main() -> None:
    args = parse_arguments()
    tables = parse_tables(args.tables, args.tables_file)
    config = get_load_env()
    spark = create_spark_session("DataGenLoadTables")
    try:
        load_tables(
            spark,
            config,
            tables,
            continue_on_error=args.continue_on_error,
            manage_constraints=not args.no_manage_constraints,
            validate=args.validate_constraints,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Verify compile, full test suite, and lint**

```bash
uv run --no-project python -c "import load_tables"
uv run --no-project --with pytest python -m pytest tests/test_load_tables.py -v
uv run --no-project --with ruff ruff check load_tables.py
```
Expected: import succeeds, all tests PASS (31), no lint errors.

- [ ] **Step 5: Commit**

```bash
git add load_tables.py
git commit -m "feat: wire load_table orchestration and main entrypoint"
```

---

### Task 10: README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document load_tables.py**

In `README.md`, after the "Fast Raw Table Extract" section, add:

```markdown
## Fast Parallel Load

`load_tables.py` is the inverse of `save_tables.py`: it loads per-table Parquet into
the target Oracle database through many short-lived parallel JDBC partitions, so a
load survives the Data Flow→ADB connection killer (each partition commits in seconds,
and Spark retries any killed partition cleanly). Run one Data Flow job per big table.

```bash
python load_tables.py --tables BIG_TABLE
```

Reads `{DATAGEN_LOAD_BASE_URI}/{DATAGEN_LOAD_PREFIX}/<TABLE>` and overwrites the target
table (`TRUNCATE` then parallel append), so reruns are idempotent. Point
`DATAGEN_LOAD_BASE_URI` at the raw bucket for an Oracle→Oracle copy or at synthetic
output.

Foreign keys are managed automatically: enabled FKs referencing each target table
(plus the target's own FKs) are disabled before truncate and re-enabled `NOVALIDATE`
after (use `--validate-constraints` for `ENABLE VALIDATE`). Pass
`--no-manage-constraints` to skip this when constraints are handled externally;
cross-schema constraints need `ALTER` privileges in the referencing schema.

Configuration: `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`,
`DATAGEN_TARGET_DB_USER` (default `ADMIN`), `DATAGEN_LOAD_BASE_URI`,
`DATAGEN_LOAD_PREFIX`, `DATAGEN_JDBC_NUM_PARTITIONS` (default 256),
`DATAGEN_JDBC_BATCH_SIZE` (default 10000), `DATAGEN_JDBC_READ_TIMEOUT_MS`
(default 600000). Set `spark.task.maxFailures` high (e.g. 8) in the Data Flow job so
killed partitions are retried.

Note: parallel JDBC append is at-least-once — a partition that commits but is then
reported failed will be retried and duplicate that partition's rows. The per-run
truncate bounds this to a single run.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: describe parallel load script"
```

---

### Task 11: Real-DB validation (run where target Oracle is reachable)

**Files:** none (operational verification)

- [ ] **Step 1: Load a mid-size table and capture timing**

Set `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`, `DATAGEN_LOAD_BASE_URI`
(pointing at existing per-table Parquet), then:

```bash
python load_tables.py --tables <TABLE>
```

Expected log: `Disabling N FK constraint(s)`, `Truncating ADMIN.<TABLE>`,
`Writing ADMIN.<TABLE> in 256 partitions`, `[1/1] Loaded ADMIN.<TABLE> in <s>`.

- [ ] **Step 2: Verify row count matches the source Parquet**

Compare target count to the Parquet row count:

```python
spark.read.parquet("<DATAGEN_LOAD_BASE_URI>/<prefix>/<TABLE>").count()
```
versus `SELECT COUNT(*) FROM ADMIN.<TABLE>` on Oracle. They must match (modulo the
documented at-least-once duplicate edge case).

- [ ] **Step 3: Confirm constraints returned to ENABLED**

```sql
SELECT constraint_name, status FROM all_constraints
WHERE constraint_type = 'R' AND (
  (owner = 'ADMIN' AND table_name = '<TABLE>') OR
  (r_owner, r_constraint_name) IN (
    SELECT owner, constraint_name FROM all_constraints
    WHERE owner = 'ADMIN' AND table_name = '<TABLE>'));
```
All must show `ENABLED`.

- [ ] **Step 4: Confirm re-enable survives a failed load**

Temporarily point at a non-existent input path so the write fails, run with a single
table, and confirm the run exits non-zero but the FKs from Step 3 are still `ENABLED`
(the `finally` re-enable fired). Then restore the path.
