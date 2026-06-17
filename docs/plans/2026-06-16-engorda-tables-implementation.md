# Engorda Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `engorda_tables.py`, a self-contained OCI Data Flow app that reads raw Parquet (written by `save_tables.py`), generates synthetic relational data preserving PK/FK integrity, and writes synthetic Parquet — processing FK-connected table groups one at a time to bound memory.

**Architecture:** A single file. The proven multi-table synthesizer from `transform/transform.py` is vendored (copied) into `engorda_tables.py`, trimmed to its Parquet-only paths, with one patch so `run_synthesis_from_tables` honours `save_mode`. A thin entrypoint layer on top parses CLI/env, loads a prebuilt `specs.json`, splits the spec graph into FK-connected components, and synthesizes each component then releases memory before the next. The entrypoint logic is written as pure, Spark-free functions (testable with plain pytest) plus one optional local-Spark integration test.

**Tech Stack:** Python 3.11+, PySpark, pytest. No new dependencies (pandas/LLM/OCI-auth are explicitly NOT bundled).

**Spec:** `docs/plans/2026-06-16-engorda-tables-design.md`

---

## File Structure

- **Create `engorda_tables.py`** — the whole app. Two regions:
  1. *Vendored synthesizer* (trimmed copy of `transform/transform.py`).
  2. *Entrypoint* — `parse_arguments`, `get_engorda_env`, `table_path_name`, `raw_path`,
     `synthetic_base_path`, `normalize_specs`, `load_specs`, `connected_components`,
     `effective_n_rows`, `read_parquet`, `release`, `engorda`, `main`.
- **Create `tests/test_engorda_tables.py`** — pure-unit tests for the entrypoint helpers, plus
  one Spark integration test (skipped when `pyspark` is unimportable).

The entrypoint helpers are deliberately Spark-free (they take plain dicts / counts), mirroring the
pure-unit style of `tests/test_save_tables.py`. Only `read_parquet`, `load_specs`, and `engorda`
touch Spark.

---

## Conventions for every task

- TDD: write the failing test, watch it fail, write minimal code, watch it pass, commit.
- Run a single test with: `uv run pytest tests/test_engorda_tables.py::ClassName::test_name -v`
- Run the whole file with: `uv run pytest tests/test_engorda_tables.py -v`
- Commit messages use the repo's `feat:` / `test:` / `chore:` prefixes and end with the
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.
- Work happens on the current `engorda-tables-design` branch.

---

## Task 1: Scaffold the module and test file

**Files:**
- Create: `engorda_tables.py`
- Create: `tests/test_engorda_tables.py`

- [ ] **Step 1: Create the module skeleton**

Create `engorda_tables.py` with imports, logging, and constants only (no synthesizer yet):

```python
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

REQUIRED_ENV_VARS = (
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
    "DATAGEN_SPECS_URI",
)
DEFAULT_SCALE_FACTOR = 1.0
DEFAULT_SEED = 42
```

- [ ] **Step 2: Create the test file with an import smoke test**

The test file's import block must include everything later tasks use (`json` for TestLoadSpecs,
`sys` for TestParseArguments, `pytest` for `raises`/`importorskip`):

```python
import json
import sys

import pytest

import engorda_tables


def test_module_imports():
    assert engorda_tables.REQUIRED_ENV_VARS == (
        "DATAGEN_RAW_BASE_URI",
        "DATAGEN_SYNTHETIC_BASE_URI",
        "DATAGEN_SPECS_URI",
    )
```

- [ ] **Step 3: Run it and confirm it passes**

Run: `uv run pytest tests/test_engorda_tables.py::test_module_imports -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "chore: scaffold engorda_tables module and tests"
```

---

## Task 2: `table_path_name` and path builders

These mirror `save_tables.py` so engorda reads/writes the exact directories ingest used.

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestPaths:
    CONFIG = {
        "DATAGEN_RAW_BASE_URI": "oci://raw@ns",
        "DATAGEN_RAW_PREFIX": "datagen/raw",
        "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns",
        "DATAGEN_SYNTHETIC_PREFIX": "",
    }

    def test_table_path_name_strips_schema(self):
        assert engorda_tables.table_path_name("ADMIN.ORDERS") == "ORDERS"
        assert engorda_tables.table_path_name("ORDERS") == "ORDERS"

    def test_raw_path_with_prefix(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_raw_path_reduces_dotted_name(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ADMIN.ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_synthetic_base_without_prefix(self):
        assert engorda_tables.synthetic_base_path(self.CONFIG) == "oci://syn@ns"

    def test_synthetic_base_with_prefix(self):
        cfg = dict(self.CONFIG, DATAGEN_SYNTHETIC_PREFIX="datagen/synthetic")
        assert (
            engorda_tables.synthetic_base_path(cfg) == "oci://syn@ns/datagen/synthetic"
        )
```

- [ ] **Step 2: Run and confirm failure** — Run: `uv run pytest tests/test_engorda_tables.py::TestPaths -v` → FAIL (functions not defined).

- [ ] **Step 3: Implement**

```python
def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def raw_path(config: dict[str, str], table: str) -> str:
    parts = [config["DATAGEN_RAW_BASE_URI"]]
    if config.get("DATAGEN_RAW_PREFIX"):
        parts.append(config["DATAGEN_RAW_PREFIX"])
    parts.append(table_path_name(table))
    return "/".join(parts)


def synthetic_base_path(config: dict[str, str]) -> str:
    base = config["DATAGEN_SYNTHETIC_BASE_URI"]
    prefix = config.get("DATAGEN_SYNTHETIC_PREFIX")
    return f"{base}/{prefix}" if prefix else base
```

Note: `get_engorda_env` (Task 3) is responsible for `rstrip("/")`-ing the base URIs and
`strip("/")`-ing the prefixes, so these builders assume already-normalized values.

- [ ] **Step 4: Run and confirm pass** — Run: `uv run pytest tests/test_engorda_tables.py::TestPaths -v` → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: add engorda path builders matching save_tables convention"
```

---

## Task 3: `get_engorda_env`

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestGetEngordaEnv:
    def test_reads_required_and_normalizes(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_RAW_BASE_URI", "oci://raw@ns/")
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://syn@ns/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://cfg@ns/specs.json")
        monkeypatch.setenv("DATAGEN_RAW_PREFIX", "/datagen/raw/")
        monkeypatch.delenv("DATAGEN_SYNTHETIC_PREFIX", raising=False)
        config = engorda_tables.get_engorda_env()
        assert config["DATAGEN_RAW_BASE_URI"] == "oci://raw@ns"
        assert config["DATAGEN_RAW_PREFIX"] == "datagen/raw"
        assert config["DATAGEN_SYNTHETIC_PREFIX"] == ""
        assert config["DATAGEN_SPECS_URI"] == "oci://cfg@ns/specs.json"

    def test_exits_when_required_missing(self, monkeypatch):
        for name in engorda_tables.REQUIRED_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(SystemExit):
            engorda_tables.get_engorda_env()
```

(`import pytest` was added to the test file in Task 1 Step 2.)

- [ ] **Step 2: Run and confirm failure** — `uv run pytest tests/test_engorda_tables.py::TestGetEngordaEnv -v` → FAIL

- [ ] **Step 3: Implement**

```python
import os  # add to module imports


def get_engorda_env() -> dict[str, str]:
    config: dict[str, str] = {}
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
    config["DATAGEN_RAW_PREFIX"] = os.environ.get("DATAGEN_RAW_PREFIX", "").strip("/")
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", ""
    ).strip("/")
    return config
```

Note: `DATAGEN_SPECS_URI` keeps its value as-is except a trailing-slash rstrip; it must point at a
single object, not a prefix (enforced in Task 6).

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: add engorda env loading and normalization"
```

---

## Task 4: `normalize_specs`

Reduces dotted `OWNER.TABLE` keys and FK `parent_table` references to bare `TABLE`, once, so reads,
FK matching, and writes are all consistent. Rejects collisions.

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestNormalizeSpecs:
    def test_reduces_keys_and_parent_table(self):
        raw = {
            "ADMIN.ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "foreign_keys": [
                    {"columns": ["CUSTOMER_ID"], "parent_table": "ADMIN.CUSTOMERS"}
                ],
            },
            "ADMIN.CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"], "static": True},
        }
        out = engorda_tables.normalize_specs(raw)
        assert set(out) == {"ORDERS", "CUSTOMERS"}
        assert out["ORDERS"]["foreign_keys"][0]["parent_table"] == "CUSTOMERS"

    def test_handles_fks_alias_key(self):
        raw = {
            "ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "fks": [{"columns": ["C_ID"], "parent_table": "X.CUSTOMERS"}],
            }
        }
        out = engorda_tables.normalize_specs(raw)
        assert out["ORDERS"]["fks"][0]["parent_table"] == "CUSTOMERS"

    def test_rejects_collision(self):
        raw = {
            "A.ORDERS": {"pk_cols": ["ID"]},
            "B.ORDERS": {"pk_cols": ["ID"]},
        }
        with pytest.raises(ValueError):
            engorda_tables.normalize_specs(raw)

    def test_passes_through_when_no_schema(self):
        raw = {"ORDERS": {"pk_cols": ["ID"], "n_rows": 10}}
        assert engorda_tables.normalize_specs(raw) == raw
```

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement**

```python
import copy  # add to module imports


def normalize_specs(specs: dict) -> dict:
    out: dict = {}
    for raw_name, cfg in specs.items():
        name = table_path_name(str(raw_name))
        if name in out:
            raise ValueError(
                f"Spec key collision after schema stripping: `{raw_name}` reduces to "
                f"`{name}`, which is already present."
            )
        new_cfg = copy.deepcopy(dict(cfg))
        for fk_key in ("foreign_keys", "fks"):
            fks = new_cfg.get(fk_key)
            if not isinstance(fks, (list, tuple)):
                continue
            for fk in fks:
                if isinstance(fk, dict) and fk.get("parent_table"):
                    fk["parent_table"] = table_path_name(str(fk["parent_table"]))
        out[name] = new_cfg
    return out
```

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: normalize spec table names and FK parents once at load"
```

---

## Task 5: `connected_components`

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestConnectedComponents:
    def _comps(self, specs):
        return sorted(sorted(c) for c in engorda_tables.connected_components(specs))

    def test_chain_is_one_component(self):
        specs = {
            "CUSTOMERS": {"pk_cols": ["CID"]},
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
            "ITEMS": {"pk_cols": ["IID"],
                      "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}]},
        }
        assert self._comps(specs) == [["CUSTOMERS", "ITEMS", "ORDERS"]]

    def test_disjoint_components(self):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        assert self._comps(specs) == [["A", "B"], ["C"]]

    def test_isolated_node(self):
        specs = {"LOG": {"pk_cols": ["ID"]}}
        assert self._comps(specs) == [["LOG"]]

    def test_fk_to_absent_parent_is_no_edge(self):
        specs = {
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "MISSING"}]},
            "OTHER": {"pk_cols": ["ID"]},
        }
        # MISSING is not a node, so ORDERS stays isolated from OTHER.
        assert self._comps(specs) == [["ORDERS"], ["OTHER"]]
```

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement** (union-find over tables; edges only when parent is in specs)

```python
def connected_components(specs: dict) -> list[list[str]]:
    parent: dict[str, str] = {t: t for t in specs}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        parent[find(a)] = find(b)

    for table, cfg in specs.items():
        for fk_key in ("foreign_keys", "fks"):
            for fk in cfg.get(fk_key) or []:
                if not isinstance(fk, dict):
                    continue
                p = fk.get("parent_table")
                if p in specs:
                    union(table, p)

    groups: dict[str, list[str]] = {}
    for table in specs:
        groups.setdefault(find(table), []).append(table)
    return [sorted(g) for g in groups.values()]
```

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: split spec graph into FK-connected components"
```

---

## Task 6: `effective_n_rows`

Pure function: given a component's specs and the source row counts, compute the synthesis target
per table. Encodes the Volume rules from the spec.

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestEffectiveNRows:
    SPECS = {
        "CUSTOMERS": {"pk_cols": ["CID"]},  # parent (referenced by ORDERS)
        "ORDERS": {"pk_cols": ["OID"],
                   "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
    }

    def test_scales_non_static(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=3.0)
        assert out["ORDERS"] == 3000

    def test_parent_floor_blocks_shrink(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=0.5)
        # CUSTOMERS is an FK parent: cannot go below its source count.
        assert out["CUSTOMERS"] == 100
        # ORDERS is a leaf: free to scale down.
        assert out["ORDERS"] == 500

    def test_override_wins_for_non_static(self):
        specs = {"BIG": {"pk_cols": ["ID"], "n_rows": 50}}
        out = engorda_tables.effective_n_rows(specs, {"BIG": 10}, scale_factor=3.0)
        assert out["BIG"] == 50

    def test_static_is_one_to_one_override_ignored(self):
        specs = {"REF": {"pk_cols": ["ID"], "static": True, "n_rows": 999}}
        out = engorda_tables.effective_n_rows(specs, {"REF": 7}, scale_factor=3.0)
        assert out["REF"] == 7

    def test_empty_source_is_zero(self):
        specs = {"EMPTY": {"pk_cols": ["ID"], "n_rows": 100}}
        out = engorda_tables.effective_n_rows(specs, {"EMPTY": 0}, scale_factor=3.0)
        assert out["EMPTY"] == 0
```

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement**

```python
def _fk_parent_tables(specs: dict) -> set[str]:
    parents: set[str] = set()
    for cfg in specs.values():
        for fk_key in ("foreign_keys", "fks"):
            for fk in cfg.get(fk_key) or []:
                if isinstance(fk, dict) and fk.get("parent_table") in specs:
                    parents.add(fk["parent_table"])
    return parents


def effective_n_rows(
    specs: dict, source_counts: dict[str, int], scale_factor: float
) -> dict[str, int]:
    parents = _fk_parent_tables(specs)
    targets: dict[str, int] = {}
    for table, cfg in specs.items():
        count = int(source_counts[table])
        static = bool(cfg.get("static", False))
        override = cfg.get("n_rows")
        if count == 0:
            target = 0
        elif static:
            target = count  # static is terminal; override ignored (see warn in engorda)
        elif override is not None:
            target = int(override)
        else:
            target = int(round(count * scale_factor))
        if not static and count > 0 and table in parents:
            target = max(target, count)  # parent floor: keep_all_source_rows needs target >= count
        targets[table] = target
    return targets
```

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: compute per-table synthesis targets with parent floor"
```

---

## Task 7: `parse_arguments`

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests**

```python
class TestParseArguments:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["engorda_tables.py"])
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 1.0
        assert args.seed == 42
        assert args.continue_on_error is False
        assert args.specs is None

    def test_overrides(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv",
            ["engorda_tables.py", "--scale-factor", "3", "--seed", "7",
             "--continue-on-error", "--specs", "oci://cfg@ns/s.json"],
        )
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 3.0
        assert args.seed == 7
        assert args.continue_on_error is True
        assert args.specs == "oci://cfg@ns/s.json"
```

(`import sys` was added to the test file in Task 1 Step 2.)

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement**

```python
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic relational Parquet from ingested raw Parquet."
    )
    parser.add_argument("--scale-factor", type=float, default=DEFAULT_SCALE_FACTOR,
                        help="Global row-count multiplier for non-static tables.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help="Synthesis seed.")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Continue with remaining components after a failure, then exit non-zero.")
    parser.add_argument("--specs", default=None,
                        help="Override DATAGEN_SPECS_URI (URI of a single specs.json object).")
    return parser.parse_args()
```

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: add engorda CLI argument parsing"
```

---

## Task 8: Vendor and trim the synthesizer

Copy the proven synthesizer from `transform/transform.py` into `engorda_tables.py` (between the
constants and the entrypoint helpers), trimmed to Parquet-only paths, with one patch.

**Files:**
- Modify: `engorda_tables.py`
- Reference: `transform/transform.py`

- [ ] **Step 1: Copy the KEEP set verbatim** into `engorda_tables.py`, in the same order they
  appear in `transform.py`:

  - Type aliases: `NullableFkPolicy`, `ValidateMode`, `RelationshipPolicy`, `SaveErrorPolicy`.
  - Dataclasses / types: `ForeignKeySpec`, `PostProcessor`, `TableSpec`.
  - Utils: `_stable_seed`, `_is_integer_type`, `_is_float_type`, `_is_decimal_type`,
    `_is_numeric_pk_type`, `_is_string_type`, `_is_safe_pk_type`, `_get_field_type`, `_persist`,
    `_safe_unpersist`, `_warn_or_raise`, `_format_fk`.
  - Validation/topology: `_sanitize_specs_against_known_tables`, `_fk_has_data_problem`,
    `_sanitize_specs_for_available_relationships`, `_validate_relationship_columns`,
    `_validate_specs`, `_topological_order`, `_referenced_parent_columns`.
    (`_validate_relationship_columns` is REQUIRED — `run_synthesis_from_tables` calls it; it only
    wraps `_sanitize_specs_for_available_relationships`, already kept.)
  - Index/bootstrap: `_with_contiguous_row_id`, `_bootstrap_rows_exact`.
  - PK generation: `_INT_TYPE_LIMITS`, `_FLOAT_EXACT_INT_LIMIT`, `_DOUBLE_EXACT_INT_LIMIT`,
    `_max_pk_value`, `_set_unique_pk_column`, `_generate_pk_columns`.
  - Mapping/FK: `_build_mapping_for_parent_cols`, `_fk_join_condition`, `_apply_fk_mapping`.
  - Result validation: `_rows_to_spark_df`, `validate_primary_keys`,
    `_filter_child_fk_for_validation`, `validate_foreign_keys`, `_run_validation_or_raise`,
    `run_validation_or_raise`.
  - Main synthesis: `synthesize_multitable_spark`.
  - Spec building: `_normalize_cols`, `_try_normalize_cols`, `_infer_parent_table_from_config`,
    `_build_specs_from_config`, `build_specs_from_config`.
  - I/O helpers: `_normalize_save_path`, `_is_local_path`, `_INVALID_COL_CHARS_PATTERN`,
    `_sanitize_columns_for_save`, `_save_hint_for_error`.
  - Runner + save: `run_synthesis_from_tables`, `save_synthetic_tables`.

  Also copy the module-level imports `transform.py` needs (`math`, `re`, `warnings`, `zlib`,
  `from collections.abc import Mapping as ABCMapping`, `from dataclasses import ...`,
  `from functools import reduce`, `from typing import ...`, `from pyspark import StorageLevel`,
  `from pyspark.sql import DataFrame, SparkSession, Window`, `from pyspark.sql import functions as F`,
  `from pyspark.sql import types as T`). Merge them with the existing imports; do not duplicate.

- [ ] **Step 2: DROP everything not listed above.** Specifically do NOT copy these functions and
  constants: `configure_oci_for_spark` and its supporting helpers and constants (`_OCI_FS_IMPL`,
  `_OCI_ABSTRACT_FS_IMPL`, `_OCI_INSTANCE_PRINCIPAL_AUTH`, `_OCI_RESOURCE_PRINCIPAL_AUTH`,
  `OciAuthMode`, `_looks_like_oci`, `_any_oci_path`, `_hostname_from_region`,
  `_read_oci_config_file`), `run_synthesis_from_paths`, `_preflight_relationships`, `_read_table`,
  and the module docstring of `transform.py`. (Nothing in the KEEP set references any of these once
  the Step 5 patch removes the `oci` block from `run_synthesis_from_tables`.)

- [ ] **Step 3: Do not copy `_read_table`.** The entrypoint reads Parquet via its own
  `read_parquet` helper (Task 9), so `_read_table` is not needed at all. (The design spec mentions
  keeping `_read_table`'s Parquet branch; that is superseded here by `read_parquet`.)

- [ ] **Step 4: Trim `save_synthetic_tables`** — keep it, but remove the CSV/ORC branches and the
  `save_single_file` parameter usage is fine to keep (harmless), OR keep the function verbatim. The
  simplest safe choice: keep `save_synthetic_tables` verbatim (it already handles parquet) — the
  CSV branch is dead code that is never reached because the entrypoint always passes
  `save_format="parquet"`. Keeping it verbatim minimizes edit risk. (Trimming the CSV branch is
  optional cleanup, not required.)

- [ ] **Step 5: Patch `run_synthesis_from_tables` to forward `save_mode`.** In the copied
  `run_synthesis_from_tables`, the `save_synthetic_tables(...)` call omits `save_mode`. Add it:

```python
    if save_path:
        save_synthetic_tables(
            synthetic,
            save_path,
            save_format=save_format,
            save_options=save_options,
            save_single_file=save_single_file,
            save_error_policy=save_error_policy,
            save_mode=save_mode,          # <-- ADD THIS LINE (upstream omits it)
            verbose=verbose,
        )
```

  Also remove the `oci` parameter handling at the top of `run_synthesis_from_tables` (the
  `if oci is not None: configure_oci_for_spark(...)` block) since `configure_oci_for_spark` is not
  vendored; drop the `oci` parameter from the signature.

  Note: `run_synthesis_from_tables` contains a *local* variable also named `effective_n_rows` (a
  dict, in the `n_rows_by_table is None` branch). Leave it exactly as-is — it is local-scope and
  unrelated to the module-level `effective_n_rows` function; the entrypoint always passes an
  explicit `n_rows_by_table`, so that branch never runs. Do not rename or "fix" it.

- [ ] **Step 6: Verify the module imports cleanly**

Run: `uv run python -c "import engorda_tables"`
Expected: no ImportError / NameError. If a `NameError` for a dropped helper appears, a KEEP
function still references it — re-check the drop list.

- [ ] **Step 7: Run the full unit suite to confirm no regression**

Run: `uv run pytest tests/test_engorda_tables.py -v`
Expected: all prior tests still PASS.

- [ ] **Step 8: Commit**

```bash
git add engorda_tables.py
git commit -m "feat: vendor trimmed parquet-only synthesizer into engorda_tables"
```

---

## Task 9: `read_parquet`, `release`, and `load_specs`

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write failing tests for `load_specs` parsing/validation** (Spark read is monkeypatched)

```python
class TestLoadSpecs:
    def _fake_spark(self, records):
        class _RDD:
            def collect(self_inner):
                return records
        class _SC:
            def wholeTextFiles(self_inner, uri):
                return _RDD()
        class _Spark:
            sparkContext = _SC()
        return _Spark()

    def test_loads_and_normalizes(self):
        content = json.dumps({"ADMIN.ORDERS": {"pk_cols": ["OID"]}})
        spark = self._fake_spark([("oci://cfg/specs.json", content)])
        specs = engorda_tables.load_specs(spark, "oci://cfg/specs.json")
        assert set(specs) == {"ORDERS"}

    def test_rejects_zero_records(self):
        spark = self._fake_spark([])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_multiple_records(self):
        spark = self._fake_spark([("a", "{}"), ("b", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/")

    def test_rejects_empty_dict(self):
        spark = self._fake_spark([("a", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_malformed_json(self):
        spark = self._fake_spark([("a", "{not json")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")
```

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement**

```python
def read_parquet(spark: "SparkSession", path: str) -> "DataFrame":
    return spark.read.parquet(path)


def release(*dataframes) -> None:
    for df in dataframes:
        if df is None:
            continue
        try:
            df.unpersist()
        except Exception:
            pass


def load_specs(spark: "SparkSession", specs_uri: str) -> dict:
    records = spark.sparkContext.wholeTextFiles(specs_uri).collect()
    if len(records) != 1:
        raise ValueError(
            f"Expected exactly one specs object at `{specs_uri}`, found {len(records)}. "
            "DATAGEN_SPECS_URI must point at a single specs.json file, not a prefix."
        )
    try:
        parsed = json.loads(records[0][1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"specs.json at `{specs_uri}` is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"specs.json at `{specs_uri}` must be a non-empty object.")
    return normalize_specs(parsed)
```

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: add parquet read, df release, and specs loading"
```

---

## Task 10: `engorda` orchestration loop + `main`

**Files:**
- Modify: `engorda_tables.py`
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write a failing unit test for the component loop** (synthesis + read monkeypatched,
  so no Spark needed)

```python
class TestEngordaLoop:
    def _config(self):
        return {
            "DATAGEN_RAW_BASE_URI": "oci://raw@ns", "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns", "DATAGEN_SYNTHETIC_PREFIX": "",
        }

    def test_processes_each_component_and_releases(self, monkeypatch):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        synth_calls = []
        released = []

        class FakeDF:
            def __init__(self, name): self.name = name
            def count(self): return 10

        monkeypatch.setattr(engorda_tables, "read_parquet",
                            lambda spark, path: FakeDF(path))
        monkeypatch.setattr(engorda_tables, "release",
                            lambda *dfs: released.extend(dfs))

        def fake_run(tables, comp_specs, **kwargs):
            synth_calls.append((set(comp_specs), kwargs["n_rows_by_table"]))
            return {t: FakeDF(t) for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=2.0, seed=42, continue_on_error=False)

        processed = sorted(sorted(s) for s, _ in synth_calls)
        assert processed == [["A", "B"], ["C"]]
        assert released  # something was released between/after components

    def test_continue_on_error_collects_and_exits(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}, "C": {"pk_cols": ["ID"]}}

        class FakeDF:
            def count(self): return 5
        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)

        def fake_run(tables, comp_specs, **kwargs):
            raise RuntimeError("boom")
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        with pytest.raises(SystemExit):
            engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                                   scale_factor=1.0, seed=42, continue_on_error=True)
```

- [ ] **Step 2: Run and confirm failure** → FAIL

- [ ] **Step 3: Implement**

```python
def engorda(spark, config, specs, scale_factor, seed, continue_on_error) -> None:
    components = connected_components(specs)
    save_base = synthetic_base_path(config)
    total = len(components)
    logger.info("Loaded %d table(s) in %d component(s)", len(specs), total)
    run_started = time.perf_counter()
    failures: list[str] = []

    for index, comp in enumerate(sorted(components, key=lambda c: sorted(c)[0]), start=1):
        comp_specs = {t: specs[t] for t in comp}
        label = ",".join(sorted(comp))
        comp_tables = {}
        synthetic = {}
        try:
            started = time.perf_counter()
            comp_tables = {t: read_parquet(spark, raw_path(config, t)) for t in comp}
            counts = {t: comp_tables[t].count() for t in comp}
            for t in comp:
                if comp_specs[t].get("static") and comp_specs[t].get("n_rows") is not None:
                    logger.warning("Table %s is static; ignoring n_rows override", t)
            n_rows = effective_n_rows(comp_specs, counts, scale_factor)
            logger.info("[%d/%d] Component {%s}: n_rows=%s", index, total, label, n_rows)
            synthetic = run_synthesis_from_tables(
                comp_tables, comp_specs,
                n_rows_by_table=n_rows, seed=seed,
                save_path=save_base, save_format="parquet",
                save_mode="overwrite", validate_mode="full", verbose=False,
            )
            logger.info("[%d/%d] Component {%s} done in %.1fs",
                        index, total, label, time.perf_counter() - started)
        except Exception as exc:
            logger.exception("[%d/%d] Component {%s} failed: %s", index, total, label, exc)
            failures.append(label)
            if not continue_on_error:
                raise
        finally:
            release(*comp_tables.values(), *synthetic.values())
            try:
                spark.catalog.clearCache()
            except Exception:
                pass

    logger.info("Finished: %d/%d component(s) in %.1fs",
                total - len(failures), total, time.perf_counter() - run_started)
    if failures:
        logger.error("Failed component(s): %s", "; ".join(failures))
        sys.exit(1)


def create_spark_session(app_name: str) -> "SparkSession":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in {
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    }.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main() -> None:
    args = parse_arguments()
    config = get_engorda_env()
    spark = create_spark_session("DataGenEngordaTables")
    try:
        specs_uri = args.specs or config["DATAGEN_SPECS_URI"]
        specs = load_specs(spark, specs_uri)
        engorda(spark, config, specs, args.scale_factor, args.seed, args.continue_on_error)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

Note the `spark.stop()` in `main` — in the loop unit test we never call `main`, so the dummy
`spark=object()` is fine; `clearCache` is guarded by try/except.

- [ ] **Step 4: Run and confirm pass** → PASS

- [ ] **Step 5: Commit**

```bash
git add engorda_tables.py tests/test_engorda_tables.py
git commit -m "feat: add engorda component loop and main entrypoint"
```

---

## Task 11: Local-Spark integration test (end-to-end, skippable)

Proves the vendored synthesizer + entrypoint actually round-trips Parquet with PK/FK integrity and
correct scaling and output paths. Skipped when `pyspark` is not importable so the pure-unit suite
stays fast and dependency-light.

**Files:**
- Test: `tests/test_engorda_tables.py`

- [ ] **Step 1: Write the integration test**

```python
pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.appName("engorda-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestEngordaIntegration:
    def test_round_trip_preserves_keys_and_scales(self, spark, tmp_path):
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        customers = spark.createDataFrame(
            [(i, f"name{i}") for i in range(1, 11)], ["CUSTOMER_ID", "NAME"]
        )
        orders = spark.createDataFrame(
            [(i, (i % 10) + 1, i * 1.5) for i in range(1, 101)],
            ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
        )
        customers.write.parquet(str(raw / "CUSTOMERS"))
        orders.write.parquet(str(raw / "ORDERS"))

        config = {
            "DATAGEN_RAW_BASE_URI": str(raw), "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn), "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"]},
            "ORDERS": {"pk_cols": ["ORDER_ID"],
                       "foreign_keys": [{"columns": ["CUSTOMER_ID"],
                                         "parent_table": "CUSTOMERS"}]},
        }

        engorda_tables.engorda(spark, config, specs, scale_factor=3.0, seed=1,
                               continue_on_error=False)

        out_customers = spark.read.parquet(str(syn / "CUSTOMERS"))
        out_orders = spark.read.parquet(str(syn / "ORDERS"))

        # CUSTOMERS is an FK parent: floored at source count (10), scaled up by 3 -> 30.
        assert out_customers.count() == 30
        # ORDERS scaled 100 -> 300.
        assert out_orders.count() == 300
        # PK uniqueness.
        assert out_orders.select("ORDER_ID").distinct().count() == 300
        assert out_customers.select("CUSTOMER_ID").distinct().count() == 30
        # FK integrity: every synthetic ORDERS.CUSTOMER_ID exists in synthetic CUSTOMERS.
        orphans = out_orders.join(out_customers, "CUSTOMER_ID", "left_anti").count()
        assert orphans == 0
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/test_engorda_tables.py::TestEngordaIntegration -v`
Expected: PASS (or SKIPPED if `pyspark` is unavailable in the environment).

If it fails on FK orphans or counts, re-check the Task 8 vendoring (a dropped helper or a missed
patch is the likely cause), not the entrypoint.

- [ ] **Step 3: Run the whole suite**

Run: `uv run pytest tests/test_engorda_tables.py -v`
Expected: all PASS / integration SKIPPED if no pyspark.

- [ ] **Step 4: Commit**

```bash
git add tests/test_engorda_tables.py
git commit -m "test: add local-spark end-to-end engorda integration test"
```

---

## Task 12: Lint and final verification

**Files:** none (verification only)

- [ ] **Step 1: Lint** — Run: `uv run ruff check engorda_tables.py tests/test_engorda_tables.py`
  Expected: no errors. Fix any and re-run.

- [ ] **Step 2: Full suite** — Run: `uv run pytest tests/test_engorda_tables.py -v` → all PASS/SKIP.

- [ ] **Step 3: Import smoke as Data Flow would** — Run: `uv run python -c "import engorda_tables"`
  Expected: clean import (this is the single file Data Flow uploads as the main script).

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -A
git commit -m "chore: lint engorda_tables"
```

---

## Done

`engorda_tables.py` is a single self-contained file that reads raw Parquet, synthesizes
FK-connected components one at a time (memory-bounded), and writes synthetic Parquet — ready to be
uploaded as the Data Flow main script with no `archive.zip`.
