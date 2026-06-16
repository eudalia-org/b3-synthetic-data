# Engorda Tables Script Design

**Date:** 2026-06-16
**Purpose:** Design for `engorda_tables.py` — the synthetic ("fatten") data-generation Data Flow app that reads ingested raw Parquet from OCI Object Storage and writes synthetic Parquet back.

## Overview

`engorda_tables.py` is the second of three OCI Data Flow applications in the data-gen
pipeline:

1. **Ingest** — `save_tables.py` reads source Oracle tables and writes raw Parquet to Object Storage.
2. **Engorda (this script)** — reads the raw Parquet, generates synthetic relational data
   preserving primary-key uniqueness and foreign-key integrity, and writes synthetic Parquet
   back to Object Storage.
3. **Load** — reads the synthetic Parquet and loads it into the target OracleDB.

The script is **Parquet in, Parquet out**, and **self-contained**: the proven multi-table
synthesizer in `transform/transform.py` is trimmed to its Parquet-only paths and inlined under
a thin entrypoint, so the app does not depend on `archive.zip`. Authentication is left to the
Data Flow environment (resource principal + native `oci://`), so no OCI auth plumbing is
bundled.

## Goals and Non-Goals

**Goals:**

- Read raw Parquet written by `save_tables.py` and write synthetic Parquet for the load stage.
- Preserve PK uniqueness and FK referential integrity within each related group of tables.
- Grow ("fatten") table volumes via a global scale factor with per-table overrides.
- Bound peak memory by processing one FK-connected group of tables at a time.
- Be a single self-contained file requiring no `archive.zip`.

**Non-Goals:**

- Building or inferring `specs_config` (PK/FK relationships). That is a separate offline step;
  this script consumes a prebuilt `specs.json`.
- Any LLM / OCI GenAI usage (lives in `transform/spec_build.py`, not bundled here).
- CSV/ORC I/O, single-file output, or OCI authentication configuration.
- Cross-run state, orchestration, or scheduling (handled by Data Flow run invocations).

## What Gets Inlined vs Dropped

The synthesizer is copied from `transform/transform.py` and **trimmed to the Parquet-only code
paths**.

**Kept (trimmed):**

- `TableSpec` / `ForeignKeySpec` dataclasses and supporting type/seed utilities.
- Spec sanitization and validation (`_build_specs_from_config`, relationship sanitizers,
  `_validate_specs`, topological ordering).
- Row bootstrap, PK generation, FK remapping, and PK/FK result validation.
- `run_synthesis_from_tables` (used because engorda owns the reads — see Volume section).
  **Patch required:** the upstream `run_synthesis_from_tables` does *not* forward `save_mode` to
  `save_synthetic_tables` (unlike `run_synthesis_from_paths`). The inlined copy must forward
  `save_mode` so the `"overwrite"` idempotency this design relies on is actually applied rather
  than depending on the `save_synthetic_tables` default.
- The **Parquet branch** of `_read_table` and of `save_synthetic_tables`, including column-name
  sanitization for Parquet.

**Dropped:**

- `transform/spec_build.py` entirely (specs are prebuilt).
- `configure_oci_for_spark` and all OCI auth helpers (auth handled by the Data Flow environment).
- CSV and ORC reader/writer branches.
- `save_single_file` output mode.
- `run_synthesis_from_paths` (engorda uses `run_synthesis_from_tables` so it reads each Parquet
  exactly once and controls per-table volume).

Estimated size: roughly 1,200–1,500 lines (trimmed synthesizer + entrypoint).

## Configuration

### Environment variables (matching `save_tables.py` conventions)

Required:

- `DATAGEN_RAW_BASE_URI` — input base. Reads `{base}/[{prefix}/]{TABLE}`.
- `DATAGEN_SYNTHETIC_BASE_URI` — output base. Writes `{base}/[{prefix}/]{TABLE}`.
- `DATAGEN_SPECS_URI` — URI of the prebuilt `specs.json` (for example
  `oci://<bucket>@<namespace>/datagen/configs/specs.json`).

Optional:

- `DATAGEN_RAW_PREFIX` — extra prefix under the raw base.
- `DATAGEN_SYNTHETIC_PREFIX` — extra prefix under the synthetic base.

### Path convention

Input and output paths follow the **actual `save_tables.py` convention**:
`{base}/[{prefix}/]{TABLE}` — a per-table directory, no date component and no `.parquet`
filename suffix. This intentionally differs from the older
`2026-05-18-single-dataflow-etl-...` deployment-plan doc, which described dated paths
(`{YYYYMMDD}_{TABLE}.parquet`); that convention was not what `save_tables.py` implemented, so
engorda aligns with the real ingest output.

engorda must reproduce `save_tables.py`'s exact normalization so a table name resolves to the
same directory the ingest stage wrote *and* the load stage will read:

- base URI `rstrip("/")` and prefix `strip("/")` (as `save_tables.py.get_extract_env` does);
- `table_path_name` reduction: a dotted `OWNER.TABLE` name is reduced to `TABLE` (the schema
  prefix is stripped), matching `save_tables.py`, which calls `build_raw_path` with
  `table_path_name(table)`.

**Single normalization point.** To keep input reads, FK matching, and output writes consistent,
the `table_path_name` reduction is applied **once, in `load_specs`**: every spec key and every FK
`parent_table` reference is reduced to its `TABLE` form before any downstream use. After that, the
component graph, the synthesizer's FK resolution, `raw_path` (reads `{raw_base}/[{prefix}/]TABLE`),
and `save_synthetic_tables` (writes `{syn_base}/[{prefix}/]TABLE` from the dict key) all operate on
the reduced names — so a dotted `OWNER.TABLE` in `specs.json` lands its synthetic output under
`TABLE`, exactly where the load stage expects it. (This assumes table base names are unique after
schema stripping; `load_specs` errors if two spec keys collide to the same `TABLE` name.)

The synthetic output base/prefix get the same `rstrip`/`strip` normalization as the raw base.

### Command-line arguments

- `--scale-factor FLOAT` (default `1.0`) — global growth multiplier applied to every
  non-static table without a per-table override.
- `--seed INT` (default `42`) — synthesis seed.
- `--continue-on-error` — on a component failure, log and continue to the next component, then
  exit non-zero if any component failed (same semantics as `save_tables.py`).
- `--specs PATH` — optional override of `DATAGEN_SPECS_URI`.

### `specs.json` format

The same declarative dict the synthesizer already consumes, plus an optional per-table `n_rows`
override:

```json
{
  "ORDERS":    {"pk_cols": ["ORDER_ID"],
                "foreign_keys": [{"columns": ["CUSTOMER_ID"], "parent_table": "CUSTOMERS"}]},
  "CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"], "static": true},
  "BIG_TABLE": {"pk_cols": ["ID"], "n_rows": 50000000}
}
```

`n_rows` is engorda-specific (consumed by the entrypoint, not by `TableSpec`); all other keys
are passed through to the existing spec builder.

## Connected-Components Batching

Foreign keys partition the tables into **connected components** — groups linked by FKs directly
or transitively. Tables in different components never reference each other, so they never need
to be in memory together. Processing one component at a time bounds peak memory to the largest
single component rather than the whole graph.

Algorithm:

1. Load `specs.json` via `spark.sparkContext.wholeTextFiles(specs_uri)`. The URI must point at a
   single file (not a prefix/directory): `.collect()` the `(path, content)` pairs and require
   exactly one record — error clearly on 0 or >1 — then `json.loads` that record's content.
   Validate the result is a non-empty dict.
2. Build an **undirected graph**: nodes are the tables present in specs; an edge connects a
   child to each FK `parent_table` that is also present in specs. (An FK whose parent is absent
   from specs contributes no edge; the synthesizer later warns and skips that FK.) Because the
   graph is built from the parent relation, **each connected component is closed under FK
   parenthood** — every parent of an in-component table is also in that component. This closure
   is what makes per-component synthesis safe: the synthesizer's `_sanitize_specs_against_known_tables`
   would otherwise silently drop a real FK whose parent was split into another batch.
3. Compute connected components (union-find or BFS). An isolated table with no FK edges is its
   own single-node component.
4. Process components sequentially, releasing memory between them:

```text
for comp in connected_components(specs):
    comp_specs  = {t: specs[t] for t in comp}
    comp_tables = {t: read_parquet(raw_path(t)) for t in comp}
    n_rows      = effective_n_rows(comp_specs, comp_tables, scale_factor)
    run_synthesis_from_tables(comp_tables, comp_specs,
                              n_rows_by_table=n_rows, seed=seed,
                              save_path=synthetic_base, save_format="parquet",
                              save_mode="overwrite", validate_mode="full")
    release(comp_tables, synthetic)   # unpersist DataFrames; spark.catalog.clearCache()
```

All components are processed within a single Data Flow run by default. A single component that
is itself large is not split (splitting could break FK integrity); Spark spills its persisted
intermediates to disk via `MEMORY_AND_DISK` rather than failing with OOM.

## Volume: Global Factor Plus Per-Table Override

engorda reads each component's tables once and computes `n_rows_by_table` explicitly, then
passes it to `run_synthesis_from_tables`.

**Static tables bypass this entirely.** The inlined synthesizer's static branch always copies the
source rows (`src_count`) and ignores `n_rows_by_table` for that table. So for `static: true`
tables only the "1:1" outcome applies; the value `effective_n_rows` returns for a static table is
advisory (engorda still emits `src_count` and warns if an override differs), and the empty-clamp
and scale-factor steps below do not govern static output. The ordering below therefore applies to
**non-static** tables; static is listed only to make the 1:1 rule explicit.

For each table, the target is resolved in this order:

1. **Empty source** (`source_count == 0`) → target `0`, regardless of override or factor. The
   bootstrap raises `ValueError("Fonte vazia mas n_rows > 0")` if a positive target is requested
   for an empty source, so empty tables are clamped to 0 first.
2. **`static: true`** → keep its source row count (1:1). Static is terminal: an `n_rows` override
   on a static table is **ignored** (the inlined synthesizer hard-forces `src_count` for static
   tables and warns on a differing target — see transform.py's static branch). engorda mirrors
   that by not emitting a differing target for static tables, and logs a warning if a static
   table also carries an `n_rows` value.
3. **`n_rows` override** in specs (non-static tables) → use it (override wins over the factor).
4. Otherwise → `round(source_count × scale_factor)`.

**Parent-table floor.** A **non-static** table referenced as an FK `parent_table` by any other
table in the component is bootstrapped with `keep_all_source_rows=True`, which *requires*
`target >= source_count` and raises otherwise. So after steps 1–4, such a parent's target is
floored: `target = max(target, source_count)`. This means `--scale-factor < 1` cannot shrink a
non-static parent below its source size; if an override or a sub-1 factor would, engorda bumps it
up to the source count and logs a warning. (Static parents bypass bootstrap and are already 1:1;
leaf/child tables and standalone non-parent tables can scale down freely.)

`run_synthesis_from_tables` is chosen over `run_synthesis_from_paths` precisely so engorda owns
the Parquet reads: this avoids reading each file twice (once for counts, once for synthesis) and
keeps the override-plus-factor merge in one place.

## Implementation Structure

```python
def parse_arguments() -> argparse.Namespace: ...
def get_engorda_env() -> dict[str, str]: ...          # required/optional env vars, exits if missing
def create_spark_session(app_name: str) -> SparkSession: ...
def load_specs(spark, specs_uri) -> dict: ...          # wholeTextFiles(single record) + json.loads
                                                       # + validate non-empty dict
                                                       # + table_path_name-reduce keys & FK parent_table
                                                       #   (reject collisions)
def connected_components(specs: dict) -> list[list[str]]: ...
def raw_path(config, table) -> str: ...                # {raw_base}/[{raw_prefix}/]{TABLE}
def synthetic_base_path(config) -> str: ...            # {syn_base}/[{syn_prefix}/]
def effective_n_rows(comp_specs, comp_tables, scale_factor) -> dict[str, int]: ...
def read_parquet(spark, path) -> DataFrame: ...        # trimmed _read_table parquet branch
def release(*dataframes) -> None: ...                  # unpersist + clearCache

# --- inlined trimmed synthesizer (TableSpec, FK remap, run_synthesis_from_tables, save...) ---

def engorda(spark, config, specs, scale_factor, seed, continue_on_error) -> None:
    # iterate components, synthesize, save, release, collect failures

def main() -> None:
    args = parse_arguments()
    config = get_engorda_env()
    spark = create_spark_session("DataGenEngordaTables")
    try:
        specs = load_specs(spark, args.specs or config["DATAGEN_SPECS_URI"])
        engorda(spark, config, specs, args.scale_factor, args.seed, args.continue_on_error)
    finally:
        spark.stop()
```

## Error Handling

- Missing required env var, or unreadable/invalid `specs.json` → log and `sys.exit(1)` before
  any Spark synthesis work.
- Per-component failure:
  - Without `--continue-on-error`: log and re-raise, failing the run.
  - With `--continue-on-error`: log, append to a failures list, continue, and `sys.exit(1)` at
    the end if any component failed.
  - Progress is logged `[i/N]` per component, mirroring `save_tables.py`.
- An FK whose `parent_table` is absent from specs: existing synthesizer behavior — warn and skip
  that one relationship; the rest of the table is still synthesized. Never silently corrupts FK
  integrity.
- **Table declared in specs but missing from raw Object Storage** (ingest never wrote it, or it
  failed): `spark.read.parquet(raw_path(t))` raises a path-not-found error. This fails the entire
  component the table belongs to (a component is FK-linked, so its tables cannot be synthesized
  independently). engorda logs a clear message naming the table, its component, and the missing
  path; under `--continue-on-error` it records the component as failed and moves on, otherwise it
  re-raises. (engorda does not fabricate data for a missing input table.)
- Output is written per table with `save_mode="overwrite"` (see the patch note in the inlining
  section), so reruns of a component are safe and idempotent.

## Logging

Use the standard `logging` module at INFO (same setup as `save_tables.py`):

- Loaded specs: table count and component count.
- Per component `[i/N]`: tables, topological order, target `n_rows_by_table`.
- Read/write paths per table.
- Per-component completion and elapsed time.
- Final summary: components succeeded/failed, total elapsed.

## Testing

`tests/test_engorda_tables.py` (mirrors `tests/test_save_tables.py` style):

- Unit tests:
  - `connected_components`: isolated nodes, transitive chains, multiple disjoint components,
    and FK pointing at a table absent from specs.
  - `effective_n_rows`: `n_rows` override on a non-static table wins over the factor; `n_rows` on
    a `static: true` table is ignored (stays 1:1, warns); static with no override stays 1:1;
    others scaled by factor; empty source clamped to 0; parent-table floor
    (`max(target, source_count)`) so `scale_factor < 1` does not shrink a parent.
  - `raw_path` / `synthetic_base_path` with and without prefixes.
  - `load_specs` name normalization: a dotted `OWNER.TABLE` key is reduced to `TABLE` and its FK
    `parent_table` references are reduced consistently; reads and writes both resolve to
    `{base}/[{prefix}/]TABLE`; colliding reduced names are rejected.
  - `load_specs` parsing: valid single-record dict, empty dict rejected, malformed JSON rejected,
    and the 0-record / >1-record `wholeTextFiles` cases rejected.
- One small local-Spark integration test: a tiny parent/child two-table set written to local
  Parquet, run through `engorda`, asserting PK uniqueness, FK validity, and that non-static row
  counts scale by the factor.

## Design Decisions

1. **Prebuilt `specs.json` over inline spec building** — keeps the runtime lean (no pandas, no
   metadata file, no LLM) and self-contained; spec building stays a separate offline concern.
2. **Trim the synthesizer rather than copy verbatim** — drops CSV/ORC, OCI auth, and unused
   runners to keep the single file focused on the Parquet path actually exercised.
3. **Connected-components batching** — bounds peak memory to the largest FK-linked group while
   guaranteeing FK integrity, which a per-table split could not.
4. **`run_synthesis_from_tables` (not `_from_paths`)** — engorda owns the reads so it can read
   once and merge the global scale factor with per-table overrides in one place.
5. **Align paths to real `save_tables.py` output** — `{base}/[{prefix}/]{TABLE}`, not the dated
   paths from the older deployment-plan doc.
6. **Auth left to the environment** — Data Flow resource principal resolves `oci://` natively,
   so no auth code is bundled.
