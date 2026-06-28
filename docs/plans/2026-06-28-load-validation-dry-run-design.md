# Load Validation Dry-Run — Design

**Date:** 2026-06-28
**Status:** Approved (design)
**Component:** `datagen/load_tables.py` — new `validate_load` pre-flight + `--dry-run`

## Problem

`load_tables.py` appends the synthetic Parquet into the **real, FK-constrained
CETIP Oracle tables**. This is the first real production load (~3.49B rows), and
the failure mode is ugly: Spark's JDBC writer auto-commits per partition, so a
constraint violation surfaces partway through, leaving a partially-loaded table
to roll back. We want the load to **work first try**.

The existing guardrails are runtime *recovery* aids, not *prevention*, and cover
only three failure modes:

| Guard | Prevents |
|---|---|
| `topo_sort_for_load` | `ORA-02291` only for FKs whose parent is also in the load set |
| `apply_pk_guard` (left-anti vs existing keys) | `ORA-00001` only for single-column **numeric** PKs |
| `normalize_pk_bound` | `NumberFormatException` on JDBC partition bounds |

Unguarded and able to fail a first-try load: `ORA-01438` (numeric overflow),
`ORA-12899` (string too long), `ORA-01400` (NULL into NOT NULL), `ORA-00001` on
composite PKs / `UNIQUE` constraints, and `ORA-02291` to a **static** parent.

## Goals

- A read-only **pre-flight validation** that runs before any insert and aborts
  (inserting nothing) if the synthetic data would violate the target schema.
- Cover all six failure modes above.
- `--dry-run` runs the validation and stops; a real load runs it as a mandatory
  pre-flight.
- Read-only and reasonably cheap (metadata + bounded production reads + Parquet
  profiling), validating exactly the rows that would be inserted.

## Non-goals

- CHECK-constraint (`ORA-02290`) validation (parsing `SEARCH_CONDITION` is fiddly;
  rare here).
- A transactional "insert-then-rollback" test (not clean with Spark's per-
  partition auto-commit).
- Changing the insert path, the dup-guard, or the rollback manifest.

## Decisions (from brainstorming)

1. **All six checks** in the first cut (first real load → comprehensive).
2. **Mandatory pre-flight** before every real load; `--dry-run` runs it and stops
   (Approach A / Approach 1).
3. **Bounded production reads** for the expensive anti-joins (reuse the existing
   synthetic-PK-range trick) to keep cost down.
4. Validate **exactly what will be inserted** — same Parquet, respects `--limit`.
5. **Collect all violations** across all tables before aborting (batch fix).

## Architecture & flow

In `main`, after `resolve_load_tables` and before any insert:

```
tables = resolve_load_tables(...)                         # topo-sorted
violations = validate_load(spark, properties, config, specs,
                           target_schema, tables, limit)
if violations:   log per-table report  →  sys.exit(1)     # nothing inserted
if args.dry_run: log "validation passed; nothing loaded"  →  return
# clean real run: write rollback manifest → load_tables() insert loop (unchanged)
```

- New `--dry-run` flag (store_true). On `--dry-run` or any violation, the manifest
  write and the insert loop are skipped entirely.
- `--continue-on-error` does **not** apply to validation — a violation means the
  *data* is wrong, so the whole run aborts (fix and re-run), not skip-and-continue.
- Read-only: metadata queries + bounded production reads + Parquet profiling. No
  inserts, no DDL.

## Inputs gathered per run

- **Target metadata** (reuse `read_rows`, owner = `DATAGEN_TARGET_SCHEMA`):
  - `ALL_TAB_COLUMNS WHERE OWNER=:schema` → `COLUMN_NAME, DATA_TYPE,
    DATA_PRECISION, DATA_SCALE, DATA_LENGTH, CHAR_LENGTH, NULLABLE, DATA_DEFAULT`
    (one query for all loaded tables).
  - `ALL_CONSTRAINTS`/`ALL_CONS_COLUMNS` for `P`/`U`/`R` constraints (columns per
    constraint) → PK/UK column sets and FK→parent mappings.
- **Synthetic profile** per table (footer-fast / single-scan aggregates): per-
  column `max`/`min` (numeric), `max(octet_length)` (string), null-counts; and
  `count(*)` vs `countDistinct` for each PK/UK column set.
- **Bounded production reads** for anti-joins: reuse `read_existing_keys` /
  `build_existing_keys_query` (range-bounded by the synthetic key min/max) for
  single-numeric keys; read the (small) parent key set for FK-to-static.

## The six checks

A **violation** is a record `(table, check, columns, detail)` accumulated into a
report. Checks do not stop at the first failure.

1. **Column alignment** (structural). Any synthetic column not in the target
   (uppercased) → violation; any target `NOT NULL`-without-`DATA_DEFAULT` column
   missing from the synthetic → violation.
2. **Numeric domain → `ORA-01438`.** For each synthetic numeric column mapped to a
   target `NUMBER(p,s)` (non-null precision): `capacity = 10^(p-s) - 1`; violation
   if `max(col) > capacity` or `min(col) < -capacity`. Excess scale is not an
   insert error (Oracle rounds), so only magnitude is checked.
3. **String length → `ORA-12899`.** For each string column mapped to
   `VARCHAR2`/`CHAR`: violation if `max(octet_length(col)) > DATA_LENGTH` (byte
   semantics — conservative/correct bound). The metadata query also pulls
   `CHAR_LENGTH`, so if `octet_length` proves over-strict for CHAR-semantics
   (multibyte) columns, the check can fall back to `length(col) > CHAR_LENGTH`
   for those — but byte-length is the safe default.
4. **NOT NULL → `ORA-01400`.** For each target `NULLABLE='N'` column present in the
   synthetic: violation if its synthetic null-count > 0.
5. **Uniqueness → `ORA-00001`.** For each PK (incl. composite) and `UNIQUE`
   constraint whose columns are all present in the synthetic:
   - **internal dups:** `count(*) ≠ count(distinct cols)` → violation;
   - **vs production:** anti-join synthetic distinct keys against production,
     **bounded by the leading numeric column's `[min,max]`** when available; any
     synthetic key already in production → violation. (New coverage over
     `apply_pk_guard`, which only handles single-numeric PKs. For composite or
     non-numeric keys the production read can't be range-bounded — see Risks.)
6. **FK-to-static → `ORA-02291`.** For each FK whose parent is **static** (not in
   the load set): anti-join synthetic non-null FK values against the production
   parent's key column(s); any orphan → violation. Static parents are small
   code/reference tables, so the production read is cheap.

## Output / error handling

- Violations log as a grouped per-table report
  (`table → [check: column(s) — detail]`) and the run `sys.exit(1)`, inserting
  nothing.
- `--dry-run` with no violations → log "validation passed; nothing loaded",
  exit 0.
- Missing target tables / unreadable metadata → fail fast with a clear message
  (reuse the existing env/`validate_identifier` patterns).

## Code organization / self-contained

`load_tables.py` is a self-contained single-file Data Flow app. Reuses what's
already there (`read_rows`, `build_connection_properties`, `validate_identifier`,
`read_existing_keys`, `build_existing_keys_query`, `table_owner_and_name`,
`pk_cols_for`, `is_static`, `build_load_path`). Adds:

- pure check functions (metadata + profile dicts → violations) — one per check,
  unit-tested;
- thin Spark/Oracle I/O wrappers (read target metadata, profile a synthetic
  table, bounded production-key reads);
- `validate_load(...)` orchestrator;
- vendored `capacity_from_precision_scale` (small; same as `shift_keys`);
- `--dry-run` arg + `main` wiring.

## Testing

Local Spark via the JDK-17 path. Mirrors `shift_keys` (pure logic fully tested,
Oracle reads not exercised locally).

- **Unit** — each pure check against crafted metadata/profile dicts, positive and
  violating cases: column alignment (extra + missing-required), numeric domain
  (`capacity_from_precision_scale`, over/under), string length, NOT NULL,
  uniqueness (internal dup + vs-production set), FK-to-static (orphan set).
- **Integration** (local Spark) — the profiling aggregates over small Parquet
  produce the expected profile (max/min/max-octet-length/null-count/distinct),
  feeding the pure checks end-to-end without Oracle.
- Oracle metadata + bounded anti-join reads: thin wrappers, not run locally.

## Risks & mitigations

- **Cost of uniqueness vs-production for composite/non-numeric keys** — can't be
  range-bounded, so it reads the production key columns more broadly. By design
  synthetic PKs sit above production (pk-safety-band + `shift_keys` offset), so we
  bound where we can and log clearly when a check falls back to an unbounded read;
  a follow-up can gate or sample it if it proves expensive.
- **Schema drift between metadata read and load** — small window; the load itself
  is the source of truth and would still error, but the manifest enables rollback.
- **CHECK constraints uncovered** — out of scope; documented.

## Out of scope / follow-ups

- CHECK-constraint validation.
- Gating/sampling the unbounded uniqueness check if it proves costly.
- Persisting a machine-readable validation report artifact (currently logged only).
