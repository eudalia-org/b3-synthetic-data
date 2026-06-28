# Shift Synthetic Keys — Design

**Date:** 2026-06-28
**Status:** Approved (design)
**Component:** new standalone Data Flow app `datagen/shift_keys.py`

## Problem

The engorda pipeline writes synthetic ("fattened") Parquet whose generated
primary keys are meant to sit above the real CETIP production maxima so the
load into the FK-constrained Oracle tables doesn't collide. After a full run we
need a way to **increase the already-generated PK/FK values by a uniform amount
`N`** — a post-hoc analogue of `--pk-safety-band` — applied **in place** to the
synthetic output, using the full parallelism of OCI Data Flow.

This is a pure per-row arithmetic shift (`col + N`) on key columns. It needs no
joins and no shuffle, so it is embarrassingly parallel and I/O-bound.

## Goals

- Add a single uniform constant `N` to every **generated** key value, preserving
  referential integrity between PKs and the FKs that reference them.
- Run as a standalone OCI Data Flow application, maximally parallel.
- Mutate the synthetic tables **in place** (no second persistent copy).
- Fail safe: detect numeric-domain overflow **before** writing anything.
- Keep the output schema byte-identical so the downstream JDBC load is unaffected.

## Non-goals

- Per-key or production-max-derived offsets (a uniform constant `N` was chosen;
  see Decision 1).
- Shifting static reference/code data (see Decision 2).
- Idempotency / re-run protection beyond clear logging (see Risks).
- Changing engorda or the load apps.

## Decisions (from brainstorming)

1. **Uniform constant `N`** added to every shifted key (not per-key, not
   production-max-derived). One `--offset N` for the whole run.
2. **Scope: only generated (non-static) keys.** Static tables were copied 1:1
   from production; their PKs *are* real production values and must not move.
   This mirrors how `pk_safety_band` only applied to non-static tables.
3. **In-place overwrite** (no second persistent dataset), via the existing
   scoped-delete-then-append write path.
4. **Overflow handling: fail fast.** A read-only pre-flight aborts before any
   write if a shifted value would exceed a column's numeric domain.
5. **In-place mechanism: checkpoint-swap, per table** (Approach 1) — read →
   shift → sever lineage via checkpoint → scoped-delete the table's own prefix →
   append to the same path.

## Which columns get shifted

Computed from `specs.json` alone (no data read). Let `static` be the set of
tables with `"static": true`.

A column `(table, col)` is shifted **iff**:

- it is an **FK column whose parent table is non-static**, OR
- it is the **PK of a non-static table** AND it is **not** an FK to a static
  parent.

The second clause's exclusion is the key correctness rule: for a shared-key
child whose PK *is* an FK to a static parent (`bind_shared_key_children` makes
the PK equal the parent's real keys), the FK-to-static constraint wins and the
column stays put so the reference still matches.

Because every occurrence of a logical key — its PK column plus every FK column
referencing it — shifts by the *same* `N`, **referential integrity is preserved
by construction**. No joins are needed.

### Verified against the current `specs.json`

- 47 tables: 15 non-static, 32 static. All PKs/FKs are single-column.
- 21 FK columns reference a non-static parent (shift); 61 reference a static
  parent (do **not** shift).
- **31 distinct columns** are shifted (15 non-static PKs + 21 FK-to-non-static,
  deduped for shared-key children).
- **Zero conflicts**: no non-static PK is also an FK to a static parent.

The script computes this set generically, so it remains correct if `specs.json`
changes (and surfaces any future conflict explicitly rather than silently
mis-shifting).

## Architecture

A standalone script `datagen/shift_keys.py` (mirroring `engorda_tables.py`,
`load_tables.py`, etc.) that **imports existing helpers** rather than
reimplementing them:

- `write_synthetic_table` — scoped Hadoop-FS delete of just the table's prefix +
  `mode("append")`. Required: plain `mode("overwrite")` on the OCI HDFS connector
  deletes the *shared parent prefix* and would clobber sibling tables.
- `_pk_capacity` — numeric-domain capacity per column type (for overflow check).
- `read_parquet`, `raw_path`/`synthetic_base_path`, `create_spark_session`.

### Flow

```
load specs (DATAGEN_SPECS_URI) -> compute shift-column-set per table
                                |
        Phase 1: PRE-FLIGHT (read-only) -- abort on overflow, write nothing
                                |
        Phase 2: MUTATE (per table, in place)
                                |
        print deployment summary (env vars + Data Flow config)
```

### Phase 1 — Pre-flight (read-only)

For each shiftable column `(table, col)`:

- Read `max(col)` from the **full** synthetic Parquet using
  `spark.sql.parquet.aggregatePushdown=true` (footer-only metadata read, fast
  even on the 665M-row tables).
- Compute the column's capacity via `_pk_capacity` for its dtype.
- If `max + N > capacity`, record an overflow.

If any column overflows, **abort with a report** listing each offending
`(table, col, dtype, max, max+N, capacity)`. Nothing is written.

### Phase 2 — Mutate (per table)

Process tables **one at a time** (blast radius = one re-generable table). For
each table that has ≥1 shiftable column:

1. `df = read_parquet(synthetic_path(table))`
2. For each shiftable col: `df = df.withColumn(col, (F.col(col) + F.lit(N)).cast(original_dtype))`
   - `cast(original_dtype)` keeps the schema byte-identical (e.g. `Decimal(38,9)`
     stays `Decimal(38,9)`) so the JDBC load is unaffected.
   - `NULL` FK values (left by `null_orphan_fks`) stay `NULL` (`null + N = null`).
     PK columns are never null.
3. Sever lineage from the source files: `df = df.localCheckpoint(eager=True)`
   (or reliable `df.checkpoint()` if `DATAGEN_CHECKPOINT_URI` is set — see Risks).
   This is mandatory: without it the lazy read would re-read the source files
   that the next step deletes, corrupting the output.
4. `write_synthetic_table(spark, df, synthetic_path(table))` — scoped-delete the
   table's own prefix, then `append` to the **same** path.
5. Log `[i/total] shifted <table> (<cols>)`.

Tables with zero shiftable columns are skipped entirely (no I/O) — including any
static table that happens to hold an FK to a non-static parent, handled
generically by the rule.

## CLI / configuration

```
python datagen/shift_keys.py --offset N [--dry-run]
```

- `--offset N` (required): uniform amount added to every shifted key.
- `--dry-run`: run Phase 1 only; report the shift-column-set, each column's
  `max`, and `max+N` vs capacity; exit without writing. Recommended first step
  given in-place + non-idempotent mutation.

Environment variables:

| Var | Required | Purpose |
|---|---|---|
| `DATAGEN_SYNTHETIC_BASE_URI` | yes | synthetic tables read + mutated in place |
| `DATAGEN_SPECS_URI` | yes | `specs.json` — which keys to shift |
| `DATAGEN_CHECKPOINT_URI` | no | if set, reliable checkpoint instead of `localCheckpoint` |

It does **not** read raw source data (`DATAGEN_RAW_BASE_URI` is not needed).

## Deployment summary output

On completion (and on `--dry-run`) the script prints a copy-pasteable block:

```
Required env vars:
  DATAGEN_SYNTHETIC_BASE_URI   oci://<bucket>@<namespace>/<prefix>
  DATAGEN_SPECS_URI            oci://<bucket>@<namespace>/specs.json
  DATAGEN_CHECKPOINT_URI       (optional) oci://<bucket>@<namespace>/_chk

Data Flow application:
  Main:       datagen/shift_keys.py
  Arguments:  --offset <N> [--dry-run]
  Spark:      create_spark_session workload conf (aggregatePushdown, Kryo,
              memoryOverheadFactor=0.3). No shuffle -> shuffle.partitions irrelevant.
  Shape:      Driver    8 OCPU / 64 GB
              Executors 4 x (16-32 OCPU / 128 GB)   # I/O-bound; scale OCPU for throughput
```

## Error handling

- **Overflow:** Phase 1 aborts before any write, with a per-column report.
- **Per-table failure in Phase 2:** logged with the table name; the run stops (or
  continues per a `--continue-on-error` flag matching engorda's convention). A
  failed table can be regenerated by re-running engorda for it. The log makes
  clear which tables were already shifted vs not.
- **Missing env / specs:** fail fast with a clear message (reuse engorda's env
  validation pattern).

## Risks & mitigations

- **Non-idempotent:** re-running double-shifts. Mitigation: a clear startup
  banner, `--dry-run` first, per-table progress logging.
- **In-place failure window:** between the scoped-delete and the append
  completing, a lost executor holding a `localCheckpoint` block means that one
  table's data is gone (the original prefix was already deleted) and must be
  regenerated. Mitigation: process one table at a time so only the in-flight
  table is at risk; offer reliable checkpoint via `DATAGEN_CHECKPOINT_URI`
  (durable, survives executor loss) for runs that need stronger guarantees.
- **Schema drift:** avoided by `cast(original_dtype)` on every shifted column.

## Testing

Local Spark via the JDK-17 path (PySpark 4.1 needs Java 17–21).

- **Unit** — pure `specs -> shift-column-set` function: static rule,
  FK-to-non-static, FK-to-static-wins for shared keys, conflict detection.
- **Integration** — small parent/child/shared-key tables:
  - PK and FK columns shift by exactly `N`;
  - static keys and FK-to-static columns unchanged;
  - FK integrity preserved (child FK still joins parent PK after the shift);
  - `NULL` FK values preserved; dtypes preserved.
- **Overflow** — a tight-domain column where `max + N` exceeds capacity: Phase 1
  aborts with a report and **nothing is written**.

## Out of scope / follow-ups

- Idempotency marker / re-run guard (could add a per-run sentinel later).
- Per-key or production-max-derived offsets (only uniform `N` for now).
