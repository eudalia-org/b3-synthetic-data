# Parallel Extract Orchestrator — Design

**Date:** 2026-06-29
**Status:** Approved (design)
**Component:** `scripts/parallel_extract.py` — fan `save_tables.py` out across many concurrent OCI Data Flow runs

## Problem

The Oracle→OCI extract (`datagen/save_tables.py`) reads source tables and writes each as
raw Parquet. Today a single Data Flow run processes its whole `--tables` list
**sequentially** (rowid-parallel *within* each table, but one table at a time). For the
full source set this is slow: the run's wall-clock is the *sum* of every table's extract.

Extraction tables are **independent** — there is no FK ordering needed to *read* a table —
so the work is embarrassingly parallel. We want to launch **multiple Data Flow runs of the
same Application concurrently**, each extracting a disjoint subset of tables, to collapse
wall-clock from "sum of all tables" toward "the slowest single bucket".

## Decision

Build `scripts/parallel_extract.py`: a **local Python driver** (not a Data Flow app) that
shells out to the **OCI CLI**. It fetches per-table sizes from Oracle (via a graceful
fallback chain), bin-packs tables into size-balanced buckets, submits one
`oci data-flow run create` per bucket against the same `--application-id` (capped at
`--max-concurrent-runs` in flight), polls each run to completion, retries failures, and
reports. A `--dry-run` mode plans everything and submits nothing.

This is the chosen approach over (B) using the OCI Python SDK directly — we prefer reusing
the existing CLI auth/config and keeping bin-packing in testable Python — and over (C) a
pure `bash`/`xargs -P` loop, which makes balanced bucketing and retry/monitoring fragile.

## Scope (what this is NOT)

- **Not** a change to `save_tables.py`'s extract logic — the orchestrator only *invokes*
  it with different `--tables` arguments and shape overrides. (The one possible exception
  is the overwrite-scope safety item below, gated on a smoke test.)
- **Not** an OCI-native DAG orchestrator (Data Integration / Functions / Resource Manager).
  A local driver is sufficient and transparent.
- **Not** an extension of the load or engorda workloads — extraction only.
- **No** within-table re-partitioning beyond what `save_tables.py` already does via rowid.

## Output layout (shared bucket)

All runs write to the **same** output bucket under the **same** prefix. `save_tables.py`'s
`build_raw_path` already yields `<DATAGEN_RAW_BASE_URI>/<DATAGEN_RAW_PREFIX>/<TABLE>`, so
passing every run the **same** `DATAGEN_RAW_BASE_URI` + `DATAGEN_RAW_PREFIX=export`
produces exactly:

```
export/TABLE_1/   export/TABLE_2/   export/TABLE_3/   ...
```

one folder per table, all siblings under `export/`. The orchestrator passes this env
identically to every run; it does not compute per-run output paths.

### Concurrency safety of the shared prefix

Buckets are **disjoint table sets** (bin-packing guarantees no table appears in two
buckets), so no two concurrent runs ever write the same `export/<TABLE>/` folder — write
contention is structurally impossible.

The one residual risk is the known OCI HDFS-connector gotcha: `mode("overwrite")` deleting
the **parent** prefix rather than just the table folder (see
`~/wiki/eudalia/spark-oci-object-storage.md`). `save_tables.py:564` uses
`df.write.mode("overwrite").parquet("<…>/export/<TABLE>")`. If that overwrite escalates to
deleting `export/`, run A (TABLE_1) would wipe `export/TABLE_2/` that run B just wrote.

**Resolution (in priority order):**

1. **Verify-then-trust (default):** run the **2-table concurrency smoke test** below; if
   per-table-path overwrite is scoped to `export/<TABLE>/` (expected for distinct target
   paths), `save_tables.py` is left unchanged.
2. **Scoped delete + append (fallback, only if the smoke test clobbers):** change
   `save_tables.py` to delete only `export/<TABLE>/` via the Hadoop FileSystem API, then
   `mode("append")` — the pattern already proven in the load's `write_synthetic_table`.

The smoke test is part of the implementation plan's manual verification, not an automated
test (it needs live OCI + Oracle).

## Design

### Components / flow

```
parse args / env
  → fetch table sizes (Oracle, fallback chain)        [read-only]
  → bin-pack tables into N balanced buckets            [pure]
  → build `oci data-flow run create` command per bucket [pure]
  → if --dry-run: print sizes report + buckets + commands, exit 0
  → else: submit (≤ max-concurrent), poll, retry, report
```

### 1. Live size fetch — fallback chain

The orchestrator connects to the **source** Oracle using **`python-oracledb` thin mode**
(pure-Python, no Instant Client; `pip install oracledb`), reusing the source connection
details (host/service/credentials derived from the same source config the extract uses).

It resolves a `{table: weight}` map by trying tiers **in order**, each filling only the
tables still missing a weight. Every tier is wrapped so that a privilege error
(`ORA-00942`), a `NULL`/absent statistic, or a timeout **falls through** to the next tier
rather than aborting:

| Tier | Source | Query shape (for `:owner`) | Typical failure → fallthrough |
|---|---|---|---|
| 1 | physical bytes | `SELECT SEGMENT_NAME, SUM(BYTES) FROM DBA_SEGMENTS WHERE OWNER=:owner AND SEGMENT_TYPE LIKE 'TABLE%' GROUP BY SEGMENT_NAME` | `ORA-00942` (no catalog priv) |
| 2 | stats bytes | `SELECT TABLE_NAME, NUM_ROWS*AVG_ROW_LEN FROM ALL_TABLES WHERE OWNER=:owner` | `NUM_ROWS`/`AVG_ROW_LEN` NULL (no stats) |
| 3 | stats rows | `SELECT TABLE_NAME, NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER=:owner AND PARTITION_NAME IS NULL` | NULL rows |
| 4 | sampled rows | `SELECT COUNT(*) FROM :owner.:table SAMPLE(0.1)` per still-missing table, scaled ×1000 | (rare) read error |
| 5 | equal weight | assign the **median** of resolved weights (or 1.0 if none resolved) | never fails |

Units differ across tiers (bytes vs rows). Because weights are used **only** for relative
bin balance, the orchestrator normalizes each table's weight to a fraction of the total
within whatever tier produced it; cross-tier mixing is approximate and affects only
balance, never correctness. The resolved tier per table is captured for the
**`--sizes-report`**.

### 2. Bin-packing

Greedy **longest-processing-time-first**: sort tables by weight descending, assign each to
the currently-lightest bucket (min-heap on bucket total). Deterministic for a given input
(ties broken by table name). Bucket count = `--num-buckets`, defaulting to
`--max-concurrent-runs` so every bucket can be in flight at once; set it higher for more,
smaller buckets and a smoother tail.

### 3. Run submission

One `oci data-flow run create` per bucket, all sharing `--application-id`, overriding:

- `--application-arguments '["--tables","T1,T2,…", <passthrough>]'` — the bucket's table
  list plus any pass-through extract flags (e.g. `--continue-on-error`).
- `--display-name extract-bucket-<i>`.
- **Extract-tuned shape** (IO-bound, deliberately small so `max-concurrent-runs` can be
  high): `--num-executors`, `--driver-shape`, `--executor-shape`, and flex
  `--driver-shape-config` / `--executor-shape-config` — all CLI-overridable, with
  conservative defaults.

The command is built by a **pure function** `build_run_create_command(bucket, opts)` so it
can be unit-tested and printed verbatim in `--dry-run`.

### 4. Concurrency · monitoring · retry

- A work queue of buckets; at most `--max-concurrent-runs` runs in flight. As one reaches
  a terminal state, the next bucket is submitted.
- Poll `oci data-flow run get --run-id <id>` for `lifecycle-state` every `--poll-seconds`.
  Terminal states: `SUCCEEDED`, `FAILED`, `CANCELED` (treat `CANCELING`/`STOPPED` as
  terminal-failure for retry purposes).
- On `FAILED`, retry the bucket up to `--max-retries` (extract is idempotent per table —
  re-running overwrites that table's `export/<TABLE>/`).

### 5. `--dry-run`

Plans everything, submits nothing:
1. Run the size-fetch chain (read-only) and print the **sizes report** — which tier
   resolved each table and its weight.
2. Print the **buckets**: tables per bucket, per-bucket total weight, and the balance skew
   (max/min bucket weight ratio).
3. Print the **exact `oci data-flow run create` command** per bucket (copy-pasteable).
4. Exit 0 — no submission, no polling.

Doubles as a safe connectivity/privilege probe: the operator sees which fallback tier fired
before committing any compute.

### 6. Report & idempotency

A local JSON manifest: per bucket → tables, run id, final state, duration, retry count, and
the resolved size tier. The whole orchestration is re-runnable end-to-end (each table's
Parquet is overwritten).

## Discovering the concurrency ceiling

`--max-concurrent-runs` is an explicit knob; its correct value is found empirically, not at
design time. Two independent ceilings:

- **OCI capacity:** per-run OCPU × concurrent runs vs the tenancy Data Flow OCPU quota
  (`oci limits value list --service-name data-flow`). Because extract is IO-bound, the
  per-run shape is small, so many runs fit a given quota.
- **Oracle source load:** `max-concurrent-runs × num_partitions` live JDBC sessions vs the
  source's session/IO budget and the ADB connection-killer.

**Ramp procedure (operator runbook):** start `--max-concurrent-runs 4`, watch run durations
and Oracle session count, increase (5, 8, …) until throughput plateaus or
throttling/connection errors appear; back off one step.

## Error handling / interactions

- **Oracle fetch fully fails** (all tiers error): tier 5 assigns equal weight → degrades to
  round-robin bucketing; the run still proceeds. Logged prominently.
- **`oci` CLI not configured / `run create` error:** abort before submitting further runs
  in a clear message; already-submitted runs are listed so they can be tracked/cancelled.
- **Partial failure:** with `--max-retries` exhausted on some buckets, the orchestrator
  exits non-zero and the manifest marks which tables did **not** extract.
- **`save_tables.py` is unchanged** unless the overwrite smoke test forces the scoped-append
  fallback.

## Testing

Pure functions, unit-tested with **no cloud/DB**:
- **Fallback merge** — tier coverage and gap-fill: later tiers fill only missing tables;
  tier 5 backstops; weight normalization is correct.
- **Bin-packing** — balance quality and determinism; N buckets; disjoint coverage of all
  input tables.
- **`build_run_create_command`** — correct `application-arguments` JSON, display name, shape
  flags, escaping.

Manual / live verification (in the plan, not automated):
- **2-table concurrency smoke test** for the shared-`export/` overwrite scope (the safety
  gate above).
- A small end-to-end ramp run.

## Out of scope / follow-ups

- OCI Python SDK variant (approach B) — only if shelling to the CLI proves limiting.
- Size-aware *hybrid* bucketing (dedicated run per huge table) — start with balanced
  buckets; revisit if a single table dominates a bucket's tail.
- Auto-ramp (orchestrator self-tunes `max-concurrent-runs`) — manual ramp first.
- Driving the source `:owner` / connect string: reuse the extract's source config; exact
  derivation from `DATAGEN_SOURCE_JDBC_URL` is an implementation detail.
