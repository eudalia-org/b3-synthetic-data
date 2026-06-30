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

one folder per table, all siblings under `export/`. This env lives on the **Application's**
configuration, so **every run of that Application inherits the same `export/` prefix** —
the orchestrator does not (and cannot) inject it per run; it overrides only the table list
and shape. It never computes per-run output paths.

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

**Connection (a first-class decision, not an implementation detail).** The source is an
**on-prem Oracle** (not a wallet/mTLS Autonomous DB), so no wallet or TLS material is
involved — a plain host/port/service DSN suffices. The extract reaches Oracle via the
**JDBC** driver using `DATAGEN_SOURCE_JDBC_URL` (`jdbc:oracle:thin:@<host>:<port>:<sid>` or
`…@//<host>:<port>/<service>`). `python-oracledb` **thin** mode cannot consume a JDBC URL
directly — it needs a parsed DSN — so the orchestrator must:
1. **Derive an oracledb DSN** from `DATAGEN_SOURCE_JDBC_URL`: strip the `jdbc:oracle:thin:@`
   prefix and pass the resulting host/port/service (EZConnect) descriptor to
   `oracledb.connect`. Handle both the SID colon form and the `//host:port/service` form.
   Credentials come from `DATAGEN_SOURCE_DB_USER` + `DATAGEN_SOURCE_DB_PASSWORD`. No
   `config_dir`/`wallet_location`/`wallet_password` needed.
2. **Run a tier-0 connectivity probe** (`SELECT 1 FROM dual`) that **fails loudly** (clear
   error, non-zero exit unless `--allow-equal-weight-fallback`) rather than letting a broken
   connection silently cascade all tables to tier 5 — otherwise the whole size mechanism is
   dead code that still "succeeds".

**Owner handling.** `--tables` entries may be schema-qualified (`OWNER.TABLE`); unqualified
names default to `DATAGEN_SOURCE_DB_USER`, exactly as `save_tables.py` resolves them. The
size map is therefore keyed on **`(owner, table)`**, and every tier query selects the owner
column so multi-schema sets don't collide.

It resolves a `{(owner, table): rows}` map by trying tiers **in order**, each filling only
the keys still missing a value. Every tier is wrapped so that a privilege error
(`ORA-00942`), a `NULL`/absent statistic, or a timeout **falls through** to the next tier:

| Tier | Source | Query shape | Typical failure → fallthrough |
|---|---|---|---|
| 1 | physical bytes → rows | `SELECT OWNER, SEGMENT_NAME, SUM(BYTES) FROM DBA_SEGMENTS WHERE OWNER IN (:owners) AND SEGMENT_TYPE LIKE 'TABLE%' GROUP BY OWNER, SEGMENT_NAME` | `ORA-00942` (no catalog priv) |
| 2 | stats rows | `SELECT OWNER, TABLE_NAME, NUM_ROWS, AVG_ROW_LEN FROM ALL_TABLES WHERE OWNER IN (:owners)` | `NUM_ROWS` NULL (no stats) |
| 3 | stats rows | `SELECT OWNER, TABLE_NAME, NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER IN (:owners) AND PARTITION_NAME IS NULL` | NULL rows |
| 4 | sampled rows | per still-missing `(owner,table)`: `SELECT COUNT(*) FROM <owner>.<table> SAMPLE(0.1)`, scaled ×1000 | (rare) read error |
| 5 | equal weight | assign the **median** of resolved row-weights (or 1.0 if none resolved) | never fails |

**One common unit.** All tiers normalize to a single unit — **estimated rows** — so weights
are directly comparable: tier 1's `SUM(BYTES)` is divided by a nominal/observed
`AVG_ROW_LEN` to estimate rows (or, where tier 2 also resolves, bytes are simply unused);
tiers 2–4 are already rows. Bin-packing uses these raw row-weights directly — **no
fraction-within-tier normalization** (which would let a lone tier-4 table dominate).
Tier 4's SQL is built by **validated string interpolation** of the identifier (reusing
`IDENTIFIER_PATTERN` from `save_tables.py`), never bind variables — Oracle cannot bind a
schema/table *identifier*. The resolved tier per `(owner,table)` is captured for the
**`--sizes-report`**.

The DB fetch and the **merge** (tier-by-tier gap-fill + tier-5 backstop + median) are
factored into separate functions: `merge_size_tiers(tier_dicts) -> {(owner,table): rows}`
is pure and unit-testable by passing in tier dicts; only the per-tier query functions touch
the DB.

### 2. Bin-packing

Greedy **longest-processing-time-first**: sort tables by weight descending, assign each to
the currently-lightest bucket (min-heap on bucket total). Deterministic for a given input
(ties broken by table name). Bucket count = `--num-buckets`, defaulting to
`--max-concurrent-runs` so every bucket can be in flight at once; set it higher for more,
smaller buckets and a smoother tail.

### 3. Run submission

One `oci data-flow run create` per bucket, all sharing `--application-id` and
`--compartment-id` (a **required** flag for `run create` — sourced from a
`DATAGEN_OCI_COMPARTMENT_ID` env or `--compartment-id` orchestrator flag), overriding:

- the **application arguments** — `["--tables","T1,T2,…", <passthrough>]"` — the bucket's
  table list plus any pass-through extract flags (e.g. `--continue-on-error`).
- the **display name** — `extract-bucket-<i>`.
- **Extract-tuned shape** (IO-bound, deliberately small so `max-concurrent-runs` can be
  high): executor count + driver/executor shapes (incl. flex shape configs) — all
  orchestrator-overridable, with conservative defaults.

> **Implementer must verify exact flag spellings** against `oci data-flow run create --help`
> before wiring `build_run_create_command` — e.g. the run-arguments flag is `--arguments`
> (not `--application-arguments`), and confirm the shape flag names
> (`--num-executors`, `--driver-shape`, `--executor-shape`, `--driver-shape-config`,
> `--executor-shape-config`). The plan includes a `--help` check step.

**Env is inherited from the Application, not injected per run.** `oci data-flow run create`
has no per-run env flag — the source JDBC URL/password and **`DATAGEN_RAW_PREFIX=export`**
(hence the shared `export/` output and per-table subfolders) live on the **Application's**
configuration. The orchestrator overrides only the **arguments** (table list) and the
**shape**; everything else is the Application's existing config. The plan must confirm
`DATAGEN_RAW_PREFIX=export` (and the source/output config) is set on the target Application.

The command is built by a **pure function** `build_run_create_command(bucket, opts)` so it
can be unit-tested and printed verbatim in `--dry-run`.

### 4. Concurrency · monitoring · retry

- A work queue of buckets; at most `--max-concurrent-runs` runs in flight. As one reaches
  a terminal state, the next bucket is submitted.
- Poll `oci data-flow run get --run-id <id>` for `lifecycle-state` every `--poll-seconds`.
  The full Data Flow state set: **non-terminal** `ACCEPTED`, `IN_PROGRESS`, `CANCELING`,
  `STOPPING` (keep polling); **terminal-success** `SUCCEEDED`; **terminal-failure**
  `FAILED`, `CANCELED`, `STOPPED`. Any unrecognized state is logged and treated as
  non-terminal (keep polling) rather than silently classified as done/failed.
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

- **Oracle is unreachable** (tier-0 probe fails — bad DSN/wallet/credentials): fail loudly
  with a clear message and non-zero exit, *unless* `--allow-equal-weight-fallback` is set
  (then degrade to equal-weight/round-robin and warn). This prevents the size mechanism
  silently becoming dead code.
- **Oracle reachable but no stats** (tiers 1–4 yield nothing for some/all tables): tier 5
  assigns the median/equal weight for those keys → bin-packing still proceeds. Logged.
- **`oci` CLI not configured / `run create` error:** abort before submitting further runs
  in a clear message; already-submitted runs are listed so they can be tracked/cancelled.
- **Partial failure:** with `--max-retries` exhausted on some buckets, the orchestrator
  exits non-zero and the manifest marks which tables did **not** extract.
- **`save_tables.py` is unchanged** unless the overwrite smoke test forces the scoped-append
  fallback.

## Testing

Pure functions, unit-tested with **no cloud/DB**:
- **`merge_size_tiers`** — tier coverage and gap-fill: later tiers fill only missing
  `(owner,table)` keys; tier-5 median backstop; single-unit (rows) weights; owner-qualified
  keys don't collide across schemas.
- **Bin-packing** — balance quality and determinism; N buckets; disjoint coverage of all
  input tables.
- **`build_run_create_command`** — correct arguments JSON, display name, shape flags,
  `--compartment-id`, escaping.
- **JDBC-URL → oracledb DSN** derivation — parses both `DATAGEN_SOURCE_JDBC_URL` forms
  (`@host:port:sid` and `@//host:port/service`) into a usable host/port/service DSN.

Manual / live verification (in the plan, not automated):
- **`oci data-flow run create --help`** check to confirm exact flag spellings before wiring.
- **Tier-0 connectivity probe** against the real source (confirms oracledb thin + wallet
  reach the ADB and which size tier actually fires) — this is what `--dry-run` surfaces.
- **2-table concurrency smoke test** for the shared-`export/` overwrite scope (the safety
  gate above).
- A small end-to-end ramp run.

## Out of scope / follow-ups

- OCI Python SDK variant (approach B) — only if shelling to the CLI proves limiting.
- Size-aware *hybrid* bucketing (dedicated run per huge table) — start with balanced
  buckets; revisit if a single table dominates a bucket's tail.
- Auto-ramp (orchestrator self-tunes `max-concurrent-runs`) — manual ramp first.
