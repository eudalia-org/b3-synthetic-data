# Parallel Load Design (load_tables.py)

**Date:** 2026-06-12
**Purpose:** Standalone `load_tables.py` that writes per-table Parquet into target Oracle through parallel JDBC partitions, surviving the Data Flow→ADB connection killer — the inverse of the optimized `save_tables.py` extract.

## Problem

The load path today lives inside `etl.py` as `load_tables()`: a plain
`df.write.format("jdbc").mode("append")` with `batchsize=10000`, looping
tables in FK-topological order. It inherits whatever partition count the
synthetic Parquet happens to have, so a large table writes through one or a
few long-lived JDBC connections. The same network path that silently kills
long Data Flow→ADB connections on reads (see
`docs/plans/2026-06-11-rowid-parallel-extract-design.md`) kills long writes,
and a multi-minute write transaction is killed before it commits, rolls back,
retries, and is killed again — never completing.

We want a self-contained `load_tables.py`, run as one Data Flow job per big
table, that loads fast and survives the killer.

## Approach

Mirror the read side's philosophy: keep the script focused and self-contained,
make each connection's unit of work short, and let one job manage the whole
`--tables` set.

Spark JDBC writes run one task per DataFrame partition. Each task opens a
connection, inserts in batches of `batchsize`, and — with a transactional
`isolationLevel` (not `NONE`) — wraps the **entire partition in one
transaction that commits only at the end**. This gives the property we want
for free:

- **Per-partition atomic commit = retry-safe.** A killed partition never
  commits, the server rolls it back, Spark retries it on a fresh connection,
  and it re-inserts cleanly — no partial data, no duplicates.
- **So the fix is many small partitions.** Repartition so each partition's
  transaction commits in seconds, under the kill window. A huge partition
  would be killed before committing and loop forever — the same failure seen
  on reads.

`isolationLevel=NONE` (autocommit per batch) is avoided: it would leave
committed batches behind on a kill, and a retry would duplicate them.

### Alternatives considered

- **Optimize `etl.py`'s `load_tables()` in place:** keeps one entrypoint but
  ties the per-big-table-job model to the integrated pipeline. Rejected in
  favor of a standalone script mirroring `save_tables.py`.
- **Spark `mode("overwrite")` + `truncate=true`:** Spark can DROP+recreate the
  Oracle table (losing constraints, indexes, grants) when it decides not to
  truncate. Rejected for explicit truncate + append (below).
- **Disable/manage constraints in a separate companion helper:** rejected per
  requirement — constraint management must live inside the self-contained
  `load_tables.py`.

## DDL execution mechanism

Spark's DataFrame reader only runs SELECTs. For `TRUNCATE` and
`ALTER TABLE ... DISABLE/ENABLE CONSTRAINT`, the script executes statements
through the JVM's JDBC `DriverManager` on the driver:

```python
conn = spark._sc._jvm.java.sql.DriverManager.getConnection(url, user, password)
stmt = conn.prepareStatement(sql)
stmt.execute()
```

This reuses the already-loaded Oracle JDBC jars and the same
`DATAGEN_TARGET_JDBC_URL`, adding no `oracledb`/DSN dependency (consistent with
`save_tables.py`). Constraint-discovery SELECTs reuse the proven Spark
`read_rows` pattern from `save_tables.py`.

Overwrite is implemented as **explicit `TRUNCATE TABLE` + `mode("append")`**,
not Spark's overwrite, so truncate timing and semantics are fully controlled
and the table structure is never dropped.

## Per-table load sequence

Constraint management spans the whole `--tables` set: the script disables the
relevant constraints up front, loads each table, then re-enables them at the
end, so FK ordering within the set never matters.

1. **Discover & disable FKs** (unless `--no-manage-constraints`): query
   `all_constraints` for enabled `constraint_type = 'R'` constraints that
   *reference* each target table (incoming, any schema — these block TRUNCATE)
   plus the target's own outgoing FKs (avoids parallel-partition ordering and
   self-FK violations). `ALTER TABLE <owner>.<table> DISABLE CONSTRAINT
   <name>`. Record each disabled constraint as `(owner, table, name)`.
2. **Truncate** the target: `TRUNCATE TABLE <owner>.<table>`.
3. **Read Parquet** from `{DATAGEN_LOAD_BASE_URI}/{prefix}/{TABLE}`.
4. **Repartition** to the resolved partition count
   (`DATAGEN_JDBC_NUM_PARTITIONS`) so each partition commits in seconds. This
   is a shuffle, accepted to guarantee even, small partitions.
5. **Write** `df.write.format("jdbc").mode("append")` with `batchsize` and a
   transactional isolation level.
6. **Re-enable** the recorded constraints: `ALTER TABLE ... ENABLE NOVALIDATE
   CONSTRAINT <name>` by default (fast; enforces future DML without rescanning
   existing rows), or `ENABLE VALIDATE` with `--validate-constraints`.

## Configuration

Environment variables (reusing existing naming where possible):

- `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`,
  `DATAGEN_TARGET_DB_USER` (default `ADMIN`) — reused from `etl.py`.
- `DATAGEN_LOAD_BASE_URI` + `DATAGEN_LOAD_PREFIX` — input Parquet location,
  mirroring `DATAGEN_RAW_BASE_URI` / `DATAGEN_RAW_PREFIX`. Point at the raw
  bucket for an Oracle→Oracle copy, or at synthetic output.
- `DATAGEN_JDBC_NUM_PARTITIONS` (reused) — write partition count; default 256.
- `DATAGEN_JDBC_BATCH_SIZE` — JDBC `batchsize`; default 10000.
- `DATAGEN_JDBC_READ_TIMEOUT_MS` (reused) — `oracle.jdbc.ReadTimeout`; the
  socket timeout that turns a hung write into a retryable task failure;
  default 600000.

CLI: `--tables` / `--tables-file` (mutually exclusive, required),
`--continue-on-error`, `--no-manage-constraints`, `--validate-constraints`.
No `--limit` (read-side only). `spark.task.maxFailures` is set in the Data Flow
job config, as on the read side.

Connection properties built once: `url`, `user`, `password`,
`driver=oracle.jdbc.OracleDriver`, `oracle.jdbc.ReadTimeout`, plus `batchsize`
and `isolationLevel` applied as write options.

Input path: `{DATAGEN_LOAD_BASE_URI}/{DATAGEN_LOAD_PREFIX}/{TABLE}`, symmetric
with `save_tables.py`'s output convention. `dbtable` is `{owner}.{TABLE}`,
where owner is the schema prefix on the table name or `DATAGEN_TARGET_DB_USER`.

## Error handling

- Per-table `try/except` with `--continue-on-error`; exit non-zero if any table
  failed (same as `save_tables.py`).
- **Constraint re-enable runs in a `finally`** over the recorded
  disabled-constraint list, so a load failure or killed job never leaves FKs
  disabled. Re-enabling an already-enabled constraint is harmless, making it
  idempotent.
- Reruns are idempotent: the truncate at the start of each table resets it to a
  clean state.
- DDL failures (insufficient privilege to alter a cross-schema constraint, etc.)
  are logged with the table and constraint name; with `--continue-on-error` the
  run proceeds, otherwise it raises after attempting re-enable.
- Logging mirrors `save_tables.py`: unbuffered stderr, timestamped format,
  `[i/N]` per-table progress, resolved partition count, per-table elapsed
  time, and an end-of-run summary.

## Known limitations

- **At-least-once into a non-idempotent sink.** A partition that commits but is
  then reported failed (e.g. executor lost just after commit) is retried,
  duplicating that partition's rows. The per-run truncate bounds this to a
  single run; a staging-table + `MERGE` dedup is the future fix if exactly-once
  is ever required. Out of scope now.
- **Cross-schema constraints** require `ALTER` privileges in the referencing
  schema. Without them, use `--no-manage-constraints` and manage constraints
  externally.
- **Re-enable with `NOVALIDATE`** does not validate existing child rows against
  reloaded parents; acceptable for synthetic data into QAB. `VALIDATE` is
  available via flag.

## Testing

Pure-Python unit tests (run via `uv run --no-project --with pytest python -m
pytest`, per project convention):

- Table/owner parsing and `dbtable` construction.
- Input path building from base URI + prefix + table.
- Constraint-discovery SQL builder (incoming + outgoing FKs, identifier
  validation rejecting injection).
- Connection-property assembly (ReadTimeout, batchsize, isolation).
- Partition-count resolution from config.
- Disabled-constraint bookkeeping: the list re-enabled in `finally` matches the
  list disabled.

Real-DB validation (run where the target Oracle is reachable):

- Load a mid-size table; compare target `SELECT COUNT(*)` against the Parquet
  row count.
- Confirm the disabled FKs return to `ENABLED` (`all_constraints.status`) after
  the run, including after an intentionally failed load.
- Confirm the log shows the resolved partition count and per-table timing.

## Expected outcome

A large synthetic table loads through many short-lived JDBC connections that
each commit before the kill window, with Spark retrying any killed partition
cleanly. One Data Flow job per big table, constraints managed automatically
within the job, reruns idempotent — the write-side counterpart to the ~25-min
extract of a 300M-row table.
