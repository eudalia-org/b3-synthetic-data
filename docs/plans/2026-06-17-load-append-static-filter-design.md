# Load Append + Static Filter Design (load_tables.py)

**Date:** 2026-06-17
**Purpose:** Change `load_tables.py` to append (not overwrite/truncate), load only non-`static` tables from `specs.json`, skip synthetic rows whose primary key already exists (a no-DDL, PK-range-bounded anti-join so partial-failure reruns are duplicate-free), support sampled loads via `--limit`, and log the run clearly. Removes the FK/DDL constraint machinery that overwrite required.

## Motivation

`load_tables.py` currently overwrites each target table (explicit `TRUNCATE` + `mode("append")`) and disables/re-enables foreign keys around the truncate (because TRUNCATE is blocked by incoming FKs, ORA-02266). The pipeline now needs to:

- **Append** synthetic rows to existing target tables rather than replacing them.
- **Skip reference/lookup tables** that are pre-loaded in the target and must not be touched. These are marked `"static": true` in `specs.json` (e.g. `TIPO_DEBITO`, `OPCAO_RECOMPRA`, `NATUREZA_ECONOMICA`).
- **Recover from partial failures gracefully**: a run attempts every table, reports which failed, and a rerun of the failed tables must not duplicate rows the previous run already committed.
- **Load a sample** for smoke tests / timing via `--limit`.

With TRUNCATE gone, the FK-disable machinery loses its main reason to exist, so it is removed entirely (per decision) to simplify the script. The lost truncate also removed the per-run reset that previously bounded duplicate risk, so a PK-based guard (below) provides duplicate-free reruns instead.

## Changes

### 1. Append instead of overwrite
`load_table` no longer truncates. It reads the table's Parquet, repartitions, and writes with `mode("append")`. The connection-resilience design is unchanged: `repartition(DATAGEN_JDBC_NUM_PARTITIONS)` for short-lived per-partition transactions, `batchsize`, transactional `isolationLevel` (`READ_COMMITTED`), and `oracle.jdbc.ReadTimeout` so killed connections fail and Spark retries. Each partition is one transaction; a retried partition re-appends its rows (at-least-once — see Limitations).

### 2. Remove FK / DDL machinery
Delete these (nothing else uses them once truncate is gone):
- Functions: `truncate_sql`, `disable_constraint_sql`, `enable_constraint_sql`,
  `build_constraint_discovery_query`, `constraints_disabled`, `read_rows`,
  `execute_statement`, `discover_constraints`.
- CLI flags: `--no-manage-constraints`, `--validate-constraints`.
- Tests: `TestSqlBuilders`, `TestConstraintsDisabled`, `TestDiscoverConstraints`.

`validate_identifier` is **kept** and applied when building the `dbtable` owner/name, so the Spark JDBC `dbtable` option stays injection-safe even though raw DDL is gone.

### 3. Specs-driven table selection
- New `--specs` argument, default `specs.json`. Read through Spark
  (`spark.sparkContext.textFile(path)` joined + `json.loads`), mirroring
  `etl.py`'s `load_run_config`, so local paths and OCI URIs both work.
- `--tables` / `--tables-file` become **optional** (drop `required=True` on the
  mutually exclusive group; the group stays mutually exclusive).
- New pure function `resolve_load_tables(specs, requested) -> list[str]`:
  - `is_static(specs, table)` ≡ truthy `specs.get(table_path_name(table).upper(), {}).get("static")`.
  - If `requested` is a non-empty list: return those that are **not** static, in
    the given order; log each skipped static table. A requested table absent
    from `specs` is treated as non-static (loaded) and logged at info level.
  - If `requested` is `None`: return all keys of `specs` whose entry is not
    static, in `specs` (insertion) order.
  - If the result is empty: log an error and `sys.exit(1)`.
- New `load_specs(spark, path) -> dict` (Spark-read + `json.loads`).

### 4. Sample loads via `--limit`
- New `--limit` argument validated by `positive_int` (same as `save_tables.py`).
- In `load_table`, when `limit` is set, apply `df.limit(limit)` after reading
  Parquet and before repartition/write.
- Unlike `save_tables.py` (which writes samples to a separate `_limit_<N>` path),
  there is no separate target on the load side: `--limit` appends up to `N` rows
  **into the real target table**, per table. The log marks the sample.

### 5. Duplicate guard — skip synthetic rows whose PK already exists

Spark's JDBC writer has no upsert/insert-ignore, so the guard runs in Spark
before the append, using only `SELECT` (no `CREATE`/`MERGE`/`DROP`, no extra
privileges). It is **bounded by the synthetic PK range** so it scales to
600M-row targets — the pre-existing real data is never read.

Rationale: synthetic PKs are minted above the target's current max
(`append_after_max_pk`), so a synthetic key can only already exist if a prior
run of this same batch committed it. We therefore only look for collisions
inside the synthetic batch's own PK range.

Per table, after reading Parquet (and applying any `--limit`):

1. Determine `pk_cols` from `specs[bare_name]["pk_cols"]`.
2. **Applicability:** the guard runs only when there is exactly one PK column
   and its DataFrame type is numeric. Otherwise (composite PK, non-numeric PK,
   or table absent from specs / missing `pk_cols`) log a warning and append
   without the guard. The non-static tables actually loaded have single numeric
   `NUM_ID_*` PKs, so the guard applies to them; the fallback is a safety net.
3. Compute the synthetic batch's `[lo, hi]` = `df.agg(min(pk), max(pk))` (one
   projected-column pass — cheap; on an empty DataFrame both are null → skip the
   guard and append nothing).
4. Read existing keys in range via a **partitioned** JDBC read:
   `dbtable = "(SELECT <pk> FROM <owner.table> WHERE <pk> BETWEEN <lo> AND <hi>) q"`
   with `partitionColumn=<pk>`, `lowerBound=lo`, `upperBound=hi`,
   `numPartitions=DATAGEN_JDBC_NUM_PARTITIONS` — so even a large rerun read is
   parallel and connection-kill-resilient. `lo`/`hi` are numeric literals
   (validated numeric); `<pk>` and `owner.table` pass `validate_identifier`.
5. If the existing-keys DataFrame is empty (the common first-run case) → skip
   the join entirely, append `df` as-is.
6. Otherwise `df.join(existing_keys, on=pk_col, how="left_anti")` → append only
   keys not already present.

This makes every (re)load idempotent within the synthetic range: a
partially-failed table, rerun, skips exactly the keys its committed partitions
already wrote, with no read of the 600M pre-existing rows. We use the
committed-key *set* (not just `MAX(pk)`) because committed partitions are not a
contiguous PK prefix — a max-only filter could skip never-inserted rows.

### 6. Clear, structured logging
All to stderr (unbuffered), no color codes, thousands separators on row counts,
consistent `[i/N] TABLE: <phase>` structure.

- Run header:
  ```
  Load run: specs=specs.json, mode=APPEND, partitions=256, batchsize=10000, limit=none
  Resolved 22 table(s) to load
  Skipped 15 static table(s): TIPO_DEBITO, OPCAO_RECOMPRA, NATUREZA_ECONOMICA, ...
  ```
- Per table (first run — no existing keys in range):
  ```
  [3/22] LANCAMENTO: reading oci://.../LANCAMENTO
  [3/22] LANCAMENTO: 38,201,544 synthetic rows; PK NUM_ID_LANCAMENTO range [..,..]
  [3/22] LANCAMENTO: 0 existing keys in range -> appending 38,201,544 rows to ADMIN.LANCAMENTO in 256 partitions
  [3/22] LANCAMENTO: appended 38,201,544 rows in 412.3s
  ```
- Per table (rerun — some keys already loaded):
  ```
  [3/22] LANCAMENTO: 1,234 existing keys in range -> skipping already-loaded, appending 38,200,310 rows ...
  ```
- When the guard does not apply, the phase line says
  `no PK guard (pk_cols=<...>) -> appending <N> rows`.
- With `--limit`, the synthetic-rows line notes `(limit N)`.
- Synthetic count is `df.count()` (after `--limit`); appended count is the
  post-anti-join count.
- Summary:
  ```
  Finished: loaded 22/22 table(s), 41,203,118 rows in 1832.4s
  ```
  Failed tables listed on a `Failed tables: ...` line; non-zero exit if any failed.

## Data flow (main)

1. Parse args (`--specs` default `specs.json`; optional `--tables`/`--tables-file`;
   `--limit`; `--continue-on-error`).
2. `get_load_env()` (unchanged).
3. Create Spark session.
4. `specs = load_specs(spark, args.specs)`.
5. `requested = parse_tables(args.tables, args.tables_file)` if either was given,
   else `None`.
6. `tables = resolve_load_tables(specs, requested)`.
7. `load_tables(spark, config, specs, tables, continue_on_error, limit)` — `specs`
   is passed through so `load_table` can read each table's `pk_cols` for the guard.

## Error handling

- **Graceful partial failures:** per-table `try/except`; a failure logs and is
  recorded, and the run continues to the next table when `--continue-on-error` is
  set. At the end, failed tables are listed and the process exits non-zero. Reruns
  of failed tables are duplicate-free thanks to the PK guard, so recovery is
  "rerun the failed tables."
- `load_specs` failure (unreadable/invalid JSON) is fatal: log and `sys.exit(1)`.
- Empty resolved table set is fatal (`sys.exit(1)`).
- A guard read failure (e.g. existing-keys query) fails that table like any other
  per-table error; it does not partially append.

## Known limitations

- The PK guard makes reruns duplicate-free, but parallel JDBC append remains
  at-least-once **within a single run**: a partition that commits and is then
  reported failed gets retried and re-inserts its rows — the guard's existing-keys
  snapshot was taken before the write, so it cannot catch this. This narrow
  within-run self-duplication is the accepted residual; closing it fully would
  require a server-side staging+MERGE (needs `CREATE TABLE` on the target), which
  is out of scope.
- The guard applies only to single-column numeric PKs (all non-static tables
  loaded today qualify); other tables append without it and log a warning.

## Testing

Pure-Python unit tests (run via `uv run --no-project --with pytest python -m pytest`):

- Keep: `TestValidateIdentifier`, `TestParseTables`, `TestGetLoadEnv`,
  `TestNameAndPathHelpers`, `TestConnectionProperties`.
- Remove: `TestSqlBuilders`, `TestConstraintsDisabled`, `TestDiscoverConstraints`.
- Add `TestResolveLoadTables`:
  - Requested list drops static tables, keeps order.
  - Requested table absent from specs is kept (non-static).
  - `requested=None` returns all non-static specs keys in order.
  - Schema-qualified / lowercase requested names match specs keys via
    `table_path_name(...).upper()`.
  - Empty result exits.
- Add `TestPositiveInt` (mirror `save_tables.py`): rejects non-int / <= 0.
- Add `TestGuardHelpers` for the pure pieces of the duplicate guard:
  - `pk_cols_for(specs, table)` returns the spec's `pk_cols` (matched via
    `table_path_name(...).upper()`), `[]` when absent.
  - `guard_applies(pk_cols, is_numeric)` is true only for a single numeric PK;
    false for composite, empty, or non-numeric.
  - `build_existing_keys_query(owner, table, pk_col, lo, hi)` produces the bounded
    `SELECT <pk> ... WHERE <pk> BETWEEN <lo> AND <hi>` subquery, validates
    identifiers, formats `lo`/`hi` as numeric literals, and rejects non-numeric
    bounds / bad identifiers.

`load_specs`, the Spark read of existing keys, the anti-join, and the write need a
live Spark session and are covered by the real-DB validation step:
- Load a sample with `--limit`; confirm appended row counts and that static tables
  are skipped in the log.
- Run a full table once, then **run it again**; confirm the second run logs
  "existing keys in range" and appends 0 new rows (idempotent rerun).
- Confirm a non-numeric/composite-PK table (if any is introduced) logs the
  "no PK guard" warning and still appends.
