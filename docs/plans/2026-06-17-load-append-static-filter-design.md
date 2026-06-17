# Load Append + Static Filter Design (load_tables.py)

**Date:** 2026-06-17
**Purpose:** Change `load_tables.py` to append (not overwrite/truncate), load only non-`static` tables from `specs.json`, support sampled loads via `--limit`, and log the run clearly. Removes the FK/DDL constraint machinery that overwrite required.

## Motivation

`load_tables.py` currently overwrites each target table (explicit `TRUNCATE` + `mode("append")`) and disables/re-enables foreign keys around the truncate (because TRUNCATE is blocked by incoming FKs, ORA-02266). The pipeline now needs to:

- **Append** synthetic rows to existing target tables rather than replacing them.
- **Skip reference/lookup tables** that are pre-loaded in the target and must not be touched. These are marked `"static": true` in `specs.json` (e.g. `TIPO_DEBITO`, `OPCAO_RECOMPRA`, `NATUREZA_ECONOMICA`).
- **Load a sample** for smoke tests / timing via `--limit`.

With TRUNCATE gone, the FK-disable machinery loses its main reason to exist, so it is removed entirely (per decision) to simplify the script.

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

### 5. Clear, structured logging
All to stderr (unbuffered), no color codes, thousands separators on row counts,
consistent `[i/N] TABLE: <phase>` structure.

- Run header:
  ```
  Load run: specs=specs.json, mode=APPEND, partitions=256, batchsize=10000, limit=none
  Resolved 22 table(s) to load
  Skipped 15 static table(s): TIPO_DEBITO, OPCAO_RECOMPRA, NATUREZA_ECONOMICA, ...
  ```
- Per table:
  ```
  [3/22] LANCAMENTO: reading oci://.../LANCAMENTO
  [3/22] LANCAMENTO: 38,201,544 rows -> appending to ADMIN.LANCAMENTO in 256 partitions
  [3/22] LANCAMENTO: appended 38,201,544 rows in 412.3s
  ```
  Row count comes from `df.count()` (after any `--limit`); on Parquet this reads
  footer metadata and is cheap. With `--limit`, the second line notes `(limit N)`.
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
7. `load_tables(spark, config, tables, continue_on_error, limit)`.

## Error handling

- Per-table `try/except` with `--continue-on-error`; non-zero exit if any failed
  (unchanged).
- `load_specs` failure (unreadable/invalid JSON) is fatal: log and `sys.exit(1)`.
- Empty resolved table set is fatal (`sys.exit(1)`).

## Known limitations (carried over)

- Parallel JDBC append is at-least-once: a partition that commits but is then
  reported failed is retried and duplicates that partition's rows. With append
  (no truncate) there is no per-run reset, so a failed-then-retried run can leave
  duplicates in the target; rerunning a fully failed table also re-appends. This
  is acceptable for the synthetic-data use case; dedup/MERGE is out of scope.

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

`load_specs`, `load_table`, and `load_tables` need a live Spark session and are
covered by the real-DB validation step (load a sample with `--limit`, confirm row
counts appended and static tables skipped in the log).
