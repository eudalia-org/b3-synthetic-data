# data-gen

Synthetic data generation pipeline for Oracle Cloud.

## Setup

1. Install dependencies: `uv sync` or `pip install -r requirements.txt`
2. Download Oracle JDBC drivers to project root (ojdbc8.jar, oraclepki.jar, osdt_cert.jar, osdt_core.jar, ucp.jar)
3. Configure OCI Vault secrets (see secrets.py for required secrets)

## Usage

```bash
python etl.py --config specs.json --date <YYYYMMDD>
```

The single `etl.py` entrypoint runs extract, multi-table relational synthesis, and load in one Spark session.

Example config:

```json
{
  "tables": {
    "CUSTOMERS": {
      "pk_cols": ["CUSTOMER_ID"]
    },
    "ORDERS": {
      "pk_cols": ["ORDER_ID"],
      "foreign_keys": [
        {
          "columns": ["CUSTOMER_ID"],
          "parent_table": "CUSTOMERS",
          "parent_columns": ["CUSTOMER_ID"]
        }
      ]
    }
  },
  "validate_mode": "full",
  "relationship_policy": "warn_and_skip"
}
```

The config path is read through Spark, so local paths and Spark-readable Object Storage URIs are supported when configured in the runtime environment.

## Fast Raw Table Extract

`save_tables.py` extracts source Oracle tables directly to raw Parquet. It avoids a
pre-write `count()`. Full-table reads are parallelized across
`DATAGEN_JDBC_NUM_PARTITIONS` (default 32) ROWID-range JDBC partitions computed from
the table's extent map; no numeric partition column is required. Set
`DATAGEN_JDBC_PARTITION_COLUMNS="OWNER.TABLE=COLUMN"` to use numeric-column
partitioning for specific tables instead. If extent metadata is unavailable (missing
privileges, empty table, or a view), the read falls back to a single JDBC partition.
Note: parallel partitions read in separate Oracle sessions, so the extract is not a
single consistent snapshot if the source changes mid-run.

Connections abort socket reads after `DATAGEN_JDBC_READ_TIMEOUT_MS` (default
600000 = 10 min) of silence, so a dropped network path fails the task and Spark
retries it instead of hanging the job. LOB columns are prefetched inline up to
`DATAGEN_JDBC_LOB_PREFETCH` bytes (default 262144) to avoid per-row locator round
trips. For long-running extracts, also add `(ENABLE=BROKEN)` inside the
`DESCRIPTION` of the JDBC connect string to enable TCP keepalive, which stops
firewalls/load balancers from silently dropping busy connections.

```bash
python save_tables.py --tables BIG_TABLE
```

Run a limited sample first to estimate runtime without overwriting the full extract path:

```bash
python save_tables.py --tables BIG_TABLE --limit 100000
```

Full runs write to `.../<TABLE>` and limited runs write to `.../<TABLE>_limit_<N>`.
Both log elapsed time per table.

Set `DATAGEN_RAW_PREFIX` to place extracts under a prefix inside the target bucket:

```bash
DATAGEN_RAW_PREFIX=onprem-export python save_tables.py --tables BIG_TABLE
```

This writes to `.../onprem-export/<TABLE>`.

Optional performance environment variables:

```text
DATAGEN_RAW_PREFIX=onprem-export
DATAGEN_JDBC_FETCH_SIZE=50000
```

If a table does have a safe numeric split column, you can opt into Spark JDBC partitioning:

```text
DATAGEN_JDBC_NUM_PARTITIONS=64
DATAGEN_JDBC_PARTITION_COLUMNS=BIG_TABLE=ID,OTHER_SCHEMA.OTHER_TABLE=OTHER_ID
```

Without `DATAGEN_JDBC_PARTITION_COLUMNS`, the script uses one JDBC partition and does not
query Oracle metadata to discover one.

The script writes Parquet with Spark's datetime rebase mode set to `CORRECTED` so Oracle
DATE/TIMESTAMP values before Spark's ancient-date thresholds do not fail the raw extract.

To estimate how to split tables across multiple Data Flow jobs from the VDI/on-prem side,
query Oracle table statistics:

```bash
export ORACLE_DB_USER=YOUR_SCHEMA
export ORACLE_DB_PASSWORD='YOUR_PASSWORD'
export ORACLE_DSN='host:1521/service_name'

python scripts/oracle_table_sizes.py --tables BIG_TABLE,OTHER_TABLE --format csv
```

The helper uses `USER_TABLES` and `USER_TAB_STATISTICS`, so it avoids `SYS`/`DBA_*`/
segment views that often require elevated catalog privileges. It reports tables owned by
the connected `ORACLE_DB_USER`. Row count and size are estimates from Oracle statistics:
`NUM_ROWS`, `AVG_ROW_LEN`, and `BLOCKS * --block-size` where `--block-size` defaults to
8192 bytes. Check `last_analyzed`/`stale_stats` before relying on estimates for exact job
sizing.

For a table in another schema that the connected user can query, opt into an exact count:

```bash
python scripts/oracle_table_sizes.py \
  --tables CETIP.CREDITO \
  --allow-external-count \
  --compressed-bytes-per-row 54.5 \
  --format csv
```

Cross-schema mode runs `COUNT(*)`, so it can be expensive on very large tables. It cannot
read segment size without additional catalog grants; `--compressed-bytes-per-row` lets you
estimate output size from a measured limited extract.

## Fast Parallel Load

`load_tables.py` loads per-table Parquet into the target Oracle database through
many short-lived parallel JDBC partitions (each partition commits in seconds, so a
load survives the Data Flow→ADB connection killer; Spark retries any killed
partition). It **appends** to existing target tables. Run one Data Flow job per big
table, or omit `--tables` to load every non-static table from the specs.

```bash
python load_tables.py --tables LANCAMENTO            # one table
python load_tables.py                                # all non-static tables in specs.json
python load_tables.py --tables LANCAMENTO --limit 100000   # sample load
```

Reads `{DATAGEN_LOAD_BASE_URI}/{DATAGEN_LOAD_PREFIX}/<TABLE>`. Tables marked
`"static": true` in `--specs` (default `specs.json`) are skipped — they are
pre-loaded reference data. `--limit N` appends at most N rows per table into the
real target (no separate sample target).

Duplicate guard: before appending, synthetic rows whose primary key already exists
in the target are skipped. The check is bounded to the synthetic batch's
`[min, max]` PK range (synthetic PKs are minted above the current max), so it reads
only that range — never the full target — and is skipped entirely when the range is
empty (the common first-load case). This makes rerunning failed tables
duplicate-free. The guard applies to single-column numeric PKs (from specs
`pk_cols`); other tables append without it and log a warning.

Partial failures are handled gracefully: with `--continue-on-error` the run attempts
every table, lists failed ones, and exits non-zero — rerun the failed tables (the PK
guard keeps the rerun duplicate-free).

Each run writes a rollback manifest to `{DATAGEN_LOAD_BASE_URI}/_load_manifests/<run_id>`
(the `run_id` is auto-generated and logged; override with `--run-id`). To undo a load,
delete the rows it appended:

```bash
python scripts/rollback_load.py --run-id <run_id>
```

It reads the manifest and deletes everything above each table's pre-load `MAX(pk)`,
in PK chunks (`--chunk-size`, default 5,000,000), and is idempotent. Only
single-column numeric-PK tables are rolled back; others are logged as needing a DB
restore point.

Configuration: `DATAGEN_TARGET_JDBC_URL`, `DATAGEN_TARGET_DB_PASSWORD`,
`DATAGEN_TARGET_DB_USER` (default `ADMIN`), `DATAGEN_LOAD_BASE_URI`,
`DATAGEN_LOAD_PREFIX`, `DATAGEN_JDBC_NUM_PARTITIONS` (default 256),
`DATAGEN_JDBC_BATCH_SIZE` (default 10000), `DATAGEN_JDBC_READ_TIMEOUT_MS`
(default 600000). Set `spark.task.maxFailures` high (e.g. 8) in the Data Flow job.

Note: the guard makes reruns duplicate-free, but parallel JDBC append is
at-least-once within a single run (a partition that commits then is reported failed
is retried and re-inserts its rows). Closing that fully would need a server-side
staging+MERGE (CREATE TABLE on the target), which is out of scope.

## VDI One-Time ROWID Migration

`scripts/migrate_rowid_to_oci.py` is for the on-prem access pattern where the VDI can reach both
Oracle and OCI Object Storage, but OCI cannot reach Oracle directly. It exports each table
by Oracle `ROWID` ranges, writes one local Parquet chunk at a time, uploads that chunk with
the OCI CLI, then deletes the local file after a successful upload.

Install the VDI-specific dependencies and OCI CLI before running it:

```bash
pip install -r requirements-vdi-migration.txt
oci os ns get
```

Set the Oracle connection environment variables:

```bash
export ORACLE_DB_USER=YOUR_SCHEMA
export ORACLE_DB_PASSWORD='YOUR_PASSWORD'
export ORACLE_DSN='host:1521/service_name'
```

Run a dry-run first to confirm that ROWID chunks can be generated:

```bash
python scripts/migrate_rowid_to_oci.py \
  --tables BIG_TABLE_1,BIG_TABLE_2 \
  --bucket your-bucket \
  --prefix onprem-export/20260605 \
  --dry-run
```

Run the migration:

```bash
python scripts/migrate_rowid_to_oci.py \
  --tables-file tables.txt \
  --bucket your-bucket \
  --prefix onprem-export/20260605 \
  --config-file C:\\Users\\me\\.oci\\config \
  --profile your-profile \
  --auth security_token \
  --work-dir D:\\rowid_export_work \
  --target-blocks 131072 \
  --fetch-size 10000 \
  --compression snappy \
  --continue-on-error
```

The checkpoint file is stored under the work directory as
`rowid_migration_checkpoint.jsonl`. Re-running the same command skips chunks already marked
as uploaded. It also records per-chunk row counts, Parquet size, export duration, upload
duration, and rough throughput so a small-table pilot can estimate large-table runtime.
Avoid table moves/shrinks/reorgs during the migration because those operations can change
Oracle `ROWID` values.

## OCI Data Flow Deployment

### Build Archive

Use Oracle's recommended Docker image to build a compatible archive:

```bash
docker run --rm -v $(pwd):/app -w /app ghcr.io/oracle/oraclelinux8-python:3.11 \
  bash -c "pip install -r requirements.txt -t python/ && zip -r archive.zip python/"
```

### Upload Archive

```bash
oci os object put --bucket-name datagen-apps --file archive.zip --name archive.zip --force
```

### Upload ETL Script

```bash
oci os object put --bucket-name datagen-apps --file etl.py --name etl.py --force
```

### Run on Data Flow

Use `--archive-uri` to include the dependencies archive when creating or running Data Flow applications.
