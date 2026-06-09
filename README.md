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

`save_tables.py` extracts source Oracle tables directly to raw Parquet. For large tables,
it now avoids a pre-write `count()` and tries to parallelize JDBC reads by discovering a
single-column numeric primary key.

```bash
python save_tables.py --tables BIG_TABLE --date 20260602
```

Run a limited sample first to estimate runtime without overwriting the full extract path:

```bash
python save_tables.py --tables BIG_TABLE --date 20260602 --limit 100000
```

Limited runs write to `.../<TABLE>/<YYYYMMDD>_<TABLE>_limit_<N>.parquet` and log elapsed
time per table.

Optional performance environment variables:

```text
DATAGEN_JDBC_FETCH_SIZE=50000
DATAGEN_JDBC_NUM_PARTITIONS=64
DATAGEN_JDBC_PARTITION_COLUMNS=BIG_TABLE=ID,OTHER_SCHEMA.OTHER_TABLE=OTHER_ID
```

Use `DATAGEN_JDBC_PARTITION_COLUMNS` when the fastest split column is not the numeric
primary key. Tune `DATAGEN_JDBC_NUM_PARTITIONS` to available Oracle sessions, Spark
executors, network bandwidth, and Object Storage write throughput.

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
