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
