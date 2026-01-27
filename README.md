# data-gen

Synthetic data generation pipeline for Oracle Cloud.

## Setup

1. Install dependencies: `uv sync` or `pip install -r requirements.txt`
2. Download Oracle JDBC drivers to project root (ojdbc8.jar, oraclepki.jar, osdt_cert.jar, osdt_core.jar, ucp.jar)
3. Configure OCI Vault secrets (see secrets.py for required secrets)

## Usage

```bash
python extract.py <table> <YYYYMMDD>
python transform.py <table> <YYYYMMDD> [key_columns]
python load.py <table> <YYYYMMDD>
```

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

### Upload ETL Scripts

```bash
oci os object put --bucket-name datagen-apps --file extract.py --name extract.py --force
oci os object put --bucket-name datagen-apps --file transform.py --name transform.py --force
oci os object put --bucket-name datagen-apps --file load.py --name load.py --force
oci os object put --bucket-name datagen-apps --file secrets.py --name secrets.py --force
```

### Run on Data Flow

Use `--archive-uri` to include the dependencies archive when creating or running Data Flow applications.
