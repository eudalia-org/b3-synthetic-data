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
