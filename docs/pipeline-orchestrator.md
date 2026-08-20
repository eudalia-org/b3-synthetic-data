# Pipeline Orchestrator

The first tracer runs immutable `engorda -> validate` branches through OCI Data Flow.
Copy `docs/pipeline-config.example.json`, replace its OCI values, and keep one config
file per environment.

`scripts/run_pipeline.py` is self-contained for operator distribution and includes
PEP 723 metadata for Python 3.11 and Click. Copy that one Python file to the Windows
workstation; `uv run` installs Click automatically. `oci_dataflow.py` and
`pipeline_reservations.py` remain repository compatibility facades and are not needed
beside the distributed runner.

## Adopt existing inputs

Register an existing RAW and faltantes snapshot before starting at engorda:

```powershell
uv run --allow-insecure-host pypi.org `
  --allow-insecure-host files.pythonhosted.org `
  --no-project `
  .\run_pipeline.py adopt-inputs `
  --config .\pipeline-qab.json `
  --product cdb_resgate `
  --product rdb_resgate `
  --raw-uri oci://bucket@namespace/raw-snapshot `
  --faltantes-uri oci://bucket@namespace/faltantes.parquet `
  --output-manifest .\adopted-inputs.json `
  --profile p-lmirabella `
  --auth security_token `
  --config-file C:\Users\p-lmirabella\.oci\config `
  --cert-bundle C:\Users\p-lmirabella\Documents\corp-root-ca.cer
```

Add `--dry-run` to validate and print the adoption plan without OCI calls.

## Run engorda through validation

```powershell
uv run --allow-insecure-host pypi.org `
  --allow-insecure-host files.pythonhosted.org `
  --no-project `
  .\run_pipeline.py run `
  --config .\pipeline-qab.json `
  --product cdb_resgate `
  --product rdb_resgate `
  --from engorda `
  --to validate `
  --upstream-manifest .\adopted-inputs.json `
  --n-instrumentos 100 `
  --fator-k 1 `
  --max-concurrency 4 `
  --poll-seconds 30 `
  --oci-timeout-seconds 60 `
  --auth-refresh-seconds 1800 `
  --profile p-lmirabella `
  --auth security_token `
  --config-file C:\Users\p-lmirabella\.oci\config `
  --cert-bundle C:\Users\p-lmirabella\Documents\corp-root-ca.cer
```

Use `--dry-run` first. It performs no OCI or Oracle calls and prints the resolved
DAG, immutable paths, Data Flow application arguments, and reservation contract.
The engorda config must include an Object Storage `query_num_if_sql` URI. Upload
`datagen/queries_produtos.sql` there; both plan and materialize receive that exact
URI and freeze it into plan lineage, so no Data Flow local companion file is needed.
Live execution prints every submission and every observed Data Flow state. Change
`--poll-seconds 30` to control the status interval. Every OCI CLI call announces
itself and fails after `--oci-timeout-seconds 60` instead of waiting indefinitely.
With `--auth security_token`, the runner validates credentials through the configured
Data Flow Application before Object Storage preflight; it does not call the unreliable
`oci session validate` path. A real 401 prompts for refresh and, if refresh fails,
offers browser authentication. `adopt-inputs` probes Object Storage instead. Use
`--no-auth-prompt` for non-interactive automation; `--region` overrides the region
read from the OCI profile when browser auth is needed.
Normal OCI subprocesses automatically decline the CLI's own hidden re-auth prompt;
only the runner prompts. Browser authentication inherits the terminal visibly.
For long engorda runs, the runner refreshes once before submission and every
`--auth-refresh-seconds 1800` during polling. Set `0` to disable proactive refresh;
a reactive 401 still uses the interactive refresh/browser flow.

The first tracer supports `cdb_simplificado`, `cdb_resgate`, `cdb_escalonamento`,
`rdb_inclusao`, `rdb_resgate`, `lci`, and `lca`. Validation accepts `PASS` or
`PARTIAL` only when the report contains zero ERROR findings and its product/input
lineage matches the branch exactly.
