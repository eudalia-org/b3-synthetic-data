# Pipeline Orchestrator

The runner executes immutable `engorda -> validate -> load` branches through OCI Data Flow.
Copy `docs/pipeline-config.example.json`, replace its OCI values, and keep one config
file per environment. `products` may enable any nonempty subset of the registry; both
`run` and `adopt-inputs` reject products not enabled in that environment's config.
Each product may define persistent `engorda`/`validate`/`load` overrides. Precedence is:
stage defaults, common CLI flags, product config, then explicit `--set`.

```json
"lci": {
  "capabilities": ["engorda", "validate", "load"],
  "engorda": {"n_instrumentos": 50000, "fator_k": 2}
}
```

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
Object Storage preflight samples one object with `--limit 1`; it never runs
`object list --all` over large RAW/synthetic prefixes. Deep integrity remains in
the plan-v2 snapshot schemas, counts, root IDs, and spec hash.
For validate-only products, adopt the existing synthetic output too:

```powershell
--product gravame `
--synthetic-uri gravame=oci://bucket@namespace/existing-gravame-output
```

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
The plan job writes an `engorda_plan` schema v2 artifact. It snapshots the selected
lote to a location derived by engorda and commits that location as the plan's
`selected_lote` descriptor; there is intentionally no runner snapshot-path option.
Materialization consumes that committed snapshot instead of rebuilding the lote.
Schema v1 plans are incompatible with reservation and must be regenerated with the
current plan job; reservation artifacts remain schema v1.
Before burning ranges, the runner verifies the plan-v2 snapshot descriptor's UUID,
table paths, schema-object presence, source row counts, and optional selective-missing
dataset contract against the hashed plan. Spark performs the deeper
Parquet/StructType checks during materialization.
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

Before creating the local manifest or submitting any job, a live run verifies that
the run root, remote manifest, and every product's exact synthetic output URI are
absent. An existing synthetic output fails closed. Validate-only runs skip the
materialize-output checks because they do not create synthetic output.

The first tracer supports `cdb_simplificado`, `cdb_resgate`, `cdb_escalonamento`,
`rdb_inclusao`, `rdb_resgate`, `lci`, `lca`, `ccb_pppre`, `ccb_pfpre`, `ccb_pgrpre`,
`ccb_favcp`, `ccb_fapre`, `gravame`, `lastro`, and `direito_creditorio`. Validation
accepts `PASS` or `PARTIAL` only when the report contains zero ERROR findings and its
product/input lineage matches the branch exactly.
The five CCB variants and `gravame` support `engorda -> validate -> load`. `lastro` and
`direito_creditorio` support validation and load of adopted synthetic outputs, but not
engorda because the generic generator does not provide their root/domain contract yet.

## Load validated output

Load is APPEND-only and requires explicit approval. It consumes the exact synthetic
URI and validation report recorded by the branch. PASS and PARTIAL reports are
accepted only with zero ERROR findings and matching product/input lineage. The loader
does not repeat its separate Oracle preflight; it still applies the numeric PK guard.

```powershell
uv run --no-project .\run_pipeline.py run `
  --config .\pipeline-qab.json `
  --product cdb_resgate `
  --from validate `
  --to load `
  --upstream-manifest .\adopted-inputs.json `
  --approve-load
```

Use `--from load --to load` with the manifest from a prior successful validation to
load later. Validation reports do not expire. `--dry-run` never requires approval and
prints `approval_required=true` without making remote calls.

Loads run one product at a time in `--product` order under the environment's renewable
`load.lease_uri`. A failed product does not block later products, but no load receives
an automatic whole-job retry. The runner never rolls back automatically.

Before the first INSERT, the load application writes one immutable JSON manifest under
`products/<product>/load/manifest.json`. It records the ordered validated table
inventory, explicit write transformations, and each synthetic numeric PK range. A
create-once claim under `load.claim_root` blocks an unnoticed second attempt. Resume a
failed or unknown attempt explicitly:

```powershell
--resume-load-manifest cdb_resgate=oci://bucket@namespace/.../load/manifest.json `
--approve-load
```

A known successful attempt cannot be resumed. Manual rollback accepts the exact
manifest URI and deletes only its reserved synthetic PK ranges, in child-before-parent
order:

```powershell
python scripts/rollback_load.py `
  --manifest-uri oci://bucket@namespace/.../load/manifest.json `
  --dry-run
```

If OCI submission fails before returning a Data Flow run ID, the claim is retained
because the outcome is ambiguous. The runner also marks the environment load lease as
quarantined and blocks subsequent loads without an automatic expiry. Inspect OCI runs
before manually removing the claim and lease; the remote load may still be active.
