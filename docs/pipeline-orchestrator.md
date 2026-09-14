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
workstation; `uv run` installs Click automatically. Reservation, Object Storage,
lease, and Data Flow orchestration are implemented directly in that script.
`oci_dataflow.py` remains a repository compatibility facade and is not needed beside
the distributed runner.

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
Add `--osias` to forward that flag to every validator Data Flow run; it does not
affect engorda or load and has no config or `--set` form.
The plan job writes an `engorda_plan` schema v3 artifact. It snapshots the selected
lote to a location derived by engorda and commits that location as the plan's
`selected_lote` descriptor; there is intentionally no runner snapshot-path option.
Materialization consumes that committed snapshot instead of rebuilding the lote.
Schema v1 plans are incompatible with reservation. Existing schema v2 plans and
schema v1 reservations retain their legacy global meu-number allocation behavior.
New reservations and the reservation ledger use schema v2; the runner migrates a
valid schema v1 ledger under the existing lease and ETag/CAS boundary.
Before burning ranges, the runner verifies the plan snapshot descriptor's UUID,
table paths, schema-object presence, source row counts, and optional selective-missing
dataset contract against the hashed plan. Spark performs the deeper
Parquet/StructType checks during materialization.

Plan v3 allocates meu-number ordinals by operational-date/account/TOS groups. One
shared interval may be reused by disjoint groups, but overlapping groups receive
non-overlapping intervals through per-group ledger high-water marks. Plans store
only SHA-256 group identifiers and counts, not raw account/TOS values; these hashes
are pseudonymous rather than secret. Live meu-number generation must use the
`plan -> reserve -> materialize` flow. Direct `phase=all` remains available only for
dry/no-Oracle runs because it cannot participate safely in concurrent reservations.
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

`rdb_inclusao` and `rdb_resgate` use distinct validator profiles. Inclusion requires one
`SEM TABELA` RESGATE, no active schedule rows, and `TITULO.QTD_RESGATADA=0`; resgate requires
`COM TABELA` plus an active `CONDICAO_RESGATE` schedule. The generic `rdb` validator name
remains only as a compatibility alias for direct invocations.

## Synthetic CDB codes

For CDB runs affected by the official COD_IF allocator's `ORA-06502` response,
the explicit `engorda.cod_if_allocator=synthetic_cdb` override reserves locally
generated, format-compatible codes above a live Oracle floor. See
[Synthetic CDB codes](synthetic-cdb-codes.md) for deployment, new-plan requirements,
range guarantees, and per-product `--set` examples.

## Quota-aware submissions

When OCI explicitly rejects `data-flow run create` with a complete `LimitExceeded`
service error, the runner waits and retries that submission rather than failing
the product immediately. This covers transient Data Flow capacity exhaustion such
as `vm-total` while other runs are still using or releasing resources.

`--quota-wait-seconds` defaults to `1800` (30 minutes) per remote execution attempt;
set `0` to disable it or choose up to `86400`. Base delays are 30, 60, 120, 240,
then 300 seconds, with +/-20% jitter, capped at 300 seconds and the remaining
budget. The deadline starts at the first quota rejection and includes subsequent
submission-call time. It prevents new waits and adapter submissions after expiry;
it does not kill an in-flight submission or override interactive authentication
and existing OCI command timeouts. An accepted response is tracked even if it
arrives after expiry, avoiding an orphaned run or duplicate submission.

This budget is separate from `--max-retries`, which governs failed remote
executions. A quota rejection has not started a remote job, so it does not consume
that replay budget. Global `--max-retries 0` still permits quota waiting; Oracle
load and explicitly no-retry nodes, including GenAI planning, remain excluded.

`[quota-wait]` logs identify the node, service code, retry number, delay, and
remaining budget. Attempt metadata records quota rejections, waits, retries, and
the latest 20 events; no run OCID is assigned until OCI accepts the submission.
Waits are interruptible, and other active branches continue to be polled. A waiting
submission occupies one scheduler worker. `--max-concurrency` limits workers, not
tenant VM consumption: waiting cannot fix a job whose resource request never fits
the tenant quota, so budget exhaustion remains an explicit failure.

Only positively identified quota rejections are retried. Timeouts, missing run IDs,
malformed or conflicting responses, authentication failures, other service errors,
and polling failures do not cause blind resubmission. Existing load claims and
ambiguous-load quarantine rules are unchanged.

For a longer queue allowance, add `--quota-wait-seconds 3600` to the existing run
command. This recovery applies to the running scheduler, not retrospectively to a
finished FAILED manifest. If engorda already succeeded and only validation was
rejected, adopt that product's existing synthetic URI and run `--from validate
--to validate` under a new run ID; do not rerun engorda merely to recover validation.

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

For CDB/RDB output, the loader reconciles the original spec's static defaults with
engorda's product table set and the accepted validation inventory. A table must be
in both sets to inherit a runtime non-static override; reference tables remain
protected. See [load inventory recovery](load-static-inventory-recovery.md) for
the `AMORTIZACAO is marked static` failure and reuse of already validated outputs.

Clone-mapping Parquet artifacts (`MAPA_CLONE_NUM_IF`, `MAPA_CLONE_COD_IF`,
`MAPA_CLONE_COD_OPERACAO`) are validation evidence, not Oracle load tables. New
schema-v2 reports separate them into `auxiliary_artifacts`; the updated loader
also excludes those exact names from older accepted reports. Unknown inventory
names still fail, and artifact-only inventories cannot create a load claim.

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

## Generate without Oracle

Use `--no-oracle` only for inspection/test artifacts. Engorda skips live FK admission,
target PK floors, business-key allocators, and meu-numero collision checks. It writes
deterministic placeholders and `_DATAGEN_OFFLINE.json`, while pipeline lineage records
`oracle_access=disabled` and `load_eligible=false`.

```powershell
uv run --no-project .\run_pipeline.py run `
  --config .\pipeline-qab.json `
  --product cdb_resgate `
  --from engorda `
  --to engorda `
  --upstream-manifest .\adopted-inputs.json `
  --no-oracle `
  --data-controle-operacional 2026-06-03
```

`--data-controle-operacional YYYY-MM-DD` supplies the operational business date
without querying Oracle. Planning freezes this value; materialization reuses it
even if it runs on a later day. The normal business-date rules still apply,
including `DAT_EMISSAO = DAT_SITUACAO_IF`; audit timestamps remain separate.
If omitted, offline planning uses the engorda timestamp's date, as before.
The option requires offline engorda and an interval containing `engorda`;
pipeline `--dry-run` alone does not imply `--no-oracle`.
For a per-product date, use
`--set cdb_resgate.engorda.controle_operacional_date=2026-06-03` with that
product's `engorda.no_oracle=true`. Offline output remains ineligible for load.

`--to validate` propagates `--no-oracle` to both applications, so the validator skips
Oracle metadata/residual checks and emits a PARTIAL report with
`oracle_access=disabled` and `load_eligible=false`. An interval containing load is
rejected. For one product in a multi-product run, use
`--set cdb_resgate.engorda.no_oracle=true`; it automatically propagates downstream.
For validate-only runs, use `--no-oracle` or
`--set cdb_resgate.validate.no_oracle=true`.

## Stable synthetic key allocation

Allocation freezes sorted source-key/clone-index rows, partition IDs, and row order
before measuring partition sizes. Independent Spark range-exchange executions can
otherwise resample partition boundaries, making prefix offsets overlap and exceed
reserved intervals. The final PK map is materialized before the temporary snapshot
is released. Keys remain stable across retries and partition layouts, with the same
reserved start, count, and spacing; plan/reservation schemas are unchanged.

Cloning and FK remapping no longer force full PK maps into broadcast joins. Only
bounded partition-offset and clone-factor tables are explicitly broadcast. Local
checkpoints are not durable executor-loss recovery: a lost checkpoint must fail
the run, and a retry from the frozen inputs must reproduce the same keys. Keep all
pre-write validation enabled.

The opt-in `tests/test_engorda_pk_scale.py` suite exercises actual `clona_tabela`
and Parquet readback for the five reported CDB/RDB workload shapes, including
K=176 and a 6,396,672-row condition table. All five passed locally on Spark 3.5
with four workers, 2 GiB driver memory, 16 shuffle partitions, a 1 MiB driver-result
limit, and automatic broadcasts disabled. The separate 512-partition reproducer
also passed; it allows 8 MiB for Spark's bounded internal range samples, while
Python still collects at most 512 partition summaries. Enable these tests with
`DATAGEN_SCALE_TESTS=1`; they are not the full business pipeline or an OCI canary.

For deployment, use the updated engorda script and fresh planning as agreed with
the operator, then verify CDB escalonamento and RDB resgate on OCI. Local success
does not substitute for the operator's production confirmation. See
`docs/adr/0003-freeze-partition-layout-before-pk-allocation.md` for the trade-off.

## CCB classification evidence

CCB planning freezes the selected instruments' `RENT_INDEXADOR_TAXA_FLU` and
`FORMA_PAGAMENTO` from RAW `ACTPCCB_CONDICAO_IF`. Identical classification rows
collapse; missing or conflicting classifications fail. This is a three-column
evidence projection, not a declaration that the physical table has a primary key.

Materialization remaps the frozen evidence to synthetic instrument IDs and publishes
`_CCB_CLASSIFICATION/` plus `_CCB_CLASSIFICATION.json` inside the staged output.
The Parquet includes original ID, clone index, synthetic ID, and both classification
fields. It is excluded from physical-table discovery and the validation report's
table inventory; do not add it to the Oracle spec or load list.

CCB `--osias` validation checks content checksums, source-classification consistency,
output URI, instrument/clone-map coverage, and clone-factor coverage before using the
evidence. Validation does not reread RAW or the planning snapshot. A present but
corrupt sidecar is an ERROR, even when an actual ACTPCCB table is available. Existing
complete ACTPCCB exports remain supported when no sidecar is present.

Use a fresh CCB plan and reservations after deploying this change. Old CCB plans
without frozen classification evidence cannot be materialized by the new generator.
The evidence works in live and `--no-oracle` generation; offline load restrictions
remain unchanged. It records source classification and does not independently
recalculate financial classifications from the generated condition rows.

The evidence path uses distributed joins and a versioned bucketed content checksum;
at most 256 aggregate summaries reach the driver, regardless of instrument count.
No classification rows are embedded in JSON. Run the opt-in million-row regression
with `DATAGEN_SCALE_TESTS=1` and `tests/test_ccb_evidence_scale.py`.
The local Spark 3.5 regression completed a 1,000,000-source/1,000,000-synthetic
roundtrip, including Osias checks, in 267 seconds with two workers, a 1 MiB driver
result limit, and broadcast joins disabled. This measures the evidence path, not
the complete Data Flow generation job or its OCI transfer time.

## Final terminal summary

Every real run that reaches the scheduler ends with a summary on stderr. Dry-run and
config/auth/preflight failures before manifest creation keep their existing output.
The summary contains global product counts and elapsed time, followed by one line per
product with wall-clock duration, accepted-run retries, quota waits/retries, and compact states for
`plan/reserve/materialize/validate/load`.

FAILED and CANCELLED products include the problem node, that node's latest attempt
run ID (or `-` if none was created), and a shortened error; complete details remain
in the manifest. A rejected validation submission never borrows its materialization
run ID. The final lines print both
the local and OCI manifest paths plus `upload=SUCCEEDED|FAILED`. A manifest-upload
failure still renders the product summary and changes the pipeline result to FAILED.

## Benchmark GenAI concurrency

Set `genai.max_concurrency` in the environment config. It defaults to `4` and accepts
any positive integer. Start with the same 100-source, `K=1` input at `4`, `8`, `16`, and
`32`; use a new immutable run ID for each measurement. The GenAI manifest reports the
configured/effective concurrency, endpoint status counts, successful-call p50/p95/max
latency, endpoint calls per second, and sources per second. Stop increasing when
throttling appears, p95 latency grows materially, or throughput stops scaling.
