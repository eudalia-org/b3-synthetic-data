# Recover CDB/RDB loads rejected by the original static defaults

## Cause and fix

Engorda reads the shared specification, then marks its selected product tables
`static=False` in memory. The original specification object can therefore mark
`AMORTIZACAO`, `INSTRUMENTO_FINANCEIRO`, and the other emitted tables static while
the generated output and product-validation report are correct.

The loader previously reread that original object and rejected the first static
inventory entry. `load_tables.py` now builds an effective in-memory spec for
`cdb_simplificado`, `cdb_resgate`, `cdb_escalonamento`, `rdb_inclusao`, and
`rdb_resgate`. Only tables in **both** the accepted report and the matching product's
engorda table set are marked non-static. It does not rewrite the input spec or
add tables absent from the report. The same effective spec drives ordering,
optional FK preflight, PK capture, and insertion. Static reference tables remain
blocked, and the original PK/FK metadata is preserved.

The loader is distributed as one file, so its CDB/RDB table sets are kept local;
regression tests compare each set against the generator's canonical definition.

### Clone maps in existing validation inventories

An additional old-report failure is `Inventory table 'MAPA_CLONE_COD_IF' is absent
from specs`. The validator formerly put every readable output directory in
`table_inventory`, including all three clone maps. Those maps have no Oracle
table specification because they are provenance artifacts.

Deploy the updated `datagen/load_tables.py` to consume existing accepted reports:
it excludes exactly `MAPA_CLONE_NUM_IF`, `MAPA_CLONE_COD_IF`, and
`MAPA_CLONE_COD_OPERACAO` before table resolution, PK capture, and insert. It retains
the original report and all regular table checks. Deploy updated
`scripts/validate_products.py` for future reports to put these names in a separate
`auxiliary_artifacts` list; the validator still reads and uses their Parquet data.
The updated standalone runner rejects an inventory containing only maps before
creating a claim. The deployed files remain self-contained.

The missing-map-spec failure is also before inserts and before load-manifest
creation. If it occurred on a retry, inspect that **latest** failed pipeline
manifest: its load claims have new ETags even if the synthetic input URI and claim
path are unchanged. The earlier run's claim-release script is bound to the earlier
ETags and is not the recovery record for the new attempts.

## What the reported failure means

The traceback ends in `resolve_load_tables`, before `capture_manifest_entries`,
`write_manifest`, and `load_tables`. An attempt with that exact traceback made no
inserts and did not create its load-attempt manifest. The pipeline runner still
created a load **claim before submitting** the Data Flow job. A plain rerun against
the same synthetic input will therefore encounter the existing claim.

This conclusion applies to attempts confirmed to have this pre-insert failure.
A missing manifest alone does not establish that another attempt is no longer
running. Preserve the failed pipeline manifest and Data Flow run/log evidence.

## Load-only recovery

1. Deploy the updated `datagen/load_tables.py` to the Data Flow application selected
   by `applications.load`. Existing engorda outputs, validation reports, and specs
   can be reused for this fix.
2. For each product, inspect its final pipeline load node and corresponding Data
   Flow run. Confirm a terminal failure with this traceback, and confirm the exact
   node `output_uri` (the load-attempt manifest) does not exist. After that manual
   confirmation, release **only its exact `claim_uri`**. Do not remove the key
   reservation ledger, other claims, or a lease held by an active/ambiguous load.
3. Prepare a separate `ADOPTED` upstream manifest containing the exact successful
   `synthetic` and `validation_report` descriptors for those products, preserving
   input lineage and offline/load-eligibility metadata. The overall failed pipeline
   manifest cannot be passed directly: the reader requires `ADOPTED` or `SUCCEEDED`.
   Keep that original failed record unchanged. The current `adopt-inputs` CLI can
   adopt synthetic URIs but has no validation-report option; reuse of those reports
   requires constructing this separate manifest from the recorded artifacts.
4. Start a fresh run using that local upstream file:

   ```powershell
   uv run --no-project .\run_pipeline.py run `
     --config .\pipeline-config.json `
     --product cdb_simplificado `
     --product cdb_resgate `
     --product cdb_escalonamento `
     --product rdb_inclusao `
     --product rdb_resgate `
     --from load --to load `
     --upstream-manifest .\validated-load-inputs.json `
     --approve-load
   ```

Add the usual OCI authentication flags. `--resume-load-manifest` is appropriate
when an earlier immutable load manifest exists; it cannot resume a missing
manifest from this pre-insert failure. If any attempt reached insertion, use its
normal manifest-linked recovery process instead of the pre-insert claim release.
