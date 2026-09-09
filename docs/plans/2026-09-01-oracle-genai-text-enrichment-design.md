# Oracle GenAI instrument text enrichment - Design

Status: agreed
Date: 2026-09-01

## Purpose

Add optional Oracle Generative AI dedicated-endpoint support to engorda for small
samples. The feature generates best-effort observational text diversity for cloned
financial instruments. It is not anonymization and does not intentionally create
business scenarios.

The feature is disabled by default. A normal engorda run does not load a GenAI policy,
import the OCI SDK, validate endpoint configuration, call an endpoint, or alter any
additional columns.

When enabled, the pilot may fill absent values and replace existing values. Operators
accept that this can change NoMe behavior even though the prompt does not target a
specific business outcome.

## Evidence

Profiling the extracted RAW Parquet showed that a `StringType` column absent from
`specs.json` is not necessarily free text. The apparent candidates include controlled
labels, identifiers, UUIDs, hashes, timestamps, XML, protocol strings, names, and
polymorphic business values.

The frozen `cdb_simplificado` sample contained 1,000 instruments, 17,964 rows, and
11,980,488 JSON characters. `LANCAMENTO` alone contributed 4,128,868 characters,
primarily through `TXT_XML_LANCAMENTO`. Excluding that XML leaves whole-instrument
context small enough for one request per source instrument in the pilot.

Observed target-column evidence:

| Column | Observed role | Generation maximum |
|---|---|---:|
| `INSTRUMENTO_FINANCEIRO.TXT_CARACT_COMPLEMENTARES` | Mostly opaque identifiers, with occasional capacity-test labels | 772 characters |
| `EVENTO.TXT_OBSERVACAO` | Prose/templates mixed with numeric identifiers and multiline values | 60 characters |
| `OPERACAO.TXT_HISTORICO` | Prose mixed with UUIDs, hashes, timestamps, and protocol strings | 138 characters |

The maxima are observed Parquet maxima, not verified Oracle column domains. The
current orchestrated validator is `scripts/validate_products.py`; it does not consume
the older `schema.json` contract used by `datagen/validate_tables.py`. Oracle-domain
length validation is therefore explicitly deferred.

## Domain Decisions

### Objective

The objective is best-effort observational text diversity. Generated text should look
like realistic Brazilian Portuguese business text. There is no uniqueness acceptance
threshold: generated values may equal the source or a sibling clone. The run only
measures and reports source-change and sibling-uniqueness rates.

### Semantic unit

One source financial-instrument aggregate is the prompt context. Every eligible source
row and clone index receives an independent target cell, but all targets for one source
instrument are requested together so the model can coordinate them.

### Pilot allowlist

The initial `cdb_simplificado` policy contains exactly:

- `INSTRUMENTO_FINANCEIRO.TXT_CARACT_COMPLEMENTARES`
- `EVENTO.TXT_OBSERVACAO`
- `OPERACAO.TXT_HISTORICO`

An allowlisted column must be textual in the selected Parquet schema and must not occur
in a `specs.json` PK, FK, or NOT NULL rule. Any overlap is a configuration error. An
allowlisted table that is runtime-static, or an allowlisted column also selected by
`--anular-cols`, is also a configuration error.

The pilot deliberately asks the model to rewrite identifier-shaped and otherwise
structured values as natural-language prose. It does not ask the model to preserve UUID,
hash, timestamp, numeric identifier, or protocol-string lexical shapes; prose compliance
remains best effort rather than a local validation rule.

### Context

The model receives the complete selected source aggregate except
`LANCAMENTO.TXT_XML_LANCAMENTO`. Rows are grouped by table and sorted by canonicalized
source PK. Columns have stable ordering; dates use ISO representations, decimals use
strings, and null remains JSON null.

Source values, names, CPF/CNPJ values, accounts, and other identifiers may be sent to
the endpoint and may be copied into generated output. The model may also invent
realistic identifiers and names. No GenAI guardrails are applied.

A canonical context over 50,000 characters is not sent. Its target cells keep source
values and are reported as `CONTEXT_TOO_LARGE`. If every instrument is skipped this way,
the zero-success rule still prevents an enabled run from pretending that GenAI worked.

## Operator Interface

### Pipeline command

`run_pipeline.py` adds an explicit `--enable-genai` flag. The flag defaults to false and
is valid only when exactly one product is selected. A multi-product GenAI command fails
before DAG submission.

The environment configuration adds optional values:

```json
{
  "genai": {
    "endpoint_id": "ocid1.generativeaiendpoint...",
    "compartment_id": "ocid1.compartment...",
    "region": "sa-saopaulo-1",
    "max_concurrency": 4
  },
  "stage_defaults": {
    "engorda": {
      "genai_policy": "oci://bucket@namespace/contracts/genai-policy.json"
    }
  }
}
```

These keys remain optional for ordinary runs. Endpoint, compartment, region, and policy
URI are required and validated before submission only when `--enable-genai` is present.
Concurrency is optional and defaults to 4. The runner passes the resolved values to
engorda planning. Materialization receives the enable flag as an acknowledgement but
does not need endpoint access.

Direct engorda exposes conditional arguments equivalent to:

```text
--enable-genai
--genai-policy <oci-or-local-json>
--genai-endpoint-id <ocid>
--genai-compartment-id <ocid>
--genai-region <region>
--genai-concurrency <positive-integer>  # optional; default 4
--genai-artifact-root <immutable-sibling-uri>  # required only for direct phase all
```

No GenAI argument is required without `--enable-genai`.

### Limits

Enabled planning enforces these pilot limits before endpoint calls:

| Limit | Value |
|---|---:|
| Source instruments | 10,000 |
| `fator_k` | 5 |
| Concurrent requests | 4 by default; positive integer from environment config |
| Logical generation attempts per source | 3 |
| Read timeout per attempt | 120 seconds |
| Context size per source | 50,000 characters |

Exceeding the source-instrument or `fator_k` limit fails before calls. Those two limits
are engine hard caps, not advisory policy defaults, and cannot be raised through the
policy or CLI. Concurrency has no fixed upper bound. Engorda validates the final
`fator_k` after any automatic deficit adjustment and before constructing requests.

### Modes

- Split `plan`/`materialize`: GenAI calls occur only during planning. Materialize must
  receive `--enable-genai` exactly when the plan is GenAI-enabled; either mismatch fails.
- Direct `phase all`: calls and replacements run in memory, then equivalent GenAI audit
  artifacts are published with the output. This path is less reproducible because no
  reusable plan exists.
- `--no-oracle`: compatible with `--enable-genai`; it disables Oracle database access,
  not OCI GenAI. Existing load-ineligible artifact rules remain unchanged.
- Pipeline `--dry-run`: performs its existing local-only validation of config presence,
  one-product scope, argument limits, and the resolved DAG. It cannot read the OCI policy
  or inspect the remote Data Flow SDK.
- Direct `--phase all --dry-run`: reads and validates the policy, endpoint configuration
  shape, SDK capabilities, allowlist, and conflicts without calling the endpoint or
  generating output. Existing `--phase plan --dry-run` remains invalid.

## External Policy Contract

The policy is a versioned JSON document with a cross-product `products` map. Required
keys and supported versions are validated. Unknown keys are ignored for forward
compatibility. Enabling a product absent from the map fails before calls.

Endpoint and compartment OCIDs do not belong in this policy because they are
environment-specific.

Initial shape:

```json
{
  "version": 1,
  "defaults": {
    "model_family": "meta_llama",
    "language": "pt-BR",
    "temperature": 0.8,
    "top_p": 0.9,
    "max_tokens": 4096
  },
  "products": {
    "cdb_simplificado": {
      "system_instruction": "Gere somente os valores solicitados em portugues brasileiro, como texto operacional realista e coerente com o contexto fornecido.",
      "excluded_context_columns": {
        "LANCAMENTO": ["TXT_XML_LANCAMENTO"]
      },
      "targets": [
        {
          "table": "INSTRUMENTO_FINANCEIRO",
          "column": "TXT_CARACT_COMPLEMENTARES",
          "max_chars": 772,
          "instruction": "Descreva caracteristicas complementares plausiveis do instrumento."
        },
        {
          "table": "EVENTO",
          "column": "TXT_OBSERVACAO",
          "max_chars": 60,
          "instruction": "Escreva uma observacao curta e plausivel para o evento."
        },
        {
          "table": "OPERACAO",
          "column": "TXT_HISTORICO",
          "max_chars": 138,
          "instruction": "Escreva um historico operacional curto e plausivel."
        }
      ]
    }
  }
}
```

Sampling settings and field instructions come from the policy. Operators cannot override
temperature, top-p, token count, target columns, prompts, or character limits per run.

The source-instrument, clone-factor, attempt, timeout, and context limits are engine
constants for the pilot and do not appear in the policy. Concurrency is deployment
configuration, defaults to 4, has no fixed upper bound, and is frozen into benchmark
telemetry. For version 1,
`cdb_simplificado` must contain exactly the three pilot targets and must exclude exactly
`LANCAMENTO.TXT_XML_LANCAMENTO` from context. A changed target/exclusion set requires a
new supported policy version and code review. Version 1 also requires the exact reviewed
character maxima of 772, 60, and 138 respectively; unknown unrelated keys remain ignored.

The resolved product policy is persisted beside every enabled plan/output and hashed
into lineage. Persisting the policy and the pre-existing selected-lote source snapshot is
allowed; rendered GenAI prompts and canonical request payloads are never persisted or
logged.

## GenAI Module

The implementation introduces one internal seam with two adapters:

- Oracle adapter: uses `oci.generative_ai_inference.GenerativeAiInferenceClient`, a
  resource-principal signer, regional client configuration, `DedicatedServingMode`
  with the endpoint OCID, and OCI's Meta Llama/generic chat request format.
- Fake adapter: returns scripted responses for deterministic tests without network
  calls.

The external interface operates on a sequence of canonical instrument requests and
returns validated replacement rows plus aggregate metrics. Spark details, OCI request
types, retries, response parsing, and artifact layout remain inside the module.

The Spark driver owns all endpoint calls. Executors and UDFs never create clients or
perform network calls, preventing speculative execution and Spark task retries from
duplicating cost.

The job uses the OCI SDK supplied by the Data Flow runtime. On enabled runs, startup
performs a capability preflight before expensive selection. Missing modules or required
Meta Llama/JSON response classes fail clearly. Ordinary runs do not import the OCI SDK.

Authentication uses the Data Flow resource principal. IAM grants the smallest usable
access to the dedicated endpoint; user config files and API keys are not mounted.

## Request and Response

### Target identity

The closure engine already computes per-table provenance keyed by source table PK and
`__root_num_if`. Enabled planning retains this provenance in memory instead of
discarding it immediately.

For each source instrument, eligible source rows are sorted by table, canonical PK, and
column. The planner assigns opaque target IDs such as `t0001`. The model receives target
IDs and field instructions but cannot choose table names, PKs, columns, or clone indices.

One initial request contains the source context and asks for every target across all
clone indices `1..K`:

```json
{
  "variants": [
    {
      "k": 1,
      "values": {
        "t0001": "texto gerado",
        "t0002": "outro texto"
      }
    }
  ]
}
```

The response schema requires exactly the expected clone indices and target IDs, string
values, and no duplicate identities. Character limits come from the external policy.
Before a call, the planner computes an unescaped decoded-value character budget from the
eligible row count, clone factor, target character limits, and exact JSON envelope using
one safe ASCII placeholder per allowed value character. As an admission heuristic, it
sends the request only when that budget is at most `2 * max_tokens` characters. This is
not a maximum wire size: JSON escaping can expand values, and character count cannot
guarantee the Meta Llama token count without the endpoint model's tokenizer. Endpoint
context-limit rejection follows normal request retry/fallback behavior. A source above
the heuristic output budget is not sent; its cells keep source values and are reported
as `OUTPUT_BUDGET_EXCEEDED`.

"Brazilian Portuguese prose" is a prompt objective, not a local acceptance rule. Local
validation checks identities, JSON/string shape, and configured character lengths only;
identifier-shaped, English, repeated, or source-equal output remains valid by design.

### Attempts and partial fallback

Transport retries preserve one stable OCI retry token for the same logical request.
Model regeneration is a new logical attempt with a deterministic distinct seed. There
are at most three logical attempts per source.

After each response, valid cells are retained and later attempts request only unresolved
cells. A malformed response resolves no cells. Missing, non-string, or oversized cells
remain unresolved. After the third attempt, unresolved cells use the exact source value;
source null and blank therefore remain absent. Valid sibling cells remain generated,
even though this can weaken aggregate consistency.

At least one OCI chat request in the run must return successfully at the transport/API
level. Authentication failure, networking failure, or endpoint unavailability across
all requests fails planning instead of publishing an all-fallback plan.

No Oracle guardrail call is made. Input prompt-injection, source PII propagation,
fabricated identifiers, and unsafe output are accepted risks.

## Frozen Artifacts

### Replacement Parquet

The replacement artifact has one row for every eligible source cell and clone index,
including fallback rows:

| Column | Meaning |
|---|---|
| `ROOT_NUM_IF` | Canonical source instrument identity |
| `TABLE_NAME` | Allowlisted table |
| `SOURCE_PK_JSON` | Canonical source row PK |
| `CLONE_INDEX` | `K` value |
| `COLUMN_NAME` | Allowlisted column |
| `TARGET_ID` | Opaque request-local identity |
| `GENERATED_VALUE` | Accepted model value, nullable on fallback |
| `ACTION` | `REPLACE` or `KEEP_SOURCE` |
| `STATUS` | `GENERATED`, `FALLBACK_INVALID`, `FALLBACK_REQUEST`, `CONTEXT_TOO_LARGE`, or `OUTPUT_BUDGET_EXCEEDED` |
| `ATTEMPT_COUNT` | Logical attempts used |
| `DIFFERS_FROM_SOURCE` | Diversity metric input |

The artifact receives a logical content hash over canonically sorted rows, not a hash of
Parquet physical bytes. The plan descriptor records path, schema, row count, content
hash, and status counts.

### Manifest

The GenAI manifest records:

- endpoint, compartment, region, model family, and policy URI/hash;
- persisted policy-snapshot URI/hash;
- source count, clone factor, configured/effective concurrency, request/attempt counts,
  sanitized endpoint status counts, successful-call p50/p95/max latency, total duration,
  endpoint calls per second, and sources per second;
- generated/fallback/context-too-large cells by table/column;
- source-change and sibling-uniqueness rates by table/column;
- `SUCCESS` when every target is generated, `DEGRADED` when any fallback occurs, or
  `FAILED` when no endpoint request succeeds.

It does not contain rendered GenAI prompts, canonical request payloads, or endpoint
responses outside the accepted replacement values. The selected-lote artifact continues
to persist source rows under the existing engorda contract.

Planning writes policy and replacement artifacts first, then publishes the immutable
plan last. If no endpoint request succeeds, it writes `FAILED` replacement/manifest
artifacts with benchmark telemetry and then fails without publishing a selection plan.
Orphaned pre-plan artifacts after any failed write are harmless and remain subject to
bucket lifecycle retention.

## Materialization

Materialize validates the enable-flag match, plan hash, policy-snapshot descriptor, and
replacement descriptor before cloning. It performs no GenAI calls.

Replacement rows are keyed by source PK plus clone index. Existing engorda PK maps
resolve those identities to cloned rows. `clona_tabela` applies each table's optional
replacement frame after PK/FK remapping, while source-key aliases and `K_COL` are still
available, and before dropping those temporary columns, local checkpointing, and
`valida_tabela`. Existing date and nullification transformations run afterward in the
outer per-table flow; they are disjoint from the GenAI targets by startup validation.
Later business-key allocation also remains unchanged because its columns are outside the
GenAI allowlist. `KEEP_SOURCE` rows require no mutation but remain auditable.

The normal output invariant becomes:

> With GenAI disabled, clones retain the existing engorda invariant. With GenAI enabled,
> only the frozen, allowlisted replacement cells may differ additionally.

The policy snapshot, replacement Parquet, and GenAI manifest are published beneath a
sibling `genai/` artifact path under the immutable product run root, never inside the
synthetic table root. This prevents `validate_products.py` and the loader from discovering
them as database tables.

## Pipeline Behavior

`run_pipeline.py` validates GenAI configuration before submitting Data Flow work and
adds the enable acknowledgement to both engorda plan and materialize argument vectors.
The selection plan and pipeline manifest record GenAI artifact descriptors so resume and
lineage checks cannot mix enabled and disabled artifacts.

The existing immutable run-path and create-once behavior applies to every GenAI
artifact. Automatic Data Flow node retries are disabled specifically for GenAI-enabled
planning because a submission/polling ambiguity could otherwise duplicate endpoint
calls before the local runner sees the published plan. The three in-process logical
attempts remain the only automatic generation retries.

Direct `phase all` uses the same policy parser, context builder, adapter, validator, and
replacement applier, but keeps replacements in memory until output staging. It still
writes the policy snapshot, replacement Parquet, and manifest to the sibling `genai/`
artifact path before publishing output. An enabled direct `phase all` requires an
explicit `--genai-artifact-root` that is absent at startup and is outside the synthetic
table root; there is no fixed-prefix or overwrite fallback. The pipeline runner derives
this sibling URI from its immutable product run root.

## Validation and Testing

Automated tests make no real endpoint calls.

Required test coverage:

1. Policy parsing: required/version checks, ignored unknown keys, absent products,
   textual columns, spec overlap, static/nullification conflicts, and policy limits.
2. Oracle adapter with mocked OCI client: resource-principal construction, dedicated
   endpoint request, Meta Llama JSON response format, stable transport retry token,
   deterministic attempt seeds, timeouts, and error translation.
3. Response validation: exact target/clone identities, partial accumulation, length
   limits, malformed JSON, fallback status, zero-success failure, and diversity metrics.
4. Spark application: repeated EVENTO rows, source null/blank replacement, source-PK/K
   mapping, valid-cell plus fallback mixtures, and no mutation outside the allowlist.
5. Pipeline contract: default-off argv, one-product restriction, conditional config,
   plan/materialize flag mismatch, both dry-run contracts, non-retryable enabled planning,
   immutable artifact lineage, and direct `all` audit artifacts.

The disabled path must retain existing output and behavior. A fake adapter supplies
scripted outputs to end-to-end plan/materialize tests.

## Acceptance Criteria

- Ordinary runs require no GenAI parameters and do not import or call the OCI SDK.
- Enabled runs reject more than 10,000 source instruments or final adjusted `fator_k > 5`
  before calls, regardless of policy contents.
- The pilot can generate/fallback values for every row of the three allowlisted columns
  and every clone index without changing other columns.
- Split materialization consumes exact frozen replacements and performs no endpoint call.
- Partial invalid output produces `DEGRADED` artifacts with cell-level fallback counts.
- Zero successful OCI responses fail planning.
- Both dry-run modes make zero endpoint calls and expose their different validation scope.
- No rendered GenAI prompt or canonical request payload is persisted or logged.
- Diversity rates are reported but never gate the run.
- `--no-oracle --enable-genai` remains load-ineligible under existing rules.

## Accepted Risks and Non-goals

- Generated/fill values may alter NoMe behavior.
- Existing identifiers and structured values are deliberately submitted for prose
  rewriting, but local validation does not guarantee prose output.
- Source PII and identifiers may be sent to and copied by the model.
- The model may fabricate realistic PII and identifiers.
- No prompt-injection, PII, or content-moderation guardrails run.
- Observed maxima may exceed Oracle domains; planning does not validate Oracle lengths.
- GenAI is not guaranteed to increase uniqueness or diversity.
- Data Flow runtime OCI SDK compatibility is checked only at enabled-run startup.
- There is no automated real-endpoint test.
- Other products and additional columns remain out of scope until separately profiled,
  approved in a new supported policy version, and implemented in engorda validation.

## References

- `datagen/engorda_tables.py`
- `scripts/run_pipeline.py`
- `scripts/validate_products.py`
- `cdb_capacity_contract.json`
- `docs/adr/0002-freeze-genai-replacements-during-planning.md`
- [OCI Python SDK `GenerativeAiInferenceClient`](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/generative_ai_inference/client/oci.generative_ai_inference.GenerativeAiInferenceClient.html)
- [OCI Python SDK `DedicatedServingMode`](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/generative_ai_inference/models/oci.generative_ai_inference.models.DedicatedServingMode.html)
- [OCI Generative AI IAM policies](https://docs.oracle.com/en-us/iaas/Content/generative-ai/iam-policies.htm)
