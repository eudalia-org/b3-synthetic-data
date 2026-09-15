# Per-product GenAI enrichment source limit

Status: Implemented — focused checks pass; full-suite verification is incomplete due to timeouts and baseline Spark failures.

## Goal

Allow a large instrument selection, such as 1,000,000 CDB sources, while bounding instrument text enrichment to a smaller subset, such as 10,000 sources.

## Confirmed decisions

1. The enrichment source limit counts source instruments before cloning, not synthetic instruments, related table rows, or text cells. Clone factor does not multiply the source allowance.
2. The allowance is per product within a run, not shared across the pipeline. Sources outside the enrichment subset follow normal cloning.
3. Choose the enrichment subset deterministically using the run's existing seed. The same selected source set and seed yield the same subset, regardless of input ordering or execution partitioning; changing the seed can change the subset.
4. Remove `--enable-genai`; the effective enrichment source limit is the sole activation control. When the effective limit is zero, skip GenAI entirely. Resolve omitted product values through configured defaults, falling back to zero when no value is configured (supersedes the earlier 10,000 default). A positive value enables GenAI and requires endpoint/policy configuration. If the selected source count is below the limit, select all sources for enrichment; otherwise select a sample of the configured size.
5. Support multiple selected products in the same pipeline run, with enrichment activated independently per product. Products with an effective zero enrichment source limit proceed through normal generation.

## Configuration

Confirmed: configure `genai_rows` under each product's `engorda` settings. Endpoint settings remain shared under the top-level `genai` object.

Confirmed: inherit `genai_rows` from `stage_defaults.engorda` when omitted on a product, using the existing settings precedence: per-product CLI override, then product configuration, then stage default, then zero. An explicit product value of zero overrides a positive stage default and disables enrichment for that product.

Confirmed CLI: `--set cdb_simplificado.engorda.genai_rows=10000` for the pipeline runner and `--genai-rows 10000` for direct engorda execution.

Accept any non-negative integer; 10,000 is not a hard ceiling. Zero disables enrichment. Positive limits above the selected source count select all available sources for enrichment. Negative and non-integer values are invalid.

```json
{
  "products": {
    "cdb_simplificado": {
      "engorda": {"n_instrumentos": 1000000, "genai_rows": 10000}
    },
    "rdb_inclusao": {
      "engorda": {"n_instrumentos": 500000}
    }
  }
}
```

This configuration enriches up to 10,000 CDB Simplificado sources and runs RDB Inclusao normally.

Confirmed: before launching the pipeline, reject any selected product with a positive effective enrichment source limit but no matching GenAI policy, identifying the product in the error. Products with an effective zero limit require no GenAI policy.

## Failure accounting

Confirmed: the sampled source set stays fixed. A source consumes its allowance even if enrichment fails after the existing retries; failed values retain their source text. Do not backfill failures with additional source instruments. The cap measures sources selected for enrichment, not guaranteed successful enrichments.

Confirmed: total GenAI endpoint failure must not fail product planning or generation. Keep the original source texts and log that GenAI enrichment was not executed successfully. When requests were attempted, the log must distinguish attempted-but-failed enrichment from enrichment skipped because the effective source limit was zero. This changes the existing total-endpoint-failure behavior; the agreed pre-launch error for a missing policy on an enabled product still applies.

## Clone factor

Confirmed: remove the GenAI-specific `fator_k <= 5` restriction. Enriched products may use the normal generation clone-factor range. Split clone-specific text generation across bounded requests for larger clone factors, preserving each clone's identity. One sampled source consumes one source allowance even when it requires multiple requests; the source limit is not an API-call limit.

Confirmed: size clone batches automatically against the policy's existing output budget, rather than using a fixed five-clone batch. Larger text targets yield smaller batches; shorter targets can fit more clones.

## Existing architectural constraint

[ADR 0002](../../docs/adr/0002-freeze-genai-replacements-during-planning.md) freezes GenAI replacements during planning so materialization reuses them without endpoint calls. The sampling decision must fit that established lifecycle.

Materialization derives enrichment from the immutable plan and validates its frozen replacements without requiring GenAI activation arguments. This replaces the former matching-acknowledgement requirement. The ADR documents this evolution.

## Verification

330 tests passed across `tests/test_engorda_genai.py`, `tests/test_run_pipeline.py`, and `tests/test_pipeline_quota_backoff.py`. Ruff and Python compilation passed. Standards and spec reviews have no open findings; see [review.md](review.md) for the resolved findings and broader-suite limitations.
