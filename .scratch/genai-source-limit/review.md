# GenAI source-limit review

Base: `0cbe477090221b461b59aeaab31875759045c61c`, compared with the uncommitted implementation.

## Standards

No documented-standard violations. One judgement-only duplicated-validation finding was resolved by introducing `_genai_source_limit` within the self-contained Data Flow module while retaining checks at each entry point. Follow-up read-only review confirmed the resolution.

## Spec

One P2 finding was resolved: an accepted allowance above Spark's JVM integer range previously reached `DataFrame.limit()` unchanged. Sampling now counts sources in Spark and bypasses the limit when the allowance covers all available sources. The regression checks an allowance of `2**63` against 50 sources. Follow-up review confirmed resolution for the agreed million-source scale.

## Differential evidence

Both reviewers examined the implementation independently. The standards review also compared the baseline and current components using adversarial inputs and simulated endpoint responses.

| Scenario | Verified current behavior |
| --- | --- |
| Omitted/zero, invalid, and mixed-product limits | Disabled by default; invalid values rejected; independent inherited/product/CLI settings |
| Large clone factors and small output budgets | Automatic bounded batches with stable global clone indices |
| Total endpoint timeout | `DEGRADED`, source-preserving replacements, continued planning |
| Repartitioned selection | Same source set and seed select the same subset |
| Model-directed text | Braces and placeholders preserved; excluded XML remains excluded |

## Verification results

- **330 passed**: `tests/test_engorda_genai.py`, `tests/test_run_pipeline.py`, `tests/test_pipeline_quota_backoff.py` after the final code fix.
- **Passed**: Ruff on changed Python files, Python compilation, and `git diff --check`.
- The full suite reached 83% before a 600-second timeout, showing two failures. Both were reproduced on the unchanged base with the same environment: `test_strict_domain_excludes_any_invalid_operation_and_optional_account_refs` fails on an empty-string-to-decimal cast under Spark 4.2; `test_oracle_char_and_latin1_byte_capacity` fails on strict ISO-8859-1 encoding under Spark 4.2.
- A follow-up run covering the remaining validator files also exceeded 600 seconds, with no failures shown before timeout. Full-suite success is not claimed.

Final review: Standards **0 open findings**; Spec **0 open findings**.
