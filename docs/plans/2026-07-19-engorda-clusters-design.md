# engorda_clusters — Design (grill-me session 2026-07-19)

New synthetic-data generation step, built from scratch (does not reuse `engorda_tables.py`),
expandable to any instrumento financeiro. Every decision below was interviewed and agreed;
evidence references are the shape profiles of 2026-07-18/19 (`docs/cdb-shapes-findings.md`)
and the filtering proposal (`docs/cdb-simplificado-filtering-proposal.md`).

## Decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Purpose | **Capacity testing**: multiply the product population k× so the NoMe batch runs under load. Privacy is an explicit **non-goal** (QAB already holds production data). Functional scenario-mass is out of scope (different tool). |
| 2 | Mechanism | **Deterministic offset-cloning.** Copy i of every cluster row transforms keys by a pure function of the old value — no join/rebind bookkeeping (the bug class of the old engorda). Volume = k integer copies; finer control via source-side sampling predicate on NUM_IF. |
| 3 | Ownership boundary | **Derived**: owned = non-static tables (`specs.json` `static` flag; 23 tables today) reachable via FK graph from the product root; everything static = reference (FK values untouched, resolve to production rows). Per-product `include`/`exclude` overrides for gaps (e.g. `ESPECIFICACAO_COMITENTE` has no FK path to IF). FK-to-owned columns offset **iff value ∈ cloned key set** (e.g. `OPERACAO.NUM_IF_PERTENCE` may point outside the universe → keep original). |
| 4 | Comitentes/contas | **Phase 2** as a second product config (root COMITENTE/ENTIDADE). Blocked on extraction gaps: `PESSOA_FISICA`, `CONTA_COMITENTE` (± `MEIO_COMUNICACAO_ENTIDADE`, `E_MAIL`) are not in specs/raw, and `V_COMITENTES_SIMPL` requires them for a clone to count as comitente simplificado. Needs CPF/CNPJ/conta re-minting + un-static-ing for load. **`PARTICIPANTE` stays reference, never cloned.** Phase 1 ships IF clusters only. |
| 5 | Unique columns | **Constraint-driven re-minting registry.** Verified in QAB (2026-07-19): the 23 IF-cluster tables have **zero** UNIQUE constraints and zero standalone unique indexes — PKs are the only uniqueness, so the registry ships **empty** for CDB. Plan phase fails loudly if a future product has unique columns with no registered transformer. Known accepted duplicate: business codes (ISIN etc.) repeat across clones. |
| 6 | Dates | **Per-cluster rebase** (requirement: freshly-registered semantics). Delta per cluster = emission target − source `DAT_EMISSAO`, applied to **every** date/timestamp column of the whole cluster (type-discovered from Parquet schema) → all intra-cluster orderings preserved. Exceptions: audit columns (`DAT_INCLUSAO`, `DAT_ALTERACAO`, `DAT_INCLUSAO_REGISTRO`, `DAT_ATUALIZACAO_REGISTRO`, `DAT_ULTIMA_ATUALIZACAO`; pattern list + per-product override) pinned to the run timestamp. Emission target spread deterministically over a window: `run_date − (hash(NUM_IF, copy) mod W)`, **default W = 90 days** (W=0 → all on run_date). Corollary: universe predicate requires **≥1 active CONDICAO_IF** (rebase makes everything live; live-but-conditionless is ClassCast territory). Known limitation (documented, not solved): eventos/operações with settled estados carry rebased dates that may sit in the future relative to their state. |
| 7 | Offsets | Per **key domain** (parent table): `new = BASE_t + (old − min_t) + i × span_t`, `span_t = max_t − min_t + 1`; FK columns apply the parent table's function. `BASE_t` = **full unfiltered raw max + `--pk-safety-band`** (footer stats, no JDBC), with `--check-target` (default ON in Data Flow) verifying `BASE_t > target MAX(pk)` via JDBC. Plan-time guardrails: minted-key **precision check** against `NUMBER(p)` of every PK and FK column; `BASE_t` table written to the run manifest (rollback floor — `rollback_load.py` semantics unchanged). |
| 9 | Variability | **Two seeded levers, both on by default; no unseeded randomness anywhere.** (A) **Bootstrap composition**: the k×N copies are drawn with replacement from the universe (seeded hash) instead of exactly k per source — every value stays real production data, all distributions preserved exactly, composition/aggregates vary. (B) **Per-cluster scale factor** `f(run_seed, NUM_IF, copy)`, E[f]=1, lognormal clipped to **[0.5, 2]**, applied to every `QTD_*`/`VAL_*` column across the whole cluster (pattern + per-product override; quantities rounded to integers, valores to 2 decimals). Per-cluster uniformity preserves all within-instrument invariants (emitida ≥ depositada, VAL = QTD × PU, carteira sums) exactly; marginals approximately (size distribution smoothed, mean intact). **Rates/taxas/PU/percentuais are never perturbed** (copied or bootstrap-varied only). Rejected: per-column independent jitter/resampling (preserves marginals, destroys joints — the old engorda's failure transposed to values). Emission-spread note: the hashed 90-day window is uniform, so synthetic emissions land on weekends/holidays; snapping to business days is a listed future option. |
| 8 | Packaging | `datagen/engorda_clusters.py`. I/O drop-in: reads `{DATAGEN_RAW_BASE_URI}/{PREFIX}/<TABLE>`, writes `{DATAGEN_SYNTHETIC_BASE_URI}/{PREFIX}/<TABLE>`. **Product = JSON file** (`products/cdb_simplificado.json`): `root_table`, `universe` predicates (column ops + `exists` semi-joins — nothing richer), `include_tables`/`exclude_tables`, `audit_date_columns_extra`, `unique_transformers`. CLI: `--product --copies --run-date --emission-window-days --pk-safety-band --no-check-target --dry-run --self-test`. `--run-date` explicit for reproducibility; `--dry-run` prints universe size, owned/skipped tables, per-table BASE/span, precision verdict, estimated rows. Run manifest JSON next to output. Data Flow app cloned from the validator app (JDBC jar + private endpoint for `--check-target`). |

## Phase-1 universe (CDB Simplificado)

`NUM_TIPO_IF = 49 AND DAT_EXCLUSAO IS NULL` + `EXISTS active CONDICAO_IF`. The
comitente-simplificado `exists` predicate and the resgate/escalonamento scope decisions from
`docs/cdb-simplificado-filtering-proposal.md` slot in as additional `universe` entries once
the business answers land — IF-level only, never row-level.

## Acceptance

Clones are exact copies of their sources except keys and dates, so the expected shape
distribution equals the source universe's. Wiring: teach `profile_cdb_shapes.py` to accept
the same `--product` config for its filter (superseding `--apply-filtros-fonte`), then
baseline = profiler(raw, product config) and gate = `validate_cdb_simplificado.py
--shape-baseline <baseline>` (Categories 1–7) on the output. One predicate definition,
three consumers.

## Engine invariant (the one-sentence promise)

*A clone is identical to its source cluster except: keys (offset by its copy's function),
dates (uniform per-cluster rebase, audit columns pinned), quantity/value columns (scaled by
the cluster's seeded factor), and registered unique columns (re-minted).* Everything a
validator flags beyond that is a bug in the engine, not a rule to patch around. All
variability is seeded by `(run_seed, NUM_IF, copy)` — reruns with the same inputs are
byte-identical.

## Coordination note

`engorda_instrumentos.py` / `diagnostica_clonagem.py` appeared on main (2026-07-18/19) —
convergent cluster-cloning work by the engorda owner. Compare notes before implementing to
avoid parallel divergent generators.
