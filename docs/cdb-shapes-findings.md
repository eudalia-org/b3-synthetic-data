# CDB Simplificado — Per-Instrument Cardinality Analysis (Synthetic vs Production)

**TL;DR:** We profiled how many child rows each CDB instrument has per table ("shape") in
production raw data and in the latest synthetic output (`synthetic_50k_simplificado`). The
synthetic data preserves 1:1 relationships correctly, but **no synthetic instrument (0 of
100,000) has a production-valid shape**: 1:N relationships (condições, eventos, operações,
carteiras) don't follow the per-instrument cardinalities that exist in production. This is
very likely the root cause of the NoMe batch failures — and it's fixable structurally, with
an automated acceptance test already in place.

## Method (reproducible)

`scripts/profile_cdb_shapes.py` (commits `0e203f3` + `cc2d3fd`) counts, for every active CDB
(`NUM_TIPO_IF=49 AND DAT_EXCLUSAO IS NULL`), its rows in 13 related tables and reports the
distribution of count-vectors. Three Data Flow runs:

1. **`profile_raw.json`** — production raw Parquet (`onprem-export-full`), 67.2M IFs.
2. **`profile_raw_filtered.json`** — same data with the engorda `FILTROS_FONTE` predicates
   applied, i.e. the exact image engorda consumes. This is the fair baseline: engorda's
   output should reproduce *this* distribution.
3. **`profile_synthetic.json`** — the synthetic output, diffed against (2).

Reports: `oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/reports/cdb-shapes/`. The expected
registration shape was independently confirmed from a p6spy trace of a real CDB-simplificado
registration (`docs/cetip.out`): `1 IF : 1 TITULO : 1 CREDITO : 2 CONDICAO_IF (1 RESGATE +
1 JUROS_FLUTUANTE) : 2 EVENTO : 1 OPERACAO : 2 DADO_OPERACAO : 1 LANCAMENTO :
1 DEPOSITO_AUTOMATICO_IF : 1 CARTEIRA_COMITENTE : 1 CARTEIRA_PARTICIPANTE`.

## What the synthetic data gets right

The shared-key 1:1 tables (PK = `NUM_IF`) are essentially correct — the rebinding machinery
works:

- `CREDITO`: 100% exactly 1 (matches production)
- `DEPOSITO_AUTOMATICO_IF`: 96.6% (prod: 99.4%)
- `TITULO`: 95.2% (prod filtered: 92.3%)
- `EVENTO.NUM_IF` vs `EVENTO.NUM_CONDICAO_IF→NUM_IF` consistency: clean in both datasets

## Findings — broken per-instrument cardinalities

| Production invariant (filtered baseline, 67.2M IFs) | Synthetic (100k IFs) |
|---|---|
| `EVENTO = 2` per IF — 98.4% exactly 2, only 80 IFs with 0 | 58.3% have **0**; rest smeared across 1–5+ |
| Condição mix: `JUROS_FLUTUANTE` dominant (48.4M tipo 3 vs 38.7M tipo 20) | **99.2% of condições are tipo 20 (RESGATE)**; 30 `JUROS_FLUTUANTE` rows total; 0 `JUROS_FIXO` |
| 57.7% of IFs have condições ativas (52.4% exactly 2) | 81.7% have none; 2.9% have 2 |
| `RESGATE ≤ 1` per IF — no exception in 67.2M IFs | up to 5+ per IF |
| `DADO_OPERACAO = 2 × OPERACAO` and `LANCAMENTO = OPERACAO`, exactly | 12% of IFs have operações, but `DADO_OPERACAO`/`LANCAMENTO` ≈ zero everywhere — operações without data rows |
| `CARTEIRA_COMITENTE = 1` for 99%+ of IFs | 48.4% have 0; rest smeared 1–5+ |
| 83% of IFs held by comitente simplificado | 41% |

Additionally, the synthetic set contains **4,139 shapes that never occur in production**, and
they account for the *majority* of synthetic volume — e.g. `CONDICAO_IF=1` with only a
`RESGATE` and no juros leg (0.05% in prod; top-15 shape in synthetic), or `OPERACAO=1,
DADO_OPERACAO=0, LANCAMENTO=0` (nonexistent in prod).

The error pattern — counts *smeared* around low means rather than biased — is the signature
of sampling each child table independently to a row target and binding child rows to parents
without preserving per-parent counts.

## Connection to the observed batch failures

- Condition sets without a juros leg (or with the wrong subtype mix) leave the daily
  revaluation (`AtualizacaoDiariaTitulo`) nothing valid to price → the `ClassCastException`
  family.
- `OPERACAO` rows without `DADO_OPERACAO`/`LANCAMENTO` mean the batch builds downstream
  records from empty operation data → the `ORA-01400 COD_MOTIVO` / `SEM MODALIDADE` family.

## Side observations on the source filters (scope questions, not bugs)

From the unfiltered production profile, worth confirming as intended scope:

1. `COD_COND_RESGATE = 'SEM TABELA'` keeps only 54% of resgates (drops `COM TABELA` 16.1M,
   `MERCADO` 1.8M).
2. `COD_TIPO_ESCALONAMENTO IS NULL` drops 5.2M titles (~7.7%), likely the whole
   taxa-escalonada sub-product.
3. There is **no filter on comitente simplificado** — 17% of the filtered universe (11.4M
   IFs) is held by non-simplificado comitentes.

## Recommended fix

Rather than patching invariants one by one, sample **per-IF clusters**: select source
`NUM_IF`s and clone each instrument's entire key-closure (all child rows across all tables,
re-keyed above max, same machinery already used for the shared-key children). Cardinalities,
subtype mixes and the 1:2:1 operação ratio then hold **by construction**.

**Acceptance test (already automated):** rerun the profiler on the regenerated output with
`--compare-with profile_raw_filtered.json`. Pass = shape distribution converges to the
baseline and "shapes only in synthetic" ≈ 0. This can be added as Category 7 of
`validate_cdb_simplificado.py` to gate every future run.
