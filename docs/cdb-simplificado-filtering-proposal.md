# CDB Simplificado — Proposed Source Filtering for Engorda (IF-Level Clusters)

**TL;DR:** the current `FILTROS_FONTE` filters rows table-by-table, which tears instruments
apart (e.g. a `CONDICAO_IF` survives while its `RESGATE` subtype row is dropped — a dangling
condição in engorda's *input*), and it never filters on the condition that names the product
(comitente **simplificado**). Proposal: define the universe as a set of `NUM_IF`s via
IF-level predicates, then extract **whole clusters** (all rows of all tables belonging to
those IFs) via semi-joins. Cluster integrity then holds by construction.

All numbers below come from the shape profiles of 2026-07-18/19
(`oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/reports/cdb-shapes/`; method and full
findings in `docs/cdb-shapes-findings.md`).

## Why row-level filtering is structurally wrong

`FILTROS_FONTE` predicates remove *rows*, not *instruments*. Consequences measured on the
filtered production image (the exact input engorda consumes):

- `RESGATE: COD_COND_RESGATE = 'SEM TABELA'` drops the resgate row but keeps its parent
  `CONDICAO_IF` (tipo 20). Result: **16.5% of the filtered universe** is
  `CONDICAO_IF=2, RESGATE=0` — condições with no subtype row. That is the same
  polymorphism violation (Hibernate cannot type the condição) that Category 1 of
  `validate_cdb_simplificado.py` flags in the *output*, already present in the *input*.
- `TITULO: COD_TIPO_ESCALONAMENTO IS NULL` drops the titulo row of 5.2M IFs (7.7%) but
  keeps everything else of those instruments (`TITULO=0` shapes in the baseline).
- `CARTEIRA_*: QTD > 0` similarly removes position rows from otherwise-in-scope IFs.

A generator fed torn clusters cannot produce whole instruments, no matter how good its
sampling is.

## Proposed universe definition (IF-level predicates)

Each predicate decides whether an **instrument** is in scope — never whether a row survives.

| # | Predicate | Status | Evidence / rationale |
|---|---|---|---|
| 1 | `NUM_TIPO_IF = 49 AND DAT_EXCLUSAO IS NULL` | keep as-is | Independently backed by `TipoIFDO.CDB = Id("49")` in the NoMe source. 67.2M active IFs. |
| 2 | IF is held by a **comitente simplificado**: exists `CARTEIRA_COMITENTE → COMITENTE` with `IND_COMITENTE_SIMPLIFICADO = 'S'` (optionally `NUM_ID_SITUACAO_COMITENTE = 1`) | **add — currently missing** | The product is named *Simplificado*, yet 17% of the current universe (11.4M IFs) is held only by non-simplificado comitentes. `IND_COMITENTE_SIMPLIFICADO` is a physical column on `COMITENTE`, joinable offline via `CARTEIRA_COMITENTE.NUM_ID_ENTIDADE`. |
| 3 | Exclude the IF if its `TITULO.COD_TIPO_ESCALONAMENTO IS NOT NULL` | convert from row-drop to IF-exclusion — **if** escalonados are out of scope | Business decision to confirm: this excludes 5.2M IFs (7.7%), plausibly the whole taxa-escalonada sub-product (the `1 RESGATE + 5 JUROS_FLUTUANTE` shape family, ~2.9M IFs). |
| 4 | Exclude the IF if it has any resgate with `COD_COND_RESGATE ≠ 'SEM TABELA'` | convert from row-drop to IF-exclusion — **if** `COM TABELA`/`MERCADO` CDBs are out of scope | Business decision to confirm: `SEM TABELA` covers only 54% of real resgates (`COM TABELA` 16.1M, `MERCADO` 1.8M). Whichever way it's decided, the *instrument* is kept whole or excluded whole. |
| 5 | (Optional) require ≥ 1 active `CONDICAO_IF` | recommended for batch-oriented runs | 42.3% of active CDBs have zero active condições (matured/renegotiated; their condições carry `DAT_EXCLUSAO`). If the synthetic data must survive `AtualizacaoDiariaTitulo`, instruments with active juros condições are what the batch actually exercises. |

Predicates 3–5 are scope questions, not correctness claims — the correctness claim is only
that whatever the answers are, they must be applied **per instrument**.

## Extraction: whole clusters via semi-joins

Once the `NUM_IF` set is fixed:

1. Filter `INSTRUMENTO_FINANCEIRO` to the set.
2. Every other table joins in by **semi-join on its path to the IF** — direct `NUM_IF`
   (`TITULO`, `CREDITO`, `EVENTO`, `OPERACAO`, `DEPOSITO_AUTOMATICO_IF`, `CARTEIRA_*`,
   `CONDICAO_IF`), via `CONDICAO_IF.NUM_CONDICAO_IF` (subtype tables `RESGATE`,
   `JUROS_FLUTUANTE`, `JUROS_FIXO`, …), via `OPERACAO.NUM_ID_OPERACAO` (`DADO_OPERACAO`,
   `LANCAMENTO`).
3. **No further row predicates** inside the cluster.

This is the same join machinery engorda already uses for its domain derivation; the change
is replacing the per-table row predicates with the one universe semi-join. It also composes
directly with per-IF cluster *cloning* for generation: sample source `NUM_IF`s, clone each
instrument's whole closure re-keyed above max — per-instrument cardinalities, subtype
pairing and the `OPERACAO:DADO_OPERACAO:LANCAMENTO = 1:2:1` ratio then hold by construction.

## Acceptance

Regenerate and run the gate:

```
spark-submit profile_cdb_shapes.py --product cdb_simplificado --base-uri <raw> \
    --apply-filtros-fonte --label raw_filtered --report-path <baseline.json>

spark-submit --jars ojdbc8.jar validate_cdb_simplificado.py \
    --product cdb_simplificado \
    --shape-baseline <baseline.json> --report-path <report.json>
```

The baseline is tagged with its product/`NUM_TIPO_IF` (schema v2); the validator rejects a
cross-product or wrong-type baseline. Build a per-product baseline
(`--product cdb`/`rdb`) before enabling shape checks for that product.

Category 7 (shape conformance) fails the run when synthetic shapes don't exist in the
baseline (7a), the distributions drift (7b), the 1:2:1 operação ratio breaks (7c), or any
IF carries more than one `RESGATE` (7d). Note: if the filters change per this proposal, the
baseline must be rebuilt with the *same* new filters so the comparison stays apples-to-apples.
