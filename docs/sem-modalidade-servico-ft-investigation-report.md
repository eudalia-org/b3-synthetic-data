# SEM MODALIDADE / servico_ft nao encontrado — Reverse-Engineered Lookup Report

**Project:** b3-synthetic-data (engorda synthetic data pipeline)

**Context:** This repo does NOT contain the NoMe Java/Hibernate source code. The findings below are
derived from batch-validation error logs, p6spy SQL traces (`docs/cetip.out`), schema metadata
(`specs.json`, `ordem.rtf`), and the analysis documents in this repo. Where a claim depends on
the NoMe source (not present here), it is flagged as `[inferred from evidence]` with the
supporting evidence cited.

---

## a. Error sites (file:line within this repo) and one-line description

There are **zero** error-throw/log sites in this repository — the NoMe Java source is not checked in.

The errors are produced by the **NoMe application** (CETIP/B3 post-trade system) during its
daily/operational batch. What this repo contains are the *descriptions of those errors*, from
analysis of batch logs:

| Reported error | Source reference in this repo | One-line description |
|---|---|---|
| `"nao foi encontrado o servico_ft"` | `docs/cdb-simplificado-ingestion-analysis.md:81-87` | Operation state machine `ProcessaEstimulo` (estado 479) could not resolve the serviço (ObjetoServico) mapping for the operation's (tipo IF, tipo operação, modalidade de liquidação) combination. |
| `"CDB:53:SEM MODALIDADE"` | `docs/cdb-simplificado-ingestion-analysis.md:81-87` | Same root cause: the lookup for the service/object combination returned no row. |
| `Cat 6 Lookup combinations -> "SEM MODALIDADE / servico_ft nao encontrado"` | `scripts/validate_cdb_simplificado.py:29` | The pre-load validator's Category 6 is named after this error. |

The observed failing parameters from the batch log: `TIPO_OPERACAO=1381` + `MODALIDADE_LIQUIDACAO=6`
(`docs/cdb-simplificado-ingestion-analysis.md:83-84`).

---

## b. Call chain (reconstructed from evidence)

The NoMe Java source is not in this repo, but the call chain is documented as:

```
[Batch entry point]
  └─ ProcessaEstimulo (state machine, estado 479)
       └─ resolves: TIPO_OPERACAO + MODALIDADE_LIQUIDACAO → ObjetoServico (servico_ft)
            └─ lookup: (tipo IF × tipo operação × modalidade de liquidação)
                 └─ when no row found → "nao foi encontrado o servico_ft" / "SEM MODALIDADE"
```

From the p6spy trace (`docs/cetip.out`), the flow for a *successful* CDB registration shows the
lookup pattern. Two services execute similar queries:

| `ServicoValidaInformacoesGeraisRF` | lines 28, 38-39 | Pre-registration validation |
|---|---|---|
| `ServicoRegistraRF` | lines 80, 91, 99-100 | Registration execution |
| `ServicoAtualizaOperacaoPendente` | line 135 | Post-registration operation processing — this is the daily batch equivalent |

The batch path likely follows the `ServicoAtualizaOperacaoPendente` pattern, which joins
`OPERACAO → TIPO_OPER_OBJETO_SERV → TIPO_OPERACAO → OBJETO_SERVICO → MODALIDADE_LIQUIDACAO`.

---

## c. The verbatim queries

### c.1 Resolve OBJETO_SERVICO by COD_OBJETO_SERVICO (via view)

```sql
-- Source: docs/cetip.out:38 (ServicoValidaInformacoesGeraisRF)
SELECT ...
  FROM CETIP.V_OBJETOS_SERVICO objetosser0_
 WHERE objetosser0_.COD_OBJETO_SERVICO = 'CDB'
   AND objetosser0_.IND_PLATAFORMA_BAIXA = 'S'
```

Same query appears at line 99 (`ServicoRegistraRF`). Returns `NUM_ID_OBJETO_SERVICO` for the
product code.

### c.2 Resolve NUM_ID_TIPO_OPER_OBJETO_SERV from (COD_TIPO_OPERACAO, NUM_ID_OBJETO_SERVICO)

```sql
-- Source: docs/cetip.out:28 (ServicoValidaInformacoesGeraisRF)
SELECT ...
  FROM CETIP.TIPO_OPER_OBJETO_SERV tipooperob0_,
       CETIP.TIPO_OPERACAO tipooperac1_
 WHERE tipooperob0_.NUM_ID_TIPO_OPERACAO = tipooperac1_.NUM_ID_TIPO_OPERACAO
   AND tipooperob0_.NUM_ID_OBJETO_SERVICO = :obj_servico_id
   AND tipooperac1_.COD_TIPO_OPERACAO = :cod_tipo_operacao
   AND tipooperob0_.IND_DISPONIVEL_IDENTIFICACAO = 'S'
```

Same query at lines 39, 80, 91, 100 — some variants omit `IND_DISPONIVEL_IDENTIFICACAO`.

### c.3 Batch operation context query (the one closest to "SEM MODALIDADE")

```sql
-- Source: docs/cetip.out:135 (ServicoAtualizaOperacaoPendente)
SELECT ...
  FROM CETIP.OPERACAO operacaodo0_,
       CETIP.TIPO_OPER_OBJETO_SERV tipooperob1_,
       CETIP.TIPO_OPERACAO tipooperac2_,
       CETIP.OBJETO_SERVICO objetoserv4_,
       CETIP.MODALIDADE_LIQUIDACAO modalidade5_,
       CETIP.SITUACAO_OPERACAO situacaoop6_
 WHERE operacaodo0_.NUM_ID_TIPO_OPER_OBJETO_SERV = tipooperob1_.NUM_ID_TIPO_OPER_OBJETO_SERV
   AND tipooperob1_.NUM_ID_TIPO_OPERACAO = tipooperac2_.NUM_ID_TIPO_OPERACAO
   AND tipooperob1_.NUM_ID_OBJETO_SERVICO = objetoserv4_.NUM_ID_OBJETO_SERVICO
   AND operacaodo0_.NUM_ID_MODALIDADE_LIQUIDACAO = modalidade5_.NUM_ID_MODALIDADE_LIQUIDACAO
   AND operacaodo0_.COD_SITUACAO_OPERACAO = situacaoop6_.COD_SITUACAO_OPERACAO
   AND operacaodo0_.NUM_ID_OPERACAO = :num_id_operacao
  FOR UPDATE OF operacaodo0_.NUM_ID_OPERACAO
```

### c.4 Habilitacao check query (date-sensitive, batch-relevant)

```sql
-- Source: docs/cetip.out:176 (ServicoAtualizaOperacaoPendente)
SELECT ...
  FROM CETIP.TCTPHABILITA_OPERACAO_SERVICO habilitati0_
 WHERE habilitati0_.NUM_TIPO_OPERACAO_OBJETO_SERVI = :num_id_tipo_oper_objeto_serv
   AND habilitati0_.NUM_ID_PARAMETRO_CONFIGURACAO = :param_config_id
   AND (habilitati0_.DATA_EXCLUSAO_REGISTRO IS NULL)
```

### c.5 PARAMETRIZACAO_REGIME_MERCADO join

```sql
-- Source: docs/cetip.out:112 (ServicoRegistraRF)
SELECT ...
  FROM cetip.PARAMETRIZACAO_REGIME_MERCADO parametriz0_,
       CETIP.TIPO_OPER_OBJETO_SERV tipooperob1_,
       CETIP.TIPO_OPERACAO tipooperac3_
 WHERE parametriz0_.NUM_ID_TIPO_OPER_OBJETO_SERV = tipooperob1_.NUM_ID_TIPO_OPER_OBJETO_SERV
   AND tipooperob1_.NUM_ID_TIPO_OPERACAO = tipooperac3_.NUM_ID_TIPO_OPERACAO
   AND tipooperob1_.NUM_ID_OBJETO_SERVICO = :obj_servico_id
   AND tipooperac3_.COD_TIPO_OPERACAO = :cod_tipo_operacao
```

---

## d. Physical table / column mapping & equivalent standalone Oracle SQL

### Entity mapping summary

| Hibernate entity / alias | Physical table | Key column(s) | Notes |
|---|---|---|---|
| `TIPO_OPER_OBJETO_SERV` | `CETIP.TIPO_OPER_OBJETO_SERV` | `NUM_ID_TIPO_OPER_OBJETO_SERV` (PK) | Static lookup, FK to `OBJETO_SERVICO` + `TIPO_OPERACAO` |
| `TIPO_OPERACAO` | `CETIP.TIPO_OPERACAO` | `NUM_ID_TIPO_OPERACAO` (PK) | `COD_TIPO_OPERACAO` is the business code |
| `OBJETO_SERVICO` | `CETIP.OBJETO_SERVICO` | `NUM_ID_OBJETO_SERVICO` (PK) | Static lookup |
| `MODALIDADE_LIQUIDACAO` | `CETIP.MODALIDADE_LIQUIDACAO` | `NUM_ID_MODALIDADE_LIQUIDACAO` (PK) | Layer-0 static lookup |
| `SITUACAO_OPERACAO` | `CETIP.SITUACAO_OPERACAO` | `COD_SITUACAO_OPERACAO` (PK) | |
| `V_OBJETOS_SERVICO` | `CETIP.V_OBJETOS_SERVICO` (view) | `COD_OBJETO_SERVICO` | View wrapping `OBJETO_SERVICO` with platform filter |
| `TCTPHABILITA_OPERACAO_SERVICO` | `CETIP.TCTPHABILITA_OPERACAO_SERVICO` | Composite | Has `DATA_EXCLUSAO_REGISTRO` soft-delete |
| `PARAMETRIZACAO_REGIME_MERCADO` | `CETIP.PARAMETRIZACAO_REGIME_MERCADO` | `NUM_ID_TIPO_OPER_OBJETO_SERV` (FK) | Links regime/mercado to the operation-service mapping |

### Standalone Oracle SQL — the "servico_ft" lookup

```sql
-- ============================================================
-- Resolve the service-object (ObjetoServico / servico_ft) for
-- a given CDB operation at registration time.
-- Parameters:
--   :cod_objeto_servico  = 'CDB' (from product type, TipoIF=49)
--   :cod_tipo_operacao   = '1'  (from TIPO_OPERACAO.COD_TIPO_OPERACAO)
--   :num_id_modalidade   = NUM_ID_MODALIDADE_LIQUIDACAO (from OPERACAO)
--   :cod_situacao_oper   = COD_SITUACAO_OPERACAO (from OPERACAO)
--   :num_id_operacao     = NUM_ID_OPERACAO (from OPERACAO)
-- ============================================================

-- Step 1: Resolve NUM_ID_OBJETO_SERVICO from product code
-- (likely cached at batch start, or queried once per product type)
SELECT NUM_ID_OBJETO_SERVICO
  FROM CETIP.V_OBJETOS_SERVICO
 WHERE COD_OBJETO_SERVICO = :cod_objeto_servico
   AND IND_PLATAFORMA_BAIXA = 'S';

-- Step 2: Resolve NUM_ID_TIPO_OPER_OBJETO_SERV from
-- (COD_TIPO_OPERACAO, NUM_ID_OBJETO_SERVICO)
SELECT tipooperob0_.NUM_ID_TIPO_OPER_OBJETO_SERV,
       tipooperob0_.IND_DISPONIVEL_IDENTIFICACAO,
       tipooperob0_.IND_ATIVIDADE
  FROM CETIP.TIPO_OPER_OBJETO_SERV tipooperob0_
  JOIN CETIP.TIPO_OPERACAO tipooperac1_
       ON tipooperob0_.NUM_ID_TIPO_OPERACAO = tipooperac1_.NUM_ID_TIPO_OPERACAO
 WHERE tipooperac1_.COD_TIPO_OPERACAO = :cod_tipo_operacao
   AND tipooperob0_.NUM_ID_OBJETO_SERVICO = :num_id_objeto_servico
   AND tipooperob0_.IND_DISPONIVEL_IDENTIFICACAO = 'S';

-- Step 3: Load the full operation + service context
-- (this is what ServicoAtualizaOperacaoPendente does)
SELECT operacaodo0_.NUM_ID_OPERACAO,
       operacaodo0_.NUM_ID_TIPO_OPER_OBJETO_SERV,
       operacaodo0_.NUM_ID_MODALIDADE_LIQUIDACAO,
       operacaodo0_.COD_SITUACAO_OPERACAO,
       tipooperob1_.NUM_ID_TIPO_OPERACAO,
       tipooperob1_.NUM_ID_OBJETO_SERVICO,
       tipooperac2_.COD_TIPO_OPERACAO,
       objetoserv4_.COD_OBJETO_SERVICO,
       objetoserv4_.NUM_ID_OBJETO_SERVICO,
       modalidade5_.NUM_ID_MODALIDADE_LIQUIDACAO,
       situacaoop6_.COD_SITUACAO_OPERACAO
  FROM CETIP.OPERACAO operacaodo0_
  JOIN CETIP.TIPO_OPER_OBJETO_SERV tipooperob1_
       ON operacaodo0_.NUM_ID_TIPO_OPER_OBJETO_SERV = tipooperob1_.NUM_ID_TIPO_OPER_OBJETO_SERV
  JOIN CETIP.TIPO_OPERACAO tipooperac2_
       ON tipooperob1_.NUM_ID_TIPO_OPERACAO = tipooperac2_.NUM_ID_TIPO_OPERACAO
  JOIN CETIP.OBJETO_SERVICO objetoserv4_
       ON tipooperob1_.NUM_ID_OBJETO_SERVICO = objetoserv4_.NUM_ID_OBJETO_SERVICO
  JOIN CETIP.MODALIDADE_LIQUIDACAO modalidade5_
       ON operacaodo0_.NUM_ID_MODALIDADE_LIQUIDACAO = modalidade5_.NUM_ID_MODALIDADE_LIQUIDACAO
  JOIN CETIP.SITUACAO_OPERACAO situacaoop6_
       ON operacaodo0_.COD_SITUACAO_OPERACAO = situacaoop6_.COD_SITUACAO_OPERACAO
 WHERE operacaodo0_.NUM_ID_OPERACAO = :num_id_operacao;
```

---

## e. Runtime parameters and their source

| Parameter | Value at runtime | Source (OPERACAO column) | Notes |
|---|---|---|---|
| `:cod_objeto_servico` | `'CDB'` | `INSTRUMENTO_FINANCEIRO → COD_IF` model-derived, or hardcoded per TipoIF | From `CodigoTipoIF.CDB` = `Id("49")`. The first two chars of `COD_IF` = `'CDB'`. Resolved via `V_OBJETOS_SERVICO` view. |
| `:cod_tipo_operacao` | `'1'` | Hardcoded by service (`ServicoRegistraRF`, `ServicoValidaInformacoesGeraisRF`) | COD_TIPO_OPERACAO='1' = registration operation type. In batch processing, this may come from TIPO_OPERACAO. |
| `:num_id_objeto_servico` | e.g. `44` | Resolved from `V_OBJETOS_SERVICO` by COD_OBJETO_SERVICO | In the trace, CDB resolution returned `NUM_ID_OBJETO_SERVICO = 44` (first value from `COD_OBJETO_SERVICO` starting with 'CDB'). |
| `:num_id_tipo_oper_objeto_serv` | e.g. `4509` | Resolved from join of TIPO_OPER_OBJETO_SERV + TIPO_OPERACAO | This is the PK of the mapping row. Stored in `OPERACAO.NUM_ID_TIPO_OPER_OBJETO_SERV`. |
| `:num_id_modalidade` | e.g. `6` | `OPERACAO.NUM_ID_MODALIDADE_LIQUIDACAO` | **This is the key column.** When this FK value has no valid combination row in TIPO_OPER_OBJETO_SERV, the error fires. |
| `:cod_situacao_operacao` | e.g. `252` → `415` → `21` → `400` → `43` | `OPERACAO.COD_SITUACAO_OPERACAO` | Evolves through the state machine. The lookup must succeed at estado 479. |

### Date-sensitive parameters (critical for cloned data)

| Table | Column(s) | Role |
|---|---|---|
| `TCTPFEATURE_TOGGLE` | `DATA_INIC_VIG_FTRE`, `DATA_FIM_VIG_FTRE` | Date window check: `data_inicio <= :data_operacao AND data_fim >= :data_operacao`. The `:data_operacao` comes from `OPERACAO.DAT_OPERACAO`. |
| `TCTPHABILITA_OPERACAO_SERVICO` | `DATA_EXCLUSAO_REGISTRO` | Soft-delete: `DATA_EXCLUSAO_REGISTRO IS NULL`. If synthetic data sets this to a non-null date, the habilitação check fails. |

---

## f. Anti-join SQL — find operations that would fail with SEM MODALIDADE

### f.1 Simple anti-join (tipo_operacao × modalidade × objeto_servico)

This is the validator's current best-effort approach at
`scripts/validate_cdb_simplificado.py:998-1006`:

```sql
-- Find OPERACAO rows whose (NUM_ID_TIPO_OPERACAO, NUM_ID_MODALIDADE_LIQUIDACAO)
-- have NO matching row in TIPO_OPER_OBJETO_SERV
-- (simplified: assumes NUM_ID_TIPO_OPERACAO from TIPO_OPERACAO join)

WITH valid_combos AS (
    SELECT DISTINCT
           tos.NUM_ID_TIPO_OPERACAO,
           NULL AS NUM_ID_MODALIDADE_LIQUIDACAO  -- TIPO_OPER_OBJETO_SERV does NOT store this
      FROM CETIP.TIPO_OPER_OBJETO_SERV tos
)
SELECT op.NUM_ID_OPERACAO,
       op.NUM_IF,
       op.NUM_ID_TIPO_OPER_OBJETO_SERV,
       op.NUM_ID_MODALIDADE_LIQUIDACAO,
       op.COD_SITUACAO_OPERACAO,
       op.DAT_OPERACAO
  FROM CETIP.OPERACAO op
  LEFT JOIN CETIP.TIPO_OPER_OBJETO_SERV tos
       ON op.NUM_ID_TIPO_OPER_OBJETO_SERV = tos.NUM_ID_TIPO_OPER_OBJETO_SERV
 WHERE op.NUM_IF IN (SELECT NUM_IF
                       FROM CETIP.INSTRUMENTO_FINANCEIRO
                      WHERE NUM_TIPO_IF = 49
                        AND DAT_EXCLUSAO IS NULL)
   AND (   tos.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL  -- broken FK
        OR NOT EXISTS (
               SELECT 1
                 FROM CETIP.TIPO_OPER_OBJETO_SERV tos2
                 JOIN CETIP.TIPO_OPERACAO top
                     ON tos2.NUM_ID_TIPO_OPERACAO = top.NUM_ID_TIPO_OPERACAO
                 JOIN CETIP.OBJETO_SERVICO os
                     ON tos2.NUM_ID_OBJETO_SERVICO = os.NUM_ID_OBJETO_SERVICO
                WHERE tos2.NUM_ID_TIPO_OPER_OBJETO_SERV = op.NUM_ID_TIPO_OPER_OBJETO_SERV
                  AND os.COD_OBJETO_SERVICO LIKE 'CDB%'
                  AND top.COD_TIPO_OPERACAO = '1'
           ));
```

**Important caveat:** TIPO_OPER_OBJETO_SERV does NOT contain NUM_ID_MODALIDADE_LIQUIDACAO as a
column. The modalidade constraint is enforced elsewhere (either in the `ProcessaEstimulo` state
machine logic itself, or through `MODALIDADE_LIQUIDACAO` validation separate from the
TIPO_OPER_OBJETO_SERV lookup). The exact combination rule involves application logic not fully
expressed in the schema.

### f.2 Complete anti-join (best-effort approximation for the validator)

The existing Cat 6 check in `validate_cdb_simplificado.py:998-1006` performs the simplest
valid-combination anti-join:

```sql
SELECT DISTINCT t, m
  FROM (
    SELECT op.NUM_ID_TIPO_OPERACAO AS t,
           op.NUM_ID_MODALIDADE_LIQUIDACAO AS m
      FROM CETIP.OPERACAO op
      JOIN CETIP.INSTRUMENTO_FINANCEIRO ifr
           ON op.NUM_IF = ifr.NUM_IF
     WHERE ifr.NUM_TIPO_IF = 49
       AND ifr.DAT_EXCLUSAO IS NULL
  ) cd_ops
  LEFT JOIN (
    SELECT DISTINCT
           tos.NUM_ID_TIPO_OPERACAO AS t
      FROM CETIP.TIPO_OPER_OBJETO_SERV tos
  ) valid
       ON cd_ops.t = valid.t
 WHERE valid.t IS NULL;
```

This is acknowledged as incomplete in
`docs/plans/2026-07-25-cdb-validation-handoff.md:225-226`:

> Cat 6 combo check is only best-effort — needs the real
> tipo_operacao×modalidade×serviço mapping table added to COMBO_TABLE_PATTERNS.

---

## g. Mapping tables, join keys, and composite uniqueness

### `TIPO_OPER_OBJETO_SERV` (static lookup, layer 1)

| Column | Type | FK to | Notes |
|---|---|---|---|
| `NUM_ID_TIPO_OPER_OBJETO_SERV` | NUMBER (PK) | — | Surrogate PK |
| `NUM_ID_TIPO_OPERACAO` | NUMBER (FK) | `TIPO_OPERACAO.NUM_ID_TIPO_OPERACAO` | → resolves `COD_TIPO_OPERACAO` business code |
| `NUM_ID_OBJETO_SERVICO` | NUMBER (FK) | `OBJETO_SERVICO.NUM_ID_OBJETO_SERVICO` | → resolves the "serviço" (e.g., CDB) |
| `IND_DISPONIVEL_IDENTIFICACAO` | CHAR(1) | — | Flag: `'S'` = available for identificação |
| `IND_ATIVIDADE` | CHAR(1) | — | Flag: active/inactive |
| `NUM_ID_ENTIDADE_ATUALIZ` | NUMBER (FK) | `USUARIO.NUM_ID_ENTIDADE` | Last updater |

**Assumed composite uniqueness (business key):** `(NUM_ID_TIPO_OPERACAO, NUM_ID_OBJETO_SERVICO)`.
The code resolves by `COD_TIPO_OPERACAO` (business code) + `NUM_ID_OBJETO_SERVICO` (FK).

### `OBJETO_SERVICO` (static lookup, layer 0)

| Column | Type | FK to | Notes |
|---|---|---|---|
| `NUM_ID_OBJETO_SERVICO` | NUMBER (PK) | — | Surrogate PK |
| `COD_OBJETO_SERVICO` | VARCHAR2 | — | Business code, e.g. `'CDB'` |
| `IND_PLATAFORMA_BAIXA` | CHAR(1) | — | Flag: `'S'` = active on low platform |
| `NUM_ID_TIPO_OBJETO_SERVICO` | NUMBER (FK) | `TIPO_OBJETO_SERVICO.NUM_ID_TIPO_OBJETO_SERVICO` | Classification |

### `V_OBJETOS_SERVICO` (view)

Filters `OBJETO_SERVICO` rows. In the p6spy trace, the predicate is
`COD_OBJETO_SERVICO = 'CDB' AND IND_PLATAFORMA_BAIXA = 'S'`.

### `MODALIDADE_LIQUIDACAO` (layer 0)

| Column | Type | Notes |
|---|---|---|
| `NUM_ID_MODALIDADE_LIQUIDACAO` | NUMBER (PK) | The value on OPERACAO, e.g. `6` |
| `COD_MODALIDADE_LIQUIDACAO` | VARCHAR2 | Business code |

### `TCTPHABILITA_OPERACAO_SERVICO` (layer 2)

| Column | Type | FK to | Notes |
|---|---|---|---|
| `NUM_TIPO_OPERACAO_OBJETO_SERVI` | NUMBER | `TIPO_OPER_OBJETO_SERV.NUM_ID_TIPO_OPER_OBJETO_SERV` | The mapping PK |
| `NUM_ID_PARAMETRO_CONFIGURACAO` | NUMBER | `PARAMETRO_CONFIG.NUM_ID_PARAMETRO_CONFIG` | Config parameter |
| `DATA_EXCLUSAO_REGISTRO` | DATE | — | Soft-delete (must be NULL) |

### `OPERACAO` (our source table, layer 6)

| Column | Type | FK to | Role in this lookup |
|---|---|---|---|
| `NUM_ID_OPERACAO` | NUMBER (PK) | — | Operation id |
| `NUM_IF` | NUMBER (FK) | `INSTRUMENTO_FINANCEIRO.NUM_IF` | Links to the instrument |
| `NUM_ID_TIPO_OPER_OBJETO_SERV` | NUMBER (FK) | `TIPO_OPER_OBJETO_SERV.NUM_ID_TIPO_OPER_OBJETO_SERV` | **The resolved mapping** — this is the FK that carries the lookup result |
| `NUM_ID_MODALIDADE_LIQUIDACAO` | NUMBER (FK) | `MODALIDADE_LIQUIDACAO.NUM_ID_MODALIDADE_LIQUIDACAO` | **One of the lookup dimensions** |
| `COD_SITUACAO_OPERACAO` | VARCHAR2 (FK) | `SITUACAO_OPERACAO.COD_SITUACAO_OPERACAO` | State machine state |
| `DAT_OPERACAO` | DATE | — | Used in feature-toggle date-window checks |

---

## h. Edge conditions for cloned data

### h.1 Date rebase (±90 days) — affects date-window predicates

Our cloned data rebases `DAT_*` columns by ±90 days around a reference date. The following
queries use date comparisons that can break:

| Table | Date column | Predicate | Risk |
|---|---|---|---|
| `TCTPFEATURE_TOGGLE` | `DATA_INIC_VIG_FTRE`, `DATA_FIM_VIG_FTRE` | `data_inicio <= :dat_operacao AND data_fim >= :dat_operacao` | **HIGH.** If feature toggles have absolute dates (e.g., `01-jan-2020` to `31-dec-2099`), a rebased `DAT_OPERACAO` stays within window. But toggles with narrow windows may fall outside. The `:dat_operacao` comes from `OPERACAO.DAT_OPERACAO` — which itself is rebased. |
| `TCTPHABILITA_OPERACAO_SERVICO` | `DATA_EXCLUSAO_REGISTRO` | `IS NULL` (soft-delete) | **LOW** (if null, stays null). **Check:** ensure engorda does not populate this. |

### h.2 Remapped surrogate IDs — affects FK resolution

Engorda remaps all surrogate PKs above their per-table max. For the service lookup:

| Table | PK column | Engorda behavior | Risk |
|---|---|---|---|
| `TIPO_OPER_OBJETO_SERV` | `NUM_ID_TIPO_OPER_OBJETO_SERV` | Static table, NOT cloned — original FK values preserved | **LOW.** Engorda does NOT remap static tables. `OPERACAO.NUM_ID_TIPO_OPER_OBJETO_SERV` keeps its original FK value. |
| `TIPO_OPERACAO` | `NUM_ID_TIPO_OPERACAO` | Static, NOT cloned | **LOW.** Preserved. |
| `OBJETO_SERVICO` | `NUM_ID_OBJETO_SERVICO` | Static, NOT cloned | **LOW.** Preserved. |
| `MODALIDADE_LIQUIDACAO` | `NUM_ID_MODALIDADE_LIQUIDACAO` | Static, NOT cloned | **LOW.** Preserved. |
| `OPERACAO.NUM_ID_TIPO_OPER_OBJETO_SERV` | FK to static table | Original value kept | **LOW** as long as this FK is not nulled during engorda processing. **Verify** engorda does not null it. |

### h.3 Static tables may differ between production and QAB

The static tables (`TIPO_OPER_OBJETO_SERV`, `TIPO_OPERACAO`, `OBJETO_SERVICO`,
`MODALIDADE_LIQUIDACAO`, `TCTPFEATURE_TOGGLE`) are **not cloned by engorda**. The synthetic
operations inherit their FK values from the production source snapshot. If QAB has a different
set of static rows (e.g., missing certain combinations for CDB), the batch will fail even though
the synthetic data is correct.

### h.4 COD_OBJETO_SERVICO = 'CDB' resolution (product code)

The lookup starts by resolving `COD_OBJETO_SERVICO = 'CDB'` via `V_OBJETOS_SERVICO`. This
depends on:
1. The view being populated in QAB (standard, but verify)
2. `IND_PLATAFORMA_BAIXA = 'S'` on that row

### h.5 Summary of edge conditions to test

| # | Condition | What to check | Priority |
|---|---|---|---|
| 1 | Feature toggle date windows with rebased DAT_OPERACAO | Clone a few ops and verify `TCTPFEATURE_TOGGLE` date-window predicates still pass. | HIGH |
| 2 | The exact combination rule for MODALIDADE_LIQUIDACAO | TIPO_OPER_OBJETO_SERV does NOT store modalidade — find where the modalidade constraint is enforced in the state machine code. | HIGH |
| 3 | IND_DISPONIVEL_IDENTIFICACAO flag | Some query variants include `IND_DISPONIVEL_IDENTIFICACAO='S'`; others omit it. Determine which variant the batch uses. | MEDIUM |
| 4 | DATA_EXCLUSAO_REGISTRO on TCTPHABILITA_OPERACAO_SERVICO | Ensure engorda does not set this to a non-null value. | MEDIUM |
| 5 | Static table completeness in QAB | Verify QAB has the same combo rows as production for CDB operations. | MEDIUM |
| 6 | OPERACAO.NUM_ID_TIPO_OPER_OBJETO_SERV is NOT nulled | Check engorda code path to ensure this FK survives. | HIGH |

---

## i. Outstanding unknowns (requires NoMe source access)

1. **The exact Java class throwing "SEM MODALIDADE".** The error code `CDB:53` points to
   `CodigoErro.java` in the `atributos/` package — likely `CodigoErro.SEM_MODALIDADE` or
   similar. Confirm via grep on the NoMe source.

2. **The exact Java class throwing "servico_ft nao encontrado".** The string "servico_ft"
   suggests an entity name `ServicoFT` — a DO or service class. The string pattern
   `"nao foi encontrado o servico_ft"` suggests a lookup returning `null`.

3. **The state machine code at estado 479.** The `ProcessaEstimulo` state machine dispatches
   to an `Acao*` class. Identify which action fires at estado 479 for the CDB operation type.

4. **Whether MODALIDADE_LIQUIDACAO participates in the TIPO_OPER_OBJETO_SERV lookup directly
   or indirectly** (e.g., through an intermediate mapping table, through
   `PARAMETRIZACAO_REGIME_MERCADO`, or through the state machine dispatch logic itself). The
   current `validate_cdb_simplificado.py` Cat 6 assumes it does, but the schema has no direct
   column for it — the modalidade constraint is enforced elsewhere.

5. **Whether the batch caches the service mapping in a HashMap at startup** (a common pattern).
   If so, the composite HashMap key IS the combination rule. The construction of that key (e.g.,
   `(COD_TIPO_OPERACAO, NUM_ID_MODALIDADE_LIQUIDACAO)` or `(TipoIF, COD_TIPO_OPERACAO, ...)`)
   is the exact business rule we need.
