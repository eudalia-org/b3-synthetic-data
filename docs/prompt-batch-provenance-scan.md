# Agent prompt — batch-derived NOT NULL provenance scan (CDB Simplificado)

Copy everything below the line into the agent that has the NoMe/CETIP Java source checked out
(`dados/`, `atributos/`, `input/`). The agent's JSON output feeds a new check category in
`scripts/validate_cdb_simplificado.py` (see "How the output is consumed" at the bottom).

---

## Mission

You are scanning the CETIP/B3 **NoMe** Java codebase to build a **provenance catalog**: every place
where a **batch/engine process writes a row into an Oracle table** whose **NOT NULL columns are
derived from data we ingest synthetically**. We need this because our synthetic-data pipeline
(engorda) appends rows to a subset of Oracle tables, and the NoMe batch later *derives* new rows
from them; when a synthetic source field is empty, the batch write fails with `ORA-01400` — but
only at batch time, long after our pre-load validation passed.

Your job is NOT to validate anything. Your job is to produce the **mapping** that lets a PySpark
validator check the *source* columns before the Oracle append:

> "Batch writer `W` inserts into `TARGET_TABLE.TARGET_COLUMN` (NOT NULL), and that value is
> read from `SOURCE_TABLE.SOURCE_COLUMN` of the ingested data, when trigger condition `P` holds."

## Known anchor case (calibrate on this first)

Trace this one end-to-end before generalizing. It is a confirmed production failure:

- Error: `ORA-01400: cannot insert NULL into "CETIP"."TCTPDETALHE_TRAN_SEM_FINA"."COD_MOTIVO"`
- Target DO: `DetalheTransferenciaSemFinanceiroDO` — `idMotivo` → `@Column(name="COD_MOTIVO")`
  (`dados/.../depositaria/DetalheTransferenciaSemFinanceiroDO.java:38-39`)
- Writer: batch action `AcaoAtualizaDetalheTransferencia`, which builds the transfer detail from an
  `OPERACAO` (the operation's fields were empty strings in the synthetic data)

Deliver for this case: exactly which `OPERACAO` / `DADO_OPERACAO` column(s) the action reads to
populate `idMotivo`, and under what condition the action fires. If your method can't fully resolve
this anchor case, say so explicitly and describe where the trace broke — do not proceed to
guess the rest of the catalog with a method that failed the calibration.

## Codebase orientation

| Package / area | What it is |
|---|---|
| `br.com.cetip.dados` (`dados/`) | Hibernate domain objects (`*DO`, `*VDO`) mapped to Oracle schema `CETIP`, plus business services and batch actions (`Acao*`). Mappings live in annotations (`@Table`, `@Column`) and/or `dados/xml/*.hbm.xml`. |
| `br.com.cetip.infra.atributo` (`atributos/`) | Typed-attribute framework. Mandatory-ness: `AtributoAbstrato.isMandatorio`; validation funnel: `AtributoAbstratoSimples.atribuirConteudo → verificar()`. |
| Operation engine | `ProcessaEstimulo` state machine drives operations through states; states dispatch `Acao*` classes. Daily batch example: `AtualizacaoDiariaTitulo` (title revaluation). |
| `input/` (`b3.balcao.capacidade.cdb`) | Test-mass generator. Out of scope as a writer; ignore it. |

Domain scope — **CDB Simplificado** only:
- CDB = `TipoIFDO.CDB = Id("49")` / `CodigoTipoIF.CDB` (`NUM_TIPO_IF = 49` on `INSTRUMENTO_FINANCEIRO`).
- "Simplificado" = comitente simplificado, view `V_COMITENTES_SIMPL`
  (`ComitentesSimplVDO`, flag `IND_COMITENTE_SIMPLIFICADO`).
- Restrict to writers reachable from CDB processing: registration/deposit flows, daily revaluation
  (`AtualizacaoDiariaTitulo`), operation/transfer processing (`ProcessaEstimulo` states and their
  `Acao*` classes for CDB operation types), custody/comitente updates. If a writer is generic
  (fires for many IF types including CDB), include it and note `"scope": "generic"`.

## The ingested (synthetic source) tables

A value counts as "derived from ingested data" when it is read — directly or through the DO graph —
from one of these tables:

```
INSTRUMENTO_FINANCEIRO, TITULO, CONDICAO_IF,
JUROS_FIXO, JUROS_FLUTUANTE, ATUALIZACAO_POS, ATUALIZACAO_PRE, SPREAD,
AMORTIZACAO, RESGATE, RESET, PARTICIPACAO_LUCROS, DESDOBRAMENTO, CONDICAO_RESGATE,
DEPOSITO_AUTOMATICO_IF, CARTULA, CARTEIRA_COMITENTE, CARTEIRA_PARTICIPANTE,
ESPECIFICACAO_COMITENTE, OPERACAO, DADO_OPERACAO, LANCAMENTO,
FORMA_PAGAMENTO, GARANTIA, EVENTO, DISTRIBUICAO_TITULO,
PAPEL_PJ_TITULO, PENDENCIA_IF, INSTRUMENTO_CAPTACAO, HIST_INSTRUMENTO_FINANCEIRO
```

(Values coming from pure lookup/reference tables — `TIPO_OPERACAO`, `MODALIDADE_LIQUIDACAO`,
`TIPO_IF`, `SITUACAO_IF`, etc. — are NOT synthetic-derived; classify them as `lookup`.)

## Method

Work writer-by-writer, evidence-first. For every claim, record `file:line`.

1. **Find the writers.** Enumerate code paths that persist NEW rows during batch/operation
   processing: Hibernate `save`/`persist`/`saveOrUpdate` calls, DAO insert methods, and `Acao*` /
   batch classes that construct a `*DO` and hand it to the session. Exclude pure UPDATEs of columns
   that already exist in ingested rows unless the UPDATE sets a NOT NULL column to a value read
   from ingested data (those can raise ORA-01407, same failure class — include them, flagged
   `"operation": "update"`).

2. **Resolve the target table + NOT NULL set.** From `@Table`/`@Column(nullable=false)` or the
   `.hbm.xml` (`not-null="true"`), plus attribute mandatory-ness (`isMandatorio`) where the DO uses
   the atributos framework. You do not have Oracle access: when the mapping doesn't state
   nullability, still include the column with `"not_null_evidence": "unknown"` — the consumer
   cross-checks against `ALL_TAB_COLUMNS` and will drop columns that are actually nullable.

3. **Trace provenance backwards** for each target column the writer populates: from the setter /
   constructor argument, through intermediate variables and service calls, to the origin. Classify:
   - `copy` — read from an ingested table's DO field, assigned (possibly trimmed/cast) unchanged
   - `transform` — computed from one or more ingested fields (say which, describe the formula)
   - `lookup` — resolved from a reference/lookup table (possibly keyed by an ingested field —
     record the ingested key column too, since a bad key also breaks the write)
   - `constant` — hardcoded / enum constant
   - `generated` — sequence, sysdate, engine-generated id
   Only `copy`, `transform`, and the ingested *keys* of `lookup` become validation rules; still
   record the others so we know the column is covered.

4. **Capture the trigger.** Under what condition does this writer run for a CDB? (operation type
   ids, estimulo/state ids, IF type checks, situation codes…). Express it twice: prose, and — where
   the condition is over ingested columns — a best-effort Spark SQL predicate over the source
   table (e.g. `NUM_ID_TIPO_OPERACAO IN (1381) AND NUM_TIPO_IF = 49`). If you cannot express it in
   SQL, set `"predicate_sql": null` and keep the prose.

5. **Don't guess.** Every relationship carries `"confidence"`: `high` (full code path read, every
   hop cited), `medium` (one hop inferred from naming/convention — say which), `low` (plausible but
   unverified — say why). A wrong high-confidence mapping is worse than a gap: it becomes a false
   validation error that blocks ingestion runs.

## Output format

Return a single JSON document:

```json
{
  "anchor_case_resolved": true,
  "relationships": [
    {
      "target_table": "TCTPDETALHE_TRAN_SEM_FINA",
      "target_column": "COD_MOTIVO",
      "operation": "insert",
      "not_null_evidence": "@Column(nullable=false) dados/.../DetalheTransferenciaSemFinanceiroDO.java:38",
      "writer": {
        "class": "AcaoAtualizaDetalheTransferencia",
        "entry_point": "ProcessaEstimulo estado <id>",
        "scope": "generic",
        "evidence": ["dados/.../AcaoAtualizaDetalheTransferencia.java:<line>"]
      },
      "provenance": {
        "kind": "copy",
        "source_table": "OPERACAO",
        "source_columns": ["<COL>"],
        "path": [
          "OperacaoDO.get<X>()  dados/.../OperacaoDO.java:<line>",
          "DetalheTransferenciaSemFinanceiroDO.setIdMotivo(...)  <file>:<line>"
        ],
        "transform_description": null
      },
      "trigger": {
        "description": "fires when the operation is a transferencia sem financeiro (...)",
        "predicate_sql": "NUM_ID_TIPO_OPERACAO IN (...)"
      },
      "suggested_rule": {
        "table": "OPERACAO",
        "predicate_sql": "NOT (<trigger>) OR (<COL> IS NOT NULL AND TRIM(<COL>) != '')"
      },
      "confidence": "high"
    }
  ],
  "not_traceable": [
    { "writer": "...", "target": "TABLE.COLUMN", "reason": "value passes through reflection / dynamic attribute map at <file:line>" }
  ],
  "coverage_notes": "which batch areas you did NOT scan and why"
}
```

Rules for the output:
- One entry per `(target_table, target_column, writer)` triple.
- `suggested_rule` only for `copy`/`transform`/`lookup`-key provenance; `null` otherwise.
- Remember Oracle stores `''` as NULL — every string-column rule must include the
  `TRIM(col) != ''` clause, as in the example.
- List everything you skipped or couldn't trace in `not_traceable` / `coverage_notes`; silent gaps
  are the failure mode this whole exercise exists to eliminate.

---

## How the output is consumed (context for the b3-synthetic-data side, not part of the agent prompt)

The `relationships[].suggested_rule` entries become a curated map in
`validate_cdb_simplificado.py` — a new check category ("Cat 7 — batch-derived NOT NULL") that
evaluates each `predicate_sql` against the named source table in the synthetic output, exactly like
`DOMAIN_RULES` / `check_domain` does today, with the finding's hint pointing at the downstream
`target_table.target_column` and writer class. Entries with `not_null_evidence: "unknown"` are
cross-checked against `ALL_TAB_COLUMNS` (`NULLABLE='N'`) before being enforced.
