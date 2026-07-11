# CDB Simplificado — Synthetic Ingestion Failure Analysis & Validation Reference

**Scope:** why the last synthetic ingestion (`engorda_tables.py` output → Oracle append → NoMe batch)
failed, what validations exist (application-side and our new pre-load validator), which Oracle tables
make up a CDB Simplificado, and the recommended fixes.

**Context:** CETIP/B3 **NoMe** platform. A CDB (Certificado de Depósito Bancário) is the financial
instrument identified by `TipoIFDO.CDB = Id("49")` / `CodigoTipoIF.CDB`
(`dados/.../instrumentofinanceiro/TipoIFDO.java:49`). "**Simplificado**" means the CDB is
registered/deposited to a **comitente simplificado** (streamlined investor account), driven off the
Oracle view `V_COMITENTES_SIMPL` (`dados/.../sic/ComitentesSimplVDO.java`, flag
`IND_COMITENTE_SIMPLIFICADO`).

---

## 1. How the pipeline is wired

Three independent pieces are involved:

| Piece | Package / file | Role |
|---|---|---|
| `input/` GeraMassa | `b3.balcao.capacidade.cdb` | Generates the CDB registration **messages** (test mass). Not the app. |
| `atributos/` | `br.com.cetip.infra.atributo` | Attribute/field framework — **field-level validation** (typed attributes self-validate). |
| `dados/` | `br.com.cetip.dados` | Hibernate domain objects (DO/VDO) → Oracle (schema `CETIP`) + business services. |
| `engorda_tables.py` | PySpark job | **Synthetic data generation** ("engorda"): reads production Parquet, filters to the CDB-simplificado domain, samples, bootstraps new rows above the real PK max, and writes synthetic Parquet that is later appended to Oracle. |

The failing flow: **engorda output → Oracle append → NoMe daily/operational batch runs on top of the
appended data**. The "batch validation" errors you saw are the *real* NoMe engine choking on
domain-inconsistent synthetic rows — not a field-format rejection.

---

## 2. Why the last ingestion failed

Three distinct error signatures, one common root cause: **`engorda_tables.py` preserves flat PK/FK
referential integrity but breaks the application-level (structural/business) invariants the NoMe engine
relies on.**

### 2.1 `ClassCastException: JurosFlutuanteDO cannot be cast to JurosFixoDO`  (the important one)

Raised by `AtualizacaoDiariaTitulo` (the daily title revaluation batch) while processing a synthetic
CDB.

**Mechanism (confirmed in the mappings):**

- `CONDICAO_IF` is a Hibernate `<class>` (`dados/xml/CondicaoIFDO.hbm.xml:5-11`) with **no
  `<discriminator>`**. Its subtypes are `<joined-subclass>` tables — `juros_fixo` (`:637`),
  `juros_flutuante` (`:764`), `amortizacao`, `spread`, `resgate`, `reset`, … — **all keyed by the same
  column `NUM_CONDICAO_IF`** (`:644-646`).
- With no discriminator, Hibernate decides the **concrete class purely by which subtype table physically
  contains the row** for that `NUM_CONDICAO_IF`.
- The parent carries `COD_TIPO_CONDICAO_IF` (`:42-50`), a lookup to `TipoCondicaoIFDO` where
  **`JUROS_FIXO="2"`, `JUROS_FLUTUANTE="3"`** (`TipoCondicaoIFDO.java:49-50`). The application reads that
  code to decide the CDB is prefixado and casts to `JurosFixoDO`.

**The invariant that must hold:** for each `NUM_CONDICAO_IF`, its `COD_TIPO_CONDICAO_IF` must agree with
the single subtype table that holds the row.

**Why engorda breaks it:** `CONDICAO_IF` and its subtype tables are modeled as independent flat tables.
`NUM_CONDICAO_IF` is simultaneously the **PK of `CONDICAO_IF`** and the **PK + FK-to-parent of each
subtype table** (a shared-key 1:1). PK generation is **per-table, above each table's own max**
(`compute_pk_maxes` + `_set_unique_pk_column`), so the three id spaces don't align. The mitigation
`bind_shared_key_children` (line 3731) rebinds each shared-key child to a distinct slice of parent keys —
but it processes **each subtype table independently, both starting at parent row 0, and ignores
`COD_TIPO_CONDICAO_IF`**. So a single `NUM_CONDICAO_IF` gets claimed by **both** `juros_fixo` and
`juros_flutuante` (or the wrong one), and the batch's `(JurosFixoDO)` cast fails.

### 2.2 `ORA-01400: cannot insert NULL into "CETIP"."TCTPDETALHE_TRAN_SEM_FINA"."COD_MOTIVO"`

A NOT-NULL business column left empty. `DetalheTransferenciaSemFinanceiroDO.idMotivo →
@Column(name="COD_MOTIVO")` (`dados/.../depositaria/DetalheTransferenciaSemFinanceiroDO.java:38-39`);
the Oracle column is NOT NULL. The batch action `AcaoAtualizaDetalheTransferencia` built a transfer
detail from a synthetic operation whose fields were empty (`''`). Two gaps:

- Engorda's `assert_not_null_ok` (line 3967) only checks columns listed in the spec's `not_null_cols`
  (from `cols_real.csv`) and only tests Python `isNull()` — **not empty string `''`**, which **Oracle
  stores as NULL**.
- `TCTPDETALHE_TRAN_SEM_FINA` may not even be in the engorda run — it is written by the *batch*, so the
  null is a downstream effect of an under-populated synthetic `OPERACAO`.

### 2.3 `nao foi encontrado o servico_ft` / `CDB:53:SEM MODALIDADE`

The operation state machine (`ProcessaEstimulo`, estado 479) resolving `TIPO_OPERACAO=1381` +
`MODALIDADE_LIQUIDACAO=6` could not find the **serviço (ObjetoServico)** that maps *tipo IF × tipo
operação × modalidade de liquidação*. The synthetic `OPERACAO`/`DADO_OPERACAO` rows reference a
**combination** that isn't consistent in the lookup tables for a CDB. Engorda preserves referential
*shape* but not valid business *combinations*.

### 2.4 Root cause, in one sentence

Engorda guarantees **structural referential integrity** (unique PKs above max, FKs point at existing
parents, orphans neutralized) but **not the NoMe application invariants**: polymorphic subtype
consistency, valid lookup combinations, and mandatory non-PK/FK/date business columns.

---

## 3. What kinds of validation are done

### 3.1 Application-side (NoMe / CETIP)

**A. Field-level (attribute) validation — `atributos/`.** Every field is a typed attribute; storing a
value funnels through `AtributoAbstratoSimples.atribuirConteudo → verificar()` (trim + validate), with
mandatory-ness in `AtributoAbstrato.isMandatorio`. Representative types used by a CDB:

- `CPFOuCNPJ` → valid CPF/CNPJ mask (`NAO_EH_CPF_NEM_CNPJ`)
- `CodigoContaCetip` → mask `99999.99-9` + account-type + **check digit**
- `CodIsin` → 12 chars, `BR` prefix, mod-10 DV
- `QuantidadeInteiraPositiva` → numeric ≥ 0, ≤ 14 digits
- `Percentual` (`999,99999%`), `ValorMonetario`/`NumeroReal` (size/format), `Data` (`dd/MM/yyyy` +
  ordering), `Enumerado` domain checks (`CriterioCalculoJuros`, `IdTipoRegimeTitulo`,
  `CodigoTipoPrazoJuros`)
- Orchestration in `atributo.tipo.validador` (`IValidador`, `ListaDeValidadores`, `ValidadorRegra`)

**B. Business-rule validation — `dados/` + `CodigoTipoIF` predicates.** Examples relevant to CDB:
PU obligatory on deposit (`campoPUehObrigatorioOperacaoDeposito`), comitente must be identified
(`identificarComitenteOperacaoDeposito`), escalonamento allowed (`possuiEscalonamentoDeTaxas`),
emissor/detentor accounts mandatory & valid, emitida vs depositada quantities, `DAT_VENCIMENTO >
DAT_REGISTRO`, forma de pagamento / curvas, condição de resgate antecipado, tipo de regime
(`IdTipoRegimeTitulo`: 1=Depositado, 2=Registrado), comitente-simplificado rules. Error constants live in
`atributos/.../CodigoErro.java`.

**C. Runtime structural invariants (implicit, enforced by Hibernate + the batch).** The
`CONDICAO_IF` polymorphic subtype consistency, lookup-combination validity, and NOT-NULL constraints —
**these are exactly what the synthetic ingestion violated**, because they are not "validations" a form
rejects but assumptions the engine makes at load/processing time.

### 3.2 Engorda-side (already in `engorda_tables.py`)

- `validate_primary_keys` / `validate_foreign_keys` / `run_validation_or_raise` — PK uniqueness/nulls +
  FK orphans.
- `bind_shared_key_children` (3731) — attempts shared-key 1:1 rebind (**incomplete**, see §2.1).
- `null_orphan_fks` / `_rebind_orphan_fk_to_valid_parent` / `neutraliza_orfaos_na_fonte` — orphan
  neutralization.
- `assert_not_null_ok` (3967) — NOT-NULL gate (**incomplete**: spec-only, ignores `''`, see §2.2).
- `_warn_filtros_fonte_sem_not_null` (2988) — warns when the product filter can cause silent ORA-01400.

### 3.3 New pre-load validator — `validate_cdb_simplificado.py`

Self-contained PySpark job that runs on the **synthetic Parquet output** and reads authoritative
**PK / FK / NOT NULL** from Oracle (`ALL_*` views over JDBC). Six categories, each finding carries a fix
hint:

| Cat | Function | Catches |
|---|---|---|
| 1 | `check_polymorphism` | `CONDICAO_IF` subtype: exactly-one table (1a), matches `COD_TIPO_CONDICAO_IF` (1b), no orphan subtype (1c) → **ClassCastException** |
| 2 | `check_domain` | `FILTROS_FONTE` product image → out-of-product rows |
| 3 | `check_referential` | all Oracle FKs resolve (synthetic-first, else Oracle); shared-key 1:1 |
| 4 | `check_not_null` | NOT-NULL incl. **empty string** → **ORA-01400** |
| 5 | `check_dates` | emissão ≤ vencimento, registro ≤ vencimento, condição início ≤ fim |
| 6 | `check_lookup_combos` | operação × modalidade × serviço combos → **SEM MODALIDADE** |

Run:
```
spark-submit --jars ojdbc8.jar validate_cdb_simplificado.py \
    --report-path report.json --fail-severity error --validate-against union
```
Env: `DATAGEN_SYNTHETIC_BASE_URI` (+ `DATAGEN_SYNTHETIC_PREFIX`), `DATAGEN_SOURCE_JDBC_URL`,
`DATAGEN_SOURCE_DB_USER`, `DATAGEN_SOURCE_DB_PASSWORD`, `DATAGEN_SOURCE_SCHEMA` (default
`CETIP`) — the same names the other datagen jobs use.

---

## 4. Tables that make up a CDB Simplificado (schema `CETIP`)

There is **no dedicated `IF_CDB` table** — a CDB is stored generically as `INSTRUMENTO_FINANCEIRO` +
`TITULO`, threaded by the key **`NUM_IF`**; its rentability lives in `CONDICAO_IF` + subtype tables; its
deposit in the custody tables.

### (i) Core instrument
| Table | DO / mapping | Role |
|---|---|---|
| `INSTRUMENTO_FINANCEIRO` | `InstrumentoFinanceiroDO` (`:46`) | Master IF row (PK `NUM_IF`); dates, values, quantities, situation |
| `TITULO` | `TituloDO` (`:29`, joined-subclass of IF on `NUM_IF`) | Title-level `QTD_DEPOSITADA/EMITIDA/RESGATADA`, regime, FGC |
| `TIPO_IF` | `TipoIFDO` (`:20`) | IF-type lookup; CDB = Id("49") |
| `SITUACAO_IF` | `SituacaoIFDO` | Situation/status lookup |
| `HIST_INSTRUMENTO_FINANCEIRO` | `HistoricoInstrumentoFinanceiroDO` | IF change history |
| `PAPEL_PJ_TITULO` | `PapelPJTituloDO` | Parties/roles on the title |
| `PENDENCIA_IF` | `PendenciaIFDO` | Pending items on the IF |
| `INSTRUMENTO_CAPTACAO` | `InstrumentoCaptacaoDO` | Bank-funding classification |

### (ii) Conditions / rentability
Supertable **`CONDICAO_IF`** (`CondicaoIFDO.hbm.xml`, PK `NUM_CONDICAO_IF`, FK `NUM_IF`,
discriminator-lookup `COD_TIPO_CONDICAO_IF`) + shared-key joined-subclass tables:

| Table | `COD_TIPO_CONDICAO_IF` | Role for a CDB |
|---|---|---|
| `JUROS_FIXO` | 2 | Fixed-interest leg |
| `JUROS_FLUTUANTE` | 3 | Floating-interest leg (% of index) |
| `ATUALIZACAO_POS` | 4 | Post-fixed monetary correction |
| `ATUALIZACAO_PRE` | 14 | Pre-fixed update |
| `SPREAD` | 5 | Spread over index |
| `AMORTIZACAO` | 1 | Amortization schedule |
| `RESGATE` | 20 | Redemption condition (early redemption) |
| `RESET` | 23 | Reset of variable |
| `PARTICIPACAO_LUCROS` | 6 | Profit participation |
| `DESDOBRAMENTO` | 24 | Split ratio |

`CONDICAO_RESGATE` (`CondicaoResgateDO`, child of `RESGATE`) holds the per-date early-redemption
schedule. *(Derivative-only subtypes — `TERMO*`, `OPCAO`, `PREMIO_*`, `TRIGGER_SPR` — exist on
`CONDICAO_IF` but do not apply to a plain CDB.)*

### (iii) Deposit / custody (the "Simplificado" part)
| Table | DO | Role |
|---|---|---|
| `DEPOSITO_AUTOMATICO_IF` | `DepositoAutomaticoIFDO` | 1:1 deposit of the CDB to a favored account/comitente (qty, PU, CPF/CNPJ) |
| `CARTULA` (+ `SITUACAO_CARTULA`) | `CartulaDO` | Deposit/registration certificate |
| `CARTEIRA_COMITENTE` | `CarteiraComitenteDO` | Holder/comitente position (qty per `NUM_IF`) |
| `ESPECIFICACAO_COMITENTE` | `EspecificacaoComitenteDO` | Specification of deposited quantity to the comitente |

Driven off the **`V_COMITENTES_SIMPL`** view (simplified investor: `IND_COMITENTE_SIMPLIFICADO`,
`NUM_ID_SITUACAO_COMITENTE = 1`).

### (iv) Payment / guarantee / event / distribution
`FORMA_PAGAMENTO` (+ `GRUPO_FORMA_PAGAMENTO`), `GARANTIA` (+ `TIPO_GARANTIA`), `EVENTO`,
`DISTRIBUICAO_TITULO`.

### (v) Reporting views
`V_IF_CDB` (`CaracteristicasGeraisCDBVDO`) — general CDB characteristics; `V_CARACTIF_CDB`
(`CaracteristicasCDBVDO`) — detailed, incl. custody accounts, garantias, situação, CVM/distribuição,
rating, tipo regime.

### (vi) Operation / transfer tables (exercised by the batch, source of errors 2.2 & 2.3)
`OPERACAO`, `DADO_OPERACAO`, `LANCAMENTO`, `TIPO_OPERACAO`, `MODALIDADE_LIQUIDACAO`, the
tipo-operação × objeto-serviço mapping, and `TCTPDETALHE_TRAN_SEM_FINA` (transfer detail — the ORA-01400
table).

### CDB-simplificado domain filter (engorda `FILTROS_FONTE`)
```
INSTRUMENTO_FINANCEIRO : NUM_TIPO_IF = 49  AND DAT_EXCLUSAO IS NULL
RESGATE                : COD_COND_RESGATE = 'SEM TABELA' AND DAT_EXCLUSAO IS NULL
TITULO                 : COD_TIPO_ESCALONAMENTO IS NULL
CONDICAO_IF            : DAT_EXCLUSAO IS NULL
CARTEIRA_COMITENTE     : QTD_CARTEIRA_COMITENTE > 0
CARTEIRA_PARTICIPANTE  : QTD_CARTEIRA_PARTICIPANTE > 0
```

---

## 5. Recommended fixes (the "fix next" work in `engorda_tables.py`)

1. **`bind_shared_key_children` — make it subtype-aware (fixes 2.1).** Treat `CONDICAO_IF` + its subtype
   tables as one unit: partition the parent `NUM_CONDICAO_IF` key space **by `COD_TIPO_CONDICAO_IF`**, and
   bind each subtype child only to the subset of parent keys whose tipo matches that subtype. Guarantee
   **exactly one** subtype row per parent, in the table implied by `COD_TIPO_CONDICAO_IF`.

2. **`assert_not_null_ok` — treat `''` as NULL and source NOT NULL from Oracle (fixes 2.2).** Oracle
   stores empty string as NULL; the current gate misses it. Also derive the NOT-NULL catalog from
   `ALL_TAB_COLUMNS` (`NULLABLE='N'`) rather than only the spec, and populate mandatory non-PK/FK/date
   columns during generation (copy from the bootstrapped source row instead of blanking).

3. **Seed/constrain operation reference combos (fixes 2.3).** Ensure synthetic `OPERACAO`/`DADO_OPERACAO`
   reference a valid `(tipo IF, tipo_operação, modalidade_liquidação, objeto_serviço)` combination for CDB.

4. **Gate every run with `validate_cdb_simplificado.py`** before the Oracle append (CI: fail on ERROR).

---

## 6. Key references

- `dados/xml/CondicaoIFDO.hbm.xml` — supertable + subtype joined-subclasses (no discriminator).
- `dados/.../instrumentofinanceiro/TipoCondicaoIFDO.java` — `COD_TIPO_CONDICAO_IF` code → subtype name.
- `dados/.../instrumentofinanceiro/TipoIFDO.java:49` — CDB = Id("49").
- `dados/.../depositaria/DetalheTransferenciaSemFinanceiroDO.java:38` — `COD_MOTIVO` (ORA-01400).
- `engorda_tables.py`: `FILTROS_FONTE` (221), `bind_shared_key_children` (3731), `assert_not_null_ok`
  (3967), `_fk_is_whole_pk` (3023), `compute_pk_maxes`/`_set_unique_pk_column` (1238).
- `validate_cdb_simplificado.py` — the pre-load validator described in §3.3.