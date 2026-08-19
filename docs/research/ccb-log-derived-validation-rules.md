# CCB Log-Derived Validation Rules

## TL;DR

The five traces contain 22 successful CCB registrations across five route/variant families and
multiple exact cardinality shapes. They prove
that the observed CCB registration snapshots are `INSTRUMENTO_FINANCEIRO` type `53` and all contain
a title/credit/CCB-extension core, one fixed-interest condition, a CCB-specific event schedule,
and a registration/deposit operation. They do **not** prove that every lifecycle snapshot has this
exact presence or cardinality.

Validation should therefore have three layers:

1. Hard schema-backed identity, PK/FK/NOT NULL, and joined-subclass integrity.
2. Conditional rules selected from persisted payment form and condition/event discriminators.
3. Opt-in advisory registration profiles for exact counts and fixture constants.

The current generator cannot yet produce a complete CCB aggregate. All five CCB entries in
`TABELAS_ENGORDA_POR_PRODUTO` are empty (`datagen/engorda_tables.py:760-764`), and the committed
`specs.json` contains `TCTPCRONOGRAMA_CCB` but not several observed core and variant tables.

## Source Qualification

All five files are insertion evidence, not failed attempts:

| File | Route | Successful registrations | Completion evidence |
|---|---|---:|---|
| `ccb_fapre.log` | file `CCB INCL` | 5 | worker commits at `:3088`, `:3118`, `:3122-3124` |
| `ccb_FAVCP.log` | file `CCB INCL` | 5 | worker commits at `:4477`, `:4554`, `:4587`, `:4630`, `:4648` |
| `ccb_PPFPRE.log` | simplified file `CCB REG` | 5 | worker commits at `:1943-1966` |
| `ccb_PGRPRE.log` | file `CCB INCL` | 5 | worker commits at `:2812-2887` |
| `ccb_PPPRE.log` | API `POST /pipeline/b3/v1/ccb/instruments` | 2 | commits at `:934`, `:1885`; HTTP 200 at `:941-951`, `:1892-1904` |

The file-level `PROCESSADOS=0 ERRO=0` messages are emitted before asynchronous workers finish
(`ccb_fapre.log:186-200`, `ccb_FAVCP.log:169-183`, `ccb_PPFPRE.log:59-68`,
`ccb_PGRPRE.log:115-124`). They do not mean zero registrations. Likewise, startup and trailing
connection rollbacks do not undo the later worker commits.

Confidence is **high** for the observed common graph, type identity, joined-subclass mappings,
platform, and registration route. Confidence is **medium** for the resgate/baixa relationship,
payment-form
variant allow-lists and schedule semantics because each form was captured in one batch. Exact
counts, dates, rates, accounts, and party identities are empirical fixtures.

## Common Persisted Aggregate

Every one of the 22 registrations writes the following instrument-scoped core:

| Table | Observed rule | Representative evidence |
|---|---|---|
| `INSTRUMENTO_FINANCEIRO` | exactly one root, `NUM_TIPO_IF=53` | `ccb_fapre.log:703-708`; `ccb_FAVCP.log:597-601`; `ccb_PPFPRE.log:564-568`; `ccb_PGRPRE.log:572-578`; `ccb_PPPRE.log:381,1334` |
| `TITULO` | exactly one row sharing `NUM_IF` | `ccb_fapre.log:707-717`; `ccb_FAVCP.log:602-606`; `ccb_PPFPRE.log:569-580`; `ccb_PPPRE.log:382,1335` |
| `CREDITO` | exactly one row sharing `NUM_IF` | `ccb_fapre.log:725-729`; `ccb_FAVCP.log:619-623`; `ccb_PPFPRE.log:589-593`; `ccb_PGRPRE.log:593-601`; `ccb_PPPRE.log:386,1339` |
| `CONDICAO_IF` | at least two active rows; exactly one observed type `2` | `ccb_fapre.log:735-765`; `ccb_FAVCP.log:624-648`; `ccb_PPFPRE.log:594-636`; `ccb_PGRPRE.log:603-624`; `ccb_PPPRE.log:402-408,1355-1361` |
| `JUROS_FIXO` | exactly one row sharing the type-`2` condition key | same condition ranges |
| `HISTORICO_PU_CURVA` | exactly one row by `NUM_IF` in a registration snapshot | `ccb_fapre.log:767-771`; `ccb_FAVCP.log:640-650`; `ccb_PPFPRE.log:631-646`; `ccb_PPPRE.log:409,1362` |
| `HISTORICO_IF_TITULO` | exactly one registration-history row | `ccb_fapre.log:1063-1067`; `ccb_FAVCP.log:715-719`; `ccb_PPFPRE.log:790-895`; `ccb_PGRPRE.log:702-708`; `ccb_PPPRE.log:427,1380` |
| `ALTERACAO_IF` | exactly one registration alteration, observed type `R` | `ccb_fapre.log:1073-1077`; `ccb_FAVCP.log:725-729`; `ccb_PPFPRE.log:1771-1957`; `ccb_PGRPRE.log:712-716`; `ccb_PPPRE.log:429,1382` |
| `TCTPIF_CCB` | exactly one active CCB extension sharing `NUM_IF` | `ccb_fapre.log:1081-1088`; `ccb_FAVCP.log:735-741`; `ccb_PPFPRE.log:1785-1959`; `ccb_PGRPRE.log:722-726`; `ccb_PPPRE.log:431,1384` |
| `TCTPCRONOGRAMA_CCB` | one or more event rows; fan-out depends on variant and horizon | ranges detailed below |
| `OPERACAO` | exactly one registration/deposit operation in each captured snapshot | `ccb_fapre.log:2115-2119`; `ccb_FAVCP.log:2851-2855`; `ccb_PPFPRE.log:1119-1576`; `ccb_PGRPRE.log:1605-1618`; `ccb_PPPRE.log:735,1687` |
| `LANCAMENTO` | exactly one launch under that operation | `ccb_fapre.log:2200-2672`; `ccb_FAVCP.log:2941-3413`; `ccb_PPPRE.log:756-873,1708-1825` |

`GARANTIA` and `TCTPCADEIA_IPOC` are conditional. Only the two API PPPRE registrations write
them: five guarantees and four IPOC-chain rows per IF (`ccb_PPPRE.log:389-401,617-620` and
`:1342-1354,1570-1573`). No captured application-issued SQL writes either table in the other 20
registrations, so they are not part of those observed registration profiles. Confirm committed
snapshots and trigger/procedure behavior before treating them as absent from every such aggregate.

No captured application-issued SQL writes `DEPOSITO_AUTOMATICO_IF`, `DADO_OPERACAO`,
`ESPECIFICACAO`, or either wallet table. This is sufficient to reject blindly reusing CDB/RDB's
registration closure, but committed snapshots and trigger/procedure behavior still need checking
before declaring those tables absent from every CCB aggregate.

## Executable Rule Candidates

Schema-backed ownership, identity, and joined-subclass integrity can be hard errors. Correlations
inferred only from successful insertions should initially be warnings; promote them only after
application source, rejection tests, or a production cohort supplies independent evidence.

### 1. Identity and active roots

- Active roots must have `NUM_TIPO_IF=53`; a CCB run containing another active type is invalid.
- The observed `COD_IF` values are nonblank and unique, but committed metadata does not make that
  a hard CCB constraint. Start with a registration-profile warning. Codes such as `25H00017669`
  and `26F00481979` prove that a CDB-style `CCB` prefix rule is invalid.
- `DAT_EMISSAO <= DAT_VENCIMENTO` and `DAT_REGISTRO <= DAT_VENCIMENTO`.
- Root and credit dates must parse before order checks; title dates are inherited from the root and
  are not persisted on the physical `TITULO` rows in these traces. Malformed dates must not pass as
  SQL null comparisons.

### 2. Required one-to-one core

All 22 observed registration snapshots resolve each active type-53 root to exactly one active
`TITULO`, `CREDITO`, `TCTPIF_CCB`, and registration `HISTORICO_PU_CURVA` row. Required presence and
exact-one should initially be registration-profile warnings unless live constraints, application
source, or a production cohort confirms them. The history and alteration tables should be
preserved for exact registration-aggregate cloning, but later lifecycle snapshots can legitimately
contain more than one historical row.

Every present child row must satisfy live Oracle PK, FK, NOT NULL, and capacity metadata. Those
metadata checks remain hard independently of the inferred presence/cardinality profile.

### 3. Condition polymorphism

`CONDICAO_IF` uses the same shared-key joined-subclass model as CDB/RDB. For every active known
condition, exactly one physical subtype row must exist on the same `NUM_CONDICAO_IF`, and no other
known subtype may claim that key:

| `COD_TIPO_CONDICAO_IF` | Physical table | CCB evidence |
|---:|---|---|
| `1` | `AMORTIZACAO` | `ccb_fapre.log:756-764`; `ccb_FAVCP.log:633-649` |
| `2` | `JUROS_FIXO` | all five traces; representative `ccb_PPFPRE.log:612-628` |
| `4` | `ATUALIZACAO_POS` | `ccb_fapre.log:735-744`; `ccb_PGRPRE.log:603-613` |
| `5` | `SPREAD` | `ccb_PPFPRE.log:602-621` |
| `14` | `ATUALIZACAO_PRE` | `ccb_PPFPRE.log:594-611`; `ccb_PPPRE.log:402-403,1355-1356` |
| `20` | `RESGATE` | all traces in the conditional subsets described below |

Unknown condition codes should produce `WARN` until their physical mapping and a successful CCB
route are captured. A known code with a missing, duplicate, wrong, or orphan physical subtype is
an `ERROR`.

Every observed CCB contains exactly one active type-`2`/`JUROS_FIXO` pair. Report missing or extra
pairs as a registration-profile warning until application source, rejection evidence, or a
production cohort confirms mandatory lifetime semantics.

### 4. Resgate, CCB extension, and terminal event

Across every exercised combination, these three facts correlate exactly:

```text
active CONDICAO_IF type 20 + shared RESGATE
  <=> TCTPIF_CCB.IND_BAIXA_VENCIMENTO = 'S'
  <=> one active TCTPCRONOGRAMA_CCB legacy type 85
```

Positive and negative cases coexist inside `fapre`, `FAVCP`, and `PGRPRE`; PPFPRE and PPPRE are
all positive. Evidence includes `ccb_fapre.log:765-766,1016,1081-1088`,
`ccb_FAVCP.log:643-644,688,735-741`, `ccb_PGRPRE.log:623-628,667-673,722-726`,
`ccb_PPFPRE.log:622-636,731-845,1785-1959`, and `ccb_PPPRE.log:407-415,431`.

Initial validation consequences (`WARN`, not `ERROR`, while supported only by insertion traces):

- `IND_BAIXA_VENCIMENTO='S'` requires exactly one active type-20 parent, one shared `RESGATE`,
  and one active type-85 schedule event.
- `IND_BAIXA_VENCIMENTO='N'` rejects those active rows.
- `RESGATE.DAT_RESGATE` and the type-85 original event date must equal the observed maturity date
  for these registration profiles. Keep this advisory until a broader production cohort confirms
  that early-redemption CCB variants cannot use another date.
- `COD_TIPO_EXERCICIO='EUROPEIA'` is consistent in every observed resgate but remains an advisory
  allowed-value profile rather than a universal rule.

### 5. Event schedule integrity

`TCTPCRONOGRAMA_CCB` is not interchangeable with generic `EVENTO`.

Schema-backed checks:

- Every present schedule row must satisfy its live `NUM_IF` FK and `NUM_EVENTO_CCB` PK/NOT NULL
  metadata.
- Exact event counts belong in a trusted shape baseline or opt-in registration profile, never in
  the baseline-free CCB validator.

Semantic insertion-profile candidates are date parsing, issuance/maturity bounds for original
dates, unique nonblank `COD_PARCELA` within an IF, and finite nonnegative event/PU values.
Occurrence and liquidation can
move beyond maturity after business-day adjustment (`ccb_fapre.log:912,1016`;
`ccb_PPFPRE.log:730,733,844`). The successful traces show these properties but do not show rejection
of counterexamples, so they should initially be warnings.

Observed event families are `83/84/85` for FAPRE/FAVCP, `157/85` for PPFPRE and PPPRE, and
`90/85` for PGRPRE. Their stored codes are exact evidence; labels beyond the log's explicit
type-85 `RESGATE` wording require lookup metadata.

### 6. Registration operation and platform

Every trace consults active CCB platform availability through
`V_OBJETOS_SERVICO.COD_OBJETO_SERVICO='CCB'` and `IND_PLATAFORMA_BAIXA='S'`, for example
`ccb_fapre.log:1898-1904`, `ccb_PPFPRE.log:900-934`, `ccb_PGRPRE.log:1391-1439`, and
`ccb_PPPRE.log:694-698`.

The registration route resolves object service `47`, operation code `1`, and persists
`OPERACAO.NUM_ID_TIPO_OPER_OBJETO_SERV=871`, settlement modality `6`, debit sides `1/2`, and one
launch. This supports the same narrowing used for CDB/RDB: enforce CCB registration semantics only
for operations whose target TOS resolves to operation code `1`; keep generic FK validation on all
historical operations.

For the captured registration operation:

- P1 is the creditor/custodian `.00` account and P2 is the registrar/payment-agent `.40` account.
- Both operation account references resolve in the captured target. CCB-specific eligibility rules
  need application rejection evidence before becoming more than advisory.
- Nonblank, locally/target-unique `COD_OPERACAO` and P1/P2 meu-numero tuples are advisory until the
  corresponding collision queries or application source are reviewed as rejection evidence.
- Exactly one operation and launch is advisory registration shape. Do not reject later historical
  operations merely because a whole-entity clone has more than one operation.

## Variant Matrix

Payment form is a useful selector, but `3253` has two valid observed shapes and therefore needs an
additional discriminator. The corpus supports root agenda and type-4 condition presence as
candidate discriminators. Unknown payment forms should not be forced into one of these profiles.

| Corpus label | Form | Root agenda | Required observed condition graph | Schedule family | Sample |
|---|---:|---|---|---|---:|
| FAPRE | `3253` | constant | `4/ATUALIZACAO_POS`, `2/JUROS_FIXO`, `1/AMORTIZACAO`, optional `20/RESGATE` | `83`, `84`, optional `85` | 5 |
| FAVCP | `3253` | variable | `2/JUROS_FIXO`, `1/AMORTIZACAO`, optional `20/RESGATE` | `83`, `84`, optional `85` | 5 |
| PPFPRE | `229` | constant | `14/ATUALIZACAO_PRE`, `5/SPREAD`, `2/JUROS_FIXO`, `20/RESGATE` | semiannual `157` plus `85` | 5 |
| PGRPRE | `8` | nonconstant | `4/ATUALIZACAO_POS`, `2/JUROS_FIXO`, optional `20/RESGATE` | annual `90`, optional `85` | 5 |
| PPPRE | `3261` | constant | `14/ATUALIZACAO_PRE`, `2/JUROS_FIXO`, `20/RESGATE` | explicit `157` rows plus `85` | 2 |

The matrix supports an opt-in `--registration-profile` check or cohort classifier. It is not yet
a universal allow-list because the corpus does not prove that these are every CCB payment form or
every valid shape under each form.

### FAPRE versus FAVCP

Both persist payment form `3253`, type-`2` fixed interest, type-`1` amortization, and optional
resgate. FAPRE additionally persists type `4`/`ATUALIZACAO_POS` and uses a constant-flow service
(`ccb_fapre.log:735-764,1725-1738`). FAVCP has no type-4 row and uses nonconstant flow
(`ccb_FAVCP.log:624-649,2476-2496`). A selector based on payment form alone would merge distinct
valid graphs.

### PPFPRE

All five simplified records persist types `14`, `5`, `2`, and `20` with shared physical rows
(`ccb_PPFPRE.log:594-636`). The schedule has 16-24 type-157 rows plus one type-85 row depending on
maturity horizon (`:655-845`). The exact count and six-month cadence are profile data, not hard
global cardinalities.

### PGRPRE

All five records persist type `4`/`ATUALIZACAO_POS` and type `2`/`JUROS_FIXO`; only two add
type `20`/`RESGATE` (`ccb_PGRPRE.log:603-628`). The schedule has 9-12 type-90 rows and those two
IFs add type `85` (`:667-673`, `:1238-2876`). Both resgate outcomes occur under regimes `1` and
`2`; title form remains inconclusive because the only cartular sample is resgate-positive.

### PPPRE API

Both API requests persist types `14`, `2`, and `20`, two type-157 schedule rows, one generated
type-85 row, five guarantees, and a four-row IPOC chain (`ccb_PPPRE.log:389-415,617-620,664-667,
898-911`; repeated at `:1342-1369,1570-1573,1617-1620,1849-1862`). Five guarantees and four IPOC
rows are demonstrated fixture shape, not proven minimums. Live FK metadata can make ownership and
chain-link resolution hard errors. Acyclicity and exactly one chain root per CCB should initially
be advisory because only two linear fixtures were observed.

## Advisory Registration Profile

The following constants are useful for fixture reproduction and drift warnings, but are unsafe as
baseline-free errors:

- `NUM_SISTEMA=55`, quantity `1`, `IND_ADITAMENTO='N'`, and initial/final status transitions.
- Payment-form IDs `3253`, `229`, `8`, and `3261` outside their explicit variant classifiers.
- Credit municipality, modality, qualification, PF nature, and `SEM COOBRIGACAO` values.
- Exact account IDs, Bank Leme/Facta identities, participant documents, rates, nominal values,
  operation object `871`, and state transitions `402 -> 542`.
- Exact event counts, yearly or half-yearly cadence, event state, PU, and value formulas.
- Exact guarantee count/types/owners and four-level IPOC depth.
- Exact `COD_IF` body pattern. The corpus proves nonblank uppercase allocator output, not every
  allocator branch.

## Unsafe Inferences

- Do not reuse the CDB/RDB 16-table registration graph; no captured application-issued CCB SQL
  writes deposits, operation data, specifications, or wallets in these traces.
- Do not require `AMORTIZACAO`, `ATUALIZACAO_POS`, `ATUALIZACAO_PRE`, `SPREAD`, `RESGATE`,
  `GARANTIA`, or `TCTPCADEIA_IPOC` for every CCB.
- Do not require one exact schedule cardinality or event-code mix globally.
- Do not infer failure from asynchronous `PROCESSADOS=0` or unrelated rollbacks.
- Do not treat SELECT row counts as rejection rules. They prove that configuration was consulted,
  not that every empty result aborts registration.
- Do not treat Oracle empty text as a distinct nonnull value in Spark.
- Do not require all historical operations to use registration TOS `871`; narrow the rule through
  object `47` plus operation code `1`.

## Implementation Gap and Recommended Split

### Current blockers

1. The generator declares all five CCB products but assigns each an empty dynamic-table tuple at
   `datagen/engorda_tables.py:760-764`; no CCB aggregate is currently cloned.
2. `specs.json` includes `TCTPCRONOGRAMA_CCB` and marks it static (`specs.json:4829`). Engorda will
   override that flag in memory once the table is added to a CCB product tuple, but the default
   loader skips tables left static when invoked with unchanged metadata. The spec does not include
   observed `TCTPIF_CCB`,
   `TCTPCADEIA_IPOC`, `GARANTIA`, `HISTORICO_PU_CURVA`, `HISTORICO_IF_TITULO`, or `ALTERACAO_IF`
   definitions. `spec.local.json` adds `GARANTIA` and `HISTORICO_PU_CURVA`, but still does not
   close the full observed graph.
3. `validate_products.py --product ccb` now provides dedicated type-53 identity, metadata,
   ownership, polymorphism, resgate correlation, target route/platform, shape, and opt-in
   registration-profile checks. It deliberately bypasses the incompatible CDB/RDB domain and
   account assumptions.
4. `profile_cdb_shapes.py --product ccb` now profiles the condition mix, six physical subtypes,
   CCB schedule event families, histories, operation closure, guarantees, and IPOC chains. CCB
   rejects CDB-specific source filters and domain-universe mode.

### Recommended implementation order

1. Add the complete common CCB table metadata and direct/shared-key ownership edges first.
2. Done: one `ccb` validator profile covers type `53`, object `47`, platform code `CCB`, and
   CCB-specific graph checks while keeping insertion-only variants advisory.
3. Done: the CCB shape profiler includes condition-type and `TCTPCRONOGRAMA_CCB` event-code metrics
   and requires a trusted compatible baseline before enforcing distributions.
4. Populate each `ccb_*` generator table set from the variant matrix, preserving schedule fan-out,
   optional resgate, guarantees, and IPOC chains by root rather than independently sampling rows.
5. Capture production cohorts per payment form before promoting exact condition/event mixes from
   `WARN` to `ERROR`.

The safe first validator verdict is therefore **PARTIAL** until live Oracle metadata covers the
full graph and a production-derived CCB shape baseline is supplied.
