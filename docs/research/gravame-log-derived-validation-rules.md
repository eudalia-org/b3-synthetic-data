# Gravame Log-Derived Validation Rules

## Summary

`gravame_ativo.log` and `gravame_conta.log` contain ten successful asynchronous `GRVM INCL`
registrations, five in each trace. Both persist a type-175 `GRVM` instrument and a common contract,
endpoint, document, protocol, account, and operation graph. The second trace additionally links a
CDB/LCI guarantee and runs the constitution/transfer operation chain.

The filenames are not reliable variant selectors. Validation should classify the persisted graph:

| Profile | Selector | Link/operation shape | Samples |
|---|---|---|---:|
| Contract-only | `IF_GRVM.NUM_ID_TIPO_CONST_GRAVAME=1` | no active pledge link; root operations `520 -> 527` | 5 |
| Instrument-backed | `IF_GRVM.NUM_ID_TIPO_CONST_GRAVAME=2` | one active pledge link; `520 -> 527 -> 541 -> 529` | 5 |

The traces are successful insertion evidence, not rejection tests. Exact cardinalities and constants
must begin as advisory registration-profile checks.

## Evidence Qualification

- Both routes use asynchronous `ProcessadorArquivoInclusaoContratoGarantia` under `GRVM INCL`
  (`gravame_ativo.log:9`; `gravame_conta.log:10`).
- Five registration messages are dispatched in each trace (`gravame_ativo.log:365,713,1686,2034,
  2383`; `gravame_conta.log:371,720,1694,2043,2393`).
- All ten pass `validarInclusaoContratoGarantia`, commit their roots, and later produce approval
  alerts (`gravame_ativo.log:2728-2811,3220-3224,3994-3999`;
  `gravame_conta.log:2737-2806,3557-3561,4329-4334`).
- `PROCESSADOS=0 ERRO=0` is emitted after dispatch, before workers finish. It is not a zero-success
  verdict.
- Root registration and downstream approval/constitution/transfer use separate commits. A partial
  lifecycle state is possible after interruption; validation must not model the aggregate as one
  atomic transaction.

## Root Identity

The aggregate root is `INSTRUMENTO_FINANCEIRO.NUM_IF`. Every observed root has
`NUM_TIPO_IF=175`, resolved as `GRVM`, while `COD_IF` is allocated by
`PKG_CODIGO.f_getcodigoifgrvm` (`gravame_ativo.log:2891-2905`;
`gravame_conta.log:3236-3250`). `COMPLEMENTO_CONTRATO` and `IF_GRVM` are shared-key extensions on
the same `NUM_IF` (`gravame_ativo.log:2906-2915`; `gravame_conta.log:3251-3263`).

Hard rules:

- Active roots selected as Gravame must have `NUM_TIPO_IF=175`.
- Every present extension/child must resolve to an active type-175 root, except explicitly external
  guaranteed-instrument references.
- Root and contract dates must parse before ordering checks; issuance/registration must not exceed
  maturity.
- Live Oracle PK, FK, NOT NULL, and capacity metadata remains authoritative.

Advisory rules:

- Active trimmed `COD_IF` is nonblank and unique in all ten registrations.
- The observed `26F...` allocator output is not a proven universal regex.
- Exact-one root, complement, and Gravame-extension rows is a registration-snapshot profile, not a
  guaranteed lifecycle invariant.

## Common Aggregate

Every registration eventually persists the following rows:

| Table | Observed rows/root | Ownership |
|---|---:|---|
| `INSTRUMENTO_FINANCEIRO` | 1 | root `NUM_IF` |
| `COMPLEMENTO_CONTRATO` | 1 | shared `NUM_IF` |
| `IF_GRVM` | 1 | shared `NUM_IF` |
| `PARAMETRO_PONTA` | 2 | `NUM_IF`; P1/P2 roles |
| `CONTA` | 2 | generated account referenced by endpoint parameters |
| `ARQUIVO_TRANSF` | 1 | PDF metadata |
| `ARQUIVO_TRANSF_CONTEUDO` | 1 | shared transfer-file ID/blob |
| `ARQUIVO_IF` | 1 | root-to-transfer-file bridge |
| `PROTOCOLO` | 1 | root `NUM_IF` |
| `ALERTA` | 1 | approval side effect; no stable root FK observed |
| `OPERACAO` | at least 2 | root or guaranteed IF, linked by original operation code |
| `LANCAMENTO` | one per observed operation | `NUM_ID_OPERACAO` |

Representative DML appears at `gravame_ativo.log:2843-2935,2990-3059,3305-3511,
3583-3996` and `gravame_conta.log:3188-3396,3642-5003`.

Hard graph checks:

- Present `COMPLEMENTO_CONTRATO`, `IF_GRVM`, `PARAMETRO_PONTA`, `ARQUIVO_IF`, and `PROTOCOLO`
  rows must resolve to a root.
- Present launches and operation-data rows must resolve to an included operation.
- Present `PARAMETRO_PONTA.NUM_CONTA` references must resolve to included generated `CONTA` rows.
  External participant accounts are separate operation fields and remain Oracle-backed.
- Every nonblank `COD_OPERACAO_ORIGINAL` in the exported closure must resolve to one included
  operation code. Duplicate codes and cycles within the five-link audit boundary are errors;
  deeper historical chains remain `WARN` until a scalable recursive audit is available.
- `ARQUIVO_IF.NUM_ID_ARQUIVO_TRANSF` and transfer content must resolve to the same included transfer
  file.

Exact counts, endpoint roles, account types, protocol/alert constants, PDF name/MIME, operation
states, and final `IF_GRVM` situation `3` remain advisory.

## Endpoint And Account Graph

Each root has two `PARAMETRO_PONTA` rows. P1 is titular (`S`) and P2 is non-titular (`N`), both use
role `74`, and their generated accounts are named `<COD_IF>P1` and `<COD_IF>P2`. The account rows
are type `161`, active situation `1`, and later written back to the endpoint parameters
(`gravame_ativo.log:2921-2935,3386-3435`; `gravame_conta.log:3266-3280,3723-3767`).

The application validates participant status, family access, account radical, CPF/CNPJ relations,
FATCA/INR data, and grade `CTP36`, but the two successful batches do not expose rejected account
counterexamples. Therefore:

- FK existence and unambiguous local endpoint-to-generated-account ownership are hard.
- Exactly two endpoints/accounts, P1/P2 suffixes, role `74`, titular polarity, and account
  type/status are opt-in advisory checks.
- Do not reuse CDB/RDB `.40/.10` account eligibility assumptions.

## Operations And Routes

Both variants start with root operation `520` on object service `1132` and follow with operation
`527`. The application explicitly resolves these routes and requires platform
`COD_OBJETO_SERVICO='GRVM'`, `IND_PLATAFORMA_BAIXA='S'`
(`gravame_ativo.log:2936-2954,3553-3564`; `gravame_conta.log:3281-3294,3890-3897`).

Hard target checks:

- Target type identity must resolve active type `175` / code `GRVM`.
- Platform `GRVM` must be enabled for baixa.
- Every active Gravame root must have an operation-type `520` route on object service `1132`.
- Operation `527` and exact route IDs are registration-profile evidence until source/rejection
  evidence proves lifecycle-wide requirements.

Observed advisory constants include settlement modality `6`, quantity `1`, debit type `1`, route
IDs `15394`/`15512`, CDB constitution/transfer routes `15464`/`15035`, LCI alternatives
`15422`/`15046`, and the status choreography through `402`, `21`, `548`, and `43`.

## Instrument-Backed Variant

`gravame_conta.log` validates and links four CDBs and one LCI. Every root adds:

- one active `GRAVAME_GRAU_PENHOR` row keyed by `NUM_IF_GRAVAME` and externally referencing
  `NUM_IF_GARANTIA` (`gravame_conta.log:6221-6437`);
- one operation `541` against the guaranteed IF and one operation `529` chained after it;
- one launch per added operation;
- six `DADO_OPERACAO` rows per added operation, twelve per Gravame.

The strongest application uniqueness evidence is the pre-insert lookup by Gravame, guaranteed IF,
degree `1`, and active status (`gravame_conta.log:6179,6196-6197,6410,6416`). This supports warning
on duplicate active tuples but does not prove a database unique constraint.

Hard rules:

- Present pledge rows must resolve `NUM_IF_GRAVAME` to one active Gravame root.
- `NUM_IF_GARANTIA` must resolve to an active, non-matured target instrument through live Oracle
  lookup; it is not required to be another type-175 root in the synthetic batch.
- Present operation `541/529` closure and operation-data ownership must be internally consistent.
- Known root routes `15394/15512` must target the Gravame root; known constitution/transfer routes
  `15422/15464/15035/15046` must target that root's pledged `NUM_IF_GARANTIA`. Other historical
  route families remain under generic FK validation until their semantics are captured.

Advisory rules:

- Constitution type `2` correlates with one pledge, four operations, four launches, and twelve
  operation-data rows.
- Quantity `1`, degree `1`, blocked prenotation `S`, and guaranteed-event flag `N` are observed
  fixture shape, not universal business limits.
- One guaranteed IF per Gravame is not a proven maximum.

## Contract-Only Variant

`gravame_ativo.log` persists constitution type `1`, no pledge row, no operation-data rows, and only
the `520 -> 527` operation chain. Later generic reads of `GRAVAME_GRAU_PENHOR` do not prove a
missing insert (`gravame_ativo.log:2911-2915,2990-2994,3583-3591,4033-4036`).

Treat the correlation as an opt-in warning profile. Do not infer from the filename that this is the
instrument-backed route.

## Unsafe Inferences

- Do not treat exact row counts, IDs, dates, rates, debts, statuses, or account values as universal.
- Do not infer failure from asynchronous `PROCESSADOS=0`, startup rollbacks, or informational stack
  traces followed by commits.
- Do not require every Gravame to reference a guaranteed instrument.
- Do not reject lifecycle snapshots with additional historical operations.
- Do not reuse CDB/CCB condition, schedule, deposit, specification, or wallet closures.
- Do not infer that absent event/history rows are prohibited.
- Do not assume all target instruments are CDB or LCI; the route engine lists many eligible types.

## Implementation Boundary

The validator/profiler is implemented as an isolated `pipeline="gravame"` using type `175`,
object service `1132`, platform `GRVM`, graph ownership, operation-chain closure, advisory variant
profiles, and Gravame-specific shape metrics.

Generation remains blocked:

1. `datagen/engorda_tables.py` declares `"gravame": ()`.
2. The Gravame query block in `datagen/queries_produtos.sql` is empty.
3. Committed metadata omits `IF_GRVM`, `COMPLEMENTO_CONTRATO`, the document/protocol tables, and
   `GRAVAME_GRAU_PENHOR`.
4. Gravame requires `f_getcodigoifgrvm`, generated P1/P2 accounts, transfer-file remapping, and
   original-operation-chain remapping that the generic generator does not provide.

The first validator verdict should remain **PARTIAL** without live Oracle metadata and a compatible
production-derived Gravame shape baseline.
