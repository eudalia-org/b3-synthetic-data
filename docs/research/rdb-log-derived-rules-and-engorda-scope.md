# RDB Log-Derived Rules and Engorda Scope

## TL;DR

Both files are now successful five-instrument RDB registration traces. They prove a 16-table
shared instrument graph and a precise liquidity delta: plain inclusion persists
`RESGATE.COD_COND_RESGATE='SEM TABELA'`; com-resgate persists `COM TABELA`, adds a variable
1-to-5-row `CONDICAO_RESGATE` schedule, and uses `PENDENCIA_IF` while that schedule is pending.

Complete cloning therefore needs 16 tables for either variant and 2 additional tables for
com-resgate. `HISTORICO_PU_CURVA` is shared but missing from `specs.json`; `CONDICAO_RESGATE`
and `PENDENCIA_IF` are also missing. The inclusion run additionally created six comitente
bootstrap tables because its five CPFs did not exist yet. Those rows are conditional on target
state and must be ensured once per comitente, not multiplied blindly per RDB.

## Source Qualification

### `rdb_inclusao.log` is valid plain-RDB evidence

The replacement file selects trancode `RDB INCL` and `ProcessadorArquivoRegistroRF`
(`rdb_inclusao.log:16`). The rule input explicitly says `Nao tem condicao de liquidez`
(`:110-111`), and all five roots persist `NUM_TIPO_IF=50` with codes `RDB72601DH5` through
`RDB72601DH9` (`:598`, `:600`, `:2340`, `:2408`, `:2427`).

It writes 22 table types. Six are conditional comitente bootstrap rows; the 16-table
instrument graph is identical to the com-resgate core. Every RDB has a type-20 `RESGATE`, but
its condition is `SEM TABELA` (`:616`, `:629`, `:2437`, `:2461`, `:2649`). There are no
`CONDICAO_RESGATE` or `PENDENCIA_IF` writes.

### `rdb_com_resgate.log` is valid RDB evidence

The file selects trancode `RDB INCL` and `ProcessadorArquivoRegistroRF`
(`rdb_com_resgate.log:18`). It validates `TIPO_IF.COD_TIPO_IF='RDB'` (`:126-127`) and commits
five complete registrations. The persisted roots have `NUM_TIPO_IF=50` and codes
`RDB72601DHA` through `RDB72601DHE` (`:1354`, `:1367-1369`, `:1444`).

Overall confidence is **high for the observed variant delta and shared registration profile**:
the two independent files agree on the 16-table core and differ exactly at liquidity-specific
tables. Confidence remains **medium for universal RDB rules** because both files use the same
issuer, payment form, date, quantity, and floating-rate family.

## Inferred RDB Rules

### Identity and platform

| Rule | Status | Evidence |
|---|---|---|
| `NUM_TIPO_IF=50` | Confirmed | Root inserts at `rdb_com_resgate.log:1354`, `:1367-1369`, `:1444` |
| Object service `45` | Confirmed | Runtime lookup at `:188-192` |
| Platform code `RDB`, baixa enabled `S` | Confirmed | Repeated `V_OBJETOS_SERVICO` predicate at `:674-678` |
| `COD_IF` begins with `RDB` | Confirmed for this allocator run | Five allocated values at `:1354`, `:1367-1369`, `:1444` |
| Full observed COD_IF shape `^RDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$` | Supported by sample, not exhaustive | `RDB72601DHA`-`E` at the same lines |

The trace upgrades platform evidence from UNKNOWN to confirmed. The COD_IF prefix and current
shape are now observed, but five sequential values do not prove every allocator branch.

### Root and title registration profile

All five roots share these persisted values (`rdb_com_resgate.log:1354`, `:1367-1369`,
`:1444`); the title values are at `:1362`, `:1370`, `:1374`, `:1377`, and `:1446`:

| Table.column | Observed value |
|---|---|
| `INSTRUMENTO_FINANCEIRO.IND_AGENDA_CONSTANTE` | `S` |
| `INSTRUMENTO_FINANCEIRO.NUM_SISTEMA` | `55` |
| `INSTRUMENTO_FINANCEIRO.NUM_TIPO_IF` | `50` |
| `INSTRUMENTO_FINANCEIRO.IND_ESPECIFICA_COMITENTE` | `N` |
| `INSTRUMENTO_FINANCEIRO.NUM_ID_MOTIVO_SITUACAO_IF` | `73` |
| `INSTRUMENTO_FINANCEIRO.COD_SITUACAO_IF` | initially `0`, temporarily `24`, finally `0` |
| `INSTRUMENTO_FINANCEIRO.NUM_ID_FORMA_PAGAMENTO` | `171` |
| `INSTRUMENTO_FINANCEIRO.IND_RESIDUO` / `IND_PADRAO` | `N` / `N` |
| `INSTRUMENTO_FINANCEIRO.IND_EXCLUI_IOF` / `IND_ELEGIVEL_IOF` | `N` / `N` |
| `TITULO.NUM_CONTA_PARTICIPANTE` | issuer account `.40` |
| `TITULO.IND_FRACIONAMENTO` | `N` |
| `TITULO.NUM_ID_TIPO_REGIME_TITULO` | `2` |
| `TITULO.QTD_EMITIDA` | same quantity as the registration/deposit (`50000` here) |

Payment form `171` is selected from payment group `7` for RDB (`:117`). These constants are a
strong observed profile for this route, not proof that every alternative RDB payment form uses
the same values.

### Condition and resgate graph

Every sampled RDB in both files has exactly two `CONDICAO_IF` rows:

1. One type `3` row plus one shared-key `JUROS_FLUTUANTE` row (`:1384-1386`, `:1420`,
   `:1445`, `:2173`, `:2459`). The sample uses index `4`, commercial-year `2`, calendar-days
   `1`, indicator type `0`, agenda `CONSTANTE`, and rates `90.01` to `90.05`.
2. One type `20` row plus one shared-key `RESGATE` row in both variants. Every observed
   resgate is `EUROPEIA`; plain inclusion uses `SEM TABELA`
   (`rdb_inclusao.log:615-616`, `:619`, `:629`, `:2436-2437`, `:2445-2446`, `:2461`, `:2649`),
   while com-resgate uses `COM TABELA` (`rdb_com_resgate.log:1387-1393`, `:1447`,
   `:1540-1541`, `:1592`, `:2460-2461`).

`CONDICAO_RESGATE` is the schedule below `RESGATE`. It is not one-to-one: the five instruments
have 1, 2, 3, 4, and 5 schedule rows, totaling 15 (`:1542-1567`, `:1831-1882`,
`:2096-2118`, `:2998-3012`, `:3151`). Within each instrument:

- `DAT_RESGATE` is after registration and before maturity in this sample.
- Dates are strictly increasing.
- `VAL_PERCENTUAL` is also increasing in each schedule.
- Values can exceed 100 (`113.33` and `116.66`), so a `[0,100]` validation would be wrong.
- `VAL_SPREAD` and `COD_SITUACAO_EXERCICIO` are blank in the sample.

All five observed `COM TABELA` resgates have at least one schedule row. That supports a cohort
selector and clone-integrity check for this route, but not a universal claim that the
application rejects zero rows. Preserve all rows and their order by date; do not require an
exact count.

The plain-inclusion sample proves the complementary rule: all five type-20 rows persist
`COD_COND_RESGATE='SEM TABELA'` and have zero `CONDICAO_RESGATE` rows. The input-level selector
is `RESGATE_ANTECIPADO_RF='Nao tem condicao de liquidez'` (`rdb_inclusao.log:110-111`), versus
`'Tem condicao especifica de liquidez'` in the com-resgate trace
(`rdb_com_resgate.log:128-129`).

### Events, valuation, and workflow

Each RDB writes exactly two maturity events, legacy types `83` and `85`, state `1`,
`IND_INCORPORA='N'` (`:1397-1399`, `:1570-1574`, `:1633-1669`, `:2473`, `:2486`). Each also
writes one `HISTORICO_PU_CURVA` with nominal and PU `1.00000000` (`:1392`, `:1456`, `:1550`,
`:1593`, `:2465`).

One `PENDENCIA_IF` type `1` is created per RDB while its schedule is pending (`:1418`, `:1602`,
`:1817`, `:2291`, `:2690`), then closed when the resgate conditions are persisted
(`:1573`, `:1900`, `:2137`, `:3016`, `:3155`). This table is workflow history rather than the
schedule itself, but omitting
it breaks exact entity reproduction.

### Accounts and certification

The trace establishes role-specific account rules:

- Issuer/title/P2 account uses `.40`; deposit/client/P1 uses `.10`.
- Both accounts are accepted only in situation `1` or `2` and require local access `L`, area
  `1` (`:1080-1083`, `:1731-1750`).
- The `.10` and `.40` accounts must belong to the same entity, and the `.10` account is checked
  for account type `96` (`:1608`, `:2164`).
- The participant's `.00` account must have CETIP certification status `2` or `3` for type
  `50` (`:860-892`, `:1766`).

This is enough to define an RDB-specific account check. It differs from the current strict CDB
check because RDB registration explicitly allows account situation `2` as well as `1`.

### Deposit and operation cluster

Each RDB writes one deposit operation cluster:

| Component | Observed rule | Evidence |
|---|---|---|
| `DEPOSITO_AUTOMATICO_IF` | one per IF; `.10` account, group modalidade `0`, PF CPF, PU `1` | `:1425`, `:1617`, `:1885`, `:2500`, `:2736` |
| `OPERACAO` | one per IF; TOS `5177`, modalidade `6`, debit sides `1/2`, P1 `.10`, P2 `.40`, final status `43` | `:2260`, `:2297`, `:2472`, `:3288`, `:3300`; transitions at `:3883-3884`, `:4081`, `:4100`, `:4199-4201` |
| `DADO_OPERACAO` | two per operation; types `688` (CPF) and `701` (natureza `PF`) | `:2282-2284`, `:2298-2299`, `:2475-2476`, `:3289-3290`, `:3500-3508` |
| `LANCAMENTO` | one P2 row per operation | `:2315`, `:2513`, `:2841`, `:3352`, `:3561` |
| `ESPECIFICACAO` | one P1 row, state `2` | `:3706`, `:3768`, `:3812`, `:3971`, `:4144` |
| `ESPECIFICACAO_COMITENTE` | one row, entry `4`, position `1`, movement `I`, updated `S` | `:3771`, `:3774`, `:3909`, `:3977`, `:4147` |
| `CARTEIRA_COMITENTE` | one row, system `55`, position `1`, deposit quantity | `:3778`, `:3782`, `:3960`, `:3987`, `:4150` |
| `CARTEIRA_PARTICIPANTE` | one row, system `55`, position `1`, deposit quantity | `:3855`, `:3865`, `:4030`, `:4055`, `:4185` |

For this registration operation, service `45`, operation code `1`, and
`IND_DISPONIVEL_IDENTIFICACAO='S'` are explicitly queried (`:780-784`). TOS `5177` is the
persisted TOS, linked to operation type ID `1233` (`:940-944`, `:2093`). This closes the
previous evidence gap **for the registration/deposit operation**. It does not prove that every
historical RDB operation must have operation code `1`; a whole-entity clone may legitimately
contain later operation types.

## Tables That Must Be Engordadas

### Shared core for both variants

Both traces write these **16 instrument-scoped tables**, with the same cardinalities:

| Layer | Tables | Observed rows per IF |
|---|---|---:|
| Root/subclass | `INSTRUMENTO_FINANCEIRO`, `TITULO`, `CREDITO` | 1 each |
| Conditions | `CONDICAO_IF`, `JUROS_FLUTUANTE`, `RESGATE` | 2, 1, 1 |
| Events/history | `EVENTO`, `HISTORICO_PU_CURVA` | 2, 1 |
| Deposit | `DEPOSITO_AUTOMATICO_IF` | 1 |
| Operation | `OPERACAO`, `DADO_OPERACAO`, `LANCAMENTO` | 1, 2, 1 |
| Identification | `ESPECIFICACAO`, `ESPECIFICACAO_COMITENTE` | 1, 1 |
| Positions | `CARTEIRA_COMITENTE`, `CARTEIRA_PARTICIPANTE` | 1, 1 |

Plain RDB inclusion stops at this 16-table graph.

### Complete RDB com-resgate entity

The successful five-instrument write set is:

| Layer | Tables | Observed rows per IF |
|---|---|---:|
| Resgate schedule | `CONDICAO_RESGATE` | 1-5 |
| Workflow | `PENDENCIA_IF` | 1 |

That is **18 dynamic tables**. Clone all rows reachable from each selected `NUM_IF`, preserving
the original per-IF fan-out. Do not independently resample any child table.

### Conditional comitente bootstrap

The inclusion trace creates these six additional table types, one row per previously absent
CPF: `ENTIDADE`, `COMITENTE`, `HIST_ATUALIZ_COMITENTE`, `CONTA_COMITENTE`, `RELACAO`, and
`HIST_COMITENTE` (`rdb_inclusao.log:1412-1519`, `:3629-3905`). The later com-resgate run reuses
the same five CPFs and creates none of them.

This is a destination-state difference, not a liquidity-variant rule. For a standalone target,
ensure this six-table closure once per unique comitente. If the target already contains the
comitente, do not clone it again. A fully standalone com-resgate load can therefore touch up to
24 table types: 16 shared + 2 liquidity-specific + 6 bootstrap.

### Current implementation gap

Fifteen of the 18 instrument/com-resgate tables exist in `specs.json`. These are missing:

1. `CONDICAO_RESGATE` - **blocking/business-critical** for RDB com resgate.
2. `HISTORICO_PU_CURVA` - required for complete valuation-history parity.
3. `PENDENCIA_IF` - required for complete workflow-history parity.

For standalone comitente bootstrap, `ENTIDADE`, `COMITENTE`, and `RELACAO` exist but are marked
static, while `HIST_ATUALIZ_COMITENTE`, `CONTA_COMITENTE`, and `HIST_COMITENTE` are absent.
That is compatible only when the destination already has every referenced comitente.

Because `datagen/engorda_tables.py` builds its clone plan from `specs.json`, absent tables are
not part of the entity closure. `tests/rdb.sql` also requires `RESGATE` but never requires or
classifies `CONDICAO_RESGATE` (`tests/rdb.sql:11-18`). The current query therefore mixes RDB
variants while failing to guarantee the schedule for `COM TABELA` instruments.

### Static destination prerequisites, not engorda targets

Do not multiply lookup/configuration rows. They must already exist and match the observed RDB
configuration: `TIPO_IF`, `FORMA_PAGAMENTO`, `OBJETO_SERVICO`/`V_OBJETOS_SERVICO`,
`TIPO_OPER_OBJETO_SERV`, `TIPO_OPERACAO`, `MODALIDADE_LIQUIDACAO`, `TIPO_DADO_OPERACAO`,
`CONTA_PARTICIPANTE`, `V_FAMILIA_CONTAS`, `CERTIFICACAO_CETIP`, `TIPO_OPER_PTA_CARTEIRA`,
`TIPO_OPERACAO_PONTA`, and `SITUACAO_CONTA`.

`COMITENTE`/`ENTIDADE` references also need to resolve. The updated inclusion trace proves the
application can create them, but they remain static in the current specs. Cloning
`ESPECIFICACAO_COMITENTE` and `CARTEIRA_COMITENTE` therefore assumes those entity IDs exist in
the target or are handled by the existing faltantes workflow.

## Recommended Product Split

1. Define `rdb_inclusao` from active type-50 roots, active type `20`/`RESGATE` rows,
   `RESGATE.COD_COND_RESGATE='SEM TABELA'`, and no **active** `CONDICAO_RESGATE` rows.
2. Define `rdb_com_resgate` from the same active root/condition predicates,
   `RESGATE.COD_COND_RESGATE='COM TABELA'`, and at least one **active**
   `CONDICAO_RESGATE` row. Resolve activity through `IND_EXCLUIDO`; deleted historical rows
   must not classify a product variant.
3. Add `CONDICAO_RESGATE`, `HISTORICO_PU_CURVA`, and `PENDENCIA_IF` to the extracted schema and
   `specs.json`, with their real PK/FK/NOT NULL metadata.
4. Decide explicitly whether comitentes are destination prerequisites or part of a standalone
   publication; the current static treatment cannot support both behaviors implicitly.
5. Validate registration-operation TOS `5177` separately from unrestricted historical RDB
   operations; otherwise the evidence-backed code-`1` rule becomes an overbroad false failure.

`check_rdb_resgate_schedule_rules` now enforces COM/SEM schedule ownership and valid values as
errors. Duplicate dates, out-of-bounds dates, and non-increasing percentages are advisory WARNs
until broader production evidence promotes them.

## Countercheck and Limits

The strongest alternative interpretation is that only final business-state tables need to be
cloned, allowing `PENDENCIA_IF` and `HISTORICO_PU_CURVA` to be omitted. That may produce a
currently usable instrument, but it is not an exact entity clone and can change application
queries over pending state and valuation history. Given this project's stated whole-entity
cloning contract, preserving both is the safer conclusion.

No shape rule should be generalized beyond this route from five homogeneous examples. In
particular, exact condition/event/operation counts may differ for other payment forms, rate
types, or later lifecycle operations.
