# Synthetic Financial Product Validation

This context names the financial-product aggregates whose synthetic data is generated and validated against NoMe/CETIP behavior.

## Language

**DICRE**:
The Direito Creditorio product handled by the `DICREINCL` registration route.
_Avoid_: Credito SCR, Lastro SCR

**Credito DC**:
The master credit entity persisted for DICRE. It is distinct from a Credito SCR even when both represent backing credit data.
_Avoid_: Credito SCR, Instrumento Financeiro

**Credito SCR**:
The credit-information entity persisted by SCR/Lastro routes. It is a separate aggregate from Credito DC.
_Avoid_: Credito DC, DICRE

**LCI**:
The Letra de Credito Imobiliario product rooted at an Instrumento Financeiro. Its placeholder Credito row and Lastro prerequisites do not make it a Credito SCR or Credito DC.
_Avoid_: Lastro LCI, Credito SCR, DICRE

**LCA**:
The Letra de Credito do Agronegocio product rooted at an Instrumento Financeiro. Its populated Credito, guarantee, and representative rows belong to the LCA aggregate, not to Credito SCR or Credito DC.
_Avoid_: Credito SCR, Credito DC, DICRE

**DICRE IROP closure**:
The conditional family of IROP records linked to a Credito DC. Presence varies by DICRE subtype, but present records remain part of the aggregate.
_Avoid_: Mandatory CCB closure, mandatory CMER closure

**Product validation**:
The evidence report that a synthetic product aggregate satisfied its product contract against a specific input URI. An accepted report is PASS or PARTIAL with no ERROR findings.
_Avoid_: Load preflight, schema check

**Oracle load**:
One explicitly approved APPEND attempt that writes a validated synthetic product aggregate to the target database.
_Avoid_: Import, merge, synchronization

**Load attempt manifest**:
The immutable recovery record created before an Oracle load starts. It identifies the validated input, ordered tables, write transformations, and each synthetic numeric primary-key range.
_Avoid_: Pipeline manifest, validation report

**Load claim**:
The durable assertion that a synthetic product aggregate has already had a load attempt. A later attempt is a resume linked to the preceding load attempt manifest.
_Avoid_: Reservation, environment lease

**Offline synthetic artifact**:
A synthetic product aggregate generated without Oracle admission, PK-floor checks, or official business-key allocation. It may be inspected or validated but is never eligible for Oracle load.
_Avoid_: Dry run, loadable synthetic artifact

**RDB inclusion**:
A newly registered RDB with one type-20 RESGATE in `SEM TABELA` mode, no active CONDICAO_RESGATE schedule, and zero redeemed quantity on TITULO.
_Avoid_: RDB without RESGATE, RDB resgate

**RDB resgate**:
An RDB with one type-20 RESGATE in `COM TABELA` mode and at least one active CONDICAO_RESGATE schedule row.
_Avoid_: RDB inclusion, redemption row

**Registration account roles**:
The product-specific account-code groups assigned to the party and counterparty of a registration operation. CDB, RDB, LCI, and LCA use `.10`/`.40`; CCB uses `.00`/`.40`.
_Avoid_: Universal operation account regex, participant P1/P2 IDs

**Operation-party nature**:
The mutually exclusive PF or PJ classification associated with an operation party. A valid party is one nature or the other, never a required PF-and-PJ pair.
_Avoid_: PF/PJ pair

**Redemption schedule entry**:
One dated entry below a redemption condition. References to three or four resgates in manual validation mean multiple schedule entries under one redemption condition, not multiple independent redemptions.
_Avoid_: Independent resgate, redemption parent

**Registration operation**:
The operation that registers the financial instrument, identified by its operation and service classification. Additional operations are assessed by their own classification and are not invalid merely because more than one operation exists for the instrument.
_Avoid_: Only operation, extra operation error

**CDB operation closure**:
The mandatory per-operation CDB structure rooted at one operation: exactly two operation-data records and one launch. A CDB may have multiple operations, but each operation must independently satisfy this 1:2:1 closure.
_Avoid_: Per-instrument operation ratio, aggregate 1:2:1 ratio
