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

**LCI backing set**:
The active Credito SCR masters linked to an LCI through their shared lot, together with the Historico Credito SCR rows linked to those masters. Engorda selects only complete master/history pairs, and under the Osias validation profile every cloned master has history evidence in the synthetic output.
_Avoid_: Same-NUM_IF credit, placeholder Credito

**LCA**:
The Letra de Credito do Agronegocio product rooted at an Instrumento Financeiro. Its populated Credito, guarantee, and representative rows belong to the LCA aggregate, not to Credito SCR or Credito DC.
_Avoid_: Credito SCR, Credito DC, DICRE

**LCA backing set**:
The active Credito DC masters linked to an LCA through their shared lot, together with the Historico Credito DC rows linked to those masters by credit code. These backing rows remain distinct from the LCA aggregate's own populated Credito row.
_Avoid_: Same-NUM_IF credit, LCA Credito row

**Operational control date**:
The business date governing synthetic financial and operation dates. `DAT_FINANCEIRO` and `DAT_OPERACAO` use this date alongside the other operational date fields; audit timestamps remain tied to execution time.
_Avoid_: Run date, audit timestamp

**Meu-number allocation group**:
Operation sides sharing an operational date, normalized participant account, and normalized operation-and-service classification. Control-number ordinals may repeat across different groups, while the complete date/account/control/classification tuple remains unique. Concurrent runs reserve non-overlapping intervals whenever their groups overlap.
_Avoid_: Globally unique control number, operation-only sequence

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
An RDB registering a redemption schedule, with one type-20 RESGATE in `COM TABELA` mode, at least one active CONDICAO_RESGATE schedule row, and no quantity yet redeemed on TITULO. Under the Osias validation profile, every operation uses operation-and-service classification 5177.
_Avoid_: RDB inclusion, redemption row

**CDB escalonamento**:
A CDB with an issuance escalation schedule and no quantity redeemed on TITULO. Under the Osias validation profile, every operation uses operation-and-service classification 4509.
_Avoid_: CDB resgate, escalonamento with redeemed quantity

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

**Osias validation profile**:
An opt-in scenario acceptance profile in which every cloned operation must use an explicitly approved operation-and-service classification for that scenario. Unlike general product validation, an otherwise valid historical or secondary operation outside the scenario allowlist is an error.
_Avoid_: Registration operation check, universal product rule

**CDB operation closure**:
The mandatory per-operation CDB structure rooted at one operation: exactly two operation-data records and one launch. A CDB may have multiple operations, but each operation must independently satisfy this 1:2:1 closure.
_Avoid_: Per-instrument operation ratio, aggregate 1:2:1 ratio

**Best-effort observational text diversity**:
Variation in synthetic textual values intended to make cloned records less visibly repetitive rather than exercise specific business behavior. Operators accept that populating previously absent text can still affect NoMe behavior.
_Avoid_: Guaranteed behavior-preserving enrichment, functional scenario generation, anonymization

**Instrument text enrichment**:
The optional generation of allowlisted observational text values using one selected source financial-instrument aggregate as semantic context.
_Avoid_: Independent cell generation, business-rule generation
