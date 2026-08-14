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
