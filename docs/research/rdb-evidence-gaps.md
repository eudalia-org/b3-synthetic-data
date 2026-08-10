# RDB Evidence Gaps — Blocking Items Before Strict Semantic Validation

Status: **RDB is validated in structural-only mode.** The `rdb` `ValidationProfile` in
`scripts/validate_cdb_simplificado.py` marks the capabilities below as **required but
unsupported**, which forces every RDB run to report **PARTIAL** (non-zero exit) until each
item is closed with primary-source target evidence. Do **not** fill these with CDB defaults.

Proven RDB values already encoded (do not re-derive):

| value | evidence |
|---|---|
| `NUM_TIPO_IF = 50` | `framework/dados/.../instrumentofinanceiro/TipoIFDO.java:56` (`RDB = new Id("50")`) |
| object service `45` | `framework/dados/.../sca/ObjetoServicoDO.java:80` (`TIPO_IF_RDB = new Id("45")`) |
| COD_IF allocator | `CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(50, <date>)` (`datagen/engorda_tables.py:2235`, `:432`) |

## Open items (each blocks one capability)

1. **`platform`** — object-service platform code/flag for RDB.
   ```sql
   SELECT COD_OBJETO_SERVICO, IND_PLATAFORMA_BAIXA
   FROM CETIP.V_OBJETOS_SERVICO
   WHERE NUM_ID_OBJETO_SERVICO = 45;
   ```
   Fill `rdb.object_service_code` and set `platform_check_enabled=True` only after the string
   (e.g. `'RDB'`?) and the flag are confirmed.

2. **`modalidade` (`sem_modalidade_ids`)** — confirm whether IDs 6/16 apply to tipo 50.
   Trace `IND_SEM_MODALIDADE_INFOHUB` usage for RDB operations, or SME decision. Set
   `sem_modalidade_ids` only when confirmed.

3. **`account`** — confirm the account-eligibility rule for RDB (situação, `COD_TIPO_ACESSO`,
   `NUM_ID_AREA_ATUACAO`, and the `.40/.10` code shape) is identical to CDB or capture the
   RDB-specific rule. Set `account_check_enabled=True` only then.

4. **`cod_if_format`** — capture a real RDB registration (p6spy/`cetip.out`-style) and read
   `INSTRUMENTO_FINANCEIRO.COD_IF`. Only then set `rdb.cod_if_pattern`. The generator's
   assumed `^RDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$` (`engorda_tables.py:433`) is **not** promoted.

5. **SIC compatibility** — confirm `V_PARAMETRO_SIC` exposes `(NUM_TIPO_IF=50,
   NUM_ID_OBJETO_SERVICO=45)`.
   ```sql
   SELECT DISTINCT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_TIPO_IF, NUM_ID_OBJETO_SERVICO
   FROM CETIP.V_PARAMETRO_SIC WHERE NUM_TIPO_IF = 50;
   ```
   Set `sic_enabled=True` only after confirmation.

6. **`shape`** — produce and review an exact-source-key type-50 baseline:
   ```bash
   spark-submit profile_cdb_shapes.py --product rdb --apply-filtros-fonte \
     --universe-keys <MAPA_CLONE_NUM_IF> --universe-keys-column NUM_IF_ORIG \
     --report-path <oci>/profile_rdb.json
   ```
   Then decide which `hard_shape_rules` (if any) generalize to RDB.

7. **`registration_profile`** — full CDB and RDB deliberately leave
   `registration_constants=None`. If a persisted-profile check is wanted for RDB, re-derive
   the constants from a real RDB registration; do not reuse the simplificado (`cetip.out`)
   values.

## CONDICAO_IF subtype completeness (all products)

`EXPECTED_CONDICAO_TYPE_CODES` (from `TipoCondicaoIFDO.java:42-73`) lists every code. The
following have **no confirmed physical joined-subclass table** in `CondicaoIFDO.hbm.xml` yet
and are therefore reported as `1b.unknown_tipo` (WARN) rather than silently accepted; confirm
each against the mapping before full-product/RDB strict rollout:

`8 TRIGGER_IN, 18 TRIGGER_OUT, 19 TERMO_MOEDA, 25 TERMO_COMMODITY, 26 PAGTO_MONETARIO,
27 TERMO_INDICE, 28 CORRECAO, 29 TERMO_FLUXO, 30 TRIGGER_CCP`.