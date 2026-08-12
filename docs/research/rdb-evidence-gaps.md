# RDB Evidence Gaps — Blocking Items Before Strict Semantic Validation

Status: **RDB is validated in structural-only mode.** The `rdb` `ValidationProfile` in
`scripts/validate_products.py` marks the capabilities below as **required but
unsupported**, which forces every RDB run to report **PARTIAL** (non-zero exit) until each
item is closed with primary-source target evidence. Do **not** fill these with CDB defaults.

Update from the August 2026 traces: `rdb_com_resgate.log` now supplies route-specific evidence
for platform `RDB`/`S`, account roles and status `1|2`, default modalidade group `0`, operation
modalidade `6`, registration TOS `5177`, service `45`, operation code `1`, identification `S`,
and the `COM TABELA` resgate schedule. See
[`rdb-log-derived-rules-and-engorda-scope.md`](rdb-log-derived-rules-and-engorda-scope.md).
These broad capabilities remain marked unsupported until the validator scopes those rules to
the observed registration route rather than applying them to every historical RDB operation.
The replacement `rdb_inclusao.log` is a valid plain-RDB trace. It confirms the same platform,
account, modalidade, TOS, and registration core, with `RESGATE='SEM TABELA'` and no
`CONDICAO_RESGATE`/`PENDENCIA_IF` rows.

Application-source values already encoded:

| value | evidence |
|---|---|
| `NUM_TIPO_IF = 50` | `framework/dados/.../instrumentofinanceiro/TipoIFDO.java:56` (`RDB = new Id("50")`) |
| object service `45` | `framework/dados/.../sca/ObjetoServicoDO.java:80` (`TIPO_IF_RDB = new Id("45")`) |
| generic COD_IF normalization | uppercase letters, digits, spaces, or hyphen; maximum 14 characters (`atributos/.../identificador/CodigoIF.java:17-30,68-74`) |

The generator calls `CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(50, <date>)`, but no RDB
application registration path or package implementation was found. That call is not evidence
for an RDB prefix or complete allocator format.

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

4. **`cod_if_format`** — generic `CodigoIF` normalization is enforced, but capture a real RDB
   registration and read `INSTRUMENTO_FINANCEIRO.COD_IF` before defining an RDB prefix or
   allocator rule. The generator's assumed `^RDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$` is **not**
   promoted, so this broad capability remains unsupported.

5. **`lookup_tos` / SIC compatibility** — confirm `V_PARAMETRO_SIC` exposes
   `(NUM_TIPO_IF=50, NUM_ID_OBJETO_SERVICO=45)` and capture the RDB
   `TIPO_OPER_OBJETO_SERV` rows. Application source does not confirm CDB's
   `IND_DISPONIVEL_IDENTIFICACAO='S'` or operation type code `1` for RDB.
   ```sql
   SELECT DISTINCT NUM_ID_TIPO_OPER_OBJETO_SERV, NUM_TIPO_IF, NUM_ID_OBJETO_SERVICO
   FROM CETIP.V_PARAMETRO_SIC WHERE NUM_TIPO_IF = 50;
   ```
   Set `sic_enabled=True` and support `lookup_tos` only after confirmation.

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

8. **`polymorphism` RDB allow-list** — generic shared-key consistency remains checked, but
   application source does not establish which condition codes RDB registration permits.
   Keep the broad capability unsupported until RDB registrations and physical subtype rows
   are observed.

## CONDICAO_IF subtype completeness (all products)

`EXPECTED_CONDICAO_TYPE_CODES` (from `TipoCondicaoIFDO.java:42-73`) lists every code. The
following have **no confirmed physical joined-subclass table** in `CondicaoIFDO.hbm.xml` yet
and are therefore reported as `1b.unknown_tipo` (WARN) rather than silently accepted; confirm
each against the mapping before full-product/RDB strict rollout:

`8 TRIGGER_IN, 18 TRIGGER_OUT, 19 TERMO_MOEDA, 25 TERMO_COMMODITY, 26 PAGTO_MONETARIO,
27 TERMO_INDICE, 28 CORRECAO, 29 TERMO_FLUXO, 30 TRIGGER_CCP`.
