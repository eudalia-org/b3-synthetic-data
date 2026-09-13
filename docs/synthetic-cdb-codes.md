# Synthetic CDB COD_IF allocation

Enable `engorda.cod_if_allocator=synthetic_cdb` for CDB test data when the official
`CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(49, date)` allocator is unavailable. This mode
uses live Oracle admission, PK floors, and collision checks; it is eligible for
normal product validation and the existing explicitly requested Oracle load stage.
It does not enable `no_oracle` or substitute offline placeholders.

## Evidence and format

| Evidence | Observed codes |
| --- | --- |
| `cdb_simplificado_insert.log:68` | `CDB5268TVJJ` |
| `more_cdb_simplificado_insertions.log` | `CDB62600CNP` through `CDB62600CNT` |
| `cdb_com_escalonamento.log:1347-1371` | `CDB72602Q6X`, `02Q6Y`, `02Q6Z`, `02Q70`, `02Q71` |
| `cdb_com_resgate.log:1328-1333` | `CDB72602Q72` through `CDB72602Q76` |

The observed rollover `02Q6Z -> 02Q70` supports a five-character base-36 suffix,
confirmed by the subsequently supplied `PKG_CODIGO` package body.
The generator uses `CDB` + month (`1..9`, `A`, `B`, `C`) + two-digit year + suffix
(`00000..ZZZZZ`), matching the existing CDB validation pattern. The package uses
the **month of `CETIP.GET_DATAHOJE`** and the **year of its `V_DATA` argument**.
Planning reads that month and combines it with the operational control date's
year, then freezes the resulting prefix. Materialization never rereads the clock.
For operational date `2026-06-03`, the prefix is `CDB626` if GET_DATAHOJE is in June,
or `CDB926` if GET_DATAHOJE is in September. This distinction was not visible in
the insertion-log samples alone.

`A0000` is the chosen synthetic starting suffix: decimal 16,796,160, above the
suffixes in these logs. It is not an officially reserved CETIP range. Planning
reads the binary maximum valid code in the current prefix, including excluded
instruments, and requests `max(A0000, live_max_suffix + 1)`. Reservations can move
that start higher to avoid previous synthetic allocations. The five-character
limit is enforced before reservation and generation; exhaustion fails rather than
wrapping, truncating, or silently switching months.

## Enable and rerun

1. Deploy the updated `datagen/engorda_tables.py` to the configured Data Flow
   application and update the workstation's standalone `scripts/run_pipeline.py`.
   Both files are self-contained; no additional deployment module is needed.
2. Start a **new engorda run** against the existing adopted RAW/faltantes inputs.
   Add these overrides to the usual `run --from engorda --to validate` command:

   ```text
   --set cdb_simplificado.engorda.cod_if_allocator=synthetic_cdb
   --set cdb_resgate.engorda.cod_if_allocator=synthetic_cdb
   ```

   Alternatively, put `"cod_if_allocator": "synthetic_cdb"` inside each product's
   `"engorda"` configuration. The override also supports `cdb_escalonamento`.
3. Inspect the new plan's `cod_if` descriptor and the reservation's `cod_if` range.
   If Oracle has no higher code and the ledger has no preceding reservation, the
   first generated code for the June/2026 prefix is `CDB626A0000`. Other products/runs
   receive subsequent non-overlapping ranges for that prefix.
4. Validate the new outputs using the normal product checks, then verify a small
   sample in NoMe before a large load. Log-derived format compatibility has been
   tested locally; acceptance by the live application has not been verified here.

Direct engorda CLI planning accepts `--cod-if-allocator synthetic_cdb` and requires
the existing `plan -> reserve -> materialize` flow with live Oracle access.
Materialization inherits the allocator from its hashed plan; an explicitly
different CLI allocator is rejected. Existing official-allocator plans/reservations
remain readable, but cannot be switched in place: their hash, requested range,
and reservation must be regenerated. Runner `--dry-run` can preview the new DAG;
engorda's own offline/dry generation mode cannot establish a live synthetic range.

## Reservation and collision guarantees

The shared environment ledger's existing `reservations[].reservation.cod_if`
history is authoritative for the synthetic suffix high-water mark. Preserve it,
including failed-publication records. Concurrent pipeline jobs using the same
ledger are serialized by the lease and protected by conditional ETag updates.
All CDB scenarios use the same prefix space; changing the product does not reset
the suffix. Reusing an immutable reservation reproduces the same code mapping.

Before writing the code map, a read-only Oracle query checks the entire reserved
code interval, including excluded instruments. A collision aborts publication
and requires a new plan/reservation. Generated codes are attached to
`INSTRUMENTO_FINANCEIRO` and propagated to related `OPERACAO` rows through the
existing mapping path. Uniqueness, cardinality, and format checks remain enabled.

Native CETIP writers do not consult this ledger. The starting gap reduces near-term
overlap, but it cannot guarantee exclusivity against future official allocations
or writes after preflight. `COD_OPERACAO` still uses `CETIP.GET_COD_OPERACAO`.

## Package-source diagnosis

The supplied package body contains this sequence in `F_GETCODIF21`:

```sql
V_LTR varchar2(5);
-- ...
V_SQC := F_PEGASEQUENCENOVO(V_TTTTYYYY);
V_LTR := F_CONVERTEBASE(V_SQC, 36);
V_RTRN := V_TTTT || V_YY || LPAD(V_LTR, (V_TAMTOTAL - 2 - V_TAMTTTT), '0');
-- exception when others then return sqlerrm;
```

`36**5 = 60,466,176` converts to `100000`, which cannot fit in `V_LTR`. This is a
concrete candidate for the observed string-buffer overflow, not yet a confirmed
live sequence value. Widening V_LTR alone would not be a correct fix: the following
five-character LPAD would truncate a longer code. The error handler returns
SQLERRM as text, explaining why the outer caller sees a successful call with a
91-character error string instead of an exception. The numeric-returning
`F_PEGASEQUENCENOVO` also incorrectly returns SQLERRM on error, so underlying
sequence access/creation failures can be masked by secondary conversion errors.

An operator can inspect visible sequence metadata without consuming a NEXTVAL:

```sql
SELECT sequence_name, last_number, cache_size
FROM all_sequences
WHERE sequence_owner = 'CETIP'
  AND sequence_name LIKE 'S_21_NUM_CODIGONOVOCDB%2026'
ORDER BY sequence_name;
```

The package names the sequence `S_21_NUM_CODIGONOVO` + CDB/month prefix + four-digit
argument year. `LAST_NUMBER` is cache-related metadata, not necessarily the last
issued value. Synthetic planning deliberately uses the maximum *stored code*,
not a possibly exhausted sequence or values burned by failed prior runs.
