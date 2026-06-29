# Null Self-Referential FK Columns on Load — Design

**Date:** 2026-06-29
**Status:** Approved (design)
**Component:** `datagen/load_tables.py` — null self-ref FK columns before insert

## Problem

`load_tables.py` appends the 15 synthetic (non-static) tables into the populated
production CETIP Oracle schema. One of them, `INSTRUMENTO_FINANCEIRO`, has
**self-referential FK columns** (`NUM_IF_ORIGEM`, `NUM_IF_PERTENCE` →
`INSTRUMENTO_FINANCEIRO.NUM_IF`). During a single parallel `append`, a row can
reference another synthetic row in the same table that hasn't been inserted yet,
which Oracle rejects with `ORA-02291` (parent key not found). The values are also
stale: the self-ref FK was dropped from `specs.json` (so it was never PK-shifted),
while `NUM_IF` *was* shifted — so the columns point at neither the new synthetic
keys nor a guaranteed-present production row.

## Decision

Insert these self-referential columns as **NULL and leave them** (they are
nullable). No post-insert UPDATE / back-fill. This is the loss-tolerant choice the
data owner approved — the self-ref linkage is dropped in the loaded synthetic data.

## Scope (what this is NOT)

Confirmed during brainstorming, so the change stays small:

- **Still load only the 15 non-static tables.** The 32 static tables (reference/
  code data *and* `ENTIDADE`/`USUARIO`/`PARTICIPANTE`/`CONTA_PARTICIPANTE`/`MALOTE`)
  already exist in the populated target; re-inserting their 1:1 copies would
  collide (`ORA-00001`). The full 47-table insertion order treats static tables as
  prerequisites that must already exist — not as tables to insert.
- **No load-order change.** The 15 form a clean DAG; existing `topo_sort_for_load`
  already orders them correctly.
- **No cycle two-phase handling for other tables.** The `ENTIDADE↔USUARIO` and
  `PARTICIPANTE↔CONTA_PARTICIPANTE↔MALOTE` cycles are entirely among static tables,
  which are not loaded.
- **`INSTRUMENTO_FINANCEIRO` is the only self-referential table among the 15.**

## Design

A module constant maps a table to the columns to NULL on insert:

```python
NULL_ON_INSERT = {
    "INSTRUMENTO_FINANCEIRO": ["NUM_IF_ORIGEM", "NUM_IF_PERTENCE"],
}
```

Keyed by the bare uppercase table name. Easy to extend if another self-referential
table appears, but only `INSTRUMENTO_FINANCEIRO` is needed today.

A helper nulls the listed columns present in the DataFrame, preserving dtype:

```python
def null_self_ref_columns(df, table, null_map):
    cols = null_map.get(table_path_name(table).upper(), [])
    actual = {c.upper(): c for c in df.columns}
    for c in cols:
        real = actual.get(c.upper())
        if real is not None:
            df = df.withColumn(real, F.lit(None).cast(df.schema[real].dataType))
    return df
```

Applied in `load_table` **after** `apply_pk_guard` and **before** `df.write`. It is
a no-op for the other 14 tables and for any listed column not present in `df`. The
`cast(dtype)` keeps the insert schema byte-identical.

## Error handling / interactions

- **Validation pre-flight:** unaffected. Nulling only relaxes constraints (the
  columns are nullable), so the pre-flight — which profiles the original values —
  remains a safe superset check. No coupling needed.
- **Logging:** when columns are nulled for a table, log which ones, so the run
  record shows the self-ref linkage was dropped.

## Testing

Local Spark (JDK-17 path). Unit-test `null_self_ref_columns`:

- listed columns become NULL; other columns are untouched;
- dtype is preserved (schema unchanged);
- a table not in the map is returned unchanged;
- a listed column absent from the DataFrame is skipped (no error);
- case-insensitive column matching.

## Out of scope / follow-ups

- Post-insert UPDATE to restore `NUM_IF_ORIGEM`/`NUM_IF_PERTENCE` (explicitly
  declined).
- Loading static tables / empty-target full load.
- Driving `NULL_ON_INSERT` from `specs_full.json` instead of a constant (only one
  table needs it today).
