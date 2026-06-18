# FK-Topological Load Order (Design)

Date: 2026-06-18
Status: approved / implementing
Related: `load_tables.py`, `engorda_tables.py` (`topo_order_tables`),
`validate_tables.py`, `specs.json`

## Problem

`load_tables.py` loads tables **sequentially** in the order they are resolved
(`resolve_load_tables` → load loop). That order is:
- the order typed in `--tables`, or
- `specs.json` key order (for the all-non-static path).

Neither is FK-aware. Synthetic rows carry FK values that point at **synthetic
parent rows** (engorda mints keys as `source_max + id`, above the real max), so
a parent's synthetic rows must be committed before its children's. If a child
loads first (e.g. `--tables JUROS_FLUTUANTE,CONDICAO_IF`, or an unfavourable
`specs.json` order), the append fails with ORA-02291 (parent key not found).

`validate_tables.py` confirms the synthetic data is internally consistent
(every FK resolves within raw ∪ synthetic) but does **not** model load order —
so a green validation can still be followed by an ordering-induced ORA-02291.
This change closes that one FK failure mode the validator structurally can't
catch.

## Solution

Sort the resolved load list parents-before-children, always, silently, for both
the `--tables` and the all-non-static paths.

Reuse engorda's proven, self-contained `topo_order_tables(specs)` (and its small
`_fk_list` helper); copy them into `load_tables.py` (each Data Flow app is a
single uploaded file — no cross-import). `topo_order_tables` orders parents
before children, ignores self-references, and breaks cycles arbitrarily so every
table is returned exactly once.

New thin wrapper + one changed call site:

```python
def topo_sort_for_load(specs: dict, tables: list[str]) -> list[str]:
    order = topo_order_tables(specs)            # all specs keys, parents first
    rank = {name: i for i, name in enumerate(order)}
    # in-specs tables sort by topo rank; tables absent from specs (no FK
    # metadata) get a max rank -> land at the end, original order preserved.
    return sorted(tables,
                  key=lambda t: rank.get(table_path_name(t).upper(), len(order)))
```

`resolve_load_tables` returns `topo_sort_for_load(specs, result)`.

Properties:
- Python's sort is **stable** → siblings keep their requested/specs order; only
  FK-dependent pairs get reordered.
- Ranks are global over all specs, so a parent in the load set always precedes
  its child even when intermediate tables aren't in the load set.
- Name-qualifier safe: `CETIP.X` → `X` via `table_path_name(...).upper()`,
  matching how `is_static` / `pk_cols_for` already normalize.

## Scope limit (documented, not coded)

This only helps when related tables are in the **same** load job. The README's
recommended pattern is one Data Flow job per big table — across separate jobs,
ordering is manual and the sort can't reach it. (Launch the parent's job, let it
finish, then the child's.)

## Testing (pure, runs locally with `.venv/bin/python -m pytest`)

- parent ordered before child;
- a child-first `--tables` list is reordered parent-first;
- a table absent from specs lands last, preserving relative order;
- a self-referencing FK does not break ordering;
- a 2-table cycle returns both tables exactly once.

## Out of scope

- Cross-job ordering / orchestration.
- Changing the per-table parallel JDBC append.
