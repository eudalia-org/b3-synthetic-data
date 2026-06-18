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

A single self-contained `topo_sort_for_load(specs, tables)` (plus the tiny
`_fk_list` helper) in `load_tables.py` (each Data Flow app is a single uploaded
file — no cross-import). `resolve_load_tables` returns
`topo_sort_for_load(specs, result)`.

It is a **stable** topological sort: input order is preserved except where a
foreign key forces a parent ahead of its child. Algorithm — repeatedly scan the
remaining list in order and emit the first table whose in-load-set parents are
already emitted; if none qualifies (a cycle), emit the rest in input order:

```python
def topo_sort_for_load(specs, tables):
    norm = {t: table_path_name(t).upper() for t in tables}
    present = set(norm.values())
    parents = {}
    for t in tables:
        deps = set()
        for fk in _fk_list(specs.get(norm[t], {})):
            parent = (fk.get("parent_table") or "").upper()
            if parent and parent != norm[t] and parent in present:
                deps.add(parent)
        parents[t] = deps
    result, emitted, remaining = [], set(), list(tables)
    while remaining:
        for i, t in enumerate(remaining):
            if parents[t] <= emitted:
                result.append(t); emitted.add(norm[t]); remaining.pop(i); break
        else:
            result.extend(remaining); break
    return result
```

Properties:
- **Only FK-dependent pairs get reordered.** Independent tables — including ones
  absent from `specs` (no FK metadata) — keep their original relative position.
  This is why the existing `test_requested_drops_static_keeps_order` still holds.
  (An earlier rank-based draft reused engorda's `topo_order_tables`, but that
  `sorted(ready)` alphabetizes independent tables — an unwanted silent reshuffle
  — so it was dropped in favour of this stable sort.)
- **Only parents that are themselves in the load set** impose a constraint, so an
  FK to a table that isn't being loaded (e.g. a static/code parent already in the
  DB) adds no edge.
- Self-references are ignored; a cycle is broken by emitting the rest in input
  order, so every input table is returned exactly once.
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
- schema-qualified names (`CETIP.X`) resolve correctly;
- an independent / absent-from-specs table keeps its input position;
- a self-referencing FK does not break ordering;
- a 2-table cycle returns both tables exactly once.

## Out of scope

- Cross-job ordering / orchestration.
- Changing the per-table parallel JDBC append.
