"""Generate specs.json from an Oracle constraint dump.

Input: the CSV produced by scripts/extract_constraints.sql (one row per
constraint column, types P/U/R). Output: specs.json in the engorda format:

    {
      "TABLE": {
        "pk_cols": ["COL", ...],
        "foreign_keys": [
          {"columns": ["FKCOL"], "parent_table": "PARENT", "parent_columns": ["PKCOL"]}
        ],
        "static": true            # optional, for reference/code tables
      }
    }

Usage:
    python scripts/build_specs_from_constraints.py --constraints constraints.csv --out specs.json
    # preserve static / n_rows you've already curated in an existing specs.json:
    python scripts/build_specs_from_constraints.py --constraints constraints.csv \
        --out specs.json --current specs.json
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict

# Reference / code / domain tables that must NOT be fattened (constrained PK
# domains, copied 1:1). Edit this set if the schema changes. Used when no
# --current specs.json is given to carry static flags over.
# Only the 15 "ESCRITA" (write/transaction) tables are fattened; everything
# else is copied 1:1 (static). Fattened: CARTEIRA_COMITENTE, CARTEIRA_PARTICIPANTE,
# ESPECIFICACAO, ESPECIFICACAO_COMITENTE, INSTRUMENTO_FINANCEIRO, LANCAMENTO,
# OPERACAO, TITULO, EVENTO, CONDICAO_IF, CREDITO, DADO_OPERACAO,
# DEPOSITO_AUTOMATICO_IF, JUROS_FLUTUANTE, RESGATE.
STATIC_TABLES = {
    "TIPO_DEBITO", "TIPO_POSICAO_CARTEIRA", "TIPO_IF", "TIPO_OPER_OBJETO_SERV",
    "TIPO_OPER_PTA_CARTEIRA", "TIPO_DADO_OPERACAO", "NAT_ECO_TIPO_IF",
    "NAT_ECON_TP_OPER_PONTA", "NATUREZA_ECONOMICA", "MODALIDADE_LIQUIDACAO",
    "MOTIVO_SITUACAO_IF", "SITUACAO_CONTA", "FORMA_PAGAMENTO", "PAPEL_PARTICIPANTE",
    "OBJETO_SERVICO", "OPCAO_RECOMPRA", "CERTIFICACAO_CETIP",
    "PARAMETRIZACAO_REGIME_MERCADO", "PARAMETRIZACAO_TIPO_REGIME", "PARAMETRO_CONFIG",
    "TCTPFEATURE_TOGGLE", "TCTPHABILITA_OPERACAO_SERVICO", "MALOTE",
    # Structural / entity / reference tables — copied 1:1, not transactions.
    "BLOQUEIO_OPERACAO_IF", "COMITENTE_FATCA", "COMITENTE_INR", "CONTA_PARTICIPANTE",
    "ENTIDADE", "PARTICIPANTE", "RELACAO", "TCTPCONTROLE_IF_DEPR", "USUARIO",
}

# Audit "last updated by" columns (NUM_ID_ENTIDADE_ATUALIZ -> USUARIO/ENTIDADE).
# These are metadata pointers, not structural parent-child relationships, and
# they form FK cycles (e.g. ENTIDADE <-> USUARIO). We exclude them from the FK
# graph; the synthesizer leaves the column's raw values in place.
AUDIT_FK_COL_SUFFIX = "_ATUALIZ"

# Structural back-reference FKs that close a genuine cycle. Each entry is the
# NON-owning side of a mutual reference; we keep the ownership edge and drop
# this one so the graph is a DAG. Keyed by (child_table, (columns...)).
#   - account belongs to participant -> keep CONTA_PARTICIPANTE.NUM_ID_ENTIDADE
#     -> PARTICIPANTE; drop the participant's pointer back to its account.
#   - malote belongs to account -> keep MALOTE.NUM_CONTA_PARTICIPANTE ->
#     CONTA_PARTICIPANTE; drop the account's pointer back to its malote.
BACK_REFERENCE_FKS = {
    ("PARTICIPANTE", ("NUM_CONTA_PARTICIPANTE",)),
    ("CONTA_PARTICIPANTE", ("NUM_ID_MALOTE",)),
}


def _is_audit_fk(fk: dict) -> bool:
    cols = fk.get("columns") or []
    return bool(cols) and all(c.endswith(AUDIT_FK_COL_SUFFIX) for c in cols)


def prune_cycle_fks(specs: dict) -> tuple[list[str], list[str]]:
    """Drop audit (*_ATUALIZ) and structural back-reference FKs in place.

    Both classes of FK would otherwise create cycles that the engorda
    topological order cannot satisfy. Returns (audit_dropped, cycle_dropped)
    as human-readable descriptions for reporting.
    """
    audit_dropped: list[str] = []
    cycle_dropped: list[str] = []
    for table, entry in specs.items():
        fks = entry.get("foreign_keys")
        if not fks:
            continue
        kept: list[dict] = []
        for fk in fks:
            label = f"{table}.{fk.get('columns')} -> {fk.get('parent_table')}"
            if _is_audit_fk(fk):
                audit_dropped.append(label)
            elif (table, tuple(fk.get("columns") or [])) in BACK_REFERENCE_FKS:
                cycle_dropped.append(label)
            else:
                kept.append(fk)
        if kept:
            entry["foreign_keys"] = kept
        else:
            entry.pop("foreign_keys", None)
    return audit_dropped, cycle_dropped


def parse_constraints_csv(path: str) -> list[dict]:
    """Read the constraint dump, normalizing header case and whitespace."""
    rows: list[dict] = []
    with open(path, newline="") as handle:
        for raw in csv.DictReader(handle):
            row = {
                (key or "").strip().upper(): (val.strip() if isinstance(val, str) else val)
                for key, val in raw.items()
            }
            if not row.get("CONSTRAINT_NAME") or not row.get("COLUMN_NAME"):
                continue
            rows.append(row)
    return rows


def build_specs(
    rows: list[dict],
    static_tables: set[str] | None = None,
    overrides: dict | None = None,
) -> dict:
    """Build the specs dict from constraint rows.

    overrides: an existing specs dict whose per-table `static` / `n_rows` are
    carried over (takes precedence over static_tables).
    """
    static_tables = set(static_tables or ())
    overrides = overrides or {}

    # constraint_name -> ordered [column_name]; constraint_name -> meta
    columns: dict[str, list[tuple[int, str]]] = defaultdict(list)
    meta: dict[str, dict] = {}
    for row in rows:
        name = row["CONSTRAINT_NAME"]
        try:
            position = int(float(row.get("COL_POSITION") or 1))
        except (TypeError, ValueError):
            position = 1
        columns[name].append((position, row["COLUMN_NAME"]))
        meta[name] = {
            "type": (row.get("CONSTRAINT_TYPE") or "").strip().upper(),
            "table": row["TABLE_NAME"],
            "ref": (row.get("R_CONSTRAINT_NAME") or "").strip() or None,
        }

    def ordered(name: str) -> list[str]:
        return [col for _, col in sorted(columns[name])]

    # Primary keys (a table appears in specs only if it has a PK).
    specs: dict = {}
    for name, info in meta.items():
        if info["type"] == "P":
            specs[info["table"]] = {"pk_cols": ordered(name)}

    # Foreign keys.
    fks_by_table: dict[str, list[dict]] = defaultdict(list)
    self_refs: list[str] = []
    skipped: list[str] = []
    for name, info in meta.items():
        if info["type"] != "R":
            continue
        child = info["table"]
        ref = info["ref"]
        if child not in specs:
            skipped.append(f"{name} (child {child} has no PK)")
            continue
        if not ref or ref not in meta:
            skipped.append(f"{name} (referenced constraint {ref} not in dump)")
            continue
        child_cols = ordered(name)
        parent_cols = ordered(ref)
        if len(child_cols) != len(parent_cols):
            skipped.append(f"{name} (column count mismatch)")
            continue
        parent_table = meta[ref]["table"]
        if parent_table == child:
            self_refs.append(f"{child}.{child_cols} (engorda nulls these on load)")
        fks_by_table[child].append({
            "columns": child_cols,
            "parent_table": parent_table,
            "parent_columns": parent_cols,
        })

    for table, fks in fks_by_table.items():
        # deterministic order
        specs[table]["foreign_keys"] = sorted(fks, key=lambda fk: fk["columns"])

    # Drop audit + structural back-reference FKs that would create cycles.
    audit_fks, cycle_breaks = prune_cycle_fks(specs)

    # static / n_rows: prefer overrides from an existing specs, else the set.
    for table in specs:
        ov = overrides.get(table, {})
        if ov.get("static") or table in static_tables:
            specs[table]["static"] = True
        if "n_rows" in ov:
            specs[table]["n_rows"] = ov["n_rows"]

    # Emit with stable key order: pk_cols, foreign_keys, static, n_rows.
    out: dict = {}
    for table in sorted(specs):
        entry: dict = {"pk_cols": specs[table]["pk_cols"]}
        if "foreign_keys" in specs[table]:
            entry["foreign_keys"] = specs[table]["foreign_keys"]
        if specs[table].get("static"):
            entry["static"] = True
        if "n_rows" in specs[table]:
            entry["n_rows"] = specs[table]["n_rows"]
        out[table] = entry

    build_specs.last_report = {  # type: ignore[attr-defined]
        "tables": len(out),
        "fks": sum(len(v.get("foreign_keys", [])) for v in out.values()),
        "static": sum(1 for v in out.values() if v.get("static")),
        "self_refs": self_refs,
        "audit_fks": audit_fks,
        "cycle_breaks": cycle_breaks,
        "skipped": skipped,
    }
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate specs.json from an Oracle constraint dump.")
    parser.add_argument("--constraints", required=True, help="CSV from extract_constraints.sql")
    parser.add_argument("--out", default="specs.json", help="Output specs.json path")
    parser.add_argument("--current", default=None,
                        help="Existing specs.json to carry over static / n_rows flags")
    args = parser.parse_args()

    overrides = {}
    if args.current:
        with open(args.current) as handle:
            overrides = json.load(handle)

    rows = parse_constraints_csv(args.constraints)
    specs = build_specs(rows, static_tables=STATIC_TABLES, overrides=overrides)
    with open(args.out, "w") as handle:
        json.dump(specs, handle, indent=2, ensure_ascii=False)

    report = build_specs.last_report  # type: ignore[attr-defined]
    print(f"Wrote {args.out}: {report['tables']} tables, {report['fks']} FKs, "
          f"{report['static']} static.")
    if report["self_refs"]:
        print("Self-referencing FKs (engorda nulls orphans on load):")
        for item in report["self_refs"]:
            print(f"  - {item}")
    if report["audit_fks"]:
        print("Audit FKs excluded (*_ATUALIZ; metadata, not structural):")
        for item in report["audit_fks"]:
            print(f"  - {item}")
    if report["cycle_breaks"]:
        print("Structural back-reference FKs dropped to break cycles:")
        for item in report["cycle_breaks"]:
            print(f"  - {item}")
    if report["skipped"]:
        print("Skipped constraints:")
        for item in report["skipped"]:
            print(f"  - {item}")


if __name__ == "__main__":
    main()
