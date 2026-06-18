"""Generate schema.json from Oracle column + constraint dumps.

Inputs:
  - columns.csv     from scripts/extract_schema.sql (all_tab_columns)
  - constraints.csv from scripts/extract_constraints.sql (reused; U rows only)

Output: schema.json in the validate_tables format:

    {
      "TABLE": {
        "columns": {
          "COL": {"type": "NUMBER", "precision": 38, "scale": 0, "nullable": false},
          ...
        },
        "unique": [["COL_A", "COL_B"]]
      }
    }

Usage:
    python scripts/build_schema_from_dump.py \
        --columns columns.csv --constraints constraints.csv --out schema.json
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict


def _norm_row(raw: dict) -> dict:
    return {
        (k or "").strip().upper(): (v.strip() if isinstance(v, str) else v)
        for k, v in raw.items()
    }


def parse_columns_csv(path: str) -> list[dict]:
    """Read the all_tab_columns dump, normalizing header case/whitespace."""
    rows: list[dict] = []
    with open(path, newline="") as handle:
        for raw in csv.DictReader(handle):
            row = _norm_row(raw)
            if not row.get("TABLE_NAME") or not row.get("COLUMN_NAME"):
                continue
            rows.append(row)
    return rows


def _emit(schema: dict) -> dict:
    out: dict = {}
    for table in sorted(schema):
        entry: dict = {"columns": schema[table]["columns"]}
        if schema[table].get("unique"):
            entry["unique"] = schema[table]["unique"]
        out[table] = entry
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate schema.json from Oracle column + constraint dumps.")
    parser.add_argument("--columns", required=True, help="CSV from extract_schema.sql")
    parser.add_argument("--constraints", required=True,
                        help="CSV from extract_constraints.sql (U rows reused)")
    parser.add_argument("--out", default="schema.json", help="Output schema.json path")
    args = parser.parse_args()

    column_rows = parse_columns_csv(args.columns)
    constraint_rows = parse_constraints_csv(args.constraints)
    schema = _emit(build_schema(column_rows, constraint_rows))
    with open(args.out, "w") as handle:
        json.dump(schema, handle, indent=2, ensure_ascii=False)
    n_unique = sum(1 for v in schema.values() if v.get("unique"))
    print(f"Wrote {args.out}: {len(schema)} tables, {n_unique} with UNIQUE keys.")


def _table_name(raw: str) -> str:
    return raw.split(".", 1)[1] if "." in raw else raw


def _int_or_none(value) -> int | None:
    try:
        text = str(value).strip()
        return int(float(text)) if text else None
    except (TypeError, ValueError):
        return None


def _column_entry(row: dict) -> dict:
    data_type = (row.get("DATA_TYPE") or "").strip().upper()
    nullable = (row.get("NULLABLE") or "Y").strip().upper() != "N"
    entry: dict = {"type": data_type, "nullable": nullable}
    precision = _int_or_none(row.get("DATA_PRECISION"))
    scale = _int_or_none(row.get("DATA_SCALE"))
    length = _int_or_none(row.get("CHAR_LENGTH"))
    if precision is not None:
        entry["precision"] = precision
        entry["scale"] = scale or 0
    elif length:  # character columns: length, no precision (0/empty -> omit)
        entry["length"] = length
    return entry


def build_schema(column_rows: list[dict], constraint_rows: list[dict]) -> dict:
    """Build schema.json dict from column rows (+ constraint rows for UNIQUE)."""
    schema: dict = {}
    for row in column_rows:
        table = _table_name(row["TABLE_NAME"])
        schema.setdefault(table, {"columns": {}})
        schema[table]["columns"][row["COLUMN_NAME"]] = _column_entry(row)
    # UNIQUE constraints: position-ordered column lists, only for known tables.
    uniques: dict[str, list[tuple[int, str]]] = defaultdict(list)
    unique_table: dict[str, str] = {}
    for row in constraint_rows:
        norm = _norm_row(row)
        if (norm.get("CONSTRAINT_TYPE") or "").upper() != "U":
            continue
        name = norm.get("CONSTRAINT_NAME")
        if not name or not norm.get("COLUMN_NAME"):
            continue
        position = _int_or_none(norm.get("COL_POSITION")) or 1
        uniques[name].append((position, norm["COLUMN_NAME"]))
        unique_table[name] = _table_name(norm["TABLE_NAME"])
    per_table: dict[str, list[list[str]]] = defaultdict(list)
    for name, cols in uniques.items():
        table = unique_table[name]
        if table not in schema:
            continue
        per_table[table].append([c for _, c in sorted(cols)])
    for table, keys in per_table.items():
        schema[table]["unique"] = sorted(keys)
    return schema


def parse_constraints_csv(path: str) -> list[dict]:
    rows: list[dict] = []
    with open(path, newline="") as handle:
        for raw in csv.DictReader(handle):
            row = _norm_row(raw)
            if not row.get("CONSTRAINT_NAME") or not row.get("COLUMN_NAME"):
                continue
            rows.append(row)
    return rows


if __name__ == "__main__":
    main()
