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
    # UNIQUE constraints added in Task 3.
    return schema
