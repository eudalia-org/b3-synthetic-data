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
