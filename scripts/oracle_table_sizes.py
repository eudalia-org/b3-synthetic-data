from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import oracledb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report Oracle table row-count estimates and segment sizes."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--tables",
        help="Comma-separated table list, for example CUSTOMERS,HR.ORDERS.",
    )
    source.add_argument(
        "--tables-file",
        help="Text file with one table per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--format",
        choices=("table", "csv", "json"),
        default="table",
        help="Output format.",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=8192,
        help=(
            "Oracle database block size in bytes, used to estimate table size from "
            "USER_TABLES.BLOCKS."
        ),
    )
    parser.add_argument(
        "--allow-external-count",
        action="store_true",
        help=(
            "For schema-qualified tables outside ORACLE_DB_USER, run SELECT COUNT(*) instead "
            "of USER_* stats. This can be expensive on large tables."
        ),
    )
    parser.add_argument(
        "--compressed-bytes-per-row",
        type=float,
        help=(
            "Optional measured Parquet bytes per row, used to estimate output size for "
            "COUNT(*) rows."
        ),
    )
    return parser.parse_args()


def required_env(*names: str) -> dict[str, str]:
    values = {}
    missing = []
    for name in names:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            values[name] = value
    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        sys.exit(1)
    return values


def parse_tables(tables: str | None, tables_file: str | None) -> list[str]:
    if tables:
        raw_tables = tables.split(",")
    else:
        path = Path(tables_file or "")
        try:
            raw_tables = path.read_text().splitlines()
        except OSError as exc:
            logger.error("Failed to read table list %s: %s", path, exc)
            sys.exit(1)

    parsed = []
    for item in raw_tables:
        table = item.strip()
        if table and not table.startswith("#"):
            parsed.append(table)

    deduped = list(dict.fromkeys(parsed))
    if not deduped:
        logger.error("No tables provided")
        sys.exit(1)
    return deduped


def table_owner_and_name(default_owner: str, table: str) -> tuple[str, str]:
    if "." in table:
        owner, table_name = table.split(".", 1)
        return owner.upper(), table_name.upper()
    return default_owner.upper(), table.upper()


def qualified_table_name(owner: str, table_name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(owner) or not IDENTIFIER_PATTERN.fullmatch(table_name):
        raise ValueError(f"Unsupported table identifier: {owner}.{table_name}")
    return f"{owner}.{table_name}"


def fetch_one(connection: oracledb.Connection, query: str, **binds: str) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(query, binds)
        columns = [column[0].lower() for column in cursor.description]
        row = cursor.fetchone()
    if not row:
        return None
    return dict(zip(columns, row, strict=True))


def table_stats(connection: oracledb.Connection, table_name: str) -> dict[str, Any]:
    row = fetch_one(
        connection,
        """
        SELECT t.table_name,
               t.num_rows,
               t.blocks,
               t.avg_row_len,
               t.last_analyzed,
               s.stale_stats
        FROM user_tables t
        LEFT JOIN user_tab_statistics s
          ON s.table_name = t.table_name
         AND s.object_type = 'TABLE'
        WHERE t.table_name = :table_name
        """,
        table_name=table_name,
    )
    if row is None:
        raise ValueError(f"Table not found in connected schema: {table_name}")
    return row


def count_rows(connection: oracledb.Connection, owner: str, table_name: str) -> int:
    table_ref = qualified_table_name(owner, table_name)
    row = fetch_one(connection, f"SELECT COUNT(*) AS row_count FROM {table_ref}")
    return int(row["row_count"])


def collect_table_info(
    connection: oracledb.Connection,
    default_owner: str,
    table: str,
    block_size: int,
    allow_external_count: bool,
    compressed_bytes_per_row: float | None,
) -> dict[str, Any]:
    owner, table_name = table_owner_and_name(default_owner, table)
    if owner != default_owner.upper():
        if not allow_external_count:
            raise ValueError(
                f"{owner}.{table_name} is outside connected schema {default_owner.upper()}; "
                "USER_* dictionary views can only report tables owned by the current user. "
                "Pass --allow-external-count to run SELECT COUNT(*) for cross-schema tables."
            )
        row_count = count_rows(connection, owner, table_name)
        output_bytes_estimate = (
            row_count * compressed_bytes_per_row if compressed_bytes_per_row is not None else None
        )
        return {
            "table": f"{owner}.{table_name}",
            "num_rows_estimate": row_count,
            "blocks_gb_estimate": None,
            "row_gb_estimate": round(output_bytes_estimate / 1024**3, 3)
            if output_bytes_estimate is not None
            else None,
            "avg_row_len": None,
            "blocks": None,
            "block_size": block_size,
            "last_analyzed": None,
            "stale_stats": "COUNT_STAR",
        }
    stats = table_stats(connection, table_name)
    blocks = int(stats["blocks"] or 0)
    num_rows = int(stats["num_rows"] or 0)
    avg_row_len = int(stats["avg_row_len"] or 0)
    table_bytes_estimate = blocks * block_size
    row_bytes_estimate = num_rows * avg_row_len
    last_analyzed = stats["last_analyzed"]

    return {
        "table": f"{owner}.{table_name}",
        "num_rows_estimate": stats["num_rows"],
        "blocks_gb_estimate": round(table_bytes_estimate / 1024**3, 3),
        "row_gb_estimate": round(row_bytes_estimate / 1024**3, 3),
        "avg_row_len": stats["avg_row_len"],
        "blocks": stats["blocks"],
        "block_size": block_size,
        "last_analyzed": last_analyzed.isoformat() if last_analyzed else None,
        "stale_stats": stats["stale_stats"],
    }


def print_table(rows: list[dict[str, Any]]) -> None:
    headers = [
        "table",
        "num_rows_estimate",
        "blocks_gb_estimate",
        "row_gb_estimate",
        "avg_row_len",
        "blocks",
        "last_analyzed",
        "stale_stats",
    ]
    widths = {
        header: max(len(header), *(len(str(row[header] or "")) for row in rows))
        for header in headers
    }
    print("  ".join(header.ljust(widths[header]) for header in headers))
    print("  ".join("-" * widths[header] for header in headers))
    for row in rows:
        print("  ".join(str(row[header] or "").ljust(widths[header]) for header in headers))


def print_csv(rows: list[dict[str, Any]]) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)


def main() -> None:
    args = parse_arguments()
    env = required_env("ORACLE_DB_USER", "ORACLE_DB_PASSWORD", "ORACLE_DSN")
    tables = parse_tables(args.tables, args.tables_file)

    logger.info("Connecting to Oracle DSN %s as %s", env["ORACLE_DSN"], env["ORACLE_DB_USER"])
    connection = oracledb.connect(
        user=env["ORACLE_DB_USER"],
        password=env["ORACLE_DB_PASSWORD"],
        dsn=env["ORACLE_DSN"],
    )
    try:
        rows = [
            collect_table_info(
                connection,
                env["ORACLE_DB_USER"],
                table,
                args.block_size,
                args.allow_external_count,
                args.compressed_bytes_per_row,
            )
            for table in tables
        ]
    finally:
        connection.close()

    rows.sort(key=lambda row: row["blocks_gb_estimate"], reverse=True)

    if args.format == "json":
        print(json.dumps(rows, indent=2))
    elif args.format == "csv":
        print_csv(rows)
    else:
        print_table(rows)


if __name__ == "__main__":
    main()
