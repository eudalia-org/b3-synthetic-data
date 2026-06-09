from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import oracledb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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


def fetch_one(connection: oracledb.Connection, query: str, **binds: str) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(query, binds)
        columns = [column[0].lower() for column in cursor.description]
        row = cursor.fetchone()
    if not row:
        return None
    return dict(zip(columns, row, strict=True))


def table_stats(connection: oracledb.Connection, owner: str, table_name: str) -> dict[str, Any]:
    row = fetch_one(
        connection,
        """
        SELECT t.owner,
               t.table_name,
               t.num_rows,
               t.blocks,
               t.avg_row_len,
               t.last_analyzed,
               s.stale_stats
        FROM all_tables t
        LEFT JOIN all_tab_statistics s
          ON s.owner = t.owner
         AND s.table_name = t.table_name
         AND s.object_type = 'TABLE'
        WHERE t.owner = :owner
          AND t.table_name = :table_name
        """,
        owner=owner,
        table_name=table_name,
    )
    if row is None:
        raise ValueError(f"Table not found or not visible: {owner}.{table_name}")
    return row


def segment_bytes(connection: oracledb.Connection, owner: str, table_name: str) -> dict[str, Any]:
    row = fetch_one(
        connection,
        """
        WITH table_segments AS (
            SELECT s.bytes
            FROM all_segments s
            WHERE s.owner = :owner
              AND s.segment_name = :table_name
              AND s.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
        ),
        lob_segments AS (
            SELECT s.bytes
            FROM all_lobs l
            JOIN all_segments s
              ON s.owner = l.owner
             AND s.segment_name = l.segment_name
            WHERE l.owner = :owner
              AND l.table_name = :table_name
        )
        SELECT COALESCE((SELECT SUM(bytes) FROM table_segments), 0) AS table_bytes,
               COALESCE((SELECT SUM(bytes) FROM lob_segments), 0) AS lob_bytes
        FROM dual
        """,
        owner=owner,
        table_name=table_name,
    )
    return row or {"table_bytes": 0, "lob_bytes": 0}


def collect_table_info(
    connection: oracledb.Connection,
    default_owner: str,
    table: str,
) -> dict[str, Any]:
    owner, table_name = table_owner_and_name(default_owner, table)
    stats = table_stats(connection, owner, table_name)
    sizes = segment_bytes(connection, owner, table_name)
    table_bytes = int(sizes["table_bytes"] or 0)
    lob_bytes = int(sizes["lob_bytes"] or 0)
    total_bytes = table_bytes + lob_bytes
    last_analyzed = stats["last_analyzed"]

    return {
        "table": f"{owner}.{table_name}",
        "num_rows_estimate": stats["num_rows"],
        "table_gb": round(table_bytes / 1024**3, 3),
        "lob_gb": round(lob_bytes / 1024**3, 3),
        "total_gb": round(total_bytes / 1024**3, 3),
        "avg_row_len": stats["avg_row_len"],
        "blocks": stats["blocks"],
        "last_analyzed": last_analyzed.isoformat() if last_analyzed else None,
        "stale_stats": stats["stale_stats"],
    }


def print_table(rows: list[dict[str, Any]]) -> None:
    headers = [
        "table",
        "num_rows_estimate",
        "total_gb",
        "table_gb",
        "lob_gb",
        "avg_row_len",
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
        rows = [collect_table_info(connection, env["ORACLE_DB_USER"], table) for table in tables]
    finally:
        connection.close()

    rows.sort(key=lambda row: row["total_gb"], reverse=True)

    if args.format == "json":
        print(json.dumps(rows, indent=2))
    elif args.format == "csv":
        print_csv(rows)
    else:
        print_table(rows)


if __name__ == "__main__":
    main()
