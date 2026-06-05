from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_FETCH_SIZE = 10_000
DEFAULT_TARGET_BLOCKS = 131_072
DEFAULT_COMPRESSION = "snappy"
CHECKPOINT_FILE = "rowid_migration_checkpoint.jsonl"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export Oracle tables to local Parquet files by ROWID ranges, upload each file "
            "to OCI Object Storage with the OCI CLI, then delete the local file."
        )
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
    parser.add_argument("--bucket", required=True, help="OCI Object Storage bucket name.")
    parser.add_argument(
        "--prefix",
        default="oracle-rowid-export",
        help="Object name prefix inside the bucket.",
    )
    parser.add_argument(
        "--work-dir",
        default="rowid_export_work",
        help="Local directory used for temporary Parquet files and checkpoint state.",
    )
    parser.add_argument(
        "--target-blocks",
        type=int,
        default=DEFAULT_TARGET_BLOCKS,
        help=(
            "Approximate Oracle blocks per ROWID chunk. With 8 KB DB blocks, 131072 is "
            "about 1 GB of table segment before Parquet compression."
        ),
    )
    parser.add_argument(
        "--fetch-size",
        type=int,
        default=DEFAULT_FETCH_SIZE,
        help="Rows fetched from Oracle per batch.",
    )
    parser.add_argument(
        "--compression",
        default=DEFAULT_COMPRESSION,
        choices=("snappy", "zstd", "gzip", "brotli", "lz4", "none"),
        help="Parquet compression codec.",
    )
    parser.add_argument(
        "--namespace",
        help="Optional OCI namespace. If omitted, OCI CLI default/config is used.",
    )
    parser.add_argument("--profile", help="Optional OCI CLI profile name.")
    parser.add_argument(
        "--config-file",
        help="Optional OCI CLI config file path, for example C:\\Users\\me\\.oci\\config.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with remaining chunks/tables after a failure.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate chunks and log planned object names without exporting/uploading data.",
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
            raw_tables = path.read_text(encoding="utf-8").splitlines()
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


def split_table_name(table: str, default_owner: str) -> tuple[str, str]:
    if "." in table:
        owner, table_name = table.split(".", 1)
    else:
        owner, table_name = default_owner, table
    return owner.upper(), table_name.upper()


def quote_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*", name):
        raise ValueError(f"Unsupported Oracle identifier: {name!r}")
    return name.upper()


def qualified_table(owner: str, table_name: str) -> str:
    return f"{quote_name(owner)}.{quote_name(table_name)}"


def safe_path_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def load_completed_chunks(checkpoint_path: Path) -> set[str]:
    completed = set()
    if not checkpoint_path.exists():
        return completed

    for line in checkpoint_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Ignoring invalid checkpoint line: %s", line)
            continue
        if event.get("status") == "uploaded" and event.get("chunk_key"):
            completed.add(event["chunk_key"])
    return completed


def append_checkpoint(checkpoint_path: Path, event: dict[str, Any]) -> None:
    event = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
    with checkpoint_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def fetch_rowid_ranges(
    connection: oracledb.Connection,
    owner: str,
    table_name: str,
    target_blocks: int,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            dbms_rowid.rowid_create(1, o.data_object_id, e.relative_fno, e.block_id, 0)
                AS start_rowid,
            dbms_rowid.rowid_create(
                1,
                o.data_object_id,
                e.relative_fno,
                e.block_id + e.blocks - 1,
                32767
            ) AS end_rowid,
            e.blocks
        FROM all_extents e
        JOIN all_objects o
          ON o.owner = e.owner
         AND o.object_name = e.segment_name
         AND o.object_type = 'TABLE'
        WHERE e.owner = :owner
          AND e.segment_name = :table_name
          AND e.segment_type = 'TABLE'
        ORDER BY e.relative_fno, e.block_id
    """
    try:
        rows = connection.cursor().execute(
            query,
            owner=owner,
            table_name=table_name,
        )
    except oracledb.DatabaseError as exc:
        logger.warning("ALL_EXTENTS query failed for %s.%s: %s", owner, table_name, exc)
        query = """
            SELECT
                dbms_rowid.rowid_create(1, o.data_object_id, e.relative_fno, e.block_id, 0)
                    AS start_rowid,
                dbms_rowid.rowid_create(
                    1,
                    o.data_object_id,
                    e.relative_fno,
                    e.block_id + e.blocks - 1,
                    32767
                ) AS end_rowid,
                e.blocks
            FROM user_extents e
            JOIN user_objects o
              ON o.object_name = e.segment_name
             AND o.object_type = 'TABLE'
            WHERE e.segment_name = :table_name
              AND e.segment_type = 'TABLE'
            ORDER BY e.relative_fno, e.block_id
        """
        rows = connection.cursor().execute(query, table_name=table_name)

    chunks = []
    current: dict[str, Any] | None = None
    for start_rowid, end_rowid, blocks in rows:
        blocks = int(blocks)
        if current is None:
            current = {
                "start_rowid": start_rowid,
                "end_rowid": end_rowid,
                "blocks": blocks,
            }
            continue

        if current["blocks"] + blocks <= target_blocks:
            current["end_rowid"] = end_rowid
            current["blocks"] += blocks
        else:
            chunks.append(current)
            current = {
                "start_rowid": start_rowid,
                "end_rowid": end_rowid,
                "blocks": blocks,
            }

    if current:
        chunks.append(current)

    return chunks


def rows_to_arrow_table(rows: list[tuple[Any, ...]], columns: list[str]) -> pa.Table:
    normalized_rows = [tuple(normalize_value(value) for value in row) for row in rows]
    column_values = (
        list(zip(*normalized_rows, strict=False)) if rows else [[] for _ in columns]
    )
    arrays = [pa.array(values) for values in column_values]
    return pa.Table.from_arrays(arrays, names=columns)


def normalize_value(value: Any) -> Any:
    if isinstance(value, oracledb.LOB):
        return value.read()
    return value


def export_chunk_to_parquet(
    connection: oracledb.Connection,
    table_ref: str,
    start_rowid: str,
    end_rowid: str,
    output_file: Path,
    fetch_size: int,
    compression: str,
) -> int:
    query = f"""
        SELECT *
        FROM {table_ref}
        WHERE ROWID >= CHARTOROWID(:start_rowid)
          AND ROWID <= CHARTOROWID(:end_rowid)
    """
    cursor = connection.cursor()
    cursor.arraysize = fetch_size
    cursor.prefetchrows = fetch_size
    cursor.execute(query, start_rowid=start_rowid, end_rowid=end_rowid)

    columns = [description[0] for description in cursor.description]
    writer = None
    total_rows = 0
    parquet_compression = None if compression == "none" else compression

    try:
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            table = rows_to_arrow_table(rows, columns)
            if writer is None:
                writer = pq.ParquetWriter(
                    output_file,
                    table.schema,
                    compression=parquet_compression,
                )
            writer.write_table(table)
            total_rows += len(rows)

        if writer is None:
            empty_table = pa.Table.from_arrays(
                [pa.array([]) for _ in columns],
                names=columns,
            )
            writer = pq.ParquetWriter(
                output_file,
                empty_table.schema,
                compression=parquet_compression,
            )
            writer.write_table(empty_table)
    finally:
        if writer is not None:
            writer.close()
        cursor.close()

    return total_rows


def upload_with_oci_cli(
    file_path: Path,
    bucket: str,
    object_name: str,
    namespace: str | None,
    profile: str | None,
    config_file: str | None,
) -> None:
    command = [
        "oci",
        "os",
        "object",
        "put",
        "--bucket-name",
        bucket,
        "--file",
        str(file_path),
        "--name",
        object_name,
        "--force",
    ]
    if namespace:
        command.extend(["--namespace-name", namespace])
    if profile:
        command.extend(["--profile", profile])
    if config_file:
        command.extend(["--config-file", config_file])

    subprocess.run(command, check=True)


def migrate_table(
    connection: oracledb.Connection,
    table: str,
    default_owner: str,
    args: argparse.Namespace,
    work_dir: Path,
    checkpoint_path: Path,
    completed_chunks: set[str],
) -> list[str]:
    owner, table_name = split_table_name(table, default_owner)
    table_ref = qualified_table(owner, table_name)
    table_slug = safe_path_part(f"{owner}.{table_name}")

    chunks = fetch_rowid_ranges(connection, owner, table_name, args.target_blocks)
    if not chunks:
        raise RuntimeError(f"No ROWID chunks found for {table_ref}")

    logger.info("%s: generated %s ROWID chunk(s)", table_ref, len(chunks))
    failures = []

    for index, chunk in enumerate(chunks, start=1):
        chunk_key = f"{owner}.{table_name}:{index:06d}:{chunk['start_rowid']}:{chunk['end_rowid']}"
        object_name = (
            f"{args.prefix.rstrip('/')}/{table_slug}/chunk_{index:06d}.parquet"
        )
        local_file = work_dir / table_slug / f"chunk_{index:06d}.parquet"

        if chunk_key in completed_chunks:
            logger.info("%s chunk %s already uploaded; skipping", table_ref, index)
            continue

        if args.dry_run:
            logger.info(
                "DRY RUN %s chunk %s blocks=%s object=%s",
                table_ref,
                index,
                chunk["blocks"],
                object_name,
            )
            continue

        local_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            logger.info("Exporting %s chunk %s to %s", table_ref, index, local_file)
            export_started = time.monotonic()
            row_count = export_chunk_to_parquet(
                connection=connection,
                table_ref=table_ref,
                start_rowid=chunk["start_rowid"],
                end_rowid=chunk["end_rowid"],
                output_file=local_file,
                fetch_size=args.fetch_size,
                compression=args.compression,
            )
            export_seconds = time.monotonic() - export_started
            size_bytes = local_file.stat().st_size

            logger.info(
                "Uploading %s chunk %s rows=%s size_mb=%.2f export_seconds=%.2f to %s",
                table_ref,
                index,
                row_count,
                size_bytes / 1024 / 1024,
                export_seconds,
                object_name,
            )
            upload_started = time.monotonic()
            upload_with_oci_cli(
                file_path=local_file,
                bucket=args.bucket,
                object_name=object_name,
                namespace=args.namespace,
                profile=args.profile,
                config_file=args.config_file,
            )
            upload_seconds = time.monotonic() - upload_started
            total_seconds = export_seconds + upload_seconds
            size_mb = size_bytes / 1024 / 1024
            append_checkpoint(
                checkpoint_path,
                {
                    "status": "uploaded",
                    "table": table_ref,
                    "chunk_index": index,
                    "chunk_key": chunk_key,
                    "object_name": object_name,
                    "row_count": row_count,
                    "size_bytes": size_bytes,
                    "size_mb": round(size_mb, 2),
                    "export_seconds": round(export_seconds, 2),
                    "upload_seconds": round(upload_seconds, 2),
                    "total_seconds": round(total_seconds, 2),
                    "export_rows_per_second": round(row_count / export_seconds, 2)
                    if export_seconds
                    else None,
                    "upload_mb_per_second": round(size_mb / upload_seconds, 2)
                    if upload_seconds
                    else None,
                    "start_rowid": chunk["start_rowid"],
                    "end_rowid": chunk["end_rowid"],
                },
            )
            completed_chunks.add(chunk_key)
            local_file.unlink()
            logger.info("Deleted local file %s", local_file)
        except Exception as exc:
            logger.exception("Failed %s chunk %s: %s", table_ref, index, exc)
            append_checkpoint(
                checkpoint_path,
                {
                    "status": "failed",
                    "table": table_ref,
                    "chunk_index": index,
                    "chunk_key": chunk_key,
                    "error": str(exc),
                },
            )
            failures.append(chunk_key)
            if not args.continue_on_error:
                raise

    return failures


def main() -> None:
    args = parse_arguments()
    env = required_env("ORACLE_DB_USER", "ORACLE_DB_PASSWORD", "ORACLE_DSN")
    tables = parse_tables(args.tables, args.tables_file)
    work_dir = Path(args.work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = work_dir / CHECKPOINT_FILE
    completed_chunks = load_completed_chunks(checkpoint_path)

    logger.info(
        "Connecting to Oracle DSN %s as %s",
        env["ORACLE_DSN"],
        env["ORACLE_DB_USER"],
    )
    connection = oracledb.connect(
        user=env["ORACLE_DB_USER"],
        password=env["ORACLE_DB_PASSWORD"],
        dsn=env["ORACLE_DSN"],
    )

    failures = []
    try:
        for table in tables:
            try:
                failures.extend(
                    migrate_table(
                        connection=connection,
                        table=table,
                        default_owner=env["ORACLE_DB_USER"],
                        args=args,
                        work_dir=work_dir,
                        checkpoint_path=checkpoint_path,
                        completed_chunks=completed_chunks,
                    )
                )
            except Exception as exc:
                logger.exception("Failed table %s: %s", table, exc)
                failures.append(table)
                if not args.continue_on_error:
                    raise
    finally:
        connection.close()

    if failures:
        logger.error("Migration finished with failures: %s", ", ".join(failures))
        sys.exit(1)

    logger.info("Migration finished successfully")


if __name__ == "__main__":
    main()
