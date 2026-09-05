from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_TARGET_DB_USER = "ADMIN"
DEFAULT_READ_TIMEOUT_MS = "600000"
DEFAULT_CHUNK_SIZE = "5000000"
REQUIRED_ENV_VARS = (
    "DATAGEN_TARGET_JDBC_URL",
    "DATAGEN_TARGET_DB_PASSWORD",
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def validate_identifier(name: str) -> str:
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Unsupported Oracle identifier: {name!r}")
    return upper


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Roll back a load_tables.py attempt by deleting its exact recorded"
            " synthetic numeric-PK ranges."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--manifest-uri", help="Exact load manifest JSON object.")
    source.add_argument("--run-id", help="Legacy run id under DATAGEN_LOAD_BASE_URI.")
    parser.add_argument(
        "--chunk-size",
        type=positive_int,
        default=int(DEFAULT_CHUNK_SIZE),
        help="PK values to delete per chunk (default 5000000).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Try remaining tables after a failure, then exit non-zero if any failed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the rows that would be deleted per table without deleting anything.",
    )
    return parser.parse_args()


def rollback_order(entries: list) -> list:
    """Children-before-parents delete order.

    The manifest lists tables in load (parent-first) order. Deleting a parent
    while a child still references it raises ORA-02292, so reverse the manifest
    order — the reverse of a valid parent-first topo order is a valid
    children-first order.
    """
    return list(reversed(entries))


def get_env(require_load_base: bool = True) -> dict[str, str]:
    config = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    load_base = os.environ.get("DATAGEN_LOAD_BASE_URI")
    if load_base:
        config["DATAGEN_LOAD_BASE_URI"] = load_base.rstrip("/")
    elif require_load_base:
        missing.append("DATAGEN_LOAD_BASE_URI")
    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        sys.exit(1)
    config["DATAGEN_TARGET_DB_USER"] = os.environ.get(
        "DATAGEN_TARGET_DB_USER", DEFAULT_TARGET_DB_USER
    )
    config["DATAGEN_JDBC_READ_TIMEOUT_MS"] = os.environ.get(
        "DATAGEN_JDBC_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS
    )
    return config


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()


def build_connection_properties(config: dict[str, str]) -> dict[str, str]:
    return {
        "url": config["DATAGEN_TARGET_JDBC_URL"],
        "user": config["DATAGEN_TARGET_DB_USER"],
        "password": config["DATAGEN_TARGET_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
        "oracle.jdbc.ReadTimeout": config["DATAGEN_JDBC_READ_TIMEOUT_MS"],
    }


def read_rows(spark: SparkSession, properties: dict[str, str], query: str) -> list:
    return (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", f"({query}) DATAGEN_Q")
        .load()
        .collect()
    )


def execute_statement(spark: SparkSession, properties: dict[str, str], sql: str) -> None:
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
        properties["url"], properties["user"], properties["password"]
    )
    try:
        stmt = conn.prepareStatement(sql)
        try:
            stmt.execute()
        finally:
            stmt.close()
    finally:
        conn.close()


def _local_artifact_path(uri: str) -> str | None:
    parsed = urlsplit(uri)
    if not parsed.scheme:
        return uri
    if parsed.scheme == "file" and parsed.netloc in {"", "localhost"}:
        return unquote(parsed.path)
    return None


def _read_exact_json_object(spark: SparkSession, uri: str) -> dict:
    try:
        local_path = _local_artifact_path(uri)
        if local_path is not None:
            with open(local_path, encoding="utf-8") as handle:
                parsed = json.load(handle)
        else:
            jvm = spark._jvm
            path = jvm.org.apache.hadoop.fs.Path(uri)
            fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
            if not fs.exists(path) or fs.getFileStatus(path).isDirectory():
                raise ValueError(f"expected one JSON object at {uri!r}")
            stream = fs.open(path)
            try:
                text = jvm.org.apache.commons.io.IOUtils.toString(
                    stream, jvm.java.nio.charset.StandardCharsets.UTF_8
                )
            finally:
                stream.close()
            parsed = json.loads(text)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.error("Failed to read manifest %s: %s", uri, exc)
        sys.exit(1)
    if not isinstance(parsed, dict):
        logger.error("Manifest %s must contain a JSON object", uri)
        sys.exit(1)
    return parsed


def read_manifest(
    spark: SparkSession,
    config: dict[str, str],
    run_id: str | None = None,
    manifest_uri: str | None = None,
) -> dict:
    if manifest_uri is not None:
        return _read_exact_json_object(spark, manifest_uri)
    path = f"{config['DATAGEN_LOAD_BASE_URI']}/_load_manifests/{run_id}"
    try:
        text = "\n".join(spark.sparkContext.textFile(path).collect())
        parsed = json.loads(text)
    except Exception as exc:
        logger.error("Failed to read manifest %s: %s", path, exc)
        sys.exit(1)
    if not isinstance(parsed, dict):
        logger.error("Manifest %s must contain a JSON object", path)
        sys.exit(1)
    return parsed


def is_exact_load_manifest(manifest: dict) -> bool:
    version = manifest.get("schema_version")
    kind = manifest.get("kind")
    if version is None and kind is None:
        return False
    if version != 1 or kind != "load-attempt":
        raise ValueError("Unsupported load manifest schema or kind")
    entries = manifest.get("tables")
    if not isinstance(entries, list):
        raise ValueError("Load manifest tables must be a list")
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Load manifest table entries must be objects")
        if not entry.get("rollbackable"):
            continue
        for key in ("synthetic_pk_min", "synthetic_pk_max"):
            value = entry.get(key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"Rollbackable table requires integer {key}")
        if entry["synthetic_pk_min"] > entry["synthetic_pk_max"]:
            raise ValueError("synthetic_pk_min must not exceed synthetic_pk_max")
    return True


def require_exact_load_manifest(manifest: dict) -> None:
    if not is_exact_load_manifest(manifest):
        raise ValueError(
            "Legacy load manifests have no exact synthetic PK ranges and cannot be "
            "rolled back safely"
        )


def pk_chunk_ranges(lower_exclusive: int, upper: int, chunk_size: int) -> list[tuple[int, int]]:
    ranges = []
    lo = lower_exclusive + 1
    while lo <= upper:
        hi = min(lo + chunk_size - 1, upper)
        ranges.append((lo, hi))
        lo = hi + 1
    return ranges


def delete_above_sql(owner: str, table_name: str, pk_col: str, lo, hi) -> str:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    pk_col = validate_identifier(pk_col)
    for bound in (lo, hi):
        if isinstance(bound, bool) or not isinstance(bound, int):
            raise ValueError(f"PK bound must be an integer: {bound!r}")
    return f"DELETE FROM {owner}.{table_name} WHERE {pk_col} BETWEEN {lo} AND {hi}"


def scalar(spark, properties, query):
    rows = read_rows(spark, properties, query)
    return rows[0][0] if rows and rows[0][0] is not None else None


def rollback_table(spark, properties, entry, chunk_size, index, total, dry_run=False) -> int:
    owner, name, pk_col = entry["owner"], entry["name"], entry["pk_col"]
    if "synthetic_pk_min" in entry or "synthetic_pk_max" in entry:
        lower = entry["synthetic_pk_min"]
        upper = entry["synthetic_pk_max"]
        ranges = pk_chunk_ranges(lower - 1, upper, chunk_size)
        logger.info(
            "[%d/%d] %s: %s exact synthetic PK [%s, %s] in %d chunk(s)",
            index,
            total,
            entry["table"],
            "DRY RUN — would delete" if dry_run else "deleting",
            lower,
            upper,
            len(ranges),
        )
        if dry_run:
            return len(ranges)
        for lo, hi in ranges:
            execute_statement(spark, properties, delete_above_sql(owner, name, pk_col, lo, hi))
        return len(ranges)

    max_before = entry["max_pk_before"]
    o, t, p = validate_identifier(owner), validate_identifier(name), validate_identifier(pk_col)
    current_max = scalar(spark, properties, f"SELECT MAX({p}) FROM {o}.{t}")
    if current_max is None:
        logger.info("[%d/%d] %s: empty -> nothing to roll back", index, total, entry["table"])
        return 0
    current_max = int(current_max)
    if max_before is None:
        min_pk = scalar(spark, properties, f"SELECT MIN({p}) FROM {o}.{t}")
        lower_exclusive = int(min_pk) - 1 if min_pk is not None else current_max
    else:
        lower_exclusive = int(max_before)
    ranges = pk_chunk_ranges(lower_exclusive, current_max, chunk_size)
    if not ranges:
        logger.info("[%d/%d] %s: nothing above max_pk_before", index, total, entry["table"])
        return 0
    logger.info(
        "[%d/%d] %s: %s PK (%s, %s] in %d chunk(s)",
        index,
        total,
        entry["table"],
        "DRY RUN — would delete" if dry_run else "deleting",
        lower_exclusive,
        current_max,
        len(ranges),
    )
    if dry_run:
        return len(ranges)
    for lo, hi in ranges:
        execute_statement(spark, properties, delete_above_sql(owner, name, pk_col, lo, hi))
    return len(ranges)


def main() -> None:
    args = parse_arguments()
    config = get_env(require_load_base=args.manifest_uri is None)
    spark = create_spark_session("DataGenRollbackLoad")
    properties = build_connection_properties(config)
    failures = []
    try:
        manifest = read_manifest(spark, config, run_id=args.run_id, manifest_uri=args.manifest_uri)
        require_exact_load_manifest(manifest)
        entries = [e for e in manifest.get("tables", [])]
        # Delete children before parents (reverse of the parent-first load order)
        # so a parent delete never hits ORA-02292 from a still-present child.
        rollbackable = rollback_order([e for e in entries if e.get("rollbackable")])
        skipped = [e for e in entries if not e.get("rollbackable")]
        for e in skipped:
            logger.warning(
                "%s: not rollbackable (no single numeric PK) -> use a DB restore point",
                e["table"],
            )
        total = len(rollbackable)
        if args.dry_run:
            logger.info("DRY RUN: no rows will be deleted.")
        source = args.manifest_uri or args.run_id
        logger.info("Rolling back manifest=%s: %d rollbackable table(s)", source, total)
        for index, entry in enumerate(rollbackable, start=1):
            try:
                rollback_table(
                    spark, properties, entry, args.chunk_size, index, total, dry_run=args.dry_run
                )
                logger.info(
                    "[%d/%d] %s: %s",
                    index,
                    total,
                    entry["table"],
                    "would be rolled back" if args.dry_run else "rolled back",
                )
            except Exception as exc:
                logger.exception("[%d/%d] %s: FAILED: %s", index, total, entry["table"], exc)
                failures.append(entry["table"])
                if not args.continue_on_error:
                    raise
    finally:
        spark.stop()
    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)


if __name__ == "__main__":
    main()
