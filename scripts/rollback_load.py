from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING

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
    "DATAGEN_LOAD_BASE_URI",
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
            "Roll back a load_tables.py run by deleting rows appended above each"
            " table's pre-load max PK."
        )
    )
    parser.add_argument("--run-id", required=True, help="Run id of the load manifest to roll back.")
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
    return parser.parse_args()


def get_env() -> dict[str, str]:
    config = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
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


def read_manifest(spark: SparkSession, config: dict[str, str], run_id: str) -> dict:
    path = f"{config['DATAGEN_LOAD_BASE_URI']}/_load_manifests/{run_id}"
    try:
        text = "\n".join(spark.sparkContext.textFile(path).collect())
        return json.loads(text)
    except Exception as exc:
        logger.error("Failed to read manifest %s: %s", path, exc)
        sys.exit(1)


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


def rollback_table(spark, properties, entry, chunk_size, index, total) -> int:
    owner, name, pk_col = entry["owner"], entry["name"], entry["pk_col"]
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
        "[%d/%d] %s: deleting PK (%s, %s] in %d chunk(s)",
        index, total, entry["table"], lower_exclusive, current_max, len(ranges),
    )
    for lo, hi in ranges:
        execute_statement(spark, properties, delete_above_sql(owner, name, pk_col, lo, hi))
    return len(ranges)


def main() -> None:
    args = parse_arguments()
    config = get_env()
    spark = create_spark_session("DataGenRollbackLoad")
    properties = build_connection_properties(config)
    failures = []
    try:
        manifest = read_manifest(spark, config, args.run_id)
        entries = [e for e in manifest.get("tables", [])]
        rollbackable = [e for e in entries if e.get("rollbackable")]
        skipped = [e for e in entries if not e.get("rollbackable")]
        for e in skipped:
            logger.warning(
                "%s: not rollbackable (no single numeric PK) -> use a DB restore point",
                e["table"],
            )
        total = len(rollbackable)
        logger.info("Rolling back run_id=%s: %d rollbackable table(s)", args.run_id, total)
        for index, entry in enumerate(rollbackable, start=1):
            try:
                rollback_table(spark, properties, entry, args.chunk_size, index, total)
                logger.info("[%d/%d] %s: rolled back", index, total, entry["table"])
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
