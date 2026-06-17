from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from decimal import Decimal
from pathlib import Path
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
DEFAULT_NUM_PARTITIONS = "256"
DEFAULT_BATCH_SIZE = "10000"
DEFAULT_READ_TIMEOUT_MS = "600000"
DEFAULT_ISOLATION_LEVEL = "READ_COMMITTED"
PARQUET_REBASE_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
}
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


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load per-table Parquet into target Oracle with parallel JDBC writes."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--tables",
        help="Comma-separated table list, for example CUSTOMERS,ORDERS,ORDER_ITEMS.",
    )
    source.add_argument(
        "--tables-file",
        help="Local text file with one table per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Try remaining tables after a failure, then exit non-zero if any failed.",
    )
    return parser.parse_args()


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_tables(tables: str | None, tables_file: str | None) -> list[str]:
    if tables:
        parsed = [table.strip() for table in tables.split(",")]
    else:
        path = Path(tables_file or "")
        try:
            lines = path.read_text().splitlines()
        except OSError as exc:
            logger.error("Failed to read table list %s: %s", path, exc)
            sys.exit(1)
        parsed = []
        for line in lines:
            table = line.strip()
            if table and not table.startswith("#"):
                parsed.append(table)

    deduped = list(dict.fromkeys(table for table in parsed if table))
    if not deduped:
        logger.error("No tables provided")
        sys.exit(1)
    return deduped


def get_load_env() -> dict[str, str]:
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
    config["DATAGEN_LOAD_PREFIX"] = os.environ.get("DATAGEN_LOAD_PREFIX", "").strip("/")
    config["DATAGEN_JDBC_NUM_PARTITIONS"] = os.environ.get(
        "DATAGEN_JDBC_NUM_PARTITIONS", DEFAULT_NUM_PARTITIONS
    )
    config["DATAGEN_JDBC_BATCH_SIZE"] = os.environ.get(
        "DATAGEN_JDBC_BATCH_SIZE", DEFAULT_BATCH_SIZE
    )
    config["DATAGEN_JDBC_READ_TIMEOUT_MS"] = os.environ.get(
        "DATAGEN_JDBC_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS
    )
    return config


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in PARQUET_REBASE_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def table_owner_and_name(target_user: str, table: str) -> tuple[str, str]:
    if "." in table:
        owner, table_name = table.split(".", 1)
        return owner.upper(), table_name.upper()
    return target_user.upper(), table.upper()


def dbtable_name(target_user: str, table: str) -> str:
    return table if "." in table else f"{target_user}.{table}"


def build_load_path(config: dict[str, str], table: str) -> str:
    path_parts = [config["DATAGEN_LOAD_BASE_URI"]]
    if config["DATAGEN_LOAD_PREFIX"]:
        path_parts.append(config["DATAGEN_LOAD_PREFIX"])
    path_parts.append(table)
    return "/".join(path_parts)


def pk_cols_for(specs: dict, table: str) -> list[str]:
    entry = specs.get(table_path_name(table).upper(), {})
    return list(entry.get("pk_cols", []))


def is_static(specs: dict, table: str) -> bool:
    return bool(specs.get(table_path_name(table).upper(), {}).get("static"))


def resolve_load_tables(specs: dict, requested: list[str] | None) -> list[str]:
    if requested:
        result = []
        for table in requested:
            if is_static(specs, table):
                logger.info("Skipping static table %s", table)
                continue
            if table_path_name(table).upper() not in specs:
                logger.info("Table %s not in specs; treating as non-static", table)
            result.append(table)
    else:
        result = [name for name, entry in specs.items() if not entry.get("static")]

    if not result:
        logger.error("No tables to load")
        sys.exit(1)
    return result


def guard_applies(pk_cols: list[str], pk_is_numeric: bool) -> bool:
    return len(pk_cols) == 1 and pk_is_numeric


def build_existing_keys_query(
    owner: str, table_name: str, pk_col: str, lo, hi
) -> str:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    pk_col = validate_identifier(pk_col)
    for bound in (lo, hi):
        if isinstance(bound, bool) or not isinstance(bound, (int, float, Decimal)):
            raise ValueError(f"PK bound must be numeric: {bound!r}")
    return (
        f"(SELECT {pk_col} FROM {owner}.{table_name} "
        f"WHERE {pk_col} BETWEEN {lo} AND {hi}) DATAGEN_KEYS"
    )


def build_connection_properties(config: dict[str, str]) -> dict[str, str]:
    return {
        "url": config["DATAGEN_TARGET_JDBC_URL"],
        "user": config["DATAGEN_TARGET_DB_USER"],
        "password": config["DATAGEN_TARGET_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
        "oracle.jdbc.ReadTimeout": config["DATAGEN_JDBC_READ_TIMEOUT_MS"],
    }


def resolve_num_partitions(config: dict[str, str]) -> int:
    return int(config["DATAGEN_JDBC_NUM_PARTITIONS"])


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


def load_table(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    target_user: str,
    table: str,
) -> None:
    owner, table_name = table_owner_and_name(target_user, table)
    validate_identifier(owner)
    validate_identifier(table_name)
    dbtable = dbtable_name(target_user, table)
    input_path = build_load_path(config, table_path_name(table))
    num_partitions = resolve_num_partitions(config)
    batch_size = config["DATAGEN_JDBC_BATCH_SIZE"]

    df = spark.read.parquet(input_path).repartition(num_partitions)
    logger.info("Appending %s to %s in %d partitions", input_path, dbtable, num_partitions)
    (
        df.write.format("jdbc")
        .options(**properties)
        .option("dbtable", dbtable)
        .option("batchsize", batch_size)
        .option("isolationLevel", DEFAULT_ISOLATION_LEVEL)
        .mode("append")
        .save()
    )


def load_tables(
    spark: SparkSession,
    config: dict[str, str],
    tables: list[str],
    continue_on_error: bool,
) -> None:
    target_user = config["DATAGEN_TARGET_DB_USER"]
    properties = build_connection_properties(config)
    failures = []
    total = len(tables)
    run_started_at = time.perf_counter()
    logger.info(
        "Loading %d table(s): num_partitions=%s, batchsize=%s",
        total,
        config["DATAGEN_JDBC_NUM_PARTITIONS"],
        config["DATAGEN_JDBC_BATCH_SIZE"],
    )

    for index, table in enumerate(tables, start=1):
        try:
            started_at = time.perf_counter()
            logger.info("[%d/%d] Loading %s", index, total, table)
            load_table(
                spark=spark,
                properties=properties,
                config=config,
                target_user=target_user,
                table=table,
            )
            logger.info(
                "[%d/%d] Loaded %s in %.1fs",
                index,
                total,
                table,
                time.perf_counter() - started_at,
            )
        except Exception as exc:
            logger.exception("[%d/%d] Failed to load %s: %s", index, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise

    run_elapsed = time.perf_counter() - run_started_at
    logger.info(
        "Finished: %d/%d table(s) loaded in %.1fs", total - len(failures), total, run_elapsed
    )
    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)


def main() -> None:
    args = parse_arguments()
    tables = parse_tables(args.tables, args.tables_file)
    config = get_load_env()
    spark = create_spark_session("DataGenLoadTables")
    try:
        load_tables(
            spark,
            config,
            tables,
            continue_on_error=args.continue_on_error,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
