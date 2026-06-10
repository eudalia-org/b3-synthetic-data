from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_DB_USER = "ADMIN"
DEFAULT_FETCH_SIZE = "50000"
DEFAULT_NUM_PARTITIONS = "32"
DEFAULT_ORACLE_FETCH_OPTIONS = {
    "oracle.jdbc.useFetchSizeWithLongColumn": "true",
    "defaultRowPrefetch": DEFAULT_FETCH_SIZE,
}
PARQUET_REBASE_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
}
REQUIRED_ENV_VARS = (
    "DATAGEN_SOURCE_JDBC_URL",
    "DATAGEN_SOURCE_DB_PASSWORD",
    "DATAGEN_RAW_BASE_URI",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read source Oracle tables from a list and save each as raw Parquet."
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
        help="Try remaining tables after a read/write failure, then exit non-zero if any failed.",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        help=(
            "Read at most this many rows from each table and write to a *_limit_<N>.parquet "
            "path for runtime testing."
        ),
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


def get_extract_env() -> dict[str, str]:
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

    config["DATAGEN_SOURCE_DB_USER"] = os.environ.get(
        "DATAGEN_SOURCE_DB_USER", DEFAULT_SOURCE_DB_USER
    )
    config["DATAGEN_JDBC_FETCH_SIZE"] = os.environ.get(
        "DATAGEN_JDBC_FETCH_SIZE", DEFAULT_FETCH_SIZE
    )
    config["DATAGEN_JDBC_NUM_PARTITIONS"] = os.environ.get(
        "DATAGEN_JDBC_NUM_PARTITIONS", DEFAULT_NUM_PARTITIONS
    )
    config["DATAGEN_JDBC_PARTITION_COLUMNS"] = os.environ.get(
        "DATAGEN_JDBC_PARTITION_COLUMNS", ""
    )
    config["DATAGEN_RAW_PREFIX"] = os.environ.get("DATAGEN_RAW_PREFIX", "").strip("/")
    return config


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in PARQUET_REBASE_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def build_raw_path(config: dict[str, str], table: str, limit: int | None = None) -> str:
    suffix = f"_limit_{limit}" if limit is not None else ""
    path_parts = [config["DATAGEN_RAW_BASE_URI"]]
    if config["DATAGEN_RAW_PREFIX"]:
        path_parts.append(config["DATAGEN_RAW_PREFIX"])
    path_parts.append(f"{table}{suffix}")
    return "/".join(path_parts)


def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def dbtable_name(source_user: str, table: str) -> str:
    return table if "." in table else f"{source_user}.{table}"


def limited_dbtable_name(source_table: str, limit: int | None) -> str:
    if limit is None:
        return source_table
    return f"(SELECT * FROM {source_table} FETCH FIRST {limit} ROWS ONLY) DATAGEN_LIMITED"


def table_owner_and_name(source_user: str, table: str) -> tuple[str, str]:
    if "." in table:
        owner, table_name = table.split(".", 1)
        return owner.upper(), table_name.upper()
    return source_user.upper(), table.upper()


def parse_partition_column_overrides(raw_overrides: str) -> dict[str, str]:
    overrides = {}
    for item in raw_overrides.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            logger.warning(
                "Ignoring invalid partition override %r; expected TABLE=COLUMN",
                item,
            )
            continue
        table, column = item.split("=", 1)
        table = table.strip().upper()
        column = column.strip().upper()
        if table and column:
            overrides[table] = column
    return overrides


def read_single_value(spark: SparkSession, properties: dict[str, str], query: str):
    rows = (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", f"({query}) DATAGEN_Q")
        .load()
        .take(1)
    )
    return rows[0] if rows else None


def get_numeric_bounds(
    spark: SparkSession,
    properties: dict[str, str],
    source_table: str,
    partition_column: str,
) -> tuple[str, str] | None:
    query = f"""
        SELECT MIN({partition_column}) AS lower_bound,
               MAX({partition_column}) AS upper_bound
        FROM {source_table}
        WHERE {partition_column} IS NOT NULL
    """
    row = read_single_value(spark, properties, query)
    if not row or row[0] is None or row[1] is None:
        return None
    if row[0] == row[1]:
        return None
    return str(row[0]), str(row[1])


def build_jdbc_reader(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    source_user: str,
    table: str,
    source_table: str,
):
    reader = (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", source_table)
        .option("fetchsize", config["DATAGEN_JDBC_FETCH_SIZE"])
    )
    owner, table_name = table_owner_and_name(source_user, table)
    overrides = parse_partition_column_overrides(
        config["DATAGEN_JDBC_PARTITION_COLUMNS"]
    )
    partition_column = overrides.get(f"{owner}.{table_name}") or overrides.get(table_name)

    if not partition_column:
        logger.info("Reading %s with one JDBC partition", source_table)
        return reader

    bounds = get_numeric_bounds(spark, properties, source_table, partition_column)
    if not bounds:
        logger.warning(
            "No bounds found for %s.%s; using one JDBC partition",
            source_table,
            partition_column,
        )
        return reader

    lower_bound, upper_bound = bounds
    num_partitions = config["DATAGEN_JDBC_NUM_PARTITIONS"]
    logger.info(
        "Reading %s in %s JDBC partitions on %s [%s, %s]",
        source_table,
        num_partitions,
        partition_column,
        lower_bound,
        upper_bound,
    )
    return (
        reader.option("partitionColumn", partition_column)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("numPartitions", num_partitions)
    )


def save_tables(
    spark: SparkSession,
    config: dict[str, str],
    tables: list[str],
    continue_on_error: bool = False,
    limit: int | None = None,
) -> None:
    source_user = config["DATAGEN_SOURCE_DB_USER"]
    properties = {
        "url": config["DATAGEN_SOURCE_JDBC_URL"],
        "user": source_user,
        "password": config["DATAGEN_SOURCE_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
        **DEFAULT_ORACLE_FETCH_OPTIONS,
    }
    failures = []

    for table in tables:
        output_table = table_path_name(table)
        output_path = build_raw_path(config, output_table, limit)
        source_table = dbtable_name(source_user, table)
        read_table = limited_dbtable_name(source_table, limit)
        try:
            started_at = time.perf_counter()
            if limit is None:
                logger.info("Reading %s", source_table)
            else:
                logger.info("Reading up to %s rows from %s", limit, source_table)
            df = build_jdbc_reader(
                spark=spark,
                properties=properties,
                config=config,
                source_user=source_user,
                table=table,
                source_table=read_table,
            ).load()

            logger.info("Saving %s to %s", source_table, output_path)
            df.write.mode("overwrite").parquet(output_path)
            elapsed_seconds = time.perf_counter() - started_at
            if limit is None:
                logger.info("Saved %s in %.1fs", source_table, elapsed_seconds)
            else:
                logger.info(
                    "Saved sample from %s in %.1fs (limit=%s)",
                    source_table,
                    elapsed_seconds,
                    limit,
                )
        except Exception as exc:
            logger.exception("Failed to save %s: %s", source_table, exc)
            failures.append(source_table)
            if not continue_on_error:
                raise

    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)


def main() -> None:
    args = parse_arguments()
    tables = parse_tables(args.tables, args.tables_file)
    config = get_extract_env()
    spark = create_spark_session("DataGenSaveTables")
    try:
        save_tables(spark, config, tables, args.continue_on_error, args.limit)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
