from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_DB_USER = "ADMIN"
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
        "--date",
        required=True,
        help="Run date in YYYYMMDD format, used in raw object paths.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Try remaining tables after a read/write failure, then exit non-zero if any failed.",
    )
    return parser.parse_args()


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
    return config


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()


def build_raw_path(config: dict[str, str], table: str, date: str) -> str:
    return f"{config['DATAGEN_RAW_BASE_URI']}/{table}/{date}_{table}.parquet"


def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def dbtable_name(source_user: str, table: str) -> str:
    return table if "." in table else f"{source_user}.{table}"


def save_tables(
    spark: SparkSession,
    config: dict[str, str],
    tables: list[str],
    date: str,
    continue_on_error: bool = False,
) -> None:
    source_user = config["DATAGEN_SOURCE_DB_USER"]
    properties = {
        "url": config["DATAGEN_SOURCE_JDBC_URL"],
        "user": source_user,
        "password": config["DATAGEN_SOURCE_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
    }
    failures = []

    for table in tables:
        output_table = table_path_name(table)
        output_path = build_raw_path(config, output_table, date)
        source_table = dbtable_name(source_user, table)
        try:
            logger.info("Reading %s", source_table)
            df = (
                spark.read.format("jdbc")
                .options(**properties)
                .option("dbtable", source_table)
                .load()
            )
            row_count = df.count()
            logger.info("Read %s rows from %s", row_count, source_table)

            logger.info("Saving %s to %s", source_table, output_path)
            df.write.mode("overwrite").parquet(output_path)
            logger.info("Saved %s rows from %s", row_count, source_table)
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
        save_tables(spark, config, tables, args.date, args.continue_on_error)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
