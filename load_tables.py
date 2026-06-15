from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from contextlib import contextmanager
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
    parser.add_argument(
        "--no-manage-constraints",
        action="store_true",
        help="Do not disable/re-enable foreign keys; assume constraints are handled externally.",
    )
    parser.add_argument(
        "--validate-constraints",
        action="store_true",
        help="Re-enable foreign keys with ENABLE VALIDATE instead of ENABLE NOVALIDATE.",
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


def truncate_sql(owner: str, table_name: str) -> str:
    return f"TRUNCATE TABLE {validate_identifier(owner)}.{validate_identifier(table_name)}"


def disable_constraint_sql(owner: str, table_name: str, name: str) -> str:
    return (
        f"ALTER TABLE {validate_identifier(owner)}.{validate_identifier(table_name)} "
        f"DISABLE CONSTRAINT {validate_identifier(name)}"
    )


def enable_constraint_sql(owner: str, table_name: str, name: str, validate: bool) -> str:
    mode = "ENABLE VALIDATE" if validate else "ENABLE NOVALIDATE"
    return (
        f"ALTER TABLE {validate_identifier(owner)}.{validate_identifier(table_name)} "
        f"{mode} CONSTRAINT {validate_identifier(name)}"
    )


def build_constraint_discovery_query(owner: str, table_name: str) -> str:
    owner = validate_identifier(owner)
    table_name = validate_identifier(table_name)
    return (
        "SELECT c.owner, c.table_name, c.constraint_name "
        "FROM all_constraints c "
        "JOIN all_constraints p "
        "ON c.r_owner = p.owner AND c.r_constraint_name = p.constraint_name "
        "WHERE c.constraint_type = 'R' AND c.status = 'ENABLED' "
        f"AND p.owner = '{owner}' AND p.table_name = '{table_name}' "
        "UNION "
        "SELECT owner, table_name, constraint_name "
        "FROM all_constraints "
        "WHERE constraint_type = 'R' AND status = 'ENABLED' "
        f"AND owner = '{owner}' AND table_name = '{table_name}'"
    )


@contextmanager
def constraints_disabled(execute, constraints: list[tuple[str, str, str]], validate: bool):
    for owner, table_name, name in constraints:
        execute(disable_constraint_sql(owner, table_name, name))
    try:
        yield
    finally:
        for owner, table_name, name in constraints:
            execute(enable_constraint_sql(owner, table_name, name, validate))


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


def discover_constraints(
    spark: SparkSession, properties: dict[str, str], owner: str, table_name: str
) -> list[tuple[str, str, str]]:
    query = build_constraint_discovery_query(owner, table_name)
    rows = read_rows(spark, properties, query)
    return [(row[0], row[1], row[2]) for row in rows]
