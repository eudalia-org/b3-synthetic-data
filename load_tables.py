from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
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
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument(
        "--tables",
        help="Comma-separated table list. If omitted, all non-static tables in --specs load.",
    )
    source.add_argument(
        "--tables-file",
        help="Local text file with one table per line. Blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--specs",
        default="specs.json",
        help="Path to specs JSON (static tables are skipped; pk_cols drive the dup guard).",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        help="Append at most this many rows per table (sample load into the real target).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Try remaining tables after a failure, then exit non-zero if any failed.",
    )
    parser.add_argument(
        "--run-id",
        help="Run id for the rollback manifest. Defaults to a UTC timestamp.",
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
    # Owner of unqualified target tables. Defaults to the connection user, but can
    # differ when connecting as a privileged user (e.g. ADMIN) to write tables
    # owned by another schema (e.g. CETIP).
    config["DATAGEN_TARGET_SCHEMA"] = os.environ.get(
        "DATAGEN_TARGET_SCHEMA", config["DATAGEN_TARGET_DB_USER"]
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


def load_specs(spark: SparkSession, path: str) -> dict:
    try:
        text = "\n".join(spark.sparkContext.textFile(path).collect())
        return json.loads(text)
    except Exception as exc:
        logger.error("Failed to read specs %s: %s", path, exc)
        sys.exit(1)


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


def read_existing_keys(
    spark: SparkSession,
    properties: dict[str, str],
    num_partitions: int,
    owner: str,
    table_name: str,
    pk_col: str,
    lo,
    hi,
):
    query = build_existing_keys_query(owner, table_name, pk_col, lo, hi)
    return (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", query)
        .option("partitionColumn", validate_identifier(pk_col))
        .option("lowerBound", str(lo))
        .option("upperBound", str(hi))
        .option("numPartitions", num_partitions)
        .load()
    )


def apply_pk_guard(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    df,
    specs: dict,
    owner: str,
    table_name: str,
    table: str,
    index: int,
    total: int,
):
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    pk_cols = pk_cols_for(specs, table)
    col_map = {c.upper(): c for c in df.columns}
    pk_actual = col_map.get(pk_cols[0].upper()) if len(pk_cols) == 1 else None
    pk_is_numeric = bool(
        pk_actual is not None
        and isinstance(df.schema[pk_actual].dataType, NumericType)
    )

    if not guard_applies(pk_cols, pk_is_numeric) or pk_actual is None:
        logger.info(
            "[%d/%d] %s: no PK guard (pk_cols=%s) -> appending all rows",
            index, total, table, pk_cols,
        )
        return df, 0

    bounds = df.agg(F.min(pk_actual), F.max(pk_actual)).first()
    lo, hi = bounds[0], bounds[1]
    if lo is None:  # empty DataFrame
        return df, 0

    existing = read_existing_keys(
        spark, properties, resolve_num_partitions(config),
        owner, table_name, pk_actual, lo, hi,
    )
    existing = existing.withColumnRenamed(existing.columns[0], pk_actual)
    existing_count = existing.count()
    if existing_count == 0:
        logger.info(
            "[%d/%d] %s: 0 existing keys in PK range [%s, %s] -> appending all rows",
            index, total, table, lo, hi,
        )
        return df, 0

    to_append = df.join(existing, on=pk_actual, how="left_anti")
    logger.info(
        "[%d/%d] %s: %s existing key(s) in PK range [%s, %s] -> skipping already-loaded",
        index, total, table, f"{existing_count:,}", lo, hi,
    )
    return to_append, existing_count


def load_table(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    specs: dict,
    target_schema: str,
    table: str,
    index: int,
    total: int,
    limit: int | None,
) -> int:
    owner, table_name = table_owner_and_name(target_schema, table)
    validate_identifier(owner)
    validate_identifier(table_name)
    dbtable = dbtable_name(target_schema, table)
    input_path = build_load_path(config, table_path_name(table))
    num_partitions = resolve_num_partitions(config)
    batch_size = config["DATAGEN_JDBC_BATCH_SIZE"]

    logger.info("[%d/%d] %s: reading %s", index, total, table, input_path)
    df = spark.read.parquet(input_path)
    if limit is not None:
        df = df.limit(limit)
    df = df.repartition(num_partitions)

    df, _ = apply_pk_guard(
        spark, properties, config, df, specs, owner, table_name, table, index, total
    )

    appended = df.count()
    limit_note = f" (limit {limit})" if limit is not None else ""
    logger.info(
        "[%d/%d] %s: appending %s rows%s to %s in %d partitions",
        index,
        total,
        table,
        f"{appended:,}",
        limit_note,
        dbtable,
        num_partitions,
    )
    (
        df.write.format("jdbc")
        .options(**properties)
        .option("dbtable", dbtable)
        .option("batchsize", batch_size)
        .option("isolationLevel", DEFAULT_ISOLATION_LEVEL)
        .mode("append")
        .save()
    )
    return appended


def load_tables(
    spark: SparkSession,
    config: dict[str, str],
    specs: dict,
    tables: list[str],
    continue_on_error: bool,
    limit: int | None,
) -> None:
    target_schema = config["DATAGEN_TARGET_SCHEMA"]
    properties = build_connection_properties(config)
    failures = []
    appended_total = 0
    total = len(tables)
    run_started_at = time.perf_counter()
    logger.info(
        "Load run: mode=APPEND, partitions=%s, batchsize=%s, limit=%s",
        config["DATAGEN_JDBC_NUM_PARTITIONS"],
        config["DATAGEN_JDBC_BATCH_SIZE"],
        limit if limit is not None else "none",
    )
    logger.info("Resolved %d table(s) to load", total)

    for index, table in enumerate(tables, start=1):
        try:
            started_at = time.perf_counter()
            appended = load_table(
                spark=spark,
                properties=properties,
                config=config,
                specs=specs,
                target_schema=target_schema,
                table=table,
                index=index,
                total=total,
                limit=limit,
            )
            appended_total += appended
            logger.info(
                "[%d/%d] %s: appended %s rows in %.1fs",
                index,
                total,
                table,
                f"{appended:,}",
                time.perf_counter() - started_at,
            )
        except Exception as exc:
            logger.exception("[%d/%d] %s: FAILED: %s", index, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise

    run_elapsed = time.perf_counter() - run_started_at
    logger.info(
        "Finished: loaded %d/%d table(s), %s rows in %.1fs",
        total - len(failures),
        total,
        f"{appended_total:,}",
        run_elapsed,
    )
    if failures:
        logger.error("Failed tables: %s", ", ".join(failures))
        sys.exit(1)


def manifest_path(config: dict[str, str], run_id: str) -> str:
    return f"{config['DATAGEN_LOAD_BASE_URI']}/_load_manifests/{run_id}"


def build_manifest(run_id: str, created: str, target_schema: str, entries: list[dict]) -> dict:
    return {
        "run_id": run_id,
        "created_utc": created,
        "target_schema": target_schema,
        "tables": entries,
    }


def capture_manifest_entries(
    spark: SparkSession,
    properties: dict[str, str],
    config: dict[str, str],
    specs: dict,
    target_schema: str,
    tables: list[str],
) -> list[dict]:
    from pyspark.sql.types import NumericType

    entries = []
    for table in tables:
        owner, table_name = table_owner_and_name(target_schema, table)
        pk_cols = pk_cols_for(specs, table)
        pk_col, max_before, rollbackable = None, None, False
        if len(pk_cols) == 1:
            schema = spark.read.parquet(
                build_load_path(config, table_path_name(table))
            ).schema
            col_map = {f.name.upper(): f for f in schema.fields}
            field = col_map.get(pk_cols[0].upper())
            if field is not None and isinstance(field.dataType, NumericType):
                rollbackable = True
                pk_col = validate_identifier(pk_cols[0])
                rows = read_rows(
                    spark,
                    properties,
                    f"SELECT MAX({pk_col}) AS M "
                    f"FROM {validate_identifier(owner)}.{validate_identifier(table_name)}",
                )
                max_before = int(rows[0][0]) if rows and rows[0][0] is not None else None
        entries.append(
            {
                "table": table,
                "owner": owner,
                "name": table_name,
                "pk_col": pk_col,
                "max_pk_before": max_before,
                "rollbackable": rollbackable,
            }
        )
    return entries


def write_manifest(spark: SparkSession, config: dict[str, str], run_id: str, manifest: dict) -> str:
    path = manifest_path(config, run_id)
    spark.sparkContext.parallelize([json.dumps(manifest)], 1).saveAsTextFile(path)
    return path


def main() -> None:
    args = parse_arguments()
    config = get_load_env()
    spark = create_spark_session("DataGenLoadTables")
    try:
        specs = load_specs(spark, args.specs)
        requested = (
            parse_tables(args.tables, args.tables_file)
            if (args.tables or args.tables_file)
            else None
        )
        tables = resolve_load_tables(specs, requested)

        run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        target_schema = config["DATAGEN_TARGET_SCHEMA"]
        properties = build_connection_properties(config)
        entries = capture_manifest_entries(
            spark, properties, config, specs, target_schema, tables
        )
        manifest = build_manifest(
            run_id, datetime.now(timezone.utc).isoformat(), target_schema, entries
        )
        path = write_manifest(spark, config, run_id, manifest)
        logger.info("Load run_id=%s; manifest written to %s", run_id, path)

        load_tables(
            spark,
            config,
            specs,
            tables,
            continue_on_error=args.continue_on_error,
            limit=args.limit,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
