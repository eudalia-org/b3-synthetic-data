from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import namedtuple
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

# Self-referential FK columns set to NULL on insert: a synthetic row can point at
# another not-yet-inserted row in the same table (ORA-02291), and the values are
# stale post-PK-shift anyway. The columns are nullable; left NULL, not back-filled.
NULL_ON_INSERT = {
    "INSTRUMENTO_FINANCEIRO": ["NUM_IF_ORIGEM", "NUM_IF_PERTENCE"],
}

Violation = namedtuple("Violation", ["table", "check", "columns", "detail"])


def capacity_from_precision_scale(precision, scale):
    """Largest magnitude an Oracle NUMBER(precision, scale) holds, as an exact
    Decimal. The bound is the true max magnitude (10^precision - 1) / 10^scale,
    NOT just the integer part: NUMBER(2,0) -> Decimal('99');
    NUMBER(4,2) -> Decimal('99.99'); NUMBER(2,2) -> Decimal('0.99'). NULL
    precision (unconstrained NUMBER) -> None (no limit)."""
    if precision is None:
        return None
    p = int(precision)
    s = int(scale or 0)
    return Decimal(10 ** p - 1) / (Decimal(10) ** s)


def column_alignment_violations(table, synthetic_cols, target_cols):
    """synthetic_cols: set of UPPER names. target_cols: {COL: {nullable, has_default}}."""
    out = []
    for col in sorted(synthetic_cols - set(target_cols)):
        out.append(Violation(table, "column_alignment", col, "column not in target table"))
    required = {
        c for c, m in target_cols.items() if not m["nullable"] and not m["has_default"]
    }
    for col in sorted(required - synthetic_cols):
        out.append(Violation(
            table, "column_alignment", col, "required NOT NULL column missing from synthetic"))
    return out


def numeric_domain_violations(table, profile, target_cols):
    """profile: {COL: {max, min}} (numeric cols). target_cols: {COL: {precision, scale}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None:
            continue
        cap = capacity_from_precision_scale(meta.get("precision"), meta.get("scale"))
        if cap is None:
            continue
        if prof["max"] is not None and prof["max"] > cap:
            out.append(Violation(table, "numeric_domain", col,
                                 f"max {prof['max']} > capacity {cap}"))
        if prof["min"] is not None and prof["min"] < -cap:
            out.append(Violation(table, "numeric_domain", col,
                                 f"min {prof['min']} < -capacity {-cap}"))
    return out


def string_length_violations(table, profile, target_cols):
    """profile: {COL: {max_octet}}. target_cols: {COL: {data_length}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None or meta.get("data_length") is None:
            continue
        if prof.get("max_octet") is not None and prof["max_octet"] > meta["data_length"]:
            out.append(Violation(table, "string_length", col,
                                 f"max byte length {prof['max_octet']} > {meta['data_length']}"))
    return out


def not_null_violations(table, profile, target_cols):
    """profile: {COL: {null_count}}. target_cols: {COL: {nullable}}."""
    out = []
    for col, prof in profile.items():
        meta = target_cols.get(col)
        if meta is None or meta.get("nullable", True):
            continue
        if prof.get("null_count", 0) > 0:
            out.append(Violation(table, "not_null", col,
                                 f"{prof['null_count']} NULL(s) in NOT NULL column"))
    return out


def uniqueness_violations(table, constraints, total_count, distinct_counts,
                          prod_collision_counts, nonnull_counts=None):
    """constraints: list of (name, tuple(cols)). distinct_counts/prod_collision_counts/
    nonnull_counts keyed by tuple(cols). Flags internal dups and production
    collisions (>0 synthetic keys already in production).

    Internal dups compare distinct against the count of rows whose key is fully
    non-null (nonnull_counts), NOT total_count: Oracle UNIQUE permits unlimited
    NULL-bearing rows, so countDistinct (which drops NULL-key rows) must be
    measured against the same non-null population. Falls back to total_count when
    a non-null count isn't supplied (e.g. NOT NULL PKs, where they're equal)."""
    nonnull_counts = nonnull_counts or {}
    out = []
    for _name, cols in constraints:
        label = ",".join(cols)
        distinct = distinct_counts.get(cols)
        comparand = nonnull_counts.get(cols, total_count)
        if distinct is not None and distinct < comparand:
            out.append(Violation(table, "uniqueness_internal", label,
                                 f"{comparand - distinct} duplicate key(s) within synthetic"))
        collisions = prod_collision_counts.get(cols, 0)
        if collisions > 0:
            out.append(Violation(table, "uniqueness_vs_production", label,
                                 f"{collisions} synthetic key(s) already in production"))
    return out


def fk_to_static_violations(table, orphan_counts):
    """orphan_counts: {(tuple(cols), parent_table): count}."""
    out = []
    for (cols, parent), count in orphan_counts.items():
        if count > 0:
            out.append(Violation(table, "fk_to_static", ",".join(cols),
                                 f"{count} value(s) not present in static parent {parent}"))
    return out


def validate_table(table, synthetic_cols, profile, target_cols, constraints,
                   total_count, distinct_counts, prod_collision_counts, fk_orphan_counts,
                   nonnull_counts=None):
    """Run all six checks for one table; return the concatenated violations.
    `profile` is the per-column dict (max/min/max_octet/null_count); the numeric
    and string checks read the columns relevant to them."""
    violations = []
    violations += column_alignment_violations(table, synthetic_cols, target_cols)
    violations += numeric_domain_violations(table, profile, target_cols)
    violations += string_length_violations(table, profile, target_cols)
    violations += not_null_violations(table, profile, target_cols)
    violations += uniqueness_violations(
        table, constraints, total_count, distinct_counts, prod_collision_counts,
        nonnull_counts)
    violations += fk_to_static_violations(table, fk_orphan_counts)
    return violations


def format_violation_report(violations):
    """Group violations by table into a human-readable multi-line report."""
    if not violations:
        return "No violations."
    by_table = {}
    for v in violations:
        by_table.setdefault(v.table, []).append(v)
    lines = []
    for table in sorted(by_table):
        lines.append(f"{table}:")
        for v in by_table[table]:
            lines.append(f"  [{v.check}] {v.columns} — {v.detail}")
    return "\n".join(lines)


def profile_synthetic_table(df, target_cols, constraints):
    """One-pass profile of a synthetic table for the checks.
    target_cols: {COL: {is_numeric, is_string, nullable, ...}} (UPPER keys).
    Returns {total_count, columns: {COL: {max, min, max_octet, null_count}},
    distinct_counts: {tuple(cols): n}, nonnull_counts: {tuple(cols): n}}.
    nonnull_counts is the number of rows where ALL of a constraint's columns are
    non-null; the uniqueness check compares distinct against this (not total)
    because Oracle UNIQUE permits unlimited fully/partially-NULL rows."""
    from pyspark.sql import functions as F

    col_map = {c.upper(): c for c in df.columns}
    aggs = [F.count(F.lit(1)).alias("__total")]
    present = {}  # upper -> actual
    for up, actual in col_map.items():
        meta = target_cols.get(up)
        if meta is None:
            continue
        present[up] = actual
        if meta.get("is_numeric"):
            aggs.append(F.max(actual).alias(f"__max__{up}"))
            aggs.append(F.min(actual).alias(f"__min__{up}"))
        if meta.get("is_string"):
            aggs.append(F.max(F.octet_length(F.col(actual))).alias(f"__oct__{up}"))
        aggs.append(
            F.count(F.when(F.col(actual).isNull(), F.lit(1))).alias(f"__null__{up}"))
    # distinct + non-null-row count per constraint whose columns are all present
    constraint_keys = []
    for _name, cols in constraints:
        if all(c.upper() in present for c in cols):
            actuals = [present[c.upper()] for c in cols]
            key = "_".join(c.upper() for c in cols)
            dist_alias = "__dist__" + key
            nn_alias = "__nn__" + key
            aggs.append(F.countDistinct(*[F.col(a) for a in actuals]).alias(dist_alias))
            all_present = F.lit(True)
            for a in actuals:
                all_present = all_present & F.col(a).isNotNull()
            aggs.append(F.count(F.when(all_present, F.lit(1))).alias(nn_alias))
            constraint_keys.append((tuple(c.upper() for c in cols), dist_alias, nn_alias))

    row = df.agg(*aggs).first()
    columns = {}
    for up in present:
        meta = target_cols[up]
        columns[up] = {
            "max": row[f"__max__{up}"] if meta.get("is_numeric") else None,
            "min": row[f"__min__{up}"] if meta.get("is_numeric") else None,
            "max_octet": row[f"__oct__{up}"] if meta.get("is_string") else None,
            "null_count": row[f"__null__{up}"],
        }
    distinct_counts = {cols: row[dist_alias] for cols, dist_alias, _nn in constraint_keys}
    nonnull_counts = {cols: row[nn_alias] for cols, _dist, nn_alias in constraint_keys}
    return {"total_count": row["__total"], "columns": columns,
            "distinct_counts": distinct_counts, "nonnull_counts": nonnull_counts}


def read_target_columns(spark, properties, owner, tables):
    """{TABLE: {COL: {data_type, precision, scale, data_length,
    nullable(bool), has_default(bool), is_numeric, is_string}}} from ALL_TAB_COLUMNS."""
    owner = validate_identifier(owner)
    names = ",".join(f"'{validate_identifier(table_path_name(t))}'" for t in tables)
    # NVL2(DATA_DEFAULT,...) instead of selecting DATA_DEFAULT itself: DATA_DEFAULT
    # is an Oracle LONG column, and reading LONG via Spark JDBC alongside other
    # columns is flaky (stream-already-closed). We only need the boolean.
    rows = read_rows(spark, properties,
                     "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_PRECISION, "
                     "DATA_SCALE, DATA_LENGTH, NULLABLE, "
                     "NVL2(DATA_DEFAULT, 'Y', 'N') AS HAS_DEFAULT "
                     f"FROM ALL_TAB_COLUMNS WHERE OWNER='{owner}' "
                     f"AND TABLE_NAME IN ({names})")
    out = {}
    numeric = {"NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "INTEGER"}
    # Only single-byte-charset types are length-checked: the string-length check
    # compares octet_length against DATA_LENGTH (bytes). NVARCHAR2/NCHAR store in
    # the national charset (often 2 bytes/char), so a byte-vs-DATA_LENGTH compare
    # would be apples-to-oranges; they're intentionally excluded (not length-checked).
    string = {"VARCHAR2", "CHAR"}
    for r in rows:
        dt = r["DATA_TYPE"]
        out.setdefault(r["TABLE_NAME"], {})[r["COLUMN_NAME"]] = {
            "data_type": dt,
            "precision": r["DATA_PRECISION"],
            "scale": r["DATA_SCALE"],
            "data_length": r["DATA_LENGTH"],
            "nullable": r["NULLABLE"] == "Y",
            "has_default": r["HAS_DEFAULT"] == "Y",
            "is_numeric": dt in numeric,
            "is_string": dt in string,
        }
    return out


def read_target_constraints(spark, properties, owner, tables):
    """{TABLE: [(constraint_name, (COL,...)), ...]} for P and U constraints,
    columns ordered by POSITION."""
    owner = validate_identifier(owner)
    names = ",".join(f"'{validate_identifier(table_path_name(t))}'" for t in tables)
    rows = read_rows(spark, properties,
                     "SELECT c.TABLE_NAME, c.CONSTRAINT_NAME, acc.COLUMN_NAME, acc.POSITION "
                     "FROM ALL_CONSTRAINTS c JOIN ALL_CONS_COLUMNS acc "
                     "ON c.OWNER=acc.OWNER AND c.CONSTRAINT_NAME=acc.CONSTRAINT_NAME "
                     f"WHERE c.OWNER='{owner}' AND c.CONSTRAINT_TYPE IN ('P','U') "
                     f"AND c.TABLE_NAME IN ({names})")
    grouped = {}
    for r in rows:
        grouped.setdefault((r["TABLE_NAME"], r["CONSTRAINT_NAME"]), []).append(
            (int(r["POSITION"]), r["COLUMN_NAME"]))
    out = {}
    for (table, name), cols in grouped.items():
        ordered = tuple(c for _pos, c in sorted(cols))
        out.setdefault(table, []).append((name, ordered))
    return out


def _count_key_collisions(syn_keys_df, existing_keys_df, cols):
    """Count synthetic keys (already deduped + non-null) that exist in production.
    Both DataFrames must share the synthetic column names in `cols`. Pure
    DataFrame-level join core — no I/O, so it's unit-testable with local frames."""
    return syn_keys_df.join(existing_keys_df, on=list(cols), how="inner").count()


def _count_orphans(syn_df, parent_keys_df, cols):
    """Count synthetic FK rows whose (non-null) key is absent from the parent.
    Rows with any NULL key column are excluded (an unenforced FK). Both
    DataFrames must share the synthetic column names in `cols`. Pure
    DataFrame-level join core — no I/O, so it's unit-testable with local frames."""
    syn = syn_df.select(*cols).dropna()
    return syn.join(parent_keys_df, on=list(cols), how="left_anti").count()


def count_prod_collisions(spark, properties, config, owner, table_name, df, constraints):
    """{tuple(cols): count of synthetic keys already in production}. Range-bounds
    the production read on a single numeric key (reusing read_existing_keys);
    composite/non-numeric keys read distinct production columns directly. The
    join itself is delegated to _count_key_collisions (unit-tested)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType

    col_map = {c.upper(): c for c in df.columns}
    out = {}
    for _name, cols in constraints:
        actuals = [col_map.get(c.upper()) for c in cols]
        if any(a is None for a in actuals):
            continue
        syn_keys = df.select(*actuals).dropna().dropDuplicates()
        if len(cols) == 1 and isinstance(df.schema[actuals[0]].dataType, NumericType):
            bounds = df.agg(F.min(actuals[0]), F.max(actuals[0])).first()
            lo, hi = bounds[0], bounds[1]
            if lo is None:
                out[tuple(c.upper() for c in cols)] = 0
                continue
            lo, hi = normalize_pk_bound(lo), normalize_pk_bound(hi)
            existing = read_existing_keys(
                spark, properties, resolve_num_partitions(config),
                owner, table_name, actuals[0], lo, hi)
            existing = existing.withColumnRenamed(existing.columns[0], actuals[0])
        else:
            col_list = ",".join(validate_identifier(c) for c in cols)
            q = (f"(SELECT {col_list} FROM {validate_identifier(owner)}."
                 f"{validate_identifier(table_name)}) DATAGEN_UK")
            existing = (spark.read.format("jdbc").options(**properties)
                        .option("dbtable", q).load())
            for syn_col, prod_col in zip(actuals, existing.columns):
                existing = existing.withColumnRenamed(prod_col, syn_col)
            # A duplicated production tuple would otherwise inflate the inner-join
            # count past the number of colliding synthetic keys.
            existing = existing.dropDuplicates()
        out[tuple(c.upper() for c in cols)] = _count_key_collisions(
            syn_keys, existing, actuals)
    return out


def count_fk_static_orphans(spark, properties, config, specs, df, table, owner_for):
    """{(tuple(cols), parent): count of synthetic FK values absent from the static
    parent's key}. Only FKs whose parent is static (is_static) are checked. The
    join itself is delegated to _count_orphans (unit-tested)."""
    norm = table_path_name(table).upper()
    entry = specs.get(norm, {})
    col_map = {c.upper(): c for c in df.columns}
    out = {}
    for fk in _fk_list(entry):
        parent = (fk.get("parent_table") or "").upper()
        if not parent or not is_static(specs, parent):
            continue
        cols = [c.upper() for c in fk.get("columns", [])]
        pcols = [c.upper() for c in (fk.get("parent_columns") or [])]
        actuals = [col_map.get(c) for c in cols]
        if not cols or len(cols) != len(pcols) or any(a is None for a in actuals):
            continue
        p_owner, p_name = owner_for(parent)
        col_list = ",".join(validate_identifier(c) for c in pcols)
        q = (f"(SELECT {col_list} FROM {validate_identifier(p_owner)}."
             f"{validate_identifier(p_name)}) DATAGEN_FK")
        parent_keys = spark.read.format("jdbc").options(**properties).option(
            "dbtable", q).load()
        for a, pc in zip(actuals, parent_keys.columns):
            parent_keys = parent_keys.withColumnRenamed(pc, a)
        out[(tuple(cols), parent)] = _count_orphans(df, parent_keys, actuals)
    return out


def validate_load(spark, properties, config, specs, target_schema, tables, limit):
    """Read-only pre-flight. Returns a flat list of Violations across all tables."""
    owner_for = lambda t: table_owner_and_name(target_schema, t)  # noqa: E731
    if limit is not None:
        logger.warning(
            "Validation under --limit profiles df.limit(%d), which Spark samples "
            "nondeterministically; the inserted sample may differ. Use a full run "
            "(no --limit) for an authoritative pre-flight.", limit)
    target_columns = read_target_columns(spark, properties, target_schema, tables)
    target_constraints = read_target_constraints(spark, properties, target_schema, tables)
    violations = []
    for table in tables:
        owner, table_name = owner_for(table)
        tcols = target_columns.get(table_name)
        if tcols is None:
            violations.append(Violation(table, "column_alignment", "*",
                                        f"target table {owner}.{table_name} not found"))
            continue
        df = spark.read.parquet(build_load_path(config, table_path_name(table)))
        if limit is not None:
            df = df.limit(limit)
        # Cached: scanned three times below (profile + collisions + fk-orphans).
        # Multi-TB context — avoid repeated full Parquet scans.
        df = df.cache()
        try:
            constraints = target_constraints.get(table_name, [])
            prof = profile_synthetic_table(df, tcols, constraints)
            prod_collisions = count_prod_collisions(
                spark, properties, config, owner, table_name, df, constraints)
            fk_orphans = count_fk_static_orphans(
                spark, properties, config, specs, df, table, owner_for)
            violations += validate_table(
                table=table,
                synthetic_cols={c.upper() for c in df.columns},
                profile=prof["columns"],
                target_cols=tcols,
                constraints=constraints,
                total_count=prof["total_count"],
                distinct_counts=prof["distinct_counts"],
                prod_collision_counts=prod_collisions,
                fk_orphan_counts=fk_orphans,
                nonnull_counts=prof["nonnull_counts"],
            )
        finally:
            df.unpersist()
    return violations


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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the synthetic data against the target schema and exit; insert nothing.",
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


def _fk_list(cfg: dict) -> list[dict]:
    """A spec's foreign keys, accepting either the `foreign_keys` or `fks` key."""
    fks = cfg.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = cfg.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def topo_sort_for_load(specs: dict, tables: list[str]) -> list[str]:
    """Sort a load list so every parent precedes its children — stably.

    Input order is preserved except where a foreign key forces a parent ahead of
    its child: independent tables (no FK relationship, or absent from specs and
    thus carrying no FK metadata) keep their original relative order. Only the
    parents that are themselves in the load set are considered, so an FK to a
    table that isn't being loaded (e.g. a static/code parent) imposes no
    constraint. Self-references are ignored; a dependency cycle is broken by
    emitting the rest in input order, so every input table is returned once.

    Assumes ``tables`` is already de-duplicated (the callers — ``parse_tables``
    and the unique ``specs`` keys — guarantee this); two strings normalizing to
    the same table would otherwise both be emitted.
    """
    norm = {t: table_path_name(t).upper() for t in tables}
    present = set(norm.values())
    parents: dict[str, set[str]] = {}
    for t in tables:
        deps: set[str] = set()
        for fk in _fk_list(specs.get(norm[t], {})):
            parent = (fk.get("parent_table") or "").upper()
            if parent and parent != norm[t] and parent in present:
                deps.add(parent)
        parents[t] = deps

    result: list[str] = []
    emitted: set[str] = set()
    remaining = list(tables)
    while remaining:
        for i, t in enumerate(remaining):
            if parents[t] <= emitted:
                result.append(t)
                emitted.add(norm[t])
                remaining.pop(i)
                break
        else:  # only cyclic tables left -> emit them in input order
            result.extend(remaining)
            break
    return result


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
    # Load parents before children so synthetic FK rows never reference a
    # synthetic parent that hasn't been appended yet (ORA-02291).
    return topo_sort_for_load(specs, result)


def guard_applies(pk_cols: list[str], pk_is_numeric: bool) -> bool:
    return len(pk_cols) == 1 and pk_is_numeric


def normalize_pk_bound(value):
    """Integer-format an integral PK bound.

    Synthetic (and source) ID columns can carry a Decimal scale, e.g.
    Decimal('8044070030.000000000'). Spark's JDBC numeric partitioning parses
    lowerBound/upperBound with the column's integral type, so a fractional
    string like "8044070030.000000000" raises NumberFormatException. IDs are
    whole numbers, so collapse integral Decimal/float bounds to int; leave
    genuinely fractional values untouched.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    return value


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
    # Collapse integral Decimal/float bounds to int so Spark's JDBC partition
    # bounds don't choke on a fractional string (NumberFormatException).
    lo, hi = normalize_pk_bound(lo), normalize_pk_bound(hi)

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


def null_self_ref_columns(df, table, null_map):
    """Set a table's self-referential FK columns to NULL before insert.

    They are nullable and left NULL (not back-filled). A synthetic row may
    reference another not-yet-inserted row in the same table (ORA-02291). dtype is
    preserved so the insert schema is unchanged. No-op for tables/columns absent
    from `null_map` or from `df`. Case-insensitive column match.
    """
    from pyspark.sql import functions as F

    cols = null_map.get(table_path_name(table).upper(), [])
    actual = {c.upper(): c for c in df.columns}
    nulled = []
    for c in cols:
        real = actual.get(c.upper())
        if real is not None:
            df = df.withColumn(real, F.lit(None).cast(df.schema[real].dataType))
            nulled.append(real)
    if nulled:
        logger.info("%s: nulled self-ref FK column(s) on insert: %s",
                    table_path_name(table).upper(), ", ".join(nulled))
    return df


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

    df = null_self_ref_columns(df, table, NULL_ON_INSERT)

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
        target_schema = config["DATAGEN_TARGET_SCHEMA"]
        properties = build_connection_properties(config)

        logger.info("Pre-flight validation against %s ...", target_schema)
        violations = validate_load(
            spark, properties, config, specs, target_schema, tables, args.limit)
        if violations:
            logger.error("Pre-flight FAILED (%d violation(s)) — nothing inserted:\n%s",
                         len(violations), format_violation_report(violations))
            sys.exit(1)
        logger.info("Pre-flight validation passed.")
        if args.dry_run:
            logger.info("Dry run: validation only, nothing loaded.")
            return

        run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        entries = capture_manifest_entries(
            spark, properties, config, specs, target_schema, tables)
        manifest = build_manifest(
            run_id, datetime.now(timezone.utc).isoformat(), target_schema, entries)
        path = write_manifest(spark, config, run_id, manifest)
        logger.info("Load run_id=%s; manifest written to %s", run_id, path)

        load_tables(spark, config, specs, tables,
                    continue_on_error=args.continue_on_error, limit=args.limit)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
