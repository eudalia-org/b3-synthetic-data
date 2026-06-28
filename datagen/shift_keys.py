"""Post-hoc uniform shift of generated PK/FK values in the synthetic output.

Adds a uniform +N to every generated (non-static) key, in place, preserving FK
integrity. See docs/plans/2026-06-28-shift-synthetic-keys-design.md.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import warnings
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datagen.engorda_tables import (
    _pk_capacity,
    create_spark_session,
    load_specs,
    read_parquet,
    synthetic_base_path,
    table_path_name,
    write_synthetic_table,
)
from datagen.save_tables import build_connection_properties, read_rows, read_single_value

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def compute_shift_columns(specs: dict) -> Dict[str, List[str]]:
    """Per-table list of key columns to shift by +N.

    A column (table, col) shifts iff it's an FK column whose parent is non-static,
    OR it's the PK of a non-static table and not an FK to a static parent
    (FK-to-static wins, keeping shared-key children matched to reference data).
    """
    static = {t for t, e in specs.items() if e.get("static")}

    fk_to_static = set()  # (table, col)
    for t, e in specs.items():
        for fk in e.get("foreign_keys", []) or []:
            if fk.get("parent_table") in static:
                for c in fk.get("columns", []) or []:
                    fk_to_static.add((t, c))

    shift: Dict[str, set] = {}
    for t, e in specs.items():
        cols: set = set()
        for fk in e.get("foreign_keys", []) or []:
            parent = fk.get("parent_table")
            if parent in specs and parent not in static:
                for c in fk.get("columns", []) or []:
                    cols.add(c)
        if t not in static:
            for pk in e.get("pk_cols", []) or []:
                if (t, pk) in fk_to_static:
                    warnings.warn(
                        f"{t}.{pk}: non-static PK that is also an FK to a static "
                        "parent; NOT shifting (kept matched to reference data).",
                        UserWarning, stacklevel=2,
                    )
                else:
                    cols.add(pk)
        if cols:
            shift[t] = sorted(cols)
    return shift


def shift_table(df: DataFrame, cols: List[str], offset: int) -> DataFrame:
    """Add `offset` to each column in `cols`, preserving its dtype (so the output
    schema is byte-identical). NULLs stay NULL."""
    dtypes = {f.name: f.dataType for f in df.schema.fields}
    for c in cols:
        df = df.withColumn(c, (F.col(c) + F.lit(offset)).cast(dtypes[c]))
    return df


def capacity_from_precision_scale(precision, scale):
    """Largest integer value for an Oracle NUMBER(precision, scale).
    NULL precision (unconstrained NUMBER) -> None (no limit)."""
    if precision is None:
        return None
    int_digits = int(precision) - int(scale or 0)
    return (10 ** int_digits) - 1 if int_digits > 0 else 0


def oracle_column_capacities(rows, shift: Dict[str, List[str]]) -> Dict[Tuple[str, str], int]:
    """Map (table, col) -> capacity from ALL_TAB_COLUMNS rows
    (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE), restricted to the
    shift set; columns with no precision (unconstrained) are omitted."""
    wanted = {(t, c) for t, cols in shift.items() for c in cols}
    out: Dict[Tuple[str, str], int] = {}
    for table, col, precision, scale in rows:
        if (table, col) not in wanted:
            continue
        cap = capacity_from_precision_scale(precision, scale)
        if cap is not None:
            out[(table, col)] = cap
    return out


def find_collisions(prod_max: Dict[str, int], synth_min: Dict[str, int],
                    offset: int) -> List[Tuple[str, int, int]]:
    """Flag (table, prod_max, synth_min+offset) where the shifted synthetic key
    range does NOT clear current production (synth_min + offset <= prod_max)."""
    flagged: List[Tuple[str, int, int]] = []
    for table, pmax in prod_max.items():
        smin = synth_min.get(table)
        if smin is None or pmax is None:
            continue
        if smin + offset <= pmax:
            flagged.append((table, int(pmax), int(smin) + offset))
    return flagged


def read_oracle_capacities(spark, props: dict, owner: str):
    """Thin: ALL_TAB_COLUMNS rows for `owner`. Returns list of
    (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE)."""
    query = (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE "
        f"FROM ALL_TAB_COLUMNS WHERE OWNER = '{owner}'"
    )
    return [(r["TABLE_NAME"], r["COLUMN_NAME"], r["DATA_PRECISION"], r["DATA_SCALE"])
            for r in read_rows(spark, props, query)]


def read_oracle_max(spark, props: dict, owner: str, table: str, col: str):
    """Thin: live MAX(col) from owner.table (index-fast). None if empty."""
    row = read_single_value(spark, props, f"SELECT MAX({col}) AS M FROM {owner}.{table}")
    return None if row is None or row["M"] is None else int(row["M"])


def check_overflow(
    spark: SparkSession,
    base: str,
    shift: Dict[str, List[str]],
    offset: int,
    capacity_override: Dict[Tuple[str, str], int] | None = None,
) -> List[Tuple[str, str, int, int, int]]:
    """Read-only. For each shiftable column, read max(col) (footer-fast via
    aggregatePushdown) and flag (table, col, max, max+offset, capacity) when
    max+offset exceeds the column's numeric domain. `capacity_override` (the
    authoritative live Oracle capacity) wins over the Parquet-schema
    `_pk_capacity`. Non-numeric / empty columns are skipped."""
    capacity_override = capacity_override or {}
    overflows: List[Tuple[str, str, int, int, int]] = []
    for table, cols in shift.items():
        path = f"{base}/{table_path_name(table)}"
        df = read_parquet(spark, path)
        for c in cols:
            cap = capacity_override.get((table, c))
            if cap is None:
                cap = _pk_capacity(spark, path, c)
            if cap is None:
                continue
            row = df.agg(F.max(F.col(c)).alias("m")).first()
            if row is None or row["m"] is None:
                continue
            mx = int(row["m"])
            if mx + offset > cap:
                overflows.append((table, c, mx, mx + offset, cap))
    return overflows


SYNTHETIC_ENV = "DATAGEN_SYNTHETIC_BASE_URI"
SPECS_ENV = "DATAGEN_SPECS_URI"
CHECKPOINT_ENV = "DATAGEN_CHECKPOINT_URI"


def get_shift_env() -> dict:
    config: dict = {}
    missing = []
    for name in (SYNTHETIC_ENV, SPECS_ENV):
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Missing required env var(s): %s", ", ".join(missing))
        sys.exit(1)
    # Optional prefix — must match what engorda used so we hit the same paths.
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", "").strip("/")
    chk = os.environ.get(CHECKPOINT_ENV)
    if chk:
        config[CHECKPOINT_ENV] = chk.rstrip("/")
    # Optional Oracle pre-flight: URL + user + password travel as a set.
    for name in ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
                 "DATAGEN_SOURCE_DB_PASSWORD"):
        val = os.environ.get(name)
        if val:
            config[name] = val
    config["DATAGEN_ORACLE_OWNER"] = os.environ.get("DATAGEN_ORACLE_OWNER", "CETIP")
    # JDBC tuning defaults required by build_connection_properties.
    config.setdefault("DATAGEN_JDBC_FETCH_SIZE", os.environ.get("DATAGEN_JDBC_FETCH_SIZE", "1000"))
    config.setdefault("DATAGEN_JDBC_READ_TIMEOUT_MS",
                      os.environ.get("DATAGEN_JDBC_READ_TIMEOUT_MS", "60000"))
    config.setdefault("DATAGEN_JDBC_LOB_PREFETCH",
                      os.environ.get("DATAGEN_JDBC_LOB_PREFETCH", "262144"))
    return config


def oracle_props_or_none(config: dict):
    """Build JDBC connection properties iff the full Oracle env set is present."""
    if all(config.get(k) for k in
           ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
            "DATAGEN_SOURCE_DB_PASSWORD")):
        return build_connection_properties(config)
    return None


def parse_arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a uniform +N to generated PK/FK values in the synthetic output.")
    parser.add_argument("--offset", type=int, required=True,
                        help="Uniform amount added to every shifted key.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Pre-flight only: report shift columns + overflow, write nothing.")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Continue to remaining tables if one fails (default: stop).")
    return parser.parse_args(argv)


def print_deployment_summary(config: dict) -> None:
    base = config.get(SYNTHETIC_ENV, "oci://<bucket>@<namespace>/<prefix>")
    print(
        "\n=== Deployment ===\n"
        "Required env vars:\n"
        f"  {SYNTHETIC_ENV}   {base}\n"
        f"  {SPECS_ENV}            oci://<bucket>@<namespace>/specs.json\n"
        f"  {CHECKPOINT_ENV}       (optional) oci://<bucket>@<namespace>/_chk\n"
        "Optional (enables live Oracle datatype + collision pre-flight):\n"
        "  DATAGEN_SOURCE_JDBC_URL      jdbc:oracle:thin:@//host:port/service\n"
        "  DATAGEN_SOURCE_DB_USER       <user>\n"
        "  DATAGEN_SOURCE_DB_PASSWORD   <password>\n"
        "  DATAGEN_ORACLE_OWNER         CETIP   (default)\n"
        "\nData Flow application:\n"
        "  Main:       datagen/shift_keys.py\n"
        "  Arguments:  --offset <N> [--dry-run] [--continue-on-error]\n"
        "  Spark:      create_spark_session workload conf (aggregatePushdown, Kryo,\n"
        "              memoryOverheadFactor=0.2). No shuffle -> shuffle.partitions irrelevant.\n"
        "  Shape:      Driver    8 OCPU / 64 GB\n"
        "              Executors 4 x (16-32 OCPU / 128 GB)   # I/O-bound; scale OCPU\n"
        "  Network:    Oracle checks need Data Flow -> Oracle connectivity (ADB networking)\n"
    )


def apply_shift(
    spark: SparkSession,
    base: str,
    shift: Dict[str, List[str]],
    offset: int,
    *,
    continue_on_error: bool,
    reliable_checkpoint: bool,
) -> List[str]:
    """Phase 2: mutate each table in place. Per table: read -> shift -> checkpoint
    (sever lineage from the source files) -> scoped-delete + append to the same
    path. Returns the list of tables that failed."""
    tables = sorted(shift)
    total = len(tables)
    failures: List[str] = []
    for i, table in enumerate(tables, 1):
        path = f"{base}/{table_path_name(table)}"
        try:
            df = shift_table(read_parquet(spark, path), shift[table], offset)
            # Sever lineage: the next step deletes `path`, so a lazy read of the
            # source files would corrupt the output. Checkpoint replaces the plan
            # with a materialized RDD leaf.
            df = df.checkpoint(eager=True) if reliable_checkpoint else df.localCheckpoint(eager=True)
            write_synthetic_table(spark, df, path)
            logger.info("[%d/%d] shifted %s (%s)", i, total, table, ",".join(shift[table]))
        except Exception as exc:  # noqa: BLE001
            logger.exception("[%d/%d] FAILED shifting %s: %s", i, total, table, exc)
            failures.append(table)
            if not continue_on_error:
                raise
    return failures


def main() -> None:
    args = parse_arguments()
    config = get_shift_env()
    spark = create_spark_session("DataGenShiftKeys")
    try:
        reliable = bool(config.get(CHECKPOINT_ENV))
        if reliable:
            spark.sparkContext.setCheckpointDir(config[CHECKPOINT_ENV])

        specs = load_specs(spark, config[SPECS_ENV])
        shift = compute_shift_columns(specs)
        base = synthetic_base_path(config)  # base URI + optional prefix
        owner = config["DATAGEN_ORACLE_OWNER"]
        total_cols = sum(len(v) for v in shift.values())
        logger.info("Shifting %d column(s) across %d table(s) by +%d",
                    total_cols, len(shift), args.offset)

        # Live Oracle pre-flight when DB env is configured; else Parquet fallback.
        props = oracle_props_or_none(config)
        capacity_override: Dict[Tuple[str, str], int] = {}
        collisions: List[Tuple[str, int, int]] = []
        if props is not None:
            logger.info("Oracle pre-flight against OWNER=%s", owner)
            capacity_override = oracle_column_capacities(
                read_oracle_capacities(spark, props, owner), shift)
            prod_max: Dict[str, int] = {}
            synth_min: Dict[str, int] = {}
            for table in shift:
                pk_cols = specs[table].get("pk_cols", [])
                if specs[table].get("static") or not pk_cols:
                    continue
                pk = pk_cols[-1]
                if pk not in shift[table]:
                    continue  # PK kept fixed (FK-to-static) — no collision risk
                pmax = read_oracle_max(spark, props, owner, table, pk)
                if pmax is not None:
                    prod_max[table] = pmax
                path = f"{base}/{table_path_name(table)}"
                row = read_parquet(spark, path).agg(F.min(F.col(pk)).alias("m")).first()
                synth_min[table] = None if row is None or row["m"] is None else int(row["m"])
            collisions = find_collisions(prod_max, synth_min, args.offset)
        else:
            logger.warning("No Oracle env -> Parquet-schema capacity; production "
                           "COLLISION was NOT verified for offset %d.", args.offset)

        overflows = check_overflow(spark, base, shift, args.offset, capacity_override)
        if overflows or collisions:
            logger.error("Pre-flight FAILED — aborting, nothing written.")
            for table, col, mx, shifted, cap in overflows:
                logger.error("  overflow %s.%s: max=%d +%d=%d > capacity %d",
                             table, col, mx, args.offset, shifted, cap)
            for table, pmax, shifted_min in collisions:
                logger.error("  collision %s: synthetic min+%d=%d <= production max %d",
                             table, args.offset, shifted_min, pmax)
            print_deployment_summary(config)
            sys.exit(1)

        if args.dry_run:
            logger.info("Dry run: pre-flight OK. Writing nothing.")
            print_deployment_summary(config)
            return

        logger.warning("In-place, non-idempotent mutation — re-running double-shifts.")
        failures = apply_shift(spark, base, shift, args.offset,
                               continue_on_error=args.continue_on_error,
                               reliable_checkpoint=reliable)
        if failures:
            logger.error("Failed table(s): %s", ", ".join(failures))
            print_deployment_summary(config)
            sys.exit(1)
        logger.info("Done: shifted %d table(s) by +%d.", len(shift), args.offset)
        print_deployment_summary(config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
