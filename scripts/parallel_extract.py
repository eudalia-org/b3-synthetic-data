"""Parallel Oracle->OCI extract orchestrator.

Fans datagen/save_tables.py out across N size-balanced, concurrent OCI Data Flow
runs of one Application. Standalone local driver (no datagen.* import); oracledb is
lazy-imported only for the live size fetch.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("parallel_extract")

IDENTIFIER_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]*$")
DEFAULT_ORACLE_PORT = "1521"


def valid_identifier(name: str) -> str:
    upper = name.upper()
    if not IDENTIFIER_PATTERN.match(upper):
        raise ValueError(f"Invalid Oracle identifier: {name!r}")
    return upper


def split_owner_table(table: str, default_owner: str) -> tuple[str, str]:
    if "." in table:
        owner, name = table.split(".", 1)
    else:
        owner, name = default_owner, table
    return owner.upper(), name.upper()


def parse_tables(tables: str | None, tables_file: str | None) -> list[str]:
    """Comma list or one-per-line file (# comments, blanks ignored), order-preserving dedup.

    Vendored from save_tables.py:173 — kept self-contained (no datagen.* import).
    """
    if tables:
        parsed = [t.strip() for t in tables.split(",")]
    else:
        from pathlib import Path
        lines = Path(tables_file or "").read_text().splitlines()
        parsed = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    deduped = list(dict.fromkeys(t for t in parsed if t))
    if not deduped:
        raise ValueError("No tables provided")
    return deduped


def jdbc_url_to_dsn(jdbc_url: str) -> str:
    """Parse an on-prem Oracle thin JDBC URL into an oracledb EZConnect DSN.

    Handles `jdbc:oracle:thin:@host:port:sid` and `jdbc:oracle:thin:@//host:port/service`.
    Port defaults to 1521 when absent. No wallet/TLS (source is on-prem).
    """
    prefix = "jdbc:oracle:thin:@"
    if not jdbc_url.startswith(prefix):
        raise ValueError(f"Not an Oracle thin JDBC URL: {jdbc_url!r}")
    body = jdbc_url[len(prefix):]
    if body.startswith("//"):                       # //host:port/service
        host_port, _, service = body[2:].partition("/")
        host, _, port = host_port.partition(":")
        port = port or DEFAULT_ORACLE_PORT
        return f"{host}:{port}/{service}"
    parts = body.split(":")                          # host[:port]:sid
    if len(parts) == 3:
        host, port, sid = parts
    elif len(parts) == 2:
        host, sid, port = parts[0], parts[1], DEFAULT_ORACLE_PORT
    else:
        raise ValueError(f"Cannot parse JDBC URL: {jdbc_url!r}")
    return f"{host}:{port}/{sid}"


NOMINAL_AVG_ROW_LEN = 100   # bytes/row; tier-1 only needs relative ordering (soft constant)


def tier4_count_sql(owner: str, table: str) -> str:
    return f"SELECT COUNT(*) FROM {valid_identifier(owner)}.{valid_identifier(table)} SAMPLE (0.1)"


def bytes_to_rows(total_bytes: float) -> float:
    return float(total_bytes) / NOMINAL_AVG_ROW_LEN


def _owners_in_clause(owners):
    # returns (sql_fragment, bind_dict) binding owners as VALUES (never identifiers)
    binds = {f"o{i}": o for i, o in enumerate(sorted(owners))}
    placeholders = ", ".join(f":{k}" for k in binds)
    return placeholders, binds


def connect_source():
    """Lazy-import oracledb; connect to the on-prem source. Raises on failure."""
    import oracledb  # lazy: not needed for unit tests
    dsn = jdbc_url_to_dsn(os.environ["DATAGEN_SOURCE_JDBC_URL"])
    user = os.environ.get("DATAGEN_SOURCE_DB_USER", "")
    password = os.environ["DATAGEN_SOURCE_DB_PASSWORD"]
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    conn.cursor().execute("SELECT 1 FROM dual").fetchone()   # tier-0 probe
    return conn


def fetch_size_tiers(conn, keys) -> list:
    """Run tiers 1-4 against `conn`; return an ordered list of {(owner,table): rows}.

    Each tier is wrapped: ORA-00942 / NULL / errors are logged and skipped (the tier
    contributes whatever it resolved, or {}). keys is the full (owner,table) list.
    """
    owners = {o for o, _ in keys}
    wanted = set(keys)
    placeholders, binds = _owners_in_clause(owners)
    tiers = []

    def run(label, sql, row_to_kv, extra_binds=None):
        out = {}
        try:
            cur = conn.cursor()
            cur.execute(sql, {**binds, **(extra_binds or {})})
            for row in cur:
                key, val = row_to_kv(row)
                if key in wanted and val is not None:
                    out[key] = float(val)
        except Exception as exc:                              # noqa: BLE001 - tier fallthrough
            logger.warning("size tier %s failed (fallthrough): %s", label, exc)
        return out

    tiers.append(run("dba_segments",
        f"SELECT OWNER, SEGMENT_NAME, SUM(BYTES) FROM DBA_SEGMENTS "
        f"WHERE OWNER IN ({placeholders}) AND SEGMENT_TYPE LIKE 'TABLE%' "
        f"GROUP BY OWNER, SEGMENT_NAME",
        lambda r: ((r[0], r[1]), bytes_to_rows(r[2]) if r[2] else None)))
    tiers.append(run("all_tables",
        f"SELECT OWNER, TABLE_NAME, NUM_ROWS FROM ALL_TABLES WHERE OWNER IN ({placeholders})",
        lambda r: ((r[0], r[1]), r[2])))
    tiers.append(run("all_tab_statistics",
        f"SELECT OWNER, TABLE_NAME, NUM_ROWS FROM ALL_TAB_STATISTICS "
        f"WHERE OWNER IN ({placeholders}) AND PARTITION_NAME IS NULL",
        lambda r: ((r[0], r[1]), r[2])))

    # tier 4: per still-missing key (after merging 1-3), sampled count
    resolved = set()
    for t in tiers:
        resolved |= set(t)
    tier4 = {}
    for owner, table in sorted(wanted - resolved):
        try:
            cur = conn.cursor()
            n = cur.execute(tier4_count_sql(owner, table)).fetchone()[0]
            if n:
                tier4[(owner, table)] = float(n) * 1000.0     # 0.1% sample -> scale up
        except Exception as exc:                              # noqa: BLE001
            logger.warning("size tier sample(%s.%s) failed: %s", owner, table, exc)
    tiers.append(tier4)
    return tiers


TIER_LABELS = ["dba_segments", "all_tables", "all_tab_statistics", "sample_count"]


def resolve_sizes(keys, connect=connect_source, allow_fallback=False):
    """Resolve {(owner,table): rows} + provenance, with the loud-fail/fallback gate.

    Tries `connect()` (which runs the tier-0 probe). On failure: if allow_fallback,
    return equal weights (1.0) with 'equal-weight-fallback' provenance + a warning;
    otherwise log and `sys.exit(2)`. Injectable `connect` for tests.
    """
    try:
        conn = connect()
    except Exception as exc:                                  # noqa: BLE001
        if not allow_fallback:
            logger.error("Source unreachable (%s). Pass --allow-equal-weight-fallback to "
                         "bucket on equal weights instead.", exc)
            sys.exit(2)
        logger.warning("Source unreachable (%s); falling back to equal weights.", exc)
        return ({k: 1.0 for k in keys}, {k: "equal-weight-fallback" for k in keys})
    try:
        tiers = fetch_size_tiers(conn, keys)
    finally:
        conn.close()
    return merge_size_tiers(keys, tiers), size_provenance(keys, tiers, TIER_LABELS)


# Confirmed against `oci data-flow run create --help` (Task 8, Step 1).
RUN_ARGS_FLAG = "--arguments"        # JSON array of application arguments


def build_run_create_command(bucket: list, index: int, opts: dict) -> list:
    """Build the argv for `oci data-flow run create` for one bucket. Pure."""
    tables = ",".join(f"{owner}.{name}" for owner, name in bucket)
    arguments = ["--tables", tables, *opts["passthrough"]]
    cmd = [
        "oci", "data-flow", "run", "create",
        "--application-id", opts["application_id"],
        "--compartment-id", opts["compartment_id"],
        "--display-name", f"extract-bucket-{index}",
        RUN_ARGS_FLAG, json.dumps(arguments),
        "--num-executors", str(opts["num_executors"]),
        "--driver-shape", opts["driver_shape"],
        "--executor-shape", opts["executor_shape"],
    ]
    if opts.get("driver_shape_config"):
        cmd += ["--driver-shape-config", opts["driver_shape_config"]]
    if opts.get("executor_shape_config"):
        cmd += ["--executor-shape-config", opts["executor_shape_config"]]
    return cmd


def bin_pack(weights: dict, num_buckets: int) -> list:
    """Greedy LPT: assign each table (heaviest first) to the lightest bucket.

    Returns a list of `num_buckets` lists of (owner, table) keys. Deterministic:
    ties broken by key. Empty buckets are kept (so callers can map bucket->run 1:1).
    """
    if num_buckets < 1:
        raise ValueError("num_buckets must be >= 1")
    order = sorted(weights, key=lambda k: (-weights[k], k))
    totals = [0.0] * num_buckets
    buckets: list = [[] for _ in range(num_buckets)]
    for key in order:
        i = min(range(num_buckets), key=lambda b: (totals[b], b))
        buckets[i].append(key)
        totals[i] += weights[key]
    return buckets


def merge_size_tiers(keys, tier_dicts) -> dict:
    """Resolve {(owner,table): rows} by tier precedence, median-backfilling the rest.

    keys: full list of (owner, table) tuples to resolve.
    tier_dicts: ordered list of {(owner,table): weight}; earliest wins. Non-positive
    weights are treated as unresolved. Keys still missing get the median of resolved
    weights (1.0 if none resolved).
    """
    resolved: dict = {}
    for tier in tier_dicts:
        for key, weight in tier.items():
            if key in keys and key not in resolved and weight and weight > 0:
                resolved[key] = float(weight)
    if resolved:
        ordered = sorted(resolved.values())
        mid = len(ordered) // 2
        median = (ordered[mid] if len(ordered) % 2
                  else (ordered[mid - 1] + ordered[mid]) / 2)
    else:
        median = 1.0
    return {key: resolved.get(key, median) for key in keys}


def size_provenance(keys, tier_dicts, tier_labels) -> dict:
    """Which tier resolved each key (else 'median'). For the --sizes-report. Pure.

    Same precedence as merge_size_tiers (earliest positive wins). tier_labels names
    tiers positionally; len(tier_labels) == len(tier_dicts).
    """
    out = {}
    for key in keys:
        out[key] = "median"
        for label, tier in zip(tier_labels, tier_dicts):
            w = tier.get(key)
            if w and w > 0:
                out[key] = label
                break
    return out
