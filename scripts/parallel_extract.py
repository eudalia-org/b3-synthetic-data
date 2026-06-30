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
