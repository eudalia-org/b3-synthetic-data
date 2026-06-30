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
