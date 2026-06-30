# Parallel Extract Orchestrator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `scripts/parallel_extract.py`, a local Python driver that fans `save_tables.py` out across N size-balanced, concurrent OCI Data Flow runs of the same Application, with a graceful Oracle size-fetch fallback chain and a `--dry-run` planner.

**Architecture:** A standalone CLI driver (like `scripts/rollback_load.py`). It connects to the on-prem source Oracle via `python-oracledb` thin mode (lazy-imported) to estimate per-table row counts through a 5-tier fallback chain, bin-packs tables into balanced buckets, and submits/monitors one `oci data-flow run create` per bucket (≤ `--max-concurrent-runs`) by shelling to the OCI CLI. All pure logic (DSN parse, tier merge, bin-pack, command build, orchestration loop) is unit-tested without any cloud/DB; oracledb and the `oci` CLI are thin shells behind those.

**Tech Stack:** Python 3.13, `python-oracledb` (thin, runtime only), the OCI CLI (`oci data-flow run …`), `subprocess`, `argparse`. Self-contained — vendors the ~5 lines it needs from `save_tables.py` (no `datagen.*` import).

**Spec:** `docs/plans/2026-06-29-parallel-extract-orchestrator-design.md`

---

## File Structure

- **Create:** `scripts/parallel_extract.py` — the orchestrator (CLI driver).
- **Create:** `tests/test_parallel_extract.py` — unit tests for all pure functions.

`parallel_extract.py` module layout (top to bottom): constants/regexes → arg parsing →
DSN/owner helpers (Task 1) → tier merge (Task 2) → bin-pack (Task 3) → command build
(Task 4) → DB size fetch, lazy oracledb (Task 5) → submit/poll/orchestrate, subprocess
(Task 6) → `--dry-run` + `main` (Task 7).

## Test command

```
/private/tmp/claude-502/-Users-mateus-projects-eudalia-b3-synthetic-data/448b6c8e-627f-4aba-8e7c-d3c89ca352f6/scratchpad/venv/bin/python \
  -m pytest tests/test_parallel_extract.py -v
```

Lint: `…/scratchpad/venv/bin/ruff check scripts/parallel_extract.py tests/test_parallel_extract.py` (line length ≤ 100).

**Note:** `oracledb` is NOT installed in the venv. Tests must not import it at module load —
`parallel_extract.py` lazy-imports `oracledb` *inside* the fetch function only. Live runs
need `pip install oracledb`; unit tests do not.

The test file header (every task appends to it):

```python
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import parallel_extract as P  # noqa: E402
```

---

### Task 1: DSN parsing + owner/identifier helpers

**Files:**
- Create: `scripts/parallel_extract.py`
- Test: `tests/test_parallel_extract.py`

- [ ] **Step 1: Write the failing tests**

```python
class TestJdbcUrlToDsn:
    def test_sid_colon_form(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:1521:ORCL") == "dbhost:1521/ORCL"

    def test_service_slashes_form(self):
        assert P.jdbc_url_to_dsn(
            "jdbc:oracle:thin:@//dbhost:1521/PROD.cetip") == "dbhost:1521/PROD.cetip"

    def test_default_port_when_absent(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:ORCL") == "dbhost:1521/ORCL"

    def test_rejects_non_jdbc(self):
        with pytest.raises(ValueError):
            P.jdbc_url_to_dsn("postgres://x")


class TestOwnerSplit:
    def test_qualified(self):
        assert P.split_owner_table("CETIP.OPERACAO", "DEFOWNER") == ("CETIP", "OPERACAO")

    def test_unqualified_uses_default(self):
        assert P.split_owner_table("OPERACAO", "CETIP") == ("CETIP", "OPERACAO")

    def test_uppercases(self):
        assert P.split_owner_table("cetip.operacao", "x") == ("CETIP", "OPERACAO")


class TestValidIdentifier:
    def test_accepts(self):
        assert P.valid_identifier("CETIP") == "CETIP"

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            P.valid_identifier("OPER; DROP TABLE X")
```

- [ ] **Step 2: Run tests to verify they fail**

Run the test command (filtered: `… -k "Dsn or OwnerSplit or ValidIdentifier"`). Expected: FAIL — `module not found` / `AttributeError`.

- [ ] **Step 3: Implement the helpers + module preamble**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**, then **Step 5: Commit**

```bash
git add scripts/parallel_extract.py tests/test_parallel_extract.py
git commit -m "feat(extract): DSN parse + owner/identifier helpers for parallel_extract"
```

---

### Task 2: `merge_size_tiers` — tier gap-fill + median backstop

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

Weights are **estimated rows** keyed on `(owner, table)`. Each tier dict supplies only some
keys; earlier tiers win; any key still missing after the data tiers gets the **median** of
resolved weights (1.0 if none resolved). This is pure — the DB fetch (Task 5) produces the
tier dicts and passes them in.

- [ ] **Step 1: Write the failing tests**

```python
class TestMergeSizeTiers:
    def test_earlier_tier_wins(self):
        keys = [("CETIP", "A"), ("CETIP", "B")]
        tiers = [{("CETIP", "A"): 100.0}, {("CETIP", "A"): 999.0, ("CETIP", "B"): 50.0}]
        assert P.merge_size_tiers(keys, tiers) == {("CETIP", "A"): 100.0, ("CETIP", "B"): 50.0}

    def test_missing_key_gets_median(self):
        keys = [("CETIP", "A"), ("CETIP", "B"), ("CETIP", "C")]
        tiers = [{("CETIP", "A"): 10.0, ("CETIP", "B"): 30.0}]   # C unresolved
        out = P.merge_size_tiers(keys, tiers)
        assert out[("CETIP", "C")] == 20.0          # median(10, 30)

    def test_all_unresolved_default_one(self):
        keys = [("CETIP", "A")]
        assert P.merge_size_tiers(keys, []) == {("CETIP", "A"): 1.0}

    def test_ignores_non_positive(self):
        keys = [("CETIP", "A"), ("CETIP", "B")]
        tiers = [{("CETIP", "A"): 0.0, ("CETIP", "B"): 40.0}]    # 0 -> treat as unresolved
        out = P.merge_size_tiers(keys, tiers)
        assert out[("CETIP", "A")] == 40.0          # median of the single resolved value
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass.** **Step 5: Commit** `feat(extract): merge_size_tiers fallback/median backstop`.

---

### Task 3: `bin_pack` — greedy longest-processing-time-first

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

- [ ] **Step 1: Write the failing tests**

```python
class TestBinPack:
    def test_balances_and_covers_all(self):
        weights = {("S", "A"): 8.0, ("S", "B"): 4.0, ("S", "C"): 4.0, ("S", "D"): 2.0}
        buckets = P.bin_pack(weights, 2)
        assert len(buckets) == 2
        flat = sorted(k for b in buckets for k in b)
        assert flat == sorted(weights)                       # disjoint + complete
        totals = sorted(sum(weights[k] for k in b) for b in buckets)
        assert totals == [9.0, 9.0]                          # A | B+C+D... balanced

    def test_deterministic_tie_break_by_name(self):
        weights = {("S", "A"): 5.0, ("S", "B"): 5.0}
        assert P.bin_pack(weights, 2) == P.bin_pack(weights, 2)

    def test_more_buckets_than_tables(self):
        weights = {("S", "A"): 1.0}
        buckets = P.bin_pack(weights, 3)
        assert sum(len(b) for b in buckets) == 1             # no table duplicated
        assert len(buckets) == 3                             # empty buckets preserved

    def test_single_bucket(self):
        weights = {("S", "A"): 1.0, ("S", "B"): 2.0}
        assert sorted(P.bin_pack(weights, 1)[0]) == [("S", "A"), ("S", "B")]
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass.** **Step 5: Commit** `feat(extract): size-balanced bin_pack (greedy LPT)`.

---

### Task 4: `build_run_create_command` — pure OCI CLI command builder

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

> **Flag-spelling caveat:** the exact `oci data-flow run create` flags MUST be confirmed
> against `oci data-flow run create --help` (Task 8, Step 1) before this is trusted in a live
> run. This task encodes the documented spelling; if `--help` differs, fix the constants here
> and update the test. Tables are passed via the run **arguments** flag as a JSON array.

- [ ] **Step 1: Write the failing tests**

```python
class TestBuildRunCreateCommand:
    def _opts(self, **kw):
        base = dict(application_id="ocid1.dataflowapplication.x",
                    compartment_id="ocid1.compartment.y", num_executors=2,
                    driver_shape="VM.Standard.E4.Flex", executor_shape="VM.Standard.E4.Flex",
                    driver_shape_config=None, executor_shape_config=None, passthrough=[])
        base.update(kw)
        return base

    def test_includes_ids_and_display_name(self):
        cmd = P.build_run_create_command([("CETIP", "A"), ("CETIP", "B")], 0, self._opts())
        assert cmd[:3] == ["oci", "data-flow", "run"]
        assert "create" in cmd
        joined = " ".join(cmd)
        assert "ocid1.dataflowapplication.x" in joined
        assert "ocid1.compartment.y" in joined
        assert "extract-bucket-0" in joined

    def test_arguments_is_json_array_of_tables(self):
        cmd = P.build_run_create_command([("CETIP", "A"), ("CETIP", "B")], 1, self._opts())
        idx = cmd.index("--arguments")
        args = json.loads(cmd[idx + 1])
        assert args == ["--tables", "CETIP.A,CETIP.B"]

    def test_passthrough_flags_appended_to_arguments(self):
        cmd = P.build_run_create_command(
            [("CETIP", "A")], 0, self._opts(passthrough=["--continue-on-error"]))
        idx = cmd.index("--arguments")
        assert json.loads(cmd[idx + 1]) == ["--tables", "CETIP.A", "--continue-on-error"]

    def test_shape_config_included_when_present(self):
        cfg = '{"ocpus": 2, "memoryInGBs": 16}'
        cmd = P.build_run_create_command(
            [("CETIP", "A")], 0, self._opts(executor_shape_config=cfg))
        assert cfg in cmd
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass.** **Step 5: Commit** `feat(extract): pure build_run_create_command`.

---

### Task 5: Live size fetch (tiers + tier-0 probe) — lazy oracledb

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

DB-touching, so the *connection* and *tier queries* are thin and verified manually. What IS
unit-testable: the **tier-4 SQL builder** (identifier interpolation safety) and the
**bytes→rows conversion**. Keep those as separate pure functions.

- [ ] **Step 1: Write the failing tests** (pure pieces only)

```python
class TestTier4Sql:
    def test_interpolates_validated_identifiers(self):
        sql = P.tier4_count_sql("CETIP", "OPERACAO")
        assert sql == "SELECT COUNT(*) FROM CETIP.OPERACAO SAMPLE (0.1)"

    def test_rejects_bad_identifier(self):
        with pytest.raises(ValueError):
            P.tier4_count_sql("CETIP", "OPER; DROP")


class TestBytesToRows:
    def test_divides_by_nominal_row_len(self):
        # NOMINAL_AVG_ROW_LEN bytes/row; only relative ordering matters
        assert P.bytes_to_rows(P.NOMINAL_AVG_ROW_LEN * 5) == 5.0

    def test_zero_bytes(self):
        assert P.bytes_to_rows(0) == 0.0
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement** the pure pieces + the thin DB fetch

```python
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
```

- [ ] **Step 4: Run tests to verify they pass** (pure pieces). The DB path is exercised in
  Task 8's manual probe.

- [ ] **Step 5: Commit** `feat(extract): oracledb size-fetch tiers + tier-0 probe`.

---

### Task 6: Orchestration loop — submit / poll / retry (subprocess shells)

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

The `oci` calls are thin wrappers (`_oci_json`). The **loop logic** (concurrency cap, retry
accounting, terminal-state classification) is pure-ish and unit-tested by injecting fake
`submit`/`poll` callables — no subprocess.

- [ ] **Step 1: Write the failing tests**

```python
class TestLifecycleClassify:
    @pytest.mark.parametrize("state,kind", [
        ("SUCCEEDED", "success"), ("FAILED", "failure"), ("CANCELED", "failure"),
        ("STOPPED", "failure"), ("ACCEPTED", "pending"), ("IN_PROGRESS", "pending"),
        ("CANCELING", "pending"), ("STOPPING", "pending"), ("WAT", "pending")])
    def test_classify(self, state, kind):
        assert P.classify_state(state) == kind


class TestRunBuckets:
    def test_retries_failed_then_succeeds(self):
        # bucket 0 fails once then succeeds; bucket 1 succeeds first try
        calls = {"submit": 0}
        seq = {0: ["FAILED", "SUCCEEDED"], 1: ["SUCCEEDED"]}
        attempt = {0: 0, 1: 0}

        def fake_submit(bucket, index, opts):
            calls["submit"] += 1
            return f"run-{index}-{attempt[index]}"

        def fake_poll(run_id):
            index = int(run_id.split("-")[1])
            state = seq[index][attempt[index]]
            return state

        def on_terminal(index):
            attempt[index] += 1

        results = P.run_buckets(
            [[("S", "A")], [("S", "B")]], opts=dict(max_concurrent_runs=2, max_retries=2,
            poll_seconds=0), submit=fake_submit, poll=fake_poll, _after_terminal=on_terminal)
        assert results[0]["state"] == "SUCCEEDED" and results[0]["retries"] == 1
        assert results[1]["state"] == "SUCCEEDED" and results[1]["retries"] == 0
        assert calls["submit"] == 3                          # 2 + 1 retry

    def test_gives_up_after_max_retries(self):
        results = P.run_buckets(
            [[("S", "A")]], opts=dict(max_concurrent_runs=1, max_retries=1, poll_seconds=0),
            submit=lambda b, i, o: "r", poll=lambda r: "FAILED")
        assert results[0]["state"] == "FAILED" and results[0]["retries"] == 1
```

> The injected `submit`/`poll`/`_after_terminal` seam exists for testing; production passes
> the real subprocess-backed `submit_run`/`poll_run`. Keep `run_buckets` free of
> `subprocess`/`time.sleep` when `poll_seconds == 0`.

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement**

```python
_PENDING = {"ACCEPTED", "IN_PROGRESS", "CANCELING", "STOPPING"}
_SUCCESS = {"SUCCEEDED"}
_FAILURE = {"FAILED", "CANCELED", "STOPPED"}

def classify_state(state: str) -> str:
    if state in _SUCCESS:
        return "success"
    if state in _FAILURE:
        return "failure"
    return "pending"        # unknown states keep polling (logged by caller)

def _oci_json(cmd: list) -> dict:
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(out.stdout) if out.stdout.strip() else {}

def submit_run(bucket, index, opts) -> str:
    cmd = build_run_create_command(bucket, index, opts)
    data = _oci_json(cmd)
    return data["data"]["id"]

def poll_run(run_id: str) -> str:
    data = _oci_json(["oci", "data-flow", "run", "get", "--run-id", run_id])
    return data["data"]["lifecycle-state"]

def run_buckets(buckets, opts, submit=submit_run, poll=poll_run, _after_terminal=None) -> list:
    """Submit buckets (<= max_concurrent_runs in flight), poll to terminal, retry failures.

    Returns a list aligned to buckets: {tables, run_id, state, retries, attempts}.
    submit/poll are injectable for tests. With poll_seconds==0 no sleeping occurs.
    """
    results = [dict(tables=b, run_id=None, state=None, retries=0) for b in buckets]
    pending = [i for i, b in enumerate(buckets) if b]    # skip empty buckets
    in_flight: dict = {}                                 # index -> run_id
    cap, max_retries, wait = (opts["max_concurrent_runs"], opts["max_retries"],
                              opts["poll_seconds"])
    while pending or in_flight:
        while pending and len(in_flight) < cap:
            i = pending.pop(0)
            in_flight[i] = submit(buckets[i], i, opts)
            results[i]["run_id"] = in_flight[i]
        for i, run_id in list(in_flight.items()):
            kind = classify_state(poll(run_id))
            if kind == "pending":
                continue
            del in_flight[i]
            if _after_terminal:
                _after_terminal(i)
            if kind == "success":
                results[i]["state"] = "SUCCEEDED"
            elif results[i]["retries"] < max_retries:
                results[i]["retries"] += 1
                pending.append(i)                        # retry
            else:
                results[i]["state"] = "FAILED"
        if wait and in_flight:
            time.sleep(wait)
    return results
```

- [ ] **Step 4: Run tests to verify they pass.** **Step 5: Commit** `feat(extract): submit/poll/retry orchestration loop`.

---

### Task 7: `--dry-run`, CLI wiring, report manifest (`main`)

**Files:** Modify `scripts/parallel_extract.py`; Test `tests/test_parallel_extract.py`

- [ ] **Step 1: Write the failing tests** (arg parsing + dry-run planning are testable; `main`'s
  live path is not)

```python
class TestParseArgs:
    def test_dry_run_and_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", [
            "parallel_extract", "--application-id", "app", "--compartment-id", "cmp",
            "--tables", "A,B", "--dry-run"])
        a = P.parse_arguments()
        assert a.dry_run is True and a.max_concurrent_runs == 4 and a.tables == "A,B"

    def test_requires_application_and_compartment(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["parallel_extract", "--tables", "A"])
        with pytest.raises(SystemExit):
            P.parse_arguments()


class TestPlanReport:
    def test_plan_lists_buckets_and_commands(self):
        weights = {("S", "A"): 9.0, ("S", "B"): 1.0}
        opts = dict(application_id="app", compartment_id="cmp", num_executors=2,
                    driver_shape="d", executor_shape="e", driver_shape_config=None,
                    executor_shape_config=None, passthrough=[])
        plan = P.build_plan(weights, num_buckets=2, opts=opts)
        assert len(plan["buckets"]) == 2
        assert all("command" in b and "tables" in b and "weight" in b for b in plan["buckets"])
        # heaviest table isolated in its own bucket
        assert any(b["tables"] == ["S.A"] for b in plan["buckets"])
```

- [ ] **Step 2: Run tests to verify they fail.**

- [ ] **Step 3: Implement** `parse_arguments`, `build_plan`, and `main`

```python
def parse_arguments():
    p = argparse.ArgumentParser(description="Fan save_tables.py out across concurrent "
                                            "OCI Data Flow runs, size-balanced.")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--tables", help="Comma-separated source table list (OWNER.TABLE or TABLE).")
    src.add_argument("--tables-file", help="Local file, one table per line (# comments ok).")
    p.add_argument("--application-id", required=True,
                   default=os.environ.get("DATAGEN_DATAFLOW_APP_ID"))
    p.add_argument("--compartment-id", required=True,
                   default=os.environ.get("DATAGEN_OCI_COMPARTMENT_ID"))
    p.add_argument("--max-concurrent-runs", type=int, default=4)
    p.add_argument("--num-buckets", type=int, default=None,
                   help="Default = --max-concurrent-runs.")
    p.add_argument("--max-retries", type=int, default=1)
    p.add_argument("--poll-seconds", type=int, default=30)
    p.add_argument("--num-executors", type=int, default=2)
    p.add_argument("--driver-shape", default="VM.Standard.E4.Flex")
    p.add_argument("--executor-shape", default="VM.Standard.E4.Flex")
    p.add_argument("--driver-shape-config", default=None)
    p.add_argument("--executor-shape-config", default=None)
    p.add_argument("--passthrough", default="",
                   help="Extra save_tables flags appended to run arguments, e.g. "
                        "'--continue-on-error'.")
    p.add_argument("--allow-equal-weight-fallback", action="store_true",
                   help="If the source is unreachable, bucket on equal weights instead of "
                        "failing.")
    p.add_argument("--dry-run", action="store_true",
                   help="Plan (sizes + buckets + commands) and exit without submitting.")
    return p.parse_args()


def _opts_from_args(a) -> dict:
    return dict(application_id=a.application_id, compartment_id=a.compartment_id,
                num_executors=a.num_executors, driver_shape=a.driver_shape,
                executor_shape=a.executor_shape, driver_shape_config=a.driver_shape_config,
                executor_shape_config=a.executor_shape_config,
                passthrough=a.passthrough.split() if a.passthrough else [],
                max_concurrent_runs=a.max_concurrent_runs, max_retries=a.max_retries,
                poll_seconds=a.poll_seconds)


def build_plan(weights: dict, num_buckets: int, opts: dict) -> dict:
    buckets = bin_pack(weights, num_buckets)
    out = {"buckets": []}
    for i, bucket in enumerate(buckets):
        out["buckets"].append({
            "index": i,
            "tables": [f"{o}.{t}" for o, t in bucket],
            "weight": sum(weights[k] for k in bucket),
            "command": build_run_create_command(bucket, i, opts) if bucket else None,
        })
    return out
```

`main` ties it together: parse args → read tables (vendored `parse_tables` like
`save_tables.py`) → split each into `(owner, table)` → fetch sizes (guarded by
`--allow-equal-weight-fallback` on connect failure) → `merge_size_tiers` →
`num_buckets = args.num_buckets or args.max_concurrent_runs` → `build_plan`. If `--dry-run`:
log the sizes report + each bucket's tables/weight/command, write the plan JSON, exit 0.
Else: `run_buckets`, write the manifest (buckets + run ids + states + retries), exit non-zero
if any bucket ended `FAILED`.

- [ ] **Step 4: Run tests to verify they pass.** **Step 5: Commit** `feat(extract): dry-run planner, CLI, manifest, main`.

---

### Task 8: Verification — flag check, whole suite, lint, manual runbook

**Files:** none (verification only; fix Task 4 constants if `--help` differs)

- [ ] **Step 1: Confirm OCI CLI flag spellings.** Run `oci data-flow run create --help` and
  verify: the run-arguments flag (`--arguments` vs `--application-arguments`), `--num-executors`,
  `--driver-shape`, `--executor-shape`, `--driver-shape-config`, `--executor-shape-config`,
  `--application-id`, `--compartment-id`, `--display-name`. If any differ, update the constants
  in `build_run_create_command` (Task 4) and its test, re-run tests.
- [ ] **Step 2: Whole suite** — `… -m pytest tests/ -q`. Expect the new `test_parallel_extract`
  green and **no new failures** vs baseline (only the pre-existing `test_engorda_tables.py`
  mock/arg-default failures; their count is environment-specific — confirm none are new).
- [ ] **Step 3: Lint** — `… ruff check scripts/parallel_extract.py tests/test_parallel_extract.py`
  → All checks passed. Commit any lint fixes (`chore(extract): lint`).
- [ ] **Step 4: Manual runbook** (documented in the PR/commit, not automated — needs live cloud/DB):
  - `pip install oracledb` in the run environment.
  - **`--dry-run` first**: confirms the tier-0 probe reaches the on-prem source, prints which
    size tier fired per table, and shows the buckets + exact `run create` commands.
  - **2-table concurrency smoke test**: launch two single-table runs sharing `export/`; confirm
    both `export/<TABLE>/` folders survive (the overwrite-scope safety gate). If clobbered,
    apply the scoped-delete+append fallback to `save_tables.py` (spec §"Concurrency safety").
  - **Ramp**: start `--max-concurrent-runs 4`, watch run durations + Oracle sessions, increase
    until throughput plateaus or throttling appears; back off one step.

---

## Notes for the implementer

- **Self-contained:** no `from datagen.* import`. Vendor `parse_tables` (the comma/file +
  dedup logic from `save_tables.py:173`) and the `IDENTIFIER_PATTERN`.
- **Lazy oracledb:** import `oracledb` only inside `connect_source` — the venv has no
  `oracledb`, and all unit tests must pass without it.
- **No subprocess/sleep in tests:** `run_buckets` takes injectable `submit`/`poll` and skips
  sleeping when `poll_seconds == 0`; never call the real `oci` CLI from a test.
- **Tables passed as arguments, env on the Application:** the orchestrator overrides only the
  run **arguments** (`--tables …`) and **shape**; `DATAGEN_RAW_PREFIX=export`, source JDBC
  URL/password, and output bucket live on the Data Flow Application (confirm before a live run).
- **`--help` gate is real:** do not trust the Task 4 flag constants until Step 1 of Task 8
  confirms them.
