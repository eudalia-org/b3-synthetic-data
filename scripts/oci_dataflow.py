"""Shared argv-only OCI Data Flow CLI adapter."""
from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

RUN_ARGS_FLAG = "--arguments"

PENDING_STATES = {"ACCEPTED", "IN_PROGRESS", "CANCELING", "STOPPING"}
SUCCESS_STATES = {"SUCCEEDED"}
FAILURE_STATES = {"FAILED", "CANCELED", "STOPPED"}

_AUTH_OPTIONS = (
    ("profile", "--profile"),
    ("config_file", "--config-file"),
    ("auth", "--auth"),
    ("cert_bundle", "--cert-bundle"),
)
_AUTH_FAILURE_MARKERS = (
    "notauthenticated",
    "not authenticated",
    "authentication failed",
    "status: 401",
    "status': 401",
    'status": 401',
    "token expired",
    "token has expired",
    "unauthorized",
)


def oci_auth_flags(opts: Mapping[str, Any] | None = None) -> list[str]:
    """Return configured global OCI CLI authentication flags."""
    opts = opts or {}
    flags = []
    for key, flag in _AUTH_OPTIONS:
        if opts.get(key):
            flags.extend((flag, str(opts[key])))
    return flags


def classify_state(state: str) -> str:
    if state in SUCCESS_STATES:
        return "success"
    if state in FAILURE_STATES:
        return "failure"
    return "pending"


def build_run_create_command(
    arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]
) -> list[str]:
    """Build an OCI Data Flow run-create argv for any application arguments."""
    command = [
        "oci",
        "data-flow",
        "run",
        "create",
        "--application-id",
        str(opts["application_id"]),
        "--compartment-id",
        str(opts["compartment_id"]),
        "--display-name",
        display_name,
        RUN_ARGS_FLAG,
        json.dumps(list(arguments)),
    ]
    for key, flag in (
        ("num_executors", "--num-executors"),
        ("driver_shape", "--driver-shape"),
        ("executor_shape", "--executor-shape"),
        ("driver_shape_config", "--driver-shape-config"),
        ("executor_shape_config", "--executor-shape-config"),
    ):
        if opts.get(key):
            command.extend((flag, str(opts[key])))
    return command + oci_auth_flags(opts)


def build_run_get_command(run_id: str, opts: Mapping[str, Any] | None = None) -> list[str]:
    return ["oci", "data-flow", "run", "get", "--run-id", run_id] + oci_auth_flags(opts)


def build_run_cancel_command(
    run_id: str, opts: Mapping[str, Any] | None = None
) -> list[str]:
    return ["oci", "data-flow", "run", "cancel", "--run-id", run_id] + oci_auth_flags(opts)


def _option_value(command: Sequence[str], option: str) -> str | None:
    try:
        return command[command.index(option) + 1]
    except (ValueError, IndexError):
        return None


def _uses_security_token(command: Sequence[str]) -> bool:
    return _option_value(command, "--auth") == "security_token"


def _is_authentication_error(error: subprocess.CalledProcessError) -> bool:
    output = f"{error.stdout or ''}\n{error.stderr or ''}".lower()
    return any(marker in output for marker in _AUTH_FAILURE_MARKERS)


def _build_session_refresh_command(command: Sequence[str]) -> list[str]:
    refresh = ["oci", "session", "refresh"]
    for option in ("--profile", "--config-file", "--cert-bundle"):
        value = _option_value(command, option)
        if value:
            refresh.extend((option, value))
    return refresh


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(command), capture_output=True, text=True, check=True)


def run_json(command: Sequence[str]) -> dict[str, Any]:
    """Run OCI JSON transport, refreshing one expired security-token session once."""
    command = list(command)
    try:
        completed = _run(command)
    except subprocess.CalledProcessError as error:
        if not (_uses_security_token(command) and _is_authentication_error(error)):
            raise
        _run(_build_session_refresh_command(command))
        completed = _run(command)
    return json.loads(completed.stdout) if completed.stdout.strip() else {}


def create_run(arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]) -> str:
    data = run_json(build_run_create_command(arguments, display_name, opts))
    return data["data"]["id"]


def get_run_state(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    data = run_json(build_run_get_command(run_id, opts))
    return data["data"]["lifecycle-state"]


def cancel_run(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    data = run_json(build_run_cancel_command(run_id, opts))
    return data.get("data", {}).get("lifecycle-state", "CANCELING")
