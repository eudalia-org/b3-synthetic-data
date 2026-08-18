"""Compatibility facade for OCI helpers implemented by ``run_pipeline``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

try:
    import run_pipeline as _implementation
except ModuleNotFoundError:  # Supports ``from scripts import oci_dataflow``.
    from scripts import run_pipeline as _implementation

RUN_ARGS_FLAG = _implementation.RUN_ARGS_FLAG
PENDING_STATES = _implementation.PENDING_STATES
SUCCESS_STATES = _implementation.SUCCESS_STATES
FAILURE_STATES = _implementation.FAILURE_STATES
subprocess = _implementation.subprocess

oci_auth_flags = _implementation.oci_auth_flags
classify_state = _implementation.classify_state


def build_run_create_command(
    arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]
) -> list[str]:
    return _implementation._build_run_create_command(
        arguments, display_name, opts, oci_auth_flags
    )


def build_run_get_command(
    run_id: str, opts: Mapping[str, Any] | None = None
) -> list[str]:
    return _implementation._build_run_get_command(run_id, opts, oci_auth_flags)


def build_run_cancel_command(
    run_id: str, opts: Mapping[str, Any] | None = None
) -> list[str]:
    return _implementation._build_run_cancel_command(run_id, opts, oci_auth_flags)


def run_json(command: Sequence[str]) -> dict[str, Any]:
    return _implementation.run_json(command)


def create_run(arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]) -> str:
    return _implementation._create_run(arguments, display_name, opts, run_json)


def get_run_state(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    return _implementation._get_run_state(run_id, opts, run_json)


def cancel_run(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    return _implementation._cancel_run(run_id, opts, run_json)


def __getattr__(name: str) -> Any:
    return getattr(_implementation, name)
