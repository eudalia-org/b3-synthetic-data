#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["click>=8.1,<9"]
# ///
"""Standalone OCI Data Flow pipeline orchestrator for the first tracer slice."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from configparser import ConfigParser
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Protocol, Sequence
from uuid import UUID

import click

MANIFEST_VERSION = 1
CONFIG_VERSION = 1
PUBLIC_STAGES = ("extract", "faltantes", "engorda", "validate", "load", "verify")
TRACER_STAGES = ("engorda", "validate")
OVERRIDE_KEYS = {
    "engorda": {
        "n_instrumentos", "fator_k", "seed", "specs", "meu_numero_prefix",
        "query_num_if_sql",
    },
    "validate": {
        "fail_severity", "validate_against", "shape_baseline",
        "application_capacity_contract",
    },
}
TERMINAL_SUCCESS = {"SUCCEEDED"}
TERMINAL_FAILURE = {"FAILED", "CANCELED", "CANCELLED", "STOPPED"}
RUNNING_STATES = {"ACCEPTED", "IN_PROGRESS", "CANCELING", "STOPPING"}
NODE_TERMINAL = {"SUCCEEDED", "FAILED", "BLOCKED", "CANCELLED"}
MANIFEST_REPLACE_ATTEMPTS = 8
MANIFEST_REPLACE_DELAY_SECONDS = 0.05
MANIFEST_REPLACE_MAX_DELAY_SECONDS = 0.5

RUN_ARGS_FLAG = "--arguments"
PENDING_STATES = {"ACCEPTED", "IN_PROGRESS", "CANCELING", "STOPPING"}
SUCCESS_STATES = {"SUCCEEDED"}
FAILURE_STATES = {"FAILED", "CANCELED", "STOPPED"}

_AUTH_OPTIONS = (
    ("profile", "--profile"),
    ("config_file", "--config-file"),
    ("auth", "--auth"),
    ("cert_bundle", "--cert-bundle"),
    ("region", "--region"),
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
    "cli session has expired",
    "session has expired",
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


def _build_run_create_command(
    arguments: Sequence[str],
    display_name: str,
    opts: Mapping[str, Any],
    auth_builder: Callable[[Mapping[str, Any] | None], list[str]],
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
    return command + auth_builder(opts)


def build_run_create_command(
    arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]
) -> list[str]:
    return _build_run_create_command(arguments, display_name, opts, oci_auth_flags)


def _build_run_get_command(
    run_id: str,
    opts: Mapping[str, Any] | None,
    auth_builder: Callable[[Mapping[str, Any] | None], list[str]],
) -> list[str]:
    return ["oci", "data-flow", "run", "get", "--run-id", run_id] + auth_builder(opts)


def build_run_get_command(
    run_id: str, opts: Mapping[str, Any] | None = None
) -> list[str]:
    return _build_run_get_command(run_id, opts, oci_auth_flags)


def _build_run_cancel_command(
    run_id: str,
    opts: Mapping[str, Any] | None,
    auth_builder: Callable[[Mapping[str, Any] | None], list[str]],
) -> list[str]:
    return ["oci", "data-flow", "run", "cancel", "--run-id", run_id] + auth_builder(opts)


def build_run_cancel_command(
    run_id: str, opts: Mapping[str, Any] | None = None
) -> list[str]:
    return _build_run_cancel_command(run_id, opts, oci_auth_flags)


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


def _session_command(operation: str, auth: Mapping[str, Any]) -> list[str]:
    command = ["oci", "session", operation]
    for key, option in (
        ("profile", "--profile"),
        ("config_file", "--config-file"),
        ("cert_bundle", "--cert-bundle"),
    ):
        if auth.get(key):
            command.extend((option, str(auth[key])))
    return command


def _profile_region(auth: Mapping[str, Any]) -> str | None:
    if auth.get("region"):
        return str(auth["region"])
    config_file = Path(
        str(auth.get("config_file") or Path.home() / ".oci" / "config")
    ).expanduser()
    parser = ConfigParser(interpolation=None)
    if not parser.read(config_file):
        return None
    profile = str(auth.get("profile") or "DEFAULT")
    section = profile if parser.has_section(profile) else "DEFAULT"
    return parser.get(section, "region", fallback=None)


def _session_authenticate_command(auth: Mapping[str, Any], region: str) -> list[str]:
    command = ["oci", "session", "authenticate", "--region", region]
    if auth.get("profile"):
        command.extend(("--profile-name", str(auth["profile"])))
    if auth.get("config_file"):
        command.extend(("--config-location", str(auth["config_file"])))
    if auth.get("cert_bundle"):
        command.extend(("--cert-bundle", str(auth["cert_bundle"])))
    return command


def _command_label(command: Sequence[str]) -> str:
    parts = list(command)
    if parts and parts[0] == "oci":
        return " ".join(parts[1:4])
    return parts[0] if parts else "command"


def _run(
    command: Sequence[str],
    *,
    timeout_seconds: float | None = None,
    interactive: bool = False,
) -> subprocess.CompletedProcess[str]:
    options: dict[str, Any] = {"text": True, "check": True}
    if not interactive:
        options["capture_output"] = True
        if _uses_security_token(command):
            # OCI CLI otherwise opens its own hidden re-auth prompt while the
            # parent captures stdout/stderr. Decline it so Click can prompt.
            options["input"] = "n\n"
    if timeout_seconds is not None:
        options["timeout"] = timeout_seconds
    try:
        return subprocess.run(list(command), **options)
    except subprocess.TimeoutExpired as exc:
        raise OciExecutionError(
            f"OCI command {_command_label(command)!r} timed out after "
            f"{timeout_seconds:g}s",
            stdout=exc.stdout,
            stderr=exc.stderr,
        ) from exc


def _oci_failure(error: subprocess.CalledProcessError, command: Sequence[str]) -> OciExecutionError:
    detail = str(error.stderr or error.stdout or "").strip().replace("\n", " ")
    suffix = f": {detail[:400]}" if detail else ""
    return OciExecutionError(
        f"OCI command {_command_label(command)!r} failed with exit code "
        f"{error.returncode}{suffix}",
        returncode=error.returncode,
        stdout=error.stdout,
        stderr=error.stderr,
    )


def run_json(
    command: Sequence[str],
    *,
    timeout_seconds: float | None = None,
    progress: Any | None = None,
    wrap_errors: bool = True,
    reauthenticate: Callable[[], None] | None = None,
    auto_refresh: bool = True,
) -> dict[str, Any]:
    """Run OCI JSON transport, refreshing one expired security-token session once."""
    command = list(command)
    if progress is not None:
        progress.emit(
            f"[oci] {_command_label(command)} timeout="
            f"{timeout_seconds:g}s" if timeout_seconds is not None
            else f"[oci] {_command_label(command)} timeout=none"
        )
    try:
        completed = _run(command, timeout_seconds=timeout_seconds)
    except subprocess.CalledProcessError as error:
        if not (_uses_security_token(command) and _is_authentication_error(error)):
            if not wrap_errors:
                raise
            raise _oci_failure(error, command) from error
        if reauthenticate is not None:
            if progress is not None:
                progress.emit(
                    "[auth] OCI command reported expired session; reauthenticating"
                )
            reauthenticate()
            try:
                completed = _run(command, timeout_seconds=timeout_seconds)
            except subprocess.CalledProcessError as retry_error:
                if not wrap_errors:
                    raise
                raise _oci_failure(retry_error, command) from retry_error
            stdout = completed.stdout.strip()
            return json.loads(stdout) if stdout else {}
        if not auto_refresh:
            if not wrap_errors:
                raise
            raise _oci_failure(error, command) from error
        refresh = _build_session_refresh_command(command)
        if progress is not None:
            progress.emit("[auth] security token expired; refreshing OCI session")
        try:
            _run(refresh, timeout_seconds=timeout_seconds)
        except subprocess.CalledProcessError as refresh_error:
            if not wrap_errors:
                raise
            raise _oci_failure(refresh_error, refresh) from refresh_error
        if progress is not None:
            progress.emit("[auth] OCI session refreshed; retrying command once")
        try:
            completed = _run(command, timeout_seconds=timeout_seconds)
        except subprocess.CalledProcessError as retry_error:
            if not wrap_errors:
                raise
            raise _oci_failure(retry_error, command) from retry_error
    return json.loads(completed.stdout) if completed.stdout.strip() else {}


def _create_run(
    arguments: Sequence[str],
    display_name: str,
    opts: Mapping[str, Any],
    transport: Callable[[Sequence[str]], dict[str, Any]],
) -> str:
    data = transport(build_run_create_command(arguments, display_name, opts))
    return data["data"]["id"]


def create_run(arguments: Sequence[str], display_name: str, opts: Mapping[str, Any]) -> str:
    return _create_run(arguments, display_name, opts, run_json)


def _get_run_state(
    run_id: str,
    opts: Mapping[str, Any] | None,
    transport: Callable[[Sequence[str]], dict[str, Any]],
) -> str:
    data = transport(build_run_get_command(run_id, opts))
    return data["data"]["lifecycle-state"]


def get_run_state(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    return _get_run_state(run_id, opts, run_json)


def _cancel_run(
    run_id: str,
    opts: Mapping[str, Any] | None,
    transport: Callable[[Sequence[str]], dict[str, Any]],
) -> str:
    data = transport(build_run_cancel_command(run_id, opts))
    return data.get("data", {}).get("lifecycle-state", "CANCELING")


def cancel_run(run_id: str, opts: Mapping[str, Any] | None = None) -> str:
    return _cancel_run(run_id, opts, run_json)

# Generator product -> validator profile. This registry stays lightweight so the
# local CLI never imports the Spark-heavy generator or validator modules.
_ENGORDA_VALIDATE = frozenset({"engorda", "validate"})
_VALIDATE_ONLY = frozenset({"validate"})
PRODUCTS: dict[str, dict[str, Any]] = {
    "cdb_simplificado": {
        "validator_product": "cdb_simplificado",
        "capabilities": _ENGORDA_VALIDATE,
    },
    "cdb_resgate": {"validator_product": "cdb", "capabilities": _ENGORDA_VALIDATE},
    "cdb_escalonamento": {"validator_product": "cdb", "capabilities": _ENGORDA_VALIDATE},
    "rdb_inclusao": {"validator_product": "rdb", "capabilities": _ENGORDA_VALIDATE},
    "rdb_resgate": {"validator_product": "rdb", "capabilities": _ENGORDA_VALIDATE},
    "lci": {"validator_product": "lci", "capabilities": _ENGORDA_VALIDATE},
    "lca": {"validator_product": "lca", "capabilities": _ENGORDA_VALIDATE},
    "ccb_pppre": {"validator_product": "ccb", "capabilities": _ENGORDA_VALIDATE},
    "ccb_pfpre": {"validator_product": "ccb", "capabilities": _ENGORDA_VALIDATE},
    "ccb_pgrpre": {"validator_product": "ccb", "capabilities": _ENGORDA_VALIDATE},
    "ccb_favcp": {"validator_product": "ccb", "capabilities": _ENGORDA_VALIDATE},
    "ccb_fapre": {"validator_product": "ccb", "capabilities": _ENGORDA_VALIDATE},
    "gravame": {"validator_product": "gravame", "capabilities": _VALIDATE_ONLY},
    "lastro": {"validator_product": "credito_scr", "capabilities": _VALIDATE_ONLY},
    "direito_creditorio": {"validator_product": "dicre", "capabilities": _VALIDATE_ONLY},
}


class PipelineError(ValueError):
    """Operator-correctable planning or execution error."""


class OciExecutionError(RuntimeError):
    """OCI command failed without exposing its complete argv."""

    def __init__(
        self,
        message: str,
        *,
        returncode: int | None = None,
        stdout: str | bytes | None = None,
        stderr: str | bytes | None = None,
    ):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@dataclass(frozen=True)
class AuthOptions:
    profile: str | None = None
    config_file: str | None = None
    auth: str | None = None
    cert_bundle: str | None = None
    region: str | None = None

    def as_dict(self) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "profile": self.profile,
                "config_file": self.config_file,
                "auth": self.auth,
                "cert_bundle": self.cert_bundle,
                "region": self.region,
            }.items()
            if value is not None
        }


@dataclass
class NodeResult:
    state: str
    attempts: list[dict[str, Any]]
    detail: dict[str, Any]


class ProgressReporter:
    """Serialize operator output emitted by concurrent pipeline workers."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._lock = threading.Lock()

    def emit(self, message: str) -> None:
        if not self.enabled:
            return
        tag = message.partition("]")[0] + "]"
        color = {
            "[run]": "bright_white",
            "[preflight]": "bright_blue",
            "[oci]": "bright_black",
            "[auth]": "yellow",
            "[launch]": "bright_cyan",
            "[submit]": "cyan",
            "[poll]": "blue",
            "[reserve]": "magenta",
            "[done]": "green",
            "[failed]": "red",
            "[dry-run]": "yellow",
        }.get(tag)
        with self._lock:
            click.secho(
                message,
                fg=color,
                bold=tag in {"[run]", "[done]", "[failed]"},
                err=True,
            )


class AtomicManifest:
    """Thread-safe JSON replacement; readers never observe a partial document."""

    def __init__(self, path: Path, payload: dict[str, Any]):
        self.path = path
        self.payload = payload
        self._lock = threading.Lock()
        self._write_locked()

    def _write_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            for attempt in range(MANIFEST_REPLACE_ATTEMPTS):
                try:
                    os.replace(temporary, self.path)
                    break
                except PermissionError:
                    if attempt == MANIFEST_REPLACE_ATTEMPTS - 1:
                        raise
                    time.sleep(
                        min(
                            MANIFEST_REPLACE_DELAY_SECONDS * 2**attempt,
                            MANIFEST_REPLACE_MAX_DELAY_SECONDS,
                        )
                    )
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def update(self, mutate: Callable[[dict[str, Any]], None]) -> None:
        with self._lock:
            mutate(self.payload)
            self.payload["updated_at"] = utc_now()
            self._write_locked()


class ModuleAdapter:
    """Live adapter backed entirely by functions in this standalone module."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 60,
        auth_refresh_seconds: float = 1800,
        progress: ProgressReporter | None = None,
    ):
        self.timeout_seconds = timeout_seconds
        self.auth_refresh_seconds = auth_refresh_seconds
        self.progress = progress
        self._auth_context: tuple[
            dict[str, Any], bool, Callable[[str], bool], str | None
        ] | None = None
        self._auth_lock = threading.RLock()
        self._last_auth_refresh = 0.0

    def _auth_run(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: float | None = None,
        wrap_errors: bool = True,
        interactive: bool = False,
    ) -> None:
        if self.progress is not None:
            self.progress.emit(
                f"[auth] {_command_label(command)} timeout="
                f"{(timeout_seconds or self.timeout_seconds):g}s"
            )
        try:
            run_options: dict[str, Any] = {
                "timeout_seconds": timeout_seconds or self.timeout_seconds,
            }
            if interactive:
                run_options["interactive"] = True
            _run(command, **run_options)
        except subprocess.CalledProcessError as error:
            if not wrap_errors:
                raise
            raise _oci_failure(error, command) from error

    def ensure_auth(
        self,
        auth: Mapping[str, Any],
        *,
        allow_prompt: bool,
        prompt: Callable[[str], bool],
        application_id: str | None = None,
        force_refresh: bool = False,
    ) -> None:
        if auth.get("auth") != "security_token":
            if self.progress is not None:
                self.progress.emit(
                    f"[auth] mode={auth.get('auth') or 'api_key'}; "
                    "first OCI request will validate credentials"
                )
            return

        self._auth_context = (
            dict(auth), allow_prompt, prompt, application_id
        )

        if application_id:
            probe = [
                "oci", "data-flow", "application", "get",
                "--application-id", application_id,
                *self._auth_flags(dict(auth)),
            ]
            probe_name = "Data Flow application"
        else:
            probe = ["oci", "os", "ns", "get", *self._auth_flags(dict(auth))]
            probe_name = "Object Storage namespace"

        def probe_auth() -> bool:
            try:
                self._auth_run(probe, wrap_errors=False)
                return True
            except subprocess.CalledProcessError as error:
                if _is_authentication_error(error):
                    return False
                raise _oci_failure(error, probe) from error

        if self.progress is not None:
            self.progress.emit(
                f"[auth] validating security token against {probe_name}"
            )
        probe_valid = probe_auth()
        if probe_valid and not force_refresh:
            self._last_auth_refresh = time.monotonic()
            if self.progress is not None:
                self.progress.emit("[auth] OCI security-token session is valid")
            return

        if not probe_valid:
            if not allow_prompt:
                raise OciExecutionError(
                    "OCI security-token session is invalid and prompting is disabled "
                    "by --no-auth-prompt"
                )
            if not prompt("OCI security-token session is invalid. Refresh it now?"):
                raise OciExecutionError("OCI authentication was declined by the operator")

        refresh = _session_command("refresh", auth)
        if self.progress is not None:
            self.progress.emit("[auth] proactively refreshing OCI security-token session")
        try:
            self._auth_run(refresh, wrap_errors=False)
            refresh_succeeded = probe_auth()
        except (subprocess.CalledProcessError, OciExecutionError):
            refresh_succeeded = False
        if not refresh_succeeded:
            refresh_error = OciExecutionError(
                "OCI session refresh did not produce a valid token"
            )
            if not allow_prompt:
                raise OciExecutionError(
                    "OCI security-token refresh failed and prompting is disabled "
                    "by --no-auth-prompt"
                ) from refresh_error
            region = _profile_region(auth)
            if not region:
                raise OciExecutionError(
                    "OCI session refresh failed and browser authentication needs "
                    "--region or a region in the OCI profile"
                ) from refresh_error
            if not prompt(
                "OCI session refresh failed. Start browser authentication now?"
            ):
                raise OciExecutionError(
                    "OCI browser authentication was declined by the operator"
                ) from refresh_error
            authenticate = _session_authenticate_command(auth, region)
            if self.progress is not None:
                self.progress.emit(
                    f"[auth] starting OCI browser authentication region={region}"
                )
            self._auth_run(authenticate, interactive=True)
            if not probe_auth():
                raise OciExecutionError(
                    "OCI browser authentication completed but the configured "
                    f"profile still cannot access {probe_name}"
                )
        self._last_auth_refresh = time.monotonic()
        if self.progress is not None:
            self.progress.emit("[auth] OCI session refreshed and revalidated")

    def _reauthenticate(self, observed_refresh: float | None = None) -> None:
        if self._auth_context is None:
            raise OciExecutionError("OCI authentication context is unavailable")
        with self._auth_lock:
            if (
                observed_refresh is not None
                and self._last_auth_refresh > observed_refresh
            ):
                return
            auth, allow_prompt, prompt, application_id = self._auth_context
            self.ensure_auth(
                auth,
                allow_prompt=allow_prompt,
                prompt=prompt,
                application_id=application_id,
                force_refresh=True,
            )

    def _refresh_auth_if_due(self) -> None:
        if (
            self._auth_context is None
            or self.auth_refresh_seconds <= 0
            or time.monotonic() - self._last_auth_refresh < self.auth_refresh_seconds
        ):
            return
        with self._auth_lock:
            if time.monotonic() - self._last_auth_refresh < self.auth_refresh_seconds:
                return
            if self.progress is not None:
                self.progress.emit(
                    "[auth] proactive refresh interval reached during polling"
                )
            self._reauthenticate()

    def _compat_operation(self, name: str, default: Callable[..., Any]) -> Callable[..., Any]:
        module = getattr(self, "module", None)
        return getattr(module, name, default) if module is not None else default

    def create_run(
        self, arguments: Sequence[str], display_name: str, opts: dict[str, Any]
    ) -> Any:
        operation = self._compat_operation("create_run", None)
        if operation is not None:
            return operation(arguments, display_name, opts)
        return _create_run(arguments, display_name, opts, self._transport())

    def get_run_state(self, run_id: str, opts: dict[str, Any]) -> Any:
        operation = self._compat_operation("get_run_state", None)
        if operation is not None:
            return operation(run_id, opts)
        return _get_run_state(run_id, opts, self._transport())

    def cancel_run(self, run_id: str, opts: dict[str, Any]) -> Any:
        operation = self._compat_operation("cancel_run", None)
        if operation is not None:
            return operation(run_id, opts)
        return _cancel_run(run_id, opts, self._transport(manage_auth=False))

    def _transport(
        self, *, manage_auth: bool = True
    ) -> Callable[[Sequence[str]], dict[str, Any]]:
        operation = self._compat_operation("run_json", None)
        if operation is not None:
            return operation
        timeout_seconds = getattr(self, "timeout_seconds", 60)
        progress = getattr(self, "progress", None)

        def transport(command: Sequence[str]) -> dict[str, Any]:
            if manage_auth:
                self._refresh_auth_if_due()
            observed_refresh = self._last_auth_refresh
            return run_json(
                command,
                timeout_seconds=timeout_seconds,
                progress=progress,
                reauthenticate=(
                    (lambda: self._reauthenticate(observed_refresh))
                    if manage_auth and self._auth_context is not None
                    else None
                ),
                auto_refresh=manage_auth,
            )

        return transport

    def _auth_flags(self, auth: dict[str, str]) -> list[str]:
        return self._compat_operation("oci_auth_flags", oci_auth_flags)(auth)

    def _object_command(self, operation: str, uri: str, auth: dict[str, str]) -> list[str]:
        bucket, namespace, object_name = parse_oci_uri(uri)
        return [
            "oci",
            "os",
            "object",
            operation,
            "--bucket-name",
            bucket,
            "--namespace",
            namespace,
            "--name" if operation in {"get", "put"} else "--prefix",
            object_name,
            *self._auth_flags(auth),
        ]

    def uri_exists(self, uri: str, *, auth: dict[str, str]) -> bool:
        command = self._object_command("list", uri, auth)
        command.append("--all")
        response = self._transport()(command)
        return bool(response.get("data"))

    def describe_uri(self, uri: str, *, auth: dict[str, str]) -> dict[str, Any]:
        command = self._object_command("list", uri, auth)
        command.append("--all")
        response = self._transport()(command)
        raw_items = response.get("data", [])
        if isinstance(raw_items, dict):
            raw_items = raw_items.get("objects", [])
        if not isinstance(raw_items, list) or not raw_items:
            raise PipelineError(f"OCI input URI does not exist: {uri}")
        inventory = sorted(
            ({
                "name": item.get("name"),
                "etag": item.get("etag"),
                "size": item.get("size"),
            } for item in raw_items if isinstance(item, dict)),
            key=lambda item: (str(item["name"]), str(item["etag"])),
        )
        canonical = json.dumps(inventory, sort_keys=True, separators=(",", ":"))
        return {
            "object_count": len(inventory),
            "total_bytes": sum(
                item["size"] for item in inventory if isinstance(item["size"], int)
            ),
            "inventory_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        }

    def read_json(self, uri: str, *, auth: dict[str, str]) -> dict[str, Any]:
        fd, temporary = tempfile.mkstemp(prefix="pipeline-object-", suffix=".json")
        os.close(fd)
        os.unlink(temporary)
        try:
            command = self._object_command("get", uri, auth)
            command += ["--file", temporary]
            self._transport()(command)
            with Path(temporary).open(encoding="utf-8") as handle:
                return json.load(handle)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def upload_file(self, path: str, uri: str, *, auth: dict[str, str]) -> Any:
        command = self._object_command("put", uri, auth)
        command += ["--file", path, "--no-overwrite"]
        return self._transport()(command)

    def reserve_ranges(self, **kwargs: Any) -> Any:
        kwargs.setdefault(
            "storage",
            OciCliStorage(
                kwargs["auth"],
                transport=self._transport(),
                auth_builder=self._auth_flags,
            ),
        )
        return reserve_ranges(**kwargs)


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def default_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{secrets.token_hex(4)}"


def uri_join(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(part.strip("/") for part in parts)])


def parse_oci_uri(uri: str) -> tuple[str, str, str]:
    if not uri.startswith("oci://"):
        raise PipelineError(f"not an OCI URI: {uri}")
    authority, separator, object_name = uri.removeprefix("oci://").partition("/")
    bucket, at, namespace = authority.partition("@")
    if not separator or not bucket or not at or not namespace or not object_name:
        raise PipelineError(f"invalid OCI URI: {uri}")
    return bucket, namespace, object_name


SCHEMA_VERSION = 1
MAX_CAS_ATTEMPTS = 5
MIN_MEU_NUMERO_PREFIX = 100
MAX_MEU_NUMERO_PREFIX = 999
MAX_MEU_NUMERO_ORDINAL = 9_999_999


class ReservationError(RuntimeError):
    """The reservation contract could not be completed safely."""


class ObjectNotFound(ReservationError):
    """The requested object does not exist."""


class PreconditionFailed(ReservationError):
    """An Object Storage conditional operation lost a race."""


class LeaseUnavailable(ReservationError):
    """Another owner holds the environment lease."""


@dataclass(frozen=True)
class ObjectMetadata:
    etag: str


@dataclass(frozen=True)
class StoredObject:
    data: bytes
    etag: str


class Storage(Protocol):
    """Minimal conditional object-store seam used by reservation tests."""

    def head(self, uri: str) -> ObjectMetadata | None: ...

    def get(self, uri: str) -> StoredObject: ...

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        no_overwrite: bool = False,
        if_match: str | None = None,
    ) -> ObjectMetadata: ...

    def delete(self, uri: str, *, if_match: str | None = None) -> None: ...


def _parse_oci_uri(uri: str) -> tuple[str, str, str]:
    if not uri.startswith("oci://"):
        raise ReservationError(f"not an OCI URI: {uri}")
    authority, separator, name = uri.removeprefix("oci://").partition("/")
    bucket, at, namespace = authority.partition("@")
    if not separator or not bucket or not at or not namespace or not name:
        raise ReservationError(f"invalid OCI URI: {uri}")
    return bucket, namespace, name


def _response_etag(response: Mapping[str, Any]) -> str | None:
    for source in (response, response.get("data")):
        if isinstance(source, Mapping):
            for key in ("etag", "e-tag"):
                value = source.get(key)
                if isinstance(value, str) and value:
                    return value
    return None


class OciCliStorage:
    """OCI CLI object adapter; JSON payloads travel through local temp files."""

    def __init__(
        self,
        auth: Mapping[str, Any] | None = None,
        transport: Callable[[Sequence[str]], dict[str, Any]] | None = None,
        auth_builder: Callable[[Mapping[str, Any] | None], list[str]] | None = None,
    ):
        self._auth = dict(auth or {})
        self._transport = transport or run_json
        self._auth_builder = auth_builder or oci_auth_flags

    def _command(self, operation: str, uri: str) -> list[str]:
        bucket, namespace, name = _parse_oci_uri(uri)
        return [
            "oci",
            "os",
            "object",
            operation,
            "--bucket-name",
            bucket,
            "--namespace-name",
            namespace,
            "--name",
            name,
            *self._auth_builder(self._auth),
        ]

    @staticmethod
    def _translate_error(error: subprocess.CalledProcessError | OciExecutionError) -> None:
        output = f"{error.stdout or ''}\n{error.stderr or ''}".lower()
        if any(marker in output for marker in ("status: 404", '"status": 404', "notfound")):
            raise ObjectNotFound("Object Storage object does not exist") from error
        if any(
            marker in output
            for marker in ("status: 412", '"status": 412', "preconditionfailed")
        ):
            raise PreconditionFailed("Object Storage precondition failed") from error
        raise error

    def _run(self, command: list[str]) -> dict[str, Any]:
        try:
            return self._transport(command)
        except (subprocess.CalledProcessError, OciExecutionError) as error:
            self._translate_error(error)
            raise AssertionError("unreachable")

    def head(self, uri: str) -> ObjectMetadata | None:
        try:
            response = self._run(self._command("head", uri))
        except ObjectNotFound:
            return None
        etag = _response_etag(response)
        if etag is None:
            raise ReservationError(f"OCI head response has no ETag: {uri}")
        return ObjectMetadata(etag)

    def get(self, uri: str) -> StoredObject:
        descriptor, temporary = tempfile.mkstemp(
            prefix="pipeline-reservation-", suffix=".json"
        )
        os.close(descriptor)
        os.unlink(temporary)
        try:
            response = self._run(self._command("get", uri) + ["--file", temporary])
            etag = _response_etag(response)
            if etag is None:
                metadata = self.head(uri)
                if metadata is None:
                    raise ObjectNotFound(uri)
                etag = metadata.etag
            return StoredObject(Path(temporary).read_bytes(), etag)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def put(
        self,
        uri: str,
        data: bytes,
        *,
        no_overwrite: bool = False,
        if_match: str | None = None,
    ) -> ObjectMetadata:
        if no_overwrite and if_match is not None:
            raise ValueError("put accepts no_overwrite or if_match, not both")
        descriptor, temporary = tempfile.mkstemp(
            prefix="pipeline-reservation-", suffix=".json"
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
            command = self._command("put", uri) + ["--file", temporary]
            if no_overwrite:
                command.append("--no-overwrite")
            if if_match is not None:
                command += ["--if-match", if_match, "--force"]
            response = self._run(command)
            etag = _response_etag(response)
            if etag is None:
                metadata = self.head(uri)
                if metadata is None:
                    raise ReservationError(f"OCI put succeeded but object is absent: {uri}")
                return metadata
            return ObjectMetadata(etag)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def delete(self, uri: str, *, if_match: str | None = None) -> None:
        command = self._command("delete", uri) + ["--force"]
        if if_match is not None:
            command += ["--if-match", if_match]
        self._run(command)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()


def _read_json(storage: Storage, uri: str) -> tuple[dict[str, Any], str]:
    stored = storage.get(uri)
    try:
        payload = json.loads(stored.data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReservationError(f"invalid JSON object at {uri}: {error}") from error
    if not isinstance(payload, dict):
        raise ReservationError(f"JSON object at {uri} must contain an object")
    return payload, stored.etag


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any, location: str) -> datetime:
    if not isinstance(value, str):
        raise ReservationError(f"{location} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ReservationError(f"{location} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ReservationError(f"{location} must include a timezone")
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class _Lease:
    payload: dict[str, Any]
    etag: str


def _lease_payload(
    environment: str, run_id: str, product: str, owner: str, ttl_seconds: int
) -> dict[str, Any]:
    now = _now()
    return {
        "artifact_type": "pipeline_environment_lease",
        "schema_version": SCHEMA_VERSION,
        "environment": environment,
        "run_id": run_id,
        "product": product,
        "owner": owner,
        "acquired_at": _timestamp(now),
        "expires_at": _timestamp(now + timedelta(seconds=ttl_seconds)),
    }


def _acquire_lease(
    storage: Storage,
    uri: str,
    environment: str,
    run_id: str,
    product: str,
    ttl_seconds: int,
) -> _Lease:
    owner = secrets.token_hex(16)
    for _ in range(MAX_CAS_ATTEMPTS):
        payload = _lease_payload(environment, run_id, product, owner, ttl_seconds)
        try:
            metadata = storage.put(uri, _json_bytes(payload), no_overwrite=True)
            return _Lease(payload, metadata.etag)
        except PreconditionFailed:
            pass

        try:
            current, etag = _read_json(storage, uri)
        except ObjectNotFound:
            continue
        if current.get("environment") != environment:
            raise ReservationError("lease environment does not match the requested environment")
        if _parse_timestamp(current.get("expires_at"), "lease.expires_at") > _now():
            raise LeaseUnavailable(
                f"environment {environment!r} is leased by run {current.get('run_id')!r}"
            )
        try:
            metadata = storage.put(uri, _json_bytes(payload), if_match=etag)
            return _Lease(payload, metadata.etag)
        except PreconditionFailed:
            continue
    raise ReservationError("lease CAS retries exhausted")


def _renew_lease(storage: Storage, uri: str, lease: _Lease, ttl_seconds: int) -> _Lease:
    payload = {
        **lease.payload,
        "expires_at": _timestamp(_now() + timedelta(seconds=ttl_seconds)),
    }
    try:
        metadata = storage.put(uri, _json_bytes(payload), if_match=lease.etag)
    except PreconditionFailed as error:
        raise ReservationError("lost environment lease while renewing it") from error
    return _Lease(payload, metadata.etag)


def _positive_or_zero(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReservationError(f"{location} must be a non-negative integer")
    return value


def _validate_selected_lote(
    descriptor: Any, plan_tables: Mapping[str, Any], request_uri: str
) -> None:
    expected_keys = {
        "artifact_type",
        "schema_version",
        "snapshot_id",
        "snapshot_uri",
        "table_set",
        "tables",
        "selective_missing",
    }
    if not isinstance(descriptor, Mapping) or set(descriptor) != expected_keys:
        raise ReservationError("plan.selected_lote must have the exact plan-v2 structure")
    if descriptor.get("artifact_type") != "engorda_selected_lote":
        raise ReservationError("plan.selected_lote.artifact_type is invalid")
    if descriptor.get("schema_version") != 1:
        raise ReservationError("plan.selected_lote.schema_version must be 1")

    snapshot_id = descriptor.get("snapshot_id")
    try:
        canonical_snapshot_id = str(UUID(snapshot_id))
    except (AttributeError, TypeError, ValueError) as error:
        raise ReservationError("plan.selected_lote.snapshot_id must be a canonical UUID") from error
    if canonical_snapshot_id != snapshot_id:
        raise ReservationError("plan.selected_lote.snapshot_id must be a canonical UUID")
    snapshot_uri = descriptor.get("snapshot_uri")
    if not isinstance(snapshot_uri, str) or not snapshot_uri.strip():
        raise ReservationError("plan.selected_lote.snapshot_uri must be a non-empty string")
    expected_snapshot_uri = (
        f"{request_uri.rstrip('/')}.selected-lote/{canonical_snapshot_id}"
    )
    if snapshot_uri != expected_snapshot_uri:
        raise ReservationError(
            "plan.selected_lote.snapshot_uri must derive from request_uri and snapshot_id"
        )

    expected_tables = sorted(plan_tables)
    table_set = descriptor.get("table_set")
    if table_set != expected_tables:
        raise ReservationError(
            "plan.selected_lote.table_set must exactly match sorted plan.tables"
        )
    snapshot_tables = descriptor.get("tables")
    if not isinstance(snapshot_tables, Mapping) or set(snapshot_tables) != set(expected_tables):
        raise ReservationError("plan.selected_lote.tables must exactly match plan.tables")
    for table in expected_tables:
        entry = snapshot_tables[table]
        location = f"plan.selected_lote.tables.{table}"
        if not isinstance(entry, Mapping) or set(entry) != {"path", "row_count", "schema"}:
            raise ReservationError(f"{location} must have path, row_count, and schema")
        if entry.get("path") != f"{snapshot_uri}/tables/{table}":
            raise ReservationError(f"{location}.path is invalid")
        row_count = _positive_or_zero(entry.get("row_count"), f"{location}.row_count")
        if row_count != plan_tables[table]["source_count"]:
            raise ReservationError(f"{location}.row_count must match plan.tables source_count")
        if not isinstance(entry.get("schema"), dict):
            raise ReservationError(f"{location}.schema must be an object")

    selective = descriptor.get("selective_missing")
    selective_keys = {"present", "path", "row_count", "schema"}
    if (
        not isinstance(selective, Mapping)
        or set(selective) != selective_keys
        or type(selective.get("present")) is not bool
    ):
        raise ReservationError(
            "plan.selected_lote.selective_missing must have the exact presence contract"
        )
    if selective["present"]:
        if selective.get("path") != f"{snapshot_uri}/selective_missing":
            raise ReservationError("plan.selected_lote.selective_missing.path is invalid")
        _positive_or_zero(
            selective.get("row_count"),
            "plan.selected_lote.selective_missing.row_count",
        )
        if not isinstance(selective.get("schema"), dict):
            raise ReservationError(
                "plan.selected_lote.selective_missing.schema must be an object"
            )
    elif dict(selective) != {
        "present": False,
        "path": None,
        "row_count": 0,
        "schema": None,
    }:
        raise ReservationError(
            "plan.selected_lote.selective_missing absent contract is invalid"
        )


def _validate_plan(plan: dict[str, Any], product: str, request_uri: str) -> None:
    if plan.get("artifact_type") != "engorda_plan":
        raise ReservationError("reservation request is not an engorda plan")
    if plan.get("schema_version") != 2:
        raise ReservationError(
            "engorda plan schema_version is incompatible; regenerate it with plan-v2 "
            "before reserving ranges"
        )
    if not isinstance(plan.get("plan_id"), str) or not plan["plan_id"]:
        raise ReservationError("engorda plan must contain plan_id")
    body = {key: value for key, value in plan.items() if key != "plan_id"}
    expected_plan_id = hashlib.sha256(
        json.dumps(
            body, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    if plan["plan_id"] != expected_plan_id:
        raise ReservationError("engorda plan_id does not match plan content")
    if plan.get("product") != product:
        raise ReservationError("engorda plan product does not match requested product")
    if not isinstance(plan.get("tables"), dict) or not plan["tables"]:
        raise ReservationError("engorda plan tables must be an object")
    for table, table_plan in plan["tables"].items():
        if not isinstance(table, str) or not table or not isinstance(table_plan, Mapping):
            raise ReservationError("engorda plan contains an invalid table entry")
        _positive_or_zero(
            table_plan.get("source_count"), f"plan.tables.{table}.source_count"
        )
        pk = table_plan.get("pk")
        if not isinstance(pk, Mapping):
            raise ReservationError(f"plan.tables.{table}.pk must be an object")
        count = _positive_or_zero(
            pk.get("count_demand"), f"plan.tables.{table}.pk.count_demand"
        )
        rule = pk.get("rule")
        if rule not in {"OFFSET_PROPRIO", "VIA_PAI"}:
            raise ReservationError(f"plan.tables.{table}.pk.rule is invalid")
        step = pk.get("step")
        if isinstance(step, bool) or not isinstance(step, int) or step < 1:
            raise ReservationError(f"plan.tables.{table}.pk.step must be a positive integer")
        minimum = pk.get("minimum_start")
        if rule == "OFFSET_PROPRIO":
            if isinstance(minimum, bool) or not isinstance(minimum, int):
                raise ReservationError(
                    f"plan.tables.{table}.pk.minimum_start must be an integer"
                )
        elif count != 0 or minimum is not None:
            raise ReservationError(f"plan.tables.{table}.pk VIA_PAI must not request a range")
    _validate_selected_lote(plan.get("selected_lote"), plan["tables"], request_uri)
    for section, field in (
        ("cod_operacao", "count"),
        ("meu_numero", "ordinal_count_demand"),
    ):
        value = plan.get(section)
        if not isinstance(value, Mapping):
            raise ReservationError(f"plan.{section} must be an object")
        _positive_or_zero(value.get(field), f"plan.{section}.{field}")


def _new_ledger(environment: str) -> dict[str, Any]:
    return {
        "artifact_type": "engorda_reservation_ledger",
        "schema_version": SCHEMA_VERSION,
        "environment": environment,
        "revision": 0,
        "table_pks": {},
        "meu_numero": {"prefixes": {}},
        "reservations": [],
    }


def _validate_ledger(ledger: dict[str, Any], environment: str) -> None:
    expected = ("engorda_reservation_ledger", SCHEMA_VERSION, environment)
    actual = (
        ledger.get("artifact_type"),
        ledger.get("schema_version"),
        ledger.get("environment"),
    )
    if actual != expected:
        raise ReservationError("ledger identity or schema does not match the environment")
    if not isinstance(ledger.get("table_pks"), dict):
        raise ReservationError("ledger.table_pks must be an object")
    meu_numero = ledger.get("meu_numero")
    if not isinstance(meu_numero, dict) or not isinstance(meu_numero.get("prefixes"), dict):
        raise ReservationError("ledger.meu_numero.prefixes must be an object")
    if not isinstance(ledger.get("reservations"), list):
        raise ReservationError("ledger.reservations must be an array")


def _allocate_artifact(
    ledger: dict[str, Any],
    plan: dict[str, Any],
    run_id: str,
    product: str,
    reservation_uri: str,
) -> dict[str, Any]:
    table_pks: dict[str, dict[str, int]] = {}
    for table in sorted(plan["tables"]):
        request = plan["tables"][table]["pk"]
        count = request["count_demand"]
        if count == 0:
            continue
        state = ledger["table_pks"].setdefault(table, {})
        previous_next = state.get("next_start", request["minimum_start"])
        if isinstance(previous_next, bool) or not isinstance(previous_next, int):
            raise ReservationError(f"ledger.table_pks.{table}.next_start must be an integer")
        start = max(previous_next, request["minimum_start"])
        end = start + (count - 1) * request["step"]
        state["next_start"] = end + request["step"]
        table_pks[table] = {
            "count": count,
            "start": start,
            "end": end,
            "step": request["step"],
        }

    meu_count = plan["meu_numero"]["ordinal_count_demand"]
    meu_numero: dict[str, int | str | None]
    if meu_count == 0:
        meu_numero = {"prefix": None, "count": 0, "start": None, "end": None}
    else:
        if meu_count > MAX_MEU_NUMERO_ORDINAL:
            raise ReservationError("meu_numero demand exceeds one prefix's ordinal capacity")
        prefixes = ledger["meu_numero"]["prefixes"]
        requested_prefix = plan["meu_numero"].get("requested_prefix")
        if requested_prefix is not None:
            if (not isinstance(requested_prefix, str)
                    or not requested_prefix.isdigit()
                    or len(requested_prefix) != 3
                    or requested_prefix[0] == "0"):
                raise ReservationError("plan.meu_numero.requested_prefix is invalid")
            prefix_candidates = (int(requested_prefix),)
        else:
            prefix_candidates = range(
                MIN_MEU_NUMERO_PREFIX, MAX_MEU_NUMERO_PREFIX + 1
            )
        for prefix in prefix_candidates:
            key = str(prefix)
            start = prefixes.get(key, 1)
            if isinstance(start, bool) or not isinstance(start, int) or start < 1:
                raise ReservationError(f"ledger.meu_numero.prefixes.{key} must be an ordinal")
            end = start + meu_count - 1
            if end <= MAX_MEU_NUMERO_ORDINAL:
                prefixes[key] = end + 1
                meu_numero = {
                    "prefix": key,
                    "count": meu_count,
                    "start": start,
                    "end": end,
                }
                break
        else:
            scope = requested_prefix or "100..999"
            raise ReservationError(f"meu_numero prefix {scope} is exhausted")

    artifact = {
        "artifact_type": "engorda_reservation",
        "schema_version": SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "product": product,
        "table_pks": table_pks,
        "cod_operacao": {
            "strategy": "oracle_allocator",
            "count": plan["cod_operacao"]["count"],
        },
        "meu_numero": meu_numero,
    }
    ledger["revision"] = _positive_or_zero(ledger.get("revision"), "ledger.revision") + 1
    ledger["reservations"].append(
        {
            "run_id": run_id,
            "product": product,
            "plan_id": plan["plan_id"],
            "reservation_uri": reservation_uri,
            "reserved_at": _timestamp(_now()),
            "reservation": deepcopy(artifact),
        }
    )
    return artifact


def _reserve_in_ledger(
    storage: Storage,
    ledger_uri: str,
    environment: str,
    run_id: str,
    product: str,
    reservation_uri: str,
    plan: dict[str, Any],
) -> dict[str, Any]:
    for _ in range(MAX_CAS_ATTEMPTS):
        try:
            current, etag = _read_json(storage, ledger_uri)
        except ObjectNotFound:
            current, etag = _new_ledger(environment), None
        _validate_ledger(current, environment)
        updated = deepcopy(current)
        artifact = _allocate_artifact(updated, plan, run_id, product, reservation_uri)
        try:
            storage.put(
                ledger_uri,
                _json_bytes(updated),
                no_overwrite=etag is None,
                if_match=etag,
            )
            return artifact
        except PreconditionFailed:
            continue
    raise ReservationError("ledger CAS retries exhausted")


def _existing_reservation(
    storage: Storage, uri: str, plan: dict[str, Any], product: str
) -> tuple[dict[str, Any], str] | None:
    if storage.head(uri) is None:
        return None
    payload, etag = _read_json(storage, uri)
    expected = {
        "artifact_type": "engorda_reservation",
        "schema_version": SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "product": product,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ReservationError(f"immutable reservation URI contains a different artifact: {uri}")
    return payload, etag


def reserve_ranges(
    environment: str,
    run_id: str,
    product: str,
    request_uri: str,
    reservation_uri: str,
    lease_uri: str,
    ledger_uri: str,
    auth: Mapping[str, Any],
    storage: Storage | None = None,
    lease_ttl_seconds: int = 300,
) -> dict[str, Any]:
    """Burn ledger ranges under a lease, then publish one immutable reservation."""
    if not all(isinstance(value, str) and value for value in (environment, run_id, product)):
        raise ReservationError("environment, run_id, and product must be non-empty strings")
    if isinstance(lease_ttl_seconds, bool) or not isinstance(lease_ttl_seconds, int):
        raise ReservationError("lease_ttl_seconds must be a positive integer")
    if lease_ttl_seconds < 1:
        raise ReservationError("lease_ttl_seconds must be a positive integer")
    storage = storage or OciCliStorage(auth)
    plan, _ = _read_json(storage, request_uri)
    _validate_plan(plan, product, request_uri)

    existing = _existing_reservation(storage, reservation_uri, plan, product)
    if existing is not None:
        _, etag = existing
        return {"uri": reservation_uri, "etag": etag}

    lease = _acquire_lease(storage, lease_uri, environment, run_id, product, lease_ttl_seconds)
    try:
        artifact = _reserve_in_ledger(
            storage,
            ledger_uri,
            environment,
            run_id,
            product,
            reservation_uri,
            plan,
        )
        lease = _renew_lease(storage, lease_uri, lease, lease_ttl_seconds)
        try:
            metadata = storage.put(reservation_uri, _json_bytes(artifact), no_overwrite=True)
        except PreconditionFailed:
            existing = _existing_reservation(storage, reservation_uri, plan, product)
            if existing is None or existing[0] != artifact:
                raise ReservationError(
                    f"immutable reservation URI was concurrently populated: {reservation_uri}"
                )
            metadata = ObjectMetadata(existing[1])
        return {"uri": reservation_uri, "etag": metadata.etag}
    finally:
        try:
            storage.delete(lease_uri, if_match=lease.etag)
        except (ObjectNotFound, PreconditionFailed):
            pass


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        raise PipelineError(f"refusing to replace immutable manifest: {path}")
    AtomicManifest(path, payload)


def _need_string(payload: dict[str, Any], key: str, location: str = "config") -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise PipelineError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _reject_secrets(value: Any, location: str = "config") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(word in normalized for word in ("password", "secret", "token")):
                raise PipelineError(f"{location}.{key} must not contain credentials")
            _reject_secrets(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secrets(child, f"{location}[{index}]")


def load_config(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open(encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"cannot read config {path}: {exc}") from exc
    if not isinstance(config, dict):
        raise PipelineError("config must be one JSON object")
    if config.get("version") != CONFIG_VERSION:
        raise PipelineError(f"config.version must be {CONFIG_VERSION}")
    if "environments" in config:
        raise PipelineError("config represents exactly one environment; remove environments")
    misplaced_auth = sorted(
        key for key in ("profile", "config_file", "auth", "cert_bundle") if key in config
    )
    if misplaced_auth:
        raise PipelineError(
            "operator OCI authentication belongs on CLI flags, not config: "
            + ", ".join(misplaced_auth)
        )
    _reject_secrets(config)
    _need_string(config, "environment")
    _need_string(config, "compartment_id")
    for key in ("artifact_root", "manifest_root"):
        if not _need_string(config, key).startswith("oci://"):
            raise PipelineError(f"config.{key} must be an oci:// URI")

    applications = config.get("applications")
    if not isinstance(applications, dict):
        raise PipelineError("config.applications must be an object")
    for name in ("engorda_plan", "engorda_materialize", "validate"):
        _need_string(applications, name, "config.applications")

    reservations = config.get("reservations")
    if not isinstance(reservations, dict):
        raise PipelineError("config.reservations must be an object")
    for name in ("lease_uri", "ledger_uri"):
        if not _need_string(reservations, name, "config.reservations").startswith("oci://"):
            raise PipelineError(f"config.reservations.{name} must be an oci:// URI")

    configured_products = config.get("products")
    if not isinstance(configured_products, dict) or not configured_products:
        raise PipelineError(
            "config.products must declare at least one environment product capability"
        )
    unsupported = sorted(set(configured_products) - set(PRODUCTS))
    if unsupported:
        raise PipelineError(f"unsupported generator products in config: {', '.join(unsupported)}")
    for product, settings in configured_products.items():
        if not isinstance(settings, dict) or not isinstance(settings.get("capabilities"), list):
            raise PipelineError(f"config.products.{product}.capabilities must be a list")
        capabilities = settings["capabilities"]
        if any(stage not in TRACER_STAGES for stage in capabilities):
            raise PipelineError(f"config.products.{product} has an unsupported capability")
        if len(capabilities) != len(set(capabilities)):
            raise PipelineError(f"config.products.{product}.capabilities contains duplicates")
        unsupported_capabilities = sorted(
            set(capabilities) - set(PRODUCTS[product]["capabilities"])
        )
        if unsupported_capabilities:
            raise PipelineError(
                f"config.products.{product} enables unsupported registry capabilities: "
                f"{', '.join(unsupported_capabilities)}"
            )

    defaults = config.get("stage_defaults", {})
    if not isinstance(defaults, dict) or any(
        stage not in TRACER_STAGES or not isinstance(values, dict)
        for stage, values in defaults.items()
    ):
        raise PipelineError("config.stage_defaults may contain only engorda/validate objects")
    return config


def parse_products(values: Sequence[str]) -> list[str]:
    products = list(
        dict.fromkeys(part.strip() for value in values for part in value.split(",") if part.strip())
    )
    if not products:
        raise PipelineError("at least one --product is required")
    unsupported = [product for product in products if product not in PRODUCTS]
    if unsupported:
        raise PipelineError(
            f"unsupported generator product(s): {', '.join(unsupported)}; supported: "
            f"{', '.join(PRODUCTS)}"
        )
    return products


def require_configured_products(config: Mapping[str, Any], products: Sequence[str]) -> None:
    missing = sorted(set(products) - set(config["products"]))
    if missing:
        raise PipelineError(
            "selected product(s) are not enabled in config.products: " + ", ".join(missing)
        )


def selected_stages(first: str, last: str) -> tuple[str, ...]:
    if first not in PUBLIC_STAGES or last not in PUBLIC_STAGES:
        raise PipelineError("unknown pipeline stage")
    if PUBLIC_STAGES.index(first) > PUBLIC_STAGES.index(last):
        raise PipelineError(f"reversed stage interval: {first} through {last}")
    interval = PUBLIC_STAGES[PUBLIC_STAGES.index(first) : PUBLIC_STAGES.index(last) + 1]
    if any(stage not in TRACER_STAGES for stage in interval):
        raise PipelineError(
            "first tracer supports only the inclusive engorda through validate slice"
        )
    return interval


def read_upstream_manifest(path: str | Path, environment: str) -> dict[str, Any]:
    try:
        with Path(path).open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(f"cannot read upstream manifest {path}: {exc}") from exc
    if not isinstance(manifest, dict) or manifest.get("manifest_version") != MANIFEST_VERSION:
        raise PipelineError("upstream manifest has an unsupported manifest_version")
    if manifest.get("environment") != environment:
        raise PipelineError("upstream manifest environment does not match config")
    if manifest.get("status") not in {"ADOPTED", "SUCCEEDED"}:
        raise PipelineError("upstream manifest must be ADOPTED or SUCCEEDED")
    return manifest


def _artifact_uri(manifest: dict[str, Any], name: str, product: str | None = None) -> str:
    try:
        artifact = manifest["artifacts"]
        if product is not None:
            artifact = artifact["products"][product]
        value = artifact[name]
        uri = value["uri"] if isinstance(value, dict) else value
    except (KeyError, TypeError) as exc:
        label = f"{product}.{name}" if product else name
        raise PipelineError(f"upstream manifest lacks required {label} lineage") from exc
    if not isinstance(uri, str) or not uri.startswith("oci://"):
        raise PipelineError(f"upstream {name} lineage must be an oci:// URI")
    return uri


def _positive(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise PipelineError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"{name} must be a positive integer") from exc
    if result < 1:
        raise PipelineError(f"{name} must be a positive integer")
    return result


def parse_stage_overrides(values: Sequence[str]) -> dict[str, dict[str, dict[str, Any]]]:
    overrides: dict[str, dict[str, dict[str, Any]]] = {}
    for raw in values:
        key, separator, raw_value = raw.partition("=")
        parts = key.split(".")
        if not separator or len(parts) != 3:
            raise PipelineError(
                "--set must use product.stage.key=value"
            )
        product, stage, option = parts
        if product not in PRODUCTS:
            raise PipelineError(f"--set references unsupported product {product!r}")
        if stage not in OVERRIDE_KEYS or option not in OVERRIDE_KEYS[stage]:
            raise PipelineError(f"--set option is not allowed: {key}")
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        overrides.setdefault(product, {}).setdefault(stage, {})[option] = value
    return overrides


def build_engorda_plan_argv(
    product: str,
    raw_uri: str,
    faltantes_uri: str,
    paths: dict[str, str],
    options: dict[str, Any],
) -> list[str]:
    """Build the proposed engorda selection/cardinality planning contract."""
    argv = [
        "--phase",
        "plan",
        "--produto",
        product,
        "--raw-uri",
        raw_uri,
        "--output-uri",
        paths["synthetic"],
        "--faltantes-parquet",
        faltantes_uri,
        "--plan-uri",
        paths["selection_plan"],
        "--n-instrumentos",
        str(_positive(options.get("n_instrumentos"), "engorda.n_instrumentos")),
        "--fator-k",
        str(_positive(options.get("fator_k", 1), "engorda.fator_k")),
        "--seed",
        str(int(options.get("seed", 42))),
    ]
    for key, flag in (
        ("specs", "--specs"),
        ("meu_numero_prefix", "--meu-numero-prefix"),
        ("query_num_if_sql", "--query-num-if-sql"),
    ):
        if options.get(key) is not None:
            argv += [flag, str(options[key])]
    return argv


def build_engorda_materialize_argv(
    product: str,
    raw_uri: str,
    faltantes_uri: str,
    paths: dict[str, str],
    options: dict[str, Any],
) -> list[str]:
    """Build deterministic materialization argv from selection and reservations."""
    argv = [
        "--phase",
        "materialize",
        "--produto",
        product,
        "--raw-uri",
        raw_uri,
        "--output-uri",
        paths["synthetic"],
        "--faltantes-parquet",
        faltantes_uri,
        "--plan-uri",
        paths["selection_plan"],
        "--reservation-uri",
        paths["reservations"],
    ]
    if options.get("specs") is not None:
        argv += ["--specs", str(options["specs"])]
    if options.get("query_num_if_sql") is not None:
        argv += ["--query-num-if-sql", str(options["query_num_if_sql"])]
    return argv


def build_validator_argv(
    product: str, synthetic_uri: str, report_uri: str, options: dict[str, Any]
) -> list[str]:
    argv = [
        "--product",
        PRODUCTS[product]["validator_product"],
        "--input-base",
        synthetic_uri,
        "--report-path",
        report_uri,
        "--fail-severity",
        str(options.get("fail_severity", "error")),
        "--validate-against",
        str(options.get("validate_against", "union")),
        "--allow-partial",
    ]
    for key, flag in (
        ("shape_baseline", "--shape-baseline"),
        ("application_capacity_contract", "--application-capacity-contract"),
    ):
        if options.get(key) is not None:
            argv += [flag, str(options[key])]
    return argv


def _product_paths(run_root: str, product: str) -> dict[str, str]:
    base = uri_join(run_root, "products", product)
    return {
        "selection_plan": uri_join(base, "engorda", "selection-plan.json"),
        "reservations": uri_join(base, "engorda", "reservations.json"),
        "synthetic": uri_join(base, "synthetic"),
        "validation_report": uri_join(base, "validation", "report.json"),
    }


def build_pipeline_plan(
    config: dict[str, Any], args: SimpleNamespace, upstream: dict[str, Any]
) -> dict[str, Any]:
    products = parse_products(args.product)
    require_configured_products(config, products)
    upstream_products = upstream.get("products")
    if not isinstance(upstream_products, list):
        raise PipelineError("upstream manifest must declare its adopted products")
    unavailable_inputs = sorted(set(products) - set(upstream_products))
    if unavailable_inputs:
        raise PipelineError(
            "upstream manifest does not cover product(s): "
            + ", ".join(unavailable_inputs)
        )
    stages = selected_stages(args.from_stage, args.to_stage)
    for product in products:
        capabilities = config["products"][product]["capabilities"]
        unavailable = [stage for stage in stages if stage not in capabilities]
        if unavailable:
            raise PipelineError(
                f"product {product} lacks requested stage capability: {', '.join(unavailable)}"
            )

    run_root = uri_join(config["artifact_root"], config["environment"], args.run_id)
    manifest_uri = uri_join(
        config["manifest_root"], config["environment"], args.run_id, "manifest.json"
    )
    base_engorda_options = dict(config.get("stage_defaults", {}).get("engorda", {}))
    base_validate_options = dict(config.get("stage_defaults", {}).get("validate", {}))
    for name in ("n_instrumentos", "fator_k", "seed"):
        value = getattr(args, name, None)
        if value is not None:
            base_engorda_options[name] = value
    overrides = parse_stage_overrides(args.set_values)

    nodes: dict[str, dict[str, Any]] = {}
    artifacts: dict[str, Any] = {"products": {}}
    raw_uri = faltantes_uri = None
    if "engorda" in stages:
        raw_uri = _artifact_uri(upstream, "raw")
        faltantes_uri = _artifact_uri(upstream, "faltantes")
        artifacts["raw"] = dict(upstream["artifacts"]["raw"])
        artifacts["faltantes"] = dict(upstream["artifacts"]["faltantes"])

    for product in products:
        engorda_options = {
            **base_engorda_options,
            **overrides.get(product, {}).get("engorda", {}),
        }
        validate_options = {
            **base_validate_options,
            **overrides.get(product, {}).get("validate", {}),
        }
        paths = _product_paths(run_root, product)
        product_artifacts: dict[str, Any] = {}
        artifacts["products"][product] = product_artifacts
        dependency: str | None = None
        if "engorda" in stages:
            query_num_if_sql = engorda_options.get("query_num_if_sql")
            if not isinstance(query_num_if_sql, str) or not query_num_if_sql.startswith(
                "oci://"
            ):
                raise PipelineError(
                    f"product {product} requires engorda.query_num_if_sql as an "
                    "oci:// URI; Data Flow cannot read the runner's local SQL file"
                )
            for name in ("selection_plan", "reservations", "synthetic"):
                product_artifacts[name] = {
                    "uri": paths[name],
                    "producer": "current_run",
                }
            plan_id = f"{product}.engorda.plan"
            reserve_id = f"{product}.engorda.reserve"
            materialize_id = f"{product}.engorda.materialize"
            nodes[plan_id] = {
                "id": plan_id,
                "product": product,
                "stage": "engorda",
                "operation": "plan",
                "lane": "data-flow",
                "dependencies": [],
                "application_id": config["applications"]["engorda_plan"],
                "arguments": build_engorda_plan_argv(
                    product, raw_uri or "", faltantes_uri or "", paths, engorda_options
                ),
                "output_uri": paths["selection_plan"],
            }
            nodes[reserve_id] = {
                "id": reserve_id,
                "product": product,
                "stage": "engorda",
                "operation": "reserve",
                "lane": "reservation-cas",
                "dependencies": [plan_id],
                "application_id": None,
                "arguments": [],
                "request_uri": paths["selection_plan"],
                "output_uri": paths["reservations"],
            }
            nodes[materialize_id] = {
                "id": materialize_id,
                "product": product,
                "stage": "engorda",
                "operation": "materialize",
                "lane": "data-flow",
                "dependencies": [reserve_id],
                "application_id": config["applications"]["engorda_materialize"],
                "arguments": build_engorda_materialize_argv(
                    product,
                    raw_uri or "",
                    faltantes_uri or "",
                    paths,
                    engorda_options,
                ),
                "output_uri": paths["synthetic"],
            }
            dependency = materialize_id
        if "validate" in stages:
            synthetic_uri = (
                paths["synthetic"]
                if dependency
                else _artifact_uri(upstream, "synthetic", product)
            )
            product_artifacts["synthetic"] = {
                "uri": synthetic_uri,
                "producer": "current_run" if dependency else "upstream",
            }
            product_artifacts["validation_report"] = {
                "uri": paths["validation_report"],
                "producer": "current_run",
            }
            validate_id = f"{product}.validate"
            nodes[validate_id] = {
                "id": validate_id,
                "product": product,
                "stage": "validate",
                "operation": "validate",
                "lane": "data-flow",
                "dependencies": [dependency] if dependency else [],
                "application_id": config["applications"]["validate"],
                "arguments": build_validator_argv(
                    product, synthetic_uri, paths["validation_report"], validate_options
                ),
                "input_uri": synthetic_uri,
                "output_uri": paths["validation_report"],
            }

    return {
        "run_id": args.run_id,
        "environment": config["environment"],
        "interval": {"from": args.from_stage, "to": args.to_stage, "inclusive": True},
        "products": products,
        "run_root": run_root,
        "manifest_uri": manifest_uri,
        "artifacts": artifacts,
        "nodes": nodes,
        "data_flow_options": {
            key: getattr(args, key)
            for key in (
                "num_executors", "driver_shape", "executor_shape",
                "driver_shape_config", "executor_shape_config",
            )
            if getattr(args, key) is not None
        },
        "reservation_contract": {
            "lease_uri": config["reservations"]["lease_uri"],
            "ledger_uri": config["reservations"]["ledger_uri"],
            "reserved_kinds": ["table_pk", "meu_numero_ordinal"],
            "allocator_kinds": ["cod_if", "cod_operacao"],
            "reuse": "forbidden",
        },
    }


def _state_from_response(response: Any) -> str:
    if isinstance(response, str):
        return response.upper()
    if isinstance(response, dict):
        data = response.get("data", response)
        state = data.get("lifecycle-state", data.get("lifecycle_state", data.get("state")))
        if isinstance(state, str):
            return state.upper()
    raise PipelineError(f"OCI adapter returned an invalid lifecycle response: {response!r}")


def _run_id_from_response(response: Any) -> str:
    if isinstance(response, str) and response:
        return response
    if isinstance(response, dict):
        data = response.get("data", response)
        if isinstance(data.get("id"), str):
            return data["id"]
    raise PipelineError(f"OCI adapter returned an invalid create response: {response!r}")


def _validation_gate(
    report: dict[str, Any], *, expected_product: str, expected_input: str
) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise PipelineError("validation report must be a JSON object")
    verdict = str(report.get("verdict", "")).upper()
    counts = report.get("counts", {})
    errors = counts.get("error", counts.get("errors")) if isinstance(counts, dict) else None
    if isinstance(errors, bool) or not isinstance(errors, int):
        raise PipelineError("validation report counts.error must be an integer")
    product_matches = report.get("product") == expected_product
    resolved_input = report.get("resolved_input")
    input_matches = (
        isinstance(resolved_input, str)
        and resolved_input.rstrip("/") == expected_input.rstrip("/")
    )
    accepted = (
        errors == 0
        and verdict in {"PASS", "PARTIAL"}
        and product_matches
        and input_matches
    )
    return {
        "verdict": verdict,
        "error_count": errors,
        "product_matches": product_matches,
        "input_matches": input_matches,
        "accepted": accepted,
    }


def _execute_remote_node(
    node: dict[str, Any],
    config: dict[str, Any],
    pipeline_run_id: str,
    runtime_options: dict[str, Any],
    adapter: Any,
    auth: dict[str, str],
    max_retries: int,
    poll_seconds: float,
    active: dict[str, str],
    active_lock: threading.Lock,
    stop: threading.Event,
    on_attempt: Callable[[str, dict[str, Any]], None],
    progress: ProgressReporter,
) -> NodeResult:
    attempts: list[dict[str, Any]] = []
    for attempt_number in range(1, max_retries + 2):
        if stop.is_set():
            return NodeResult("CANCELLED", attempts, {})
        display_name = (
            f"{node['id'].replace('.', '-')}-{config['environment']}-"
            f"{pipeline_run_id}-a{attempt_number}"
        )
        data_flow_options = {
            "application_id": node["application_id"],
            "compartment_id": config["compartment_id"],
            **runtime_options,
            **auth,
        }
        progress.emit(
            f"[launch] {node['id']} attempt={attempt_number}"
        )
        response = adapter.create_run(
            list(node["arguments"]),
            display_name,
            data_flow_options,
        )
        data_flow_run_id = _run_id_from_response(response)
        progress.emit(
            f"[submit] {node['id']} attempt={attempt_number} run_id={data_flow_run_id}"
        )
        attempt = {
            "attempt": attempt_number,
            "run_id": data_flow_run_id,
            "application_id": node["application_id"],
            "display_name": display_name,
            "arguments": list(node["arguments"]),
            "state": "RUNNING",
        }
        attempts.append(attempt)
        with active_lock:
            cancel_after_create = stop.is_set()
            if not cancel_after_create:
                active[data_flow_run_id] = node["id"]
        on_attempt(node["id"], attempt)
        if cancel_after_create:
            adapter.cancel_run(data_flow_run_id, data_flow_options)
            attempt["state"] = "CANCELLED"
            on_attempt(node["id"], attempt)
            return NodeResult("CANCELLED", attempts, {})
        try:
            while not stop.is_set():
                state = _state_from_response(
                    adapter.get_run_state(data_flow_run_id, data_flow_options)
                )
                progress.emit(
                    f"[poll] {node['id']} attempt={attempt_number} "
                    f"run_id={data_flow_run_id} state={state}"
                )
                if state in TERMINAL_SUCCESS | TERMINAL_FAILURE:
                    break
                if state not in RUNNING_STATES:
                    raise PipelineError(f"unknown Data Flow lifecycle state {state!r}")
                if poll_seconds:
                    stop.wait(poll_seconds)
            else:
                state = "CANCELLED"
        finally:
            with active_lock:
                active.pop(data_flow_run_id, None)
        attempt["state"] = state
        on_attempt(node["id"], attempt)
        if state in TERMINAL_SUCCESS:
            detail: dict[str, Any] = {}
            if node["operation"] == "validate":
                report = adapter.read_json(node["output_uri"], auth=auth)
                detail["validation"] = _validation_gate(
                    report,
                    expected_product=PRODUCTS[node["product"]]["validator_product"],
                    expected_input=node["input_uri"],
                )
                if not detail["validation"]["accepted"]:
                    progress.emit(
                        f"[failed] {node['id']} FAILED attempt={attempt_number} "
                        f"run_id={data_flow_run_id} validation-gate"
                    )
                    return NodeResult("FAILED", attempts, detail)
            describe = getattr(adapter, "describe_uri", None)
            if describe is not None and node.get("output_uri"):
                detail["output_metadata"] = describe(
                    node["output_uri"], auth=auth
                )
            progress.emit(f"[done] {node['id']} SUCCEEDED")
            return NodeResult("SUCCEEDED", attempts, detail)
        progress.emit(
            f"[failed] {node['id']} {state} attempt={attempt_number} "
            f"run_id={data_flow_run_id}"
        )
        if stop.is_set():
            return NodeResult("CANCELLED", attempts, {})
    return NodeResult("FAILED", attempts, {})


def _execute_reservation_node(
    node: dict[str, Any],
    plan: dict[str, Any],
    adapter: Any,
    auth: dict[str, str],
    progress: ProgressReporter,
) -> NodeResult:
    progress.emit(
        f"[reserve] {node['id']} request={node['request_uri']} "
        f"output={node['output_uri']}"
    )
    result = adapter.reserve_ranges(
        environment=plan["environment"],
        run_id=plan["run_id"],
        product=node["product"],
        request_uri=node["request_uri"],
        reservation_uri=node["output_uri"],
        lease_uri=plan["reservation_contract"]["lease_uri"],
        ledger_uri=plan["reservation_contract"]["ledger_uri"],
        auth=auth,
    )
    progress.emit(f"[reserve] {node['id']} reservation complete")
    progress.emit(f"[done] {node['id']} SUCCEEDED")
    return NodeResult("SUCCEEDED", [], {"reservation": result or {}})


def execute_plan(
    plan: dict[str, Any],
    config: dict[str, Any],
    store: AtomicManifest,
    adapter: Any,
    auth: dict[str, str],
    max_concurrency: int,
    max_retries: int,
    poll_seconds: float,
    progress: ProgressReporter | None = None,
) -> int:
    progress = progress or ProgressReporter(enabled=False)
    active: dict[str, str] = {}
    active_lock = threading.Lock()
    stop = threading.Event()
    futures: dict[Future[NodeResult], str] = {}
    nodes = plan["nodes"]

    def set_node(node_id: str, **changes: Any) -> None:
        store.update(lambda payload: payload["nodes"][node_id].update(changes))

    def record_attempt(node_id: str, attempt: dict[str, Any]) -> None:
        def mutate(payload: dict[str, Any]) -> None:
            attempts = payload["nodes"][node_id].setdefault("attempts", [])
            replacement = dict(attempt)
            for index, old in enumerate(attempts):
                if old["attempt"] == replacement["attempt"]:
                    attempts[index] = replacement
                    break
            else:
                attempts.append(replacement)

        store.update(mutate)

    executor = ThreadPoolExecutor(max_workers=max_concurrency, thread_name_prefix="pipeline")
    interrupted = False
    try:
        while True:
            states = {node_id: store.payload["nodes"][node_id]["state"] for node_id in nodes}
            for node_id, node in nodes.items():
                if states[node_id] != "PENDING" or node_id in futures.values():
                    continue
                dependency_states = [states[dependency] for dependency in node["dependencies"]]
                if any(state in {"FAILED", "BLOCKED", "CANCELLED"} for state in dependency_states):
                    set_node(node_id, state="BLOCKED", finished_at=utc_now())
                    states[node_id] = "BLOCKED"
                    continue
                if not all(state == "SUCCEEDED" for state in dependency_states):
                    continue
                if node["lane"] == "reservation-cas" and any(
                    nodes[active_node_id]["lane"] == "reservation-cas"
                    for active_node_id in futures.values()
                ):
                    continue
                set_node(node_id, state="RUNNING", started_at=utc_now())
                if node["operation"] == "reserve":
                    future = executor.submit(
                        _execute_reservation_node, node, plan, adapter, auth, progress
                    )
                else:
                    future = executor.submit(
                        _execute_remote_node,
                        node,
                        config,
                        plan["run_id"],
                        plan["data_flow_options"],
                        adapter,
                        auth,
                        max_retries,
                        poll_seconds,
                        active,
                        active_lock,
                        stop,
                        record_attempt,
                        progress,
                    )
                futures[future] = node_id

            if not futures:
                if all(
                    store.payload["nodes"][node_id]["state"] in NODE_TERMINAL
                    for node_id in nodes
                ):
                    break
                raise PipelineError("pipeline scheduler cannot make progress")
            completed, _ = wait(futures, timeout=0.1, return_when=FIRST_COMPLETED)
            for future in completed:
                node_id = futures.pop(future)
                try:
                    result = future.result()
                except Exception as exc:  # branch-local isolation
                    progress.emit(
                        f"[failed] {node_id} {type(exc).__name__}: {exc}"
                    )
                    set_node(
                        node_id,
                        state="FAILED",
                        finished_at=utc_now(),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                else:
                    set_node(
                        node_id,
                        state=result.state,
                        finished_at=utc_now(),
                        attempts=result.attempts,
                        **result.detail,
                    )
    except KeyboardInterrupt:
        interrupted = True
        stop.set()
        with active_lock:
            active_ids = list(active)
        for data_flow_run_id in active_ids:
            try:
                adapter.cancel_run(data_flow_run_id, auth)
            except Exception as exc:  # cancellation is best effort but always recorded
                store.update(
                    lambda payload, run_id=data_flow_run_id, error=str(exc): payload.setdefault(
                        "cancellation_errors", {}
                    ).update({run_id: error})
                )
        for future in futures:
            future.cancel()
    finally:
        stop.set()
        executor.shutdown(wait=True, cancel_futures=True)

    if interrupted:
        def cancel_remaining(payload: dict[str, Any]) -> None:
            for node in payload["nodes"].values():
                if node["state"] in {"PENDING", "RUNNING", "RETRYING"}:
                    node["state"] = "CANCELLED"
                    node["finished_at"] = utc_now()

        store.update(cancel_remaining)
        return 130
    return 1 if any(node["state"] != "SUCCEEDED" for node in store.payload["nodes"].values()) else 0


def _initial_manifest(plan: dict[str, Any], upstream_path: str) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "kind": "pipeline-run",
        "run_id": plan["run_id"],
        "environment": plan["environment"],
        "status": "RUNNING",
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "run_root": plan["run_root"],
        "manifest_uri": plan["manifest_uri"],
        "interval": plan["interval"],
        "products": plan["products"],
        "upstream_manifest": str(Path(upstream_path).resolve()),
        "artifacts": plan["artifacts"],
        "reservation_contract": plan["reservation_contract"],
        "nodes": {
            node_id: {**node, "state": "PENDING", "attempts": []}
            for node_id, node in plan["nodes"].items()
        },
    }


def _auth_from_args(args: SimpleNamespace) -> dict[str, str]:
    return AuthOptions(
        args.profile,
        args.config_file,
        args.auth,
        args.cert_bundle,
        args.region,
    ).as_dict()


def _ensure_adapter_auth(
    adapter: Any,
    auth: dict[str, str],
    *,
    allow_prompt: bool,
    application_id: str | None = None,
    force_refresh: bool = False,
) -> None:
    ensure = getattr(adapter, "ensure_auth", None)
    if ensure is None:
        return
    ensure(
        auth,
        allow_prompt=allow_prompt,
        prompt=lambda message: click.confirm(message, default=True, err=True),
        application_id=application_id,
        force_refresh=force_refresh,
    )


def run_command(
    args: SimpleNamespace,
    adapter: Any | None,
    progress: ProgressReporter | None = None,
) -> int:
    progress = progress or ProgressReporter(enabled=False)
    config = load_config(args.config)
    upstream = read_upstream_manifest(args.upstream_manifest, config["environment"])
    plan = build_pipeline_plan(config, args, upstream)
    local_run_dir = Path(args.local_run_root) / config["environment"] / args.run_id
    manifest_path = local_run_dir / "manifest.json"
    if local_run_dir.exists():
        raise PipelineError(f"immutable local run path already exists: {local_run_dir}")
    progress.emit(
        f"[run] id={args.run_id} environment={config['environment']} "
        f"products={','.join(plan['products'])} "
        f"stages={plan['interval']['from']}..{plan['interval']['to']} "
        f"poll={args.poll_seconds:g}s concurrency={args.max_concurrency}"
    )
    if args.dry_run:
        progress.emit("[dry-run] resolved pipeline plan (no remote calls)")
        print(json.dumps({"dry_run": True, **plan}, indent=2, sort_keys=True))
        progress.emit("[done] dry-run complete (no remote calls)")
        return 0

    adapter = adapter or ModuleAdapter(
        timeout_seconds=args.oci_timeout_seconds,
        auth_refresh_seconds=args.auth_refresh_seconds,
        progress=progress,
    )
    auth = _auth_from_args(args)
    _ensure_adapter_auth(
        adapter,
        auth,
        allow_prompt=args.auth_prompt,
        application_id=config["applications"]["engorda_plan"],
        force_refresh=args.auth_refresh_seconds > 0,
    )
    immutable_outputs = [
        (f"materialize output for {node['product']}", node["output_uri"])
        for node in plan["nodes"].values()
        if node["operation"] == "materialize"
    ]
    for label, uri in (
        ("run path", plan["run_root"]),
        ("manifest", plan["manifest_uri"]),
        *immutable_outputs,
    ):
        progress.emit(f"[preflight] checking OCI {label}: {uri}")
        if adapter.uri_exists(uri, auth=auth):
            raise PipelineError(f"immutable OCI {label} already exists: {uri}")
    progress.emit("[preflight] OCI paths are available")
    store = AtomicManifest(manifest_path, _initial_manifest(plan, args.upstream_manifest))
    result = execute_plan(
        plan,
        config,
        store,
        adapter,
        auth,
        args.max_concurrency,
        args.max_retries,
        args.poll_seconds,
        progress,
    )
    status = "CANCELLED" if result == 130 else ("SUCCEEDED" if result == 0 else "FAILED")
    store.update(lambda payload: payload.update(status=status, finished_at=utc_now()))
    try:
        adapter.upload_file(str(manifest_path), plan["manifest_uri"], auth=auth)
    except Exception as exc:
        upload_error = str(exc)
        store.update(
            lambda payload: payload.update(
                manifest_upload_error=upload_error, status="FAILED"
            )
        )
        progress.emit(
            f"[failed] pipeline run_id={args.run_id} manifest upload failed: {exc}"
        )
        return 1
    if result == 0:
        progress.emit(f"[done] pipeline SUCCEEDED run_id={args.run_id}")
    elif result == 130:
        progress.emit(f"[failed] pipeline CANCELLED run_id={args.run_id}")
    else:
        progress.emit(f"[failed] pipeline FAILED run_id={args.run_id}")
    return result


def adopt_inputs_command(
    args: SimpleNamespace,
    adapter: Any | None,
    progress: ProgressReporter | None = None,
) -> int:
    progress = progress or ProgressReporter(enabled=False)
    config = load_config(args.config)
    products = parse_products(args.product)
    require_configured_products(config, products)
    synthetic_uris: dict[str, str] = {}
    for raw in args.synthetic_uri:
        product, separator, uri = raw.partition("=")
        if not separator or not product or not uri:
            raise PipelineError(
                "--synthetic-uri must use product=oci://bucket@namespace/path"
            )
        if product not in products:
            raise PipelineError(
                f"--synthetic-uri product {product!r} is not selected by --product"
            )
        if product in synthetic_uris:
            raise PipelineError(f"duplicate --synthetic-uri for product {product}")
        if not uri.startswith("oci://"):
            raise PipelineError(f"--synthetic-uri for {product} must start with oci://")
        synthetic_uris[product] = uri
    missing_validate_only = sorted(
        product for product in products
        if "engorda" not in PRODUCTS[product]["capabilities"]
        and product not in synthetic_uris
    )
    if missing_validate_only:
        raise PipelineError(
            "validate-only product(s) require --synthetic-uri: "
            + ", ".join(missing_validate_only)
        )
    for label, uri in (("raw", args.raw_uri), ("faltantes", args.faltantes_uri)):
        if not uri.startswith("oci://"):
            raise PipelineError(f"--{label.replace('_', '-')} URI must start with oci://")
    output = Path(args.output_manifest)
    if output.exists():
        raise PipelineError(f"refusing to replace immutable manifest: {output}")
    payload = {
        "manifest_version": MANIFEST_VERSION,
        "kind": "adopted-inputs",
        "environment": config["environment"],
        "status": "ADOPTED",
        "created_at": utc_now(),
        "products": products,
        "artifacts": {
            "raw": {"uri": args.raw_uri, "producer": "external"},
            "faltantes": {"uri": args.faltantes_uri, "producer": "external"},
            "products": {
                product: {
                    "synthetic": {"uri": uri, "producer": "external"}
                }
                for product, uri in sorted(synthetic_uris.items())
            },
        },
    }
    if args.dry_run:
        progress.emit("[dry-run] resolved input adoption (no remote calls)")
        print(json.dumps({"dry_run": True, "output_manifest": str(output), **payload}, indent=2))
        progress.emit("[done] dry-run complete (no remote calls)")
        return 0
    adapter = adapter or ModuleAdapter(
        timeout_seconds=args.oci_timeout_seconds,
        auth_refresh_seconds=args.auth_refresh_seconds,
        progress=progress,
    )
    auth = _auth_from_args(args)
    _ensure_adapter_auth(adapter, auth, allow_prompt=args.auth_prompt)
    for name, uri in (("raw", args.raw_uri), ("faltantes", args.faltantes_uri)):
        progress.emit(f"[preflight] inspecting {name}: {uri}")
        describe = getattr(adapter, "describe_uri", None)
        if describe is not None:
            payload["artifacts"][name].update(describe(uri, auth=auth))
        elif not adapter.uri_exists(uri, auth=auth):
            raise PipelineError(f"OCI input URI does not exist: {uri}")
    for product, uri in sorted(synthetic_uris.items()):
        progress.emit(f"[preflight] inspecting {product} synthetic: {uri}")
        artifact = payload["artifacts"]["products"][product]["synthetic"]
        describe = getattr(adapter, "describe_uri", None)
        if describe is not None:
            artifact.update(describe(uri, auth=auth))
        elif not adapter.uri_exists(uri, auth=auth):
            raise PipelineError(f"OCI synthetic URI does not exist: {uri}")
    progress.emit("[preflight] adopted input paths are readable")
    atomic_write_json(output, payload)
    print(str(output))
    return 0


def _auth_options(command: Callable[..., Any]) -> Callable[..., Any]:
    command = click.option("--profile", help="OCI CLI profile name.")(command)
    command = click.option("--config-file", help="OCI CLI config file path.")(command)
    command = click.option(
        "--region", help="OCI region; defaults to the selected profile region."
    )(command)
    command = click.option(
        "--auth", help="OCI auth mode, for example security_token."
    )(command)
    command = click.option("--cert-bundle", help="CA certificate bundle path.")(command)
    command = click.option(
        "--oci-timeout-seconds",
        type=click.FloatRange(min=0.1),
        default=60.0,
        show_default=True,
        help="Timeout for each OCI CLI command.",
    )(command)
    command = click.option(
        "--auth-refresh-seconds",
        type=click.FloatRange(min=0),
        default=1800.0,
        show_default=True,
        help="Proactively refresh security-token auth during long-running polls; 0 disables.",
    )(command)
    return click.option(
        "--auth-prompt/--no-auth-prompt",
        default=True,
        show_default=True,
        help="Prompt to refresh/authenticate an invalid security-token session.",
    )(command)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def cli(context: click.Context) -> None:
    """Plan and orchestrate OCI Data Flow pipeline runs."""
    context.ensure_object(dict)
    context.obj.setdefault("adapter", None)
    context.obj.setdefault("polished", True)


@cli.command("run")
@click.option("--config", required=True, type=click.Path(dir_okay=False, path_type=str))
@click.option("--product", multiple=True, required=True, help="Repeat or use commas.")
@click.option(
    "--from", "from_stage", required=True, type=click.Choice(PUBLIC_STAGES)
)
@click.option("--to", "to_stage", required=True, type=click.Choice(PUBLIC_STAGES))
@click.option(
    "--upstream-manifest", required=True, type=click.Path(dir_okay=False, path_type=str)
)
@click.option("--run-id")
@click.option("--local-run-root", default=".pipeline-runs", show_default=True)
@click.option(
    "--max-concurrency", type=click.IntRange(min=1), default=4, show_default=True
)
@click.option("--max-retries", type=click.IntRange(min=0), default=1, show_default=True)
@click.option(
    "--poll-seconds", type=click.FloatRange(min=0), default=30.0, show_default=True
)
@click.option("--num-executors", type=click.IntRange(min=1))
@click.option("--driver-shape")
@click.option("--executor-shape")
@click.option("--driver-shape-config")
@click.option("--executor-shape-config")
@click.option("--n-instrumentos", type=int)
@click.option("--fator-k", type=int)
@click.option("--seed", type=int)
@click.option(
    "--set",
    "set_values",
    multiple=True,
    help="Validated product.stage.key=value override; repeat as needed.",
)
@click.option("--dry-run", is_flag=True, help="Resolve the plan without remote calls.")
@_auth_options
@click.pass_context
def run_cli(context: click.Context, **options: Any) -> None:
    """Plan or execute the first tracer pipeline."""
    options["run_id"] = options["run_id"] or default_run_id()
    args = SimpleNamespace(**options)
    progress = ProgressReporter(enabled=context.obj["polished"])
    try:
        result = run_command(args, context.obj["adapter"], progress)
    except PipelineError as exc:
        raise click.UsageError(str(exc), context) from exc
    except OciExecutionError as exc:
        raise click.ClickException(str(exc)) from exc
    if result:
        context.exit(result)


@cli.command("adopt-inputs")
@click.option("--config", required=True, type=click.Path(dir_okay=False, path_type=str))
@click.option("--product", multiple=True, required=True, help="Repeat or use commas.")
@click.option("--raw-uri", required=True)
@click.option("--faltantes-uri", required=True)
@click.option(
    "--synthetic-uri",
    multiple=True,
    help="Existing product output as product=oci://...; required for validate-only products.",
)
@click.option(
    "--output-manifest", required=True, type=click.Path(dir_okay=False, path_type=str)
)
@click.option("--dry-run", is_flag=True, help="Resolve the adoption without remote calls.")
@_auth_options
@click.pass_context
def adopt_inputs_cli(context: click.Context, **options: Any) -> None:
    """Register existing RAW and faltantes lineage."""
    args = SimpleNamespace(**options)
    progress = ProgressReporter(enabled=context.obj["polished"])
    try:
        result = adopt_inputs_command(args, context.obj["adapter"], progress)
    except PipelineError as exc:
        raise click.UsageError(str(exc), context) from exc
    except OciExecutionError as exc:
        raise click.ClickException(str(exc)) from exc
    if result:
        context.exit(result)


def main(argv: Sequence[str] | None = None, *, adapter: Any | None = None) -> int:
    """Compatibility facade that invokes Click without terminating the caller."""
    try:
        result = cli.main(
            args=list(argv) if argv is not None else None,
            prog_name="run_pipeline",
            obj={"adapter": adapter, "polished": False},
            standalone_mode=False,
        )
    except click.ClickException as exc:
        exc.show(file=sys.stderr)
        return exc.exit_code
    except (click.Abort, KeyboardInterrupt):
        return 130
    return int(result or 0)


def _entrypoint() -> int:
    try:
        result = cli.main(
            prog_name="run_pipeline",
            obj={"adapter": None, "polished": True},
            standalone_mode=False,
        )
    except click.ClickException as exc:
        exc.show(file=sys.stderr)
        return exc.exit_code
    except (click.Abort, KeyboardInterrupt):
        click.echo("Aborted!", err=True)
        return 130
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(_entrypoint())
