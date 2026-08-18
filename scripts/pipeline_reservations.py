"""Persistent range reservations backed by OCI Object Storage CAS operations."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

try:
    import oci_dataflow
except ModuleNotFoundError:  # Supports ``from scripts import pipeline_reservations``.
    from scripts import oci_dataflow

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

    def __init__(self, auth: Mapping[str, Any] | None = None):
        self._auth = dict(auth or {})

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
            *oci_dataflow.oci_auth_flags(self._auth),
        ]

    @staticmethod
    def _translate_error(error: subprocess.CalledProcessError) -> None:
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
            return oci_dataflow.run_json(command)
        except subprocess.CalledProcessError as error:
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
        descriptor, temporary = tempfile.mkstemp(prefix="pipeline-reservation-", suffix=".json")
        os.close(descriptor)
        try:
            response = self._run(self._command("get", uri) + ["--file", temporary, "--force"])
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
        descriptor, temporary = tempfile.mkstemp(prefix="pipeline-reservation-", suffix=".json")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
            command = self._command("put", uri) + ["--file", temporary]
            if no_overwrite:
                command.append("--no-overwrite")
            if if_match is not None:
                command += ["--if-match", if_match]
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


def _validate_plan(plan: dict[str, Any], product: str) -> None:
    if plan.get("artifact_type") != "engorda_plan" or plan.get("schema_version") != 1:
        raise ReservationError("reservation request is not an engorda plan schema_version=1")
    if not isinstance(plan.get("plan_id"), str) or not plan["plan_id"]:
        raise ReservationError("engorda plan must contain plan_id")
    if plan.get("product") != product:
        raise ReservationError("engorda plan product does not match requested product")
    if not isinstance(plan.get("tables"), dict):
        raise ReservationError("engorda plan tables must be an object")
    for table, table_plan in plan["tables"].items():
        if not isinstance(table, str) or not isinstance(table_plan, Mapping):
            raise ReservationError("engorda plan contains an invalid table entry")
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
            raise ReservationError(
                f"plan.tables.{table}.pk VIA_PAI must not request a range"
            )
    for section, field in (("cod_operacao", "count"), ("meu_numero", "ordinal_count_demand")):
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
    actual = (ledger.get("artifact_type"), ledger.get("schema_version"), ledger.get("environment"))
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
    ledger: dict[str, Any], plan: dict[str, Any], run_id: str, product: str, reservation_uri: str
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
        for prefix in range(MIN_MEU_NUMERO_PREFIX, MAX_MEU_NUMERO_PREFIX + 1):
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
            raise ReservationError("meu_numero prefixes 100..999 are exhausted")

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
    _validate_plan(plan, product)

    existing = _existing_reservation(storage, reservation_uri, plan, product)
    if existing is not None:
        _, etag = existing
        return {"uri": reservation_uri, "etag": etag}

    lease = _acquire_lease(
        storage, lease_uri, environment, run_id, product, lease_ttl_seconds
    )
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
