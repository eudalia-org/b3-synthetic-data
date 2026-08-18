"""Compatibility facade for reservations implemented by ``run_pipeline``."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

try:
    import oci_dataflow
except ModuleNotFoundError:  # Supports ``from scripts import pipeline_reservations``.
    from scripts import oci_dataflow

try:
    import run_pipeline as _implementation
except ModuleNotFoundError:  # Supports ``from scripts import pipeline_reservations``.
    from scripts import run_pipeline as _implementation

SCHEMA_VERSION = _implementation.SCHEMA_VERSION
MAX_CAS_ATTEMPTS = _implementation.MAX_CAS_ATTEMPTS
MIN_MEU_NUMERO_PREFIX = _implementation.MIN_MEU_NUMERO_PREFIX
MAX_MEU_NUMERO_PREFIX = _implementation.MAX_MEU_NUMERO_PREFIX
MAX_MEU_NUMERO_ORDINAL = _implementation.MAX_MEU_NUMERO_ORDINAL

ReservationError = _implementation.ReservationError
ObjectNotFound = _implementation.ObjectNotFound
PreconditionFailed = _implementation.PreconditionFailed
LeaseUnavailable = _implementation.LeaseUnavailable
ObjectMetadata = _implementation.ObjectMetadata
StoredObject = _implementation.StoredObject
Storage = _implementation.Storage


class OciCliStorage(_implementation.OciCliStorage):
    def __init__(self, auth: Mapping[str, Any] | None = None):
        super().__init__(
            auth,
            transport=oci_dataflow.run_json,
            auth_builder=oci_dataflow.oci_auth_flags,
        )


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
    storage = storage or OciCliStorage(auth)
    return _implementation.reserve_ranges(
        environment=environment,
        run_id=run_id,
        product=product,
        request_uri=request_uri,
        reservation_uri=reservation_uri,
        lease_uri=lease_uri,
        ledger_uri=ledger_uri,
        auth=auth,
        storage=storage,
        lease_ttl_seconds=lease_ttl_seconds,
    )


def __getattr__(name: str) -> Any:
    return getattr(_implementation, name)
