import hashlib
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import pipeline_reservations as R  # noqa: E402, I001


REQUEST_A = "oci://bucket@namespace/run/a/plan.json"
REQUEST_B = "oci://bucket@namespace/run/b/plan.json"
RESERVATION_A = "oci://bucket@namespace/run/a/reservation.json"
RESERVATION_B = "oci://bucket@namespace/run/b/reservation.json"
LEASE = "oci://bucket@namespace/control/qab/lease.json"
LEDGER = "oci://bucket@namespace/control/qab/ledger.json"


def plan(
    plan_id,
    *,
    table_count=3,
    step=2,
    minimum=100,
    cod_count=7,
    meu_count=4,
    requested_prefix=None,
):
    body = {
        "artifact_type": "engorda_plan",
        "schema_version": 1,
        "test_label": plan_id,
        "product": "cdb_simplificado",
        "tables": {
            "OPERACAO": {
                "pk": {
                    "rule": "OFFSET_PROPRIO",
                    "count_demand": table_count,
                    "step": step,
                    "minimum_start": minimum,
                }
            },
            "SEM_PK_PROPRIA": {
                "pk": {
                    "rule": "VIA_PAI",
                    "count_demand": 0,
                    "step": 1,
                    "minimum_start": None,
                }
            },
        },
        "cod_operacao": {"count": cod_count},
        "meu_numero": {
            "ordinal_count_demand": meu_count,
            **({"requested_prefix": requested_prefix}
               if requested_prefix is not None else {}),
        },
    }
    digest = hashlib.sha256(
        json.dumps(
            body, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    return {**body, "plan_id": digest}


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.etags = {}
        self.calls = []
        self.fail_puts = {}
        self._version = 0
        self._lock = threading.Lock()

    def seed(self, uri, payload):
        with self._lock:
            self._version += 1
            self.objects[uri] = R._json_bytes(payload)
            self.etags[uri] = f"etag-{self._version}"

    def head(self, uri):
        with self._lock:
            self.calls.append(("head", uri))
            if uri not in self.objects:
                return None
            return R.ObjectMetadata(self.etags[uri])

    def get(self, uri):
        with self._lock:
            self.calls.append(("get", uri))
            if uri not in self.objects:
                raise R.ObjectNotFound(uri)
            return R.StoredObject(self.objects[uri], self.etags[uri])

    def put(self, uri, data, *, no_overwrite=False, if_match=None):
        with self._lock:
            self.calls.append(("put", uri, no_overwrite, if_match))
            remaining = self.fail_puts.get(uri, 0)
            if remaining:
                self.fail_puts[uri] = remaining - 1
                raise R.PreconditionFailed(uri)
            if no_overwrite and uri in self.objects:
                raise R.PreconditionFailed(uri)
            if if_match is not None and self.etags.get(uri) != if_match:
                raise R.PreconditionFailed(uri)
            self._version += 1
            self.objects[uri] = bytes(data)
            self.etags[uri] = f"etag-{self._version}"
            return R.ObjectMetadata(self.etags[uri])

    def delete(self, uri, *, if_match=None):
        with self._lock:
            self.calls.append(("delete", uri, if_match))
            if uri not in self.objects:
                raise R.ObjectNotFound(uri)
            if if_match is not None and self.etags[uri] != if_match:
                raise R.PreconditionFailed(uri)
            del self.objects[uri]
            del self.etags[uri]

    def json(self, uri):
        return json.loads(self.objects[uri])


def reserve(store, request_uri, reservation_uri, run_id):
    return R.reserve_ranges(
        environment="qab",
        run_id=run_id,
        product="cdb_simplificado",
        request_uri=request_uri,
        reservation_uri=reservation_uri,
        lease_uri=LEASE,
        ledger_uri=LEDGER,
        auth={},
        storage=store,
    )


def test_allocates_schema_compatible_ranges_and_keeps_oracle_as_cod_authority():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))

    result = reserve(store, REQUEST_A, RESERVATION_A, "run-a")

    artifact = store.json(RESERVATION_A)
    assert result == {
        "uri": RESERVATION_A,
        "etag": store.etags[RESERVATION_A],
    }
    assert artifact == {
        "artifact_type": "engorda_reservation",
        "schema_version": 1,
        "plan_id": plan("plan-a")["plan_id"],
        "product": "cdb_simplificado",
        "table_pks": {
            "OPERACAO": {"count": 3, "start": 100, "end": 104, "step": 2}
        },
        "cod_operacao": {"strategy": "oracle_allocator", "count": 7},
        "meu_numero": {"prefix": "100", "count": 4, "start": 1, "end": 4},
    }
    assert LEASE not in store.objects
    ledger_put = next(call for call in store.calls if call[:2] == ("put", LEDGER))
    reservation_put = next(call for call in store.calls if call[:2] == ("put", RESERVATION_A))
    assert store.calls.index(ledger_put) < store.calls.index(reservation_put)


def test_parallel_products_and_later_runs_never_reuse_ranges():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))
    store.seed(REQUEST_B, plan("plan-b"))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(reserve, store, REQUEST_A, RESERVATION_A, "run-a"),
            executor.submit(reserve, store, REQUEST_B, RESERVATION_B, "run-b"),
        ]
        for future in futures:
            future.result()
        artifacts = [store.json(RESERVATION_A), store.json(RESERVATION_B)]

    assert sorted(item["table_pks"]["OPERACAO"]["start"] for item in artifacts) == [100, 106]
    assert sorted(item["meu_numero"]["start"] for item in artifacts) == [1, 5]
    assert store.json(LEDGER)["table_pks"]["OPERACAO"]["next_start"] == 112
    assert store.json(LEDGER)["meu_numero"]["prefixes"]["100"] == 9


def test_requested_meu_numero_prefix_is_honored():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-prefix", requested_prefix="321"))

    reserve(store, REQUEST_A, RESERVATION_A, "run-prefix")

    assert store.json(RESERVATION_A)["meu_numero"]["prefix"] == "321"


def test_failed_publication_burns_ranges_and_ledger_cas_retries_are_bounded():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))
    store.seed(REQUEST_B, plan("plan-b"))
    store.fail_puts[LEDGER] = 1
    store.fail_puts[RESERVATION_A] = 1

    with pytest.raises(R.ReservationError, match="concurrently populated"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-a")
    reserve(store, REQUEST_B, RESERVATION_B, "run-b")
    artifact = store.json(RESERVATION_B)

    assert artifact["table_pks"]["OPERACAO"]["start"] == 106
    assert artifact["meu_numero"]["start"] == 5
    assert store.json(LEDGER)["revision"] == 2


def test_ledger_cas_stops_after_bounded_attempts():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))
    store.fail_puts[LEDGER] = R.MAX_CAS_ATTEMPTS

    with pytest.raises(R.ReservationError, match="ledger CAS retries exhausted"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-a")

    ledger_puts = [call for call in store.calls if call[:2] == ("put", LEDGER)]
    assert len(ledger_puts) == R.MAX_CAS_ATTEMPTS
    assert RESERVATION_A not in store.objects


def test_expired_lease_is_taken_over_but_live_lease_is_not():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))
    lease = {
        "artifact_type": "pipeline_environment_lease",
        "schema_version": 1,
        "environment": "qab",
        "run_id": "abandoned",
        "expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
    }
    store.seed(LEASE, lease)
    reserve(store, REQUEST_A, RESERVATION_A, "run-a")
    assert any(call[:2] == ("put", LEASE) and call[3] for call in store.calls)

    store.seed(REQUEST_B, plan("plan-b"))
    lease["expires_at"] = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    store.seed(LEASE, lease)
    with pytest.raises(R.LeaseUnavailable, match="abandoned"):
        reserve(store, REQUEST_B, RESERVATION_B, "run-b")


def test_existing_create_once_reservation_is_idempotent():
    store = FakeStorage()
    store.seed(REQUEST_A, plan("plan-a"))
    first = reserve(store, REQUEST_A, RESERVATION_A, "run-a")
    ledger_before = store.objects[LEDGER]

    second = reserve(store, REQUEST_A, RESERVATION_A, "run-a")

    assert second == first
    assert store.objects[LEDGER] == ledger_before


def test_live_storage_uses_oci_argv_temp_files_and_conditional_flags(monkeypatch):
    calls = []
    payload = b'{"value": 1}\n'

    def run_json(command):
        calls.append(list(command))
        operation = command[3]
        if operation == "get":
            Path(command[command.index("--file") + 1]).write_bytes(payload)
        return {"etag": f"etag-{operation}"}

    monkeypatch.setattr(R.oci_dataflow, "run_json", run_json)
    storage = R.OciCliStorage({"profile": "QAB"})
    uri = "oci://bucket@namespace/path/object.json"

    assert storage.head(uri) == R.ObjectMetadata("etag-head")
    assert storage.get(uri) == R.StoredObject(payload, "etag-get")
    storage.put(uri, payload, no_overwrite=True)
    storage.put(uri, payload, if_match="old-etag")
    storage.delete(uri, if_match="new-etag")

    assert [command[3] for command in calls] == ["head", "get", "put", "put", "delete"]
    assert all("--profile" in command and "QAB" in command for command in calls)
    assert "--no-overwrite" in calls[2]
    assert "--force" not in calls[1]
    assert calls[3][-3:] == ["--if-match", "old-etag", "--force"]
    assert calls[4][-2:] == ["--if-match", "new-etag"]
