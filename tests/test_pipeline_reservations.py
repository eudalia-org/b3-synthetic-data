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
SNAPSHOT_ID = "00000000-0000-4000-8000-000000000001"


def with_plan_id(body):
    body = {key: value for key, value in body.items() if key != "plan_id"}
    digest = hashlib.sha256(
        json.dumps(
            body, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    return {**body, "plan_id": digest}


def plan(
    request_uri,
    plan_id,
    *,
    table_count=3,
    step=2,
    minimum=100,
    cod_count=7,
    meu_count=4,
    requested_prefix=None,
    selective_missing=False,
):
    snapshot_uri = f"{request_uri.rstrip('/')}.selected-lote/{SNAPSHOT_ID}"
    table_source_counts = {"OPERACAO": table_count, "SEM_PK_PROPRIA": 0}
    body = {
        "artifact_type": "engorda_plan",
        "schema_version": 2,
        "test_label": plan_id,
        "product": "cdb_simplificado",
        "selected_lote": {
            "artifact_type": "engorda_selected_lote",
            "schema_version": 1,
            "snapshot_id": SNAPSHOT_ID,
            "snapshot_uri": snapshot_uri,
            "table_set": sorted(table_source_counts),
            "tables": {
                table: {
                    "path": f"{snapshot_uri}/tables/{table}",
                    "row_count": count,
                    "schema": {
                        "type": "struct",
                        "fields": [{
                            "name": "ID",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        }],
                    },
                }
                for table, count in sorted(table_source_counts.items())
            },
            "selective_missing": (
                {
                    "present": True,
                    "path": f"{snapshot_uri}/selective_missing",
                    "row_count": 1,
                    "schema": {
                        "type": "struct",
                        "fields": [{
                            "name": "TABELA",
                            "type": "string",
                            "nullable": False,
                            "metadata": {},
                        }],
                    },
                }
                if selective_missing
                else {"present": False, "path": None, "row_count": 0, "schema": None}
            ),
        },
        "tables": {
            "OPERACAO": {
                "source_count": table_count,
                "pk": {
                    "rule": "OFFSET_PROPRIO",
                    "count_demand": table_count,
                    "step": step,
                    "minimum_start": minimum,
                }
            },
            "SEM_PK_PROPRIA": {
                "source_count": 0,
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
    return with_plan_id(body)


def grouped_plan(
    request_uri,
    plan_id,
    groups,
    *,
    operational_date="2026-09-04",
    requested_prefix=None,
):
    request = plan(request_uri, plan_id, meu_count=0)
    request["schema_version"] = 3
    request["controle_operacional_date"] = operational_date
    request["meu_numero"] = {
        "strategy": "date_account_tos_shared_interval_v1",
        "operational_date": operational_date,
        "normalization": "trim_strip_decimal_zeroes_v1",
        "tuple_count_demand": sum(group["count_demand"] for group in groups),
        "ordinal_count_demand": max(
            (group["count_demand"] for group in groups), default=0
        ),
        "groups": groups,
        **(
            {"requested_prefix": requested_prefix}
            if requested_prefix is not None
            else {}
        ),
    }
    return with_plan_id(request)


GROUP_A = "a" * 64
GROUP_B = "b" * 64


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
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))

    result = reserve(store, REQUEST_A, RESERVATION_A, "run-a")

    artifact = store.json(RESERVATION_A)
    assert result == {
        "uri": RESERVATION_A,
        "etag": store.etags[RESERVATION_A],
    }
    assert artifact == {
        "artifact_type": "engorda_reservation",
        "schema_version": 2,
        "plan_id": plan(REQUEST_A, "plan-a")["plan_id"],
        "product": "cdb_simplificado",
        "table_pks": {
            "OPERACAO": {"count": 3, "start": 100, "end": 104, "step": 2}
        },
        "cod_operacao": {"strategy": "oracle_allocator", "count": 7},
        "meu_numero": {
            "strategy": "legacy_global_v1",
            "prefix": "100",
            "count": 4,
            "start": 1,
            "end": 4,
        },
    }
    assert LEASE not in store.objects
    ledger_put = next(call for call in store.calls if call[:2] == ("put", LEDGER))
    reservation_put = next(call for call in store.calls if call[:2] == ("put", RESERVATION_A))
    assert store.calls.index(ledger_put) < store.calls.index(reservation_put)


def test_plan_v1_requires_migration_and_plan_v2_is_accepted():
    store = FakeStorage()
    legacy = plan(REQUEST_A, "legacy")
    legacy["schema_version"] = 1
    legacy = with_plan_id(legacy)
    store.seed(REQUEST_A, legacy)

    with pytest.raises(R.ReservationError, match="regenerate it with plan-v2"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-v1")

    store.seed(REQUEST_A, plan(REQUEST_A, "current"))
    reserve(store, REQUEST_A, RESERVATION_A, "run-v2")

    assert store.json(RESERVATION_A)["schema_version"] == 2


def test_plan_v2_requires_selected_lote_descriptor():
    store = FakeStorage()
    request = plan(REQUEST_A, "missing-snapshot")
    request.pop("selected_lote")
    store.seed(REQUEST_A, with_plan_id(request))

    with pytest.raises(R.ReservationError, match="selected_lote"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-missing-snapshot")

    assert LEDGER not in store.objects
    assert RESERVATION_A not in store.objects


@pytest.mark.parametrize(
    "meu_numero",
    [
        {},
        {"ordinal_count_demand": 4, "extra": True},
        {"ordinal_count_demand": 4, "requested_prefix": "099"},
    ],
    ids=["missing-demand", "extra-key", "invalid-prefix"],
)
def test_plan_v2_requires_exact_legacy_meu_numero_descriptor(meu_numero):
    store = FakeStorage()
    request = plan(REQUEST_A, "invalid-legacy")
    request["meu_numero"] = meu_numero
    store.seed(REQUEST_A, with_plan_id(request))

    with pytest.raises(R.ReservationError, match="plan.meu_numero"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-invalid-legacy")

    assert store.calls == [("get", REQUEST_A)]


def test_plan_v2_accepts_present_selective_missing_descriptor():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "selective", selective_missing=True))

    reserve(store, REQUEST_A, RESERVATION_A, "run-selective")

    assert store.json(RESERVATION_A)["schema_version"] == 2


def test_external_selected_lote_uri_is_rejected_before_lease_or_ledger():
    store = FakeStorage()
    external_request = "oci://external@namespace/other/plan.json"
    store.seed(REQUEST_A, plan(external_request, "external-snapshot"))

    with pytest.raises(R.ReservationError, match="selected_lote.snapshot_uri"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-external-snapshot")

    assert store.calls == [("get", REQUEST_A)]
    assert LEASE not in store.objects
    assert LEDGER not in store.objects
    assert RESERVATION_A not in store.objects


@pytest.mark.parametrize(
    "mutation",
    [
        lambda descriptor: descriptor.update(artifact_type="wrong"),
        lambda descriptor: descriptor.update(schema_version=2),
        lambda descriptor: descriptor.update(
            snapshot_id="00000000-0000-4000-8000-00000000000A"
        ),
        lambda descriptor: descriptor.update(snapshot_uri=""),
        lambda descriptor: descriptor.update(table_set=list(reversed(descriptor["table_set"]))),
        lambda descriptor: descriptor["tables"].pop("OPERACAO"),
        lambda descriptor: descriptor["tables"]["OPERACAO"].update(path="oci://wrong"),
        lambda descriptor: descriptor["tables"]["OPERACAO"].update(row_count=-1),
        lambda descriptor: descriptor["tables"]["OPERACAO"].update(row_count=99),
        lambda descriptor: descriptor["tables"]["OPERACAO"].update(schema=[]),
        lambda descriptor: descriptor["selective_missing"].update(present="false"),
        lambda descriptor: descriptor["selective_missing"].update(path="oci://wrong"),
    ],
    ids=[
        "artifact-type",
        "schema-version",
        "snapshot-id",
        "snapshot-uri",
        "table-set",
        "tables-set",
        "table-path",
        "negative-table-count",
        "mismatched-table-count",
        "table-schema",
        "selective-presence",
        "absent-selective-shape",
    ],
)
def test_invalid_selected_lote_never_burns_ranges(mutation):
    store = FakeStorage()
    request = plan(REQUEST_A, "invalid")
    mutation(request["selected_lote"])
    store.seed(REQUEST_A, with_plan_id(request))

    with pytest.raises(R.ReservationError, match="selected_lote"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-invalid")

    assert LEDGER not in store.objects
    assert RESERVATION_A not in store.objects


@pytest.mark.parametrize(
    "field,value",
    [("path", "oci://wrong"), ("row_count", -1), ("schema", [])],
)
def test_invalid_present_selective_missing_never_burns_ranges(field, value):
    store = FakeStorage()
    request = plan(REQUEST_A, "invalid-selective", selective_missing=True)
    request["selected_lote"]["selective_missing"][field] = value
    store.seed(REQUEST_A, with_plan_id(request))

    with pytest.raises(R.ReservationError, match="selected_lote.selective_missing"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-invalid-selective")

    assert LEDGER not in store.objects
    assert RESERVATION_A not in store.objects


def test_parallel_products_and_later_runs_never_reuse_ranges():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
    store.seed(REQUEST_B, plan(REQUEST_B, "plan-b"))

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
    assert store.json(LEDGER)["meu_numero"]["legacy_prefixes"]["100"] == 9


def test_requested_meu_numero_prefix_is_honored():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-prefix", requested_prefix="321"))

    reserve(store, REQUEST_A, RESERVATION_A, "run-prefix")

    assert store.json(RESERVATION_A)["meu_numero"]["prefix"] == "321"


def test_facade_exports_independent_schema_versions():
    assert R.LEASE_SCHEMA_VERSION == 1
    assert R.LEDGER_SCHEMA_VERSION == 2
    assert R.RESERVATION_SCHEMA_VERSION == 2
    assert R.SCHEMA_VERSION == R.LEASE_SCHEMA_VERSION


@pytest.mark.parametrize(
    "mutation",
    [
        lambda request: request["meu_numero"].update(strategy="wrong"),
        lambda request: request["meu_numero"].update(operational_date="2026-09-05"),
        lambda request: request["meu_numero"].update(normalization="wrong"),
        lambda request: request["meu_numero"].update(tuple_count_demand=-1),
        lambda request: request["meu_numero"].update(ordinal_count_demand=True),
        lambda request: request["meu_numero"].update(tuple_count_demand=1),
        lambda request: request["meu_numero"].update(ordinal_count_demand=1),
        lambda request: request["meu_numero"].update(
            groups=[
                {"group_id": GROUP_B, "count_demand": 2},
                {"group_id": GROUP_A, "count_demand": 4},
            ]
        ),
        lambda request: request["meu_numero"]["groups"][0].update(group_id="A" * 64),
        lambda request: request["meu_numero"]["groups"][0].update(count_demand=0),
        lambda request: request["meu_numero"].update(extra=True),
    ],
    ids=[
        "strategy",
        "operational-date",
        "normalization",
        "negative-tuple-demand",
        "boolean-ordinal-demand",
        "tuple-sum",
        "ordinal-maximum",
        "group-order",
        "group-id",
        "group-count",
        "extra-key",
    ],
)
def test_invalid_grouped_plan_never_acquires_lease_or_burns_ranges(mutation):
    store = FakeStorage()
    request = grouped_plan(
        REQUEST_A,
        "grouped-invalid",
        [
            {"group_id": GROUP_A, "count_demand": 4},
            {"group_id": GROUP_B, "count_demand": 2},
        ],
    )
    mutation(request)
    store.seed(REQUEST_A, with_plan_id(request))

    with pytest.raises(R.ReservationError, match="plan.meu_numero"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-invalid-grouped")

    assert store.calls == [("get", REQUEST_A)]
    assert LEASE not in store.objects
    assert LEDGER not in store.objects


def test_grouped_plan_allocates_strategy_aware_reservation_and_ledger_state():
    store = FakeStorage()
    store.seed(
        REQUEST_A,
        grouped_plan(
            REQUEST_A,
            "grouped",
            [
                {"group_id": GROUP_A, "count_demand": 4},
                {"group_id": GROUP_B, "count_demand": 2},
            ],
            requested_prefix="321",
        ),
    )

    reserve(store, REQUEST_A, RESERVATION_A, "run-grouped")

    assert store.json(RESERVATION_A)["meu_numero"] == {
        "strategy": "date_account_tos_shared_interval_v1",
        "prefix": "321",
        "count": 4,
        "start": 1,
        "end": 4,
        "operational_date": "2026-09-04",
        "group_ids": [GROUP_A, GROUP_B],
    }
    ledger = store.json(LEDGER)
    assert ledger["schema_version"] == 2
    assert ledger["meu_numero"] == {
        "legacy_prefixes": {},
        "groups": {
            "2026-09-04": {
                "321": {
                    GROUP_A: 5,
                    GROUP_B: 5,
                }
            }
        },
    }


def test_zero_grouped_demand_emits_coherent_empty_reservation():
    store = FakeStorage()
    store.seed(REQUEST_A, grouped_plan(REQUEST_A, "empty", []))

    reserve(store, REQUEST_A, RESERVATION_A, "run-empty")

    assert store.json(RESERVATION_A)["meu_numero"] == {
        "strategy": "date_account_tos_shared_interval_v1",
        "prefix": None,
        "count": 0,
        "start": None,
        "end": None,
        "operational_date": "2026-09-04",
        "group_ids": [],
    }


def test_concurrent_disjoint_groups_reuse_same_interval():
    store = FakeStorage()
    store.seed(
        REQUEST_A,
        grouped_plan(REQUEST_A, "disjoint-a", [{"group_id": GROUP_A, "count_demand": 4}]),
    )
    store.seed(
        REQUEST_B,
        grouped_plan(REQUEST_B, "disjoint-b", [{"group_id": GROUP_B, "count_demand": 4}]),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(reserve, store, REQUEST_A, RESERVATION_A, "run-a"),
            executor.submit(reserve, store, REQUEST_B, RESERVATION_B, "run-b"),
        ]
        for future in futures:
            future.result()

    assert [
        store.json(uri)["meu_numero"]["start"]
        for uri in (RESERVATION_A, RESERVATION_B)
    ] == [1, 1]


def test_concurrent_overlapping_groups_get_non_overlapping_intervals():
    store = FakeStorage()
    for request_uri, label in ((REQUEST_A, "overlap-a"), (REQUEST_B, "overlap-b")):
        store.seed(
            request_uri,
            grouped_plan(
                request_uri,
                label,
                [{"group_id": GROUP_A, "count_demand": 4}],
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(reserve, store, REQUEST_A, RESERVATION_A, "run-a"),
            executor.submit(reserve, store, REQUEST_B, RESERVATION_B, "run-b"),
        ]
        for future in futures:
            future.result()

    intervals = sorted(
        (
            store.json(uri)["meu_numero"]["start"],
            store.json(uri)["meu_numero"]["end"],
        )
        for uri in (RESERVATION_A, RESERVATION_B)
    )
    assert intervals == [(1, 4), (5, 8)]


def test_v1_ledger_migrates_without_losing_legacy_floor_or_history():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "migration", meu_count=4))
    existing_reservations = [{"run_id": "older"}]
    store.seed(
        LEDGER,
        {
            "artifact_type": "engorda_reservation_ledger",
            "schema_version": 1,
            "environment": "qab",
            "revision": 7,
            "table_pks": {"OPERACAO": {"next_start": 200}},
            "meu_numero": {"prefixes": {"100": 10}},
            "reservations": existing_reservations,
        },
    )

    reserve(store, REQUEST_A, RESERVATION_A, "run-migration")

    assert store.json(RESERVATION_A)["meu_numero"]["start"] == 10
    ledger = store.json(LEDGER)
    assert ledger["schema_version"] == 2
    assert ledger["revision"] == 8
    assert ledger["table_pks"]["OPERACAO"]["next_start"] == 206
    assert ledger["meu_numero"]["legacy_prefixes"]["100"] == 14
    assert ledger["meu_numero"]["groups"] == {}
    assert ledger["reservations"][0] == existing_reservations[0]


def test_malformed_v1_ledger_is_not_migrated():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "bad-migration"))
    ledger = {
        "artifact_type": "engorda_reservation_ledger",
        "schema_version": 1,
        "environment": "qab",
        "revision": 0,
        "table_pks": {},
        "meu_numero": {"prefixes": {"100": 0}},
        "reservations": [],
    }
    store.seed(LEDGER, ledger)

    with pytest.raises(R.ReservationError, match="ordinal next value"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-bad-migration")

    assert store.json(LEDGER) == ledger


@pytest.mark.parametrize(
    "mutation",
    [
        lambda ledger: ledger.update(revision=True),
        lambda ledger: ledger["table_pks"].update(BAD={"next_start": True}),
        lambda ledger: ledger["meu_numero"]["legacy_prefixes"].update({"099": 1}),
        lambda ledger: ledger["meu_numero"]["legacy_prefixes"].update({"100": 0}),
        lambda ledger: ledger["meu_numero"]["groups"].update({"not-a-date": {}}),
        lambda ledger: ledger["meu_numero"]["groups"].update(
            {"2026-09-04": {"100": {"A" * 64: 2}}}
        ),
        lambda ledger: ledger.update(extra=True),
    ],
    ids=[
        "revision",
        "table-next",
        "legacy-prefix",
        "legacy-next",
        "group-date",
        "group-id",
        "extra-key",
    ],
)
def test_malformed_v2_ledger_is_rejected_without_replacement(mutation):
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "malformed-ledger"))
    ledger = {
        "artifact_type": "engorda_reservation_ledger",
        "schema_version": 2,
        "environment": "qab",
        "revision": 0,
        "table_pks": {},
        "meu_numero": {"legacy_prefixes": {}, "groups": {}},
        "reservations": [],
    }
    mutation(ledger)
    store.seed(LEDGER, ledger)

    with pytest.raises(R.ReservationError, match="ledger"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-malformed-ledger")

    assert store.json(LEDGER) == ledger
    assert RESERVATION_A not in store.objects


def test_legacy_and_grouped_allocations_respect_each_others_floors():
    store = FakeStorage()
    request_c = REQUEST_A.replace("/a/", "/c/")
    request_d = REQUEST_A.replace("/a/", "/d/")
    reservation_c = RESERVATION_A.replace("/a/", "/c/")
    reservation_d = RESERVATION_A.replace("/a/", "/d/")
    store.seed(
        REQUEST_A,
        grouped_plan(REQUEST_A, "group-a", [{"group_id": GROUP_A, "count_demand": 4}]),
    )
    store.seed(
        REQUEST_B,
        grouped_plan(REQUEST_B, "group-b", [{"group_id": GROUP_B, "count_demand": 7}]),
    )
    store.seed(request_c, plan(request_c, "legacy", meu_count=2))
    store.seed(
        request_d,
        grouped_plan(request_d, "after-legacy", [{"group_id": "c" * 64, "count_demand": 3}]),
    )

    reserve(store, REQUEST_A, RESERVATION_A, "run-group-a")
    reserve(store, REQUEST_B, RESERVATION_B, "run-group-b")
    reserve(store, request_c, reservation_c, "run-legacy")
    reserve(store, request_d, reservation_d, "run-after-legacy")

    assert store.json(RESERVATION_A)["meu_numero"]["start"] == 1
    assert store.json(RESERVATION_B)["meu_numero"]["start"] == 1
    assert store.json(reservation_c)["meu_numero"]["start"] == 8
    assert store.json(reservation_d)["meu_numero"]["start"] == 10


def test_existing_plan2_reservation_v1_remains_idempotently_readable():
    store = FakeStorage()
    request = plan(REQUEST_A, "legacy-reservation")
    store.seed(REQUEST_A, request)
    store.seed(
        RESERVATION_A,
        {
            "artifact_type": "engorda_reservation",
            "schema_version": 1,
            "plan_id": request["plan_id"],
            "product": "cdb_simplificado",
            "table_pks": {},
            "cod_operacao": {"strategy": "oracle_allocator", "count": 7},
            "meu_numero": {"prefix": "100", "count": 4, "start": 1, "end": 4},
        },
    )

    result = reserve(store, REQUEST_A, RESERVATION_A, "run-existing-v1")

    assert result == {"uri": RESERVATION_A, "etag": store.etags[RESERVATION_A]}
    assert LEDGER not in store.objects
    assert LEASE not in store.objects


def test_existing_reservation_must_have_schema_appropriate_for_plan_version():
    store = FakeStorage()
    request = grouped_plan(
        REQUEST_A,
        "wrong-reservation-strategy",
        [{"group_id": GROUP_A, "count_demand": 4}],
    )
    store.seed(REQUEST_A, request)
    store.seed(
        RESERVATION_A,
        {
            "artifact_type": "engorda_reservation",
            "schema_version": 2,
            "plan_id": request["plan_id"],
            "product": "cdb_simplificado",
            "meu_numero": {"strategy": "legacy_global_v1"},
        },
    )

    with pytest.raises(R.ReservationError, match="different artifact"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-wrong-strategy")

    assert LEDGER not in store.objects
    assert LEASE not in store.objects


def test_failed_publication_burns_ranges_and_ledger_cas_retries_are_bounded():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
    store.seed(REQUEST_B, plan(REQUEST_B, "plan-b"))
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
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
    store.fail_puts[LEDGER] = R.MAX_CAS_ATTEMPTS

    with pytest.raises(R.ReservationError, match="ledger CAS retries exhausted"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-a")

    ledger_puts = [call for call in store.calls if call[:2] == ("put", LEDGER)]
    assert len(ledger_puts) == R.MAX_CAS_ATTEMPTS
    assert RESERVATION_A not in store.objects


def test_expired_lease_is_taken_over_but_live_lease_is_not():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
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

    store.seed(REQUEST_B, plan(REQUEST_B, "plan-b"))
    lease["expires_at"] = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    store.seed(LEASE, lease)
    with pytest.raises(R.LeaseUnavailable, match="abandoned"):
        reserve(store, REQUEST_B, RESERVATION_B, "run-b")


def test_quarantined_lease_never_expires_automatically():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
    store.seed(LEASE, {
        "artifact_type": "pipeline_environment_lease",
        "schema_version": 1,
        "environment": "qab",
        "run_id": "ambiguous-load",
        "quarantined": True,
        "expires_at": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
    })

    with pytest.raises(R.LeaseUnavailable, match="quarantined"):
        reserve(store, REQUEST_A, RESERVATION_A, "run-a")


def test_existing_create_once_reservation_is_idempotent():
    store = FakeStorage()
    store.seed(REQUEST_A, plan(REQUEST_A, "plan-a"))
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
