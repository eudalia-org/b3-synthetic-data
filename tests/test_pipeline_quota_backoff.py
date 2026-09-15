import json
import random
import subprocess
import threading
from copy import deepcopy

import pytest
from click.testing import CliRunner
from test_run_pipeline import (
    FakeAdapter,
    P,
    read_run_manifest,
    run_args,
    write_config,
    write_upstream,
)


def quota_error(**overrides):
    payload = {
        "code": "LimitExceeded",
        "status": 400,
        "operation_name": "create_run",
        "message": "vm-total requested capacity exceeds quota",
    }
    payload.update(overrides)
    return P.OciExecutionError(
        "truncated error without service code",
        returncode=1,
        stderr="warning\nServiceError:\n" + json.dumps(payload) + "\nCLI notes",
    )


@pytest.fixture
def clock(monkeypatch):
    class Clock:
        now = 100.0

        def __init__(self):
            self.delays = []

        def advance(self, delay):
            self.delays.append(delay)
            self.now += delay

    clock = Clock()
    monkeypatch.setattr(P.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(random, "uniform", lambda _low, _high: 1.0)
    return clock


def test_cli_quota_wait_preserves_upstream_and_execution_attempt_one(tmp_path, monkeypatch, clock):
    original_wait = threading.Event.wait

    def wait(event, timeout=None):
        if timeout is not None and timeout >= 24:
            clock.advance(timeout)
            return event.is_set()
        return original_wait(event, timeout)

    monkeypatch.setattr(threading.Event, "wait", wait)

    class QuotaAdapter(FakeAdapter):
        submissions = 0

        def create_run(self, arguments, display_name, opts):
            if "-validate-" in display_name:
                self.submissions += 1
                if self.submissions <= 2:
                    raise quota_error()
            return super().create_run(arguments, display_name, opts)

    adapter = QuotaAdapter()
    result = CliRunner().invoke(
        P.cli,
        run_args(tmp_path, write_config(tmp_path), write_upstream(tmp_path)),
        obj={"adapter": adapter},
    )

    assert result.exit_code == 0, result.output
    assert clock.delays == [30, 60]
    assert len(adapter.created) == 3
    assert len(adapter.reservations) == 1
    manifest = read_run_manifest(tmp_path)
    assert manifest["quota_wait_seconds"] == 1800
    assert all(node["state"] == "SUCCEEDED" for node in manifest["nodes"].values())
    validator = manifest["nodes"]["cdb_simplificado.validate"]
    assert validator["validation"]["accepted"]
    assert len(validator["attempts"]) == 1
    attempt = validator["attempts"][0]
    assert attempt["attempt"] == 1
    assert attempt["run_id"] == "df-3"
    assert attempt["quota_rejections"] == attempt["quota_waits"] == attempt["quota_retries"] == 2
    assert attempt["quota_state"] == "ACCEPTED"
    assert "[quota-wait] cdb_simplificado.validate code=LimitExceeded retry=1" in result.output
    assert "quota_waits=2 quota_retries=2" in result.output
    assert "retries=0" in result.output


class Stop:
    def __init__(self, clock, on_wait=None):
        self.clock = clock
        self.stopped = False
        self.on_wait = on_wait

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def wait(self, delay):
        self.clock.advance(delay)
        if self.on_wait:
            self.on_wait()
        return self.stopped


class ScriptedAdapter:
    def __init__(self, responses, states=("SUCCEEDED",)):
        self.responses = iter(responses)
        self.states = iter(states)
        self.creates = []
        self.polls = []
        self.cancelled = []

    def create_run(self, arguments, display_name, opts):
        self.creates.append((arguments, display_name, opts))
        response = next(self.responses)
        if callable(response):
            response = response()
        if isinstance(response, Exception):
            raise response
        return response

    def get_run_state(self, run_id, opts):
        self.polls.append(run_id)
        state = next(self.states)
        if isinstance(state, Exception):
            raise state
        return state

    def cancel_run(self, run_id, opts):
        self.cancelled.append(run_id)


def execute(
    adapter,
    clock,
    *,
    budget=1800,
    retries=0,
    node=None,
    stop=None,
    snapshots=None,
    on_attempt=None,
    active=None,
):
    snapshots = [] if snapshots is None else snapshots

    def record(node_id, attempt):
        snapshots.append(deepcopy(attempt))
        if on_attempt:
            on_attempt(attempt)

    return P._execute_remote_node(
        {
            "id": "lci.engorda.materialize",
            "operation": "materialize",
            "product": "lci",
            "application_id": "app",
            "arguments": [],
            **(node or {}),
        },
        {"environment": "qab", "compartment_id": "cmp"},
        "pipeline-1",
        {},
        adapter,
        {"profile": "QAB"},
        retries,
        0,
        {} if active is None else active,
        threading.Lock(),
        stop or Stop(clock),
        record,
        P.ProgressReporter(enabled=False),
        quota_wait_seconds=budget,
    )


@pytest.mark.parametrize(
    "budget, delays, calls",
    [
        (0, [], 1),
        (20, [20], 1),
        (100, [30, 60, 10], 3),
        (1200, [30, 60, 120, 240, 300, 300, 150], 7),
    ],
)
def test_exponential_cap_and_remaining_budget(clock, budget, delays, calls):
    adapter = ScriptedAdapter([quota_error()] * 10)
    snapshots = []
    with pytest.raises(P.OciExecutionError, match="quota.*too small"):
        execute(adapter, clock, budget=budget, retries=5, snapshots=snapshots)
    assert clock.delays == delays
    assert len(adapter.creates) == calls
    assert adapter.polls == []
    assert snapshots[-1]["quota_state"] == "EXHAUSTED"
    assert snapshots[-1]["quota_rejections"] == calls
    assert snapshots[-1]["quota_retries"] == calls - 1
    assert snapshots[-1]["quota_waits"] == len(delays)
    assert all("run_id" not in attempt for attempt in snapshots)
    assert {attempt["attempt"] for attempt in snapshots} == {1}


@pytest.mark.parametrize(
    "jitter, expected",
    [
        (0.8, [24, 48, 96, 192, 240, 240]),
        (1.2, [36, 72, 144, 288, 300, 300]),
    ],
)
def test_jitter_is_positive_bounded_and_clipped(clock, monkeypatch, jitter, expected):
    def uniform(low, high):
        assert (low, high) == (0.8, 1.2)
        return jitter

    monkeypatch.setattr(random, "uniform", uniform)
    adapter = ScriptedAdapter([quota_error()] * 6 + ["df-1"])
    assert execute(adapter, clock).state == "SUCCEEDED"
    assert clock.delays == expected


def test_deadline_includes_subsequent_create_call_time(clock):
    def slow_rejection():
        clock.now += 80
        return quota_error()

    adapter = ScriptedAdapter([quota_error(), slow_rejection, "must-not-create"])
    with pytest.raises(P.OciExecutionError, match="exhausted"):
        execute(adapter, clock, budget=100)
    assert clock.delays == [30]
    assert len(adapter.creates) == 2


@pytest.mark.parametrize("expire_at", ["SUBMITTING", "QUOTA_WAIT"])
def test_deadline_rechecked_after_attempt_persistence(clock, expire_at):
    adapter = ScriptedAdapter([quota_error(), quota_error(), "must-not-create"])
    snapshots = []

    def persist(attempt):
        if attempt["quota_rejections"] and attempt["state"] == expire_at:
            clock.now += 100

    with pytest.raises(P.OciExecutionError, match="exhausted"):
        execute(adapter, clock, budget=100, on_attempt=persist, snapshots=snapshots)
    assert len(adapter.creates) == 1
    assert clock.delays == ([30] if expire_at == "SUBMITTING" else [])
    assert snapshots[-1]["quota_retries"] == 0
    assert snapshots[-1]["quota_waits"] == len(clock.delays)


def test_cancel_during_wait_persists_without_create_or_cancel_ocid(clock):
    stop = Stop(clock)
    stop.on_wait = stop.set
    adapter = ScriptedAdapter([quota_error(), "must-not-create"])
    snapshots = []
    result = execute(adapter, clock, stop=stop, snapshots=snapshots)
    assert result.state == "CANCELLED"
    assert len(adapter.creates) == 1
    assert adapter.cancelled == []
    assert snapshots[-1]["quota_state"] == "CANCELLED"
    assert snapshots[-1]["state"] == "CANCELLED"
    assert "run_id" not in snapshots[-1]


@pytest.mark.parametrize("status", [400, 403, 409, 429, "absent"])
@pytest.mark.parametrize("raw", [False, True])
def test_complete_service_envelope_from_bytes_stdout_or_stderr(clock, status, raw):
    payload = {"message": "x" * 1000, "code": "LimitExceeded"}
    if status != "absent":
        payload["status"] = status
    output = ("warning\nServiceError:\n" + json.dumps(payload) + "\ntrailing notes").encode()
    error = (
        subprocess.CalledProcessError(
            1, ["oci", "data-flow", "run", "create", "--secret", "credential"], output=output
        )
        if raw
        else P.OciExecutionError("truncated", returncode=1, stderr=output)
    )
    adapter = ScriptedAdapter([error, "df-1"])
    result = execute(adapter, clock)
    assert result.state == "SUCCEEDED"
    assert clock.delays == [30]
    assert len(adapter.creates) == 2
    attempt = result.attempts[0]
    assert attempt["quota_events"][0]["code"] == "LimitExceeded"
    assert "message" not in attempt["quota_events"][0]


@pytest.mark.parametrize(
    "error",
    [
        quota_error(code="TooManyRequests"),
        quota_error(code="InternalError"),
        quota_error(status=500),
        quota_error(status=408),
        quota_error(status=200),
        quota_error(status=None),
        quota_error(status="400"),
        quota_error(operation_name="get_run"),
        quota_error(operation_name="refresh_session"),
        P.OciExecutionError("LimitExceeded", returncode=1, stderr="LimitExceeded vm-total"),
        P.OciExecutionError("LimitExceeded", returncode=1, stderr='{"code":"LimitExceeded"}'),
        P.OciExecutionError(
            "bad JSON", returncode=1, stderr='ServiceError: {"code":"LimitExceeded"'
        ),
        P.OciExecutionError(
            "list", returncode=1, stderr='ServiceError: [{"code":"LimitExceeded"}]'
        ),
        P.OciExecutionError(
            "nested", returncode=1, stderr='ServiceError: {"error":{"code":"LimitExceeded"}}'
        ),
        P.OciExecutionError("timeout", stderr=quota_error().stderr),
        P.OciExecutionError("zero exit", returncode=0, stderr=quota_error().stderr),
        P.OciExecutionError("killed", returncode=-9, stderr=quota_error().stderr),
        P.OciExecutionError(
            "multiple",
            returncode=1,
            stderr=quota_error().stderr + "\n" + quota_error(status=500).stderr,
        ),
        subprocess.CalledProcessError(
            1, ["oci", "session", "refresh"], stderr=quota_error().stderr
        ),
        subprocess.CalledProcessError(
            1,
            ["oci", "data-flow", "run", "create", "credential"],
            stderr="unstructured LimitExceeded",
        ),
    ],
)
def test_ambiguous_or_nonquota_errors_never_recreate(clock, error):
    adapter = ScriptedAdapter([error, "must-not-create"])
    snapshots = []
    with pytest.raises(P.OciExecutionError) as raised:
        execute(adapter, clock, retries=3, snapshots=snapshots)
    assert "credential" not in str(raised.value)
    assert len(adapter.creates) == 1
    assert clock.delays == []
    assert snapshots[-1]["quota_retries"] == 0
    assert "run_id" not in snapshots[-1]


@pytest.mark.parametrize(
    "stderr, stdout",
    [
        (quota_error().stderr, '{"data":{"id":"df-accepted"}}'),
        ('{"data":{"id":"df-accepted"}}', quota_error().stderr),
        (quota_error().stderr + '\n{"data":{"id":"df-accepted"}}', ""),
        ('{"data":{"id":"df-accepted"}}\n' + quota_error().stderr, ""),
        (quota_error().stderr + '\n[{"id":"df-accepted"}]', ""),
        ('[{"id":"df-accepted"}]\n' + quota_error().stderr, ""),
        (quota_error().stderr, "[]"),
        (quota_error().stderr + "\n{}", ""),
        (quota_error(data={"id": "df-accepted"}).stderr, ""),
        (quota_error(id="df-accepted").stderr, ""),
        (quota_error(run_id="df-accepted").stderr, ""),
        (quota_error(details={"run_id": "df-accepted"}).stderr, ""),
        (quota_error().stderr, quota_error().stderr),
        (quota_error().stderr + "\n" + quota_error().stderr, ""),
        (quota_error().stderr, "Created run ocid1.dataflowrun.oc1.sa-saopaulo-1.accepted"),
        ("Created run ocid1.dataflowrun.oc1.sa-saopaulo-1.accepted", quota_error().stderr),
        (quota_error(message="Run ocid1.dataflowrun.oc1.region.accepted").stderr, ""),
        (quota_error().stderr + '\n{"broken": {"id":"df-accepted"}', ""),
    ],
    ids=[
        "stdout-success",
        "stderr-success",
        "trailing-object",
        "prefix-object",
        "trailing-array",
        "prefix-array",
        "other-channel-array",
        "trailing-empty-object",
        "payload-data",
        "payload-id",
        "payload-run-id",
        "nested-run-id",
        "two-channels-errors",
        "repeated-errors",
        "stdout-ocid",
        "stderr-ocid",
        "payload-ocid",
        "partial-json-with-valid-nested-object",
    ],
)
@pytest.mark.parametrize("raw", [False, True])
def test_quota_with_other_submission_evidence_never_recreates(clock, stderr, stdout, raw):
    error = (
        subprocess.CalledProcessError(
            1,
            ["oci", "data-flow", "run", "create", "credential"],
            stderr=stderr.encode(),
            output=stdout.encode(),
        )
        if raw
        else P.OciExecutionError("create rejected", returncode=1, stderr=stderr, stdout=stdout)
    )
    adapter = ScriptedAdapter([error, "must-not-create"])
    snapshots = []
    with pytest.raises(P.OciExecutionError) as raised:
        execute(adapter, clock, retries=2, budget=1800, snapshots=snapshots)
    assert "credential" not in str(raised.value)
    assert len(adapter.creates) == 1
    assert clock.delays == []
    assert snapshots[-1]["state"] == "FAILED"
    assert snapshots[-1]["quota_retries"] == 0
    assert "run_id" not in snapshots[-1]


def test_warning_brackets_and_plain_notes_do_not_prevent_definite_quota_retry(clock):
    error = quota_error()
    error.stderr = "[WARNING] use {profile} configuration\n" + error.stderr
    error.stdout = "[INFO] See CLI troubleshooting notes."
    adapter = ScriptedAdapter([error, "df-1"])
    assert execute(adapter, clock).state == "SUCCEEDED"
    assert clock.delays == [30]


@pytest.mark.parametrize(
    "notes", ["x" * 1_048_577, "[WARNING] " * 129], ids=["over-1MiB", "too-many-brackets"]
)
def test_excessive_output_scan_fails_closed(clock, notes):
    error = quota_error()
    error.stdout = notes
    adapter = ScriptedAdapter([error, "must-not-create"])
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock, retries=2)
    assert len(adapter.creates) == 1
    assert clock.delays == []


def test_json_escaped_run_ocid_in_error_payload_fails_closed(clock):
    error = quota_error(message="Run ocid1.dataflowrun.oc1.region.accepted")
    error.stderr = error.stderr.replace("ocid1.", r"ocid1\u002e")
    adapter = ScriptedAdapter([error, "must-not-create"])
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock, retries=2)
    assert len(adapter.creates) == 1
    assert clock.delays == []


def test_wrapped_quota_from_auth_refresh_is_not_a_create_rejection(clock):
    error = quota_error()
    error.__cause__ = subprocess.CalledProcessError(
        1,
        ["oci", "session", "refresh"],
        stderr=error.stderr,
    )
    adapter = ScriptedAdapter([error, "must-not-create"])
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock)
    assert len(adapter.creates) == 1
    assert clock.delays == []


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"data": {}},
        {"data": {"id": None}},
        {"data": {"id": ""}},
        {"data": None},
        {"data": []},
        {"data": "df-1"},
        {"data": 123},
    ],
)
def test_missing_run_id_never_recreates(clock, response):
    adapter = ScriptedAdapter([response, "must-not-create"])
    with pytest.raises(P.PipelineError):
        execute(adapter, clock, retries=3)
    assert len(adapter.creates) == 1
    assert clock.delays == []


def test_poll_quota_failure_never_recreates_accepted_run(clock):
    adapter = ScriptedAdapter(["df-1", "must-not-create"], [quota_error()])
    snapshots = []
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock, retries=3, snapshots=snapshots)
    assert len(adapter.creates) == 1
    assert clock.delays == []
    assert snapshots[-1]["run_id"] == "df-1"


def test_failed_execution_then_rejected_create_does_not_borrow_previous_id(clock):
    adapter = ScriptedAdapter(["df-1", quota_error()], ["FAILED"])
    snapshots = []
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock, budget=0, retries=1, snapshots=snapshots)
    assert snapshots[-1]["attempt"] == 2
    assert "run_id" not in snapshots[-1]
    attempts = [next(a for a in reversed(snapshots) if a["attempt"] == 1), snapshots[-1]]
    manifest = {
        "products": ["lci"],
        "nodes": {
            "lci.materialize": {
                "id": "lci.materialize",
                "product": "lci",
                "operation": "materialize",
                "state": "FAILED",
                "attempts": attempts,
            }
        },
    }
    lines = P.render_run_summary(manifest, "local", "remote", "SUCCEEDED")
    assert any("retries=0" in line for line in lines)
    assert any("node=lci.materialize run_id=-" in line for line in lines)


@pytest.mark.parametrize("node", [{"operation": "load"}, {"operation": "plan", "max_retries": 0}])
def test_load_and_explicit_genai_override_never_quota_retry(clock, node):
    adapter = ScriptedAdapter([quota_error(), "must-not-create"])
    with pytest.raises(P.OciExecutionError):
        execute(adapter, clock, retries=3, node=node)
    assert len(adapter.creates) == 1
    assert clock.delays == []


def test_post_create_cancel_race_retains_accepted_id(clock):
    stop = Stop(clock)

    def accept_and_stop():
        stop.set()
        return "df-1"

    adapter = ScriptedAdapter([quota_error(), accept_and_stop])
    active = {}
    snapshots = []
    result = execute(adapter, clock, stop=stop, active=active, snapshots=snapshots)
    assert result.state == "CANCELLED"
    assert adapter.cancelled == ["df-1"]
    assert adapter.polls == []
    assert active == {}
    assert snapshots[-1]["run_id"] == "df-1"
    assert snapshots[-1]["state"] == "CANCELLED"


@pytest.mark.parametrize("response", [P.OciExecutionError("create timeout"), {"data": {}}])
def test_ambiguity_after_quota_wait_is_persisted_as_aborted(clock, response):
    adapter = ScriptedAdapter([quota_error(), response, "must-not-create"])
    snapshots = []
    with pytest.raises((P.OciExecutionError, P.PipelineError)):
        execute(adapter, clock, retries=3, snapshots=snapshots)
    assert len(adapter.creates) == 2
    assert snapshots[-1]["state"] == "FAILED"
    assert snapshots[-1]["quota_state"] == "ABORTED"
    assert "run_id" not in snapshots[-1]


@pytest.mark.parametrize(
    "error",
    [
        ValueError("unexpected adapter response"),
        json.JSONDecodeError("invalid create JSON", "{", 1),
        KeyError("id"),
        TypeError("invalid response type"),
        AttributeError("invalid response attribute"),
    ],
)
def test_unexpected_create_error_after_quota_persists_aborted_attempt(clock, error):
    adapter = ScriptedAdapter([quota_error(), error, "must-not-create"])
    snapshots = []
    with pytest.raises(type(error)) as raised:
        execute(adapter, clock, retries=2, snapshots=snapshots)
    assert raised.value is error
    assert len(adapter.creates) == 2
    assert adapter.polls == []
    assert clock.delays == [30]
    assert snapshots[-1]["state"] == "FAILED"
    assert snapshots[-1]["quota_state"] == "ABORTED"
    assert snapshots[-1]["quota_retries"] == len(adapter.creates) - 1
    assert snapshots[-1]["quota_rejections"] == snapshots[-1]["quota_waits"] == 1
    assert "run_id" not in snapshots[-1]


def test_raw_create_failure_after_quota_is_sanitized_and_persisted(clock):
    error = subprocess.CalledProcessError(
        1,
        ["oci", "data-flow", "run", "create", "credential"],
        stderr="unknown failure",
    )
    adapter = ScriptedAdapter([quota_error(), error, "must-not-create"])
    snapshots = []
    with pytest.raises(P.OciExecutionError) as raised:
        execute(adapter, clock, retries=2, snapshots=snapshots)
    assert "credential" not in str(raised.value)
    assert len(adapter.creates) == 2
    assert snapshots[-1]["state"] == "FAILED"
    assert snapshots[-1]["quota_state"] == "ABORTED"
    assert snapshots[-1]["quota_retries"] == 1
    assert "run_id" not in snapshots[-1]


@pytest.mark.parametrize("data", [None, [], "df-1", 123])
def test_malformed_create_response_after_quota_is_normalized_and_persisted(clock, data):
    adapter = ScriptedAdapter([quota_error(), {"data": data}, "must-not-create"])
    snapshots = []
    with pytest.raises(P.PipelineError, match="invalid create response"):
        execute(adapter, clock, retries=2, snapshots=snapshots)
    assert len(adapter.creates) == 2
    assert snapshots[-1]["state"] == "FAILED"
    assert snapshots[-1]["quota_state"] == "ABORTED"
    assert snapshots[-1]["quota_retries"] == 1
    assert "run_id" not in snapshots[-1]


def test_create_keyboard_interrupt_is_not_caught_as_submission_failure(clock):
    def interrupt():
        raise KeyboardInterrupt()

    adapter = ScriptedAdapter([quota_error(), interrupt, "must-not-create"])
    snapshots = []
    with pytest.raises(KeyboardInterrupt):
        execute(adapter, clock, snapshots=snapshots)
    assert len(adapter.creates) == 2
    assert snapshots[-1]["state"] == "SUBMITTING"
    assert snapshots[-1]["quota_state"] != "ABORTED"


def test_unexpected_poll_error_is_not_caught_as_submission_failure(clock):
    adapter = ScriptedAdapter([quota_error(), "df-1", "must-not-create"], [ValueError("poll")])
    snapshots = []
    with pytest.raises(ValueError, match="poll"):
        execute(adapter, clock, retries=2, snapshots=snapshots)
    assert len(adapter.creates) == 2
    assert adapter.polls == ["df-1"]
    assert snapshots[-1]["state"] == "RUNNING"
    assert snapshots[-1]["quota_state"] == "ACCEPTED"
    assert snapshots[-1]["run_id"] == "df-1"


def test_accepted_create_after_deadline_is_polled_never_recreated(clock):
    def slow_success():
        clock.now += 100
        return "df-1"

    adapter = ScriptedAdapter([quota_error(), slow_success, "must-not-create"])
    result = execute(adapter, clock, budget=100)
    assert result.state == "SUCCEEDED"
    assert adapter.polls == ["df-1"]
    assert len(adapter.creates) == 2


def test_quota_events_are_bounded_but_counts_are_complete(clock):
    adapter = ScriptedAdapter([quota_error()] * 25 + ["df-1"])
    result = execute(adapter, clock, budget=86400)
    attempt = result.attempts[0]
    assert attempt["quota_rejections"] == attempt["quota_retries"] == 25
    assert len(attempt["quota_events"]) == 20


def test_execution_retry_budget_is_separate_from_quota_retries(clock):
    adapter = ScriptedAdapter(
        [quota_error(), "df-1", quota_error(), quota_error(), "df-2", "must-not-create"],
        ["FAILED", "FAILED"],
    )
    result = execute(adapter, clock, retries=1)
    assert result.state == "FAILED"
    assert len(adapter.creates) == 5
    assert [a["attempt"] for a in result.attempts] == [1, 2]
    assert [a["run_id"] for a in result.attempts] == ["df-1", "df-2"]
    assert [a["quota_retries"] for a in result.attempts] == [1, 2]


def test_quota_retries_reenter_normal_adapter_auth_transport(clock, monkeypatch):
    adapter = P.ModuleAdapter(auth_refresh_seconds=20)
    adapter._auth_context = ({"auth": "security_token"}, False, lambda _message: False, "app")
    adapter._last_auth_refresh = clock.now
    refreshes = []
    commands = []

    def refresh():
        refreshes.append(clock.now)
        adapter._last_auth_refresh = clock.now

    def run(command, **kwargs):
        commands.append(command)
        if command[1:4] == ["data-flow", "run", "create"]:
            if len(commands) <= 2:
                raise subprocess.CalledProcessError(1, command, stderr=quota_error().stderr)
            payload = {"data": {"id": "df-1"}}
        else:
            payload = {"data": {"lifecycle-state": "SUCCEEDED"}}
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload))

    monkeypatch.setattr(adapter, "_reauthenticate", refresh)
    monkeypatch.setattr(P, "_run", run)
    result = execute(adapter, clock)
    assert result.state == "SUCCEEDED"
    assert refreshes == [130, 190]
    assert [command[3] for command in commands] == ["create", "create", "create", "get"]
    assert all(command[-2:] == ["--profile", "QAB"] for command in commands)


def test_other_node_completes_while_quota_worker_waits(tmp_path, monkeypatch):
    waiting = threading.Event()
    completed = threading.Event()
    original_wait = threading.Event.wait
    wait_observed = []

    def wait(event, timeout=None):
        if timeout is not None and timeout >= 24:
            waiting.set()
            assert original_wait(completed, 2), "other worker could not complete"
            wait_observed.append(True)
            return event.is_set()
        return original_wait(event, timeout)

    class ConcurrentAdapter(FakeAdapter):
        rejected = False

        def create_run(self, arguments, display_name, opts):
            if display_name.startswith("cdb_simplificado") and not self.rejected:
                self.rejected = True
                raise quota_error()
            return super().create_run(arguments, display_name, opts)

        def get_run_state(self, run_id, opts):
            if self.created[run_id]["display_name"].startswith("lci"):
                assert original_wait(waiting, 2), "quota worker did not reach wait"
            return "SUCCEEDED"

        def describe_uri(self, uri, *, auth):
            if "/lci/validation/" in uri:
                completed.set()
            return {}

    monkeypatch.setattr(threading.Event, "wait", wait)
    adapter = ConcurrentAdapter()
    args = run_args(
        tmp_path,
        write_config(tmp_path),
        write_upstream(tmp_path),
        "--product",
        "lci",
        "--max-concurrency",
        "2",
    )
    args[args.index("--from") + 1] = "validate"
    result = CliRunner().invoke(P.cli, args, obj={"adapter": adapter})
    assert result.exit_code == 0, result.output
    assert wait_observed == [True]
    assert len(adapter.created) == 2
    assert all(
        node["state"] == "SUCCEEDED" for node in read_run_manifest(tmp_path)["nodes"].values()
    )


@pytest.mark.parametrize("budget", [0, 1, 86400])
def test_cli_policy_is_resolved_in_dry_run(tmp_path, budget):
    result = CliRunner().invoke(
        P.cli,
        run_args(
            tmp_path,
            write_config(tmp_path),
            write_upstream(tmp_path),
            "--dry-run",
            "--quota-wait-seconds",
            str(budget),
        ),
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["quota_wait_seconds"] == budget


@pytest.mark.parametrize("budget", ["-1", "86401", "1.5"])
def test_cli_rejects_invalid_quota_budget(budget):
    result = CliRunner().invoke(P.cli, ["run", "--quota-wait-seconds", budget])
    assert result.exit_code == 2
    assert "Invalid value for '--quota-wait-seconds'" in result.output


def test_cli_zero_quota_budget_disables_retry(tmp_path):
    class Rejected(FakeAdapter):
        submissions = 0

        def create_run(self, arguments, display_name, opts):
            self.submissions += 1
            raise quota_error()

    adapter = Rejected()
    result = CliRunner().invoke(
        P.cli,
        run_args(
            tmp_path, write_config(tmp_path), write_upstream(tmp_path), "--quota-wait-seconds", "0"
        ),
        obj={"adapter": adapter},
    )
    assert result.exit_code == 1
    assert adapter.submissions == 1
    assert "quota_waits=0 quota_retries=0" in result.output
    assert "run_id=-" in result.output
    assert read_run_manifest(tmp_path)["quota_wait_seconds"] == 0


def test_cli_help_explains_separate_quota_policy():
    result = CliRunner().invoke(P.cli, ["run", "--help"])
    text = " ".join(result.output.split())
    assert "--quota-wait-seconds" in text
    assert "independent of quota waiting (even at 0)" in text
    assert "No quota retries for load or GenAI planning" in text


def test_load_quota_rejection_keeps_claim_and_quarantines_lease(tmp_path):
    class Rejected(FakeAdapter):
        submissions = 0

        def create_run(self, arguments, display_name, opts):
            self.submissions += 1
            raise quota_error()

    adapter = Rejected()
    args = run_args(tmp_path, write_config(tmp_path), write_upstream(tmp_path), "--approve-load")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"
    result = CliRunner().invoke(P.cli, args, obj={"adapter": adapter})
    assert result.exit_code == 1
    assert adapter.submissions == 1
    assert adapter.load_lease_quarantines
    assert adapter.load_lease_releases == 0
    assert any(value.get("kind") == "load-claim" for value in adapter.objects.values())


def test_genai_planning_quota_rejection_is_not_retried(tmp_path):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["genai"] = {
        "endpoint_id": "ocid1.generativeaiendpoint.test",
        "compartment_id": "ocid1.compartment.test",
        "region": "sa-saopaulo-1",
    }
    payload["stage_defaults"]["engorda"]["genai_policy"] = "oci://source@namespace/policy.json"
    payload["stage_defaults"]["engorda"]["genai_rows"] = 10000
    config.write_text(json.dumps(payload))

    class Rejected(FakeAdapter):
        submissions = 0

        def create_run(self, arguments, display_name, opts):
            self.submissions += 1
            raise quota_error()

    adapter = Rejected(objects={
        "oci://source@namespace/policy.json": {"products": {"cdb_simplificado": {}}},
    })
    result = CliRunner().invoke(
        P.cli,
        run_args(tmp_path, config, write_upstream(tmp_path)),
        obj={"adapter": adapter},
    )
    assert result.exit_code == 1, result.output
    assert adapter.submissions == 1
    node = read_run_manifest(tmp_path)["nodes"]["cdb_simplificado.engorda.plan"]
    assert node["max_retries"] == 0
    assert node["attempts"][0]["quota_waits"] == 0
