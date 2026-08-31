import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import run_pipeline as P  # noqa: E402, I001


ALL_PRODUCTS = tuple(P.PRODUCTS)
LEGACY_PRODUCTS = (
    "cdb_simplificado",
    "cdb_resgate",
    "cdb_escalonamento",
    "rdb_inclusao",
    "rdb_resgate",
    "lci",
    "lca",
)


def write_config(tmp_path, *, capabilities=None, extra=None):
    products = {
        product: {"capabilities": sorted(P.PRODUCTS[product]["capabilities"])}
        for product in ALL_PRODUCTS
    }
    if capabilities:
        products.update(capabilities)
    payload = {
        "version": 1,
        "environment": "qab",
        "compartment_id": "ocid1.compartment.test",
        "artifact_root": "oci://bucket@namespace/runs",
        "manifest_root": "oci://bucket@namespace/manifests",
        "applications": {
            "engorda_plan": "app-plan",
            "engorda_materialize": "app-materialize",
            "validate": "app-validate",
            "load": "app-load",
        },
        "reservations": {
            "lease_uri": "oci://bucket@namespace/control/lease.json",
            "ledger_uri": "oci://bucket@namespace/control/ledger.json",
        },
        "load": {
            "lease_uri": "oci://bucket@namespace/control/load-lease.json",
            "claim_root": "oci://bucket@namespace/control/load-claims",
            "target_schema": "CETIP",
            "lease_ttl_seconds": 300,
        },
        "products": products,
        "stage_defaults": {
            "engorda": {
                "n_instrumentos": 4,
                "fator_k": 2,
                "seed": 7,
                "specs": "oci://source@namespace/specs.json",
                "query_num_if_sql": "oci://source@namespace/queries_produtos.sql",
            },
            "validate": {"fail_severity": "error", "validate_against": "union"},
            "load": {"num_partitions": 16, "batch_size": 1000},
        },
    }
    if extra:
        payload.update(extra)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload))
    return path


def write_upstream(tmp_path, *, products=None):
    products = tuple(products or ALL_PRODUCTS)
    artifacts = {
        "raw": {"uri": "oci://source@namespace/raw", "producer": "external"},
        "faltantes": {
            "uri": "oci://source@namespace/faltantes",
            "producer": "external",
        },
        "products": {
            product: {
                "synthetic": {
                    "uri": f"oci://source@namespace/synthetic/{product}",
                    "producer": "upstream",
                },
                "validation_report": {
                    "uri": f"oci://source@namespace/validation/{product}/report.json",
                    "producer": "upstream",
                },
            }
            for product in products
        },
    }
    path = tmp_path / "upstream.json"
    path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "kind": "adopted-inputs",
                "environment": "qab",
                "status": "ADOPTED",
                "products": list(products),
                "artifacts": artifacts,
            }
        )
    )
    return path


def run_args(tmp_path, config, upstream, *extra):
    return [
        "run",
        "--config",
        str(config),
        "--product",
        "cdb_simplificado",
        "--from",
        "engorda",
        "--to",
        "validate",
        "--upstream-manifest",
        str(upstream),
        "--run-id",
        "run-001",
        "--local-run-root",
        str(tmp_path / "local-runs"),
        "--poll-seconds",
        "0",
        "--max-retries",
        "0",
        *extra,
    ]


class FakeAdapter:
    def __init__(self, *, failed_prefixes=(), reports=None, existing=(), objects=None):
        self.failed_prefixes = tuple(failed_prefixes)
        self.reports = reports or {}
        self.existing = set(existing)
        self.objects = dict(objects or {})
        self.calls = []
        self.created = {}
        self.cancelled = []
        self.uploads = []
        self.reservations = []
        self._lock = threading.Lock()
        self._active = set()
        self.max_active = 0
        self._active_reservations = 0
        self.max_active_reservations = 0
        self._active_loads = 0
        self.max_active_loads = 0
        self.load_lease_acquisitions = 0
        self.load_lease_releases = 0
        self.load_lease_renewals = []
        self.load_lease_quarantines = []

    def uri_exists(self, uri, *, auth):
        self.calls.append(("uri_exists", uri, auth))
        return uri in self.existing or uri in self.objects

    def describe_uri(self, uri, *, auth):
        self.calls.append(("describe_uri", uri, auth))
        if uri.startswith("oci://source@") and uri not in self.existing:
            raise P.PipelineError(f"OCI input URI does not exist: {uri}")
        return {
            "object_count": 2,
            "total_bytes": 42,
            "inventory_sha256": f"sha256-{uri.rsplit('/', 1)[-1]}",
        }

    def create_run(self, arguments, display_name, opts):
        with self._lock:
            run_id = f"df-{len(self.created) + 1}"
            self.created[run_id] = {
                "arguments": list(arguments),
                "display_name": display_name,
                "opts": dict(opts),
            }
            self._active.add(run_id)
            self.max_active = max(self.max_active, len(self._active))
            if "-load-" in display_name:
                self._active_loads += 1
                self.max_active_loads = max(self.max_active_loads, self._active_loads)
        return {"data": {"id": run_id}}

    def get_run_state(self, run_id, opts):
        time.sleep(0.005)
        with self._lock:
            self._active.discard(run_id)
            if "-load-" in self.created[run_id]["display_name"]:
                self._active_loads -= 1
        display_name = self.created[run_id]["display_name"]
        return "FAILED" if display_name.startswith(self.failed_prefixes) else "SUCCEEDED"

    def read_json(self, uri, *, auth):
        if uri in self.objects:
            return dict(self.objects[uri])
        if uri.endswith("/load/manifest.json"):
            load_call = next(
                call
                for call in self.created.values()
                if "--manifest-uri" in call["arguments"]
                and call["arguments"][call["arguments"].index("--manifest-uri") + 1]
                == uri
            )
            arguments = load_call["arguments"]

            def value(flag):
                return arguments[arguments.index(flag) + 1]

            manifest = {
                "schema_version": 1,
                "kind": "load-attempt",
                "run_id": value("--run-id"),
                "product": value("--product"),
                "validation_product": value("--validation-product"),
                "input_uri": value("--input-base"),
                "validation_report_uri": value("--validation-report"),
                "pipeline_manifest_uri": value("--pipeline-manifest-uri"),
                "target_schema": value("--expected-target-schema"),
                "ordered_tables": ["INSTRUMENTO_FINANCEIRO"],
                "tables": [{
                    "table": "INSTRUMENTO_FINANCEIRO",
                    "owner": "CETIP",
                    "name": "INSTRUMENTO_FINANCEIRO",
                    "expected_rows": 1,
                    "pk_col": "NUM_IF",
                    "synthetic_pk_min": 100,
                    "synthetic_pk_max": 100,
                    "rollbackable": True,
                }],
                "transformations": [],
            }
            if "--previous-load-manifest" in arguments:
                manifest["previous_load_manifest"] = value("--previous-load-manifest")
            self.objects[uri] = manifest
            return dict(manifest)
        generator_product = next(
            (product for product in ALL_PRODUCTS if f"/{product}/" in uri),
            "cdb_simplificado",
        )
        for product, report in self.reports.items():
            if f"/{product}/" in uri:
                payload = dict(report)
                break
        else:
            payload = {"verdict": "PASS", "counts": {"error": 0}}
        validation_call = next(
            (
                call
                for call in self.created.values()
                if "--report-path" in call["arguments"]
                and call["arguments"][call["arguments"].index("--report-path") + 1]
                == uri
            ),
            None,
        )
        payload.setdefault("product", P.PRODUCTS[generator_product]["validator_product"])
        if validation_call is not None:
            arguments = validation_call["arguments"]
            input_uri = arguments[arguments.index("--input-base") + 1]
        else:
            input_uri = f"oci://source@namespace/synthetic/{generator_product}"
        payload.setdefault("resolved_input", input_uri)
        payload.setdefault("schema_version", 2)
        payload.setdefault("table_inventory", ["INSTRUMENTO_FINANCEIRO"])
        return payload

    def reserve_ranges(self, **kwargs):
        with self._lock:
            self._active_reservations += 1
            self.max_active_reservations = max(
                self.max_active_reservations, self._active_reservations
            )
        try:
            time.sleep(0.005)
            self.reservations.append(kwargs)
            return {"uri": kwargs["reservation_uri"], "etag": "etag-1"}
        finally:
            with self._lock:
                self._active_reservations -= 1

    def cancel_run(self, run_id, opts):
        self.cancelled.append((run_id, opts))

    def upload_file(self, path, uri, *, auth):
        payload = json.loads(Path(path).read_text())
        self.uploads.append((payload, uri, auth))
        self.objects[uri] = payload

    def put_json_create_once(self, uri, payload, *, auth):
        if uri in self.objects:
            raise P.PipelineError(f"create-once JSON object already exists: {uri}")
        self.objects[uri] = dict(payload)
        return "claim-etag"

    def acquire_load_lease(self, uri, environment, run_id, ttl_seconds, *, auth):
        self.load_lease_acquisitions += 1
        return P._Lease({"environment": environment, "run_id": run_id}, "lease-etag")

    def renew_load_lease(self, uri, lease, ttl_seconds, *, auth):
        self.load_lease_renewals.append(ttl_seconds)
        return lease

    def quarantine_load_lease(self, uri, lease, reason, *, auth):
        self.load_lease_quarantines.append(reason)
        return lease

    def release_load_lease(self, uri, lease, *, auth):
        self.load_lease_releases += 1


class NoCallsAdapter:
    def __getattr__(self, name):
        raise AssertionError(f"offline command called adapter.{name}")


class PollingAdapter(FakeAdapter):
    def __init__(self):
        super().__init__()
        self.polls = {}

    def get_run_state(self, run_id, opts):
        states = ("ACCEPTED", "IN_PROGRESS", "SUCCEEDED")
        index = self.polls.get(run_id, 0)
        self.polls[run_id] = index + 1
        if states[index] == "SUCCEEDED":
            with self._lock:
                self._active.discard(run_id)
        return states[index]


def read_run_manifest(tmp_path, run_id="run-001"):
    return json.loads(
        (tmp_path / "local-runs" / "qab" / run_id / "manifest.json").read_text()
    )


def test_click_cli_reports_submission_and_each_mocked_poll(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    adapter = PollingAdapter()

    arguments = run_args(tmp_path, config, upstream)
    arguments[arguments.index("--poll-seconds") + 1] = "0.001"
    result = CliRunner().invoke(
        P.cli,
        arguments,
        obj={"adapter": adapter},
    )

    assert result.exit_code == 0, result.output
    assert "[run] id=run-001" in result.output
    assert "poll=0.001s" in result.output
    assert "[preflight] checking OCI run path" in result.output
    assert "[preflight] OCI paths are available" in result.output
    assert "[submit] cdb_simplificado.engorda.plan" in result.output
    assert "[poll] cdb_simplificado.engorda.plan" in result.output
    assert "ACCEPTED" in result.output
    assert "IN_PROGRESS" in result.output
    assert "[done] cdb_simplificado.validate SUCCEEDED" in result.output


def test_click_help_exposes_commands_and_polling_default():
    runner = CliRunner()

    root = runner.invoke(P.cli, ["--help"])
    run = runner.invoke(P.cli, ["run", "--help"])

    assert root.exit_code == 0
    assert "adopt-inputs" in root.output
    assert "run" in root.output
    assert run.exit_code == 0
    assert "--poll-seconds" in run.output
    assert "30" in run.output
    assert "--oci-timeout-seconds" in run.output
    assert "60" in run.output
    assert "--auth-prompt / --no-auth-prompt" in run.output
    assert "--region" in run.output
    assert "--auth-refresh-seconds" in run.output
    assert "1800" in run.output


def test_click_dry_run_finishes_without_submitting_jobs(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))

    result = CliRunner().invoke(
        P.cli,
        [*run_args(tmp_path, config, upstream), "--dry-run"],
        obj={"adapter": NoCallsAdapter()},
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["dry_run"] is True
    assert result.stdout.lstrip().startswith("{")
    assert "[dry-run] resolved pipeline plan" in result.output
    assert "[dry-run] resolved pipeline plan" in result.stderr
    assert "[done] dry-run complete" in result.output
    assert "[submit]" not in result.output


def test_live_wrapper_matches_shared_oci_dataflow_operation_signatures():
    calls = []

    class SharedModule:
        @staticmethod
        def create_run(arguments, display_name, opts):
            calls.append(("create", arguments, display_name, opts))
            return "df-1"

        @staticmethod
        def get_run_state(run_id, opts):
            calls.append(("get", run_id, opts))
            return "SUCCEEDED"

        @staticmethod
        def cancel_run(run_id, opts):
            calls.append(("cancel", run_id, opts))
            return "CANCELING"

    adapter = P.ModuleAdapter.__new__(P.ModuleAdapter)
    adapter.module = SharedModule()
    auth = {"profile": "QAB"}

    opts = {"application_id": "app", "compartment_id": "cmp", **auth}
    assert adapter.create_run(
        ["--product", "cdb"],
        "name",
        opts,
    ) == "df-1"
    assert adapter.get_run_state("df-1", auth) == "SUCCEEDED"
    assert adapter.cancel_run("df-1", auth) == "CANCELING"
    assert calls == [
        (
            "create",
            ["--product", "cdb"],
            "name",
            {"application_id": "app", "compartment_id": "cmp", "profile": "QAB"},
        ),
        ("get", "df-1", auth),
        ("cancel", "df-1", auth),
    ]


def test_manifest_upload_is_create_once_not_force_overwrite(tmp_path):
    commands = []

    class SharedModule:
        @staticmethod
        def oci_auth_flags(_auth):
            return []

        @staticmethod
        def run_json(command):
            commands.append(command)
            return {}

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    adapter = P.ModuleAdapter.__new__(P.ModuleAdapter)
    adapter.module = SharedModule()

    adapter.upload_file(
        str(manifest), "oci://bucket@namespace/manifests/run.json", auth={}
    )

    assert "--no-overwrite" in commands[0]
    assert "--force" not in commands[0]


def test_atomic_manifest_retries_windows_access_denied(monkeypatch, tmp_path):
    manifest = tmp_path / "manifest.json"
    real_replace = os.replace
    calls = []
    sleeps = []

    def flaky_replace(source, destination):
        calls.append((source, destination))
        if len(calls) <= 2:
            error = PermissionError(5, "Access is denied")
            error.winerror = 5
            raise error
        real_replace(source, destination)

    monkeypatch.setattr(P.os, "replace", flaky_replace)
    monkeypatch.setattr(P.time, "sleep", sleeps.append)

    P.AtomicManifest(manifest, {"status": "RUNNING", "nodes": {}})

    assert len(calls) == 3
    assert sleeps == [0.05, 0.1]
    assert json.loads(manifest.read_text()) == {"status": "RUNNING", "nodes": {}}


def test_atomic_manifest_persistent_permission_error_cleans_temp(monkeypatch, tmp_path):
    manifest = tmp_path / "manifest.json"
    calls = []
    sleeps = []
    denied = PermissionError(5, "Access is denied")
    denied.winerror = 5

    def denied_replace(source, destination):
        calls.append((source, destination))
        raise denied

    monkeypatch.setattr(P.os, "replace", denied_replace)
    monkeypatch.setattr(P.time, "sleep", sleeps.append)

    with pytest.raises(PermissionError) as raised:
        P.AtomicManifest(manifest, {"status": "RUNNING"})

    assert raised.value is denied
    assert len(calls) == P.MANIFEST_REPLACE_ATTEMPTS
    assert sleeps == [0.05, 0.1, 0.2, 0.4, 0.5, 0.5, 0.5]
    assert not manifest.exists()
    assert list(tmp_path.glob(".manifest.json.*.tmp")) == []


def test_atomic_manifest_serializes_concurrent_updates(tmp_path):
    manifest = tmp_path / "manifest.json"
    store = P.AtomicManifest(manifest, {"values": []})
    workers = [
        threading.Thread(
            target=store.update,
            args=(lambda payload, value=value: payload["values"].append(value),),
        )
        for value in range(20)
    ]

    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    assert sorted(json.loads(manifest.read_text())["values"]) == list(range(20))


def test_object_json_download_does_not_use_unsupported_force_flag():
    commands = []

    class SharedModule:
        @staticmethod
        def oci_auth_flags(_auth):
            return []

        @staticmethod
        def run_json(command):
            commands.append(command)
            Path(command[command.index("--file") + 1]).write_text('{"ok": true}')
            return {}

    adapter = P.ModuleAdapter.__new__(P.ModuleAdapter)
    adapter.module = SharedModule()

    assert adapter.read_json(
        "oci://bucket@namespace/path/report.json", auth={}
    ) == {"ok": True}
    assert "--force" not in commands[0]


def test_object_prefix_preflight_samples_one_object_instead_of_listing_all():
    commands = []

    class SharedModule:
        @staticmethod
        def oci_auth_flags(_auth):
            return []

        @staticmethod
        def run_json(command):
            commands.append(command)
            return {
                "data": [
                    {"name": "part-0", "etag": "etag-0", "size": 42},
                ]
            }

    adapter = P.ModuleAdapter.__new__(P.ModuleAdapter)
    adapter.module = SharedModule()
    uri = "oci://bucket@namespace/one-terabyte-prefix"

    assert adapter.uri_exists(uri, auth={}) is True
    metadata = adapter.describe_uri(uri, auth={})

    assert all("--all" not in command for command in commands)
    assert all(command[command.index("--limit") + 1] == "1" for command in commands)
    assert metadata["inventory_mode"] == "sample"
    assert metadata["inventory_complete"] is False
    assert metadata["object_count_sampled"] == 1


def test_single_copied_script_adopts_inputs_without_sibling_modules(tmp_path):
    standalone = tmp_path / "run_pipeline.py"
    shutil.copy(Path(P.__file__), standalone)
    config = write_config(tmp_path)
    output = tmp_path / "adopted.json"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_oci = fake_bin / "oci"
    fake_oci.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' '{\"data\":[{\"name\":\"part-0\","
        "\"etag\":\"etag-1\",\"size\":42}]}'\n"
    )
    fake_oci.chmod(0o755)
    environment = {**os.environ, "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"}

    result = subprocess.run(
        [
            "uv",
            "run",
            "--no-project",
            str(standalone),
            "adopt-inputs",
            "--config",
            str(config),
            "--product",
            "cdb_resgate",
            "--raw-uri",
            "oci://source@namespace/raw",
            "--faltantes-uri",
            "oci://source@namespace/faltantes",
            "--output-manifest",
            str(output),
        ],
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text())["status"] == "ADOPTED"
    assert "[oci] os object list" in result.stderr


def test_oci_subprocess_timeout_is_reported(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(P.subprocess, "run", timeout)

    with pytest.raises(P.OciExecutionError, match="timed out after 0.1s"):
        P._run(["oci", "os", "object", "list"], timeout_seconds=0.1)


def test_security_token_subprocess_declines_hidden_cli_prompt(monkeypatch):
    options = {}

    def run(command, **kwargs):
        options.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

    monkeypatch.setattr(P.subprocess, "run", run)
    P._run([
        "oci", "data-flow", "application", "get",
        "--auth", "security_token",
    ])

    assert options["input"] == "n\n"
    assert options["capture_output"] is True


def test_cli_session_expired_message_is_an_authentication_error():
    error = subprocess.CalledProcessError(
        1,
        ["oci"],
        stderr="ERROR: This CLI session has expired, so it cannot currently be used",
    )

    assert P._is_authentication_error(error)


def test_preflight_oci_failure_is_operational_not_usage_error(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))

    class FailedPreflight(FakeAdapter):
        def uri_exists(self, uri, *, auth):
            raise P.OciExecutionError("OCI command 'os object list' timed out after 1s")

    result = CliRunner().invoke(
        P.cli,
        run_args(tmp_path, config, upstream),
        obj={"adapter": FailedPreflight()},
    )

    assert result.exit_code == 1
    assert "timed out after 1s" in result.output
    assert "Usage:" not in result.output


def test_security_token_refresh_has_visible_feedback(monkeypatch):
    calls = []

    def run(command, *, timeout_seconds=None):
        calls.append(list(command))
        if len(calls) == 1:
            raise subprocess.CalledProcessError(
                1, command, stderr="status: 401 NotAuthenticated"
            )
        stdout = '{"data": []}' if len(calls) == 3 else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(P, "_run", run)
    messages = []

    class Progress:
        def emit(self, message):
            messages.append(message)

    assert P.run_json(
        ["oci", "os", "object", "list", "--auth", "security_token"],
        timeout_seconds=5,
        progress=Progress(),
    ) == {"data": []}

    assert calls[1][:3] == ["oci", "session", "refresh"]
    assert messages == [
        "[oci] os object list timeout=5s",
        "[auth] security token expired; refreshing OCI session",
        "[auth] OCI session refreshed; retrying command once",
    ]


def test_mid_poll_401_uses_runner_reauthentication_callback(monkeypatch):
    calls = []
    reauthentications = []

    def run(command, *, timeout_seconds=None, interactive=False):
        calls.append(list(command))
        if len(calls) == 1:
            raise subprocess.CalledProcessError(
                1, command, stderr="status: 401 NotAuthenticated"
            )
        return subprocess.CompletedProcess(
            command, 0, stdout='{"data": {"lifecycle-state": "IN_PROGRESS"}}', stderr=""
        )

    monkeypatch.setattr(P, "_run", run)
    result = P.run_json(
        ["oci", "data-flow", "run", "get", "--auth", "security_token"],
        timeout_seconds=5,
        reauthenticate=lambda: reauthentications.append("browser-capable-flow"),
    )

    assert result["data"]["lifecycle-state"] == "IN_PROGRESS"
    assert reauthentications == ["browser-capable-flow"]
    assert not any(command[1:3] == ["session", "refresh"] for command in calls)


def test_invalid_data_flow_auth_probe_prompts_refresh(monkeypatch):
    calls = []
    prompts = []

    def run(command, *, timeout_seconds=None):
        calls.append(list(command))
        if command[1:4] == ["data-flow", "application", "get"] and len(calls) == 1:
            raise subprocess.CalledProcessError(
                1, command, stderr="status: 401 NotAuthenticated"
            )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    class Progress:
        def __init__(self):
            self.messages = []

        def emit(self, message):
            self.messages.append(message)

    monkeypatch.setattr(P, "_run", run)
    progress = Progress()
    adapter = P.ModuleAdapter(timeout_seconds=5, progress=progress)

    adapter.ensure_auth(
        {
            "profile": "p-lmirabella",
            "config_file": "C:\\Users\\p-lmirabella\\.oci\\config",
            "auth": "security_token",
        },
        allow_prompt=True,
        prompt=lambda message: prompts.append(message) or True,
        application_id="ocid1.dataflowapplication.test",
    )

    assert [command[1:4] for command in calls] == [
        ["data-flow", "application", "get"],
        ["session", "refresh", "--profile"],
        ["data-flow", "application", "get"],
    ]
    assert prompts == ["OCI security-token session is invalid. Refresh it now?"]
    assert progress.messages[-1] == "[auth] OCI session refreshed and revalidated"


def test_valid_session_is_refreshed_before_submitting_long_run(monkeypatch):
    calls = []

    def run(command, *, timeout_seconds=None, interactive=False):
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(P, "_run", run)
    adapter = P.ModuleAdapter(timeout_seconds=5, auth_refresh_seconds=1800)
    adapter.ensure_auth(
        {"profile": "QAB", "auth": "security_token"},
        allow_prompt=True,
        prompt=lambda _message: True,
        application_id="ocid1.dataflowapplication.test",
        force_refresh=True,
    )

    assert [command[1:3] for command in calls] == [
        ["data-flow", "application"],
        ["session", "refresh"],
        ["data-flow", "application"],
    ]
    assert adapter._last_auth_refresh > 0


def test_auth_refresh_interval_triggers_during_polling(monkeypatch):
    adapter = P.ModuleAdapter(auth_refresh_seconds=1800)
    adapter._auth_context = (
        {"auth": "security_token"}, True, lambda _message: True, "app"
    )
    adapter._last_auth_refresh = 100
    refreshed = []
    monkeypatch.setattr(P.time, "monotonic", lambda: 2000)
    monkeypatch.setattr(adapter, "_reauthenticate", lambda: refreshed.append(True))

    adapter._refresh_auth_if_due()

    assert refreshed == [True]


def test_adopt_inputs_auth_probe_uses_object_storage_namespace(monkeypatch):
    calls = []

    def run(command, *, timeout_seconds=None):
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(P, "_run", run)
    P.ModuleAdapter(timeout_seconds=5).ensure_auth(
        {"profile": "QAB", "auth": "security_token"},
        allow_prompt=True,
        prompt=lambda _message: True,
        application_id=None,
    )

    assert calls[0][1:4] == ["os", "ns", "get"]


def test_non_auth_data_flow_probe_error_does_not_prompt(monkeypatch):
    def forbidden(command, *, timeout_seconds=None):
        raise subprocess.CalledProcessError(1, command, stderr="status: 403 Forbidden")

    monkeypatch.setattr(P, "_run", forbidden)
    prompts = []
    with pytest.raises(P.OciExecutionError, match="data-flow application get"):
        P.ModuleAdapter(timeout_seconds=5).ensure_auth(
            {"profile": "QAB", "auth": "security_token"},
            allow_prompt=True,
            prompt=lambda message: prompts.append(message) or True,
            application_id="ocid1.dataflowapplication.test",
        )
    assert prompts == []


def test_auth_prompt_can_be_disabled(monkeypatch):
    def invalid(command, *, timeout_seconds=None):
        raise subprocess.CalledProcessError(
            1, command, stderr="status: 401 NotAuthenticated"
        )

    monkeypatch.setattr(P, "_run", invalid)
    adapter = P.ModuleAdapter(timeout_seconds=5)

    with pytest.raises(P.OciExecutionError, match="--no-auth-prompt"):
        adapter.ensure_auth(
            {"profile": "QAB", "auth": "security_token"},
            allow_prompt=False,
            prompt=lambda _message: pytest.fail("must not prompt"),
            application_id="ocid1.dataflowapplication.test",
        )


def test_failed_refresh_prompts_browser_authentication(monkeypatch, tmp_path):
    config_file = tmp_path / "oci-config"
    config_file.write_text("[QAB]\nregion=sa-saopaulo-1\n")
    calls = []
    prompts = []
    validations = 0
    interactive_calls = []

    def run(command, *, timeout_seconds=None, interactive=False):
        nonlocal validations
        calls.append(list(command))
        interactive_calls.append((list(command), interactive))
        operation = command[1:3]
        if command[1:4] == ["data-flow", "application", "get"]:
            validations += 1
            if validations == 1:
                raise subprocess.CalledProcessError(
                    1, command, stderr="status: 401 NotAuthenticated"
                )
        elif operation == ["session", "refresh"]:
            raise subprocess.CalledProcessError(1, command, stderr="refresh expired")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(P, "_run", run)
    adapter = P.ModuleAdapter(timeout_seconds=5)
    adapter.ensure_auth(
        {
            "profile": "QAB",
            "config_file": str(config_file),
            "auth": "security_token",
        },
        allow_prompt=True,
        prompt=lambda message: prompts.append(message) or True,
        application_id="ocid1.dataflowapplication.test",
    )

    authenticate = next(
        command for command in calls if command[1:3] == ["session", "authenticate"]
    )
    assert prompts == [
        "OCI security-token session is invalid. Refresh it now?",
        "OCI session refresh failed. Start browser authentication now?",
    ]
    assert authenticate[authenticate.index("--region") + 1] == "sa-saopaulo-1"
    assert authenticate[authenticate.index("--profile-name") + 1] == "QAB"
    assert next(
        interactive for command, interactive in interactive_calls
        if command[1:3] == ["session", "authenticate"]
    ) is True


def test_refresh_that_leaves_session_invalid_falls_back_to_browser(monkeypatch):
    validations = 0
    calls = []

    def run(command, *, timeout_seconds=None, interactive=False):
        nonlocal validations
        calls.append(list(command))
        if command[1:4] == ["data-flow", "application", "get"]:
            validations += 1
            if validations <= 2:
                raise subprocess.CalledProcessError(
                    1, command, stderr="status: 401 NotAuthenticated"
                )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(P, "_run", run)
    adapter = P.ModuleAdapter(timeout_seconds=5)
    adapter.ensure_auth(
        {
            "profile": "QAB",
            "region": "sa-saopaulo-1",
            "auth": "security_token",
        },
        allow_prompt=True,
        prompt=lambda _message: True,
        application_id="ocid1.dataflowapplication.test",
    )

    assert any(
        command[1:3] == ["session", "authenticate"] for command in calls
    )
    assert validations == 3


def test_dry_run_is_offline_and_prints_resolved_argv(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)

    result = P.main(
        run_args(tmp_path, config, upstream, "--dry-run", "--profile", "QAB"),
        adapter=NoCallsAdapter(),
    )

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["interval"] == {"from": "engorda", "to": "validate", "inclusive": True}
    assert output["run_root"].endswith("/qab/run-001")
    assert set(output["nodes"]) == {
        "cdb_simplificado.engorda.plan",
        "cdb_simplificado.engorda.reserve",
        "cdb_simplificado.engorda.materialize",
        "cdb_simplificado.validate",
    }
    plan_argv = output["nodes"]["cdb_simplificado.engorda.plan"]["arguments"]
    assert plan_argv[:4] == ["--phase", "plan", "--produto", "cdb_simplificado"]
    assert "--plan-uri" in plan_argv
    assert "--raw-uri" in plan_argv
    assert "--output-uri" in plan_argv
    assert plan_argv[plan_argv.index("--query-num-if-sql") + 1] == (
        "oci://source@namespace/queries_produtos.sql"
    )
    materialize_argv = output["nodes"][
        "cdb_simplificado.engorda.materialize"
    ]["arguments"]
    assert "--reservation-uri" in materialize_argv
    assert "--faltantes-parquet" in materialize_argv
    assert materialize_argv[materialize_argv.index("--query-num-if-sql") + 1] == (
        "oci://source@namespace/queries_produtos.sql"
    )
    validator_argv = output["nodes"]["cdb_simplificado.validate"]["arguments"]
    assert validator_argv[validator_argv.index("--product") + 1] == "cdb_simplificado"
    assert "--allow-partial" in validator_argv
    assert not (tmp_path / "local-runs").exists()


def test_load_dry_run_needs_no_approval_and_uses_exact_artifacts(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("engorda")] = "load"
    args[args.index("validate")] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 0

    plan = json.loads(capsys.readouterr().out)
    node = plan["nodes"]["cdb_simplificado.load"]
    assert plan["load_contract"]["approval_required"] is True
    assert plan["load_contract"]["approved"] is False
    assert node["input_uri"] == "oci://source@namespace/synthetic/cdb_simplificado"
    assert node["validation_report_uri"].endswith(
        "/validation/cdb_simplificado/report.json"
    )
    assert node["output_uri"].endswith(
        "/products/cdb_simplificado/load/manifest.json"
    )
    assert node["arguments"][node["arguments"].index("--run-id") + 1] == "run-001"
    assert "--skip-validation" in node["arguments"]
    assert "--continue-on-error" not in node["arguments"]


def test_no_oracle_propagates_to_both_engorda_phases_and_marks_artifact(
    tmp_path, capsys
):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--dry-run", "--no-oracle")

    assert P.main(args, adapter=NoCallsAdapter()) == 0

    plan = json.loads(capsys.readouterr().out)
    assert "--no-oracle" in plan["nodes"][
        "cdb_simplificado.engorda.plan"
    ]["arguments"]
    assert "--no-oracle" in plan["nodes"][
        "cdb_simplificado.engorda.materialize"
    ]["arguments"]
    assert "--no-oracle" in plan["nodes"][
        "cdb_simplificado.validate"
    ]["arguments"]
    synthetic = plan["artifacts"]["products"]["cdb_simplificado"]["synthetic"]
    assert synthetic["oracle_access"] == "disabled"
    assert synthetic["load_eligible"] is False
    report = plan["artifacts"]["products"]["cdb_simplificado"][
        "validation_report"
    ]
    assert report["oracle_access"] == "disabled"
    assert report["load_eligible"] is False


def test_validate_only_no_oracle_propagates_without_engorda(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--dry-run", "--no-oracle")
    args[args.index("--from") + 1] = "validate"
    args[args.index("--to") + 1] = "validate"

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    assert set(plan["nodes"]) == {"cdb_simplificado.validate"}
    assert "--no-oracle" in plan["nodes"]["cdb_simplificado.validate"][
        "arguments"
    ]


def test_global_no_oracle_cannot_be_disabled_by_product_config_or_set(tmp_path, capsys):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["products"]["cdb_simplificado"].update({
        "engorda": {"no_oracle": False},
        "validate": {"no_oracle": False},
    })
    config.write_text(json.dumps(payload))
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--set",
        "cdb_simplificado.engorda.no_oracle=false",
        "--set",
        "cdb_simplificado.validate.no_oracle=false",
        "--dry-run",
        "--no-oracle",
    )

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    assert "--no-oracle" in plan["nodes"][
        "cdb_simplificado.engorda.plan"
    ]["arguments"]
    assert "--no-oracle" in plan["nodes"]["cdb_simplificado.validate"][
        "arguments"
    ]


def test_validate_only_inherits_no_oracle_from_upstream_synthetic(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    payload = json.loads(upstream.read_text())
    payload["artifacts"]["products"]["cdb_simplificado"]["synthetic"].update({
        "oracle_access": "disabled",
        "load_eligible": False,
    })
    upstream.write_text(json.dumps(payload))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("--from") + 1] = "validate"
    args[args.index("--to") + 1] = "validate"

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    assert "--no-oracle" in plan["nodes"]["cdb_simplificado.validate"][
        "arguments"
    ]
    report = plan["artifacts"]["products"]["cdb_simplificado"][
        "validation_report"
    ]
    assert report["oracle_access"] == "disabled"
    assert report["load_eligible"] is False


def test_no_oracle_interval_cannot_include_load(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--dry-run", "--no-oracle")
    args[args.index("--to") + 1] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "no_oracle and is not eligible for load" in capsys.readouterr().err


def test_per_product_no_oracle_override_only_changes_target_branch(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(
        tmp_path, products=("cdb_simplificado", "lci")
    )
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci",
        "--set",
        "cdb_simplificado.engorda.no_oracle=true",
        "--dry-run",
    )

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    assert "--no-oracle" in plan["nodes"][
        "cdb_simplificado.engorda.plan"
    ]["arguments"]
    assert "--no-oracle" not in plan["nodes"]["lci.engorda.plan"]["arguments"]
    assert "--no-oracle" in plan["nodes"]["cdb_simplificado.validate"]["arguments"]
    assert "--no-oracle" not in plan["nodes"]["lci.validate"]["arguments"]
    assert plan["artifacts"]["products"]["cdb_simplificado"]["synthetic"][
        "load_eligible"
    ] is False
    assert plan["artifacts"]["products"]["lci"]["synthetic"][
        "load_eligible"
    ] is True


def test_mixed_product_load_rejects_one_offline_override(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(
        tmp_path, products=("cdb_simplificado", "lci")
    )
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci",
        "--set",
        "cdb_simplificado.engorda.no_oracle=true",
        "--dry-run",
    )
    args[args.index("--to") + 1] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "cdb_simplificado uses no_oracle" in capsys.readouterr().err


def test_validate_no_oracle_override_cannot_continue_to_load(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--set",
        "cdb_simplificado.validate.no_oracle=true",
        "--dry-run",
    )
    args[args.index("--from") + 1] = "validate"
    args[args.index("--to") + 1] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "uses no_oracle and is not eligible for load" in capsys.readouterr().err


def test_load_only_rejects_offline_upstream_metadata(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    payload = json.loads(upstream.read_text())
    payload["artifacts"]["products"]["cdb_simplificado"]["synthetic"].update({
        "oracle_access": "disabled",
        "load_eligible": False,
    })
    upstream.write_text(json.dumps(payload))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "offline and not eligible for load" in capsys.readouterr().err


def test_load_only_rejects_offline_validation_report_metadata(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    payload = json.loads(upstream.read_text())
    payload["artifacts"]["products"]["cdb_simplificado"][
        "validation_report"
    ].update({
        "oracle_access": "disabled",
        "load_eligible": False,
    })
    upstream.write_text(json.dumps(payload))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "validation report is offline" in capsys.readouterr().err


def test_reused_synthetic_descriptor_becomes_upstream_producer(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    payload = json.loads(upstream.read_text())
    payload["artifacts"]["products"]["cdb_simplificado"]["synthetic"][
        "producer"
    ] = "current_run"
    upstream.write_text(json.dumps(payload))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("--from") + 1] = "validate"

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["artifacts"]["products"]["cdb_simplificado"]["synthetic"][
        "producer"
    ] == "upstream"


def test_live_load_requires_explicit_approval_before_remote_calls(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream)
    args[args.index("engorda")] = "load"
    args[args.index("validate")] = "load"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "requires --approve-load" in capsys.readouterr().err
    assert not (tmp_path / "local-runs").exists()


def test_synthetic_output_uri_is_exact_across_plan_materialize_validator_and_artifact(
    tmp_path, capsys
):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado", "lci"))
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci",
        "--dry-run",
    )

    assert P.main(args, adapter=NoCallsAdapter()) == 0

    plan = json.loads(capsys.readouterr().out)
    output_uris = []
    for product in ("cdb_simplificado", "lci"):
        artifact_uri = plan["artifacts"]["products"][product]["synthetic"]["uri"]
        plan_node = plan["nodes"][f"{product}.engorda.plan"]
        materialize_node = plan["nodes"][f"{product}.engorda.materialize"]
        validator_node = plan["nodes"][f"{product}.validate"]
        output_uris.append(artifact_uri)

        assert plan_node["arguments"][
            plan_node["arguments"].index("--output-uri") + 1
        ] == artifact_uri
        assert materialize_node["arguments"][
            materialize_node["arguments"].index("--output-uri") + 1
        ] == artifact_uri
        assert materialize_node["output_uri"] == artifact_uri
        assert validator_node["input_uri"] == artifact_uri
        assert validator_node["arguments"][
            validator_node["arguments"].index("--input-base") + 1
        ] == artifact_uri

    assert len(set(output_uris)) == 2


@pytest.mark.parametrize(
    "product,validator",
    [
        ("cdb_simplificado", "cdb_simplificado"),
        ("cdb_resgate", "cdb"),
        ("cdb_escalonamento", "cdb"),
        ("rdb_inclusao", "rdb_inclusao"),
        ("rdb_resgate", "rdb_resgate"),
        ("lci", "lci"),
        ("lca", "lca"),
        ("ccb_pppre", "ccb"),
        ("ccb_pfpre", "ccb"),
        ("ccb_pgrpre", "ccb"),
        ("ccb_favcp", "ccb"),
        ("ccb_fapre", "ccb"),
        ("gravame", "gravame"),
        ("lastro", "credito_scr"),
        ("direito_creditorio", "dicre"),
    ],
)
def test_registry_maps_generator_products_to_validator_profiles(
    tmp_path, capsys, product, validator
):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=(product,))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("cdb_simplificado")] = product
    if "engorda" not in P.PRODUCTS[product]["capabilities"]:
        args[args.index("engorda")] = "validate"

    assert P.main(args, adapter=NoCallsAdapter()) == 0

    output = json.loads(capsys.readouterr().out)
    argv = output["nodes"][f"{product}.validate"]["arguments"]
    assert argv[argv.index("--product") + 1] == validator


def test_validate_only_product_rejects_engorda_interval(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("lastro",))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("cdb_simplificado")] = "lastro"

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    assert "lacks requested stage capability: engorda" in capsys.readouterr().err


def test_registry_exposes_every_engorda_generator_name():
    assert ALL_PRODUCTS == (
        *LEGACY_PRODUCTS,
        "ccb_pppre",
        "ccb_pfpre",
        "ccb_pgrpre",
        "ccb_favcp",
        "ccb_fapre",
        "gravame",
        "lastro",
        "direito_creditorio",
    )


def test_rejects_unsupported_product_and_non_tracer_interval(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)
    unsupported = run_args(tmp_path, config, upstream, "--dry-run")
    unsupported[unsupported.index("cdb_simplificado")] = "unknown_product"
    assert P.main(unsupported, adapter=NoCallsAdapter()) == 2
    assert "unsupported generator product" in capsys.readouterr().err

    interval = run_args(tmp_path, config, upstream, "--dry-run")
    interval[interval.index("engorda")] = "extract"
    assert P.main(interval, adapter=NoCallsAdapter()) == 2
    assert "engorda through load" in capsys.readouterr().err


def test_validated_set_applies_only_to_target_product_and_stage(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci",
        "--dry-run",
        "--set",
        "lci.engorda.n_instrumentos=12",
        "--set",
        'lci.validate.fail_severity="warn"',
    )

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    lci_plan = plan["nodes"]["lci.engorda.plan"]["arguments"]
    lci_validate = plan["nodes"]["lci.validate"]["arguments"]
    cdb_plan = plan["nodes"]["cdb_simplificado.engorda.plan"]["arguments"]
    assert lci_plan[lci_plan.index("--n-instrumentos") + 1] == "12"
    assert lci_validate[lci_validate.index("--fail-severity") + 1] == "warn"
    assert cdb_plan[cdb_plan.index("--n-instrumentos") + 1] == "4"

    bad = run_args(
        tmp_path,
        config,
        upstream,
        "--dry-run",
        "--set",
        "lci.engorda.unknown=1",
    )
    assert P.main(bad, adapter=NoCallsAdapter()) == 2
    assert "option is not allowed" in capsys.readouterr().err


def test_product_config_overrides_global_size_and_set_overrides_product_config(
    tmp_path, capsys
):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["products"]["lci"]["engorda"] = {
        "n_instrumentos": 50000,
        "fator_k": 2,
    }
    payload["products"]["lca"]["engorda"] = {
        "n_instrumentos": 50000,
        "fator_k": 2,
    }
    config.write_text(json.dumps(payload))
    upstream = write_upstream(tmp_path)
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci,lca",
        "--n-instrumentos",
        "100000",
        "--fator-k",
        "1",
        "--set",
        "lci.engorda.n_instrumentos=45000",
        "--dry-run",
    )

    assert P.main(args, adapter=NoCallsAdapter()) == 0
    plan = json.loads(capsys.readouterr().out)
    lci = plan["nodes"]["lci.engorda.plan"]["arguments"]
    lca = plan["nodes"]["lca.engorda.plan"]["arguments"]

    assert lci[lci.index("--n-instrumentos") + 1] == "45000"
    assert lci[lci.index("--fator-k") + 1] == "2"
    assert lca[lca.index("--n-instrumentos") + 1] == "50000"
    assert lca[lca.index("--fator-k") + 1] == "2"


def test_config_is_one_environment_and_rejects_unsupported_registry_entries(tmp_path):
    config = write_config(
        tmp_path,
        extra={"environments": ["qab"], "products": {"future_product": {"capabilities": []}}},
    )
    with pytest.raises(P.PipelineError, match="exactly one environment"):
        P.load_config(config)

    payload = json.loads(config.read_text())
    payload.pop("environments")
    config.write_text(json.dumps(payload))
    with pytest.raises(P.PipelineError, match="unsupported generator products"):
        P.load_config(config)


def test_legacy_product_subset_config_remains_valid(tmp_path):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["products"] = {
        product: payload["products"][product] for product in LEGACY_PRODUCTS
    }
    config.write_text(json.dumps(payload))

    loaded = P.load_config(config)

    assert tuple(loaded["products"]) == LEGACY_PRODUCTS


@pytest.mark.parametrize(
    "products,error",
    [
        ({}, "at least one"),
        ({"cdb_simplificado": {"capabilities": ["unknown"]}}, "unsupported capability"),
        (
            {"cdb_simplificado": {"capabilities": ["engorda", "engorda"]}},
            "contains duplicates",
        ),
    ],
)
def test_config_rejects_empty_or_invalid_product_capabilities(tmp_path, products, error):
    config = write_config(tmp_path, extra={"products": products})

    with pytest.raises(P.PipelineError, match=error):
        P.load_config(config)


@pytest.mark.parametrize(
    "engorda,error",
    [
        ([], "must be an object"),
        ({"unknown": 1}, "contains unsupported option"),
    ],
)
def test_config_rejects_invalid_product_stage_options(tmp_path, engorda, error):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["products"]["lci"]["engorda"] = engorda
    config.write_text(json.dumps(payload))

    with pytest.raises(P.PipelineError, match=error):
        P.load_config(config)


@pytest.mark.parametrize("command", ["run", "adopt-inputs"])
def test_selected_registry_product_must_be_enabled_in_config(
    tmp_path, capsys, command
):
    config = write_config(tmp_path)
    payload = json.loads(config.read_text())
    payload["products"] = {
        product: payload["products"][product] for product in LEGACY_PRODUCTS
    }
    config.write_text(json.dumps(payload))

    if command == "run":
        upstream = write_upstream(tmp_path, products=("gravame",))
        args = run_args(tmp_path, config, upstream, "--dry-run")
        args[args.index("cdb_simplificado")] = "gravame"
    else:
        args = [
            "adopt-inputs",
            "--config",
            str(config),
            "--product",
            "gravame",
            "--raw-uri",
            "oci://source@namespace/raw",
            "--faltantes-uri",
            "oci://source@namespace/faltantes",
            "--output-manifest",
            str(tmp_path / "adopted.json"),
            "--dry-run",
        ]

    assert P.main(args, adapter=NoCallsAdapter()) == 2
    error = capsys.readouterr().err
    assert "gravame" in error
    assert "not enabled in config.products" in error


def test_config_rejects_credentials_and_operator_auth(tmp_path):
    config = write_config(tmp_path, extra={"profile": "QAB"})
    with pytest.raises(P.PipelineError, match="authentication belongs on CLI flags"):
        P.load_config(config)

    payload = json.loads(config.read_text())
    payload.pop("profile")
    payload["api_token"] = "not-allowed"
    config.write_text(json.dumps(payload))
    with pytest.raises(P.PipelineError, match="must not contain credentials"):
        P.load_config(config)


def test_adopt_inputs_dry_run_is_offline_and_normal_mode_validates_uris(tmp_path, capsys):
    config = write_config(tmp_path)
    output = tmp_path / "adopted.json"
    argv = [
        "adopt-inputs",
        "--config",
        str(config),
        "--product",
        "cdb_simplificado",
        "--raw-uri",
        "oci://source@namespace/raw",
        "--faltantes-uri",
        "oci://source@namespace/faltantes",
        "--output-manifest",
        str(output),
        "--profile",
        "QAB",
    ]

    assert P.main([*argv, "--dry-run"], adapter=NoCallsAdapter()) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ADOPTED"
    assert not output.exists()

    adapter = FakeAdapter()
    assert P.main(argv, adapter=adapter) == 2
    assert "does not exist" in capsys.readouterr().err

    adapter.existing.update({"oci://source@namespace/raw", "oci://source@namespace/faltantes"})
    assert P.main(argv, adapter=adapter) == 0
    manifest = json.loads(output.read_text())
    assert manifest["artifacts"]["raw"]["producer"] == "external"
    assert manifest["artifacts"]["raw"]["object_count"] == 2
    assert manifest["artifacts"]["faltantes"]["total_bytes"] == 42
    assert all(call[2] == {"profile": "QAB"} for call in adapter.calls)


def test_adopt_validate_only_product_requires_and_records_synthetic_uri(
    tmp_path, capsys
):
    config = write_config(tmp_path)
    output = tmp_path / "lastro-inputs.json"
    argv = [
        "adopt-inputs",
        "--config", str(config),
        "--product", "lastro",
        "--raw-uri", "oci://source@namespace/raw",
        "--faltantes-uri", "oci://source@namespace/faltantes",
        "--output-manifest", str(output),
    ]

    assert P.main([*argv, "--dry-run"], adapter=NoCallsAdapter()) == 2
    assert "require --synthetic-uri" in capsys.readouterr().err

    synthetic = "oci://source@namespace/lastro-output"
    argv += ["--synthetic-uri", f"lastro={synthetic}"]
    assert P.main([*argv, "--dry-run"], adapter=NoCallsAdapter()) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["artifacts"]["products"]["lastro"]["synthetic"]["uri"] == synthetic

    adapter = FakeAdapter(existing=(
        "oci://source@namespace/raw",
        "oci://source@namespace/faltantes",
        synthetic,
    ))
    assert P.main(argv, adapter=adapter) == 0
    adopted = json.loads(output.read_text())
    assert adopted["artifacts"]["products"]["lastro"]["synthetic"][
        "producer"
    ] == "external"


def test_dependency_execution_is_concurrent_and_isolates_failed_branch(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci,rdb_resgate",
        "--max-concurrency",
        "2",
    )
    adapter = FakeAdapter(failed_prefixes=("cdb_simplificado-engorda-plan",))

    assert P.main(args, adapter=adapter) == 1

    manifest = read_run_manifest(tmp_path)
    assert manifest["status"] == "FAILED"
    assert manifest["nodes"]["cdb_simplificado.engorda.plan"]["state"] == "FAILED"
    assert manifest["nodes"]["cdb_simplificado.engorda.reserve"]["state"] == "BLOCKED"
    assert all(
        node["state"] == "SUCCEEDED"
        for node_id, node in manifest["nodes"].items()
        if node_id.startswith(("lci.", "rdb_resgate."))
    )
    assert adapter.max_active == 2
    assert len(adapter.reservations) == 2
    assert adapter.max_active_reservations == 1
    assert adapter.uploads[0][0]["status"] == "FAILED"


def test_loads_are_serial_in_product_order_and_failure_does_not_block_next(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(
        tmp_path, products=("cdb_simplificado", "lci")
    )
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--product",
        "lci",
        "--approve-load",
    )
    args[args.index("engorda")] = "load"
    args[args.index("validate")] = "load"
    args[args.index("0", args.index("--max-retries"))] = "3"
    adapter = FakeAdapter(failed_prefixes=("cdb_simplificado-load",))

    assert P.main(args, adapter=adapter) == 1

    load_calls = [
        call for call in adapter.created.values() if "--manifest-uri" in call["arguments"]
    ]
    assert [call["arguments"][1] for call in load_calls] == ["cdb_simplificado", "lci"]
    assert len(load_calls) == 2
    assert adapter.max_active_loads == 1
    assert adapter.load_lease_acquisitions == 1
    assert adapter.load_lease_releases == 1
    manifest = read_run_manifest(tmp_path)
    assert manifest["nodes"]["cdb_simplificado.load"]["state"] == "FAILED"
    assert manifest["nodes"]["lci.load"]["state"] == "SUCCEEDED"


def test_load_claim_blocks_unmarked_second_attempt(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    adapter = FakeAdapter()

    first = run_args(tmp_path, config, upstream, "--approve-load")
    first[first.index("engorda")] = "load"
    first[first.index("validate")] = "load"
    assert P.main(first, adapter=adapter) == 0

    second = run_args(tmp_path, config, upstream, "--approve-load")
    second[second.index("engorda")] = "load"
    second[second.index("validate")] = "load"
    second[second.index("run-001")] = "run-002"
    assert P.main(second, adapter=adapter) == 1

    load_calls = [
        call for call in adapter.created.values() if "--manifest-uri" in call["arguments"]
    ]
    assert len(load_calls) == 1
    assert "already exists" in read_run_manifest(tmp_path, run_id="run-002")["nodes"][
        "cdb_simplificado.load"
    ]["error"]


def test_resume_rejects_a_load_known_to_have_succeeded(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    adapter = FakeAdapter()

    first = run_args(tmp_path, config, upstream, "--approve-load")
    first[first.index("engorda")] = "load"
    first[first.index("validate")] = "load"
    assert P.main(first, adapter=adapter) == 0
    prior = (
        "oci://bucket@namespace/runs/qab/run-001/products/"
        "cdb_simplificado/load/manifest.json"
    )

    second = run_args(
        tmp_path,
        config,
        upstream,
        "--approve-load",
        "--resume-load-manifest",
        f"cdb_simplificado={prior}",
    )
    second[second.index("engorda")] = "load"
    second[second.index("validate")] = "load"
    second[second.index("run-001")] = "run-002"

    assert P.main(second, adapter=adapter) == 1
    error = read_run_manifest(tmp_path, "run-002")["nodes"][
        "cdb_simplificado.load"
    ]["error"]
    assert "known to have succeeded" in error


def test_failed_current_validation_blocks_dependent_load(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--approve-load")
    args[args.index("--from") + 1] = "validate"
    args[args.index("--to") + 1] = "load"
    adapter = FakeAdapter(
        reports={"cdb_simplificado": {"verdict": "FAIL", "counts": {"error": 1}}}
    )

    assert P.main(args, adapter=adapter) == 1
    manifest = read_run_manifest(tmp_path)
    assert manifest["nodes"]["cdb_simplificado.validate"]["state"] == "FAILED"
    assert manifest["nodes"]["cdb_simplificado.load"]["state"] == "BLOCKED"
    assert not [
        call for call in adapter.created.values() if "--manifest-uri" in call["arguments"]
    ]


def test_load_rejects_noncanonical_report_before_creating_claim(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--approve-load")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"
    adapter = FakeAdapter(
        reports={
            "cdb_simplificado": {
                "verdict": "pass",
                "counts": {"error": 0},
                "table_inventory": ["A", "a"],
            }
        }
    )

    assert P.main(args, adapter=adapter) == 1
    assert not [
        payload for payload in adapter.objects.values()
        if payload.get("kind") == "load-claim"
    ]
    assert not adapter.created


def test_load_rejects_no_oracle_report_before_creating_claim(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--approve-load")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"
    adapter = FakeAdapter(reports={"cdb_simplificado": {
        "verdict": "PARTIAL",
        "counts": {"error": 0},
        "oracle_access": "disabled",
        "load_eligible": False,
    }})

    assert P.main(args, adapter=adapter) == 1
    assert not [
        payload for payload in adapter.objects.values()
        if payload.get("kind") == "load-claim"
    ]
    assert not adapter.created


def test_load_detects_offline_marker_before_creating_claim(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--approve-load")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"
    marker_uri = (
        "oci://source@namespace/synthetic/cdb_simplificado/"
        f"{P.OFFLINE_ARTIFACT_MARKER}"
    )
    adapter = FakeAdapter(objects={marker_uri: {
        "artifact_type": "datagen_offline_synthetic",
        "load_eligible": False,
    }})

    assert P.main(args, adapter=adapter) == 1
    assert not [
        payload for payload in adapter.objects.values()
        if payload.get("kind") == "load-claim"
    ]
    assert not adapter.created


def test_ambiguous_submit_failure_retains_load_claim(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream, "--approve-load")
    args[args.index("--from") + 1] = "load"
    args[args.index("--to") + 1] = "load"

    class SubmitFailureAdapter(FakeAdapter):
        def create_run(self, arguments, display_name, opts):
            raise P.OciExecutionError("submit failed")

    adapter = SubmitFailureAdapter()
    assert P.main(args, adapter=adapter) == 1
    assert [
        payload for payload in adapter.objects.values()
        if payload.get("kind") == "load-claim"
    ]
    assert adapter.load_lease_releases == 0
    assert adapter.load_lease_quarantines


@pytest.mark.parametrize(
    "report,expected_status,expected_exit",
    [
        ({"verdict": "PARTIAL", "counts": {"error": 0}}, "SUCCEEDED", 0),
        ({"verdict": "PASS", "counts": {"error": 1}}, "FAILED", 1),
        ({"verdict": "FAIL", "counts": {"error": 0}}, "FAILED", 1),
    ],
)
def test_validation_report_gate_accepts_only_zero_error_pass_or_partial(
    tmp_path, report, expected_status, expected_exit
):
    case = tmp_path / expected_status / report["verdict"]
    case.mkdir(parents=True)
    config = write_config(case)
    upstream = write_upstream(case, products=("cdb_simplificado",))
    args = run_args(case, config, upstream)
    args[args.index("engorda")] = "validate"
    adapter = FakeAdapter(reports={"cdb_simplificado": report})

    assert P.main(args, adapter=adapter) == expected_exit

    manifest = read_run_manifest(case)
    node = manifest["nodes"]["cdb_simplificado.validate"]
    assert manifest["status"] == expected_status
    assert node["validation"] == {
        "accepted": expected_exit == 0,
        "error_count": report["counts"]["error"],
        "input_matches": True,
        "product_matches": True,
        "verdict": report["verdict"],
    }


def test_validation_report_must_match_product_and_exact_input(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_resgate",))
    args = run_args(tmp_path, config, upstream)
    args[args.index("cdb_simplificado")] = "cdb_resgate"
    args[args.index("engorda")] = "validate"
    adapter = FakeAdapter(reports={
        "cdb_resgate": {
            "verdict": "PASS",
            "counts": {"error": 0},
            "product": "rdb",
            "resolved_input": "oci://wrong@namespace/output",
        }
    })

    assert P.main(args, adapter=adapter) == 1
    validation = read_run_manifest(tmp_path)["nodes"]["cdb_resgate.validate"][
        "validation"
    ]
    assert validation["accepted"] is False
    assert validation["product_matches"] is False
    assert validation["input_matches"] is False


def test_auth_flags_flow_to_create_poll_reservation_and_upload(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)
    adapter = FakeAdapter()
    args = run_args(
        tmp_path,
        config,
        upstream,
        "--profile",
        "QAB",
        "--config-file",
        "C:\\oci\\config",
        "--auth",
        "security_token",
        "--cert-bundle",
        "C:\\certs\\corp.pem",
    )

    assert P.main(args, adapter=adapter) == 0

    expected = {
        "profile": "QAB",
        "config_file": "C:\\oci\\config",
        "auth": "security_token",
        "cert_bundle": "C:\\certs\\corp.pem",
    }
    assert all(
        {key: call["opts"][key] for key in expected} == expected
        for call in adapter.created.values()
    )
    assert adapter.reservations[0]["auth"] == expected
    assert adapter.uploads[0][2] == expected


def test_existing_local_or_remote_run_path_is_rejected(tmp_path, capsys):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)
    local = tmp_path / "local-runs" / "qab" / "run-001"
    local.mkdir(parents=True)
    assert P.main(run_args(tmp_path, config, upstream), adapter=FakeAdapter()) == 2
    assert "immutable local run path already exists" in capsys.readouterr().err

    local.rmdir()
    remote = "oci://bucket@namespace/runs/qab/run-001"
    assert P.main(
        run_args(tmp_path, config, upstream), adapter=FakeAdapter(existing=(remote,))
    ) == 2
    assert "immutable OCI run path already exists" in capsys.readouterr().err


def test_existing_materialize_output_fails_before_manifest_submit_or_reserve(
    tmp_path, capsys
):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    synthetic = (
        "oci://bucket@namespace/runs/qab/run-001/products/"
        "cdb_simplificado/synthetic"
    )
    adapter = FakeAdapter(existing=(synthetic,))

    assert P.main(run_args(tmp_path, config, upstream), adapter=adapter) == 2

    assert "immutable OCI materialize output" in capsys.readouterr().err
    assert adapter.created == {}
    assert adapter.reservations == []
    assert not (tmp_path / "local-runs" / "qab" / "run-001" / "manifest.json").exists()


def test_validate_only_skips_materialize_output_preflight(tmp_path):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=("cdb_simplificado",))
    args = run_args(tmp_path, config, upstream)
    args[args.index("engorda")] = "validate"
    adapter = FakeAdapter()

    assert P.main(args, adapter=adapter) == 0

    checked_uris = [call[1] for call in adapter.calls if call[0] == "uri_exists"]
    assert checked_uris == [
        "oci://bucket@namespace/runs/qab/run-001",
        "oci://bucket@namespace/manifests/qab/run-001/manifest.json",
    ]


def test_keyboard_interrupt_cancels_active_runs_and_records_manifest(tmp_path, monkeypatch):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path)

    class BlockingAdapter(FakeAdapter):
        def __init__(self):
            super().__init__()
            self.release = threading.Event()

        def get_run_state(self, run_id, opts):
            self.release.wait(1)
            return "CANCELED"

        def cancel_run(self, run_id, opts):
            super().cancel_run(run_id, opts)
            self.release.set()

    adapter = BlockingAdapter()

    def interrupt_wait(*args, **kwargs):
        deadline = time.time() + 1
        while not adapter.created and time.time() < deadline:
            time.sleep(0.001)
        raise KeyboardInterrupt

    monkeypatch.setattr(P, "wait", interrupt_wait)
    assert P.main(run_args(tmp_path, config, upstream), adapter=adapter) == 130

    manifest = read_run_manifest(tmp_path)
    assert manifest["status"] == "CANCELLED"
    assert adapter.cancelled
    assert all(node["state"] in P.NODE_TERMINAL for node in manifest["nodes"].values())
    assert adapter.uploads[0][0]["status"] == "CANCELLED"
