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


def write_config(tmp_path, *, capabilities=None, extra=None):
    products = {
        product: {"capabilities": ["engorda", "validate"]}
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
        },
        "reservations": {
            "lease_uri": "oci://bucket@namespace/control/lease.json",
            "ledger_uri": "oci://bucket@namespace/control/ledger.json",
        },
        "products": products,
        "stage_defaults": {
            "engorda": {"n_instrumentos": 4, "fator_k": 2, "seed": 7},
            "validate": {"fail_severity": "error", "validate_against": "union"},
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
                }
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
    def __init__(self, *, failed_prefixes=(), reports=None, existing=()):
        self.failed_prefixes = tuple(failed_prefixes)
        self.reports = reports or {}
        self.existing = set(existing)
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

    def uri_exists(self, uri, *, auth):
        self.calls.append(("uri_exists", uri, auth))
        return uri in self.existing

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
        return {"data": {"id": run_id}}

    def get_run_state(self, run_id, opts):
        time.sleep(0.005)
        with self._lock:
            self._active.discard(run_id)
        display_name = self.created[run_id]["display_name"]
        return "FAILED" if display_name.startswith(self.failed_prefixes) else "SUCCEEDED"

    def read_json(self, uri, *, auth):
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
            call
            for call in self.created.values()
            if "--report-path" in call["arguments"]
            and call["arguments"][call["arguments"].index("--report-path") + 1] == uri
        )
        arguments = validation_call["arguments"]
        payload.setdefault("product", P.PRODUCTS[generator_product]["validator_product"])
        payload.setdefault(
            "resolved_input", arguments[arguments.index("--input-base") + 1]
        )
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
        self.uploads.append((json.loads(Path(path).read_text()), uri, auth))


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


def read_run_manifest(tmp_path):
    return json.loads((tmp_path / "local-runs" / "qab" / "run-001" / "manifest.json").read_text())


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


def test_invalid_security_token_prompts_refresh_before_preflight(monkeypatch):
    calls = []
    prompts = []

    def run(command, *, timeout_seconds=None):
        calls.append(list(command))
        if command[1:3] == ["session", "validate"] and len(calls) == 1:
            raise subprocess.CalledProcessError(1, command, stderr="session expired")
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
    )

    assert [command[1:3] for command in calls] == [
        ["session", "validate"],
        ["session", "refresh"],
        ["session", "validate"],
    ]
    assert prompts == ["OCI security-token session is invalid. Refresh it now?"]
    assert progress.messages[-1] == "[auth] OCI security-token session is valid"


def test_auth_prompt_can_be_disabled(monkeypatch):
    def invalid(command, *, timeout_seconds=None):
        raise subprocess.CalledProcessError(1, command, stderr="session expired")

    monkeypatch.setattr(P, "_run", invalid)
    adapter = P.ModuleAdapter(timeout_seconds=5)

    with pytest.raises(P.OciExecutionError, match="--no-auth-prompt"):
        adapter.ensure_auth(
            {"profile": "QAB", "auth": "security_token"},
            allow_prompt=False,
            prompt=lambda _message: pytest.fail("must not prompt"),
        )


def test_failed_refresh_prompts_browser_authentication(monkeypatch, tmp_path):
    config_file = tmp_path / "oci-config"
    config_file.write_text("[QAB]\nregion=sa-saopaulo-1\n")
    calls = []
    prompts = []
    validations = 0

    def run(command, *, timeout_seconds=None):
        nonlocal validations
        calls.append(list(command))
        operation = command[1:3]
        if operation == ["session", "validate"]:
            validations += 1
            if validations == 1:
                raise subprocess.CalledProcessError(1, command, stderr="expired")
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


def test_refresh_that_leaves_session_invalid_falls_back_to_browser(monkeypatch):
    validations = 0
    calls = []

    def run(command, *, timeout_seconds=None):
        nonlocal validations
        calls.append(list(command))
        if command[1:3] == ["session", "validate"]:
            validations += 1
            if validations <= 2:
                raise subprocess.CalledProcessError(1, command, stderr="still expired")
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
    materialize_argv = output["nodes"][
        "cdb_simplificado.engorda.materialize"
    ]["arguments"]
    assert "--reservation-uri" in materialize_argv
    assert "--faltantes-parquet" in materialize_argv
    validator_argv = output["nodes"]["cdb_simplificado.validate"]["arguments"]
    assert validator_argv[validator_argv.index("--product") + 1] == "cdb_simplificado"
    assert "--allow-partial" in validator_argv
    assert not (tmp_path / "local-runs").exists()


@pytest.mark.parametrize(
    "product,validator",
    [
        ("cdb_resgate", "cdb"),
        ("cdb_escalonamento", "cdb"),
        ("rdb_inclusao", "rdb"),
        ("rdb_resgate", "rdb"),
        ("lci", "lci"),
        ("lca", "lca"),
    ],
)
def test_registry_maps_generator_products_to_validator_profiles(
    tmp_path, capsys, product, validator
):
    config = write_config(tmp_path)
    upstream = write_upstream(tmp_path, products=(product,))
    args = run_args(tmp_path, config, upstream, "--dry-run")
    args[args.index("cdb_simplificado")] = product

    assert P.main(args, adapter=NoCallsAdapter()) == 0

    output = json.loads(capsys.readouterr().out)
    argv = output["nodes"][f"{product}.validate"]["arguments"]
    assert argv[argv.index("--product") + 1] == validator


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
    assert "first tracer supports only" in capsys.readouterr().err


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
