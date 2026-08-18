import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import oci_dataflow as O  # noqa: E402


def opts(**overrides):
    values = {
        "application_id": "ocid1.dataflowapplication.test",
        "compartment_id": "ocid1.compartment.test",
        "profile": "DEV",
        "config_file": None,
        "auth": "security_token",
        "cert_bundle": None,
    }
    values.update(overrides)
    return values


class TestCommandBuilders:
    def test_auth_flags_emit_only_configured_values(self):
        assert O.oci_auth_flags(opts(config_file="C:/oci/config", cert_bundle=None)) == [
            "--profile", "DEV", "--config-file", "C:/oci/config", "--auth",
            "security_token"]

    def test_create_serializes_arguments_and_runtime_overrides(self):
        command = O.build_run_create_command(
            ["--product", "cdb"],
            "pipeline-engorda-cdb-1",
            opts(num_executors=3, executor_shape="VM.Standard.E4.Flex"),
        )

        assert command[:4] == ["oci", "data-flow", "run", "create"]
        assert json.loads(command[command.index("--arguments") + 1]) == ["--product", "cdb"]
        assert command[command.index("--display-name") + 1] == "pipeline-engorda-cdb-1"
        assert command[command.index("--num-executors") + 1] == "3"
        assert command[command.index("--executor-shape") + 1] == "VM.Standard.E4.Flex"
        assert command[-4:] == ["--profile", "DEV", "--auth", "security_token"]

    def test_get_and_cancel_include_run_id_and_auth(self):
        assert O.build_run_get_command("run-1", opts(profile="P", auth=None)) == [
            "oci", "data-flow", "run", "get", "--run-id", "run-1", "--profile", "P"]
        assert O.build_run_cancel_command("run-1", opts(profile="P", auth=None)) == [
            "oci", "data-flow", "run", "cancel", "--run-id", "run-1", "--profile", "P"]


class TestJsonTransport:
    def test_uses_argv_without_shell(self, monkeypatch):
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(command, 0, stdout='{"data": {"id": "run-1"}}')

        monkeypatch.setattr(O.subprocess, "run", fake_run)

        assert O.run_json(["oci", "data-flow", "run", "get"]) == {
            "data": {"id": "run-1"}}
        assert calls == [
            (["oci", "data-flow", "run", "get"], {
                "capture_output": True, "text": True, "check": True})]

    def test_refreshes_security_token_once_then_retries(self, monkeypatch):
        command = O.build_run_get_command(
            "run-1", opts(config_file="C:/Users/me/.oci/config", cert_bundle="C:/corp/ca.pem"))
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            if len(calls) == 1:
                raise subprocess.CalledProcessError(
                    1, argv, stderr="ServiceError: 401 NotAuthenticated")
            if argv[1:3] == ["session", "refresh"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            return subprocess.CompletedProcess(argv, 0, stdout='{"data": {"id": "run-1"}}')

        monkeypatch.setattr(O.subprocess, "run", fake_run)

        assert O.run_json(command)["data"]["id"] == "run-1"
        assert calls == [
            command,
            ["oci", "session", "refresh", "--profile", "DEV", "--config-file",
             "C:/Users/me/.oci/config", "--cert-bundle", "C:/corp/ca.pem"],
            command,
        ]

    def test_does_not_refresh_other_auth_modes(self, monkeypatch):
        command = O.build_run_get_command("run-1", opts(auth="api_key"))

        def fail(argv, **kwargs):
            raise subprocess.CalledProcessError(1, argv, stderr="401 NotAuthenticated")

        monkeypatch.setattr(O.subprocess, "run", fail)

        with pytest.raises(subprocess.CalledProcessError):
            O.run_json(command)

    def test_does_not_retry_twice(self, monkeypatch):
        command = O.build_run_get_command("run-1", opts())
        calls = []

        def fail_requests(argv, **kwargs):
            calls.append(argv)
            if argv[1:3] == ["session", "refresh"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            raise subprocess.CalledProcessError(1, argv, stderr="401 NotAuthenticated")

        monkeypatch.setattr(O.subprocess, "run", fail_requests)

        with pytest.raises(subprocess.CalledProcessError):
            O.run_json(command)
        assert calls == [command, ["oci", "session", "refresh", "--profile", "DEV"], command]


class TestRunOperations:
    def test_create_get_and_cancel_decode_data(self, monkeypatch):
        responses = iter([
            {"data": {"id": "run-1"}},
            {"data": {"lifecycle-state": "IN_PROGRESS"}},
            {"data": {"lifecycle-state": "CANCELING"}},
        ])
        monkeypatch.setattr(O, "run_json", lambda command: next(responses))

        assert O.create_run(["--product", "cdb"], "name", opts()) == "run-1"
        assert O.get_run_state("run-1", opts()) == "IN_PROGRESS"
        assert O.cancel_run("run-1", opts()) == "CANCELING"


@pytest.mark.parametrize("state, kind", [
    ("SUCCEEDED", "success"),
    ("FAILED", "failure"),
    ("CANCELED", "failure"),
    ("STOPPED", "failure"),
    ("IN_PROGRESS", "pending"),
    ("unknown", "pending"),
])
def test_classify_state(state, kind):
    assert O.classify_state(state) == kind
