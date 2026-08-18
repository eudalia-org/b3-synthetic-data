#!/usr/bin/env python3
"""Local OCI Data Flow pipeline orchestrator for the first tracer slice.

The module deliberately does not import Spark jobs or ``oci_dataflow`` at import
time. Tests and callers can inject the small adapter contract accepted by
``main``; live execution loads ``scripts/oci_dataflow.py`` lazily.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import secrets
import sys
import tempfile
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Sequence

MANIFEST_VERSION = 1
CONFIG_VERSION = 1
PUBLIC_STAGES = ("extract", "faltantes", "engorda", "validate", "load", "verify")
TRACER_STAGES = ("engorda", "validate")
OVERRIDE_KEYS = {
    "engorda": {
        "n_instrumentos", "fator_k", "seed", "specs", "meu_numero_prefix",
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

# Generator product -> validator profile. This registry stays lightweight so the
# local CLI never imports the Spark-heavy generator or validator modules.
PRODUCTS: dict[str, dict[str, Any]] = {
    "cdb_simplificado": {"validator_product": "cdb_simplificado"},
    "cdb_resgate": {"validator_product": "cdb"},
    "cdb_escalonamento": {"validator_product": "cdb"},
    "rdb_inclusao": {"validator_product": "rdb"},
    "rdb_resgate": {"validator_product": "rdb"},
    "lci": {"validator_product": "lci"},
    "lca": {"validator_product": "lca"},
}


class PipelineError(ValueError):
    """Operator-correctable planning or execution error."""


@dataclass(frozen=True)
class AuthOptions:
    profile: str | None = None
    config_file: str | None = None
    auth: str | None = None
    cert_bundle: str | None = None

    def as_dict(self) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "profile": self.profile,
                "config_file": self.config_file,
                "auth": self.auth,
                "cert_bundle": self.cert_bundle,
            }.items()
            if value is not None
        }


@dataclass
class NodeResult:
    state: str
    attempts: list[dict[str, Any]]
    detail: dict[str, Any]


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
            os.replace(temporary, self.path)
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
    """Thin wrapper around the future shared ``oci_dataflow`` module.

    Data Flow create/get/cancel are the required shared operations. Object URI,
    manifest, report, and reservation operations intentionally remain explicit
    adapter contracts until their shared implementations land.
    """

    def __init__(self) -> None:
        try:
            self.module = importlib.import_module("oci_dataflow")
        except ModuleNotFoundError as exc:
            raise PipelineError(
                "live execution requires scripts/oci_dataflow.py; use --dry-run or inject "
                "an adapter"
            ) from exc

    def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        operation = getattr(self.module, name, None)
        if operation is None:
            raise PipelineError(f"oci_dataflow.{name} is required for this operation")
        return operation(*args, **kwargs)

    def create_run(
        self, arguments: Sequence[str], display_name: str, opts: dict[str, Any]
    ) -> Any:
        return self._call("create_run", arguments, display_name, opts)

    def get_run_state(self, run_id: str, opts: dict[str, Any]) -> Any:
        return self._call("get_run_state", run_id, opts)

    def cancel_run(self, run_id: str, opts: dict[str, Any]) -> Any:
        return self._call("cancel_run", run_id, opts)

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
            *self.module.oci_auth_flags(auth),
        ]

    def uri_exists(self, uri: str, *, auth: dict[str, str]) -> bool:
        command = self._object_command("list", uri, auth)
        command.append("--all")
        response = self.module.run_json(command)
        return bool(response.get("data"))

    def describe_uri(self, uri: str, *, auth: dict[str, str]) -> dict[str, Any]:
        command = self._object_command("list", uri, auth)
        command.append("--all")
        response = self.module.run_json(command)
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
        try:
            command = self._object_command("get", uri, auth)
            command += ["--file", temporary, "--force"]
            self.module.run_json(command)
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
        return self.module.run_json(command)

    def reserve_ranges(self, **kwargs: Any) -> Any:
        try:
            reservations = importlib.import_module("pipeline_reservations")
        except ModuleNotFoundError as exc:
            raise PipelineError(
                "live engorda requires pipeline_reservations.reserve_ranges; the ledger "
                "implementation is outside this orchestrator"
            ) from exc
        return reservations.reserve_ranges(**kwargs)


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
    if not isinstance(configured_products, dict):
        raise PipelineError("config.products must declare the environment product capabilities")
    unsupported = sorted(set(configured_products) - set(PRODUCTS))
    if unsupported:
        raise PipelineError(f"unsupported generator products in config: {', '.join(unsupported)}")
    missing = sorted(set(PRODUCTS) - set(configured_products))
    if missing:
        raise PipelineError(f"config.products is missing registry products: {', '.join(missing)}")
    for product, settings in configured_products.items():
        if not isinstance(settings, dict) or not isinstance(settings.get("capabilities"), list):
            raise PipelineError(f"config.products.{product}.capabilities must be a list")
        capabilities = settings["capabilities"]
        if any(stage not in TRACER_STAGES for stage in capabilities):
            raise PipelineError(f"config.products.{product} has an unsupported capability")
        if len(capabilities) != len(set(capabilities)):
            raise PipelineError(f"config.products.{product}.capabilities contains duplicates")

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
    for key, flag in (("specs", "--specs"), ("meu_numero_prefix", "--meu-numero-prefix")):
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
    config: dict[str, Any], args: argparse.Namespace, upstream: dict[str, Any]
) -> dict[str, Any]:
    products = parse_products(args.product)
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
        response = adapter.create_run(
            list(node["arguments"]),
            display_name,
            data_flow_options,
        )
        data_flow_run_id = _run_id_from_response(response)
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
                    return NodeResult("FAILED", attempts, detail)
            describe = getattr(adapter, "describe_uri", None)
            if describe is not None and node.get("output_uri"):
                detail["output_metadata"] = describe(
                    node["output_uri"], auth=auth
                )
            return NodeResult("SUCCEEDED", attempts, detail)
        if stop.is_set():
            return NodeResult("CANCELLED", attempts, {})
    return NodeResult("FAILED", attempts, {})


def _execute_reservation_node(
    node: dict[str, Any], plan: dict[str, Any], adapter: Any, auth: dict[str, str]
) -> NodeResult:
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
) -> int:
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
                    future = executor.submit(_execute_reservation_node, node, plan, adapter, auth)
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


def _auth_from_args(args: argparse.Namespace) -> dict[str, str]:
    return AuthOptions(args.profile, args.config_file, args.auth, args.cert_bundle).as_dict()


def run_command(args: argparse.Namespace, adapter: Any | None) -> int:
    config = load_config(args.config)
    upstream = read_upstream_manifest(args.upstream_manifest, config["environment"])
    plan = build_pipeline_plan(config, args, upstream)
    local_run_dir = Path(args.local_run_root) / config["environment"] / args.run_id
    manifest_path = local_run_dir / "manifest.json"
    if local_run_dir.exists():
        raise PipelineError(f"immutable local run path already exists: {local_run_dir}")
    if args.dry_run:
        print(json.dumps({"dry_run": True, **plan}, indent=2, sort_keys=True))
        return 0

    adapter = adapter or ModuleAdapter()
    auth = _auth_from_args(args)
    for label, uri in (("run path", plan["run_root"]), ("manifest", plan["manifest_uri"])):
        if adapter.uri_exists(uri, auth=auth):
            raise PipelineError(f"immutable OCI {label} already exists: {uri}")
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
        return 1
    return result


def adopt_inputs_command(args: argparse.Namespace, adapter: Any | None) -> int:
    config = load_config(args.config)
    products = parse_products(args.product)
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
        },
    }
    if args.dry_run:
        print(json.dumps({"dry_run": True, "output_manifest": str(output), **payload}, indent=2))
        return 0
    adapter = adapter or ModuleAdapter()
    auth = _auth_from_args(args)
    for name, uri in (("raw", args.raw_uri), ("faltantes", args.faltantes_uri)):
        describe = getattr(adapter, "describe_uri", None)
        if describe is not None:
            payload["artifacts"][name].update(describe(uri, auth=auth))
        elif not adapter.uri_exists(uri, auth=auth):
            raise PipelineError(f"OCI input URI does not exist: {uri}")
    atomic_write_json(output, payload)
    print(str(output))
    return 0


def _add_auth_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", help="OCI CLI profile name")
    parser.add_argument("--config-file", help="OCI CLI config file path")
    parser.add_argument("--auth", help="OCI auth mode, for example security_token")
    parser.add_argument("--cert-bundle", help="CA certificate bundle path")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="plan or execute the first tracer pipeline")
    run.add_argument("--config", required=True)
    run.add_argument("--product", action="append", required=True, help="repeat or use commas")
    run.add_argument("--from", dest="from_stage", required=True, choices=PUBLIC_STAGES)
    run.add_argument("--to", dest="to_stage", required=True, choices=PUBLIC_STAGES)
    run.add_argument("--upstream-manifest", required=True)
    run.add_argument("--run-id", default=None)
    run.add_argument("--local-run-root", default=".pipeline-runs")
    run.add_argument("--max-concurrency", type=int, default=4)
    run.add_argument("--max-retries", type=int, default=1)
    run.add_argument("--poll-seconds", type=float, default=30)
    run.add_argument("--num-executors", type=int)
    run.add_argument("--driver-shape")
    run.add_argument("--executor-shape")
    run.add_argument("--driver-shape-config")
    run.add_argument("--executor-shape-config")
    run.add_argument("--n-instrumentos", type=int)
    run.add_argument("--fator-k", type=int)
    run.add_argument("--seed", type=int)
    run.add_argument(
        "--set", dest="set_values", action="append", default=[],
        help="validated product.stage.key=value override",
    )
    run.add_argument("--dry-run", action="store_true")
    _add_auth_flags(run)

    adopt = commands.add_parser("adopt-inputs", help="register existing RAW/faltantes lineage")
    adopt.add_argument("--config", required=True)
    adopt.add_argument("--product", action="append", required=True, help="repeat or use commas")
    adopt.add_argument("--raw-uri", required=True)
    adopt.add_argument("--faltantes-uri", required=True)
    adopt.add_argument("--output-manifest", required=True)
    adopt.add_argument("--dry-run", action="store_true")
    _add_auth_flags(adopt)
    return parser


def main(argv: Sequence[str] | None = None, *, adapter: Any | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        args.run_id = args.run_id or default_run_id()
        if args.max_concurrency < 1:
            parser.error("--max-concurrency must be >= 1")
        if args.max_retries < 0:
            parser.error("--max-retries must be >= 0")
        if args.poll_seconds < 0:
            parser.error("--poll-seconds must be >= 0")
        if args.num_executors is not None and args.num_executors < 1:
            parser.error("--num-executors must be >= 1")
    try:
        if args.command == "run":
            return run_command(args, adapter)
        return adopt_inputs_command(args, adapter)
    except PipelineError as exc:
        print(f"run_pipeline: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
