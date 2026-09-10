import json
import os
import sys
import threading
import time
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from datagen import engorda_tables as E
from scripts import run_pipeline as P


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("engorda-genai-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def genai_policy_document():
    return {
        "version": 1,
        "unknown_top_level": "ignored",
        "defaults": {
            "model_family": "meta_llama",
            "language": "pt-BR",
            "temperature": 0.8,
            "top_p": 0.9,
            "max_tokens": 4096,
            "unknown_default": True,
        },
        "products": {
            "cdb_simplificado": {
                "system_instruction": "Gere texto operacional realista.",
                "excluded_context_columns": {
                    "LANCAMENTO": ["TXT_XML_LANCAMENTO"],
                },
                "targets": [
                    {
                        "table": "INSTRUMENTO_FINANCEIRO",
                        "column": "TXT_CARACT_COMPLEMENTARES",
                        "max_chars": 772,
                        "instruction": "Descreva caracteristicas complementares.",
                    },
                    {
                        "table": "EVENTO",
                        "column": "TXT_OBSERVACAO",
                        "max_chars": 60,
                        "instruction": "Escreva uma observacao curta.",
                    },
                    {
                        "table": "OPERACAO",
                        "column": "TXT_HISTORICO",
                        "max_chars": 138,
                        "instruction": "Escreva um historico operacional.",
                    },
                ],
                "unknown_product_key": {"ignored": True},
            },
        },
    }


class TestGenAiPolicy:
    def test_resolves_reviewed_cdb_policy_and_ignores_unknown_keys(self):
        policy = E.resolve_genai_policy(
            json.dumps(genai_policy_document()),
            product="cdb_simplificado",
        )

        assert policy.version == 1
        assert policy.model_family == "meta_llama"
        assert policy.language == "pt-BR"
        assert policy.temperature == 0.8
        assert policy.top_p == 0.9
        assert policy.max_tokens == 4096
        assert policy.excluded_context_columns == {
            "LANCAMENTO": ("TXT_XML_LANCAMENTO",),
        }
        assert [(target.table, target.column, target.max_chars) for target in policy.targets] == [
            ("INSTRUMENTO_FINANCEIRO", "TXT_CARACT_COMPLEMENTARES", 772),
            ("EVENTO", "TXT_OBSERVACAO", 60),
            ("OPERACAO", "TXT_HISTORICO", 138),
        ]
        assert len(policy.source_sha256) == 64
        assert len(policy.resolved_sha256) == 64

    @pytest.mark.parametrize(
        ("mutate", "message"),
        [
            (lambda document: document.update(version=2), "version"),
            (
                lambda document: document["products"].pop("cdb_simplificado"),
                "cdb_simplificado",
            ),
            (
                lambda document: document["products"]["cdb_simplificado"]["targets"][0].update(
                    max_chars=773
                ),
                "targets",
            ),
            (
                lambda document: document["products"]["cdb_simplificado"].update(
                    excluded_context_columns={}
                ),
                "excluded_context_columns",
            ),
        ],
    )
    def test_rejects_unsupported_or_unreviewed_policy(self, mutate, message):
        document = genai_policy_document()
        mutate(document)

        with pytest.raises(ValueError, match=message):
            E.resolve_genai_policy(document, product="cdb_simplificado")

    def test_validates_targets_against_runtime_contracts(self):
        policy = E.resolve_genai_policy(genai_policy_document(), product="cdb_simplificado")
        column_types = {target.table: {target.column: "string"} for target in policy.targets}
        specs = {
            target.table: {
                "pk_cols": ["ID"],
                "not_null_cols": [],
                "foreign_keys": [],
            }
            for target in policy.targets
        }

        E.validate_genai_policy_runtime(
            policy,
            specs=specs,
            column_types=column_types,
            static_tables=(),
            nullified_columns={},
        )

    @pytest.mark.parametrize(
        ("change", "message"),
        [
            ("spec", "specs.json"),
            ("type", "textual"),
            ("static", "static"),
            ("nullify", "nullification"),
        ],
    )
    def test_rejects_runtime_target_conflicts(self, change, message):
        policy = E.resolve_genai_policy(genai_policy_document(), product="cdb_simplificado")
        column_types = {target.table: {target.column: "string"} for target in policy.targets}
        specs = {
            target.table: {
                "pk_cols": ["ID"],
                "not_null_cols": [],
                "foreign_keys": [],
            }
            for target in policy.targets
        }
        static_tables = set()
        nullified_columns = {}
        first = policy.targets[0]
        if change == "spec":
            specs[first.table]["not_null_cols"] = [first.column]
        elif change == "type":
            column_types[first.table][first.column] = "long"
        elif change == "static":
            static_tables.add(first.table)
        else:
            nullified_columns[first.table] = [first.column]

        with pytest.raises(ValueError, match=message):
            E.validate_genai_policy_runtime(
                policy,
                specs=specs,
                column_types=column_types,
                static_tables=static_tables,
                nullified_columns=nullified_columns,
            )


class ScriptedAdapter:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def complete(self, attempt):
        self.calls.append(attempt)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ConcurrentAdapter:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def complete(self, attempt):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.03)
            variants = {}
            for clone, target_id in attempt.expected:
                variants.setdefault(clone, {})[target_id] = f"value-{clone}-{target_id}"
            return json.dumps({
                "variants": [
                    {"k": clone, "values": values}
                    for clone, values in sorted(variants.items())
                ],
            })
        finally:
            with self.lock:
                self.active -= 1


def resolved_policy():
    return E.resolve_genai_policy(genai_policy_document(), product="cdb_simplificado")


def sample_instrument(root=10):
    return E.GenAiInstrumentAggregate(
        root_num_if=root,
        rows=(
            E.GenAiSourceRow(
                table="INSTRUMENTO_FINANCEIRO",
                source_pk={"NUM_IF": root},
                values={
                    "NUM_IF": root,
                    "DAT_EMISSAO": date(2026, 1, 2),
                    "TXT_CARACT_COMPLEMENTARES": "001234",
                },
            ),
            E.GenAiSourceRow(
                table="EVENTO",
                source_pk={"NUM_ID_EVENTO": 99},
                values={
                    "NUM_ID_EVENTO": 99,
                    "NUM_IF": root,
                    "TXT_OBSERVACAO": None,
                },
            ),
            E.GenAiSourceRow(
                table="LANCAMENTO",
                source_pk={"NUM_ID_LANCAMENTO": 7},
                values={
                    "NUM_ID_LANCAMENTO": 7,
                    "TXT_XML_LANCAMENTO": "<ignored>large</ignored>",
                },
            ),
        ),
    )


class TestGenAiGeneration:
    def test_builds_canonical_request_with_opaque_targets_and_excludes_xml(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=2,
            run_seed=42,
        )

        assert request.root_num_if == "10"
        assert request.clone_factor == 2
        assert [cell.target_id for cell in request.cells] == ["t0001", "t0002"]
        assert [(cell.table, cell.column) for cell in request.cells] == [
            ("EVENTO", "TXT_OBSERVACAO"),
            ("INSTRUMENTO_FINANCEIRO", "TXT_CARACT_COMPLEMENTARES"),
        ]
        assert "TXT_XML_LANCAMENTO" not in request.context_json
        assert "2026-01-02" in request.context_json

    def test_retries_unresolved_cells_then_falls_back_only_invalid_cell(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=2,
            run_seed=42,
        )
        adapter = ScriptedAdapter(
            [
                json.dumps(
                    {
                        "variants": [
                            {"k": 1, "values": {"t0001": "Evento 1", "t0002": "X" * 773}},
                            {"k": 2, "values": {"t0001": "Evento 2"}},
                        ]
                    }
                ),
                json.dumps(
                    {
                        "variants": [
                            {"k": 1, "values": {"t0002": "Caracteristica 1"}},
                        ]
                    }
                ),
                json.dumps({"variants": []}),
            ]
        )

        result = E.generate_genai_replacements([request], adapter=adapter)

        rows = {(row.clone_index, row.target_id): row for row in result.rows}
        assert rows[(1, "t0001")].generated_value == "Evento 1"
        assert rows[(1, "t0002")].generated_value == "Caracteristica 1"
        assert rows[(2, "t0001")].generated_value == "Evento 2"
        assert rows[(2, "t0002")].action == "KEEP_SOURCE"
        assert rows[(2, "t0002")].status == "FALLBACK_INVALID"
        assert len(adapter.calls) == 3
        assert adapter.calls[1].expected == ((1, "t0002"), (2, "t0002"))
        assert result.metrics.status == "DEGRADED"
        assert result.metrics.generated_cells == 3
        assert result.metrics.fallback_cells == 1
        assert result.metrics.diversity["EVENTO.TXT_OBSERVACAO"] == {
            "source_change_rate": 1.0,
            "sibling_uniqueness_rate": 1.0,
        }
        assert result.metrics.diversity["INSTRUMENTO_FINANCEIRO.TXT_CARACT_COMPLEMENTARES"] == {
            "source_change_rate": 0.5,
            "sibling_uniqueness_rate": 1.0,
        }
        assert len(result.content_sha256) == 64

    def test_fails_when_no_endpoint_request_succeeds(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        adapter = ScriptedAdapter([TimeoutError("down")] * 9)

        with pytest.raises(
            E.GenAiNoSuccessfulRequests,
            match="no Oracle GenAI request succeeded",
        ) as raised:
            E.generate_genai_replacements([request], adapter=adapter)
        assert len(adapter.calls) == 9
        assert [attempt.retry_token for attempt in adapter.calls[:3]] == [
            adapter.calls[0].retry_token,
        ] * 3
        assert len({attempt.retry_token for attempt in adapter.calls}) == 3
        assert raised.value.result.metrics.status == "FAILED"
        assert raised.value.result.metrics.endpoint_status_counts == {"TIMEOUT": 9}

    def test_duplicate_json_target_identity_resolves_no_cells(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        duplicate = (
            '{"variants":[{"k":1,"values":{"t0001":"first","t0001":"second","t0002":"value"}}]}'
        )

        result = E.generate_genai_replacements([request], adapter=ScriptedAdapter([duplicate] * 3))

        assert result.metrics.generated_cells == 0
        assert result.metrics.fallback_cells == 2
        assert {row.status for row in result.rows} == {"FALLBACK_INVALID"}

    def test_retries_text_not_representable_in_oracle_charset(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        adapter = ScriptedAdapter(
            [
                json.dumps(
                    {
                        "variants": [
                            {
                                "k": 1,
                                "values": {
                                    "t0001": "Observação — teste",
                                    "t0002": "Característica válida",
                                },
                            }
                        ]
                    }
                ),
                json.dumps(
                    {
                        "variants": [
                            {"k": 1, "values": {"t0001": "Observação - teste"}}
                        ]
                    }
                ),
            ]
        )

        result = E.generate_genai_replacements([request], adapter=adapter)

        rows = {row.target_id: row for row in result.rows}
        assert rows["t0001"].generated_value == "Observação - teste"
        assert rows["t0001"].attempt_count == 2
        assert rows["t0002"].generated_value == "Característica válida"
        assert len(adapter.calls) == 2

    def test_repeated_event_rows_receive_distinct_target_identities(self):
        base = sample_instrument()
        second_event = E.GenAiSourceRow(
            table="EVENTO",
            source_pk={"NUM_ID_EVENTO": 100},
            values={
                "NUM_ID_EVENTO": 100,
                "NUM_IF": 10,
                "TXT_OBSERVACAO": "old",
            },
        )
        request = E.build_genai_request(
            E.GenAiInstrumentAggregate(
                root_num_if=10,
                rows=(*base.rows, second_event),
            ),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )

        event_cells = [cell for cell in request.cells if cell.table == "EVENTO"]
        assert [cell.target_id for cell in event_cells] == ["t0001", "t0002"]
        assert len({cell.source_pk_json for cell in event_cells}) == 2

    def test_reports_concurrency_latency_throughput_and_endpoint_statuses(self):
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        adapter = ScriptedAdapter([
            TimeoutError("temporary timeout"),
            json.dumps({
                "variants": [{
                    "k": 1,
                    "values": {"t0001": "Evento", "t0002": "Caracteristica"},
                }],
            }),
        ])

        result = E.generate_genai_replacements(
            [request], adapter=adapter, max_concurrency=32
        )

        assert result.metrics.configured_concurrency == 32
        assert result.metrics.effective_concurrency == 1
        assert result.metrics.endpoint_call_count == 2
        assert result.metrics.endpoint_status_counts == {
            "SUCCESS": 1,
            "TIMEOUT": 1,
        }
        assert set(result.metrics.successful_call_latency_ms) == {"p50", "p95", "max"}
        assert result.metrics.successful_call_latency_ms["max"] >= 0
        assert result.metrics.endpoint_calls_per_second > 0
        assert result.metrics.sources_per_second > 0

    def test_runs_multiple_sources_at_configured_concurrency(self):
        requests = [
            E.build_genai_request(
                sample_instrument(root),
                policy=resolved_policy(),
                clone_factor=1,
                run_seed=42,
            )
            for root in range(10, 14)
        ]
        adapter = ConcurrentAdapter()

        result = E.generate_genai_replacements(
            requests, adapter=adapter, max_concurrency=2
        )

        assert adapter.max_active == 2
        assert result.metrics.configured_concurrency == 2
        assert result.metrics.effective_concurrency == 2

    @pytest.mark.parametrize(
        ("status", "expected"),
        [(429, "HTTP_429"), (503, "HTTP_5XX"), (403, "HTTP_403")],
    )
    def test_classifies_oci_http_statuses_for_benchmarking(self, status, expected):
        error = RuntimeError("service error")
        error.status = status

        assert E._genai_endpoint_error_status(error) == expected

    def test_classifies_oci_request_exception_read_timeout(self):
        RequestException = type("RequestException", (Exception,), {})
        error = RequestException(
            "HTTPSConnectionPool: Read timed out. (read timeout=120)"
        )

        assert E._genai_endpoint_error_status(error) == "TIMEOUT"


class FakeOciModel:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)


class FakeOciClient:
    def __init__(self, config, **kwargs):
        self.config = config
        self.kwargs = kwargs
        self.calls = []

    def chat(self, chat_details, **kwargs):
        self.calls.append((chat_details, kwargs))
        content = SimpleNamespace(text='{"variants": []}')
        message = SimpleNamespace(content=[content])
        choice = SimpleNamespace(message=message)
        chat_response = SimpleNamespace(choices=[choice])
        return SimpleNamespace(data=SimpleNamespace(chat_response=chat_response))


def fake_oci_module():
    model_names = (
        "ChatDetails",
        "DedicatedServingMode",
        "GenericChatRequest",
        "SystemMessage",
        "UserMessage",
        "TextContent",
        "JsonSchemaResponseFormat",
        "ResponseJsonSchema",
    )
    models = SimpleNamespace(**{name: FakeOciModel for name in model_names})
    signer = object()
    return SimpleNamespace(
        auth=SimpleNamespace(
            signers=SimpleNamespace(get_resource_principals_signer=lambda: signer)
        ),
        generative_ai_inference=SimpleNamespace(
            GenerativeAiInferenceClient=FakeOciClient,
            models=models,
        ),
        _signer=signer,
    )


class TestOracleGenAiAdapter:
    def test_calls_dedicated_meta_llama_endpoint_with_resource_principal(self):
        oci = fake_oci_module()
        adapter = E.OracleGenAiChatAdapter(
            endpoint_id="ocid1.generativeaiendpoint.test",
            compartment_id="ocid1.compartment.test",
            region="sa-saopaulo-1",
            oci_module=oci,
        )
        request = E.build_genai_request(
            sample_instrument(),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        attempt = E._build_genai_attempt(request, 1, {(1, "t0001")})

        assert adapter.complete(attempt) == '{"variants": []}'

        client = adapter.client
        assert client.config == {"region": "sa-saopaulo-1"}
        assert client.kwargs["signer"] is oci._signer
        assert client.kwargs["timeout"] == (10, 120)
        details, call_kwargs = client.calls[0]
        assert details.compartment_id == "ocid1.compartment.test"
        assert details.serving_mode.endpoint_id == "ocid1.generativeaiendpoint.test"
        assert details.chat_request.seed == attempt.seed
        assert details.chat_request.response_format.type == "JSON_SCHEMA"
        assert details.chat_request.response_format.json_schema.is_strict is True
        assert call_kwargs["opc_retry_token"] == attempt.retry_token

    def test_preflight_rejects_incompatible_runtime_sdk(self):
        oci = fake_oci_module()
        del oci.generative_ai_inference.models.GenericChatRequest

        with pytest.raises(ValueError, match="GenericChatRequest"):
            E.OracleGenAiChatAdapter(
                endpoint_id="ocid1.generativeaiendpoint.test",
                compartment_id="ocid1.compartment.test",
                region="sa-saopaulo-1",
                oci_module=oci,
            )


class TestGenAiJobContract:
    def test_runner_and_dataflow_hard_caps_cannot_drift(self):
        assert P.GENAI_MAX_SOURCE_INSTRUMENTS == E.GENAI_MAX_SOURCE_INSTRUMENTS
        assert P.GENAI_MAX_FACTOR_K == E.GENAI_MAX_FACTOR_K
        assert P.GENAI_DEFAULT_CONCURRENCY == E.GENAI_DEFAULT_CONCURRENCY

    def test_direct_plan_derives_sibling_genai_root(self):
        assert (
            E._default_genai_artifact_root(
                "oci://bucket@ns/run/products/cdb/engorda/selection-plan.json"
            )
            == "oci://bucket@ns/run/products/cdb/genai"
        )

    def test_cli_parses_enabled_plan_contract(self):
        args = E.parse_arguments(
            [
                "--phase",
                "plan",
                "--produto",
                "cdb_simplificado",
                "--n-instrumentos",
                "10",
                "--raw-uri",
                "oci://bucket@ns/raw",
                "--output-uri",
                "oci://bucket@ns/synthetic",
                "--plan-uri",
                "oci://bucket@ns/selection-plan.json",
                "--enable-genai",
                "--genai-policy",
                "oci://bucket@ns/genai-policy.json",
                "--genai-endpoint-id",
                "ocid1.generativeaiendpoint.test",
                "--genai-compartment-id",
                "ocid1.compartment.test",
                "--genai-region",
                "sa-saopaulo-1",
                "--genai-concurrency",
                "32",
                "--genai-artifact-root",
                "oci://bucket@ns/genai",
            ]
        )

        assert args.enable_genai is True
        assert args.genai_policy == "oci://bucket@ns/genai-policy.json"
        assert args.genai_concurrency == 32
        assert args.genai_artifact_root == "oci://bucket@ns/genai"

    def test_enabled_plan_requires_conditional_inputs_and_hard_limits(self):
        base = dict(
            produto="cdb_simplificado",
            n_instrumentos=10,
            phase="plan",
            plan_uri="plan.json",
            raw_uri="raw",
            output_uri="synthetic",
            enable_genai=True,
        )
        with pytest.raises(ValueError, match="genai_policy"):
            E._validate_engorda_job(E.EngordaJob(**base))

        configured = {
            **base,
            "genai_policy": "policy.json",
            "genai_endpoint_id": "ocid1.generativeaiendpoint.test",
            "genai_compartment_id": "ocid1.compartment.test",
            "genai_region": "sa-saopaulo-1",
            "genai_concurrency": 32,
            "genai_artifact_root": "genai",
        }
        E._validate_engorda_job(E.EngordaJob(**configured))
        E._validate_engorda_job(E.EngordaJob(**{**configured, "n_instrumentos": 10000}))

        with pytest.raises(ValueError, match="at most 10000"):
            E._validate_engorda_job(E.EngordaJob(**{**configured, "n_instrumentos": 10001}))
        with pytest.raises(ValueError, match="fator_k <= 5"):
            E._validate_engorda_job(E.EngordaJob(**{**configured, "fator_k": 6}))
        with pytest.raises(ValueError, match="positive integer"):
            E._validate_engorda_job(
                E.EngordaJob(**{**configured, "genai_concurrency": 0})
            )
        with pytest.raises(ValueError, match="endpoint OCID"):
            E._validate_engorda_job(
                E.EngordaJob(
                    **{
                        **configured,
                        "genai_endpoint_id": "not-an-ocid",
                    }
                )
            )

    def test_materialize_needs_only_enabled_acknowledgement(self):
        E._validate_engorda_job(
            E.EngordaJob(
                produto="cdb_simplificado",
                phase="materialize",
                plan_uri="plan.json",
                reservation_uri="reservation.json",
                raw_uri="raw",
                output_uri="synthetic",
                enable_genai=True,
            )
        )

    def test_direct_all_checks_artifact_absence_before_policy_read(self, monkeypatch):
        class ArtifactExists(RuntimeError):
            pass

        class FakeSpark:
            def stop(self):
                pass

        monkeypatch.setattr(E, "create_spark_session", lambda *_args: FakeSpark())
        monkeypatch.setattr(
            E,
            "get_engorda_env",
            lambda *_args, **_kwargs: {
                "DATAGEN_RAW_BASE_URI": "raw",
                "DATAGEN_RAW_PREFIX": "",
                "DATAGEN_SYNTHETIC_BASE_URI": "synthetic",
                "DATAGEN_SYNTHETIC_PREFIX": "",
                "DATAGEN_CLONE_PREFIX": "cdb",
                "DATAGEN_OUTPUT_URI": "synthetic/cdb",
                "DATAGEN_SPECS_URI": "spec.json",
            },
        )
        monkeypatch.setattr(
            E,
            "_assert_exact_output_absent",
            lambda _spark, path: (_ for _ in ()).throw(ArtifactExists(path)),
        )
        monkeypatch.setattr(
            E,
            "_read_json_artifact",
            lambda *_args: (_ for _ in ()).throw(AssertionError("policy read too early")),
        )

        with pytest.raises(ArtifactExists, match="genai-root"):
            E.executar_job(
                E.EngordaJob(
                    produto="cdb_simplificado",
                    num_ifs=(10,),
                    meu_numero_prefix="321",
                    no_oracle=True,
                    enable_genai=True,
                    genai_policy="policy.json",
                    genai_endpoint_id="ocid1.generativeaiendpoint.test",
                    genai_compartment_id="ocid1.compartment.test",
                    genai_region="sa-saopaulo-1",
                    genai_artifact_root="genai-root",
                )
            )


class TestGenAiSparkApplication:
    def test_collects_decimal_root_identity_without_integer_string_parsing(self, spark):
        root = spark.createDataFrame(
            [(Decimal("2253875817.0000000000"), "text")],
            "NUM_IF decimal(38,10), TXT_CARACT_COMPLEMENTARES string",
        )
        provenance = spark.createDataFrame(
            [(Decimal("2253875817.0000000000"), Decimal("2253875817.0000000000"))],
            "NUM_IF decimal(38,10), __root_num_if decimal(38,10)",
        )
        aggregates = E.collect_genai_instruments(
            {E.TABELA_RAIZ: root},
            {E.TABELA_RAIZ: provenance},
            {
                E.TABELA_RAIZ: E.PlanoTabela(
                    E.TABELA_RAIZ,
                    (E.COL_NUM_IF,),
                    pk_regra="OFFSET_PROPRIO",
                    pk_start=1,
                ),
            },
        )

        assert aggregates[0].root_num_if == "2253875817.0000000000"
        assert aggregates[0].rows[0].source_pk == {"NUM_IF": Decimal("2253875817.0000000000")}

    def test_materialize_rejects_enablement_mismatch_before_loading_tables(self, spark):
        with pytest.raises(ValueError, match="--enable-genai diverge"):
            E.executa_clonagem(
                spark,
                {},
                {},
                product_profile=E.get_product_profile("cdb_simplificado"),
                phase="materialize",
                planned_artifact={
                    "oracle_access": "disabled",
                    "genai": {"enabled": True},
                },
                reservation={},
                snapshot_lotes={},
                snapshot_lote_counts={},
                no_oracle=True,
                enable_genai=False,
            )

    def test_clone_matches_decimal_source_pk_using_canonical_string(self, spark):
        lote = spark.createDataFrame(
            [(Decimal("1.0000000000"), "original")],
            "ID decimal(38,10), TXT string",
        )
        replacements = spark.createDataFrame(
            [
                (
                    "EXAMPLE",
                    '{"ID":"1.0000000000"}',
                    1,
                    "TXT",
                    "generated",
                    "REPLACE",
                ),
            ],
            [
                "TABLE_NAME",
                "SOURCE_PK_JSON",
                "CLONE_INDEX",
                "COLUMN_NAME",
                "GENERATED_VALUE",
                "ACTION",
            ],
        )

        clones, _mapping = E.clona_tabela(
            spark,
            E.PlanoTabela(
                name="EXAMPLE",
                pk_cols=("ID",),
                pk_regra="OFFSET_PROPRIO",
                pk_start=100,
            ),
            lote,
            1,
            {},
            genai_replacements=replacements,
        )

        assert clones.first().TXT == "generated"

    def test_composite_unicode_source_pk_matches_python_and_spark_json(self, spark):
        source = E.GenAiSourceRow(
            table="EVENTO",
            source_pk={
                "Z_CODE": "ação",
                "A_ID": Decimal("1.0000000000"),
            },
            values={
                "Z_CODE": "ação",
                "A_ID": Decimal("1.0000000000"),
                "TXT_OBSERVACAO": "original",
            },
        )
        request = E.build_genai_request(
            E.GenAiInstrumentAggregate(root_num_if=10, rows=(source,)),
            policy=resolved_policy(),
            clone_factor=1,
            run_seed=42,
        )
        source_pk_json = request.cells[0].source_pk_json
        replacements = spark.createDataFrame(
            [
                (
                    "EVENTO",
                    source_pk_json,
                    1,
                    "TXT_OBSERVACAO",
                    "gerado",
                    "REPLACE",
                ),
            ],
            [
                "TABLE_NAME",
                "SOURCE_PK_JSON",
                "CLONE_INDEX",
                "COLUMN_NAME",
                "GENERATED_VALUE",
                "ACTION",
            ],
        )
        clones = spark.createDataFrame(
            [
                (
                    "novo",
                    Decimal("100.0000000000"),
                    "original",
                    "ação",
                    Decimal("1.0000000000"),
                    1,
                ),
            ],
            (
                "Z_CODE string, A_ID decimal(38,10), TXT_OBSERVACAO string, "
                "__orig_Z_CODE string, __orig_A_ID decimal(38,10), __clone_k int"
            ),
        )

        enriched = E._aplica_genai_replacements(
            clones,
            E.PlanoTabela(
                name="EVENTO",
                pk_cols=("Z_CODE", "A_ID"),
                pk_regra="OFFSET_PROPRIO",
            ),
            {"Z_CODE": "__orig_Z_CODE", "A_ID": "__orig_A_ID"},
            replacements,
        )

        assert source_pk_json == '{"A_ID":"1.0000000000","Z_CODE":"ação"}'
        assert enriched.first().TXT_OBSERVACAO == "gerado"

    def test_replacement_artifact_schema_is_versioned_for_string_identities(self):
        assert E.GENAI_ARTIFACT_SCHEMA_VERSION == 2

    def test_clone_applies_replacement_by_source_pk_and_clone_index(self, spark):
        lote = spark.createDataFrame(
            [(1, "original", "keep-1"), (2, "second", "keep-2")],
            ["ID", "TXT", "OUTSIDE_ALLOWLIST"],
        )
        plan = E.PlanoTabela(
            name="EXAMPLE",
            pk_cols=("ID",),
            pk_regra="OFFSET_PROPRIO",
            pk_start=100,
        )
        replacements = spark.createDataFrame(
            [
                (
                    "EXAMPLE",
                    '{"ID":"1"}',
                    1,
                    "TXT",
                    "generated",
                    "REPLACE",
                ),
            ],
            [
                "TABLE_NAME",
                "SOURCE_PK_JSON",
                "CLONE_INDEX",
                "COLUMN_NAME",
                "GENERATED_VALUE",
                "ACTION",
            ],
        )

        clones, _mapping = E.clona_tabela(
            spark,
            plan,
            lote,
            2,
            {},
            genai_replacements=replacements,
        )

        assert [
            (row.ID, row.TXT, row.OUTSIDE_ALLOWLIST) for row in clones.orderBy("ID").collect()
        ] == [
            (100, "generated", "keep-1"),
            (101, "original", "keep-1"),
            (102, "second", "keep-2"),
            (103, "second", "keep-2"),
        ]

    def test_writes_and_loads_immutable_genai_artifacts(self, spark, tmp_path):
        policy = resolved_policy()
        request = E.build_genai_request(
            sample_instrument(), policy=policy, clone_factor=1, run_seed=42
        )
        adapter = ScriptedAdapter(
            [
                json.dumps(
                    {
                        "variants": [
                            {
                                "k": 1,
                                "values": {"t0001": "Evento", "t0002": "Caracteristica"},
                            }
                        ],
                    }
                )
            ]
        )
        result = E.generate_genai_replacements([request], adapter=adapter)
        root = str(tmp_path / "genai")

        descriptor = E.write_genai_artifacts(
            spark,
            root,
            policy=policy,
            result=result,
            endpoint_id="ocid1.generativeaiendpoint.test",
            compartment_id="ocid1.compartment.test",
            region="sa-saopaulo-1",
        )

        assert descriptor["enabled"] is True
        assert descriptor["status"] == "SUCCESS"
        assert descriptor["replacements"]["content_sha256"] == result.content_sha256
        manifest = json.loads((tmp_path / "genai" / "manifest.json").read_text())
        assert manifest["metrics"]["configured_concurrency"] == 4
        assert manifest["metrics"]["endpoint_status_counts"] == {"SUCCESS": 1}
        assert manifest["metrics"]["sources_per_second"] > 0
        loaded = E.load_genai_replacements(spark, descriptor)
        assert loaded.count() == len(result.rows)
        cloned_root, _mapping = E.clona_tabela(
            spark,
            E.PlanoTabela(
                name="INSTRUMENTO_FINANCEIRO",
                pk_cols=("NUM_IF",),
                pk_regra="OFFSET_PROPRIO",
                pk_start=100,
            ),
            spark.createDataFrame(
                [(10, "001234")],
                "NUM_IF long, TXT_CARACT_COMPLEMENTARES string",
            ),
            1,
            {},
            genai_replacements=loaded,
        )
        assert cloned_root.first().TXT_CARACT_COMPLEMENTARES == "Caracteristica"
        with pytest.raises(ValueError, match="already exists|já existe"):
            E.write_genai_artifacts(
                spark,
                root,
                policy=policy,
                result=result,
                endpoint_id="ocid1.generativeaiendpoint.test",
                compartment_id="ocid1.compartment.test",
                region="sa-saopaulo-1",
            )

    def test_writes_failed_endpoint_benchmark_telemetry(self, spark, tmp_path):
        policy = resolved_policy()
        request = E.build_genai_request(
            sample_instrument(), policy=policy, clone_factor=1, run_seed=42
        )
        with pytest.raises(E.GenAiNoSuccessfulRequests) as raised:
            E.generate_genai_replacements(
                [request], adapter=ScriptedAdapter([TimeoutError("down")] * 9)
            )

        E.write_genai_artifacts(
            spark,
            str(tmp_path / "failed-genai"),
            policy=policy,
            result=raised.value.result,
            endpoint_id="ocid1.generativeaiendpoint.test",
            compartment_id="ocid1.compartment.test",
            region="sa-saopaulo-1",
        )

        manifest = json.loads(
            (tmp_path / "failed-genai" / "manifest.json").read_text()
        )
        assert manifest["status"] == "FAILED"
        assert manifest["metrics"]["endpoint_status_counts"] == {"TIMEOUT": 9}
        assert manifest["metrics"]["successful_call_latency_ms"] == {
            "p50": 0.0,
            "p95": 0.0,
            "max": 0.0,
        }

    def test_enabled_plan_freezes_generated_replacements(self, spark, tmp_path, monkeypatch):
        root = spark.createDataFrame(
            [(10, 49, "original")],
            "NUM_IF long, NUM_TIPO_IF long, TXT_CARACT_COMPLEMENTARES string",
        )
        event = spark.createDataFrame(
            [(20, 10, None)],
            "NUM_ID_EVENTO long, NUM_IF long, TXT_OBSERVACAO string",
        )
        operation = spark.createDataFrame(
            [(30, 10, "old", "1", "2", 4509)],
            (
                "NUM_ID_OPERACAO long, NUM_IF long, TXT_HISTORICO string, "
                "NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string, "
                "NUM_ID_TIPO_OPER_OBJETO_SERV long"
            ),
        )
        lotes = {
            "INSTRUMENTO_FINANCEIRO": root,
            "EVENTO": event,
            "OPERACAO": operation,
        }
        plans = {
            table: E.PlanoTabela(
                table,
                (pk,),
                pk_regra="OFFSET_PROPRIO",
                pk_start=start,
            )
            for table, pk, start in (
                ("INSTRUMENTO_FINANCEIRO", "NUM_IF", 100),
                ("EVENTO", "NUM_ID_EVENTO", 200),
                ("OPERACAO", "NUM_ID_OPERACAO", 300),
            )
        }
        provenances = {
            "INSTRUMENTO_FINANCEIRO": spark.createDataFrame(
                [(10, 10)], "NUM_IF long, __root_num_if long"
            ),
            "EVENTO": spark.createDataFrame([(20, 10)], "NUM_ID_EVENTO long, __root_num_if long"),
            "OPERACAO": spark.createDataFrame(
                [(30, 10)], "NUM_ID_OPERACAO long, __root_num_if long"
            ),
        }
        spec = {
            "INSTRUMENTO_FINANCEIRO": {
                "pk_cols": ["NUM_IF"],
                "foreign_keys": [],
                "static": False,
            },
            "EVENTO": {
                "pk_cols": ["NUM_ID_EVENTO"],
                "foreign_keys": [],
                "static": False,
            },
            "OPERACAO": {
                "pk_cols": ["NUM_ID_OPERACAO"],
                "foreign_keys": [],
                "static": False,
            },
        }
        monkeypatch.setitem(
            E.TABELAS_ENGORDA_POR_PRODUTO,
            "cdb_simplificado",
            tuple(lotes),
        )
        monkeypatch.setattr(E, "monta_plano", lambda *_args, **_kwargs: plans)
        monkeypatch.setattr(E, "ordem_topologica", lambda *_args: list(lotes))
        monkeypatch.setattr(E, "seleciona_instrumentos", lambda *_args, **_kwargs: [10])
        monkeypatch.setattr(E, "_deriva_tipo_oracle", lambda *_args: 49)
        monkeypatch.setattr(E, "_valida_contrato_nulificacao_seletiva", lambda *_args: None)
        monkeypatch.setattr(E, "_valida_lastro_obrigatorio", lambda *_args: None)
        monkeypatch.setattr(E, "_aplica_cod_credito_sintetico", lambda *_args: None)
        monkeypatch.setattr(E, "_diagnostico_lastro", lambda *_args: None)

        def calculate(*_args, counts_out=None, provenance_out=None, **_kwargs):
            counts_out.update({table: frame.count() for table, frame in lotes.items()})
            provenance_out.update(provenances)
            return lotes

        monkeypatch.setattr(E, "calcula_lotes", calculate)
        adapter = ScriptedAdapter(
            [
                json.dumps(
                    {
                        "variants": [
                            {
                                "k": 1,
                                "values": {
                                    "t0001": "Evento",
                                    "t0002": "Caracteristica",
                                    "t0003": "Historico",
                                },
                            }
                        ],
                    }
                )
            ]
        )
        plan_uri = str(tmp_path / "selection-plan.json")
        artifact_root = str(tmp_path / "genai")
        config = {
            "DATAGEN_RAW_BASE_URI": str(tmp_path / "raw"),
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(tmp_path / "synthetic"),
            "DATAGEN_SYNTHETIC_PREFIX": "",
            "DATAGEN_CLONE_PREFIX": "cdb",
            "DATAGEN_SPECS_URI": "spec.json",
        }

        result = E.executa_clonagem(
            spark,
            config,
            spec,
            product_profile=E.get_product_profile("cdb_simplificado"),
            num_ifs=[10],
            fator_k=1,
            no_oracle=True,
            phase="plan",
            plan_uri=plan_uri,
            specs_uri="spec.json",
            enable_genai=True,
            genai_execution=E.GenAiExecutionConfig(
                policy=resolved_policy(),
                policy_uri="policy.json",
                adapter=adapter,
                artifact_root=artifact_root,
                endpoint_id="ocid1.generativeaiendpoint.test",
                compartment_id="ocid1.compartment.test",
                region="sa-saopaulo-1",
            ),
        )

        assert result["plan"]["genai"]["enabled"] is True
        assert result["plan"]["genai"]["replacements"]["row_count"] == 3
        assert (
            json.loads((tmp_path / "selection-plan.json").read_text())["plan_id"]
            == (result["plan"]["plan_id"])
        )
        assert len(adapter.calls) == 1

    def test_direct_all_publishes_audit_artifacts_before_clone_output(
        self, spark, tmp_path, monkeypatch
    ):
        class ArtifactsPublished(RuntimeError):
            pass

        root = spark.createDataFrame(
            [(10, 49, "old")],
            "NUM_IF long, NUM_TIPO_IF long, TXT_CARACT_COMPLEMENTARES string",
        )
        provenance = spark.createDataFrame([(10, 10)], "NUM_IF long, __root_num_if long")
        plan = E.PlanoTabela(
            E.TABELA_RAIZ,
            (E.COL_NUM_IF,),
            pk_regra="OFFSET_PROPRIO",
            pk_start=100,
        )
        monkeypatch.setitem(
            E.TABELAS_ENGORDA_POR_PRODUTO,
            "cdb_simplificado",
            (E.TABELA_RAIZ,),
        )
        monkeypatch.setattr(E, "_valida_contrato_nulificacao_seletiva", lambda *_: None)
        monkeypatch.setattr(E, "_carrega_faltantes", lambda *_: None)
        monkeypatch.setattr(E, "monta_plano", lambda *_args, **_kwargs: {E.TABELA_RAIZ: plan})
        monkeypatch.setattr(E, "ordem_topologica", lambda *_: [E.TABELA_RAIZ])
        monkeypatch.setattr(E, "seleciona_instrumentos", lambda *_args, **_kwargs: [10])
        monkeypatch.setattr(E, "_deriva_tipo_oracle", lambda *_: 49)
        monkeypatch.setattr(E, "_valida_lastro_obrigatorio", lambda *_: None)
        monkeypatch.setattr(E, "_aplica_cod_credito_sintetico", lambda *_: None)
        monkeypatch.setattr(E, "_diagnostico_lastro", lambda *_: None)
        monkeypatch.setattr(E, "validate_genai_policy_runtime", lambda *_args, **_kwargs: None)

        def calculate(*_args, counts_out=None, provenance_out=None, **_kwargs):
            counts_out[E.TABELA_RAIZ] = 1
            provenance_out[E.TABELA_RAIZ] = provenance
            return {E.TABELA_RAIZ: root}

        monkeypatch.setattr(E, "calcula_lotes", calculate)
        monkeypatch.setattr(E, "collect_genai_instruments", lambda *_: ("aggregate",))
        monkeypatch.setattr(E, "build_genai_request", lambda *_args, **_kwargs: "request")
        generation = object()
        monkeypatch.setattr(E, "generate_genai_replacements", lambda *_args, **_kwargs: generation)
        captured = {}

        def write(_spark, artifact_root, **kwargs):
            captured.update(root=artifact_root, generation=kwargs["result"])
            return {"enabled": True}

        monkeypatch.setattr(E, "write_genai_artifacts", write)
        monkeypatch.setattr(
            E,
            "load_genai_replacements",
            lambda *_args: (_ for _ in ()).throw(ArtifactsPublished()),
        )
        config = {
            "DATAGEN_RAW_BASE_URI": str(tmp_path / "raw"),
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(tmp_path / "synthetic"),
            "DATAGEN_SYNTHETIC_PREFIX": "",
            "DATAGEN_CLONE_PREFIX": "cdb",
            "DATAGEN_SPECS_URI": "spec.json",
        }
        artifact_root = str(tmp_path / "genai")

        with pytest.raises(ArtifactsPublished):
            E.executa_clonagem(
                spark,
                config,
                {
                    E.TABELA_RAIZ: {
                        "pk_cols": [E.COL_NUM_IF],
                        "foreign_keys": [],
                        "static": False,
                    },
                },
                product_profile=E.get_product_profile("cdb_simplificado"),
                num_ifs=[10],
                meu_numero_prefix="321",
                no_oracle=True,
                phase="all",
                enable_genai=True,
                genai_execution=E.GenAiExecutionConfig(
                    policy=resolved_policy(),
                    policy_uri="policy.json",
                    adapter=ScriptedAdapter([]),
                    artifact_root=artifact_root,
                    endpoint_id="ocid1.generativeaiendpoint.test",
                    compartment_id="ocid1.compartment.test",
                    region="sa-saopaulo-1",
                ),
            )

        assert captured == {"root": artifact_root, "generation": generation}
