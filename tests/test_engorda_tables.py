import dataclasses
import json
import sys
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from datagen import engorda_tables
from scripts import run_pipeline


def test_module_imports():
    assert engorda_tables.REQUIRED_ENV_VARS == (
        "DATAGEN_RAW_BASE_URI",
        "DATAGEN_SYNTHETIC_BASE_URI",
        "DATAGEN_SPECS_URI",
    )


def test_rdb_inclusao_uses_schema_subtype_and_nullification_policy():
    profile = engorda_tables.get_product_profile("rdb_inclusao")

    subtype = profile.integrity.subtype
    assert subtype.condition_table == "CONDICAO_IF"
    assert subtype.condition_pk == "NUM_CONDICAO_IF"
    assert subtype.condition_type_column == "COD_TIPO_CONDICAO_IF"
    assert subtype.active_column == "DAT_EXCLUSAO"
    assert dict(subtype.subtype_by_type)["20"] == "RESGATE"
    assert profile.integrity.nullify_mapping() == {
        "OPERACAO": ("NUM_ID_TRANSF_ARQ_P1", "NUM_ID_TRANSF_ARQ_P2")
    }
    assert profile.integrity.selective_missing_keys == frozenset(
        {("OPERACAO", "NUM_ID_CTX_MSG_P1"), ("OPERACAO", "NUM_ID_CTX_MSG_P2")}
    )


@pytest.fixture
def build_plan(monkeypatch):
    """Use the real planner, replacing only RAW schema/max I/O."""

    def build(specs, *, schemas=None, maxes=None, **options):
        types = engorda_tables.T
        frames = {}
        for table, spec in specs.items():
            columns = dict.fromkeys(
                [
                    *spec.get("pk_cols", []),
                    *(column for fk in spec.get("foreign_keys", []) for column in fk["columns"]),
                ]
            )
            schema = (schemas or {}).get(table)
            if schema is None:
                schema = types.StructType(
                    [types.StructField(column, types.LongType()) for column in columns]
                )
            frames[table] = SimpleNamespace(schema=schema)
        monkeypatch.setattr(
            engorda_tables, "read_parquet", lambda _spark, path: frames[path.rsplit("/", 1)[-1]]
        )
        monkeypatch.setattr(
            engorda_tables,
            "_read_pk_max",
            lambda _spark, path, _column: (maxes or {}).get(path.rsplit("/", 1)[-1], 100),
        )
        defaults = dict(
            estaticas_extra=set(),
            pk_floor=0,
            pk_band=0,
            offset_num_if=None,
            n_clones_estimado=10,
        )
        return engorda_tables.monta_plano(
            object(), {"DATAGEN_RAW_BASE_URI": "raw"}, specs, **(defaults | options)
        )

    return build


class TestEngordaPhaseCli:
    def test_pipeline_argv_matches_real_engorda_parser(self):
        paths = {
            "selection_plan": "oci://bucket@ns/run/plan.json",
            "reservations": "oci://bucket@ns/run/reservation.json",
            "synthetic": "oci://bucket@ns/run/synthetic/cdb_resgate",
        }
        options = {
            "n_instrumentos": 10,
            "fator_k": 2,
            "seed": 7,
            "no_oracle": True,
        }

        planned = engorda_tables.parse_arguments(
            run_pipeline.build_engorda_plan_argv(
                "cdb_resgate",
                "oci://bucket@ns/run/raw",
                "oci://bucket@ns/run/faltantes",
                paths,
                options,
            )
        )
        materialized = engorda_tables.parse_arguments(
            run_pipeline.build_engorda_materialize_argv(
                "cdb_resgate",
                "oci://bucket@ns/run/raw",
                "oci://bucket@ns/run/faltantes",
                paths,
                options,
            )
        )

        assert planned.phase == "plan"
        assert planned.no_oracle is True
        assert planned.plan_uri == paths["selection_plan"]
        assert materialized.phase == "materialize"
        assert materialized.no_oracle is True
        assert materialized.reservation_uri == paths["reservations"]
        assert materialized.output_uri == paths["synthetic"]

    def test_all_is_default_and_keeps_selection_contract(self):
        args = engorda_tables.parse_arguments(
            [
                "--produto",
                "cdb_simplificado",
                "--num-ifs",
                "123",
                "--meu-numero-prefix",
                "321",
            ]
        )

        assert args.phase == "all"
        assert args.num_ifs == [123]
        assert args.raw_uri is None
        assert args.output_uri is None
        assert args.sem_poda_cronograma_resgate is False
        assert args.sem_poda_conta is False
        assert args.sem_ajuste_k is False

    def test_live_all_with_meu_numero_requires_reserved_materialization(self):
        job = engorda_tables.EngordaJob(
            produto="cdb_simplificado",
            num_ifs=(123,),
            meu_numero_prefix="321",
            phase="all",
        )

        with pytest.raises(ValueError, match="plan.*reserve.*materialize"):
            engorda_tables._validate_engorda_job(job)

        assert (
            engorda_tables._validate_engorda_job(dataclasses.replace(job, no_oracle=True)).name
            == "cdb_simplificado"
        )

    def test_cli_disables_schedule_pruning_and_main_forwards_it(self, monkeypatch):
        captured = []
        monkeypatch.setattr(engorda_tables, "executar_job", lambda job: captured.append(job))

        engorda_tables.main(
            [
                "--produto",
                "cdb_resgate",
                "--num-ifs",
                "123",
                "--meu-numero-prefix",
                "321",
                "--sem-poda-cronograma-resgate",
            ]
        )

        assert captured[0].poda_cronograma_resgate is False

    def test_cli_disables_account_pruning_and_k_adjustment(self, monkeypatch):
        captured = []
        monkeypatch.setattr(engorda_tables, "executar_job", lambda job: captured.append(job))

        engorda_tables.main(
            [
                "--produto",
                "lci",
                "--num-ifs",
                "123",
                "--sem-poda-conta",
                "--sem-ajuste-k",
            ]
        )

        assert captured[0].poda_conta is False
        assert captured[0].ajusta_fator_k is False

    def test_schedule_pruning_job_option_is_boolean(self):
        job = engorda_tables.EngordaJob(
            produto="cdb_simplificado",
            num_ifs=(1,),
            meu_numero_prefix="321",
            poda_cronograma_resgate="yes",
        )

        with pytest.raises(ValueError, match="poda_cronograma_resgate.*booleano"):
            engorda_tables._validate_engorda_job(job)

    @pytest.mark.parametrize("field", ["poda_conta", "ajusta_fator_k"])
    def test_new_job_options_are_boolean(self, field):
        job = engorda_tables.EngordaJob(
            produto="lci",
            num_ifs=(1,),
            **{field: "yes"},
        )

        with pytest.raises(ValueError, match=rf"{field}.*booleano"):
            engorda_tables._validate_engorda_job(job)

    def test_materialize_uses_artifacts_and_rejects_resampling(self):
        args = engorda_tables.parse_arguments(
            [
                "--phase",
                "materialize",
                "--produto",
                "cdb_simplificado",
                "--plan-uri",
                "oci://bucket@ns/run/plan",
                "--reservation-uri",
                "oci://bucket@ns/run/reservation",
                "--raw-uri",
                "oci://raw@ns/run/RAW",
                "--output-uri",
                "oci://out@ns/run/synthetic/cdb",
            ]
        )

        assert args.num_ifs is None
        assert args.n_instrumentos is None
        assert args.raw_uri == "oci://raw@ns/run/RAW"
        assert args.output_uri == "oci://out@ns/run/synthetic/cdb"

        with pytest.raises(SystemExit):
            engorda_tables.parse_arguments(
                [
                    "--phase",
                    "materialize",
                    "--produto",
                    "cdb_simplificado",
                    "--plan-uri",
                    "plan.json",
                    "--reservation-uri",
                    "reservation.json",
                    "--num-ifs",
                    "123",
                ]
            )

    def test_main_forwards_public_artifact_contract(self, monkeypatch):
        captured = []
        monkeypatch.setattr(engorda_tables, "executar_job", lambda job: captured.append(job))

        engorda_tables.main(
            [
                "--phase",
                "plan",
                "--produto",
                "cdb_simplificado",
                "--n-instrumentos",
                "2",
                "--plan-uri",
                "plan.json",
                "--raw-uri",
                "oci://raw@ns/exact",
                "--output-uri",
                "oci://out@ns/exact",
            ]
        )

        assert captured == [
            engorda_tables.EngordaJob(
                produto="cdb_simplificado",
                n_instrumentos=2,
                phase="plan",
                plan_uri="plan.json",
                raw_uri="oci://raw@ns/exact",
                output_uri="oci://out@ns/exact",
            )
        ]


class TestEngordaArtifacts:
    @staticmethod
    def _selected_lote(table_counts=None):
        table_counts = table_counts or {"INSTRUMENTO_FINANCEIRO": 1}
        snapshot = "oci://cfg@ns/run/plan.json.selected-lote/00000000-0000-4000-8000-000000000001"
        return {
            "artifact_type": engorda_tables.ENGORDA_SELECTED_LOTE_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_SELECTED_LOTE_SCHEMA_VERSION,
            "snapshot_id": "00000000-0000-4000-8000-000000000001",
            "snapshot_uri": snapshot,
            "table_set": sorted(table_counts),
            "tables": {
                table: {
                    "path": f"{snapshot}/tables/{table}",
                    "row_count": count,
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "NUM_IF"
                                if table == "INSTRUMENTO_FINANCEIRO"
                                else "NUM_ID_OPERACAO",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            },
                        ]
                        + (
                            [
                                {
                                    "name": "NUM_TIPO_IF",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {},
                                },
                            ]
                            if table == "INSTRUMENTO_FINANCEIRO"
                            else []
                        ),
                    },
                }
                for table, count in sorted(table_counts.items())
            },
            "selective_missing": {
                "present": False,
                "path": None,
                "row_count": 0,
                "schema": None,
            },
        }

    @staticmethod
    def _plan():
        body = {
            "artifact_type": engorda_tables.ENGORDA_PLAN_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_PLAN_SCHEMA_VERSION,
            "product": "cdb_simplificado",
            "selected_num_ifs": [10],
            "fator_k": 2,
            "seed": 42,
            "engorda_timestamp": "2026-08-18T10:00:00",
            "controle_operacional_date": "2026-08-18",
            "raw_uri": "oci://raw@ns/run/RAW",
            "output_uri": "oci://out@ns/run/synthetic/cdb",
            "specs_uri": "oci://cfg@ns/spec.json",
            "spec_sha256": "a" * 64,
            "faltantes_uri": "oci://cfg@ns/faltantes",
            "query_num_if_uri": "oci://cfg@ns/queries_produtos.sql",
            "selected_lote": TestEngordaArtifacts._selected_lote(),
            "tables": {
                "INSTRUMENTO_FINANCEIRO": {
                    "source_count": 1,
                    "synthetic_count": 2,
                    "pk": {
                        "rule": "OFFSET_PROPRIO",
                        "count_demand": 2,
                        "step": 1,
                        "minimum_start": 101,
                    },
                },
            },
            "cod_if": {"count": 2, "oracle_type": 49},
            "cod_operacao": {"count": 2},
            "meu_numero": {
                "strategy": "date_account_tos_shared_interval_v1",
                "operational_date": "2026-08-18",
                "normalization": "trim_strip_decimal_zeroes_v1",
                "tuple_count_demand": 4,
                "ordinal_count_demand": 3,
                "groups": [
                    {"group_id": "a" * 64, "count_demand": 3},
                    {"group_id": "b" * 64, "count_demand": 1},
                ],
            },
        }
        return {**body, "plan_id": engorda_tables._plan_id(body)}

    def test_local_json_is_deterministic_and_tamper_evident(self, tmp_path):
        path = tmp_path / "plan.json"
        plan = self._plan()

        engorda_tables._write_json_artifact(object(), str(path), plan)

        assert engorda_tables._read_json_artifact(object(), str(path)) == plan
        assert list(json.loads(path.read_text()).keys()) == sorted(plan)
        tampered = dict(plan, fator_k=3)
        with pytest.raises(ValueError, match="plan_id"):
            engorda_tables._validate_plan_artifact(tampered)
        tampered_snapshot = json.loads(json.dumps(plan))
        tampered_snapshot["selected_lote"]["tables"]["INSTRUMENTO_FINANCEIRO"]["row_count"] = 2
        with pytest.raises(ValueError, match="plan_id"):
            engorda_tables._validate_plan_artifact(tampered_snapshot)

    def test_plan_v1_requires_regeneration(self):
        old_plan = self._plan()
        old_body = {key: value for key, value in old_plan.items() if key != "plan_id"}
        old_body["schema_version"] = 1
        old_body.pop("selected_lote")
        old_plan = {**old_body, "plan_id": engorda_tables._plan_id(old_body)}

        with pytest.raises(ValueError, match="schema_version=1.*gere novamente"):
            engorda_tables._validate_plan_artifact(old_plan)

    def test_plan_v2_requires_spec_sha256(self):
        body = {key: value for key, value in self._plan().items() if key != "plan_id"}
        body["schema_version"] = 2
        body["meu_numero"] = {"ordinal_count_demand": 3}
        body.pop("spec_sha256")
        plan = {**body, "plan_id": engorda_tables._plan_id(body)}

        with pytest.raises(ValueError, match="spec_sha256"):
            engorda_tables._validate_plan_artifact(plan)

    def test_plan_v3_rejects_inconsistent_group_demands_after_hash_validation(self):
        body = {key: value for key, value in self._plan().items() if key != "plan_id"}
        body["meu_numero"]["tuple_count_demand"] = 5
        plan = {**body, "plan_id": engorda_tables._plan_id(body)}

        with pytest.raises(ValueError, match="tuple_count_demand diverge dos grupos"):
            engorda_tables._validate_plan_artifact(plan)

    def test_plan_builder_freezes_exact_public_demands(self):
        class CountFrame:
            def __init__(self, count):
                self._count = count

            def count(self):
                return self._count

        profile = engorda_tables.get_product_profile("cdb_simplificado")
        profile = dataclasses.replace(
            profile,
            business_keys=dataclasses.replace(profile.business_keys, operation=None),
        )
        plan = engorda_tables._build_engorda_plan(
            config={
                "DATAGEN_RAW_BASE_URI": "oci://raw@ns/run/RAW",
                "DATAGEN_RAW_PREFIX": "",
                "DATAGEN_SYNTHETIC_BASE_URI": "oci://out@ns",
                "DATAGEN_CLONE_PREFIX": "run/synthetic/cdb",
            },
            specs_uri="oci://cfg@ns/spec.json",
            spec_sha256="a" * 64,
            product_profile=profile,
            valores=[20, 10],
            fator_k=3,
            seed=7,
            engorda_ts=datetime(2026, 8, 18, 10, 11, 12, 123456),
            controle_operacional_date=date(2026, 8, 18),
            tipo_derivado=49,
            planos={
                "INSTRUMENTO_FINANCEIRO": engorda_tables.PlanoTabela(
                    name="INSTRUMENTO_FINANCEIRO",
                    pk_cols=("NUM_IF",),
                    pk_regra="OFFSET_PROPRIO",
                    pk_start=1000,
                    pk_passo=10,
                ),
            },
            lotes={"INSTRUMENTO_FINANCEIRO": CountFrame(2)},
            faltantes_uri="oci://cfg@ns/faltantes",
            query_num_if_uri="oci://cfg@ns/queries_produtos.sql",
            selected_lote=self._selected_lote({"INSTRUMENTO_FINANCEIRO": 2}),
        )

        assert plan["selected_num_ifs"] == [10, 20]
        assert plan["engorda_timestamp"] == "2026-08-18T10:11:12.123456"
        assert plan["query_num_if_uri"] == "oci://cfg@ns/queries_produtos.sql"
        assert plan["tables"]["INSTRUMENTO_FINANCEIRO"] == {
            "source_count": 2,
            "synthetic_count": 6,
            "pk": {
                "rule": "OFFSET_PROPRIO",
                "count_demand": 6,
                "step": 10,
                "minimum_start": 1000,
            },
        }
        assert plan["cod_if"] == {"count": 6, "oracle_type": 49}
        assert plan["cod_operacao"] == {"count": 0}
        assert plan["meu_numero"] == {
            "strategy": "date_account_tos_shared_interval_v1",
            "operational_date": "2026-08-18",
            "normalization": "trim_strip_decimal_zeroes_v1",
            "tuple_count_demand": 0,
            "ordinal_count_demand": 0,
            "groups": [],
        }
        assert "oracle_access" not in plan
        assert engorda_tables._validate_plan_artifact(plan) == plan

    def test_plan_builder_marks_no_oracle_artifact_as_offline(self):
        profile = engorda_tables.get_product_profile("cdb_simplificado")
        profile = dataclasses.replace(
            profile,
            business_keys=dataclasses.replace(profile.business_keys, operation=None),
        )

        plan = engorda_tables._build_engorda_plan(
            config={
                "DATAGEN_RAW_BASE_URI": "oci://raw@ns/run/RAW",
                "DATAGEN_RAW_PREFIX": "",
                "DATAGEN_SYNTHETIC_BASE_URI": "oci://out@ns",
                "DATAGEN_CLONE_PREFIX": "run/synthetic/cdb",
            },
            specs_uri="oci://cfg@ns/spec.json",
            spec_sha256="a" * 64,
            product_profile=profile,
            valores=[10],
            fator_k=1,
            seed=7,
            engorda_ts=datetime(2026, 8, 18, 10, 0),
            controle_operacional_date=date(2026, 8, 18),
            tipo_derivado=49,
            planos={
                "INSTRUMENTO_FINANCEIRO": engorda_tables.PlanoTabela(
                    "INSTRUMENTO_FINANCEIRO", ("NUM_IF",)
                )
            },
            lotes={"INSTRUMENTO_FINANCEIRO": type("Frame", (), {"count": lambda _self: 1})()},
            faltantes_uri=None,
            query_num_if_uri="oci://cfg@ns/queries_produtos.sql",
            selected_lote=self._selected_lote(),
            no_oracle=True,
        )

        assert plan["oracle_access"] == "disabled"
        assert engorda_tables._validate_plan_artifact(plan) == plan

    def test_offline_code_map_writes_deterministic_placeholders(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(
            engorda_tables,
            "_iter_oracle_code_batches",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("offline map called Oracle")
            ),
        )
        slots = spark.createDataFrame([(1, 100), (2, 101)], "ORDINAL long, NUM_IF_NOVO long")
        path = str(tmp_path / "offline-codes")

        mapping = engorda_tables._materialize_code_map(
            spark,
            slots,
            code_kind="COD_IF",
            generated_alias="COD_IF_GERADO",
            out_path=path,
            dry_run=False,
            offline=True,
            credentials=None,
            batch_size=10,
            engorda_date=date(2026, 8, 28),
            policy=engorda_tables.get_product_profile("cdb_simplificado").business_keys,
        )

        codes = [row.COD_IF_GERADO for row in mapping.orderBy("ORDINAL").collect()]
        assert codes == ["SYN10000001", "SYN10000002"]
        assert len(list((tmp_path / "offline-codes").glob("part-*.parquet"))) == 1

    def test_materialize_offline_marker_records_load_prohibition(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            engorda_tables,
            "_write_json_artifact",
            lambda _spark, uri, payload: captured.update(uri=uri, payload=payload),
        )

        engorda_tables._write_offline_artifact_marker(
            object(),
            "oci://bucket@ns/run/synthetic/cdb",
            "cdb_resgate",
            {"plan_id": "plan-123"},
        )

        assert captured == {
            "uri": (f"oci://bucket@ns/run/synthetic/cdb/{engorda_tables.OFFLINE_ARTIFACT_MARKER}"),
            "payload": {
                "artifact_type": "datagen_offline_synthetic",
                "schema_version": 1,
                "product": "cdb_resgate",
                "oracle_access": "disabled",
                "load_eligible": False,
                "plan_id": "plan-123",
            },
        }

    def test_plan_builder_uses_supplied_lote_counts_without_recounting_frames(self, spark):
        class NoCountFrame:
            def count(self):
                raise AssertionError("supplied lote counts must prevent frame.count()")

        profile = engorda_tables.get_product_profile("cdb_simplificado")
        operations = spark.createDataFrame(
            [
                (1, "100", "100.0", "4509"),
                (2, "100", "200", "4509.0"),
                (3, "300", "400", "4509"),
            ],
            "NUM_ID_OPERACAO long, NUM_CONTA_PARTICIPANTE_P1 string, "
            "NUM_CONTA_PARTICIPANTE_P2 string, NUM_ID_TIPO_OPER_OBJETO_SERV string",
        )
        planos = {
            "INSTRUMENTO_FINANCEIRO": engorda_tables.PlanoTabela(
                "INSTRUMENTO_FINANCEIRO", ("NUM_IF",)
            ),
            "OPERACAO": engorda_tables.PlanoTabela("OPERACAO", ("NUM_ID_OPERACAO",)),
        }
        plan = engorda_tables._build_engorda_plan(
            config={
                "DATAGEN_RAW_BASE_URI": "oci://raw@ns/run/RAW",
                "DATAGEN_RAW_PREFIX": "",
                "DATAGEN_SYNTHETIC_BASE_URI": "oci://out@ns",
                "DATAGEN_CLONE_PREFIX": "run/synthetic/cdb",
            },
            specs_uri="oci://cfg@ns/spec.json",
            spec_sha256="a" * 64,
            product_profile=profile,
            valores=[10, 20],
            fator_k=2,
            seed=7,
            engorda_ts=datetime(2026, 8, 20, 10, 0),
            controle_operacional_date=date(2026, 8, 20),
            tipo_derivado=49,
            planos=planos,
            lotes={
                "INSTRUMENTO_FINANCEIRO": NoCountFrame(),
                "OPERACAO": operations,
            },
            lote_counts={"INSTRUMENTO_FINANCEIRO": 2, "OPERACAO": 3},
            faltantes_uri=None,
            query_num_if_uri="oci://cfg@ns/queries_produtos.sql",
            selected_lote=self._selected_lote(
                {
                    "INSTRUMENTO_FINANCEIRO": 2,
                    "OPERACAO": 3,
                }
            ),
        )

        assert plan["tables"]["OPERACAO"]["source_count"] == 3
        assert plan["cod_operacao"] == {"count": 6}
        assert plan["meu_numero"]["tuple_count_demand"] == 12
        assert plan["meu_numero"]["ordinal_count_demand"] == 6

    def test_final_lote_counts_use_one_combined_action(self, spark, monkeypatch):
        frames = {
            "A": spark.createDataFrame([(1,), (2,)], "ID long"),
            "B": spark.createDataFrame([(1,), (2,), (3,)], "ID long"),
        }
        frame_class = type(frames["A"])
        original_collect = frame_class.collect
        collect_calls = 0

        def tracked_collect(frame):
            nonlocal collect_calls
            collect_calls += 1
            return original_collect(frame)

        monkeypatch.setattr(frame_class, "collect", tracked_collect)

        assert engorda_tables._count_final_lotes(frames) == {"A": 2, "B": 3}
        assert collect_calls == 1

    def test_active_closure_does_not_count_full_raw_source_for_logging(self, spark, monkeypatch):
        root = spark.createDataFrame([(1,)], "NUM_IF long")
        condition = spark.createDataFrame(
            [(11, 1, None), (12, 1, datetime(2026, 1, 1))],
            "NUM_CONDICAO_IF long, NUM_IF long, DAT_EXCLUSAO timestamp",
        )

        class RawSourceWithoutLoggingCount:
            def __init__(self, frame):
                self._frame = frame

            @property
            def columns(self):
                return self._frame.columns

            @property
            def schema(self):
                return self._frame.schema

            def where(self, predicate):
                return self._frame.where(predicate)

            def count(self):
                raise AssertionError("full RAW count was used only for logging")

        sources = {
            engorda_tables.TABELA_RAIZ: root,
            "CONDICAO_IF": RawSourceWithoutLoggingCount(condition),
        }
        monkeypatch.setattr(
            engorda_tables,
            "_read_source",
            lambda _spark, _config, table: sources[table],
        )
        plans = {
            engorda_tables.TABELA_RAIZ: engorda_tables.PlanoTabela(
                engorda_tables.TABELA_RAIZ, ("NUM_IF",)
            ),
            "CONDICAO_IF": engorda_tables.PlanoTabela(
                "CONDICAO_IF",
                ("NUM_CONDICAO_IF",),
                [
                    engorda_tables.FkRemap(
                        ("NUM_IF",),
                        engorda_tables.TABELA_RAIZ,
                        ("NUM_IF",),
                        True,
                    )
                ],
            ),
        }

        counts = {}
        lotes, provenances = engorda_tables._calcula_lotes_com_proveniencia(
            spark,
            {},
            {
                engorda_tables.TABELA_RAIZ: {"pk_cols": ["NUM_IF"]},
                "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
            },
            plans,
            [engorda_tables.TABELA_RAIZ, "CONDICAO_IF"],
            [1],
            3,
            somente_ativos=True,
            counts_out=counts,
        )

        assert lotes["CONDICAO_IF"].count() == 1
        assert counts == {
            engorda_tables.TABELA_RAIZ: 1,
            "CONDICAO_IF": 1,
        }
        for frame in [*lotes.values(), *provenances.values()]:
            frame.unpersist(blocking=False)

    def test_reservation_links_exact_counts_and_keeps_oracle_operation_allocator(self):
        plan = self._plan()
        reservation = {
            "artifact_type": engorda_tables.ENGORDA_RESERVATION_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_RESERVATION_SCHEMA_VERSION,
            "plan_id": plan["plan_id"],
            "product": "cdb_simplificado",
            "table_pks": {
                "INSTRUMENTO_FINANCEIRO": {
                    "start": 200,
                    "end": 201,
                    "count": 2,
                    "step": 1,
                },
            },
            "cod_operacao": {"strategy": "oracle_allocator", "count": 2},
            "meu_numero": {
                "strategy": "date_account_tos_shared_interval_v1",
                "operational_date": "2026-08-18",
                "group_ids": ["a" * 64, "b" * 64],
                "prefix": "321",
                "start": 50,
                "end": 52,
                "count": 3,
            },
        }

        validated = engorda_tables._validate_reservation_artifact(plan, reservation)
        plano = engorda_tables.PlanoTabela(
            name="INSTRUMENTO_FINANCEIRO",
            pk_cols=("NUM_IF",),
            pk_regra="OFFSET_PROPRIO",
            pk_start=101,
        )
        engorda_tables._inject_reserved_pk_starts({"INSTRUMENTO_FINANCEIRO": plano}, validated)

        assert plano.pk_start == 200
        assert validated["cod_operacao"] == {
            "strategy": "oracle_allocator",
            "count": 2,
        }

    def test_reservation_rejects_wrong_plan_or_count(self):
        plan = self._plan()
        reservation = {
            "artifact_type": engorda_tables.ENGORDA_RESERVATION_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_RESERVATION_SCHEMA_VERSION,
            "plan_id": "other",
            "product": "cdb_simplificado",
            "table_pks": {},
            "cod_operacao": {"strategy": "oracle_allocator", "count": 2},
            "meu_numero": {
                "strategy": "date_account_tos_shared_interval_v1",
                "operational_date": "2026-08-18",
                "group_ids": ["a" * 64, "b" * 64],
                "prefix": "321",
                "start": 1,
                "end": 3,
                "count": 3,
            },
        }
        with pytest.raises(ValueError, match="plan_id"):
            engorda_tables._validate_reservation_artifact(plan, reservation)

    def test_plan_v2_and_reservation_v1_keep_legacy_contract(self):
        body = {key: value for key, value in self._plan().items() if key != "plan_id"}
        body["schema_version"] = engorda_tables.ENGORDA_LEGACY_PLAN_SCHEMA_VERSION
        body["meu_numero"] = {
            "ordinal_count_demand": 3,
            "requested_prefix": "321",
        }
        plan = {**body, "plan_id": engorda_tables._plan_id(body)}
        reservation = {
            "artifact_type": engorda_tables.ENGORDA_RESERVATION_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_LEGACY_RESERVATION_SCHEMA_VERSION,
            "plan_id": plan["plan_id"],
            "product": plan["product"],
            "table_pks": {
                "INSTRUMENTO_FINANCEIRO": {
                    "start": 200,
                    "end": 201,
                    "count": 2,
                    "step": 1,
                },
            },
            "cod_operacao": {"strategy": "oracle_allocator", "count": 2},
            "meu_numero": {
                "prefix": "321",
                "start": 50,
                "end": 52,
                "count": 3,
            },
        }

        assert engorda_tables._validate_plan_artifact(plan) == plan
        assert engorda_tables._validate_reservation_artifact(plan, reservation) == reservation

        reservation_v2 = {
            **reservation,
            "schema_version": engorda_tables.ENGORDA_RESERVATION_SCHEMA_VERSION,
            "meu_numero": {
                "strategy": "legacy_global_v1",
                **reservation["meu_numero"],
            },
        }
        assert engorda_tables._validate_reservation_artifact(plan, reservation_v2) == reservation_v2

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("strategy", "global_v1", "strategy"),
            ("operational_date", "2026-08-19", "operational_date"),
            ("group_ids", ["a" * 64], "group_ids"),
        ],
    )
    def test_v3_reservation_rejects_group_scope_mismatch(self, field, value, message):
        plan = self._plan()
        reservation = {
            "artifact_type": engorda_tables.ENGORDA_RESERVATION_ARTIFACT,
            "schema_version": engorda_tables.ENGORDA_RESERVATION_SCHEMA_VERSION,
            "plan_id": plan["plan_id"],
            "product": plan["product"],
            "table_pks": {
                "INSTRUMENTO_FINANCEIRO": {
                    "start": 200,
                    "end": 201,
                    "count": 2,
                    "step": 1,
                },
            },
            "cod_operacao": {"strategy": "oracle_allocator", "count": 2},
            "meu_numero": {
                "strategy": "date_account_tos_shared_interval_v1",
                "operational_date": "2026-08-18",
                "group_ids": ["a" * 64, "b" * 64],
                "prefix": "321",
                "start": 50,
                "end": 52,
                "count": 3,
            },
        }
        reservation["meu_numero"][field] = value

        with pytest.raises(ValueError, match=message):
            engorda_tables._validate_reservation_artifact(plan, reservation)

    @pytest.mark.parametrize(
        ("plan_version", "reservation_version"),
        [(3, 1)],
    )
    def test_plan_and_reservation_schema_versions_cannot_be_crossed(
        self, plan_version, reservation_version
    ):
        plan = self._plan()
        plan = {**plan, "schema_version": plan_version}
        reservation = {
            "artifact_type": engorda_tables.ENGORDA_RESERVATION_ARTIFACT,
            "schema_version": reservation_version,
            "plan_id": plan["plan_id"],
            "product": plan["product"],
        }

        with pytest.raises(ValueError, match="schema_version"):
            engorda_tables._validate_reservation_artifact(plan, reservation)


class TestPaths:
    CONFIG = {
        "DATAGEN_RAW_BASE_URI": "oci://raw@ns",
        "DATAGEN_RAW_PREFIX": "datagen/raw",
        "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns",
        "DATAGEN_SYNTHETIC_PREFIX": "",
    }

    def test_table_path_name_strips_schema(self):
        assert engorda_tables.table_path_name("ADMIN.ORDERS") == "ORDERS"
        assert engorda_tables.table_path_name("ORDERS") == "ORDERS"

    def test_raw_path_with_prefix(self):
        assert engorda_tables.raw_path(self.CONFIG, "ORDERS") == "oci://raw@ns/datagen/raw/ORDERS"

    def test_raw_path_reduces_dotted_name(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ADMIN.ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_clone_base_uses_dedicated_default_prefix(self):
        assert (
            engorda_tables.clone_base_path(self.CONFIG) == "oci://syn@ns/sintetizacao_multiproduto"
        )

    def test_clone_base_uses_clone_prefix_not_legacy_synthetic_prefix(self):
        cfg = dict(
            self.CONFIG, DATAGEN_SYNTHETIC_PREFIX="ignored", DATAGEN_CLONE_PREFIX="clones/rdb"
        )
        assert engorda_tables.clone_base_path(cfg) == "oci://syn@ns/clones/rdb"


class TestGetEngordaEnv:
    def test_reads_required_and_normalizes(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_RAW_BASE_URI", "oci://raw@ns/")
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://syn@ns/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://cfg@ns/specs.json")
        monkeypatch.setenv("DATAGEN_RAW_PREFIX", "/datagen/raw/")
        monkeypatch.delenv("DATAGEN_SYNTHETIC_PREFIX", raising=False)
        config = engorda_tables.get_engorda_env()
        assert config["DATAGEN_RAW_BASE_URI"] == "oci://raw@ns"
        assert config["DATAGEN_RAW_PREFIX"] == "datagen/raw"
        assert config["DATAGEN_SYNTHETIC_PREFIX"] == ""
        assert config["DATAGEN_SPECS_URI"] == "oci://cfg@ns/specs.json"

    def test_exits_when_required_missing(self, monkeypatch):
        for name in engorda_tables.REQUIRED_ENV_VARS:
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(SystemExit):
            engorda_tables.get_engorda_env()

    def test_exact_raw_and_output_overrides_do_not_require_base_envs(self, monkeypatch):
        monkeypatch.delenv("DATAGEN_RAW_BASE_URI", raising=False)
        monkeypatch.delenv("DATAGEN_SYNTHETIC_BASE_URI", raising=False)
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://cfg@ns/spec.json")
        monkeypatch.setenv("DATAGEN_RAW_PREFIX", "ignored/raw")

        config = engorda_tables.get_engorda_env(
            raw_uri_override="oci://raw@ns/run/RAW/",
            output_uri_override="oci://out@ns/run/synthetic/cdb/",
        )

        assert config["DATAGEN_RAW_BASE_URI"] == "oci://raw@ns/run/RAW"
        assert config["DATAGEN_RAW_PREFIX"] == ""
        assert engorda_tables.clone_base_path(config) == ("oci://out@ns/run/synthetic/cdb")


class TestNormalizeSpecs:
    def test_reduces_keys_and_parent_table(self):
        raw = {
            "ADMIN.ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "foreign_keys": [{"columns": ["CUSTOMER_ID"], "parent_table": "ADMIN.CUSTOMERS"}],
            },
            "ADMIN.CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"], "static": True},
        }
        out = engorda_tables.normalize_specs(raw)
        assert set(out) == {"ORDERS", "CUSTOMERS"}
        assert out["ORDERS"]["foreign_keys"][0]["parent_table"] == "CUSTOMERS"

    def test_handles_fks_alias_key(self):
        raw = {
            "ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "fks": [{"columns": ["C_ID"], "parent_table": "X.CUSTOMERS"}],
            }
        }
        out = engorda_tables.normalize_specs(raw)
        assert out["ORDERS"]["foreign_keys"][0]["parent_table"] == "CUSTOMERS"
        assert "fks" not in out["ORDERS"]

    def test_rejects_collision(self):
        raw = {
            "A.ORDERS": {"pk_cols": ["ID"]},
            "B.ORDERS": {"pk_cols": ["ID"]},
        }
        with pytest.raises(ValueError):
            engorda_tables.normalize_specs(raw)

    def test_passes_through_when_no_schema(self):
        raw = {"ORDERS": {"pk_cols": ["ID"], "n_rows": 10, "foreign_keys": []}}
        assert engorda_tables.normalize_specs(raw) == raw


class TestPrincipalFkPlanning:
    def test_chain_has_principal_links_to_root(self, build_plan):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "ORDERS": {
                "pk_cols": ["OID"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
            "ITEMS": {
                "pk_cols": ["IID"],
                "foreign_keys": [
                    {"columns": ["OID"], "parent_table": "ORDERS", "parent_columns": ["OID"]}
                ],
            },
        }
        plans = build_plan(specs)
        assert set(plans) == set(specs)
        assert plans["ORDERS"].fks_remap == [
            engorda_tables.FkRemap(("NUM_IF",), "INSTRUMENTO_FINANCEIRO", ("NUM_IF",), True)
        ]
        assert plans["ITEMS"].fks_remap == [
            engorda_tables.FkRemap(("OID",), "ORDERS", ("OID",), True)
        ]
        assert engorda_tables.ordem_topologica(plans) == [
            "INSTRUMENTO_FINANCEIRO",
            "ORDERS",
            "ITEMS",
        ]

    def test_disconnected_tables_fail_together_instead_of_resampling(self, build_plan):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "B": {"pk_cols": ["ID"]},
            "C": {"pk_cols": ["ID"]},
        }
        with pytest.raises(ValueError) as error:
            build_plan(specs)
        assert "B: nenhuma FK" in str(error.value)
        assert "C: nenhuma FK" in str(error.value)

    def test_root_is_the_only_permitted_isolated_table(self, build_plan):
        plans = build_plan({"INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]}})
        assert list(plans) == ["INSTRUMENTO_FINANCEIRO"]
        assert plans["INSTRUMENTO_FINANCEIRO"].fks_remap == []
        assert plans["INSTRUMENTO_FINANCEIRO"].pk_regra == "OFFSET_PROPRIO"

    def test_fk_to_absent_parent_has_no_remap(self):
        specs = {
            "ORDERS": {
                "pk_cols": ["OID"],
                "foreign_keys": [
                    {"columns": ["CID"], "parent_table": "MISSING", "parent_columns": ["CID"]}
                ],
            },
            "OTHER": {"pk_cols": ["ID"]},
        }
        assert engorda_tables._fks_para_pais_clonados(specs, "ORDERS", set(specs)) == []


class TestTopoOrderTables:
    def _pos(self, order):
        return {t: i for i, t in enumerate(order)}

    def _plans(self, specs):
        return {
            table: engorda_tables.PlanoTabela(
                table,
                tuple(spec["pk_cols"]),
                [
                    engorda_tables.FkRemap(
                        tuple(fk["columns"]),
                        fk["parent_table"],
                        tuple(specs[fk["parent_table"]]["pk_cols"]),
                        False,
                    )
                    for fk in spec.get("foreign_keys", [])
                ],
            )
            for table, spec in specs.items()
        }

    def test_parents_before_children(self):
        specs = {
            "ITEMS": {
                "pk_cols": ["IID"],
                "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}],
            },
            "ORDERS": {
                "pk_cols": ["OID"],
                "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}],
            },
            "CUSTOMERS": {"pk_cols": ["CID"]},
        }
        pos = self._pos(engorda_tables.ordem_topologica(self._plans(specs)))
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_self_reference_ignored(self):
        specs = {
            "USUARIO": {
                "pk_cols": ["ID"],
                "foreign_keys": [{"columns": ["MGR"], "parent_table": "USUARIO"}],
            }
        }
        assert engorda_tables.ordem_topologica(self._plans(specs)) == ["USUARIO"]

    def test_cycle_is_broken_and_covers_all(self):
        specs = {
            "A": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["B"], "parent_table": "B"}]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["A"], "parent_table": "A"}]},
        }
        assert sorted(engorda_tables.ordem_topologica(self._plans(specs))) == ["A", "B"]


class TestTopologicalOrder:
    """Current cycle handling is deterministic and reports through logging."""

    def test_parents_before_children(self):
        deps = {"ITEMS": {"ORDERS"}, "ORDERS": {"CUSTOMERS"}, "CUSTOMERS": set()}
        order = engorda_tables._toposort_break_cycles(deps)
        pos = {t: i for i, t in enumerate(order)}
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_cycle_is_broken_and_warns(self, caplog):
        with caplog.at_level("WARNING", logger=engorda_tables.__name__):
            order = engorda_tables._toposort_break_cycles({"B": {"A"}, "A": {"B"}})
        assert order == ["A", "B"]
        assert len(caplog.records) == 1
        assert "Ciclo de FK" in caplog.records[0].message

    def test_acyclic_does_not_warn(self, caplog):
        with caplog.at_level("WARNING", logger=engorda_tables.__name__):
            assert engorda_tables._toposort_break_cycles({"P": set(), "C": {"P"}}) == ["P", "C"]
        assert caplog.records == []


class TestPkRuleClassification:
    @pytest.mark.parametrize("pk", [["NUM_IF"], ["NUM_IF", "SIDE"], ["SIDE", "NUM_IF"]])
    def test_shared_or_partially_shared_pk_follows_parent(self, build_plan, pk):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "CHILD": {
                "pk_cols": pk,
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
        }
        plans = build_plan(specs)
        assert plans["CHILD"].pk_regra == "VIA_PAI"
        assert plans["CHILD"].pk_start is None
        assert plans["CHILD"].pk_cols == tuple(pk)

    def test_ordinary_fk_keeps_independent_pk_offset(self, build_plan):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "CHILD": {
                "pk_cols": ["ID"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
        }
        child = build_plan(specs)["CHILD"]
        assert child.pk_regra == "OFFSET_PROPRIO"
        assert child.pk_start == 101


class TestCloneCardinality:
    def test_scales_every_source_row_by_integer_k(self, spark):
        source = spark.createDataFrame(
            [(1, "a", 10), (2, "b", 20)], "ID long, NAME string, AMOUNT long"
        )
        plan = engorda_tables.PlanoTabela("T", ("ID",), pk_regra="OFFSET_PROPRIO", pk_start=100)
        clones, mapping = engorda_tables.clona_tabela(spark, plan, source, 3, {})
        try:
            assert clones.count() == 6
            assert clones.select("ID").distinct().count() == 6
            assert [
                (r.NAME, r.AMOUNT, r["count"])
                for r in clones.groupBy("NAME", "AMOUNT").count().orderBy("NAME").collect()
            ] == [("a", 10, 3), ("b", 20, 3)]
            assert engorda_tables.valida_tabela({}, plan, clones, 2, 3) == []
        finally:
            mapping.unpersist()

    @pytest.mark.parametrize("factor", [0, 0.5, -1])
    def test_shrinking_or_fractional_clone_factor_is_rejected(self, factor):
        job = engorda_tables.EngordaJob(
            produto="cdb_simplificado",
            num_ifs=(1,),
            fator_k=factor,
            no_oracle=True,
            meu_numero_prefix="321",
        )
        with pytest.raises(ValueError, match="fator_k deve ser inteiro >= 1"):
            engorda_tables._validate_engorda_job(job)

    def test_domain_deficit_compensation_preserves_requested_volume(self):
        assert engorda_tables._ajusta_fator_k_por_dominio(3, 10, 4) == 8
        assert engorda_tables._ajusta_fator_k_por_dominio(3, None, 4) == 3
        assert engorda_tables._ajusta_fator_k_por_dominio(3, 4, 4) == 3

    def test_static_tables_are_excluded_not_resampled(self, build_plan):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "REF": {"pk_cols": ["ID"], "static": True, "n_rows": 999},
            "EXTRA": {"pk_cols": ["ID"]},
        }
        assert set(build_plan(specs, estaticas_extra={"EXTRA"})) == {"INSTRUMENTO_FINANCEIRO"}

    def test_empty_source_stays_empty(self, spark):
        source = spark.createDataFrame([], "ID long, VALUE string")
        plan = engorda_tables.PlanoTabela("T", ("ID",), pk_regra="OFFSET_PROPRIO", pk_start=100)
        clones, mapping = engorda_tables.clona_tabela(spark, plan, source, 3, {})
        try:
            assert clones.count() == 0
            assert mapping.count() == 0
            assert clones.schema == source.schema
            assert engorda_tables.valida_tabela({}, plan, clones, 0, 3) == []
        finally:
            mapping.unpersist()


class TestParseArguments:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv", ["engorda_tables.py", "--produto", "cdb_simplificado", "--num-ifs", "1"]
        )
        args = engorda_tables.parse_arguments()
        assert args.produto == "cdb_simplificado"
        assert args.num_ifs == [1]
        assert args.fator_k == 1
        assert args.seed == 42
        assert args.dry_run is False
        assert args.phase == "all"
        assert args.specs is None
        assert args.n_instrumentos is None
        assert args.pk_offset == 0
        assert args.pk_safety_band == 0
        assert args.pk_passo == 1

    def test_overrides(self, monkeypatch):
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "engorda_tables.py",
                "--produto",
                "rdb_inclusao",
                "--fator-k",
                "3",
                "--seed",
                "7",
                "--dry-run",
                "--n-instrumentos",
                "1000",
                "--pk-offset",
                "10000000000000",
                "--pk-safety-band",
                "1000000",
                "--specs",
                "oci://cfg@ns/s.json",
            ],
        )
        args = engorda_tables.parse_arguments()
        assert args.fator_k == 3
        assert args.seed == 7
        assert args.dry_run is True
        assert args.n_instrumentos == 1000
        assert args.num_ifs is None
        assert args.pk_offset == 10_000_000_000_000
        assert args.pk_safety_band == 1_000_000
        assert args.specs == "oci://cfg@ns/s.json"

    def test_rejects_non_positive_instrument_count(self, monkeypatch, capsys):
        monkeypatch.setattr(
            sys,
            "argv",
            ["engorda_tables.py", "--produto", "cdb_simplificado", "--n-instrumentos", "0"],
        )
        with pytest.raises(SystemExit):
            engorda_tables.parse_arguments()
        assert "argument --n-instrumentos" in capsys.readouterr().err


class TestReadParquet:
    class _DF:
        def __init__(self):
            self.limit_arg = None

        def limit(self, n):
            self.limit_arg = n
            return self

    class _Spark:
        def __init__(self, df):
            self._df = df

        @property
        def read(self):
            outer = self

            class _Reader:
                def parquet(self_inner, path):
                    return outer._df

            return _Reader()

    def test_no_limit_returns_full_df(self):
        df = self._DF()
        out = engorda_tables.read_parquet(self._Spark(df), "p")
        assert out is df and df.limit_arg is None

    def test_source_read_uses_exact_table_path_without_truncating(self):
        df = self._DF()
        paths = []
        spark = SimpleNamespace(read=SimpleNamespace(parquet=lambda path: paths.append(path) or df))
        out = engorda_tables._read_source(
            spark, {"DATAGEN_RAW_BASE_URI": "raw", "DATAGEN_RAW_PREFIX": "run"}, "ADMIN.T"
        )
        assert out is df and df.limit_arg is None
        assert paths == ["raw/run/T"]


class TestLoadSpecs:
    def _fake_spark(self, records):
        class _RDD:
            def collect(self_inner):
                return records

        class _SC:
            def wholeTextFiles(self_inner, uri):
                return _RDD()

        class _Spark:
            sparkContext = _SC()

        return _Spark()

    def test_loads_and_normalizes(self):
        content = json.dumps({"ADMIN.ORDERS": {"pk_cols": ["OID"]}})
        spark = self._fake_spark([("oci://cfg/specs.json", content)])
        specs = engorda_tables.load_specs(spark, "oci://cfg/specs.json")
        assert set(specs) == {"ORDERS"}

    def test_rejects_zero_records(self):
        spark = self._fake_spark([])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_multiple_records(self):
        spark = self._fake_spark([("a", "{}"), ("b", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/")

    def test_rejects_empty_dict(self):
        spark = self._fake_spark([("a", "{}")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")

    def test_rejects_malformed_json(self):
        spark = self._fake_spark([("a", "{not json")])
        with pytest.raises(ValueError):
            engorda_tables.load_specs(spark, "oci://cfg/specs.json")


class TestControleOperacionalDate:
    class _ResultSet:
        def __init__(self, values):
            self.values = iter(values)
            self.current = None
            self.closed = False

        def next(self):
            try:
                self.current = next(self.values)
                return True
            except StopIteration:
                return False

        def getDate(self, _index):
            return self.current

        def close(self):
            self.closed = True

    class _Statement:
        def __init__(self, values):
            self.result_set = TestControleOperacionalDate._ResultSet(values)
            self.closed = False

        def executeQuery(self):
            return self.result_set

        def close(self):
            self.closed = True

    class _Connection:
        def __init__(self, values):
            self.statement = TestControleOperacionalDate._Statement(values)
            self.sql = None
            self.closed = False

        def prepareStatement(self, sql):
            self.sql = sql
            return self.statement

        def close(self):
            self.closed = True

    def test_reads_exactly_one_operational_date(self, monkeypatch):
        connection = self._Connection(["2026-05-08"])
        monkeypatch.setattr(
            engorda_tables,
            "_open_oracle_connection",
            lambda *_args: connection,
        )

        result = engorda_tables._read_controle_operacional_date(
            object(), "jdbc:test", "user", "password"
        )

        assert result == date(2026, 5, 8)
        assert connection.sql == (
            "SELECT DAT_CTL_OPER FROM CETIP.CONTROLE_OPERACIONAL "
            "WHERE NUM_ORDEM = 0 AND NUM_SISTEMA IS NULL AND ROWNUM = 1"
        )
        assert connection.closed is True
        assert connection.statement.closed is True
        assert connection.statement.result_set.closed is True

    def test_rejects_missing_operational_date(self, monkeypatch):
        connection = self._Connection([])
        monkeypatch.setattr(
            engorda_tables,
            "_open_oracle_connection",
            lambda *_args: connection,
        )

        with pytest.raises(ValueError, match="CONTROLE_OPERACIONAL"):
            engorda_tables._read_controle_operacional_date(
                object(), "jdbc:test", "user", "password"
            )


class TestEngordaJobLifecycle:
    @pytest.fixture
    def runtime(self, monkeypatch):
        events, seen = [], {}
        spec = {"INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]}}

        def read_specs(path):
            events.append(("read", path))
            return SimpleNamespace(collect=lambda: [(path, json.dumps(spec))])

        spark = SimpleNamespace(
            sparkContext=SimpleNamespace(wholeTextFiles=read_specs),
            stop=lambda: events.append("stop"),
        )
        monkeypatch.delenv("DATAGEN_CLONE_PREFIX", raising=False)
        monkeypatch.setattr(engorda_tables, "create_spark_session", lambda _name: spark)
        monkeypatch.setattr(
            engorda_tables,
            "get_engorda_env",
            lambda *_args, **_kwargs: {
                "DATAGEN_RAW_BASE_URI": "raw",
                "DATAGEN_RAW_PREFIX": "",
                "DATAGEN_SYNTHETIC_BASE_URI": "synthetic",
                "DATAGEN_SPECS_URI": "spec.json",
            },
        )

        def clone(session, config, specs, **kwargs):
            assert session is spark
            events.append("clone")
            seen.update(config=config, specs=specs, **kwargs)
            return {"result": "ok"}

        monkeypatch.setattr(engorda_tables, "executa_clonagem", clone)
        job = engorda_tables.EngordaJob(
            produto="cdb_simplificado", num_ifs=(1,), no_oracle=True, meu_numero_prefix="321"
        )
        return job, events, seen

    def test_executes_one_entity_job_and_stops_session(self, runtime):
        job, events, seen = runtime
        assert engorda_tables.executar_job(job) == {"result": "ok"}
        assert events == [("read", "spec.json"), "clone", "stop"]
        assert seen["specs"] == {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"], "foreign_keys": []}
        }
        assert seen["product_profile"].name == "cdb_simplificado"
        assert (
            engorda_tables.clone_base_path(seen["config"])
            == "synthetic/sintetizacao_multiproduto/cdb_simplificado"
        )

    def test_failure_propagates_and_stops_session_without_retry(self, runtime, monkeypatch):
        job, events, _seen = runtime
        failure = RuntimeError("clone failed")

        def fail(*_args, **_kwargs):
            events.append("clone")
            raise failure

        monkeypatch.setattr(engorda_tables, "executa_clonagem", fail)
        with pytest.raises(RuntimeError) as error:
            engorda_tables.executar_job(job)
        assert error.value is failure
        assert events == [("read", "spec.json"), "clone", "stop"]

    def test_forwards_instrument_sample_instead_of_table_row_limit(self, runtime):
        job, _events, seen = runtime
        engorda_tables.executar_job(
            dataclasses.replace(job, num_ifs=None, n_instrumentos=5, fator_k=3, seed=7)
        )
        assert seen["num_ifs"] is None
        assert seen["n_instrumentos"] == 5
        assert seen["fator_k"] == 3
        assert seen["seed"] == 7

    def test_forwards_explicit_root_start_and_pk_step(self, runtime):
        job, _events, seen = runtime
        engorda_tables.executar_job(dataclasses.replace(job, offset_num_if=10000, pk_passo=5))
        assert seen["offset_num_if"] == 10000
        assert seen["pk_passo"] == 5
        assert seen["num_ifs"] == [1]

    def test_forwards_pk_floor_and_safety_band(self, runtime):
        job, _events, seen = runtime
        engorda_tables.executar_job(
            dataclasses.replace(job, pk_offset=10**13, pk_safety_band=1000000)
        )
        assert seen["pk_offset"] == 10**13
        assert seen["pk_safety_band"] == 1000000


class TestPlannedPkStarts:
    SPECS = {"INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]}}

    def test_capacity_warning_does_not_silently_shrink_safety_band(self, build_plan, caplog):
        types = engorda_tables.T
        schema = types.StructType([types.StructField("NUM_IF", types.DecimalType(3, 0))])
        with caplog.at_level("WARNING", logger=engorda_tables.__name__):
            plan = build_plan(
                self.SPECS,
                schemas={"INSTRUMENTO_FINANCEIRO": schema},
                maxes={"INSTRUMENTO_FINANCEIRO": 26},
                pk_band=1_000_000,
            )
        assert engorda_tables._pk_capacity_of(schema["NUM_IF"].dataType) == 999
        assert plan["INSTRUMENTO_FINANCEIRO"].pk_start == 1_000_027
        assert len(caplog.records) == 1
        assert "cap 999" in caplog.records[0].message

    def test_static_is_excluded_and_composite_pk_requires_parent_mapping(self, build_plan):
        specs = {
            **self.SPECS,
            "REF": {"pk_cols": ["ID"], "static": True},
            "CHILD": {
                "pk_cols": ["NUM_IF", "SIDE"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
        }
        plans = build_plan(specs, pk_floor=1000)
        assert set(plans) == {"INSTRUMENTO_FINANCEIRO", "CHILD"}
        assert plans["INSTRUMENTO_FINANCEIRO"].pk_start == 1001
        assert plans["CHILD"].pk_regra == "VIA_PAI"
        assert plans["CHILD"].pk_start is None

    def test_no_floor_starts_above_true_max(self, build_plan):
        plans = build_plan(self.SPECS, maxes={"INSTRUMENTO_FINANCEIRO": 8_000_000_000})
        assert plans["INSTRUMENTO_FINANCEIRO"].pk_start == 8_000_000_001

    def test_safety_band_added_above_true_max(self, build_plan):
        plans = build_plan(
            self.SPECS, maxes={"INSTRUMENTO_FINANCEIRO": 8_000_000_000}, pk_band=1_000_000
        )
        assert plans["INSTRUMENTO_FINANCEIRO"].pk_start == 8_001_000_001

    def test_floor_wins_over_band_when_higher(self, build_plan):
        plans = build_plan(self.SPECS, pk_floor=10**13, pk_band=1_000_000)
        assert plans["INSTRUMENTO_FINANCEIRO"].pk_start == 10**13 + 1

    def test_unreadable_max_aborts_instead_of_omitting_table(self, build_plan):
        with pytest.raises(
            ValueError, match=r"INSTRUMENTO_FINANCEIRO:.*max\(NUM_IF\).*Parquet completo"
        ):
            build_plan(self.SPECS, maxes={"INSTRUMENTO_FINANCEIRO": None})


class TestWriteSyntheticTable:
    @pytest.mark.parametrize("readback_count", [2, 1])
    def test_scoped_delete_append_and_exact_readback(self, monkeypatch, readback_count):
        events = []
        path = "oci://syn@ns/synthetic/CONDICAO_IF"

        class FakeWriter:
            def mode(self, m):
                events.append(("mode", m))
                return self

            def parquet(self, path):
                events.append(("write", path))

        class FakeDF:
            rdd = SimpleNamespace(getNumPartitions=lambda: 1)

            def count(self):
                return 2

            @property
            def write(self):
                return FakeWriter()

        df = FakeDF()
        monkeypatch.setattr(
            engorda_tables, "_delete_path", lambda _spark, path: events.append(("delete", path))
        )

        def readback(path):
            events.append(("read", path))
            return SimpleNamespace(count=lambda: readback_count)

        spark = SimpleNamespace(read=SimpleNamespace(parquet=readback))
        if readback_count == 2:
            engorda_tables.escreve_tabela(spark, df, path)
        else:
            with pytest.raises(ValueError, match="readback Parquet.*1 linha.*esperado 2"):
                engorda_tables.escreve_tabela(spark, df, path)
        assert events == [("delete", path), ("mode", "append"), ("write", path), ("read", path)]


pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("engorda-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _schedule_guard_sources(spark):
    conditions = spark.createDataFrame(
        [
            (
                root,
                100 + root,
                "21" if root == 14 else "20",
                "2020-01-01" if root == 12 else None,
            )
            for root in range(1, 17)
        ],
        "NUM_IF long, NUM_CONDICAO_IF long, COD_TIPO_CONDICAO_IF string, DAT_EXCLUSAO string",
    )
    redemptions = spark.createDataFrame(
        [
            (
                100 + root,
                "SEM TABELA" if root == 10 else ("  com tabela  " if root == 11 else "COM TABELA"),
                "2020-01-01" if root == 13 else None,
            )
            for root in range(1, 17)
        ],
        "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_EXCLUSAO string",
    )
    schedules = spark.createDataFrame(
        [
            (101, "2026-01-01", "50", None),
            (102, "2026-01-01", "50", "S"),
            (103, "not-a-date", "50", None),
            (104, "2026-01-01", "not-a-number", None),
            (105, None, "50", None),
            (106, "2026-01-01", None, None),
            (107, "2026-01-01", "NaN", None),
            (108, "2026-01-01", "Infinity", None),
            (109, "2026-01-01", "150.5", None),
            (110, "bad", "bad", None),
            (111, "2026-01-01", "100", None),
            (115, "2026-01-01", "10", None),
            (115, "bad", "20", None),
        ],
        "NUM_CONDICAO_IF long, DAT_RESGATE string, VAL_PERCENTUAL string, IND_EXCLUIDO string",
    )
    return {
        engorda_tables.CONDICAO_IF_TABLE: conditions,
        engorda_tables.RESGATE_TABELA: redemptions,
        engorda_tables.CRONOGRAMA_TABELA: schedules,
    }


def test_schedule_guard_rejects_only_active_type20_com_tabela_defects(spark, monkeypatch):
    sources = _schedule_guard_sources(spark)
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: sources[table],
    )
    domain = spark.createDataFrame([(root,) for root in range(1, 17)], "NUM_IF long")

    invalid = engorda_tables._num_if_cronograma_resgate_invalido(spark, {}, domain)

    assert {row.NUM_IF for row in invalid.collect()} == {2, 3, 4, 5, 6, 7, 8, 15, 16}


def test_rdb_schedule_guard_requires_exact_com_tabela_variant(spark, monkeypatch):
    sources = _schedule_guard_sources(spark)
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: sources[table],
    )
    domain = spark.createDataFrame([(root,) for root in range(1, 17)], "NUM_IF long")

    invalid = engorda_tables._num_if_cronograma_resgate_invalido(
        spark, {}, domain, required_mode="COM TABELA"
    )

    assert {row.NUM_IF for row in invalid.collect()} == (set(range(1, 17)) - {1, 9})


@pytest.fixture
def event_family_sources(spark):
    return {
        "HISTORICO_PU_CURVA": spark.createDataFrame(
            [(1, 1, "2024-01-01"), (2, 2, "2024-01-01")],
            "NUM_HISTORICO_PU_CURVA long, NUM_IF long, DAT_HISTORICO_VALORES string",
        ),
        engorda_tables.EVENTO_TABELA: spark.createDataFrame(
            [(101, 1, "83"), (102, 2, "83")],
            "NUM_EVENTO long, NUM_IF long, NUM_TIPO_EVENTO_LEGADO string",
        ),
        engorda_tables.CONDICAO_IF_TABLE: spark.createDataFrame(
            [(1, 11, "3"), (2, 22, "20")],
            "NUM_IF long, NUM_CONDICAO_IF long, COD_TIPO_CONDICAO_IF string",
        ),
        "JUROS_FLUTUANTE": spark.createDataFrame([(11,)], "NUM_CONDICAO_IF long"),
        engorda_tables.RESGATE_TABELA: spark.createDataFrame([(22,)], "NUM_CONDICAO_IF long"),
    }


@pytest.mark.parametrize(
    "product",
    [
        "cdb_simplificado",
        "cdb_resgate",
        "cdb_escalonamento",
        "rdb_inclusao",
        "rdb_resgate",
    ],
)
def test_product_prunes_event_without_condition_family(
    spark, monkeypatch, event_family_sources, product
):
    domain = spark.createDataFrame([(1,), (2,)], "NUM_IF long")
    monkeypatch.setattr(
        engorda_tables,
        "_dominio_num_if_produto",
        lambda *_args, **_kwargs: domain,
    )
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: event_family_sources[table],
    )

    _, valid = engorda_tables._dominio_instrumentos_elegiveis(
        spark,
        {},
        {},
        engorda_tables.get_product_profile(product),
        poda_subtipo=False,
        poda_cronograma_resgate=False,
        poda_conta=False,
        politica_estrita_operacao=False,
    )

    assert [row.NUM_IF for row in valid.orderBy("NUM_IF").collect()] == [1]


@pytest.mark.parametrize(
    "table,column",
    [
        ("EVENTO", "NUM_TIPO_EVENTO_LEGADO"),
        ("CONDICAO_IF", "COD_TIPO_CONDICAO_IF"),
        ("JUROS_FLUTUANTE", "NUM_CONDICAO_IF"),
        ("RESGATE", "NUM_CONDICAO_IF"),
    ],
)
@pytest.mark.parametrize("failure", ["unreadable", "missing_column"])
def test_event_family_guard_fails_closed(
    spark, monkeypatch, event_family_sources, table, column, failure
):
    sources = event_family_sources
    sources["EVENTO"] = sources["EVENTO"].unionByName(
        spark.createDataFrame([(103, 2, "85")], sources["EVENTO"].schema)
    )

    def read_source(_spark, _config, name):
        if name == table:
            if failure == "unreadable":
                raise OSError("source unavailable")
            return sources[name].drop(column)
        return sources[name]

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)
    domain = spark.createDataFrame([(1,), (2,)], "NUM_IF long")
    with pytest.raises(ValueError, match=table):
        engorda_tables._num_if_evento_sem_familia(spark, {}, domain).collect()


@pytest.mark.parametrize(
    "event_type,condition_type,subtype",
    [
        ("83", "3", "JUROS_FLUTUANTE"),
        ("85", "20", "RESGATE"),
    ],
)
def test_event_family_guard_matches_validator_and_reads_only_needed_family(
    spark, monkeypatch, event_type, condition_type, subtype
):
    from scripts import validate_products as validator

    sources = {
        "EVENTO": spark.createDataFrame(
            [
                (root, str(root), event_type, "2020-01-01" if root == 7 else " ")
                for root in range(1, 9)
            ],
            "NUM_EVENTO long, NUM_IF string, NUM_TIPO_EVENTO_LEGADO string, DAT_EXCLUSAO string",
        ),
        "CONDICAO_IF": spark.createDataFrame(
            [
                (
                    str(root) + ".000",
                    str(root + 10),
                    condition_type + ".0",
                    "2020-01-01" if root == 4 else " ",
                )
                for root in (1, 3, 4, 5, 6)
            ],
            "NUM_IF string, NUM_CONDICAO_IF string, COD_TIPO_CONDICAO_IF string, "
            "DAT_EXCLUSAO string",
        ),
        subtype: spark.createDataFrame(
            [(str(root + 10) + ".000", "2020-01-01" if root == 5 else "") for root in (1, 4, 5, 6)],
            "NUM_CONDICAO_IF string, DAT_EXCLUSAO string",
        ),
    }
    reads = []

    def read_source(_spark, _config, table):
        reads.append(table)
        return sources[table]

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)
    # Root 8 is outside the selected domain; root 6's family cannot serve root 2.
    domain = spark.createDataFrame([(root,) for root in range(1, 8)], "NUM_IF long")
    invalid = engorda_tables._num_if_evento_sem_familia(spark, {}, domain)
    assert {row.NUM_IF for row in invalid.collect()} == {2, 3, 4, 5}
    assert set(reads) == set(sources)

    sources["EVENTO"] = sources["EVENTO"].where("NUM_IF != '8'")
    (finding,) = validator.check_event_condition_families(
        sources, sample=10, profile=validator.get_validation_profile("cdb")
    )
    assert finding.count == 4
    assert {int(row[1]) for row in finding.sample} == {2, 3, 4, 5}


def test_event_family_guard_needs_no_conditions_without_relevant_events(spark, monkeypatch):
    events = spark.createDataFrame(
        [(1, "83", "2020-01-01"), (1, "99", None), (2, "85", None)],
        "NUM_IF long, NUM_TIPO_EVENTO_LEGADO string, DAT_EXCLUSAO string",
    )
    reads = []

    def read_source(_spark, _config, table):
        reads.append(table)
        assert table == "EVENTO"
        return events

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)
    domain = spark.createDataFrame([(1,)], "NUM_IF long")
    assert engorda_tables._num_if_evento_sem_familia(spark, {}, domain).count() == 0
    assert reads == ["EVENTO"]


def test_cdb_event_family_selection_refills_and_clones_valid_graph(
    spark, monkeypatch, event_family_sources
):
    from scripts import validate_products as validator

    sources = event_family_sources
    domain = spark.createDataFrame([(1,), (2,)], "NUM_IF long")
    monkeypatch.setattr(engorda_tables, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain)
    monkeypatch.setattr(
        engorda_tables, "_read_source", lambda _spark, _config, table: sources[table]
    )
    profile = engorda_tables.get_product_profile("cdb_resgate")
    options = dict(
        poda_subtipo=False,
        poda_cronograma_resgate=False,
        poda_conta=False,
        politica_estrita_operacao=False,
    )
    selected = engorda_tables.seleciona_instrumentos(spark, {}, {}, None, 1, 42, profile, **options)
    assert selected == [1]
    with pytest.raises(ValueError, match=r"PODADOS.*2"):
        engorda_tables.seleciona_instrumentos(spark, {}, {}, [2], None, 42, profile, **options)

    roots = domain.where(domain.NUM_IF.isin(selected))
    conditions = sources["CONDICAO_IF"].join(roots, "NUM_IF", "left_semi")
    lots = {
        "INSTRUMENTO_FINANCEIRO": roots,
        "CONDICAO_IF": conditions,
        "EVENTO": sources["EVENTO"].join(roots, "NUM_IF", "left_semi"),
        "JUROS_FLUTUANTE": sources["JUROS_FLUTUANTE"].join(
            conditions, "NUM_CONDICAO_IF", "left_semi"
        ),
    }
    root_fk = engorda_tables.FkRemap(("NUM_IF",), "INSTRUMENTO_FINANCEIRO", ("NUM_IF",), True)
    plans = [
        engorda_tables.PlanoTabela(
            "INSTRUMENTO_FINANCEIRO", ("NUM_IF",), pk_regra="OFFSET_PROPRIO", pk_start=100
        ),
        engorda_tables.PlanoTabela(
            "CONDICAO_IF", ("NUM_CONDICAO_IF",), [root_fk], "OFFSET_PROPRIO", 200
        ),
        engorda_tables.PlanoTabela("EVENTO", ("NUM_EVENTO",), [root_fk], "OFFSET_PROPRIO", 300),
        engorda_tables.PlanoTabela(
            "JUROS_FLUTUANTE",
            ("NUM_CONDICAO_IF",),
            [
                engorda_tables.FkRemap(
                    ("NUM_CONDICAO_IF",), "CONDICAO_IF", ("NUM_CONDICAO_IF",), True
                )
            ],
            "VIA_PAI",
        ),
    ]
    cloned, mappings = {}, {}
    for plan in plans:
        cloned[plan.name], mappings[plan.name] = engorda_tables.clona_tabela(
            spark, plan, lots[plan.name], 2, mappings
        )
    events = cloned["EVENTO"].collect()
    assert len(events) == 2
    assert len({row.NUM_IF for row in events}) == 2
    assert all(row.NUM_IF >= 100 and row.NUM_EVENTO >= 300 for row in events)
    (finding,) = validator.check_event_condition_families(
        cloned, sample=5, profile=validator.get_validation_profile("cdb")
    )
    assert finding.passed
    assert finding.count == 0


def test_schedule_guard_refills_sampling_but_rejects_explicit_invalid_root(spark, monkeypatch):
    sources = _schedule_guard_sources(spark)
    sources["EVENTO"] = spark.createDataFrame([], "NUM_IF long, NUM_TIPO_EVENTO_LEGADO string")
    domain = spark.createDataFrame([(root,) for root in range(1, 17)], "NUM_IF long")
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: sources[table],
    )
    monkeypatch.setattr(
        engorda_tables,
        "_dominio_num_if_produto",
        lambda *_args, **_kwargs: domain,
    )
    profile = dataclasses.replace(
        engorda_tables.get_product_profile("cdb_resgate"),
        integrity=engorda_tables.IntegrityPolicy(),
    )

    selected = engorda_tables.seleciona_instrumentos(
        spark, {}, {}, None, 7, 42, profile, poda_subtipo=False, poda_conta=False
    )
    assert selected == [1, 9, 10, 11, 12, 13, 14]

    with pytest.raises(ValueError, match=r"PODADOS.*2"):
        engorda_tables.seleciona_instrumentos(
            spark, {}, {}, [2], None, 42, profile, poda_subtipo=False, poda_conta=False
        )


def test_schedule_guard_fails_closed_when_required_source_is_unavailable(spark, monkeypatch):
    sources = _schedule_guard_sources(spark)

    def read_source(_spark, _config, table):
        if table == engorda_tables.RESGATE_TABELA:
            raise OSError("missing")
        return sources[table]

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)
    domain = spark.createDataFrame([(1,)], "NUM_IF long")

    with pytest.raises(ValueError, match=r"exige a fonte RESGATE"):
        engorda_tables._num_if_cronograma_resgate_invalido(spark, {}, domain)


def test_schedule_guard_is_scoped_to_resgate_products():
    assert engorda_tables.PRODUTOS_COM_PODA_CRONOGRAMA_RESGATE == {
        "cdb_resgate",
        "rdb_resgate",
    }


def test_post_closure_schedule_pruning_still_removes_sem_tabela_rows(spark):
    lotes = {
        engorda_tables.CONDICAO_IF_TABLE: spark.createDataFrame(
            [(1, "20"), (2, "20")],
            "NUM_CONDICAO_IF long, COD_TIPO_CONDICAO_IF string",
        ),
        engorda_tables.RESGATE_TABELA: spark.createDataFrame(
            [(1, " sem tabela "), (2, "COM TABELA")],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string",
        ),
        engorda_tables.CRONOGRAMA_TABELA: spark.createDataFrame(
            [(10, 1), (20, 2)],
            "NUM_ID_CONDICAO_RESGATE long, NUM_CONDICAO_IF long",
        ),
    }

    removed = engorda_tables._poda_cronograma_sem_tabela(lotes)

    assert removed == 1
    assert [row.NUM_CONDICAO_IF for row in lotes[engorda_tables.CRONOGRAMA_TABELA].collect()] == [2]


def test_post_closure_schedule_parent_matches_validator_case_and_type(spark):
    lotes = {
        engorda_tables.CONDICAO_IF_TABLE: spark.createDataFrame(
            [
                (1, "20", None),
                (2, "21", None),
                (3, "20.0", None),
                (4, "20.00", None),
                (5, "20", "2026-01-01"),
                (6, "20", None),
            ],
            "NUM_CONDICAO_IF long, COD_TIPO_CONDICAO_IF string, DAT_EXCLUSAO string",
        ),
        engorda_tables.RESGATE_TABELA: spark.createDataFrame(
            [
                (1, "com tabela", None),
                (2, "COM TABELA", None),
                (3, "COM TABELA", None),
                (4, "COM TABELA", None),
                (5, "COM TABELA", None),
                (6, "COM TABELA", "2026-01-01"),
            ],
            "NUM_CONDICAO_IF long, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        ),
        engorda_tables.CRONOGRAMA_TABELA: spark.createDataFrame(
            [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5), (60, 6)],
            "NUM_ID_CONDICAO_RESGATE long, NUM_CONDICAO_IF long",
        ),
    }

    assert engorda_tables._poda_cronograma_sem_tabela(lotes) == 5
    assert [row.NUM_CONDICAO_IF for row in lotes[engorda_tables.CRONOGRAMA_TABELA].collect()] == [3]


@pytest.fixture
def pu_history_sources(spark):
    table = "HISTORICO_PU_CURVA"
    sources = {
        engorda_tables.TABELA_RAIZ: spark.createDataFrame([(1,), (2,)], "NUM_IF long"),
        table: spark.createDataFrame(
            [
                (10, 1, "2024-01-02 02:00:00", 999.0),
                (21, 1, "2024-01-02T01:00:00", 123.0),
                (20, 1, "2024-01-02T01:00:00", -5.0),
                (1, 1, None, 1.0),
                (2, 1, "invalid", 2.0),
                (30, 2, "2023-12-31", None),
                (29, 2, "2024-01-01", 29.0),
            ],
            "NUM_HISTORICO_PU_CURVA long, NUM_IF long, "
            "DAT_HISTORICO_VALORES string, VAL_PU_CURVA double",
        ),
    }
    plans = {
        engorda_tables.TABELA_RAIZ: engorda_tables.PlanoTabela(
            engorda_tables.TABELA_RAIZ, ("NUM_IF",), pk_regra="OFFSET_PROPRIO", pk_start=100
        ),
        table: engorda_tables.PlanoTabela(
            table,
            ("NUM_HISTORICO_PU_CURVA",),
            [engorda_tables.FkRemap(("NUM_IF",), engorda_tables.TABELA_RAIZ, ("NUM_IF",), True)],
            "OFFSET_PROPRIO",
            200,
        ),
    }
    yield sources, plans
    for frame in sources.values():
        frame.unpersist(blocking=False)


@pytest.fixture
def pu_history_domain(spark, monkeypatch, pu_history_sources):
    sources, plans = pu_history_sources
    root, table = engorda_tables.TABELA_RAIZ, "HISTORICO_PU_CURVA"
    sources[root] = spark.createDataFrame([(i,) for i in range(1, 7)], "NUM_IF long")
    sources[table] = sources[table].unionByName(
        spark.createDataFrame(
            [(40, 4, None, 1.0), (50, 5, "invalid", 1.0), (60, 6, "", 1.0)],
            sources[table].schema,
        )
    )
    reads = []

    def read_source(_spark, _config, name):
        reads.append(name)
        return sources[name]

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)
    monkeypatch.setattr(
        engorda_tables, "_dominio_num_if_produto", lambda *_args, **_kwargs: sources[root]
    )
    # Isolate unrelated domain policies, not the history filter or selection path.
    for name in ("_num_if_evento_sem_familia", "_num_if_lote_sem_lastro"):
        monkeypatch.setattr(engorda_tables, name, lambda *_args: sources[root].limit(0))
    options = dict(
        poda_subtipo=False,
        poda_cronograma_resgate=False,
        poda_conta=False,
        politica_estrita_operacao=False,
    )
    return sources, plans, reads, options


@pytest.mark.parametrize(
    "product",
    [
        "rdb_inclusao",
        "rdb_resgate",
        "lci",
        "ccb_pppre",
        "ccb_pfpre",
        "ccb_pgrpre",
        "ccb_favcp",
        "ccb_fapre",
    ],
)
def test_pu_history_domain_filters_before_sampling(pu_history_domain, spark, product, caplog):
    _, _, reads, options = pu_history_domain
    with caplog.at_level("INFO", logger=engorda_tables.logger.name):
        selected = engorda_tables.seleciona_instrumentos(
            spark, {}, {}, None, 2, 42, engorda_tables.get_product_profile(product), **options
        )
    assert selected == [1, 2]
    assert reads == ["HISTORICO_PU_CURVA"]
    assert (
        "Poda de domínio [historico PU curva sem data utilizavel]: 4 instrumento(s) removido(s)"
        in caplog.text
    )


@pytest.mark.parametrize(
    "product", ["cdb_simplificado", "cdb_resgate", "cdb_escalonamento", "lca", "gravame"]
)
def test_pu_history_domain_does_not_read_history_outside_allowlist(
    pu_history_domain, spark, product
):
    _, _, reads, options = pu_history_domain
    selected = engorda_tables.seleciona_instrumentos(
        spark, {}, {}, None, 6, 42, engorda_tables.get_product_profile(product), **options
    )
    assert selected == list(range(1, 7))
    assert reads == []


@pytest.mark.parametrize(
    "failure",
    ["missing_table", "unreadable", "NUM_IF", "NUM_HISTORICO_PU_CURVA", "DAT_HISTORICO_VALORES"],
)
def test_pu_history_domain_fails_closed(pu_history_domain, spark, monkeypatch, failure):
    sources, _, _, options = pu_history_domain
    table = "HISTORICO_PU_CURVA"
    if failure == "missing_table":
        sources.pop(table)
    elif failure == "unreadable":

        def unreadable(*_args):
            raise OSError("RAW unavailable")

        monkeypatch.setattr(engorda_tables, "_read_source", unreadable)
    else:
        sources[table] = sources[table].drop(failure)
    with pytest.raises(ValueError, match="rdb_inclusao.*HISTORICO_PU_CURVA"):
        engorda_tables.seleciona_instrumentos(
            spark,
            {},
            {},
            None,
            2,
            42,
            engorda_tables.get_product_profile("rdb_inclusao"),
            **options,
        )


@pytest.mark.parametrize("root", [3, 4, 5, 6])
def test_pu_history_domain_rejects_explicit_ineligible_root(pu_history_domain, spark, root):
    _, _, _, options = pu_history_domain
    with pytest.raises(ValueError, match=rf"PODADOS.*{root}"):
        engorda_tables.seleciona_instrumentos(
            spark,
            {},
            {},
            [1, root],
            None,
            42,
            engorda_tables.get_product_profile("rdb_inclusao"),
            permitir_lote_menor=True,
            **options,
        )


def test_pu_history_domain_preserves_deficit_policy(pu_history_domain, spark):
    sources, _, _, options = pu_history_domain
    profile = engorda_tables.get_product_profile("rdb_inclusao")
    with pytest.raises(ValueError, match="tem só 2 instrumento"):
        engorda_tables.seleciona_instrumentos(spark, {}, {}, None, 5, 42, profile, **options)
    selected = engorda_tables.seleciona_instrumentos(
        spark, {}, {}, None, 5, 42, profile, permitir_lote_menor=True, **options
    )
    assert selected == [1, 2]
    assert engorda_tables._ajusta_fator_k_por_dominio(2, 5, len(selected)) == 5
    sources["HISTORICO_PU_CURVA"] = sources["HISTORICO_PU_CURVA"].where("NUM_IF > 2")
    with pytest.raises(ValueError, match="tem só 0 instrumento"):
        engorda_tables.seleciona_instrumentos(
            spark, {}, {}, None, 5, 42, profile, permitir_lote_menor=True, **options
        )


@pytest.mark.parametrize("product", ["rdb_inclusao", "rdb_resgate"])
@pytest.mark.parametrize("key_type", ["long", "decimal(38,10)"])
def test_pu_history_domain_live_sampling_300_roots_selects_200(
    pu_history_domain, spark, monkeypatch, product, key_type
):
    sources, plans, reads, options = pu_history_domain
    root, table = engorda_tables.TABELA_RAIZ, "HISTORICO_PU_CURVA"
    sources[root] = spark.createDataFrame([(i,) for i in range(1, 301)], "NUM_IF long")
    sources[table] = spark.createDataFrame(
        [(i, i, "2024-01-01", -5.0) for i in range(1, 291)]
        + [(i + 1000, i, "2024-01-02", 99.0) for i in range(1, 291)]
        + [(i, i, [None, "invalid", ""][i % 3], 1.0) for i in range(291, 300)],
        sources[table].schema,
    )
    for name in (root, table):
        sources[name] = sources[name].withColumn(
            "NUM_IF", engorda_tables.F.col("NUM_IF").cast(key_type)
        )
    spec = {
        root: {"pk_cols": ["NUM_IF"], "foreign_keys": []},
        table: {
            "pk_cols": ["NUM_HISTORICO_PU_CURVA"],
            "foreign_keys": [
                {"columns": ["NUM_IF"], "parent_table": root, "parent_columns": ["NUM_IF"]}
            ],
        },
    }
    candidates = []
    closure = engorda_tables._calcula_lotes_com_proveniencia

    def capture_closure(*args, **kwargs):
        candidates.extend(args[5])
        return closure(*args, **kwargs)

    monkeypatch.setattr(engorda_tables, "_calcula_lotes_com_proveniencia", capture_closure)
    selection = engorda_tables.seleciona_instrumentos_destino(
        spark,
        {},
        spec,
        None,
        200,
        42,
        engorda_tables.get_product_profile(product),
        plans,
        list(plans),
        3,
        existing_key_lookup=lambda *_args: pytest.fail("No external FK expected"),
        produto=product,
        retain_provenance=True,
        **options,
    )
    try:
        assert len(selection.values) == 200
        assert set(selection.values) <= set(range(1, 291))
        assert sorted(candidates) == list(range(1, 291))
        assert selection.lote_counts == {root: 200, table: 200}
        rows = selection.lotes[table].collect()
        assert {row.NUM_IF for row in rows} == set(selection.values)
        assert {(row.DAT_HISTORICO_VALORES, row.VAL_PU_CURVA) for row in rows} == {
            ("2024-01-01", -5.0)
        }
        assert reads.count(table) == 2  # Once for domain filtering, once for closure.
    finally:
        for frame in (*selection.lotes.values(), *selection.provenances.values()):
            frame.unpersist(blocking=False)


@pytest.mark.parametrize(
    "product",
    [
        "rdb_inclusao",
        "rdb_resgate",
        "lci",
        "ccb_pppre",
        "ccb_pfpre",
        "ccb_pgrpre",
        "ccb_favcp",
        "ccb_fapre",
    ],
)
def test_pu_history_pruning_uses_earliest_date_then_pk(pu_history_sources, product):
    sources, plans = pu_history_sources
    assert engorda_tables.get_product_profile(product).name == product
    assert "HISTORICO_PU_CURVA" in engorda_tables.TABELAS_ENGORDA_POR_PRODUTO[product]
    original = sources["HISTORICO_PU_CURVA"].persist()
    expected = original.where("NUM_HISTORICO_PU_CURVA IN (20, 30)").orderBy("NUM_IF").collect()

    assert engorda_tables._poda_historico_pu_curva(sources, plans, product) == 5

    kept = sources["HISTORICO_PU_CURVA"]
    assert kept.schema == original.schema
    assert kept.orderBy("NUM_IF").collect() == expected
    assert not original.is_cached
    assert kept.is_cached


@pytest.mark.parametrize(
    "product", ["cdb_simplificado", "cdb_resgate", "cdb_escalonamento", "lca", "gravame", None]
)
def test_pu_history_pruning_is_noop_outside_allowlist(pu_history_sources, product):
    sources, _ = pu_history_sources
    original = dict(sources)
    assert engorda_tables._poda_historico_pu_curva(sources, {}, product) is None
    assert all(sources[table] is frame for table, frame in original.items())
    assert engorda_tables._poda_historico_pu_curva({}, {}, product) is None


@pytest.mark.parametrize("date_type", ["date", "timestamp"])
def test_pu_history_pruning_preserves_typed_dates_and_existing_exclusion_semantics(
    spark, pu_history_sources, date_type
):
    sources, plans = pu_history_sources
    convert = date.fromisoformat if date_type == "date" else datetime.fromisoformat
    sources["HISTORICO_PU_CURVA"] = spark.createDataFrame(
        [
            (11, 1, convert("2024-01-02"), None),
            (12, 1, convert("2024-01-01"), "2024-01-03"),
            (20, 2, None, None),
            (21, 2, convert("2024-01-01"), None),
        ],
        f"NUM_HISTORICO_PU_CURVA long, NUM_IF long, DAT_HISTORICO_VALORES {date_type}, "
        "DAT_EXCLUSAO string",
    )
    original = sources["HISTORICO_PU_CURVA"]
    assert engorda_tables._poda_historico_pu_curva(sources, plans, "rdb_inclusao") == 2
    kept = sources["HISTORICO_PU_CURVA"]
    assert kept.schema == original.schema
    assert (
        kept.orderBy("NUM_IF").collect()
        == original.where("NUM_HISTORICO_PU_CURVA IN (12, 21)").orderBy("NUM_IF").collect()
    )


@pytest.mark.parametrize(
    "failure",
    [
        "missing_plan",
        "wrong_pk",
        "missing_fk",
        "missing_table",
        "NUM_IF",
        "NUM_HISTORICO_PU_CURVA",
        "DAT_HISTORICO_VALORES",
    ],
)
def test_pu_history_pruning_requires_metadata_and_columns(pu_history_sources, failure):
    sources, plans = pu_history_sources
    table = "HISTORICO_PU_CURVA"
    if failure == "missing_plan":
        plans.pop(table)
    elif failure == "wrong_pk":
        plans[table] = dataclasses.replace(plans[table], pk_cols=("NUM_IF",))
    elif failure == "missing_fk":
        plans[table] = dataclasses.replace(plans[table], fks_remap=[])
    elif failure == "missing_table":
        sources.pop(table)
    else:
        sources[table] = sources[table].drop(failure)
    with pytest.raises(ValueError, match="rdb_inclusao.*HISTORICO_PU_CURVA"):
        engorda_tables._poda_historico_pu_curva(sources, plans, "rdb_inclusao")


@pytest.mark.parametrize("history_date", [None, "invalid", ""])
def test_pu_history_pruning_fails_for_each_root_without_usable_history(
    spark, pu_history_sources, history_date
):
    sources, plans = pu_history_sources
    table = "HISTORICO_PU_CURVA"
    sources[table] = (
        sources[table]
        .where("NUM_IF = 1")
        .unionByName(spark.createDataFrame([(30, 2, history_date, 1.0)], sources[table].schema))
    )
    original = dict(sources)
    with pytest.raises(ValueError, match=r"HISTORICO_PU_CURVA.*utilizavel.*\[2\]"):
        engorda_tables._poda_historico_pu_curva(sources, plans, "rdb_inclusao")
    assert all(sources[table] is frame for table, frame in original.items())


def test_pu_history_pruning_requires_history_for_nonempty_roots_only(pu_history_sources):
    sources, plans = pu_history_sources
    table = "HISTORICO_PU_CURVA"
    sources[table] = sources[table].where("NUM_IF = 1")
    with pytest.raises(ValueError, match=r"HISTORICO_PU_CURVA.*\[2\]"):
        engorda_tables._poda_historico_pu_curva(sources, plans, "rdb_inclusao")
    sources[table] = sources[table].limit(0)
    with pytest.raises(ValueError, match=r"HISTORICO_PU_CURVA.*\[1, 2\]"):
        engorda_tables._poda_historico_pu_curva(sources, plans, "rdb_inclusao")
    sources[engorda_tables.TABELA_RAIZ] = sources[engorda_tables.TABELA_RAIZ].limit(0)
    sources.pop(table)
    assert engorda_tables._poda_historico_pu_curva(sources, {}, "rdb_inclusao") is None


@pytest.mark.parametrize("somente_ativos", [True, False])
def test_pu_history_closure_provenance_clone_and_plan_counts(
    spark, monkeypatch, pu_history_sources, somente_ativos
):
    sources, plans = pu_history_sources
    table = "HISTORICO_PU_CURVA"
    monkeypatch.setattr(engorda_tables, "_read_source", lambda _spark, _config, name: sources[name])
    counts, mappings = {}, {}
    lots, provenance = engorda_tables._calcula_lotes_com_proveniencia(
        spark,
        {},
        {},
        plans,
        list(plans),
        [1, 2],
        3,
        produto="rdb_inclusao",
        counts_out=counts,
        somente_ativos=somente_ativos,
    )
    try:
        assert counts == {engorda_tables.TABELA_RAIZ: 2, table: 2}
        assert {tuple(row) for row in provenance[table].collect()} == {(20, 1), (30, 2)}
        assert {
            row.NUM_IF: row["count"] for row in lots[table].groupBy("NUM_IF").count().collect()
        } == {
            1: 1,
            2: 1,
        }
        plan = engorda_tables._build_engorda_plan(
            config={"DATAGEN_RAW_BASE_URI": "raw", "DATAGEN_SYNTHETIC_BASE_URI": "synthetic"},
            specs_uri="spec.json",
            spec_sha256="a" * 64,
            product_profile=engorda_tables.get_product_profile("rdb_inclusao"),
            valores=[1, 2],
            fator_k=3,
            seed=42,
            engorda_ts=datetime(2026, 9, 8),
            controle_operacional_date=date(2026, 9, 8),
            tipo_derivado=51,
            planos=plans,
            lotes=lots,
            lote_counts=counts,
            faltantes_uri=None,
            query_num_if_uri="queries.sql",
            selected_lote={},
        )
        assert plan["tables"][table]["source_count"] == 2
        assert plan["tables"][table]["synthetic_count"] == 6
        assert plan["tables"][table]["pk"]["count_demand"] == 6
        for name in plans:
            cloned, mappings[name] = engorda_tables.clona_tabela(
                spark, plans[name], lots[name], 3, mappings
            )
            assert cloned.count() == plan["tables"][name]["synthetic_count"]
        rows = cloned.collect()
        assert len({row.NUM_IF for row in rows}) == 6
        assert len({row.NUM_HISTORICO_PU_CURVA for row in rows}) == 6
        assert {(row.DAT_HISTORICO_VALORES, row.VAL_PU_CURVA) for row in rows} == {
            ("2024-01-02T01:00:00", -5.0),
            ("2023-12-31", None),
        }
    finally:
        for frame in (*lots.values(), *provenance.values(), *mappings.values()):
            frame.unpersist(blocking=False)


def test_pu_history_pruning_precedes_live_fk_admission(spark, monkeypatch, pu_history_sources):
    sources, plans = pu_history_sources
    root, table = engorda_tables.TABELA_RAIZ, "HISTORICO_PU_CURVA"
    sources[table] = sources[table].withColumn(
        "LOOKUP_ID",
        engorda_tables.F.when(
            engorda_tables.F.col("NUM_HISTORICO_PU_CURVA").isin(20, 30), 10
        ).otherwise(999),
    )
    spec = {
        root: {"pk_cols": ["NUM_IF"], "foreign_keys": [], "static": False},
        table: {
            "pk_cols": ["NUM_HISTORICO_PU_CURVA"],
            "foreign_keys": [
                {"columns": ["NUM_IF"], "parent_table": root, "parent_columns": ["NUM_IF"]},
                {"columns": ["LOOKUP_ID"], "parent_table": "LOOKUP", "parent_columns": ["ID"]},
            ],
            "static": False,
        },
        "LOOKUP": {"pk_cols": ["ID"], "foreign_keys": [], "static": True},
    }
    monkeypatch.setattr(engorda_tables, "_read_source", lambda _spark, _config, name: sources[name])
    monkeypatch.setattr(
        engorda_tables,
        "_dominio_instrumentos_elegiveis",
        lambda *_args, **_kwargs: (sources[root], sources[root]),
    )
    lookups = []

    def lookup(parent, columns, keys, _nulls):
        assert parent == "LOOKUP"
        assert columns == ("ID",)
        lookups.extend(keys)
        return {("10",)}

    selection = engorda_tables.seleciona_instrumentos_destino(
        spark,
        {},
        spec,
        num_ifs=[1, 2],
        n_instrumentos=None,
        seed=42,
        profile=engorda_tables.get_product_profile("rdb_inclusao"),
        planos=plans,
        ordem=list(plans),
        max_passadas=3,
        existing_key_lookup=lookup,
        produto="rdb_inclusao",
        retain_provenance=True,
    )
    try:
        assert selection.values == [1, 2]
        assert lookups == [("10",)]
        assert selection.lote_counts == {root: 2, table: 2}
        assert {row.NUM_HISTORICO_PU_CURVA for row in selection.lotes[table].collect()} == {20, 30}
    finally:
        for frame in (*selection.lotes.values(), *selection.provenances.values()):
            frame.unpersist(blocking=False)


def test_sampled_domain_deficit_adjusts_k_but_empty_domain_still_fails(spark, monkeypatch):
    profile = engorda_tables.get_product_profile("lci")
    domain = spark.createDataFrame([(1,), (2,)], "NUM_IF long")
    monkeypatch.setattr(engorda_tables, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain)
    history = spark.createDataFrame(
        [(1, 1, "2024-01-01"), (2, 2, "2024-01-01")],
        "NUM_HISTORICO_PU_CURVA long, NUM_IF long, DAT_HISTORICO_VALORES string",
    )

    def read_source(_spark, _config, table):
        if table == "HISTORICO_PU_CURVA":
            return history
        raise OSError(table)

    monkeypatch.setattr(engorda_tables, "_read_source", read_source)

    selected = engorda_tables.seleciona_instrumentos(
        spark,
        {},
        {},
        None,
        5,
        42,
        profile,
        poda_subtipo=False,
        poda_cronograma_resgate=False,
        poda_conta=False,
        permitir_lote_menor=True,
    )

    assert selected == [1, 2]
    assert engorda_tables._ajusta_fator_k_por_dominio(2, 5, len(selected)) == 5
    assert engorda_tables._ajusta_fator_k_por_dominio(2, None, 1) == 2

    empty = spark.createDataFrame([], "NUM_IF long")
    monkeypatch.setattr(engorda_tables, "_dominio_num_if_produto", lambda *_args, **_kwargs: empty)
    with pytest.raises(ValueError, match=r"tem só 0 instrumento"):
        engorda_tables.seleciona_instrumentos(
            spark,
            {},
            {},
            None,
            5,
            42,
            profile,
            poda_subtipo=False,
            poda_cronograma_resgate=False,
            poda_conta=False,
            permitir_lote_menor=True,
        )


def _account_sources(spark):
    return {
        engorda_tables.CONTA_PARTICIPANTE_TABELA: spark.createDataFrame(
            [
                ("100.00", "1.0", "12345.40-1"),
                ("101", "2", "12345.40-2"),
                ("102", "1", "INVALID"),
                ("103", "1", "12345.10-3"),
            ],
            "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
            "COD_CONTA_PARTICIPANTE string",
        ),
        engorda_tables.V_FAMILIA_CONTAS_TABELA: spark.createDataFrame(
            [("12345.40-1", "1.0", "L"), ("12345.10-3", "2", "L")],
            "COD_CONTA_MEMBRO string, NUM_ID_AREA_ATUACAO string, COD_TIPO_ACESSO string",
        ),
        "TITULO": spark.createDataFrame(
            [(1, "100.0"), (2, "101"), (3, ""), (4, None)],
            "NUM_IF long, NUM_CONTA_PARTICIPANTE string",
        ),
        "DEPOSITO_AUTOMATICO_IF": spark.createDataFrame(
            [(5, "102")], "NUM_IF long, NUM_CONTA_PARTICIPANTE string"
        ),
        "OPERACAO": spark.createDataFrame(
            [(6, "103", None), (7, "100", None)],
            "NUM_IF long, NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string",
        ),
    }


def test_account_pruning_uses_full_family_and_validator_canonicalization(spark, monkeypatch):
    sources = _account_sources(spark)
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: sources[table],
    )
    domain = spark.createDataFrame([(root,) for root in range(1, 8)], "NUM_IF long")

    excluded = engorda_tables._num_if_conta_nao_elegivel(spark, {}, domain)

    assert {row.NUM_IF for row in excluded.collect()} == {2, 3, 5, 6}


def test_account_pruning_falls_back_partially_and_fails_open(spark, monkeypatch):
    sources = _account_sources(spark)
    domain = spark.createDataFrame([(6,), (7,)], "NUM_IF long")

    def without_family(_spark, _config, table):
        if table == engorda_tables.V_FAMILIA_CONTAS_TABELA:
            raise OSError("view unavailable")
        return sources[table]

    monkeypatch.setattr(engorda_tables, "_read_source", without_family)
    assert engorda_tables._num_if_conta_nao_elegivel(spark, {}, domain).count() == 0

    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda *_args: (_ for _ in ()).throw(OSError("raw unavailable")),
    )
    assert engorda_tables._num_if_conta_nao_elegivel(spark, {}, domain).count() == 0


def test_account_pruning_scope_includes_cdb_variants_lci_and_lca():
    assert engorda_tables.PRODUTOS_COM_PODA_CONTA == {
        "cdb_simplificado",
        "cdb_resgate",
        "cdb_escalonamento",
        "lci",
        "lca",
    }


def test_meu_numero_uses_reserved_ordinal_interval(spark):
    operation = spark.createDataFrame(
        [
            (
                1,
                datetime(2020, 1, 1),
                "100",
                "100",
                "old-p1",
                "old-p2",
                7,
            )
        ],
        "NUM_ID_OPERACAO long, DAT_OPERACAO timestamp, "
        "NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string, "
        "NUM_CONTROLE_LANCAMENTO_P1 string, "
        "NUM_CONTROLE_LANCAMENTO_P2 string, "
        "NUM_ID_TIPO_OPER_OBJETO_SERV long",
    )

    row = engorda_tables._generate_meu_numeros(
        operation,
        "321",
        date(2026, 8, 18),
        ordinal_start=50,
        ordinal_end=51,
    ).first()

    assert row.NUM_CONTROLE_LANCAMENTO_P1 == "3210000050"
    assert row.NUM_CONTROLE_LANCAMENTO_P2 == "3210000051"


def _grouped_meu_operations(spark):
    return spark.createDataFrame(
        [
            (1, datetime(2020, 1, 1), "100.000", "200", "old-1", "old-2", "4509.0"),
            (2, datetime(2020, 1, 1), "100", "100.0", "old-3", "old-4", "4509"),
        ],
        "NUM_ID_OPERACAO long, DAT_OPERACAO timestamp, "
        "NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string, "
        "NUM_CONTROLE_LANCAMENTO_P1 string, NUM_CONTROLE_LANCAMENTO_P2 string, "
        "NUM_ID_TIPO_OPER_OBJETO_SERV string",
    )


def test_grouped_meu_descriptor_freezes_tuple_and_shared_interval_demand(spark):
    descriptor = engorda_tables._grouped_meu_numero_descriptor(
        _grouped_meu_operations(spark),
        fator_k=2,
        operational_date=date(2026, 8, 18),
        requested_prefix="321",
    )

    assert descriptor == {
        "strategy": "date_account_tos_shared_interval_v1",
        "operational_date": "2026-08-18",
        "normalization": "trim_strip_decimal_zeroes_v1",
        "tuple_count_demand": 8,
        "ordinal_count_demand": 6,
        "groups": [
            {
                "group_id": "22b6f17eb4da2c54916fc874a20999cd75c324edf0941548f75b1e853e5171bf",
                "count_demand": 6,
            },
            {
                "group_id": "5e8f10f411a7639b4199e8f35988f2831e8518fa4cee229668e69e28a7c6f1ee",
                "count_demand": 2,
            },
        ],
        "requested_prefix": "321",
    }


def test_grouped_meu_descriptor_is_empty_when_operation_count_is_zero(spark):
    descriptor = engorda_tables._grouped_meu_numero_descriptor(
        _grouped_meu_operations(spark).limit(0),
        fator_k=3,
        operational_date=date(2026, 8, 18),
        operation_count=0,
    )

    assert descriptor["tuple_count_demand"] == 0
    assert descriptor["ordinal_count_demand"] == 0
    assert descriptor["groups"] == []


def test_meu_preflight_is_required_only_for_positive_ordinal_demand():
    assert not engorda_tables._meu_preflight_required({"ordinal_count_demand": 0})
    assert engorda_tables._meu_preflight_required({"ordinal_count_demand": 1})


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("NUM_CONTA_PARTICIPANTE_P1", "  "),
        ("NUM_CONTA_PARTICIPANTE_P2", None),
        ("NUM_ID_TIPO_OPER_OBJETO_SERV", ""),
    ],
)
def test_grouped_meu_descriptor_rejects_blank_group_fields(spark, column, value):
    operations = _grouped_meu_operations(spark).withColumn(column, engorda_tables.F.lit(value))

    with pytest.raises(ValueError, match="conta/TOS nulo ou vazio"):
        engorda_tables._grouped_meu_numero_descriptor(
            operations,
            fator_k=1,
            operational_date=date(2026, 8, 18),
        )


@pytest.mark.parametrize("attach_business_codes", [False, True])
def test_grouped_meu_generation_reuses_ordinals_only_across_complete_tuples(
    spark, tmp_path, monkeypatch, attach_business_codes
):
    operations = _grouped_meu_operations(spark)
    descriptor = engorda_tables._grouped_meu_numero_descriptor(
        operations,
        fator_k=1,
        operational_date=date(2026, 8, 18),
    )
    if attach_business_codes:
        operations = (
            operations.withColumn(
                "NUM_ID_OPERACAO", engorda_tables.F.col("NUM_ID_OPERACAO").cast("decimal(38,10)")
            )
            .withColumn("NUM_IF", engorda_tables.F.col("NUM_ID_OPERACAO"))
            .withColumn("COD_IF", engorda_tables.F.lit("old-if"))
            .withColumn("COD_OPERACAO", engorda_tables.F.lit("old-operation"))
            .localCheckpoint(eager=True)
        )
        instruments = spark.createDataFrame(
            [(1, "CDB00000001"), (2, "CDB00000002")], "NUM_IF long, COD_IF string"
        ).localCheckpoint(eager=True)
        operations = engorda_tables._propagate_root_cod_if(instruments, operations)
        mapping_path = str(tmp_path / "operation_codes")
        spark.createDataFrame(
            [(1, "000000000001"), (2, "000000000002")],
            "NUM_ID_OPERACAO_NOVO long, COD_OPERACAO_GERADO string",
        ).write.parquet(mapping_path)
        operations = engorda_tables._attach_generated_code(
            operations,
            spark.read.parquet(mapping_path),
            pk_col="NUM_ID_OPERACAO",
            new_pk_alias="NUM_ID_OPERACAO_NOVO",
            code_col="COD_OPERACAO",
            generated_alias="COD_OPERACAO_GERADO",
        )
        original_join = engorda_tables.DataFrame.join

        def join_with_independent_keys(left, right, on=None, how=None):
            # Local Spark can repair this identity clash; the Data Flow analyzer did not.
            if on == "NUM_ID_OPERACAO" and how == "inner":
                left_attrs = left._jdf.queryExecution().analyzed().outputSet()
                right_attrs = right._jdf.queryExecution().analyzed().outputSet()
                assert left_attrs.intersect(right_attrs).isEmpty(), (
                    "Meu-number maps must not share operation expression IDs"
                )
            return original_join(left, right, on, how)

        monkeypatch.setattr(engorda_tables.DataFrame, "join", join_with_independent_keys)
    generated = engorda_tables._generate_grouped_meu_numeros(
        operations,
        "321",
        date(2026, 8, 18),
        descriptor,
        ordinal_start=50,
        ordinal_end=52,
    )
    rows = {row.NUM_ID_OPERACAO: row for row in generated.collect()}

    assert generated.count() == len(rows) == 2
    assert generated.columns == operations.columns
    if attach_business_codes:
        assert [(rows[i].COD_IF, rows[i].COD_OPERACAO) for i in (1, 2)] == [
            ("CDB00000001", "000000000001"),
            ("CDB00000002", "000000000002"),
        ]
    assert rows[1].NUM_CONTROLE_LANCAMENTO_P1 == "3210000050"
    assert rows[1].NUM_CONTROLE_LANCAMENTO_P2 == "3210000050"
    assert rows[2].NUM_CONTROLE_LANCAMENTO_P1 == "3210000051"
    assert rows[2].NUM_CONTROLE_LANCAMENTO_P2 == "3210000052"
    assert rows[2].NUM_CONTROLE_LANCAMENTO_P1 != rows[2].NUM_CONTROLE_LANCAMENTO_P2
    tuples = engorda_tables._flatten_meu_tuples(generated)
    assert tuples.count() == tuples.dropDuplicates().count() == 4
    assert [field.dataType for field in generated.schema] == [
        field.dataType for field in operations.schema
    ]
    assert {row.DAT_OPERACAO.date() for row in rows.values()} == {date(2026, 8, 18)}


def test_grouped_meu_preflight_allows_control_reuse_for_another_account(spark):
    operations = _grouped_meu_operations(spark)
    descriptor = engorda_tables._grouped_meu_numero_descriptor(
        operations,
        fator_k=1,
        operational_date=date(2026, 8, 18),
    )
    generated = engorda_tables._generate_grouped_meu_numeros(
        operations,
        "321",
        date(2026, 8, 18),
        descriptor,
        ordinal_start=50,
        ordinal_end=52,
    )
    first = engorda_tables._flatten_meu_tuples(generated).first()
    existing = spark.createDataFrame(
        [
            (
                first.DAT_OPERACAO,
                "999",
                first.NUM_CONTROLE_LANCAMENTO,
                first.NUM_ID_TIPO_OPER_OBJETO_SERV,
            )
        ],
        engorda_tables._flatten_meu_tuples(generated).schema,
    )

    engorda_tables._assert_no_meu_collisions(generated, existing)

    exact = spark.createDataFrame(
        [tuple(first)], engorda_tables._flatten_meu_tuples(generated).schema
    )
    with pytest.raises(ValueError, match="colisão"):
        engorda_tables._assert_no_meu_collisions(generated, exact)


def test_grouped_meu_generation_fails_closed_on_frozen_group_mismatch(spark):
    operations = _grouped_meu_operations(spark)
    descriptor = engorda_tables._grouped_meu_numero_descriptor(
        operations,
        fator_k=1,
        operational_date=date(2026, 8, 18),
    )
    descriptor["groups"][0]["count_demand"] += 1

    with pytest.raises(ValueError, match="grupos.*plano congelado"):
        engorda_tables._generate_grouped_meu_numeros(
            operations,
            "321",
            date(2026, 8, 18),
            descriptor,
            ordinal_start=50,
            ordinal_end=53,
        )


class TestEngordaDateRules:
    ENGORDA_TS = datetime(2026, 8, 8, 10, 19, 6, 340_000)
    OPERATIONAL_DATE = date(2026, 5, 8)

    @pytest.mark.parametrize(
        "original_situation",
        [datetime(2020, 1, 1), datetime(2030, 1, 1), None],
        ids=["past-situation", "future-situation", "null-situation"],
    )
    def test_instrument_uses_operational_date_and_preserves_term(self, spark, original_situation):
        original_emission = datetime(2024, 1, 10)
        original_maturity = datetime(2025, 2, 20)
        original_term = (original_maturity.date() - original_emission.date()).days
        df = spark.createDataFrame(
            [
                (
                    original_emission,
                    original_maturity,
                    original_situation,
                    datetime(2024, 1, 11),
                    datetime(2024, 1, 12),
                    datetime(2024, 1, 13),
                    datetime(2024, 1, 14),
                    datetime(2024, 1, 15),
                    datetime(2024, 1, 16),
                    datetime(2024, 1, 17),
                )
            ],
            (
                "DAT_EMISSAO timestamp, DAT_VENCIMENTO timestamp, "
                "DAT_SITUACAO_IF timestamp, "
                "DAT_REGISTRO timestamp, DAT_VAL_NOMINAL_EM timestamp, "
                "DAT_ULTIMA_CORRECAO timestamp, DAT_PU_CURVA timestamp, "
                "DAT_VAL_NOMINAL_EM_ORIG timestamp, "
                "DAT_FATOR_JUR_FLUT_ACUM_CDB timestamp, "
                "DAT_ATUALIZACAO_REGISTRO timestamp"
            ),
        )

        out, applied = engorda_tables.aplica_regras_engorda(
            df,
            "INSTRUMENTO_FINANCEIRO",
            engorda_ts=self.ENGORDA_TS,
            controle_operacional_date=self.OPERATIONAL_DATE,
        )
        row = out.first()

        operational_midnight = datetime.combine(self.OPERATIONAL_DATE, datetime.min.time())
        for column in (
            "DAT_EMISSAO",
            "DAT_SITUACAO_IF",
            "DAT_REGISTRO",
            "DAT_VAL_NOMINAL_EM",
            "DAT_ULTIMA_CORRECAO",
            "DAT_PU_CURVA",
            "DAT_VAL_NOMINAL_EM_ORIG",
            "DAT_FATOR_JUR_FLUT_ACUM_CDB",
        ):
            assert row[column] == operational_midnight
            assert column in applied
        assert row.DAT_VENCIMENTO == operational_midnight + timedelta(days=original_term)
        assert row.DAT_ATUALIZACAO_REGISTRO == self.ENGORDA_TS.replace(microsecond=0)

    def test_operation_uses_run_timestamp_and_operational_dates(self, spark):
        old = datetime(2020, 1, 1)
        df = spark.createDataFrame(
            [(old, old, old, old, old, old, old, "old")],
            "DAT_INCLUSAO timestamp, DAT_ALTERACAO timestamp, "
            "DAT_INCLUSAO_REGISTRO timestamp, "
            "DAT_ATUALIZACAO_REGISTRO timestamp, TSP_SITUACAO timestamp, "
            "DAT_FINANCEIRO timestamp, DAT_OPERACAO timestamp, "
            "VAL_TIME_STAMP_ATUALIZACAO string",
        )

        out, applied = engorda_tables.aplica_regras_engorda(
            df,
            "OPERACAO",
            engorda_ts=self.ENGORDA_TS,
            controle_operacional_date=self.OPERATIONAL_DATE,
        )
        row = out.first()
        expected = self.ENGORDA_TS.replace(microsecond=0)
        operational_midnight = datetime.combine(self.OPERATIONAL_DATE, datetime.min.time())

        for column in (
            "DAT_INCLUSAO",
            "DAT_ALTERACAO",
            "DAT_INCLUSAO_REGISTRO",
            "DAT_ATUALIZACAO_REGISTRO",
            "TSP_SITUACAO",
        ):
            assert row[column] == expected
        assert row.VAL_TIME_STAMP_ATUALIZACAO == "2026080810190634"
        assert row.DAT_FINANCEIRO == operational_midnight
        assert row.DAT_OPERACAO == operational_midnight
        assert {"DAT_FINANCEIRO", "DAT_OPERACAO"}.issubset(applied)

    def test_event_keeps_liquidation_and_copies_related_dates(self, spark):
        liquidation = datetime(2028, 4, 27)
        df = spark.createDataFrame(
            [(liquidation, datetime(2020, 1, 1), datetime(2021, 1, 1))],
            "DAT_LIQUIDACAO timestamp, DAT_OCORRENCIA_EVENTO timestamp, "
            "DAT_ORIGINAL_EVENTO timestamp",
        )

        out, applied = engorda_tables.aplica_regras_engorda(
            df,
            "EVENTO",
            engorda_ts=self.ENGORDA_TS,
            controle_operacional_date=self.OPERATIONAL_DATE,
        )
        row = out.first()

        assert row.DAT_LIQUIDACAO == liquidation
        assert row.DAT_OCORRENCIA_EVENTO == liquidation
        assert row.DAT_ORIGINAL_EVENTO == liquidation
        assert set(applied) == {"DAT_OCORRENCIA_EVENTO", "DAT_ORIGINAL_EVENTO"}

    def test_resgate_schedule_moves_with_issuance(self, spark):
        resultados = {
            "INSTRUMENTO_FINANCEIRO": (
                spark.createDataFrame(
                    [(100, date(2026, 5, 8))],
                    "NUM_IF long, DAT_EMISSAO date",
                ),
                1,
            ),
            "CONDICAO_IF": (
                spark.createDataFrame(
                    [(200, 100)],
                    "NUM_CONDICAO_IF long, NUM_IF long",
                ),
                1,
            ),
            "RESGATE": (
                spark.createDataFrame(
                    [(200, date(2025, 5, 8))],
                    "NUM_CONDICAO_IF long, DAT_RESGATE date",
                ),
                1,
            ),
            "CONDICAO_RESGATE": (
                spark.createDataFrame(
                    [
                        (300, 200, date(2024, 6, 7)),
                        (301, 200, date(2024, 7, 7)),
                    ],
                    "NUM_ID_CONDICAO_RESGATE long, NUM_CONDICAO_IF long, DAT_RESGATE date",
                ),
                2,
            ),
        }
        lote_instrumentos = spark.createDataFrame(
            [(10, date(2024, 5, 8))],
            "NUM_IF long, DAT_EMISSAO date",
        )
        mapa_num_if = spark.createDataFrame(
            [(10, 0, 100)],
            "old_NUM_IF long, __k int, new_NUM_IF long",
        )

        adjusted, changed = engorda_tables.ajusta_datas_resgate(
            resultados, lote_instrumentos, mapa_num_if
        )

        assert changed == ["CONDICAO_RESGATE", "RESGATE"]
        assert adjusted["RESGATE"][0].first().DAT_RESGATE == date(2027, 5, 8)
        assert [
            row.DAT_RESGATE
            for row in adjusted["CONDICAO_RESGATE"][0].orderBy("NUM_ID_CONDICAO_RESGATE").collect()
        ] == [date(2026, 6, 7), date(2026, 7, 7)]


class TestContiguousRowId:
    def test_reserved_pk_mapping_is_stable_across_input_partitions(self, spark):
        schema = "ID long, __clone_k int"
        rows = [(3, 2), (1, 1), (2, 2), (3, 1), (1, 2), (2, 1)]
        plan = engorda_tables.PlanoTabela(
            name="T", pk_cols=("ID",), pk_regra="OFFSET_PROPRIO", pk_start=100
        )

        def mapped(partitions):
            frame = spark.createDataFrame(rows, schema).repartition(partitions)
            return {
                (row.old_ID, row[engorda_tables.K_COL]): row.new_ID
                for row in engorda_tables._monta_mapeamento_pk(frame, plan, {}).collect()
            }

        assert mapped(2) == mapped(5)

    def test_ids_are_contiguous_and_unique_across_partitions(self, spark):
        df = spark.range(0, 1000).repartition(7).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        rids = sorted(r["rid"] for r in out.select("rid").collect())
        assert rids == list(range(1000))  # 0..N-1, no gaps, no duplicates

    def test_id_matches_within_partition_order(self, spark):
        # Within each source partition, rid order must follow row order; offsets
        # must make the global set contiguous regardless of partition sizes.
        df = spark.range(0, 50).repartition(4).withColumnRenamed("id", "val")
        rows = engorda_tables._with_contiguous_row_id(df, "rid").select("val", "rid").collect()
        rid_by_val = {r["val"]: r["rid"] for r in rows}
        assert len(rid_by_val) == 50
        assert sorted(rid_by_val.values()) == list(range(50))

    def test_no_single_partition_window_in_plan(self, spark):
        # Guards the fix: the offset prefix-sum must not use a no-partitionBy
        # Window, which Spark executes as SinglePartition (serial, stalls at scale).
        df = spark.range(0, 100).repartition(5).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        plan = out._jdf.queryExecution().executedPlan().toString()
        assert "SinglePartition" not in plan

    def test_empty_input(self, spark):
        df = spark.range(0, 0).withColumnRenamed("id", "val")
        out = engorda_tables._with_contiguous_row_id(df, "rid")
        assert out.select("rid").collect() == []


class TestEngordaIntegration:
    def _clone_selected(self, spark, config, specs, selected, factor):
        plans = engorda_tables.monta_plano(
            spark,
            config,
            specs,
            set(),
            pk_floor=1000,
            pk_band=10,
            offset_num_if=None,
            n_clones_estimado=len(selected) * factor,
        )
        order = engorda_tables.ordem_topologica(plans)
        counts, lots, mappings = {}, {}, {}
        try:
            lots = engorda_tables.calcula_lotes(
                spark,
                config,
                specs,
                plans,
                order,
                selected,
                max_passadas=6,
                counts_out=counts,
            )
            for table in order:
                clones, mappings[table] = engorda_tables.clona_tabela(
                    spark, plans[table], lots[table], factor, mappings
                )
                assert (
                    engorda_tables.valida_tabela(
                        specs[table], plans[table], clones, counts[table], factor
                    )
                    == []
                )
                engorda_tables.escreve_tabela(
                    spark,
                    clones,
                    f"{engorda_tables.clone_base_path(config)}/{table}",
                    expected_rows=counts[table] * factor,
                )
        finally:
            for frame in [*mappings.values(), *lots.values()]:
                frame.unpersist()

    def test_round_trip_preserves_keys_and_scales(self, spark, tmp_path):
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        customers = spark.createDataFrame(
            [(i, f"name{i}") for i in range(1, 11)], ["NUM_IF", "NAME"]
        )
        orders = spark.createDataFrame(
            [(i, (i % 10) + 1, i * 1.5) for i in range(1, 101)],
            ["ORDER_ID", "NUM_IF", "AMOUNT"],
        )
        customers.write.parquet(str(raw / "INSTRUMENTO_FINANCEIRO"))
        orders.write.parquet(str(raw / "ORDERS"))

        config = {
            "DATAGEN_OUTPUT_URI": str(syn),
            "DATAGEN_RAW_BASE_URI": str(raw),
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn),
            "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
        }

        self._clone_selected(spark, config, specs, list(range(1, 11)), 3)

        out_customers = spark.read.parquet(str(syn / "INSTRUMENTO_FINANCEIRO"))
        out_orders = spark.read.parquet(str(syn / "ORDERS"))

        # Each selected root and every child row has exactly three clones.
        assert out_customers.count() == 30
        # ORDERS scaled 100 -> 300.
        assert out_orders.count() == 300
        # PK uniqueness.
        assert out_orders.select("ORDER_ID").distinct().count() == 300
        assert out_customers.select("NUM_IF").distinct().count() == 30
        orphans = out_orders.join(out_customers, "NUM_IF", "left_anti").count()
        assert orphans == 0
        assert {
            row.NAME: row["count"] for row in out_customers.groupBy("NAME").count().collect()
        } == {f"name{i}": 3 for i in range(1, 11)}
        assert {
            row.AMOUNT: row["count"] for row in out_orders.groupBy("AMOUNT").count().collect()
        } == {i * 1.5: 3 for i in range(1, 101)}

    def test_multilevel_fk_integrity_with_checkpointed_mappings(self, spark, tmp_path):
        # The root map is consumed by both B and C; B's map is also consumed by C.
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        a = spark.createDataFrame([(i,) for i in range(1, 6)], ["NUM_IF"])
        b = spark.createDataFrame([(i, (i % 5) + 1) for i in range(1, 21)], ["B_ID", "NUM_IF"])
        c = spark.createDataFrame(
            [(i, (i % 20) + 1, (i % 5) + 1) for i in range(1, 41)],
            ["C_ID", "B_ID", "NUM_IF"],
        )
        a.write.parquet(str(raw / "INSTRUMENTO_FINANCEIRO"))
        b.write.parquet(str(raw / "B"))
        c.write.parquet(str(raw / "C"))

        config = {
            "DATAGEN_OUTPUT_URI": str(syn),
            "DATAGEN_RAW_BASE_URI": str(raw),
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn),
            "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "B": {
                "pk_cols": ["B_ID"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
            "C": {
                "pk_cols": ["C_ID"],
                "foreign_keys": [
                    {"columns": ["B_ID"], "parent_table": "B", "parent_columns": ["B_ID"]},
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    },
                ],
            },
        }

        self._clone_selected(spark, config, specs, list(range(1, 6)), 2)

        out_a = spark.read.parquet(str(syn / "INSTRUMENTO_FINANCEIRO"))
        out_b = spark.read.parquet(str(syn / "B"))
        out_c = spark.read.parquet(str(syn / "C"))

        # Every remapped FK lands on an existing parent key, at both levels.
        assert out_b.join(out_a, "NUM_IF", "left_anti").count() == 0
        assert out_c.join(out_a, "NUM_IF", "left_anti").count() == 0
        assert out_c.join(out_b, "B_ID", "left_anti").count() == 0
        # PK uniqueness preserved.
        assert out_c.select("C_ID").distinct().count() == out_c.count()
        assert (out_a.count(), out_b.count(), out_c.count()) == (10, 40, 80)

    def test_selected_roots_keep_complete_multilevel_fk_closure(self, spark, tmp_path):
        raw, syn = tmp_path / "raw", tmp_path / "syn"
        spark.createDataFrame([(i,) for i in range(1, 9)], ["NUM_IF"]).write.parquet(
            str(raw / "INSTRUMENTO_FINANCEIRO")
        )
        spark.createDataFrame(
            [(i, (i % 8) + 1) for i in range(1, 41)], ["B_ID", "NUM_IF"]
        ).write.parquet(str(raw / "B"))
        spark.createDataFrame(
            [(i, (i % 40) + 1) for i in range(1, 121)], ["C_ID", "B_ID"]
        ).write.parquet(str(raw / "C"))

        config = {
            "DATAGEN_OUTPUT_URI": str(syn),
            "DATAGEN_RAW_BASE_URI": str(raw),
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn),
            "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "B": {
                "pk_cols": ["B_ID"],
                "foreign_keys": [
                    {
                        "columns": ["NUM_IF"],
                        "parent_table": "INSTRUMENTO_FINANCEIRO",
                        "parent_columns": ["NUM_IF"],
                    }
                ],
            },
            "C": {
                "pk_cols": ["C_ID"],
                "foreign_keys": [
                    {"columns": ["B_ID"], "parent_table": "B", "parent_columns": ["B_ID"]}
                ],
            },
        }

        self._clone_selected(spark, config, specs, [1, 2], 1)

        out_a = spark.read.parquet(str(syn / "INSTRUMENTO_FINANCEIRO"))
        out_b = spark.read.parquet(str(syn / "B"))
        out_c = spark.read.parquet(str(syn / "C"))
        assert out_b.join(out_a, "NUM_IF", "left_anti").count() == 0
        assert out_c.join(out_b, "B_ID", "left_anti").count() == 0
        assert out_c.count() > 0
        assert (out_a.count(), out_b.count(), out_c.count()) == (2, 10, 30)
