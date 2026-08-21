import dataclasses
import json
import sys
from datetime import date, datetime, timedelta

import pytest

from datagen import engorda_tables
from scripts import run_pipeline


def test_module_imports():
    assert engorda_tables.REQUIRED_ENV_VARS == (
        "DATAGEN_RAW_BASE_URI",
        "DATAGEN_SYNTHETIC_BASE_URI",
        "DATAGEN_SPECS_URI",
    )


def test_rdb_inclusao_enables_observed_integrity_checks():
    profile = engorda_tables.get_product_profile("rdb_inclusao")

    assert profile.integrity.invalid_num_if_checks == frozenset({
        engorda_tables.CHECK_RESGATE_COVERAGE,
        engorda_tables.CHECK_RESGATE_PARENT,
        engorda_tables.CHECK_RESGATE_VALUES,
        engorda_tables.CHECK_DATE_ORDER,
    })


class TestEngordaPhaseCli:
    def test_pipeline_argv_matches_real_engorda_parser(self):
        paths = {
            "selection_plan": "oci://bucket@ns/run/plan.json",
            "reservations": "oci://bucket@ns/run/reservation.json",
            "synthetic": "oci://bucket@ns/run/synthetic/cdb_resgate",
        }
        options = {"n_instrumentos": 10, "fator_k": 2, "seed": 7}

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
        assert planned.plan_uri == paths["selection_plan"]
        assert materialized.phase == "materialize"
        assert materialized.reservation_uri == paths["reservations"]
        assert materialized.output_uri == paths["synthetic"]

    def test_all_is_default_and_keeps_selection_contract(self):
        args = engorda_tables.parse_arguments([
            "--produto", "cdb_simplificado",
            "--num-ifs", "123",
            "--meu-numero-prefix", "321",
        ])

        assert args.phase == "all"
        assert args.num_ifs == [123]
        assert args.raw_uri is None
        assert args.output_uri is None
        assert args.sem_poda_cronograma_resgate is False

    def test_cli_disables_schedule_pruning_and_main_forwards_it(self, monkeypatch):
        captured = []
        monkeypatch.setattr(
            engorda_tables, "executar_job", lambda job: captured.append(job)
        )

        engorda_tables.main([
            "--produto", "cdb_resgate",
            "--num-ifs", "123",
            "--meu-numero-prefix", "321",
            "--sem-poda-cronograma-resgate",
        ])

        assert captured[0].poda_cronograma_resgate is False

    def test_schedule_pruning_job_option_is_boolean(self):
        job = engorda_tables.EngordaJob(
            produto="cdb_simplificado",
            num_ifs=(1,),
            meu_numero_prefix="321",
            poda_cronograma_resgate="yes",
        )

        with pytest.raises(ValueError, match="poda_cronograma_resgate.*booleano"):
            engorda_tables._validate_engorda_job(job)

    def test_materialize_uses_artifacts_and_rejects_resampling(self):
        args = engorda_tables.parse_arguments([
            "--phase", "materialize",
            "--produto", "cdb_simplificado",
            "--plan-uri", "oci://bucket@ns/run/plan",
            "--reservation-uri", "oci://bucket@ns/run/reservation",
            "--raw-uri", "oci://raw@ns/run/RAW",
            "--output-uri", "oci://out@ns/run/synthetic/cdb",
        ])

        assert args.num_ifs is None
        assert args.n_instrumentos is None
        assert args.raw_uri == "oci://raw@ns/run/RAW"
        assert args.output_uri == "oci://out@ns/run/synthetic/cdb"

        with pytest.raises(SystemExit):
            engorda_tables.parse_arguments([
                "--phase", "materialize",
                "--produto", "cdb_simplificado",
                "--plan-uri", "plan.json",
                "--reservation-uri", "reservation.json",
                "--num-ifs", "123",
            ])

    def test_main_forwards_public_artifact_contract(self, monkeypatch):
        captured = []
        monkeypatch.setattr(
            engorda_tables, "executar_job", lambda job: captured.append(job)
        )

        engorda_tables.main([
            "--phase", "plan",
            "--produto", "cdb_simplificado",
            "--n-instrumentos", "2",
            "--plan-uri", "plan.json",
            "--raw-uri", "oci://raw@ns/exact",
            "--output-uri", "oci://out@ns/exact",
        ])

        assert captured == [engorda_tables.EngordaJob(
            produto="cdb_simplificado",
            n_instrumentos=2,
            phase="plan",
            plan_uri="plan.json",
            raw_uri="oci://raw@ns/exact",
            output_uri="oci://out@ns/exact",
        )]


class TestEngordaArtifacts:
    @staticmethod
    def _selected_lote(table_counts=None):
        table_counts = table_counts or {"INSTRUMENTO_FINANCEIRO": 1}
        snapshot = (
            "oci://cfg@ns/run/plan.json.selected-lote/"
            "00000000-0000-4000-8000-000000000001"
        )
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
                            {"name": "NUM_IF" if table == "INSTRUMENTO_FINANCEIRO"
                             else "NUM_ID_OPERACAO",
                             "type": "long", "nullable": True,
                             "metadata": {}},
                        ] + ([
                            {"name": "NUM_TIPO_IF", "type": "long", "nullable": True,
                             "metadata": {}},
                        ] if table == "INSTRUMENTO_FINANCEIRO" else []),
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
            "meu_numero": {"ordinal_count_demand": 3},
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
        tampered_snapshot["selected_lote"]["tables"][
            "INSTRUMENTO_FINANCEIRO"
        ]["row_count"] = 2
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
        body.pop("spec_sha256")
        plan = {**body, "plan_id": engorda_tables._plan_id(body)}

        with pytest.raises(ValueError, match="spec_sha256"):
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
            business_keys=dataclasses.replace(
                profile.business_keys, operation=None
            ),
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
        assert plan["meu_numero"] == {"ordinal_count_demand": 0}
        assert engorda_tables._validate_plan_artifact(plan) == plan

    def test_plan_builder_uses_supplied_lote_counts_without_recounting_frames(
        self, spark
    ):
        class NoCountFrame:
            def count(self):
                raise AssertionError("supplied lote counts must prevent frame.count()")

        class SameAccountRows:
            def count(self):
                return 1

        class OperationFrame(NoCountFrame):
            def where(self, _predicate):
                return SameAccountRows()

        profile = engorda_tables.get_product_profile("cdb_simplificado")
        planos = {
            "INSTRUMENTO_FINANCEIRO": engorda_tables.PlanoTabela(
                "INSTRUMENTO_FINANCEIRO", ("NUM_IF",)
            ),
            "OPERACAO": engorda_tables.PlanoTabela(
                "OPERACAO", ("NUM_ID_OPERACAO",)
            ),
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
                "OPERACAO": OperationFrame(),
            },
            lote_counts={"INSTRUMENTO_FINANCEIRO": 2, "OPERACAO": 3},
            faltantes_uri=None,
            query_num_if_uri="oci://cfg@ns/queries_produtos.sql",
            selected_lote=self._selected_lote({
                "INSTRUMENTO_FINANCEIRO": 2,
                "OPERACAO": 3,
            }),
        )

        assert plan["tables"]["OPERACAO"]["source_count"] == 3
        assert plan["cod_operacao"] == {"count": 6}
        assert plan["meu_numero"] == {"ordinal_count_demand": 8}

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


    def test_active_closure_does_not_count_full_raw_source_for_logging(
        self, spark, monkeypatch
    ):
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
        )

        assert lotes["CONDICAO_IF"].count() == 1
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
                "prefix": "321",
                "start": 50,
                "end": 52,
                "count": 3,
            },
        }

        validated = engorda_tables._validate_reservation_artifact(
            plan, reservation
        )
        plano = engorda_tables.PlanoTabela(
            name="INSTRUMENTO_FINANCEIRO",
            pk_cols=("NUM_IF",),
            pk_regra="OFFSET_PROPRIO",
            pk_start=101,
        )
        engorda_tables._inject_reserved_pk_starts(
            {"INSTRUMENTO_FINANCEIRO": plano}, validated
        )

        assert plano.pk_start == 200
        assert validated["cod_operacao"] == {
            "strategy": "oracle_allocator", "count": 2,
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
                "prefix": "321", "start": 1, "end": 3, "count": 3,
            },
        }
        with pytest.raises(ValueError, match="plan_id"):
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
        assert (
            engorda_tables.raw_path(self.CONFIG, "ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_raw_path_reduces_dotted_name(self):
        assert (
            engorda_tables.raw_path(self.CONFIG, "ADMIN.ORDERS")
            == "oci://raw@ns/datagen/raw/ORDERS"
        )

    def test_synthetic_base_without_prefix(self):
        assert engorda_tables.synthetic_base_path(self.CONFIG) == "oci://syn@ns"

    def test_synthetic_base_with_prefix(self):
        cfg = dict(self.CONFIG, DATAGEN_SYNTHETIC_PREFIX="datagen/synthetic")
        assert (
            engorda_tables.synthetic_base_path(cfg) == "oci://syn@ns/datagen/synthetic"
        )


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

    def test_exact_raw_and_output_overrides_do_not_require_base_envs(
        self, monkeypatch
    ):
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
        assert engorda_tables.clone_base_path(config) == (
            "oci://out@ns/run/synthetic/cdb"
        )


class TestNormalizeSpecs:
    def test_reduces_keys_and_parent_table(self):
        raw = {
            "ADMIN.ORDERS": {
                "pk_cols": ["ORDER_ID"],
                "foreign_keys": [
                    {"columns": ["CUSTOMER_ID"], "parent_table": "ADMIN.CUSTOMERS"}
                ],
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
        assert out["ORDERS"]["fks"][0]["parent_table"] == "CUSTOMERS"

    def test_rejects_collision(self):
        raw = {
            "A.ORDERS": {"pk_cols": ["ID"]},
            "B.ORDERS": {"pk_cols": ["ID"]},
        }
        with pytest.raises(ValueError):
            engorda_tables.normalize_specs(raw)

    def test_passes_through_when_no_schema(self):
        raw = {"ORDERS": {"pk_cols": ["ID"], "n_rows": 10}}
        assert engorda_tables.normalize_specs(raw) == raw


class TestConnectedComponents:
    def _comps(self, specs):
        return sorted(sorted(c) for c in engorda_tables.connected_components(specs))

    def test_chain_is_one_component(self):
        specs = {
            "CUSTOMERS": {"pk_cols": ["CID"]},
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
            "ITEMS": {"pk_cols": ["IID"],
                      "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}]},
        }
        assert self._comps(specs) == [["CUSTOMERS", "ITEMS", "ORDERS"]]

    def test_disjoint_components(self):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        assert self._comps(specs) == [["A", "B"], ["C"]]

    def test_isolated_node(self):
        specs = {"LOG": {"pk_cols": ["ID"]}}
        assert self._comps(specs) == [["LOG"]]

    def test_fk_to_absent_parent_is_no_edge(self):
        specs = {
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "MISSING"}]},
            "OTHER": {"pk_cols": ["ID"]},
        }
        # MISSING is not a node, so ORDERS stays isolated from OTHER.
        assert self._comps(specs) == [["ORDERS"], ["OTHER"]]


class TestTopoOrderTables:
    def _pos(self, order):
        return {t: i for i, t in enumerate(order)}

    def test_parents_before_children(self):
        specs = {
            "ITEMS": {"pk_cols": ["IID"],
                      "foreign_keys": [{"columns": ["OID"], "parent_table": "ORDERS"}]},
            "ORDERS": {"pk_cols": ["OID"],
                       "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
            "CUSTOMERS": {"pk_cols": ["CID"]},
        }
        pos = self._pos(engorda_tables.topo_order_tables(specs))
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_self_reference_ignored(self):
        specs = {"USUARIO": {"pk_cols": ["ID"],
                             "foreign_keys": [{"columns": ["MGR"], "parent_table": "USUARIO"}]}}
        assert engorda_tables.topo_order_tables(specs) == ["USUARIO"]

    def test_cycle_is_broken_and_covers_all(self):
        specs = {
            "A": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["B"], "parent_table": "B"}]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["A"], "parent_table": "A"}]},
        }
        assert sorted(engorda_tables.topo_order_tables(specs)) == ["A", "B"]


class TestTopologicalOrder:
    """_topological_order shares topo_order_tables' cycle policy: it breaks
    cycles instead of raising (cycles are sanitized/expected, not fatal)."""

    def _spec(self, name, parents=()):
        fks = tuple(
            engorda_tables.ForeignKeySpec(columns=(f"FK_{p}",), parent_table=p,
                                          parent_columns=("ID",))
            for p in parents
        )
        return engorda_tables.TableSpec(name=name, pk_cols=("ID",), foreign_keys=fks)

    def test_parents_before_children(self):
        specs = {
            "ITEMS": self._spec("ITEMS", ["ORDERS"]),
            "ORDERS": self._spec("ORDERS", ["CUSTOMERS"]),
            "CUSTOMERS": self._spec("CUSTOMERS"),
        }
        order = engorda_tables._topological_order(specs)
        pos = {t: i for i, t in enumerate(order)}
        assert pos["CUSTOMERS"] < pos["ORDERS"] < pos["ITEMS"]

    def test_cycle_is_broken_and_warns(self):
        specs = {
            "A": self._spec("A", ["B"]),
            "B": self._spec("B", ["A"]),
        }
        with pytest.warns(UserWarning, match="[Cc]iclo"):
            order = engorda_tables._topological_order(specs)
        assert sorted(order) == ["A", "B"]

    def test_acyclic_does_not_warn(self):
        import warnings as _w

        specs = {"P": self._spec("P"), "C": self._spec("C", ["P"])}
        with _w.catch_warnings():
            _w.simplefilter("error")
            assert engorda_tables._topological_order(specs) == ["P", "C"]


class TestFkIsWholePk:
    def test_pk_equals_fk(self):
        fk = {"columns": ["NUM_CONDICAO_IF"], "parent_table": "CONDICAO_IF"}
        assert engorda_tables._fk_is_whole_pk(["NUM_CONDICAO_IF"], fk) is True

    def test_composite_pk_equals_fk_any_order(self):
        fk = {"columns": ["B", "A"]}
        assert engorda_tables._fk_is_whole_pk(["A", "B"], fk) is True

    def test_fk_is_subset_of_pk_is_false(self):
        fk = {"columns": ["A"]}
        assert engorda_tables._fk_is_whole_pk(["A", "B"], fk) is False

    def test_ordinary_fk_is_false(self):
        fk = {"columns": ["CUSTOMER_ID"]}
        assert engorda_tables._fk_is_whole_pk(["ORDER_ID"], fk) is False


class TestEffectiveNRows:
    SPECS = {
        "CUSTOMERS": {"pk_cols": ["CID"]},  # parent (referenced by ORDERS)
        "ORDERS": {"pk_cols": ["OID"],
                   "foreign_keys": [{"columns": ["CID"], "parent_table": "CUSTOMERS"}]},
    }

    def test_scales_non_static(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=3.0)
        assert out["ORDERS"] == 3000

    def test_parent_floor_blocks_shrink(self):
        counts = {"CUSTOMERS": 100, "ORDERS": 1000}
        out = engorda_tables.effective_n_rows(self.SPECS, counts, scale_factor=0.5)
        # CUSTOMERS is an FK parent: cannot go below its source count.
        assert out["CUSTOMERS"] == 100
        # ORDERS is a leaf: free to scale down.
        assert out["ORDERS"] == 500

    def test_override_wins_for_non_static(self):
        specs = {"BIG": {"pk_cols": ["ID"], "n_rows": 50}}
        out = engorda_tables.effective_n_rows(specs, {"BIG": 10}, scale_factor=3.0)
        assert out["BIG"] == 50

    def test_static_is_one_to_one_override_ignored(self):
        specs = {"REF": {"pk_cols": ["ID"], "static": True, "n_rows": 999}}
        out = engorda_tables.effective_n_rows(specs, {"REF": 7}, scale_factor=3.0)
        assert out["REF"] == 7

    def test_empty_source_is_zero(self):
        specs = {"EMPTY": {"pk_cols": ["ID"], "n_rows": 100}}
        out = engorda_tables.effective_n_rows(specs, {"EMPTY": 0}, scale_factor=3.0)
        assert out["EMPTY"] == 0


class TestParseArguments:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["engorda_tables.py"])
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 1.0
        assert args.seed == 42
        assert args.continue_on_error is False
        assert args.specs is None
        assert args.limit is None
        assert args.pk_offset is None
        assert args.pk_safety_band is None

    def test_overrides(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv",
            ["engorda_tables.py", "--scale-factor", "3", "--seed", "7",
             "--continue-on-error", "--limit", "1000", "--pk-offset", "10000000000000",
             "--pk-safety-band", "1000000", "--specs", "oci://cfg@ns/s.json"],
        )
        args = engorda_tables.parse_arguments()
        assert args.scale_factor == 3.0
        assert args.seed == 7
        assert args.continue_on_error is True
        assert args.limit == 1000
        assert args.pk_offset == 10_000_000_000_000
        assert args.pk_safety_band == 1_000_000
        assert args.specs == "oci://cfg@ns/s.json"

    def test_rejects_non_positive_limit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["engorda_tables.py", "--limit", "0"])
        with pytest.raises(SystemExit):
            engorda_tables.parse_arguments()


class TestReadParquet:
    class _DF:
        def __init__(self): self.limit_arg = None
        def limit(self, n):
            self.limit_arg = n
            return self

    class _Spark:
        def __init__(self, df): self._df = df
        @property
        def read(self):
            outer = self
            class _Reader:
                def parquet(self_inner, path): return outer._df
            return _Reader()

    def test_no_limit_returns_full_df(self):
        df = self._DF()
        out = engorda_tables.read_parquet(self._Spark(df), "p")
        assert out is df and df.limit_arg is None

    def test_applies_limit(self):
        df = self._DF()
        out = engorda_tables.read_parquet(self._Spark(df), "p", limit=250)
        assert out is df and df.limit_arg == 250


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
            engorda_tables, "_open_oracle_connection",
            lambda *_args: connection,
        )

        result = engorda_tables._read_controle_operacional_date(
            object(), "jdbc:test", "user", "password")

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
            engorda_tables, "_open_oracle_connection",
            lambda *_args: connection,
        )

        with pytest.raises(ValueError, match="CONTROLE_OPERACIONAL"):
            engorda_tables._read_controle_operacional_date(
                object(), "jdbc:test", "user", "password")


class TestEngordaLoop:
    def _config(self):
        return {
            "DATAGEN_RAW_BASE_URI": "oci://raw@ns", "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": "oci://syn@ns", "DATAGEN_SYNTHETIC_PREFIX": "",
        }

    def test_processes_each_component_and_releases(self, monkeypatch):
        specs = {
            "A": {"pk_cols": ["ID"]},
            "B": {"pk_cols": ["ID"], "foreign_keys": [{"columns": ["AID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["ID"]},
        }
        synth_calls = []
        released = []

        class FakeDF:
            def __init__(self, name): self.name = name
            def count(self): return 10

        writes = []
        monkeypatch.setattr(engorda_tables, "read_parquet",
                            lambda spark, path, limit=None: FakeDF(path))
        monkeypatch.setattr(engorda_tables, "release",
                            lambda *dfs: released.extend(dfs))
        monkeypatch.setattr(engorda_tables, "write_synthetic_table",
                            lambda spark, df, out_path: writes.append(out_path))
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, comp_specs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks",
                            lambda synthetic, comp_specs: synthetic)

        def fake_run(tables, comp_specs, **kwargs):
            synth_calls.append((set(comp_specs), kwargs["n_rows_by_table"]))
            return {t: FakeDF(t) for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=2.0, seed=42, continue_on_error=False)

        processed = sorted(sorted(s) for s, _ in synth_calls)
        assert processed == [["A", "B"], ["C"]]
        assert released  # something was released between/after components
        # every table written to its own distinct prefix
        assert sorted(writes) == [
            "oci://syn@ns/A",
            "oci://syn@ns/B",
            "oci://syn@ns/C",
        ]
        assert len(writes) == len(set(writes))

    def test_continue_on_error_collects_and_exits(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}, "C": {"pk_cols": ["ID"]}}

        class FakeDF:
            def count(self): return 5
        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p, limit=None: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})

        def fake_run(tables, comp_specs, **kwargs):
            raise RuntimeError("boom")
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)

        with pytest.raises(SystemExit):
            engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                                   scale_factor=1.0, seed=42, continue_on_error=True)

    def test_limit_uses_referential_sample(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        seen = {}

        class FakeDF:
            def count(self): return 3

        def _no_read(*a, **k):
            raise AssertionError("read_parquet used despite --limit")

        # plain read_parquet must NOT be used when --limit is set
        monkeypatch.setattr(engorda_tables, "read_parquet", _no_read)
        monkeypatch.setattr(engorda_tables, "referential_sample",
                            lambda spark, config, comp_specs, limit:
                                seen.update(limit=limit) or {t: FakeDF() for t in comp_specs})
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "write_synthetic_table", lambda s, d, p: None)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes", lambda *a, **k: {})
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks", lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables",
                            lambda tables, comp_specs, **kwargs: {t: FakeDF() for t in comp_specs})

        engorda_tables.engorda(spark=object(), config=self._config(), specs=specs,
                               scale_factor=1.0, seed=42, continue_on_error=False, limit=500)
        assert seen == {"limit": 500}

    def _run_capturing(self, monkeypatch, pk_offset, pk_maxes):
        seen = {}
        floors = []

        class FakeDF:
            def count(self): return 3

        monkeypatch.setattr(engorda_tables, "read_parquet", lambda s, p, limit=None: FakeDF())
        monkeypatch.setattr(engorda_tables, "release", lambda *dfs: None)
        monkeypatch.setattr(engorda_tables, "write_synthetic_table",
                            lambda spark, df, out_path: None)
        monkeypatch.setattr(engorda_tables, "bind_shared_key_children",
                            lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "null_orphan_fks", lambda synthetic, cs: synthetic)
        monkeypatch.setattr(engorda_tables, "compute_pk_maxes",
                            lambda spark, config, comp_specs, floor=0, band=0, n_rows=None:
                                floors.append(floor) or pk_maxes)

        def fake_run(tables, comp_specs, **kwargs):
            seen.update(kwargs)
            return {t: FakeDF() for t in comp_specs}

        monkeypatch.setattr(engorda_tables, "run_synthesis_from_tables", fake_run)
        engorda_tables.engorda(spark=object(), config=self._config(),
                               specs={"A": {"pk_cols": ["ID"]}}, scale_factor=1.0,
                               seed=42, continue_on_error=False, pk_offset=pk_offset)
        return seen, floors

    def test_forwards_true_pk_maxes_to_synthesis(self, monkeypatch):
        seen, _ = self._run_capturing(monkeypatch, pk_offset=None, pk_maxes={"A": 999})
        assert seen["pk_max_by_table"] == {"A": 999}

    def test_pk_offset_passed_as_floor(self, monkeypatch):
        _, floors = self._run_capturing(monkeypatch, pk_offset=10**13, pk_maxes={"A": 10**13})
        assert floors == [10**13]


class TestComputePkMaxes:
    CONFIG = {"DATAGEN_RAW_BASE_URI": "oci://raw@ns", "DATAGEN_RAW_PREFIX": ""}

    @pytest.fixture(autouse=True)
    def _no_clamp(self, monkeypatch):
        # default: unlimited PK domain (no clamp). Clamp test overrides this.
        monkeypatch.setattr(engorda_tables, "_pk_capacity", lambda s, p, c: None)

    def test_clamps_band_to_pk_domain(self, monkeypatch):
        specs = {"M": {"pk_cols": ["NUM_ID_MODALIDADE_LIQUIDACAO"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 26)
        monkeypatch.setattr(engorda_tables, "_pk_capacity", lambda s, p, c: 999)  # Decimal(3,0)
        # band would push start to 1_000_026; clamp to capacity - n_rows
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs,
                                              band=1_000_000, n_rows={"M": 10})
        assert out == {"M": 989}            # 999 - 10, so 989 + 10 <= 999
        assert out["M"] >= 26               # never below true_max

    def test_skips_static_floors_and_uses_last_pk(self, monkeypatch):
        specs = {
            "A": {"pk_cols": ["ID"]},                    # true max 100
            "REF": {"pk_cols": ["C"], "static": True},   # skipped (static)
            "B": {"pk_cols": ["X", "ID"]},               # composite -> last col; max 5 -> floor
        }
        seen_cols = {}
        maxes = {"oci://raw@ns/A": 100, "oci://raw@ns/B": 5}

        def fake_max(spark, path, pk_col):
            seen_cols[path] = pk_col
            return maxes[path]

        monkeypatch.setattr(engorda_tables, "_read_pk_max", fake_max)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs, floor=1000)
        assert out == {"A": 1000, "B": 1000}            # max(true_max, floor); REF omitted
        assert seen_cols["oci://raw@ns/B"] == "ID"      # last PK column

    def test_no_floor_uses_true_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 8_000_000_000)
        assert engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs) == {"A": 8_000_000_000}

    def test_safety_band_added_above_true_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 8_000_000_000)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs, band=1_000_000)
        assert out == {"A": 8_001_000_000}  # true_max + band

    def test_floor_wins_over_band_when_higher(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: 100)
        out = engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs,
                                              floor=10**13, band=1_000_000)
        assert out == {"A": 10**13}  # max(true_max + band, floor)

    def test_omits_unreadable_max(self, monkeypatch):
        specs = {"A": {"pk_cols": ["ID"]}}
        monkeypatch.setattr(engorda_tables, "_read_pk_max", lambda s, p, c: None)
        assert engorda_tables.compute_pk_maxes(object(), self.CONFIG, specs) == {}


class TestWriteSyntheticTable:
    def test_deletes_only_table_prefix_then_appends(self, monkeypatch):
        deleted = []
        appended = {}

        class FakeWriter:
            def __init__(self, df): self.df = df
            def mode(self, m):
                self.df.mode_arg = m
                return self
            def parquet(self, path): appended[path] = self.df.mode_arg

        class FakeDF:
            @property
            def write(self): return FakeWriter(self)

        df = FakeDF()
        # bypass column sanitization and the Hadoop FS plumbing
        monkeypatch.setattr(engorda_tables, "_sanitize_columns_for_save",
                            lambda d, name: d)
        monkeypatch.setattr(engorda_tables, "_delete_path",
                            lambda spark, path: deleted.append(path))

        engorda_tables.write_synthetic_table(object(), df, "oci://syn@ns/synthetic/CONDICAO_IF")

        # delete is scoped to exactly this table's prefix, never the parent
        assert deleted == ["oci://syn@ns/synthetic/CONDICAO_IF"]
        assert appended == {"oci://syn@ns/synthetic/CONDICAO_IF": "append"}


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
        "NUM_IF long, NUM_CONDICAO_IF long, COD_TIPO_CONDICAO_IF string, "
        "DAT_EXCLUSAO string",
    )
    redemptions = spark.createDataFrame(
        [
            (
                100 + root,
                "SEM TABELA" if root == 10 else (
                    "  com tabela  " if root == 11 else "COM TABELA"
                ),
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
        "NUM_CONDICAO_IF long, DAT_RESGATE string, VAL_PERCENTUAL string, "
        "IND_EXCLUIDO string",
    )
    return {
        engorda_tables.CONDICAO_IF_TABLE: conditions,
        engorda_tables.RESGATE_TABELA: redemptions,
        engorda_tables.CRONOGRAMA_TABELA: schedules,
    }


def test_schedule_guard_rejects_only_active_type20_com_tabela_defects(
    spark, monkeypatch
):
    sources = _schedule_guard_sources(spark)
    monkeypatch.setattr(
        engorda_tables,
        "_read_source",
        lambda _spark, _config, table: sources[table],
    )
    domain = spark.createDataFrame([(root,) for root in range(1, 17)], "NUM_IF long")

    invalid = engorda_tables._num_if_cronograma_resgate_invalido(
        spark, {}, domain
    )

    assert {row.NUM_IF for row in invalid.collect()} == {
        2, 3, 4, 5, 6, 7, 8, 15, 16
    }


def test_schedule_guard_refills_sampling_but_rejects_explicit_invalid_root(
    spark, monkeypatch
):
    sources = _schedule_guard_sources(spark)
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
        spark, {}, {}, None, 7, 42, profile, poda_subtipo=False
    )
    assert selected == [1, 9, 10, 11, 12, 13, 14]

    with pytest.raises(ValueError, match=r"PODADOS.*2"):
        engorda_tables.seleciona_instrumentos(
            spark, {}, {}, [2], None, 42, profile, poda_subtipo=False
        )


def test_schedule_guard_fails_closed_when_required_source_is_unavailable(
    spark, monkeypatch
):
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
    assert [row.NUM_CONDICAO_IF for row in lotes[
        engorda_tables.CRONOGRAMA_TABELA
    ].collect()] == [2]


def test_meu_numero_uses_reserved_ordinal_interval(spark):
    operation = spark.createDataFrame(
        [(
            1,
            datetime(2020, 1, 1),
            "100",
            "100",
            "old-p1",
            "old-p2",
            7,
        )],
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


class TestEngordaDateRules:
    ENGORDA_TS = datetime(2026, 8, 8, 10, 19, 6, 340_000)
    OPERATIONAL_DATE = date(2026, 5, 8)

    def test_instrument_uses_operational_date_and_preserves_term(self, spark):
        original_emission = datetime(2024, 1, 10)
        original_maturity = datetime(2025, 2, 20)
        original_term = (original_maturity.date() - original_emission.date()).days
        df = spark.createDataFrame([(
            original_emission,
            original_maturity,
            datetime(2024, 1, 11),
            datetime(2024, 1, 12),
            datetime(2024, 1, 13),
            datetime(2024, 1, 14),
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
            datetime(2024, 1, 17),
        )], (
            "DAT_EMISSAO timestamp, DAT_VENCIMENTO timestamp, "
            "DAT_REGISTRO timestamp, DAT_VAL_NOMINAL_EM timestamp, "
            "DAT_ULTIMA_CORRECAO timestamp, DAT_PU_CURVA timestamp, "
            "DAT_VAL_NOMINAL_EM_ORIG timestamp, "
            "DAT_FATOR_JUR_FLUT_ACUM_CDB timestamp, "
            "DAT_ATUALIZACAO_REGISTRO timestamp"
        ))

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

    def test_operation_uses_run_timestamp_and_formats_legacy_value(self, spark):
        old = datetime(2020, 1, 1)
        df = spark.createDataFrame(
            [(old, old, old, old, old, old, "old")],
            "DAT_INCLUSAO timestamp, DAT_ALTERACAO timestamp, "
            "DAT_INCLUSAO_REGISTRO timestamp, "
            "DAT_ATUALIZACAO_REGISTRO timestamp, TSP_SITUACAO timestamp, "
            "DAT_OPERACAO timestamp, VAL_TIME_STAMP_ATUALIZACAO string",
        )

        out, _ = engorda_tables.aplica_regras_engorda(
            df,
            "OPERACAO",
            engorda_ts=self.ENGORDA_TS,
            controle_operacional_date=self.OPERATIONAL_DATE,
        )
        row = out.first()
        expected = self.ENGORDA_TS.replace(microsecond=0)

        for column in (
            "DAT_INCLUSAO",
            "DAT_ALTERACAO",
            "DAT_INCLUSAO_REGISTRO",
            "DAT_ATUALIZACAO_REGISTRO",
            "TSP_SITUACAO",
        ):
            assert row[column] == expected
        assert row.VAL_TIME_STAMP_ATUALIZACAO == "2026080810190634"
        assert row.DAT_OPERACAO == old

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
            for row in adjusted["CONDICAO_RESGATE"][0].orderBy(
                "NUM_ID_CONDICAO_RESGATE"
            ).collect()
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
    def test_round_trip_preserves_keys_and_scales(self, spark, tmp_path):
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        customers = spark.createDataFrame(
            [(i, f"name{i}") for i in range(1, 11)], ["CUSTOMER_ID", "NAME"]
        )
        orders = spark.createDataFrame(
            [(i, (i % 10) + 1, i * 1.5) for i in range(1, 101)],
            ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
        )
        customers.write.parquet(str(raw / "CUSTOMERS"))
        orders.write.parquet(str(raw / "ORDERS"))

        config = {
            "DATAGEN_RAW_BASE_URI": str(raw), "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn), "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "CUSTOMERS": {"pk_cols": ["CUSTOMER_ID"]},
            "ORDERS": {"pk_cols": ["ORDER_ID"],
                       "foreign_keys": [{"columns": ["CUSTOMER_ID"],
                                          "parent_table": "CUSTOMERS"}]},
        }

        engorda_tables.engorda(spark, config, specs, scale_factor=3.0, seed=1,
                               continue_on_error=False)

        out_customers = spark.read.parquet(str(syn / "CUSTOMERS"))
        out_orders = spark.read.parquet(str(syn / "ORDERS"))

        # CUSTOMERS is an FK parent: floored at source count (10), scaled up by 3 -> 30.
        assert out_customers.count() == 30
        # ORDERS scaled 100 -> 300.
        assert out_orders.count() == 300
        # PK uniqueness.
        assert out_orders.select("ORDER_ID").distinct().count() == 300
        assert out_customers.select("CUSTOMER_ID").distinct().count() == 30
        # FK integrity: every synthetic ORDERS.CUSTOMER_ID exists in synthetic CUSTOMERS.
        orphans = out_orders.join(out_customers, "CUSTOMER_ID", "left_anti").count()
        assert orphans == 0

    def test_multilevel_fk_integrity_with_eager_mapping_release(self, spark, tmp_path):
        # Guards the eager mapping-release: A's mapping is consumed by both B and
        # C and must survive until C (its last consumer); B's mapping must survive
        # until C. A premature free would corrupt the grandchild's remapped FKs.
        raw = tmp_path / "raw"
        syn = tmp_path / "syn"

        a = spark.createDataFrame([(i,) for i in range(1, 6)], ["A_ID"])
        b = spark.createDataFrame(
            [(i, (i % 5) + 1) for i in range(1, 21)], ["B_ID", "A_ID"]
        )
        c = spark.createDataFrame(
            [(i, (i % 20) + 1, (i % 5) + 1) for i in range(1, 41)],
            ["C_ID", "B_ID", "A_ID"],
        )
        a.write.parquet(str(raw / "A"))
        b.write.parquet(str(raw / "B"))
        c.write.parquet(str(raw / "C"))

        config = {
            "DATAGEN_RAW_BASE_URI": str(raw), "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn), "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "A": {"pk_cols": ["A_ID"]},
            "B": {"pk_cols": ["B_ID"],
                  "foreign_keys": [{"columns": ["A_ID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["C_ID"],
                  "foreign_keys": [{"columns": ["B_ID"], "parent_table": "B"},
                                   {"columns": ["A_ID"], "parent_table": "A"}]},
        }

        engorda_tables.engorda(spark, config, specs, scale_factor=2.0, seed=7,
                               continue_on_error=False)

        out_a = spark.read.parquet(str(syn / "A"))
        out_b = spark.read.parquet(str(syn / "B"))
        out_c = spark.read.parquet(str(syn / "C"))

        # Every remapped FK lands on an existing parent key, at both levels.
        assert out_b.join(out_a, "A_ID", "left_anti").count() == 0
        assert out_c.join(out_a, "A_ID", "left_anti").count() == 0
        assert out_c.join(out_b, "B_ID", "left_anti").count() == 0
        # PK uniqueness preserved.
        assert out_c.select("C_ID").distinct().count() == out_c.count()

    def test_limit_path_referential_sample_fk_integrity(self, spark, tmp_path):
        # Exercises the --limit path (referential_sample + truncate_lineage).
        # The multi-level FK chain previously built lineage deep enough to OOM
        # the driver; this guards that the path runs and stays FK-consistent.
        raw, syn = tmp_path / "raw", tmp_path / "syn"
        spark.createDataFrame([(i,) for i in range(1, 9)], ["A_ID"]).write.parquet(str(raw / "A"))
        spark.createDataFrame(
            [(i, (i % 8) + 1) for i in range(1, 41)], ["B_ID", "A_ID"]
        ).write.parquet(str(raw / "B"))
        spark.createDataFrame(
            [(i, (i % 40) + 1) for i in range(1, 121)], ["C_ID", "B_ID"]
        ).write.parquet(str(raw / "C"))

        config = {
            "DATAGEN_RAW_BASE_URI": str(raw), "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": str(syn), "DATAGEN_SYNTHETIC_PREFIX": "",
        }
        specs = {
            "A": {"pk_cols": ["A_ID"]},
            "B": {"pk_cols": ["B_ID"],
                  "foreign_keys": [{"columns": ["A_ID"], "parent_table": "A"}]},
            "C": {"pk_cols": ["C_ID"],
                  "foreign_keys": [{"columns": ["B_ID"], "parent_table": "B"}]},
        }

        engorda_tables.engorda(spark, config, specs, scale_factor=1.0, seed=3,
                               continue_on_error=False, limit=50)

        out_a = spark.read.parquet(str(syn / "A"))
        out_b = spark.read.parquet(str(syn / "B"))
        out_c = spark.read.parquet(str(syn / "C"))
        # FK-consistent after referential sampling + synthesis.
        assert out_b.join(out_a, "A_ID", "left_anti").count() == 0
        assert out_c.join(out_b, "B_ID", "left_anti").count() == 0
        assert out_c.count() > 0  # sampling kept rows
