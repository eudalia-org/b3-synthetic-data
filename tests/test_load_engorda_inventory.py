import copy
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from datagen import engorda_tables as engorda
from datagen import load_tables as loader

PRODUCTS = (
    "cdb_simplificado",
    "cdb_resgate",
    "cdb_escalonamento",
    "rdb_inclusao",
    "rdb_resgate",
)


@pytest.mark.parametrize("product", PRODUCTS)
def test_standalone_loader_allowlist_matches_generator_contract(product):
    assert loader.ENGORDA_LOAD_TABLES[product] == set(engorda.TABELAS_ENGORDA_POR_PRODUTO[product])


def test_effective_spec_only_overrides_inventory_and_preserves_parent_metadata():
    fk = [
        {
            "columns": ["NUM_CONDICAO_IF"],
            "parent_table": "CONDICAO_IF",
            "parent_columns": ["NUM_CONDICAO_IF"],
        }
    ]
    specs = {
        "AMORTIZACAO": {"static": True, "pk_cols": ["NUM_CONDICAO_IF"], "foreign_keys": fk},
        "CONDICAO_IF": {"static": True, "pk_cols": ["NUM_CONDICAO_IF"]},
    }
    result = loader.specs_for_engorda_inventory(specs, ["cetip.amortizacao"], "cdb_simplificado")
    assert not loader.is_static(result, "AMORTIZACAO")
    assert loader.is_static(result, "CONDICAO_IF")
    assert result["AMORTIZACAO"]["foreign_keys"] == fk
    assert result["AMORTIZACAO"]["pk_cols"] == ["NUM_CONDICAO_IF"]
    assert specs["AMORTIZACAO"]["static"] is True


def test_unknown_product_does_not_override_static_flags():
    specs = {"AMORTIZACAO": {"static": True, "pk_cols": ["NUM_CONDICAO_IF"]}}
    effective = loader.specs_for_engorda_inventory(specs, ["AMORTIZACAO"], "unknown")
    with pytest.raises(ValueError, match="marked static"):
        loader.resolve_load_tables(effective, ["AMORTIZACAO"])


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    def configure(
        product, *, extra_table=None, skip_validation=False, report_errors=0, clone_maps=False
    ):
        inventory = sorted(engorda.TABELAS_ENGORDA_POR_PRODUTO[product])
        if clone_maps:
            inventory += [
                engorda.MAPA_COD_IF_TABLE,
                engorda.MAPA_COD_OPERACAO_TABLE,
                engorda.MAPA_NUM_IF_TABLE,
            ]
        if extra_table:
            inventory.append(extra_table)
        all_tables = set().union(*(engorda.TABELAS_ENGORDA_POR_PRODUTO[p] for p in PRODUCTS))
        specs = {
            table: {"static": True, "pk_cols": ["NUM_IF"], "foreign_keys": []}
            for table in all_tables | {"TIPO_DEBITO", "LOTE"}
        }
        specs["AMORTIZACAO"]["foreign_keys"] = [
            {
                "columns": ["NUM_CONDICAO_IF"],
                "parent_table": "CONDICAO_IF",
                "parent_columns": ["NUM_CONDICAO_IF"],
            }
        ]
        specs["CONDICAO_IF"]["foreign_keys"] = [
            {
                "columns": ["NUM_IF"],
                "parent_table": "INSTRUMENTO_FINANCEIRO",
                "parent_columns": ["NUM_IF"],
            }
        ]
        input_uri = str(tmp_path / "synthetic")
        profile = "cdb" if product.startswith("cdb_") else product
        report = {
            "schema_version": 2,
            "verdict": "PASS",
            "counts": {"error": report_errors},
            "product": profile,
            "resolved_input": input_uri,
            "table_inventory": inventory,
        }
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(report))
        manifest_path = tmp_path / "load" / "manifest.json"
        argv = [
            "load_tables.py",
            "--product",
            product,
            "--validation-product",
            profile,
            "--validation-report",
            str(report_path),
            "--input-base",
            input_uri,
            "--specs",
            "original-spec.json",
            "--manifest-uri",
            str(manifest_path),
            "--pipeline-manifest-uri",
            "oci://bucket@ns/pipeline/manifest.json",
            "--expected-target-schema",
            "CETIP",
            "--run-id",
            "retry-load",
        ]
        if skip_validation:
            argv.append("--skip-validation")
        monkeypatch.setattr(sys, "argv", argv)
        monkeypatch.setattr(
            loader,
            "get_load_env",
            lambda *_: {
                "DATAGEN_LOAD_BASE_URI": input_uri,
                "DATAGEN_LOAD_PREFIX": "",
                "DATAGEN_TARGET_SCHEMA": "CETIP",
                "DATAGEN_TARGET_DB_USER": "CETIP",
                "DATAGEN_TARGET_DB_PASSWORD": "test-only",
                "DATAGEN_TARGET_JDBC_URL": "jdbc:oracle:thin:@test",
                "DATAGEN_JDBC_READ_TIMEOUT_MS": "600000",
            },
        )
        session = SimpleNamespace(stop=Mock())
        monkeypatch.setattr(loader, "create_spark_session", lambda *_: session)
        monkeypatch.setattr(loader, "load_specs", lambda *_: specs)
        preflight = Mock(return_value=[])
        monkeypatch.setattr(loader, "validate_load", preflight)

        def capture(_spark, _properties, _config, _specs, owner, tables, _limit):
            return [
                {
                    "table": table,
                    "owner": owner,
                    "name": table,
                    "expected_rows": 0 if table == "AMORTIZACAO" else 1,
                    "pk_col": "NUM_IF",
                    "synthetic_pk_min": None,
                    "synthetic_pk_max": None,
                    "rollbackable": False,
                }
                for table in tables
            ], []

        captured = Mock(side_effect=capture)
        monkeypatch.setattr(loader, "capture_manifest_entries", captured)

        def insert(_spark, _config, _specs, tables, **_kwargs):
            assert json.loads(manifest_path.read_text())["ordered_tables"] == tables

        inserted = Mock(side_effect=insert)
        monkeypatch.setattr(loader, "load_tables", inserted)
        return SimpleNamespace(
            specs=specs,
            original=copy.deepcopy(specs),
            inventory=inventory,
            manifest_path=manifest_path,
            captured=captured,
            inserted=inserted,
            preflight=preflight,
            session=session,
        )

    return configure


@pytest.mark.parametrize("product", PRODUCTS)
@pytest.mark.parametrize("skip_validation", [False, True])
def test_main_loads_validated_engorda_tables_despite_original_static_defaults(
    runtime, product, skip_validation
):
    state = runtime(product, skip_validation=skip_validation)
    loader.main()

    state.inserted.assert_called_once()
    effective_specs = state.inserted.call_args.args[2]
    tables = state.inserted.call_args.args[3]
    assert set(tables) == set(state.inventory)
    assert tables.index("INSTRUMENTO_FINANCEIRO") < tables.index("CONDICAO_IF")
    assert tables.index("CONDICAO_IF") < tables.index("AMORTIZACAO")
    assert all(not loader.is_static(effective_specs, table) for table in tables)
    assert loader.is_static(effective_specs, "TIPO_DEBITO")
    assert loader.is_static(effective_specs, "LOTE")
    assert state.specs == state.original
    assert state.captured.call_args.args[3] is effective_specs
    if not skip_validation:
        assert state.preflight.call_args.args[3] is effective_specs
    else:
        state.preflight.assert_not_called()
    manifest = json.loads(state.manifest_path.read_text())
    assert next(t for t in manifest["tables"] if t["table"] == "AMORTIZACAO")["expected_rows"] == 0
    state.session.stop.assert_called_once()


@pytest.mark.parametrize("extra_table", ["TIPO_DEBITO", "LOTE", "HISTORICO_PU_CURVA"])
def test_main_keeps_reference_and_other_product_static_tables_blocked(runtime, extra_table):
    state = runtime("cdb_simplificado", extra_table=extra_table)
    with pytest.raises(ValueError, match="marked static"):
        loader.main()
    state.captured.assert_not_called()
    state.inserted.assert_not_called()
    assert not state.manifest_path.exists()
    state.session.stop.assert_called_once()


def test_main_still_rejects_failed_validation_before_inventory_override(runtime):
    state = runtime("cdb_simplificado", report_errors=1)
    with pytest.raises(ValueError, match="ERROR findings"):
        loader.main()
    state.inserted.assert_not_called()
    assert state.specs == state.original


@pytest.mark.parametrize("product", PRODUCTS)
def test_main_loads_old_report_without_inserting_any_clone_maps(runtime, product):
    state = runtime(product, clone_maps=True, skip_validation=True)
    loader.main()
    tables = state.inserted.call_args.args[3]
    assert set(tables) == set(engorda.TABELAS_ENGORDA_POR_PRODUTO[product])
    manifest = json.loads(state.manifest_path.read_text())
    assert manifest["ordered_tables"] == tables
    assert {entry["table"] for entry in manifest["tables"]} == set(tables)
    assert all(not table.startswith("MAPA_CLONE_") for table in tables)


@pytest.mark.parametrize("table", ["MAPA_CLONE_UNKNOWN", "MISSING_ORACLE_TABLE"])
def test_main_still_rejects_unrecognized_inventory_names(runtime, table):
    state = runtime("cdb_simplificado", extra_table=table, clone_maps=True)
    with pytest.raises(ValueError, match="absent from specs"):
        loader.main()
    state.captured.assert_not_called()
    state.inserted.assert_not_called()
    assert not state.manifest_path.exists()
