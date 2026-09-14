import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from datagen import engorda_tables as engorda
from datagen import load_tables as loader
from scripts import run_pipeline as runner
from scripts import validate_products as validator

MAPS = {"MAPA_CLONE_NUM_IF", "MAPA_CLONE_COD_IF", "MAPA_CLONE_COD_OPERACAO"}


def test_standalone_auxiliary_sets_match_the_generator_outputs():
    assert MAPS == {
        engorda.MAPA_NUM_IF_TABLE,
        engorda.MAPA_COD_IF_TABLE,
        engorda.MAPA_COD_OPERACAO_TABLE,
    }
    assert loader.CLONE_MAP_ARTIFACTS == validator.CLONE_MAP_ARTIFACTS == runner.CLONE_MAP_ARTIFACTS
    assert loader.CLONE_MAP_ARTIFACTS == MAPS


@pytest.mark.parametrize("profile_name", sorted(validator.VALIDATION_PROFILES))
def test_report_separates_mapping_artifacts_but_keeps_them_available_for_validation(
    monkeypatch, profile_name
):
    names = sorted(MAPS | {"INSTRUMENTO_FINANCEIRO", "AMORTIZACAO"})
    monkeypatch.setattr(validator, "list_table_dirs", lambda *_: names)
    frame = Mock()
    frame.cache.return_value = frame
    spark = SimpleNamespace(read=SimpleNamespace(parquet=Mock(return_value=frame)))
    tables = validator.read_synthetic_tables(spark, "oci://bucket@ns/synthetic", None)
    assert set(tables) == set(names)  # Clone-map and shape checks still get their evidence.
    captured = []
    monkeypatch.setattr(
        validator, "write_text", lambda _s, _p, text: captured.append(json.loads(text))
    )
    validator.emit_report(
        spark,
        [],
        "report.json",
        "error",
        validator.VALIDATION_PROFILES[profile_name],
        "oci://bucket@ns/synthetic",
        list(tables),
        [],
        allow_partial=True,
    )
    report = captured[0]
    assert report["table_inventory"] == ["AMORTIZACAO", "INSTRUMENTO_FINANCEIRO"]
    assert report["auxiliary_artifacts"] == sorted(MAPS)
    assert set(tables) == set(names)
    assert (
        loader.validation_table_inventory(report, profile_name, "oci://bucket@ns/synthetic")
        == report["table_inventory"]
    )


def legacy_report(inventory):
    return {
        "schema_version": 2,
        "product": "cdb_simplificado",
        "verdict": "PARTIAL",
        "counts": {"error": 0},
        "resolved_input": "oci://bucket@ns/synthetic",
        "table_inventory": inventory,
    }


def test_loader_compatibility_only_excludes_exact_known_artifact_names():
    original = [
        "CETIP.INSTRUMENTO_FINANCEIRO",
        "cetip.mapa_clone_cod_if",
        "MAPA_CLONE_COD_OPERACAO",
        "MAPA_CLONE_NUM_IF",
        "MAPA_CLONE_UNKNOWN",
    ]
    report = legacy_report(original)
    report["auxiliary_artifacts"] = ["MAPA_CLONE_UNKNOWN"]
    result = loader.validation_table_inventory(report, "cdb_simplificado", report["resolved_input"])
    assert result == ["CETIP.INSTRUMENTO_FINANCEIRO", "MAPA_CLONE_UNKNOWN"]
    assert report["table_inventory"] == original


def test_loader_rejects_artifact_only_inventory():
    report = legacy_report(sorted(MAPS))
    with pytest.raises(ValueError, match="no Oracle tables"):
        loader.validation_table_inventory(report, "cdb_simplificado", report["resolved_input"])


def test_loader_still_rejects_duplicates_before_filtering_artifacts():
    report = legacy_report(["AMORTIZACAO", "MAPA_CLONE_COD_IF", "mapa_clone_cod_if"])
    with pytest.raises(ValueError, match="unique"):
        loader.validation_table_inventory(report, "cdb_simplificado", report["resolved_input"])


def test_new_report_retains_unknown_names_for_missing_spec_detection(monkeypatch):
    captured = []
    monkeypatch.setattr(
        validator, "write_text", lambda _s, _p, text: captured.append(json.loads(text))
    )
    validator.emit_report(
        None,
        [],
        "report.json",
        "error",
        validator.VALIDATION_PROFILES["cdb_simplificado"],
        "/synthetic",
        ["MAPA_CLONE_UNKNOWN", "MISSING_ORACLE_TABLE", "MAPA_CLONE_COD_IF"],
        [],
    )
    assert captured[0]["table_inventory"] == ["MAPA_CLONE_UNKNOWN", "MISSING_ORACLE_TABLE"]
    assert captured[0]["auxiliary_artifacts"] == ["MAPA_CLONE_COD_IF"]
