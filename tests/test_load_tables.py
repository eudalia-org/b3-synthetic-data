import os
from decimal import Decimal

import pytest

from datagen import load_tables


class TestValidateIdentifier:
    def test_uppercases_valid_identifier(self):
        assert load_tables.validate_identifier("orders") == "ORDERS"

    def test_accepts_oracle_special_characters(self):
        assert load_tables.validate_identifier("TAB_1$#") == "TAB_1$#"

    def test_rejects_injection_attempt(self):
        with pytest.raises(ValueError):
            load_tables.validate_identifier("T; DROP TABLE X")

    def test_rejects_quoted_identifier(self):
        with pytest.raises(ValueError):
            load_tables.validate_identifier('"MixedCase"')


class TestParseTables:
    def test_parses_comma_list(self):
        assert load_tables.parse_tables("A,B , C", None) == ["A", "B", "C"]

    def test_dedupes_preserving_order(self):
        assert load_tables.parse_tables("A,B,A", None) == ["A", "B"]

    def test_reads_file_ignoring_blanks_and_comments(self, tmp_path):
        f = tmp_path / "tables.txt"
        f.write_text("ORDERS\n# comment\n\nCUSTOMERS\n")
        assert load_tables.parse_tables(None, str(f)) == ["ORDERS", "CUSTOMERS"]

    def test_exits_when_empty(self):
        with pytest.raises(SystemExit):
            load_tables.parse_tables("", None)


class TestGetLoadEnv:
    BASE = {
        "DATAGEN_TARGET_JDBC_URL": "jdbc:oracle:thin:@host",
        "DATAGEN_TARGET_DB_PASSWORD": "secret",
        "DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load/",
    }

    def test_applies_defaults(self, monkeypatch):
        for key in list(os.environ):
            if key.startswith("DATAGEN_"):
                monkeypatch.delenv(key, raising=False)
        for key, value in self.BASE.items():
            monkeypatch.setenv(key, value)
        config = load_tables.get_load_env()
        assert config["DATAGEN_TARGET_DB_USER"] == "ADMIN"
        assert config["DATAGEN_JDBC_NUM_PARTITIONS"] == "256"
        assert config["DATAGEN_JDBC_BATCH_SIZE"] == "10000"
        assert config["DATAGEN_JDBC_READ_TIMEOUT_MS"] == "600000"
        assert config["DATAGEN_LOAD_PREFIX"] == ""
        assert config["DATAGEN_TARGET_JDBC_URL"] == "jdbc:oracle:thin:@host"
        # Schema defaults to the connection user when unset.
        assert config["DATAGEN_TARGET_SCHEMA"] == "ADMIN"

    def test_schema_defaults_to_db_user_and_overrides(self, monkeypatch):
        for key in list(os.environ):
            if key.startswith("DATAGEN_"):
                monkeypatch.delenv(key, raising=False)
        for key, value in self.BASE.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("DATAGEN_TARGET_DB_USER", "ADMIN")
        monkeypatch.setenv("DATAGEN_TARGET_SCHEMA", "cetip")
        config = load_tables.get_load_env()
        assert config["DATAGEN_TARGET_DB_USER"] == "ADMIN"
        assert config["DATAGEN_TARGET_SCHEMA"] == "cetip"

    def test_strips_trailing_slash_and_prefix_slashes(self, monkeypatch):
        for key, value in self.BASE.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setenv("DATAGEN_LOAD_PREFIX", "/synthetic/")
        config = load_tables.get_load_env()
        assert config["DATAGEN_LOAD_BASE_URI"] == "oci://bucket@ns/load"
        assert config["DATAGEN_LOAD_PREFIX"] == "synthetic"

    def test_exits_when_required_missing(self, monkeypatch):
        for key in REQUIRED:
            monkeypatch.delenv(key, raising=False)
        with pytest.raises(SystemExit):
            load_tables.get_load_env()


REQUIRED = (
    "DATAGEN_TARGET_JDBC_URL",
    "DATAGEN_TARGET_DB_PASSWORD",
    "DATAGEN_LOAD_BASE_URI",
)


class TestNameAndPathHelpers:
    def test_table_path_name_strips_schema(self):
        assert load_tables.table_path_name("CETIP.LANCAMENTO") == "LANCAMENTO"
        assert load_tables.table_path_name("ORDERS") == "ORDERS"

    def test_table_owner_and_name_with_schema(self):
        assert load_tables.table_owner_and_name("ADMIN", "cetip.lancamento") == (
            "CETIP",
            "LANCAMENTO",
        )

    def test_table_owner_and_name_defaults_to_user(self):
        assert load_tables.table_owner_and_name("admin", "orders") == ("ADMIN", "ORDERS")

    def test_dbtable_name_qualifies_unqualified(self):
        assert load_tables.dbtable_name("ADMIN", "ORDERS") == "ADMIN.ORDERS"
        assert load_tables.dbtable_name("ADMIN", "CETIP.X") == "CETIP.X"

    def test_build_load_path_with_prefix(self):
        config = {
            "DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load",
            "DATAGEN_LOAD_PREFIX": "synthetic",
        }
        assert (
            load_tables.build_load_path(config, "ORDERS")
            == "oci://bucket@ns/load/synthetic/ORDERS"
        )

    def test_build_load_path_without_prefix(self):
        config = {"DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load", "DATAGEN_LOAD_PREFIX": ""}
        assert load_tables.build_load_path(config, "ORDERS") == "oci://bucket@ns/load/ORDERS"


class TestConnectionProperties:
    CONFIG = {
        "DATAGEN_TARGET_JDBC_URL": "jdbc:oracle:thin:@host",
        "DATAGEN_TARGET_DB_USER": "ADMIN",
        "DATAGEN_TARGET_DB_PASSWORD": "secret",
        "DATAGEN_JDBC_READ_TIMEOUT_MS": "600000",
        "DATAGEN_JDBC_NUM_PARTITIONS": "256",
    }

    def test_base_connection_properties(self):
        props = load_tables.build_connection_properties(self.CONFIG)
        assert props["url"] == "jdbc:oracle:thin:@host"
        assert props["user"] == "ADMIN"
        assert props["password"] == "secret"
        assert props["driver"] == "oracle.jdbc.OracleDriver"
        assert props["oracle.jdbc.ReadTimeout"] == "600000"

    def test_omits_write_only_options(self):
        # batchsize / isolationLevel are applied at the write call, not in the
        # base properties (which are also reused for metadata SELECTs).
        props = load_tables.build_connection_properties(self.CONFIG)
        assert "batchsize" not in props
        assert "isolationLevel" not in props

    def test_resolve_num_partitions(self):
        assert load_tables.resolve_num_partitions(self.CONFIG) == 256


class TestPositiveInt:
    def test_accepts_positive(self):
        assert load_tables.positive_int("100") == 100

    def test_rejects_non_integer(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("abc")

    def test_rejects_zero_and_negative(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("0")
        with pytest.raises(argparse.ArgumentTypeError):
            load_tables.positive_int("-5")


SPECS = {
    "ENTIDADE": {"pk_cols": ["NUM_ID_ENTIDADE"]},
    "TIPO_DEBITO": {"pk_cols": ["COD_TIPO_DEBITO"], "static": True},
    "LANCAMENTO": {"pk_cols": ["NUM_ID_LANCAMENTO"]},
}


class TestPkColsFor:
    def test_returns_pk_cols(self):
        assert load_tables.pk_cols_for(SPECS, "LANCAMENTO") == ["NUM_ID_LANCAMENTO"]

    def test_matches_schema_qualified_and_case(self):
        assert load_tables.pk_cols_for(SPECS, "cetip.lancamento") == ["NUM_ID_LANCAMENTO"]

    def test_empty_when_absent(self):
        assert load_tables.pk_cols_for(SPECS, "NOPE") == []


class TestIsStatic:
    def test_true_for_static(self):
        assert load_tables.is_static(SPECS, "TIPO_DEBITO") is True

    def test_false_for_non_static(self):
        assert load_tables.is_static(SPECS, "ENTIDADE") is False

    def test_false_when_absent(self):
        assert load_tables.is_static(SPECS, "NOPE") is False


class TestResolveLoadTables:
    def test_requested_drops_static_keeps_order(self):
        assert load_tables.resolve_load_tables(
            SPECS, ["LANCAMENTO", "TIPO_DEBITO", "ENTIDADE"]
        ) == ["LANCAMENTO", "ENTIDADE"]

    def test_requested_table_absent_is_kept(self):
        assert load_tables.resolve_load_tables(SPECS, ["OTHER"]) == ["OTHER"]

    def test_none_returns_all_non_static_in_order(self):
        assert load_tables.resolve_load_tables(SPECS, None) == ["ENTIDADE", "LANCAMENTO"]

    def test_empty_result_exits(self):
        with pytest.raises(SystemExit):
            load_tables.resolve_load_tables(SPECS, ["TIPO_DEBITO"])


FK_SPECS = {
    # child whose PK is also its FK to CONDICAO_IF (declared child-first on purpose)
    "JUROS_FLUTUANTE": {
        "pk_cols": ["NUM_CONDICAO_IF"],
        "foreign_keys": [
            {"columns": ["NUM_CONDICAO_IF"], "parent_table": "CONDICAO_IF",
             "parent_columns": ["NUM_CONDICAO_IF"]}
        ],
    },
    "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
}


class TestTopoSortForLoad:
    def test_parent_before_child_regardless_of_input_order(self):
        assert load_tables.topo_sort_for_load(
            FK_SPECS, ["JUROS_FLUTUANTE", "CONDICAO_IF"]
        ) == ["CONDICAO_IF", "JUROS_FLUTUANTE"]

    def test_schema_qualified_names_are_resolved(self):
        assert load_tables.topo_sort_for_load(
            FK_SPECS, ["CETIP.JUROS_FLUTUANTE", "cetip.condicao_if"]
        ) == ["cetip.condicao_if", "CETIP.JUROS_FLUTUANTE"]

    def test_independent_and_absent_tables_keep_position(self):
        # OTHER is absent from specs (no FK metadata) and independent -> it keeps
        # its input position; only the FK pair is reordered parent-first.
        assert load_tables.topo_sort_for_load(
            FK_SPECS, ["OTHER", "JUROS_FLUTUANTE", "CONDICAO_IF"]
        ) == ["OTHER", "CONDICAO_IF", "JUROS_FLUTUANTE"]

    def test_self_reference_does_not_break(self):
        specs = {"USUARIO": {"pk_cols": ["NUM_ID_ENTIDADE"], "foreign_keys": [
            {"columns": ["NUM_ID_SUP"], "parent_table": "USUARIO",
             "parent_columns": ["NUM_ID_ENTIDADE"]}]}}
        assert load_tables.topo_sort_for_load(specs, ["USUARIO"]) == ["USUARIO"]

    def test_cycle_returns_each_table_once(self):
        specs = {
            "A": {"pk_cols": ["AID"], "foreign_keys": [
                {"columns": ["BID"], "parent_table": "B", "parent_columns": ["BID"]}]},
            "B": {"pk_cols": ["BID"], "foreign_keys": [
                {"columns": ["AID"], "parent_table": "A", "parent_columns": ["AID"]}]},
        }
        out = load_tables.topo_sort_for_load(specs, ["A", "B"])
        assert sorted(out) == ["A", "B"]
        assert len(out) == 2


class TestResolveLoadTablesTopoOrder:
    def test_requested_child_first_is_reordered_parent_first(self):
        assert load_tables.resolve_load_tables(
            FK_SPECS, ["JUROS_FLUTUANTE", "CONDICAO_IF"]
        ) == ["CONDICAO_IF", "JUROS_FLUTUANTE"]

    def test_all_non_static_path_is_topo_ordered(self):
        assert load_tables.resolve_load_tables(FK_SPECS, None) == [
            "CONDICAO_IF", "JUROS_FLUTUANTE"]


class TestGuardApplies:
    def test_single_numeric_true(self):
        assert load_tables.guard_applies(["NUM_ID"], True) is True

    def test_single_non_numeric_false(self):
        assert load_tables.guard_applies(["COD_X"], False) is False

    def test_composite_false(self):
        assert load_tables.guard_applies(["A", "B"], True) is False

    def test_empty_false(self):
        assert load_tables.guard_applies([], True) is False


class TestNormalizePkBound:
    def test_collapses_integral_decimal(self):
        # The NUM_ID_LANCAMENTO case: Decimal scale on an integer ID.
        result = load_tables.normalize_pk_bound(Decimal("8044070030.000000000"))
        assert result == 8044070030
        assert isinstance(result, int)

    def test_collapses_integral_float(self):
        assert load_tables.normalize_pk_bound(8044070030.0) == 8044070030
        assert isinstance(load_tables.normalize_pk_bound(8044070030.0), int)

    def test_keeps_fractional_values(self):
        assert load_tables.normalize_pk_bound(Decimal("1.5")) == Decimal("1.5")

    def test_passes_int_through(self):
        assert load_tables.normalize_pk_bound(42) == 42

    def test_normalized_bound_builds_integer_sql(self):
        lo = load_tables.normalize_pk_bound(Decimal("8044070030.000000000"))
        q = load_tables.build_existing_keys_query("ADMIN", "LANCAMENTO", "NUM_ID", lo, lo)
        assert "BETWEEN 8044070030 AND 8044070030" in q
        assert ".000000000" not in q


class TestBuildExistingKeysQuery:
    def test_builds_bounded_subquery(self):
        q = load_tables.build_existing_keys_query("ADMIN", "LANCAMENTO", "NUM_ID", 10, 99)
        assert q == (
            "(SELECT NUM_ID FROM ADMIN.LANCAMENTO "
            "WHERE NUM_ID BETWEEN 10 AND 99) DATAGEN_KEYS"
        )

    def test_accepts_decimal_bounds(self):
        q = load_tables.build_existing_keys_query(
            "ADMIN", "T", "PK", Decimal("5"), Decimal("9")
        )
        assert "BETWEEN 5 AND 9" in q

    def test_rejects_non_numeric_bounds(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "PK", "5", "9")

    def test_rejects_boolean_bounds(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "PK", True, False)

    def test_rejects_bad_identifiers(self):
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T; DROP", "PK", 1, 2)
        with pytest.raises(ValueError):
            load_tables.build_existing_keys_query("ADMIN", "T", "P K", 1, 2)


class TestManifest:
    CFG = {"DATAGEN_LOAD_BASE_URI": "oci://bucket@ns/load"}

    def test_manifest_path(self):
        assert load_tables.manifest_path(self.CFG, "20260617T120000Z") == (
            "oci://bucket@ns/load/_load_manifests/20260617T120000Z"
        )

    def test_build_manifest_shape(self):
        entries = [{"table": "LANCAMENTO", "rollbackable": True}]
        m = load_tables.build_manifest("RID", "2026-06-17T12:00:00Z", "CETIP", entries)
        assert m == {
            "run_id": "RID",
            "created_utc": "2026-06-17T12:00:00Z",
            "target_schema": "CETIP",
            "tables": entries,
        }
