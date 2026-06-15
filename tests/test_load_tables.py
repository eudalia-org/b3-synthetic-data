import os

import pytest

import load_tables


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


class TestSqlBuilders:
    def test_truncate_sql(self):
        assert load_tables.truncate_sql("admin", "orders") == "TRUNCATE TABLE ADMIN.ORDERS"

    def test_disable_constraint_sql(self):
        assert (
            load_tables.disable_constraint_sql("ADMIN", "ORDERS", "FK_CUST")
            == "ALTER TABLE ADMIN.ORDERS DISABLE CONSTRAINT FK_CUST"
        )

    def test_enable_constraint_sql_novalidate(self):
        assert (
            load_tables.enable_constraint_sql("ADMIN", "ORDERS", "FK_CUST", validate=False)
            == "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST"
        )

    def test_enable_constraint_sql_validate(self):
        assert (
            load_tables.enable_constraint_sql("ADMIN", "ORDERS", "FK_CUST", validate=True)
            == "ALTER TABLE ADMIN.ORDERS ENABLE VALIDATE CONSTRAINT FK_CUST"
        )

    def test_discovery_query_includes_incoming_and_outgoing(self):
        query = load_tables.build_constraint_discovery_query("ADMIN", "ORDERS")
        assert "all_constraints" in query
        assert "p.owner = 'ADMIN' AND p.table_name = 'ORDERS'" in query  # incoming
        assert "owner = 'ADMIN' AND table_name = 'ORDERS'" in query      # outgoing
        assert "UNION" in query

    def test_builders_reject_bad_identifiers(self):
        with pytest.raises(ValueError):
            load_tables.truncate_sql("ADMIN", "ORDERS; DROP")
        with pytest.raises(ValueError):
            load_tables.disable_constraint_sql("ADMIN", "ORDERS", "X'); DROP")
        with pytest.raises(ValueError):
            load_tables.build_constraint_discovery_query("ADMIN", "O'R")


class TestConstraintsDisabled:
    CONSTRAINTS = [("ADMIN", "ORDERS", "FK_CUST"), ("SALES", "INVOICES", "FK_ORD")]

    def test_disables_then_reenables_in_order(self):
        calls = []
        with load_tables.constraints_disabled(calls.append, self.CONSTRAINTS, validate=False):
            calls.append("BODY")
        assert calls == [
            "ALTER TABLE ADMIN.ORDERS DISABLE CONSTRAINT FK_CUST",
            "ALTER TABLE SALES.INVOICES DISABLE CONSTRAINT FK_ORD",
            "BODY",
            "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST",
            "ALTER TABLE SALES.INVOICES ENABLE NOVALIDATE CONSTRAINT FK_ORD",
        ]

    def test_reenables_even_when_body_raises(self):
        calls = []
        with pytest.raises(RuntimeError):
            with load_tables.constraints_disabled(calls.append, self.CONSTRAINTS, validate=False):
                raise RuntimeError("load failed")
        # Both disabled constraints must still be re-enabled.
        assert "ALTER TABLE ADMIN.ORDERS ENABLE NOVALIDATE CONSTRAINT FK_CUST" in calls
        assert "ALTER TABLE SALES.INVOICES ENABLE NOVALIDATE CONSTRAINT FK_ORD" in calls

    def test_empty_constraints_is_noop(self):
        calls = []
        with load_tables.constraints_disabled(calls.append, [], validate=False):
            calls.append("BODY")
        assert calls == ["BODY"]


class TestDiscoverConstraints:
    def test_maps_rows_to_tuples(self, monkeypatch):
        monkeypatch.setattr(
            load_tables,
            "read_rows",
            lambda spark, props, query: [
                ("ADMIN", "ORDERS", "FK_CUST"),
                ("SALES", "INVOICES", "FK_ORD"),
            ],
        )
        result = load_tables.discover_constraints(None, {}, "ADMIN", "ORDERS")
        assert result == [
            ("ADMIN", "ORDERS", "FK_CUST"),
            ("SALES", "INVOICES", "FK_ORD"),
        ]

    def test_empty_when_no_constraints(self, monkeypatch):
        monkeypatch.setattr(load_tables, "read_rows", lambda *a: [])
        assert load_tables.discover_constraints(None, {}, "ADMIN", "ORDERS") == []
