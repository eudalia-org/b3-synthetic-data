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
