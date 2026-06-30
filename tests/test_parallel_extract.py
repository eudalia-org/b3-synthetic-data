import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import parallel_extract as P  # noqa: E402


class TestJdbcUrlToDsn:
    def test_sid_colon_form(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:1521:ORCL") == "dbhost:1521/ORCL"

    def test_service_slashes_form(self):
        assert P.jdbc_url_to_dsn(
            "jdbc:oracle:thin:@//dbhost:1521/PROD.cetip") == "dbhost:1521/PROD.cetip"

    def test_default_port_when_absent(self):
        assert P.jdbc_url_to_dsn("jdbc:oracle:thin:@dbhost:ORCL") == "dbhost:1521/ORCL"

    def test_rejects_non_jdbc(self):
        with pytest.raises(ValueError):
            P.jdbc_url_to_dsn("postgres://x")


class TestOwnerSplit:
    def test_qualified(self):
        assert P.split_owner_table("CETIP.OPERACAO", "DEFOWNER") == ("CETIP", "OPERACAO")

    def test_unqualified_uses_default(self):
        assert P.split_owner_table("OPERACAO", "CETIP") == ("CETIP", "OPERACAO")

    def test_uppercases(self):
        assert P.split_owner_table("cetip.operacao", "x") == ("CETIP", "OPERACAO")


class TestValidIdentifier:
    def test_accepts(self):
        assert P.valid_identifier("CETIP") == "CETIP"

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            P.valid_identifier("OPER; DROP TABLE X")


class TestParseTables:
    def test_comma_split_and_dedup(self):
        assert P.parse_tables("A, B ,A", None) == ["A", "B"]

    def test_file_with_comments_and_blanks(self, tmp_path):
        f = tmp_path / "t.txt"
        f.write_text("A\n# comment\n\nB\n")
        assert P.parse_tables(None, str(f)) == ["A", "B"]
