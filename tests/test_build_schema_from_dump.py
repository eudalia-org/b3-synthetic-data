import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import build_schema_from_dump as bsd  # noqa: E402


def _write_csv(tmp_path, name, header, rows):
    p = tmp_path / name
    lines = [",".join(header)] + [",".join(str(c) for c in r) for r in rows]
    p.write_text("\n".join(lines) + "\n")
    return str(p)


class TestParseColumnsCsv:
    def test_parses_and_normalizes_header_case(self, tmp_path):
        path = _write_csv(
            tmp_path, "columns.csv",
            ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_PRECISION",
             "DATA_SCALE", "CHAR_LENGTH", "NULLABLE"],
            [["CETIP.JUROS_FLUTUANTE", "NUM_CONDICAO_IF", "NUMBER", 38, 0, 0, "N"],
             ["CETIP.JUROS_FLUTUANTE", "COD_X", "VARCHAR2", "", "", 20, "Y"]],
        )
        rows = bsd.parse_columns_csv(path)
        assert len(rows) == 2
        # NOTE: parse_columns_csv does NOT strip the OWNER. prefix from
        # TABLE_NAME; owner-stripping is deferred to build_schema (Task 2).
        assert rows[0]["COLUMN_NAME"] == "NUM_CONDICAO_IF"
        assert rows[0]["NULLABLE"] == "N"
