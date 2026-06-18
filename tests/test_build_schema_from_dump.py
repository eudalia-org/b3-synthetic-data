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


class TestBuildSchemaColumns:
    def test_columns_typed_and_table_stripped(self):
        col_rows = [
            {"TABLE_NAME": "CETIP.T", "COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "38", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
            {"TABLE_NAME": "CETIP.T", "COLUMN_NAME": "NAME", "DATA_TYPE": "VARCHAR2",
             "DATA_PRECISION": "", "DATA_SCALE": "", "CHAR_LENGTH": "20", "NULLABLE": "Y"},
        ]
        schema = bsd.build_schema(col_rows, constraint_rows=[])
        assert set(schema.keys()) == {"T"}
        cols = schema["T"]["columns"]
        assert cols["ID"] == {"type": "NUMBER", "precision": 38, "scale": 0, "nullable": False}
        assert cols["NAME"] == {"type": "VARCHAR2", "length": 20, "nullable": True}
        assert "precision" not in cols["NAME"]  # VARCHAR carries length, not precision


class TestBuildSchemaUnique:
    def test_composite_unique_paired_by_position(self):
        col_rows = [
            {"TABLE_NAME": "T", "COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
            {"TABLE_NAME": "T", "COLUMN_NAME": "B", "DATA_TYPE": "NUMBER",
             "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0", "NULLABLE": "N"},
        ]
        constraint_rows = [
            {"CONSTRAINT_TYPE": "U", "CONSTRAINT_NAME": "T_UK", "TABLE_NAME": "T",
             "COLUMN_NAME": "B", "COL_POSITION": "2"},
            {"CONSTRAINT_TYPE": "U", "CONSTRAINT_NAME": "T_UK", "TABLE_NAME": "T",
             "COLUMN_NAME": "A", "COL_POSITION": "1"},
            {"CONSTRAINT_TYPE": "P", "CONSTRAINT_NAME": "T_PK", "TABLE_NAME": "T",
             "COLUMN_NAME": "A", "COL_POSITION": "1"},  # ignored
        ]
        schema = bsd.build_schema(col_rows, constraint_rows)
        assert schema["T"]["unique"] == [["A", "B"]]  # ordered by position

    def test_no_unique_key_omits_field(self):
        col_rows = [{"TABLE_NAME": "T", "COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
                     "DATA_PRECISION": "5", "DATA_SCALE": "0", "CHAR_LENGTH": "0",
                     "NULLABLE": "N"}]
        schema = bsd.build_schema(col_rows, constraint_rows=[])
        assert "unique" not in schema["T"]
