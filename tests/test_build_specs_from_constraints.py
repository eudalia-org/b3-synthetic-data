import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import build_specs_from_constraints as bsc  # noqa: E402


def _row(name, ctype, table, ref, col, pos):
    return {
        "CONSTRAINT_NAME": name, "CONSTRAINT_TYPE": ctype, "TABLE_NAME": table,
        "R_CONSTRAINT_NAME": ref, "COLUMN_NAME": col, "COL_POSITION": str(pos),
    }


class TestBuildSpecs:
    def test_pk_and_simple_fk(self):
        rows = [
            _row("CUST_PK", "P", "CUSTOMERS", "", "CID", 1),
            _row("ORD_PK", "P", "ORDERS", "", "OID", 1),
            _row("ORD_CUST_FK", "R", "ORDERS", "CUST_PK", "CID", 1),
        ]
        specs = bsc.build_specs(rows)
        assert specs["CUSTOMERS"] == {"pk_cols": ["CID"]}
        assert specs["ORDERS"]["pk_cols"] == ["OID"]
        assert specs["ORDERS"]["foreign_keys"] == [
            {"columns": ["CID"], "parent_table": "CUSTOMERS", "parent_columns": ["CID"]}
        ]

    def test_composite_fk_paired_by_position(self):
        rows = [
            _row("P_PK", "P", "PARENT", "", "A", 1),
            _row("P_PK", "P", "PARENT", "", "B", 2),
            _row("C_PK", "P", "CHILD", "", "ID", 1),
            # positions deliberately out of order to test sorting
            _row("C_FK", "R", "CHILD", "P_PK", "FB", 2),
            _row("C_FK", "R", "CHILD", "P_PK", "FA", 1),
        ]
        specs = bsc.build_specs(rows)
        fk = specs["CHILD"]["foreign_keys"][0]
        assert fk["columns"] == ["FA", "FB"]
        assert fk["parent_columns"] == ["A", "B"]
        assert fk["parent_table"] == "PARENT"

    def test_pk_equals_fk_one_to_one(self):
        # JUROS_FLUTUANTE.NUM_CONDICAO_IF is both PK and FK to CONDICAO_IF
        rows = [
            _row("COND_PK", "P", "CONDICAO_IF", "", "NUM_CONDICAO_IF", 1),
            _row("JUR_PK", "P", "JUROS_FLUTUANTE", "", "NUM_CONDICAO_IF", 1),
            _row("JUR_COND_FK", "R", "JUROS_FLUTUANTE", "COND_PK", "NUM_CONDICAO_IF", 1),
        ]
        specs = bsc.build_specs(rows)
        assert specs["JUROS_FLUTUANTE"]["pk_cols"] == ["NUM_CONDICAO_IF"]
        assert specs["JUROS_FLUTUANTE"]["foreign_keys"] == [
            {"columns": ["NUM_CONDICAO_IF"], "parent_table": "CONDICAO_IF",
             "parent_columns": ["NUM_CONDICAO_IF"]}
        ]

    def test_static_from_set_and_overrides(self):
        rows = [_row("T_PK", "P", "TIPO_DEBITO", "", "COD", 1),
                _row("O_PK", "P", "OPERACAO", "", "OID", 1)]
        specs = bsc.build_specs(rows, static_tables={"TIPO_DEBITO"},
                                overrides={"OPERACAO": {"n_rows": 5000}})
        assert specs["TIPO_DEBITO"]["static"] is True
        assert specs["OPERACAO"].get("static") is None
        assert specs["OPERACAO"]["n_rows"] == 5000

    def test_self_reference_kept_and_reported(self):
        rows = [
            _row("U_PK", "P", "USUARIO", "", "NUM_ID_ENTIDADE", 1),
            _row("U_SELF_FK", "R", "USUARIO", "U_PK", "NUM_ID_ENTIDADE_ATUALIZ", 1),
        ]
        specs = bsc.build_specs(rows)
        assert specs["USUARIO"]["foreign_keys"][0]["parent_table"] == "USUARIO"
        assert bsc.build_specs.last_report["self_refs"]  # reported

    def test_fk_to_table_without_pk_is_skipped(self):
        rows = [
            _row("C_PK", "P", "CHILD", "", "ID", 1),
            # references a constraint not present in the dump
            _row("C_FK", "R", "CHILD", "MISSING_PK", "PID", 1),
        ]
        specs = bsc.build_specs(rows)
        assert "foreign_keys" not in specs["CHILD"]
        assert bsc.build_specs.last_report["skipped"]

    def test_key_order_pk_fk_static(self):
        rows = [
            _row("P_PK", "P", "PARENT", "", "A", 1),
            _row("C_PK", "P", "CHILD", "", "ID", 1),
            _row("C_FK", "R", "CHILD", "P_PK", "PA", 1),
        ]
        specs = bsc.build_specs(rows, static_tables={"CHILD"})
        assert list(specs["CHILD"].keys()) == ["pk_cols", "foreign_keys", "static"]
