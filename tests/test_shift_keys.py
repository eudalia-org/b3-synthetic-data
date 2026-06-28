# tests/test_shift_keys.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import shift_keys  # noqa: E402

import pytest  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.appName("shift-keys-test")
               .master("local[2]").config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    yield session
    session.stop()


class TestComputeShiftColumns:
    def test_nonstatic_pk_shifts(self):
        specs = {"OPERACAO": {"pk_cols": ["NUM_OPER"]}}
        assert shift_keys.compute_shift_columns(specs) == {"OPERACAO": ["NUM_OPER"]}

    def test_static_pk_not_shifted(self):
        specs = {"TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True}}
        assert shift_keys.compute_shift_columns(specs) == {}

    def test_fk_to_nonstatic_parent_shifts(self):
        specs = {
            "INSTRUMENTO_FINANCEIRO": {"pk_cols": ["NUM_IF"]},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_IF"],
                                           "parent_table": "INSTRUMENTO_FINANCEIRO"}]},
        }
        out = shift_keys.compute_shift_columns(specs)
        assert sorted(out["OPERACAO"]) == ["NUM_IF", "NUM_OPER"]
        assert out["INSTRUMENTO_FINANCEIRO"] == ["NUM_IF"]

    def test_fk_to_static_parent_not_shifted(self):
        specs = {
            "TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_TIPO_IF"],
                                           "parent_table": "TIPO_IF"}]},
        }
        # NUM_TIPO_IF references a static parent -> not shifted; only NUM_OPER shifts
        assert shift_keys.compute_shift_columns(specs) == {"OPERACAO": ["NUM_OPER"]}

    def test_shared_key_child_of_static_parent_pk_not_shifted(self):
        # PK == FK to a static parent: FK-to-static wins, PK kept matched
        specs = {
            "CODE": {"pk_cols": ["COD"], "static": True},
            "EXT": {"pk_cols": ["COD"],
                    "foreign_keys": [{"columns": ["COD"], "parent_table": "CODE"}]},
        }
        assert shift_keys.compute_shift_columns(specs) == {}

    def test_shared_key_child_of_nonstatic_parent_shifts_once(self):
        # PK == FK to a non-static parent: shifts (deduped to one column)
        specs = {
            "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
            "RESGATE": {"pk_cols": ["NUM_CONDICAO_IF"],
                        "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                          "parent_table": "CONDICAO_IF"}]},
        }
        out = shift_keys.compute_shift_columns(specs)
        assert out["RESGATE"] == ["NUM_CONDICAO_IF"]

    def test_real_specs_yields_31_columns(self):
        import json
        specs = json.load(open(Path(__file__).resolve().parent.parent / "specs.json"))
        out = shift_keys.compute_shift_columns(specs)
        total = sum(len(v) for v in out.values())
        assert total == 31


class TestShiftTable:
    def test_shifts_listed_columns_and_preserves_others(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("NUM_OPER", T.LongType()),
            T.StructField("NUM_IF", T.LongType()),
            T.StructField("DESC", T.StringType()),
        ])
        df = spark.createDataFrame([(1, 10, "a"), (2, 20, "b")], schema)
        out = shift_keys.shift_table(df, ["NUM_OPER", "NUM_IF"], 1000)
        rows = {r["DESC"]: (r["NUM_OPER"], r["NUM_IF"]) for r in out.collect()}
        assert rows == {"a": (1001, 1010), "b": (1002, 1020)}

    def test_preserves_dtype(self, spark):
        from decimal import Decimal
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 9))])
        df = spark.createDataFrame([(Decimal("1"),)], schema)
        out = shift_keys.shift_table(df, ["K"], 5)
        assert out.schema["K"].dataType == T.DecimalType(38, 9)
        assert int(out.collect()[0]["K"]) == 6

    def test_null_fk_stays_null(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("FK", T.LongType())])
        df = spark.createDataFrame([(5,), (None,)], schema)
        out = shift_keys.shift_table(df, ["FK"], 100)
        vals = sorted([r["FK"] for r in out.collect()], key=lambda x: (x is None, x))
        assert vals == [105, None]


class TestCheckOverflow:
    def _write(self, spark, tmp_path, name, schema, rows):
        df = spark.createDataFrame(rows, schema)
        df.write.parquet(str(tmp_path / name))

    def test_no_overflow_returns_empty(self, spark, tmp_path):
        from decimal import Decimal
        from pyspark.sql import types as T
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 0))])
        self._write(spark, tmp_path, "T", schema, [(Decimal("10"),), (Decimal("20"),)])
        shift = {"T": ["K"]}
        assert shift_keys.check_overflow(spark, str(tmp_path), shift, 1000) == []

    def test_overflow_detected_for_tight_domain(self, spark, tmp_path):
        from decimal import Decimal
        from pyspark.sql import types as T
        # Decimal(2,0) capacity = 99; max is 90, +20 = 110 > 99 -> overflow
        schema = T.StructType([T.StructField("K", T.DecimalType(2, 0))])
        self._write(spark, tmp_path, "T", schema, [(Decimal("90"),)])
        shift = {"T": ["K"]}
        out = shift_keys.check_overflow(spark, str(tmp_path), shift, 20)
        assert len(out) == 1
        table, col, mx, shifted, cap = out[0]
        assert (table, col, mx, shifted, cap) == ("T", "K", 90, 110, 99)

    def test_capacity_override_wins_over_parquet(self, spark, tmp_path):
        from decimal import Decimal
        from pyspark.sql import types as T
        # Parquet dtype Decimal(38,0) is huge, but the live Oracle capacity is 200;
        # max 150 + 100 = 250 > 200 -> overflow detected only via the override.
        schema = T.StructType([T.StructField("K", T.DecimalType(38, 0))])
        self._write(spark, tmp_path, "T", schema, [(Decimal("150"),)])
        out = shift_keys.check_overflow(spark, str(tmp_path), {"T": ["K"]}, 100,
                                        capacity_override={("T", "K"): 200})
        assert out == [("T", "K", 150, 250, 200)]


class TestOraclePreflight:
    def test_capacity_from_precision_scale(self):
        assert shift_keys.capacity_from_precision_scale(2, 0) == 99
        assert shift_keys.capacity_from_precision_scale(5, 0) == 99999
        assert shift_keys.capacity_from_precision_scale(10, 2) == 10**8 - 1
        # NULL precision (unconstrained NUMBER) -> no limit
        assert shift_keys.capacity_from_precision_scale(None, None) is None

    def test_oracle_column_capacities_filters_to_shift_set(self):
        # rows mimic ALL_TAB_COLUMNS: (TABLE_NAME, COLUMN_NAME, DATA_PRECISION, DATA_SCALE)
        rows = [
            ("OPERACAO", "NUM_OPER", 12, 0),
            ("OPERACAO", "IGNORED", 5, 0),      # not in shift set -> dropped
            ("INSTRUMENTO_FINANCEIRO", "NUM_IF", None, None),  # unconstrained -> skipped
        ]
        shift = {"OPERACAO": ["NUM_OPER"], "INSTRUMENTO_FINANCEIRO": ["NUM_IF"]}
        out = shift_keys.oracle_column_capacities(rows, shift)
        assert out == {("OPERACAO", "NUM_OPER"): 10**12 - 1}

    def test_find_collisions(self):
        prod_max = {"OPERACAO": 5000, "CONDICAO_IF": 100}
        synth_min = {"OPERACAO": 1, "CONDICAO_IF": 1}
        # offset 4000: OPERACAO 1+4000=4001 <= 5000 -> collision; CONDICAO_IF 4001 > 100 -> ok
        out = shift_keys.find_collisions(prod_max, synth_min, 4000)
        assert out == [("OPERACAO", 5000, 4001)]

    def test_find_collisions_none_when_offset_clears(self):
        assert shift_keys.find_collisions({"T": 100}, {"T": 1}, 1000) == []
