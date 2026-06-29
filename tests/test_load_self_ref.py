import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datagen import load_tables as L  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.appName("load-selfref-test").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


class TestNullSelfRefColumns:
    def _if_df(self, spark):
        from pyspark.sql import types as T
        schema = T.StructType([
            T.StructField("NUM_IF", T.LongType()),
            T.StructField("NUM_IF_ORIGEM", T.LongType()),
            T.StructField("NUM_IF_PERTENCE", T.LongType()),
            T.StructField("DESC", T.StringType()),
        ])
        return spark.createDataFrame([(1, 7, 8, "a"), (2, 9, 10, "b")], schema)

    def test_nulls_listed_columns_keeps_others(self, spark):
        out = L.null_self_ref_columns(
            self._if_df(spark), "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        rows = {r["DESC"]: (r["NUM_IF"], r["NUM_IF_ORIGEM"], r["NUM_IF_PERTENCE"])
                for r in out.collect()}
        assert rows == {"a": (1, None, None), "b": (2, None, None)}

    def test_dtype_preserved(self, spark):
        out = L.null_self_ref_columns(
            self._if_df(spark), "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        from pyspark.sql import types as T
        assert out.schema["NUM_IF_ORIGEM"].dataType == T.LongType()

    def test_other_table_unchanged(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1, 7)], T.StructType([T.StructField("NUM_IF", T.LongType()),
                                    T.StructField("NUM_IF_ORIGEM", T.LongType())]))
        out = L.null_self_ref_columns(df, "OPERACAO", L.NULL_ON_INSERT)
        assert out.collect()[0]["NUM_IF_ORIGEM"] == 7  # not in map -> untouched

    def test_missing_column_skipped(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1,)], T.StructType([T.StructField("NUM_IF", T.LongType())]))
        # listed cols absent -> no error, df unchanged
        out = L.null_self_ref_columns(df, "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        assert out.collect()[0]["NUM_IF"] == 1

    def test_case_insensitive_match(self, spark):
        from pyspark.sql import types as T
        df = spark.createDataFrame(
            [(1, 7)], T.StructType([T.StructField("num_if", T.LongType()),
                                    T.StructField("num_if_origem", T.LongType())]))
        out = L.null_self_ref_columns(df, "INSTRUMENTO_FINANCEIRO", L.NULL_ON_INSERT)
        assert out.collect()[0]["num_if_origem"] is None

    def test_constant_contents(self):
        assert L.NULL_ON_INSERT == {
            "INSTRUMENTO_FINANCEIRO": ["NUM_IF_ORIGEM", "NUM_IF_PERTENCE"]}
