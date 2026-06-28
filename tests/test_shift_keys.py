# tests/test_shift_keys.py
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pytest  # noqa: E402

from datagen import shift_keys  # noqa: E402


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

    def test_fk_to_static_pk_logs_warning_and_excludes(self, caplog):
        specs = {
            "CODE": {"pk_cols": ["COD"], "static": True},
            "EXT": {"pk_cols": ["COD"],
                    "foreign_keys": [{"columns": ["COD"], "parent_table": "CODE"}]},
        }
        with caplog.at_level(logging.WARNING, logger="datagen.shift_keys"):
            out = shift_keys.compute_shift_columns(specs)
        assert out == {}  # column excluded from the shift set
        assert any("EXT.COD" in r.getMessage() and "NOT shifting" in r.getMessage()
                   for r in caplog.records)

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
        # scale >= precision leaves no integer digits -> capacity 0
        assert shift_keys.capacity_from_precision_scale(2, 2) == 0
        assert shift_keys.capacity_from_precision_scale(3, 5) == 0
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


class TestApplyShift:
    def test_in_place_shift_preserves_fk_integrity(self, spark, tmp_path):
        from pyspark.sql import types as T
        base = str(tmp_path / "syn")
        # CONDICAO_IF (non-static parent), RESGATE (shared-key child),
        # OPERACAO (child with FK to CONDICAO_IF), TIPO_IF (static)
        spark.createDataFrame([(1,), (2,), (3,)],
            T.StructType([T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/CONDICAO_IF")
        spark.createDataFrame([(1,), (2,)],
            T.StructType([T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/RESGATE")
        spark.createDataFrame([(10, 1), (11, 2)],
            T.StructType([T.StructField("NUM_OPER", T.LongType()),
                          T.StructField("NUM_CONDICAO_IF", T.LongType())])
        ).write.parquet(f"{base}/OPERACAO")
        spark.createDataFrame([(46,)],
            T.StructType([T.StructField("NUM_TIPO_IF", T.LongType())])
        ).write.parquet(f"{base}/TIPO_IF")

        specs = {
            "TIPO_IF": {"pk_cols": ["NUM_TIPO_IF"], "static": True},
            "CONDICAO_IF": {"pk_cols": ["NUM_CONDICAO_IF"]},
            "RESGATE": {"pk_cols": ["NUM_CONDICAO_IF"],
                        "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                          "parent_table": "CONDICAO_IF"}]},
            "OPERACAO": {"pk_cols": ["NUM_OPER"],
                         "foreign_keys": [{"columns": ["NUM_CONDICAO_IF"],
                                           "parent_table": "CONDICAO_IF"}]},
        }
        shift = shift_keys.compute_shift_columns(specs)
        failures = shift_keys.apply_shift(spark, base, shift, 1000,
                                          continue_on_error=False,
                                          reliable_checkpoint=False)
        assert failures == []

        cond = spark.read.parquet(f"{base}/CONDICAO_IF")
        oper = spark.read.parquet(f"{base}/OPERACAO")
        resg = spark.read.parquet(f"{base}/RESGATE")
        tipo = spark.read.parquet(f"{base}/TIPO_IF")

        # parent PK shifted
        assert sorted(r["NUM_CONDICAO_IF"] for r in cond.collect()) == [1001, 1002, 1003]
        # child FK shifted by same N -> still joins parent
        assert oper.join(cond, "NUM_CONDICAO_IF", "left_anti").count() == 0
        assert sorted(r["NUM_OPER"] for r in oper.collect()) == [1010, 1011]
        # shared-key child shifted, still matches parent
        assert resg.join(cond, "NUM_CONDICAO_IF", "left_anti").count() == 0
        # static table untouched
        assert [r["NUM_TIPO_IF"] for r in tipo.collect()] == [46]

    def _setup_good_and_bad(self, spark, tmp_path):
        from pyspark.sql import types as T
        base = str(tmp_path / "syn")
        spark.createDataFrame([(1,), (2,)],
            T.StructType([T.StructField("K", T.LongType())])
        ).write.parquet(f"{base}/T_GOOD")
        spark.createDataFrame([(7,)],
            T.StructType([T.StructField("K", T.LongType())])
        ).write.parquet(f"{base}/T_BAD")
        # T_BAD's shift targets a column that does not exist -> shift_table raises.
        shift = {"T_GOOD": ["K"], "T_BAD": ["NOPE"]}
        return base, shift

    def test_continue_on_error_isolates_failure(self, spark, tmp_path):
        base, shift = self._setup_good_and_bad(spark, tmp_path)
        failures = shift_keys.apply_shift(spark, base, shift, 100,
                                          continue_on_error=True,
                                          reliable_checkpoint=False)
        assert failures == ["T_BAD"]
        # The good table was still shifted in place.
        good = spark.read.parquet(f"{base}/T_GOOD")
        assert sorted(r["K"] for r in good.collect()) == [101, 102]

    def test_stop_on_error_reraises(self, spark, tmp_path):
        base, shift = self._setup_good_and_bad(spark, tmp_path)
        with pytest.raises(Exception):
            shift_keys.apply_shift(spark, base, shift, 100,
                                   continue_on_error=False,
                                   reliable_checkpoint=False)


class TestEnvAndCli:
    def test_get_shift_env_requires_synthetic_and_specs(self, monkeypatch):
        monkeypatch.delenv("DATAGEN_SYNTHETIC_BASE_URI", raising=False)
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        with pytest.raises(SystemExit):
            shift_keys.get_shift_env()

    def test_get_shift_env_ok(self, monkeypatch):
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://b@n/syn/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        monkeypatch.delenv("DATAGEN_CHECKPOINT_URI", raising=False)
        cfg = shift_keys.get_shift_env()
        assert cfg["DATAGEN_SYNTHETIC_BASE_URI"] == "oci://b@n/syn"  # trailing / stripped
        assert cfg["DATAGEN_SPECS_URI"] == "oci://b@n/specs.json"
        assert cfg.get("DATAGEN_CHECKPOINT_URI") in (None, "")

    def test_parse_arguments_offset_required(self):
        with pytest.raises(SystemExit):
            shift_keys.parse_arguments([])

    def test_parse_arguments_values(self):
        args = shift_keys.parse_arguments(["--offset", "1000000", "--dry-run"])
        assert args.offset == 1000000 and args.dry_run is True
        assert args.continue_on_error is False

    def test_parse_arguments_rejects_zero_offset(self):
        with pytest.raises(SystemExit):
            shift_keys.parse_arguments(["--offset", "0"])

    def test_parse_arguments_rejects_negative_offset(self):
        with pytest.raises(SystemExit):
            shift_keys.parse_arguments(["--offset", "-5"])

    def test_oracle_props_none_without_env(self, monkeypatch):
        for k in ("DATAGEN_SOURCE_JDBC_URL", "DATAGEN_SOURCE_DB_USER",
                  "DATAGEN_SOURCE_DB_PASSWORD"):
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("DATAGEN_SYNTHETIC_BASE_URI", "oci://b@n/syn/")
        monkeypatch.setenv("DATAGEN_SPECS_URI", "oci://b@n/specs.json")
        cfg = shift_keys.get_shift_env()
        assert cfg["DATAGEN_ORACLE_OWNER"] == "CETIP"
        assert shift_keys.oracle_props_or_none(cfg) is None

    def test_deployment_summary_lists_env_and_config(self, capsys):
        shift_keys.print_deployment_summary({"DATAGEN_SYNTHETIC_BASE_URI": "oci://b@n/syn"})
        out = capsys.readouterr().out
        assert "DATAGEN_SYNTHETIC_BASE_URI" in out
        assert "DATAGEN_SPECS_URI" in out
        assert "DATAGEN_CHECKPOINT_URI" in out
        assert "DATAGEN_SOURCE_JDBC_URL" in out
        assert "DATAGEN_ORACLE_OWNER" in out
        assert "datagen/shift_keys.py" in out
        assert "--offset" in out
