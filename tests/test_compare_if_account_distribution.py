import ast
import importlib.util
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scripts.compare_if_account_distribution import compare_if_accounts

SOURCE_SCHEMA = (
    "NUM_IF string, NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string, DAT_EXCLUSAO string"
)
SYNTHETIC_SCHEMA = "NUM_IF string, NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string"
MAP_SCHEMA = "NUM_IF_ORIG string, K string, NUM_IF_NOVO string"
BASELINES = {"full_export", "same_type", "active_same_type"}


def test_notebook_code_is_valid_and_contains_no_saved_results():
    path = Path(__file__).resolve().parents[1] / "scripts/compare_if_account_distribution.ipynb"
    notebook = json.loads(path.read_text())
    assert notebook["nbformat"] == 4
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert cell["execution_count"] is None
            assert cell["outputs"] == []
            ast.parse("".join(cell["source"]))


@pytest.mark.parametrize("include_map", [False, True])
def test_notebook_runs_against_local_parquet_and_exports_reports(
    spark, known_inputs, tmp_path, include_map
):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    notebook = json.loads((scripts / "compare_if_account_distribution.ipynb").read_text())
    source, synthetic, mapping = known_inputs
    source = source.withColumn(
        "NUM_TIPO_IF", F.when(F.col("NUM_TIPO_IF").cast("double") == 1, 49).otherwise(50)
    )
    synthetic = synthetic.withColumn("NUM_TIPO_IF", F.lit(49))
    source.write.parquet(str(tmp_path / "export" / "INSTRUMENTO_FINANCEIRO"))
    synthetic.write.parquet(str(tmp_path / "synthetic" / "INSTRUMENTO_FINANCEIRO"))
    if include_map:
        mapping.write.parquet(str(tmp_path / "synthetic" / "MAPA_CLONE_NUM_IF"))
    namespace = {
        "spark": spark,
        "RUN_ID": "local-fixture",
        "EXPORT_BASE": str(tmp_path / "export"),
        "PRODUCT_TYPES": {"cdb_simplificado": 49},
        "PRODUCTS": ["cdb_simplificado"],
        "SYNTHETIC_BASES": {"cdb_simplificado": str(tmp_path / "synthetic")},
        "HELPER_FILE": str(scripts / "compare_if_account_distribution.py"),
        "INCLUDE_CLONE_MAP": include_map,
        "BASELINE_TO_VIEW": "active_same_type",
        "TOP_N": 5,
        "REPORT_URI": str(tmp_path / "reports"),
    }
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code" and "parameters" not in cell["metadata"].get("tags", []):
            exec(compile("".join(cell["source"]), "account-notebook", "exec"), namespace)
    summary = (
        spark.read.option("header", True)
        .option("nullValue", "<NULL>")
        .csv(str(tmp_path / "reports" / "summary"))
        .collect()
    )
    expected_baselines = BASELINES | (
        {"selected_sources", "clone_weighted"} if include_map else set()
    )
    assert {row.BASELINE for row in summary} == expected_baselines
    assert {row.SYNTHETIC_TOTAL for row in summary} == {"5"}
    assert {row.CHANGED_ACCOUNT_IFS for row in summary} == ({"2"} if include_map else {None})
    assert {row.PRODUCT for row in summary} == {"cdb_simplificado"}
    assert {row.RUN_ID for row in summary} == {"local-fixture"}
    assert namespace["if_account_caches"] == []
    assert (tmp_path / "reports" / "account_changes").exists() is include_map


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("compare-if-account-distribution-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def known_inputs(spark):
    source = spark.createDataFrame(
        [
            (" 1.00 ", " A ", "1.0", None),
            ("2", "B", "1", None),
            ("3", None, "1", "deleted"),
            ("4", "C", "1", None),
            ("5", "D", "2", None),
            ("6", " ", "1", None),
        ],
        SOURCE_SCHEMA,
    )
    synthetic = spark.createDataFrame(
        [
            ("101", "A", "1"),
            ("102", "A", "1"),
            ("103", "Z", "1"),
            ("104", "", "1"),
            ("105", None, "1"),
        ],
        SYNTHETIC_SCHEMA,
    ).withColumn("DAT_EXCLUSAO", F.lit("not filtered"))
    clone_map = spark.createDataFrame(
        [
            ("1", "01.00", "101"),
            ("1.0", "2", "102"),
            ("1", "3", "103"),
            ("2", "+1", "104"),
            ("3", "1", "105.00"),
        ],
        MAP_SCHEMA,
    )
    return source, synthetic, clone_map


@pytest.fixture(scope="module")
def known_result(known_inputs):
    result = compare_if_accounts(*known_inputs, if_type=1)
    return result, {name: frame.collect() for name, frame in result.items()}


def test_known_counts_shares_selection_bias_and_clone_weighting(known_result):
    _, rows = known_result
    expected = {
        "full_export": {"A": 1, "B": 1, "C": 1, "D": 1, None: 2},
        "same_type": {"A": 1, "B": 1, "C": 1, None: 2},
        "active_same_type": {"A": 1, "B": 1, "C": 1, None: 1},
        "selected_sources": {"A": 1, "B": 1, None: 1},
        "clone_weighted": {"A": 3, "B": 1, None: 1},
    }
    synthetic = {"A": 2, "Z": 1, None: 2}
    actual = {(r.BASELINE, r.NUM_CONTA_PARTICIPANTE): r for r in rows["distribution"]}
    assert len(actual) == sum(len(set(cohort) | set(synthetic)) for cohort in expected.values())
    for baseline, cohort in expected.items():
        for account in set(cohort) | set(synthetic):
            r = actual[baseline, account]
            source_count, synthetic_count = cohort.get(account, 0), synthetic.get(account, 0)
            assert (r.SOURCE_IF_COUNT, r.SYNTHETIC_IF_COUNT) == (source_count, synthetic_count)
            assert (r.SOURCE_TOTAL, r.SYNTHETIC_TOTAL) == (sum(cohort.values()), 5)
            assert r.SOURCE_PCT == pytest.approx(source_count * 100 / sum(cohort.values()))
            assert r.SYNTHETIC_PCT == pytest.approx(synthetic_count * 20)
            assert r.DELTA_PP == pytest.approx(r.SYNTHETIC_PCT - r.SOURCE_PCT)
            assert r.PRESENCE == (
                "both"
                if source_count and synthetic_count
                else "source_only"
                if source_count
                else "synthetic_only"
            )
    summaries = {r.BASELINE: r for r in rows["summary"]}
    assert set(summaries) == set(expected)
    for baseline, cohort in expected.items():
        r = summaries[baseline]
        assert (r.SOURCE_TOTAL, r.SYNTHETIC_TOTAL) == (sum(cohort.values()), 5)
        assert (r.SOURCE_ACCOUNT_BUCKETS, r.SYNTHETIC_ACCOUNT_BUCKETS) == (len(cohort), 3)
        assert r.SOURCE_ONLY_BUCKETS == len(set(cohort) - set(synthetic))
        assert r.SYNTHETIC_ONLY_BUCKETS == 1
        assert r.TOTAL_VARIATION_PCT == pytest.approx(
            {
                "full_export": 50,
                "same_type": 40,
                "active_same_type": 50,
                "selected_sources": 100 / 3,
                "clone_weighted": 40,
            }[baseline]
        )
        assert r.CHANGED_ACCOUNT_IFS == 2
    assert {tuple(r) for r in rows["account_changes"]} == {
        ("103", "1", "3", "A", "Z"),
        ("104", "2", "1", "B", None),
    }


def test_public_schemas_and_no_owned_caches(known_result):
    frames, _ = known_result
    assert set(frames) == {"distribution", "summary", "account_changes"}
    assert frames["distribution"].dtypes == [
        ("BASELINE", "string"),
        ("NUM_CONTA_PARTICIPANTE", "string"),
        ("SOURCE_IF_COUNT", "bigint"),
        ("SYNTHETIC_IF_COUNT", "bigint"),
        ("SOURCE_TOTAL", "bigint"),
        ("SYNTHETIC_TOTAL", "bigint"),
        ("SOURCE_PCT", "double"),
        ("SYNTHETIC_PCT", "double"),
        ("DELTA_PP", "double"),
        ("PRESENCE", "string"),
    ]
    assert frames["distribution"].schema["NUM_CONTA_PARTICIPANTE"].nullable
    assert frames["summary"].dtypes == [
        ("BASELINE", "string"),
        ("SOURCE_TOTAL", "bigint"),
        ("SYNTHETIC_TOTAL", "bigint"),
        ("SOURCE_ACCOUNT_BUCKETS", "bigint"),
        ("SYNTHETIC_ACCOUNT_BUCKETS", "bigint"),
        ("SOURCE_ONLY_BUCKETS", "bigint"),
        ("SYNTHETIC_ONLY_BUCKETS", "bigint"),
        ("TOTAL_VARIATION_PCT", "double"),
        ("CHANGED_ACCOUNT_IFS", "bigint"),
    ]
    assert frames["account_changes"].dtypes == [
        (name, "string")
        for name in [
            "NUM_IF",
            "NUM_IF_ORIG",
            "K",
            "SOURCE_NUM_CONTA_PARTICIPANTE",
            "SYNTHETIC_NUM_CONTA_PARTICIPANTE",
        ]
    ]
    assert not any(frame.is_cached for frame in frames.values())


def test_unchanged_clone_weighted_is_exact_zero(known_inputs):
    source, synthetic, clone_map = known_inputs
    synthetic = synthetic.withColumn(
        "NUM_CONTA_PARTICIPANTE",
        F.when(F.col("NUM_IF") == "103", "A")
        .when(F.col("NUM_IF") == "104", "B")
        .otherwise(F.col("NUM_CONTA_PARTICIPANTE")),
    )
    result = compare_if_accounts(source, synthetic, clone_map, 1)
    summaries = {r.BASELINE: r for r in result["summary"].collect()}
    assert summaries["clone_weighted"].TOTAL_VARIATION_PCT == 0.0
    assert summaries["selected_sources"].TOTAL_VARIATION_PCT > 0
    assert all(r.CHANGED_ACCOUNT_IFS == 0 for r in summaries.values())
    assert result["account_changes"].collect() == []


def test_swapped_accounts_change_instruments_not_marginals(spark):
    source = spark.createDataFrame(
        [("1", "A", "2", "deleted"), ("2", "B", "1", None), ("3", None, "1", None)],
        SOURCE_SCHEMA,
    )
    synthetic = spark.createDataFrame(
        [("11", "B", "1"), ("12", "A", "1"), ("13", " ", "1")],
        SYNTHETIC_SCHEMA,
    )
    clone_map = spark.createDataFrame(
        [("1", "1", "11"), ("2", "1", "12"), ("3", "1", "13")],
        MAP_SCHEMA,
    )
    result = compare_if_accounts(source, synthetic, clone_map, 1)
    summary = {r.BASELINE: r for r in result["summary"].collect()}
    assert summary["clone_weighted"].TOTAL_VARIATION_PCT == 0.0
    assert summary["selected_sources"].SOURCE_TOTAL == 3  # RAW source, not same_type/active.
    assert all(r.CHANGED_ACCOUNT_IFS == 2 for r in summary.values())
    assert {r.NUM_IF for r in result["account_changes"].collect()} == {"11", "12"}


@pytest.mark.parametrize(
    "source_rows,synthetic_rows,with_map",
    [
        ([], [], False),
        ([], [], True),
        ([("1", None, "1", None)], [], False),
        ([], [("11", None, "1")], False),
        ([("1", "A", "2", None)], [("11", None, "1")], False),
    ],
)
def test_empty_populations_and_null_only_domains(spark, source_rows, synthetic_rows, with_map):
    result = compare_if_accounts(
        spark.createDataFrame(source_rows, SOURCE_SCHEMA),
        spark.createDataFrame(synthetic_rows, SYNTHETIC_SCHEMA),
        spark.createDataFrame([], MAP_SCHEMA) if with_map else None,
        1,
    )
    summaries = result["summary"].collect()
    assert {r.BASELINE for r in summaries} == (
        BASELINES | {"selected_sources", "clone_weighted"} if with_map else BASELINES
    )
    for r in summaries:
        expected_source = (
            len(source_rows)
            if r.BASELINE == "full_export"
            else sum(row[2] == "1" for row in source_rows)
        )
        assert (r.SOURCE_TOTAL, r.SYNTHETIC_TOTAL) == (expected_source, len(synthetic_rows))
        assert r.SOURCE_ACCOUNT_BUCKETS == expected_source
        assert r.SYNTHETIC_ACCOUNT_BUCKETS == len(synthetic_rows)
        assert r.CHANGED_ACCOUNT_IFS == (0 if with_map else None)
        if not expected_source or not synthetic_rows:
            assert r.TOTAL_VARIATION_PCT is None
    for r in result["distribution"].collect():
        if not r.SOURCE_TOTAL:
            assert r.SOURCE_PCT is None and r.DELTA_PP is None
            assert r.PRESENCE == "synthetic_only"
        if not r.SYNTHETIC_TOTAL:
            assert r.SYNTHETIC_PCT is None and r.DELTA_PP is None
            assert r.PRESENCE == "source_only"
    if with_map:
        assert result["account_changes"].collect() == []
    else:
        assert result["account_changes"] is None


@pytest.mark.parametrize(
    "target,column",
    [
        ("source", name)
        for name in ["NUM_IF", "NUM_CONTA_PARTICIPANTE", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
    ]
    + [("synthetic", name) for name in ["NUM_IF", "NUM_CONTA_PARTICIPANTE", "NUM_TIPO_IF"]]
    + [("clone_map", name) for name in ["NUM_IF_ORIG", "K", "NUM_IF_NOVO"]],
)
def test_required_columns(known_inputs, target, column):
    frames = dict(zip(["source", "synthetic", "clone_map"], known_inputs))
    frames[target] = frames[target].drop(column)
    with pytest.raises(ValueError, match=f"{target} missing required columns: {column}"):
        compare_if_accounts(**frames, if_type=1)


@pytest.mark.parametrize(
    "target,value",
    [
        ("source", None),
        ("source", " "),
        ("synthetic", None),
        ("synthetic", " "),
        ("source", "duplicate"),
        ("synthetic", "duplicate"),
        ("source", "normalized_duplicate"),
        ("synthetic", "normalized_duplicate"),
    ],
)
def test_invalid_root_ids(spark, target, value):
    rows = [("1", "A", "1", None)]
    if value == "duplicate":
        rows *= 2
    elif value == "normalized_duplicate":
        rows += [(" 1.00 ", "B", "2", "deleted")]
    else:
        rows = [(value, "A", "1", None)]
    bad = spark.createDataFrame(rows, SOURCE_SCHEMA)
    good = spark.createDataFrame([("2", "A", "1", None)], SOURCE_SCHEMA)
    source, synthetic = (bad, good) if target == "source" else (good, bad)
    with pytest.raises(ValueError, match=f"{target} NUM_IF must be non-null, valid and unique"):
        compare_if_accounts(source, synthetic, None, 1)


@pytest.mark.parametrize("value", [None, "", "2", "1.5"])
def test_wrong_synthetic_type(known_inputs, value):
    source, synthetic, _ = known_inputs
    synthetic = synthetic.withColumn(
        "NUM_TIPO_IF",
        F.when(F.col("NUM_IF") == "101", F.lit(value)).otherwise(F.col("NUM_TIPO_IF")),
    )
    with pytest.raises(ValueError, match="synthetic NUM_TIPO_IF must match"):
        compare_if_accounts(source, synthetic, None, 1)


@pytest.mark.parametrize("if_type", [None, "1", 1.0, True])
def test_if_type_requires_integer(known_inputs, if_type):
    with pytest.raises(ValueError, match="if_type must be an integer"):
        compare_if_accounts(*known_inputs, if_type=if_type)


@pytest.mark.parametrize("source_account,synthetic_account", [(None, "NULL"), ("NULL", None)])
def test_null_is_distinct_from_literal_null_with_nonempty_populations(
    spark, source_account, synthetic_account
):
    result = compare_if_accounts(
        spark.createDataFrame([("1", source_account, "1", None)], SOURCE_SCHEMA),
        spark.createDataFrame([("11", synthetic_account, "1")], SYNTHETIC_SCHEMA),
        spark.createDataFrame([("1", "1", "11")], MAP_SCHEMA),
        1,
    )
    rows = result["distribution"].collect()
    assert len(rows) == 10
    for r in rows:
        assert r.PRESENCE == (
            "source_only" if r.NUM_CONTA_PARTICIPANTE == source_account else "synthetic_only"
        )
        assert abs(r.DELTA_PP) == 100.0
    for r in result["summary"].collect():
        assert r.TOTAL_VARIATION_PCT == 100.0
        assert r.SOURCE_ONLY_BUCKETS == r.SYNTHETIC_ONLY_BUCKETS == 1
        assert r.CHANGED_ACCOUNT_IFS == 1
    assert [tuple(r) for r in result["account_changes"].collect()] == [
        ("11", "1", "1", source_account, synthetic_account)
    ]


@pytest.mark.parametrize(
    "map_rows,message",
    [
        ([("missing", "1", "11")], "missing source"),
        ([], "coverage"),
        ([("1", "1", "11"), ("1", "2", "12")], "coverage"),
        ([("1", "1", "12")], "coverage"),
        ([("1", "1", "11"), ("1", "1", "11")], "NUM_IF_NOVO"),
        ([("1", "1", "11"), ("1", "2", "11.0")], "NUM_IF_NOVO"),
        ([("1", "1", "11"), ("1.00", "01.0", "12")], "NUM_IF_ORIG/K"),
        ([(None, "1", "11")], "NUM_IF_ORIG/K"),
        ([(" ", "1", "11")], "NUM_IF_ORIG/K"),
        ([("1", "1", None)], "NUM_IF_NOVO"),
        ([("1", "1", " ")], "NUM_IF_NOVO"),
    ]
    + [
        ([("1", k, "11")], "NUM_IF_ORIG/K")
        for k in [
            None,
            "",
            "0",
            "00.0",
            "-1",
            "1.5",
            "NaN",
            "1e2",
            "abc",
        ]
    ],
)
def test_invalid_maps(spark, map_rows, message):
    source = spark.createDataFrame([("1", "A", "1", None)], SOURCE_SCHEMA)
    synthetic = spark.createDataFrame([("11", "A", "1")], SYNTHETIC_SCHEMA)
    clone_map = spark.createDataFrame(map_rows, MAP_SCHEMA)
    with pytest.raises(ValueError, match=message):
        compare_if_accounts(source, synthetic, clone_map, 1)


def test_oracle_decimal_keys_and_accounts_stay_exact(spark):
    ids = ["9007199254740992", "9007199254740993", "123456789012345678901234567890123456"]
    new_ids = [str(int(value) + 10) for value in ids]
    source = spark.createDataFrame(
        [(Decimal(value + ".00"), Decimal(value + ".00"), "1", None) for value in ids],
        "NUM_IF decimal(38,2), NUM_CONTA_PARTICIPANTE decimal(38,2), "
        "NUM_TIPO_IF string, DAT_EXCLUSAO string",
    )
    synthetic = spark.createDataFrame(
        [(Decimal(new), old + ".000", "1") for old, new in zip(ids, new_ids)],
        "NUM_IF decimal(38,0), NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string",
    )
    clone_map = spark.createDataFrame(
        [(old, "99999999999999999999999999999999999999", new) for old, new in zip(ids, new_ids)],
        MAP_SCHEMA,
    )
    result = compare_if_accounts(source, synthetic, clone_map, 1)
    rows = result["distribution"].collect()
    assert len(rows) == 15
    assert {r.NUM_CONTA_PARTICIPANTE for r in rows} == set(ids)
    assert all(r.SOURCE_IF_COUNT == r.SYNTHETIC_IF_COUNT == 1 for r in rows)
    assert all(r.DELTA_PP == 0.0 for r in rows)
    assert result["account_changes"].collect() == []


def test_standalone_import_and_no_driver_materialization():
    path = Path(__file__).parents[1] / "scripts" / "compare_if_account_distribution.py"
    spec = importlib.util.spec_from_file_location("standalone_comparison", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.compare_if_accounts)
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module in {"functools", "pyspark.sql"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {
                "collect",
                "toPandas",
                "toLocalIterator",
                "broadcast",
                "cache",
                "persist",
            }
            if node.func.attr == "first":
                assert isinstance(node.func.value, ast.Call)
                assert node.func.value.func.attr == "agg"
