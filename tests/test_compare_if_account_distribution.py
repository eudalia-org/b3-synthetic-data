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

from scripts.compare_if_account_distribution import compare_operation_accounts

OP_COLUMNS = ["NUM_ID_OPERACAO", "NUM_IF", "NUM_CONTA_PARTICIPANTE_P1", "NUM_CONTA_PARTICIPANTE_P2"]
IF_COLUMNS = ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
OP_SCHEMA = ", ".join(f"{c} string" for c in OP_COLUMNS)
IF_SCHEMA = ", ".join(f"{c} string" for c in IF_COLUMNS)
BASELINES = {"full_export", "same_type", "active_same_type"}


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("operation-account-test")
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
            (" 1.00 ", " 10.00 ", " A ", "X"),
            ("2", "10", "A", "Y"),
            ("3", "10", "B", "Y"),
            ("4", "20", None, "Y"),
            ("5", "30", "C", "X"),
            ("6", None, "U", None),
            ("7", "missing", "V", " "),
            ("8", "", " ", "NULL"),
        ],
        OP_SCHEMA,
    ).withColumn("DAT_EXCLUSAO", F.lit("ignored"))
    synthetic = spark.createDataFrame(
        [
            ("101", "new-root", "A", "Y"),
            ("102", "new-root", "A", "Y"),
            ("103", None, "Z", "Y"),
            ("104", "", "", "X"),
            ("105", "missing", None, "Z"),
        ],
        OP_SCHEMA,
    ).withColumn("COD_TIPO_OPERACAO", F.lit("ignored"))
    metadata = spark.createDataFrame(
        [
            ("10", "49.00", None),
            ("20", "49", "deleted"),
            ("30", "50", None),
            ("40", "49", None),
        ],
        IF_SCHEMA,
    )
    return source, synthetic, metadata


@pytest.fixture(scope="module")
def known_result(known_inputs):
    frames = compare_operation_accounts(*known_inputs, if_type=49)
    return frames, {name: frame.collect() for name, frame in frames.items()}


def test_counts_are_operations_per_role_not_ifs_or_twice_operations(known_result):
    _, rows = known_result
    expected = {
        ("full_export", "P1"): {"A": 2, "B": 1, None: 2, "C": 1, "U": 1, "V": 1},
        ("full_export", "P2"): {"X": 2, "Y": 3, None: 2, "NULL": 1},
        ("same_type", "P1"): {"A": 2, "B": 1, None: 1},
        ("same_type", "P2"): {"X": 1, "Y": 3},
        ("active_same_type", "P1"): {"A": 2, "B": 1},
        ("active_same_type", "P2"): {"X": 1, "Y": 2},
    }
    synthetic = {"P1": {"A": 2, "Z": 1, None: 2}, "P2": {"Y": 3, "X": 1, "Z": 1}}
    actual = {(r.BASELINE, r.ROLE, r.NUM_CONTA_PARTICIPANTE): r for r in rows["distribution"]}
    assert len(actual) == sum(len(set(v) | set(synthetic[k[1]])) for k, v in expected.items())
    summaries = {(r.BASELINE, r.ROLE): r for r in rows["summary"]}
    assert len(summaries) == 6
    for key, cohort in expected.items():
        synth = synthetic[key[1]]
        deltas = []
        for account in set(cohort) | set(synth):
            r = actual[*key, account]
            sc, yc = cohort.get(account, 0), synth.get(account, 0)
            assert (r.SOURCE_OPERATION_COUNT, r.SYNTHETIC_OPERATION_COUNT) == (sc, yc)
            assert (r.SOURCE_TOTAL, r.SYNTHETIC_TOTAL) == (sum(cohort.values()), 5)
            assert r.SOURCE_PCT == pytest.approx(sc * 100 / sum(cohort.values()))
            assert r.SYNTHETIC_PCT == pytest.approx(yc * 20)
            assert r.DELTA_PP == pytest.approx(r.SYNTHETIC_PCT - r.SOURCE_PCT)
            assert r.PRESENCE == (
                "both" if sc and yc else "source_only" if sc else "synthetic_only"
            )
            deltas.append(abs(r.DELTA_PP))
        summary = summaries[key]
        assert (summary.SOURCE_TOTAL, summary.SYNTHETIC_TOTAL) == (sum(cohort.values()), 5)
        assert summary.SOURCE_ACCOUNT_BUCKETS == len(cohort)
        assert summary.SYNTHETIC_ACCOUNT_BUCKETS == len(synth)
        assert summary.SOURCE_ONLY_BUCKETS == len(set(cohort) - set(synth))
        assert summary.SYNTHETIC_ONLY_BUCKETS == len(set(synth) - set(cohort))
        assert summary.TOTAL_VARIATION_PCT == pytest.approx(sum(deltas) / 2)
    assert actual["full_export", "P1", "A"].SOURCE_PCT == 25
    assert actual["full_export", "P2", "Y"].SOURCE_PCT == 37.5


def test_public_schemas_and_no_owned_caches(known_result):
    frames, _ = known_result
    assert set(frames) == {"distribution", "summary"}
    assert frames["distribution"].dtypes == [
        ("BASELINE", "string"),
        ("ROLE", "string"),
        ("NUM_CONTA_PARTICIPANTE", "string"),
        ("SOURCE_OPERATION_COUNT", "bigint"),
        ("SYNTHETIC_OPERATION_COUNT", "bigint"),
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
        ("ROLE", "string"),
        ("SOURCE_TOTAL", "bigint"),
        ("SYNTHETIC_TOTAL", "bigint"),
        ("SOURCE_ACCOUNT_BUCKETS", "bigint"),
        ("SYNTHETIC_ACCOUNT_BUCKETS", "bigint"),
        ("SOURCE_ONLY_BUCKETS", "bigint"),
        ("SYNTHETIC_ONLY_BUCKETS", "bigint"),
        ("TOTAL_VARIATION_PCT", "double"),
    ]
    assert not any(frame.is_cached for frame in frames.values())


def test_swapping_roles_can_preserve_marginals_without_mutation_claim(spark):
    source = spark.createDataFrame([("1", "10", "A", "B"), ("2", "10", "B", "A")], OP_SCHEMA)
    synthetic = source.select(
        "NUM_ID_OPERACAO",
        "NUM_IF",
        F.col(OP_COLUMNS[3]).alias(OP_COLUMNS[2]),
        F.col(OP_COLUMNS[2]).alias(OP_COLUMNS[3]),
    )
    metadata = spark.createDataFrame([("10", "49", None)], IF_SCHEMA)
    result = compare_operation_accounts(source, synthetic, metadata, 49)
    assert set(result) == {"distribution", "summary"}
    assert all(r.TOTAL_VARIATION_PCT == 0 for r in result["summary"].collect())


@pytest.mark.parametrize(
    "source_rows,synthetic_rows",
    [
        ([], []),
        ([("1", "10", None, "")], []),
        ([], [("11", None, None, "")]),
        ([("1", "other", None, "")], [("11", None, None, "")]),
    ],
)
def test_empty_populations_always_have_six_summaries(spark, source_rows, synthetic_rows):
    result = compare_operation_accounts(
        spark.createDataFrame(source_rows, OP_SCHEMA),
        spark.createDataFrame(synthetic_rows, OP_SCHEMA),
        spark.createDataFrame([("10", "49", None)] if source_rows else [], IF_SCHEMA),
        49,
    )
    summaries = result["summary"].collect()
    assert {(r.BASELINE, r.ROLE) for r in summaries} == {
        (b, p) for b in BASELINES for p in ("P1", "P2")
    }
    for r in summaries:
        n = (
            len(source_rows)
            if r.BASELINE == "full_export"
            else sum(x[1] == "10" for x in source_rows)
        )
        assert (r.SOURCE_TOTAL, r.SYNTHETIC_TOTAL) == (n, len(synthetic_rows))
        assert (r.SOURCE_ACCOUNT_BUCKETS, r.SYNTHETIC_ACCOUNT_BUCKETS) == (n, len(synthetic_rows))
        assert r.TOTAL_VARIATION_PCT == (0 if n and synthetic_rows else None)
    for r in result["distribution"].collect():
        if not r.SOURCE_TOTAL:
            assert r.SOURCE_PCT is None and r.DELTA_PP is None
        if not r.SYNTHETIC_TOTAL:
            assert r.SYNTHETIC_PCT is None and r.DELTA_PP is None


@pytest.mark.parametrize(
    "target,column",
    [
        *[(t, c) for t in ("source", "synthetic") for c in OP_COLUMNS],
        *[("source_if", c) for c in IF_COLUMNS],
    ],
)
def test_required_columns(known_inputs, target, column):
    frames = dict(zip(["source", "synthetic", "source_if"], known_inputs))
    frames[target] = frames[target].drop(column)
    with pytest.raises(ValueError, match=f"{target} missing required columns: {column}"):
        compare_operation_accounts(**frames, if_type=49)


@pytest.mark.parametrize("target", ["source", "synthetic", "source_if"])
@pytest.mark.parametrize("value", [None, " ", "duplicate", "normalized_duplicate"])
def test_invalid_primary_keys(known_inputs, target, value):
    frames = dict(zip(["source", "synthetic", "source_if"], known_inputs))
    key = "NUM_IF" if target == "source_if" else "NUM_ID_OPERACAO"
    frame = frames[target]
    if value in ("duplicate", "normalized_duplicate"):
        extra = frame.limit(1)
        if value == "normalized_duplicate":
            normalized = F.regexp_replace(F.trim(F.col(key)), r"\.0+$", "")
            extra = extra.withColumn(key, F.concat(normalized, F.lit(".00")))
        frames[target] = frame.unionByName(extra)
    else:
        frames[target] = frame.withColumn(key, F.lit(value))
    with pytest.raises(ValueError, match=f"{target} {key} must be non-null, valid and unique"):
        compare_operation_accounts(**frames, if_type=49)


@pytest.mark.parametrize("if_type", [None, "49", 49.0, True])
def test_if_type_requires_integer(known_inputs, if_type):
    with pytest.raises(ValueError, match="if_type must be an integer"):
        compare_operation_accounts(*known_inputs, if_type=if_type)


def test_decimal_identifiers_and_accounts_above_double_precision(spark):
    ids = ["9007199254740992", "9007199254740993", "123456789012345678901234567890123456"]
    source = spark.createDataFrame(
        [(Decimal(v), Decimal(v), Decimal(v), Decimal(v)) for v in ids],
        ", ".join(f"{c} decimal(38,2)" for c in OP_COLUMNS),
    )
    synthetic = spark.createDataFrame([(v, v + ".00", v + ".000", v) for v in ids], OP_SCHEMA)
    metadata = spark.createDataFrame([(v, "49", None) for v in ids], IF_SCHEMA)
    result = compare_operation_accounts(source, synthetic, metadata, 49)
    rows = result["distribution"].collect()
    assert len(rows) == 18
    assert {r.NUM_CONTA_PARTICIPANTE for r in rows} == set(ids)
    assert all(r.SOURCE_OPERATION_COUNT == r.SYNTHETIC_OPERATION_COUNT == 1 for r in rows)
    assert all(r.DELTA_PP == 0 for r in rows)


def test_standalone_import_and_no_driver_materialization():
    path = Path(__file__).parents[1] / "scripts/compare_if_account_distribution.py"
    spec = importlib.util.spec_from_file_location("standalone_comparison", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.compare_operation_accounts)
    assert not hasattr(module, "compare_if_accounts")
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module in {"functools", "pyspark", "pyspark.sql"}
        if isinstance(node, ast.Import):
            assert all(alias.name == "argparse" for alias in node.names)
    for helper in (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name != "main"):
        for node in ast.walk(helper):
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


def test_notebook_code_is_valid_and_contains_no_saved_results():
    path = Path(__file__).resolve().parents[1] / "scripts/compare_if_account_distribution.ipynb"
    notebook = json.loads(path.read_text())
    assert notebook["nbformat"] == 4
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert cell["execution_count"] is None
            assert cell["outputs"] == []
            ast.parse("".join(cell["source"]))


@pytest.mark.parametrize("metadata_valid", [True, False])
def test_notebook_runs_against_local_operations_without_reports(
    spark, known_inputs, tmp_path, monkeypatch, metadata_valid
):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    notebook = json.loads((scripts / "compare_if_account_distribution.ipynb").read_text())
    source, synthetic, metadata = known_inputs
    if not metadata_valid:
        metadata = metadata.drop("DAT_EXCLUSAO")
    source.write.parquet(str(tmp_path / "export/OPERACAO"))
    metadata.write.parquet(str(tmp_path / "export/INSTRUMENTO_FINANCEIRO"))
    synthetic.write.parquet(str(tmp_path / "synthetic/OPERACAO"))
    namespace = {
        "spark": spark,
        "RUN_ID": "local-fixture",
        "EXPORT_BASE": str(tmp_path / "export"),
        "PRODUCT_TYPES": {"cdb_simplificado": 49},
        "PRODUCTS": ["cdb_simplificado"],
        "SYNTHETIC_BASES": {"cdb_simplificado": str(tmp_path / "synthetic")},
        "HELPER_FILE": str(scripts / "compare_if_account_distribution.py"),
        "BASELINE_TO_VIEW": "active_same_type",
        "TOP_N": 5,
    }
    frame_type = type(source)
    collect, persist = frame_type.collect, frame_type.persist
    previews, caches = [], []

    def forbidden(*args, **kwargs):
        pytest.fail("notebook must not write or materialize driver rows")

    def show(frame, n=20, truncate=True, **kwargs):
        assert 1 <= n <= 1000
        previews.extend(collect(frame.limit(n)))

    def tracked_persist(frame, *args, **kwargs):
        caches.append(frame)
        return persist(frame, *args, **kwargs)

    monkeypatch.setattr(frame_type, "write", property(forbidden))
    for name in ("collect", "toPandas", "toLocalIterator"):
        monkeypatch.setattr(frame_type, name, forbidden)
    monkeypatch.setattr(frame_type, "show", show)
    monkeypatch.setattr(frame_type, "persist", tracked_persist)
    # first() internally uses collect(); allow only the helper's scalar aggregate.
    monkeypatch.setattr(
        frame_type,
        "first",
        lambda frame: collect(frame)[0] if frame.columns == ["n"] else forbidden(),
    )

    def run_notebook():
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code" and "parameters" not in cell["metadata"].get("tags", []):
                exec(compile("".join(cell["source"]), "account-notebook", "exec"), namespace)

    if not metadata_valid:
        with pytest.raises(Exception, match="DAT_EXCLUSAO"):
            run_notebook()
        assert caches and all(not f.is_cached for f in caches)
        assert namespace["operation_account_caches"] == []
        return

    run_notebook()
    summaries = [r for r in previews if "TOTAL_VARIATION_PCT" in r.asDict()]
    assert {(r.BASELINE, r.ROLE) for r in summaries} == {
        (b, p) for b in BASELINES for p in ("P1", "P2")
    }
    assert {r.SYNTHETIC_TOTAL for r in summaries} == {5}
    assert {r.SOURCE_TOTAL for r in summaries if r.BASELINE == "full_export"} == {8}
    assert caches and all(not f.is_cached for f in caches)
