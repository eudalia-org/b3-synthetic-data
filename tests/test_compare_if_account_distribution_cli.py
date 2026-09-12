import ast
import importlib.util
import os
import runpy
import sys
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
pytest.importorskip("pyspark")
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scripts import compare_if_account_distribution as cli

SCRIPT = Path(cli.__file__)
PRODUCTS = ["cdb_simplificado", "cdb_resgate", "cdb_escalonamento", "rdb_resgate", "rdb_inclusao"]
BASELINES = ["full_export", "same_type", "active_same_type", "selected_sources", "clone_weighted"]
REQUIRED = [
    "--source-base-uri",
    "oci://bucket@ns/export",
    "--synthetic-run-base-uri",
    "oci://bucket@ns/run",
]
SOURCE_SCHEMA = (
    "NUM_IF string, NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string, DAT_EXCLUSAO string"
)


@pytest.mark.parametrize(
    "argv",
    [
        [],
        REQUIRED[:2],
        REQUIRED[2:],
        ["--source-base-uri", "  ", *REQUIRED[2:]],
        [*REQUIRED[:2], "--synthetic-run-base-uri", "\t"],
        *[REQUIRED + ["--top-n", value] for value in ["0", "-1", "1001", "1.5", "abc"]],
        REQUIRED + ["--product", "lci"],
        REQUIRED + ["--product", "cdb_resgate", "--product", "cdb_resgate"],
        REQUIRED + ["--baseline", "unknown"],
        *[REQUIRED + ["--no-clone-map", "--baseline", baseline] for baseline in BASELINES[3:]],
        *[
            REQUIRED + [flag, "/tmp/report"]
            for flag in ["--output", "--output-uri", "--report-uri"]
        ],
    ],
)
def test_invalid_arguments_fail_before_spark(monkeypatch, capsys, argv):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid arguments must not configure or start Spark")

    monkeypatch.setattr(SparkSession.Builder, "appName", forbidden)
    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", forbidden)
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    assert "error:" in capsys.readouterr().err


def test_import_and_notebook_runpy_do_not_execute_main(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("loading the helper must not start Spark")

    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", forbidden)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--invalid-notebook-argument"])
    spec = importlib.util.spec_from_file_location("standalone_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.compare_if_accounts)
    namespace = runpy.run_path(str(SCRIPT))
    assert callable(namespace["compare_if_accounts"])
    assert capsys.readouterr().out == ""
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--help"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert exc.value.code == 0
    assert "--synthetic-run-base-uri" in capsys.readouterr().out


def test_cli_ast_is_read_only_bounded_and_uses_managed_spark():
    tree = ast.parse(SCRIPT.read_text())
    forbidden = {
        "write",
        "writeTo",
        "writeStream",
        "save",
        "saveAsTable",
        "insertInto",
        "csv",
        "json",
        "checkpoint",
        "localCheckpoint",
        "setCheckpointDir",
        "export",
        "open",
        "write_text",
        "write_bytes",
        "collect",
        "toPandas",
        "toLocalIterator",
        "master",
        "config",
        "set",
        "getConf",
        "hadoopConfiguration",
        "SparkContext",
        "clearCache",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in forbidden
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in forbidden - {"set"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "parquet":
                assert isinstance(node.func.value, ast.Attribute)
                assert node.func.value.attr == "read"
            if node.func.attr == "show":
                assert any(kw.arg == "n" for kw in node.keywords)
    main = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    for node in ast.walk(main):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
        ):
            assert any(
                kw.arg == "flush" and isinstance(kw.value, ast.Constant) and kw.value.value is True
                for kw in node.keywords
            )
    assert not any(
        isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) for node in tree.body
    )
    assert isinstance(tree.body[-1], ast.If)
    assert ast.unparse(tree.body[-1].test) == "__name__ == '__main__'"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("if-account-cli-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def parquet_inputs(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("if-account-cli")
    source = spark.createDataFrame(
        [
            ("1", "A", "49", None),
            ("2", "B", "49", None),
            ("3", None, "49", "deleted"),
            ("4", "other_type", "99", None),
            ("5", "A", "50", None),
            ("6", "B", "50", None),
            ("7", None, "50", "deleted"),
        ],
        SOURCE_SCHEMA,
    )
    source.withColumn("UNUSED", F.lit("discard")).write.parquet(
        str(root / "export/INSTRUMENTO_FINANCEIRO")
    )
    source.unionByName(source.where("NUM_IF = '1'")).write.parquet(
        str(root / "duplicate/INSTRUMENTO_FINANCEIRO")
    )
    source.drop("DAT_EXCLUSAO").write.parquet(str(root / "invalid/INSTRUMENTO_FINANCEIRO"))
    for product in PRODUCTS:
        if_type = "49" if product.startswith("cdb") else "50"
        base = root / "run/products" / product / "synthetic"
        synthetic = spark.createDataFrame(
            [
                ("101", "A", if_type),
                ("102", "Z", if_type),
                ("103", None, if_type),
            ],
            "NUM_IF string, NUM_CONTA_PARTICIPANTE string, NUM_TIPO_IF string",
        )
        synthetic.withColumn("UNUSED", F.lit("discard")).write.parquet(
            str(base / "INSTRUMENTO_FINANCEIRO")
        )
        originals = ["1", "2", "3"] if if_type == "49" else ["5", "6", "7"]
        mapping = spark.createDataFrame(
            [(original, "1", str(101 + i)) for i, original in enumerate(originals)],
            "NUM_IF_ORIG string, K string, NUM_IF_NOVO string",
        )
        mapping.withColumn("UNUSED", F.lit("discard")).write.parquet(
            str(base / "MAPA_CLONE_NUM_IF")
        )
        if product == PRODUCTS[0]:
            for variant in ["missing-map", "bad-map", "wrong-type"]:
                target = root / variant / "products" / product / "synthetic"
                frame = (
                    synthetic.withColumn("NUM_TIPO_IF", F.lit("50"))
                    if variant == "wrong-type"
                    else synthetic
                )
                frame.write.parquet(str(target / "INSTRUMENTO_FINANCEIRO"))
                if variant != "missing-map":
                    frame = mapping.unionByName(mapping) if variant == "bad-map" else mapping
                    frame.write.parquet(str(target / "MAPA_CLONE_NUM_IF"))
    return root


@pytest.fixture
def guarded_main(spark, parquet_inputs, monkeypatch):
    # All fixtures are written before the guard. main must never access a writer.
    unrelated = spark.range(1).cache()
    frame_type = type(unrelated)

    def forbidden(*args, **kwargs):
        pytest.fail("CLI attempted a write/checkpoint")

    monkeypatch.setattr(frame_type, "write", property(forbidden))
    for name in ["writeTo", "checkpoint", "localCheckpoint"]:
        monkeypatch.setattr(frame_type, name, forbidden)
    owned, active, stops, calls, previews = [], [], [], [], []
    persist, unpersist, compare, show = (
        frame_type.persist,
        frame_type.unpersist,
        cli.compare_if_accounts,
        frame_type.show,
    )

    def tracked_persist(frame, storageLevel):
        assert storageLevel == StorageLevel.MEMORY_AND_DISK
        owned.append(frame)
        active.append(frame)
        return persist(frame, storageLevel)

    def tracked_unpersist(frame, blocking=False):
        assert blocking
        active[:] = [cached for cached in active if cached is not frame]
        return unpersist(frame, blocking=blocking)

    def tracked_compare(source, synthetic, clone_map, if_type):
        assert source.columns == ["NUM_IF", "NUM_CONTA_PARTICIPANTE", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
        assert synthetic.columns == source.columns[:3]
        assert len(active) == (2 if clone_map is None else 3)
        if clone_map is not None:
            assert clone_map.columns == ["NUM_IF_ORIG", "K", "NUM_IF_NOVO"]
        assert all(frame.is_cached for frame in active)
        if calls:
            assert calls[0][0] is source  # One source cache survives the whole loop.
        calls.append((source, if_type, clone_map is not None))
        return compare(source, synthetic, clone_map, if_type)

    def tracked_show(frame, n, truncate):
        assert 1 <= n <= 1000 and truncate is False
        assert len(active) == (4 if calls[-1][2] else 3)
        previews.append((frame.columns, n, frame.limit(n).collect()))
        return show(frame, n=n, truncate=truncate)

    def stop_spy(session):
        assert session is spark
        assert active == []
        stops.append(session)

    monkeypatch.setattr(frame_type, "persist", tracked_persist)
    monkeypatch.setattr(frame_type, "unpersist", tracked_unpersist)
    monkeypatch.setattr(frame_type, "show", tracked_show)
    monkeypatch.setattr(cli, "compare_if_accounts", tracked_compare)
    monkeypatch.setattr(SparkSession, "stop", stop_spy)
    yield {
        "owned": owned,
        "calls": calls,
        "previews": previews,
        "stops": stops,
        "frame_type": frame_type,
    }
    assert stops == [spark]
    assert active == []
    assert all(not frame.is_cached for frame in owned)
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
    assert spark.sparkContext.master == "local[2]"
    assert unrelated.is_cached
    unpersist(unrelated, blocking=True)


@pytest.mark.parametrize(
    "extra,products,baselines,top_n,mapped",
    [
        (["--product", PRODUCTS[0]], PRODUCTS[:1], BASELINES[:1], 30, True),
        (
            [
                "--product",
                PRODUCTS[3],
                "--product",
                PRODUCTS[1],
                "--baseline",
                "all",
                "--top-n",
                "2",
            ],
            [PRODUCTS[3], PRODUCTS[1]],
            BASELINES,
            2,
            True,
        ),
        (["--top-n", "1"], PRODUCTS, BASELINES[:1], 1, True),
        (
            ["--product", PRODUCTS[0], "--baseline", "all", "--no-clone-map", "--top-n", "1000"],
            PRODUCTS[:1],
            BASELINES[:3],
            1000,
            False,
        ),
    ],
)
def test_main_local_parquet_stdout_and_cache_lifetime(
    parquet_inputs, guarded_main, capfd, extra, products, baselines, top_n, mapped
):
    root = parquet_inputs
    # Padding/trailing slashes are accepted without changing the resolved paths.
    args = [
        "--source-base-uri",
        f" {root}/export/ ",
        "--synthetic-run-base-uri",
        f" {root}/run/ ",
        *extra,
    ]
    assert cli.main(args) is None
    out = capfd.readouterr().out
    assert f"Source: {root}/export/INSTRUMENTO_FINANCEIRO" in out
    assert f"Run: {root}/run/" in out
    assert out.count("Caveat:") == 1
    assert "all source types/statuses" in out
    assert "Differences are analytics, not job failures" in out
    assert [line.split()[1] for line in out.splitlines() if line.startswith("Product:")] == products
    assert [
        line.split()[1] for line in out.splitlines() if line.startswith("Baseline:")
    ] == baselines * len(products)
    assert out.count("Summary: all available baselines") == len(products)
    assert "CHANGED_ACCOUNT_IFS" in out
    if top_n >= 5:
        assert "NULL" in out
        assert any(
            "DELTA_PP" in columns and any(r.NUM_CONTA_PARTICIPANTE is None for r in rows)
            for columns, _, rows in guarded_main["previews"]
        )
    calls = guarded_main["calls"]
    assert [call[1] for call in calls] == [49 if p.startswith("cdb") else 50 for p in products]
    assert all(call[2] == mapped for call in calls)
    assert len(guarded_main["owned"]) == 1 + len(products) * (3 if mapped else 2)
    for columns, n, rows in guarded_main["previews"]:
        if "CHANGED_ACCOUNT_IFS" in columns:
            assert {r.BASELINE for r in rows} == set(BASELINES if mapped else BASELINES[:3])
            assert {r.CHANGED_ACCOUNT_IFS for r in rows} == ({1} if mapped else {None})
            assert {r.SYNTHETIC_TOTAL for r in rows} == {3}
            assert next(r.SOURCE_TOTAL for r in rows if r.BASELINE == "full_export") == 7
        elif "DELTA_PP" in columns:
            assert n == top_n
            assert len(rows) <= top_n
            assert rows == sorted(
                rows,
                key=lambda r: (
                    r.DELTA_PP is None,
                    -abs(r.DELTA_PP or 0),
                    r.NUM_CONTA_PARTICIPANTE is not None,
                    r.NUM_CONTA_PARTICIPANTE or "",
                ),
            )
            for row in rows:
                for field in ["SOURCE_PCT", "SYNTHETIC_PCT", "DELTA_PP"]:
                    value = row[field]
                    assert value is None or value == round(value, 6)
        else:
            assert n == top_n
            assert [r.NUM_IF for r in rows] == ["102"]
    if mapped:
        assert out.count(f"account_changes: top {top_n} preview") == len(products)
        assert "check unavailable" not in out
    else:
        assert "account_changes: clone map disabled; check unavailable" in out
        assert "selected_sources" not in out and "clone_weighted" not in out


@pytest.mark.parametrize(
    "source,run,message",
    [
        ("export", "missing-map", "MAPA_CLONE_NUM_IF"),
        ("export", "bad-map", "clone_map NUM_IF_NOVO"),
        ("export", "wrong-type", "synthetic NUM_TIPO_IF"),
        ("duplicate", "run", "source NUM_IF"),
        ("invalid", "run", "DAT_EXCLUSAO"),
        ("missing-source", "run", "INSTRUMENTO_FINANCEIRO"),
    ],
)
def test_input_errors_have_context_and_cleanup(parquet_inputs, guarded_main, source, run, message):
    root = parquet_inputs
    with pytest.raises(RuntimeError, match=message) as exc:
        cli.main(
            [
                "--source-base-uri",
                str(root / source),
                "--synthetic-run-base-uri",
                str(root / run),
                "--product",
                PRODUCTS[0],
            ]
        )
    assert PRODUCTS[0] in str(exc.value)
    assert str(root / source / "INSTRUMENTO_FINANCEIRO") in str(exc.value)
    if source == "export":
        assert str(root / run / "products" / PRODUCTS[0] / "synthetic") in str(exc.value)
    assert exc.value.__cause__ is not None


def test_display_failure_releases_distribution_and_inputs(
    parquet_inputs, guarded_main, monkeypatch
):
    def broken_show(*args, **kwargs):
        raise ValueError("display failed")

    monkeypatch.setattr(guarded_main["frame_type"], "show", broken_show)
    with pytest.raises(RuntimeError, match="display failed") as exc:
        cli.main(
            [
                "--source-base-uri",
                str(parquet_inputs / "export"),
                "--synthetic-run-base-uri",
                str(parquet_inputs / "run"),
                "--product",
                PRODUCTS[0],
            ]
        )
    assert f"product={PRODUCTS[0]}" in str(exc.value)
    assert len(guarded_main["owned"]) == 4


def test_no_map_does_not_read_a_missing_map(parquet_inputs, guarded_main, capfd):
    cli.main(
        [
            "--source-base-uri",
            str(parquet_inputs / "export"),
            "--synthetic-run-base-uri",
            str(parquet_inputs / "missing-map"),
            "--product",
            PRODUCTS[0],
            "--baseline",
            "all",
            "--no-clone-map",
        ]
    )
    assert "check unavailable" in capfd.readouterr().out
    assert len(guarded_main["owned"]) == 3


def test_later_product_failure_cleans_up_and_stops_processing(
    parquet_inputs, guarded_main, monkeypatch
):
    compare = cli.compare_if_accounts

    def fail_second(source, synthetic, clone_map, if_type):
        if if_type == 50:
            raise ValueError("second product failed")
        return compare(source, synthetic, clone_map, if_type)

    monkeypatch.setattr(cli, "compare_if_accounts", fail_second)
    with pytest.raises(RuntimeError, match="second product failed") as exc:
        cli.main(
            [
                "--source-base-uri",
                str(parquet_inputs / "export"),
                "--synthetic-run-base-uri",
                str(parquet_inputs / "run"),
                "--product",
                PRODUCTS[0],
                "--product",
                PRODUCTS[3],
                "--product",
                PRODUCTS[1],
            ]
        )
    assert f"product={PRODUCTS[3]}" in str(exc.value)
    assert len(guarded_main["calls"]) == 1
    assert len(guarded_main["owned"]) == 6
