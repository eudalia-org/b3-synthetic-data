import ast
import importlib.util
import os
import re
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
BASELINES = ["full_export", "same_type", "active_same_type"]
REQUIRED = [
    "--source-base-uri",
    "oci://bucket@ns/export",
    "--synthetic-run-base-uri",
    "oci://bucket@ns/run",
]
OP_COLUMNS = ["NUM_ID_OPERACAO", "NUM_IF", "NUM_CONTA_PARTICIPANTE_P1", "NUM_CONTA_PARTICIPANTE_P2"]
IF_COLUMNS = ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
OP_SCHEMA = ", ".join(f"{c} string" for c in OP_COLUMNS)
IF_SCHEMA = ", ".join(f"{c} string" for c in IF_COLUMNS)


@pytest.mark.parametrize(
    "argv",
    [
        [],
        REQUIRED,
        REQUIRED + ["--baseline", "all"],
        REQUIRED + ["--queries-uri", "  "],
        REQUIRED[:2],
        REQUIRED[2:],
        ["--source-base-uri", "  ", *REQUIRED[2:]],
        [*REQUIRED[:2], "--synthetic-run-base-uri", "\t"],
        *[REQUIRED + ["--top-n", v] for v in ["0", "-1", "1001", "1.5", "abc"]],
        REQUIRED + ["--product", "lci"],
        REQUIRED + ["--product", "cdb_resgate", "--product", "cdb_resgate"],
        *[REQUIRED + ["--baseline", b] for b in ["unknown", "selected_sources", "clone_weighted"]],
        REQUIRED + ["--no-clone-map"],
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


def test_import_and_runpy_do_not_execute_main(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("loading the helper must not start Spark")

    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", forbidden)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--invalid-argument"])
    spec = importlib.util.spec_from_file_location("standalone_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.compare_operation_accounts)
    namespace = runpy.run_path(str(SCRIPT))
    assert callable(namespace["compare_operation_accounts"])
    assert "compare_if_accounts" not in namespace
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
        "write_text",
        "write_bytes",
        "collect",
        "toPandas",
        "toLocalIterator",
        "master",
        "config",
        "set",
        "getConf",
        "SparkContext",
        "clearCache",
        "broadcast",
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
                assert any(
                    kw.arg == "truncate"
                    and isinstance(kw.value, ast.Constant)
                    and 1 <= kw.value.value <= 1000
                    for kw in node.keywords
                )
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
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
    assert not any(isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) for n in tree.body)
    assert isinstance(tree.body[-1], ast.If)
    assert ast.unparse(tree.body[-1].test) == "__name__ == '__main__'"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("operation-account-cli-test")
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
    root = tmp_path_factory.mktemp("operation-account-cli")
    metadata = spark.createDataFrame(
        [
            ("10", "49", None),
            ("20", "49", "deleted"),
            ("30", "50", None),
            ("40", "50", "deleted"),
        ],
        IF_SCHEMA,
    ).withColumn("NUM_CONTA_PARTICIPANTE", F.lit("NEVER_READ"))
    source = spark.createDataFrame(
        [
            ("1", "10", "A", "X"),
            ("2", "10", "A", "Y"),
            ("3", "20", "B", "Y"),
            ("4", "30", "A", "X"),
            ("5", "30", "A", "Y"),
            ("6", "40", "B", "Y"),
            ("7", None, None, "X"),
            ("8", "missing", "U", None),
        ],
        OP_SCHEMA,
    ).withColumn("UNUSED", F.lit("discard"))
    source.write.parquet(str(root / "export/OPERACAO"))
    metadata.write.parquet(str(root / "export/INSTRUMENTO_FINANCEIRO"))
    for variant in ("duplicate-ops", "duplicate-if", "invalid-if", "missing-if"):
        frame = (
            source.unionByName(source.where("NUM_ID_OPERACAO = '1'"))
            if variant == "duplicate-ops"
            else source
        )
        frame.write.parquet(str(root / variant / "OPERACAO"))
        if variant != "missing-if":
            frame = metadata.unionByName(metadata) if variant == "duplicate-if" else metadata
            if variant == "invalid-if":
                frame = frame.drop("DAT_EXCLUSAO")
            frame.write.parquet(str(root / variant / "INSTRUMENTO_FINANCEIRO"))
    synthetic = spark.createDataFrame(
        [
            ("101", "new-root", "A", "Y"),
            ("102", None, "Z", "Y"),
            ("103", "", None, "X"),
        ],
        OP_SCHEMA,
    ).withColumn("UNUSED", F.lit("discard"))
    for product in PRODUCTS:
        synthetic.write.parquet(str(root / "run/products" / product / "synthetic/OPERACAO"))
    synthetic.unionByName(synthetic).write.parquet(
        str(root / "duplicate-synthetic/products" / PRODUCTS[0] / "synthetic/OPERACAO")
    )
    return root


@pytest.fixture
def guarded_main(spark, parquet_inputs, monkeypatch):
    # Guard the runtime class, including Spark 4.2's classic DataFrame subclass.
    unrelated = spark.range(1).cache()
    frame_type = type(unrelated)
    persist, unpersist, compare, show, collect = (
        frame_type.persist,
        frame_type.unpersist,
        cli.compare_operation_accounts,
        frame_type.show,
        frame_type.collect,
    )
    owned, active, stops, calls, previews, reads = [], [], [], [], [], []

    def forbidden(*args, **kwargs):
        pytest.fail("CLI attempted a write/checkpoint/driver materialization")

    for name in ("write", "writeStream"):
        monkeypatch.setattr(frame_type, name, property(forbidden))
    for name in (
        "writeTo",
        "checkpoint",
        "localCheckpoint",
        "collect",
        "toPandas",
        "toLocalIterator",
    ):
        monkeypatch.setattr(frame_type, name, forbidden)

    def scalar_first(frame):
        assert frame.columns == ["n"]
        assert "Aggregate" in frame._jdf.queryExecution().analyzed().toString()
        rows = collect(frame)
        assert len(rows) == 1
        return rows[0]

    def tracked_persist(frame, storageLevel):
        assert storageLevel == StorageLevel.MEMORY_AND_DISK
        owned.append(frame)
        active.append(frame)
        return persist(frame, storageLevel)

    def tracked_unpersist(frame, blocking=False):
        assert blocking
        active[:] = [cached for cached in active if cached is not frame]
        return unpersist(frame, blocking=blocking)

    def tracked_compare(source, synthetic, source_if, if_type):
        assert source.columns == synthetic.columns == OP_COLUMNS
        assert source_if.columns == IF_COLUMNS
        assert len(active) == 3
        assert all(frame.is_cached for frame in active)
        if calls:
            assert calls[0][0] is source and calls[0][1] is source_if
        plan = source_if._jdf.queryExecution().executedPlan().toString()
        read_schemas = re.findall(r"ReadSchema: ([^\n]+)", plan)
        assert read_schemas and all("NUM_CONTA_PARTICIPANTE" not in s for s in read_schemas)
        calls.append((source, source_if, if_type))
        return compare(source, synthetic, source_if, if_type)

    def tracked_show(frame, n, truncate):
        assert 1 <= n <= 1000 and 1 <= truncate <= 1000
        assert len(active) == 4
        previews.append((frame.columns, n, collect(frame.limit(n))))
        return show(frame, n=n, truncate=truncate)

    def stop_spy(session):
        assert session is spark and active == []
        stops.append(session)

    reader_type = type(spark.read)
    parquet = reader_type.parquet

    def tracked_read(reader, path, *args, **kwargs):
        assert not path.startswith("oci:")
        assert path.endswith("/OPERACAO") or path.endswith("/INSTRUMENTO_FINANCEIRO")
        assert "/synthetic/INSTRUMENTO_FINANCEIRO" not in path
        reads.append(path)
        return parquet(reader, path, *args, **kwargs)

    monkeypatch.setattr(reader_type, "parquet", tracked_read)
    monkeypatch.setattr(frame_type, "first", scalar_first)
    monkeypatch.setattr(frame_type, "persist", tracked_persist)
    monkeypatch.setattr(frame_type, "unpersist", tracked_unpersist)
    monkeypatch.setattr(frame_type, "show", tracked_show)
    monkeypatch.setattr(cli, "compare_operation_accounts", tracked_compare)
    monkeypatch.setattr(SparkSession, "stop", stop_spy)
    yield {
        "owned": owned,
        "calls": calls,
        "previews": previews,
        "reads": reads,
        "frame_type": frame_type,
    }
    assert stops == [spark]
    assert active == [] and all(not frame.is_cached for frame in owned)
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
    assert spark.sparkContext.master == "local[2]"
    assert unrelated.is_cached
    unpersist(unrelated, blocking=True)


@pytest.mark.parametrize(
    "extra,products,baselines,top_n",
    [
        (["--product", PRODUCTS[0], "--baseline", "full_export"], PRODUCTS[:1], BASELINES[:1], 30),
        (
            [
                "--product",
                PRODUCTS[3],
                "--product",
                PRODUCTS[1],
                "--baseline",
                "full_export",
                "--top-n",
                "2",
            ],
            [PRODUCTS[3], PRODUCTS[1]],
            BASELINES[:1],
            2,
        ),
        (["--top-n", "1", "--baseline", "full_export"], PRODUCTS, BASELINES[:1], 1),
        (
            ["--product", PRODUCTS[0], "--baseline", "active_same_type", "--top-n", "1000"],
            PRODUCTS[:1],
            ["active_same_type"],
            1000,
        ),
        (["--product", PRODUCTS[0], "--baseline", "same_type"], PRODUCTS[:1], ["same_type"], 30),
    ],
)
def test_main_local_parquet_stdout_and_cache_lifetime(
    parquet_inputs, guarded_main, capfd, extra, products, baselines, top_n
):
    root = parquet_inputs
    assert (
        cli.main(
            [
                "--source-base-uri",
                f" {root}/export/ ",
                "--synthetic-run-base-uri",
                f" {root}/run/ ",
                *extra,
            ]
        )
        is None
    )
    out = capfd.readouterr().out
    assert "Operation account distribution" in out
    assert f"Source: {root}/export/OPERACAO" in out
    assert f"IF metadata: {root}/export/INSTRUMENTO_FINANCEIRO" in out
    assert out.count("Caveat:") == 1
    assert "not the exact SQL eligibility pool" in out
    assert "not per-operation mutation proof" in out
    assert "Differences are analytics, not job failures" in out
    assert [line.split()[1] for line in out.splitlines() if line.startswith("Product:")] == products
    assert [
        line.split()[1] for line in out.splitlines() if line.startswith("Baseline:")
    ] == baselines * (2 * len(products))
    for role in ("P1", "P2"):
        assert out.count(f"Role: {role} (OPERACAO.NUM_CONTA_PARTICIPANTE_{role})") == len(products)
    assert out.count("Summary: all three baselines") == 2 * len(products)
    assert all(
        term not in out
        for term in ["account_changes", "CHANGED_ACCOUNT_IFS", "clone_weighted", "selected_sources"]
    )
    assert [call[2] for call in guarded_main["calls"]] == [
        49 if p.startswith("cdb") else 50 for p in products
    ]
    assert len(guarded_main["owned"]) == 2 + 2 * len(products)
    assert guarded_main["reads"] == [
        str(root / "export/OPERACAO"),
        str(root / "export/INSTRUMENTO_FINANCEIRO"),
        *[str(root / "run/products" / p / "synthetic/OPERACAO") for p in products],
    ]
    for columns, n, rows in guarded_main["previews"]:
        assert len({r.ROLE for r in rows}) == 1
        assert {r.SYNTHETIC_TOTAL for r in rows} == {3}
        if "TOTAL_VARIATION_PCT" in columns:
            assert n == 3 and {r.BASELINE for r in rows} == set(BASELINES)
            assert {r.BASELINE: r.SOURCE_TOTAL for r in rows} == dict(zip(BASELINES, [8, 3, 2]))
        else:
            assert n == top_n and len(rows) <= top_n
            # Ranking precedes display rounding; rounded ties need not be raw ties.
            assert rows == sorted(
                rows,
                key=lambda r: (
                    r.DELTA_PP is None,
                    -abs(
                        r.SYNTHETIC_OPERATION_COUNT * 100.0 / r.SYNTHETIC_TOTAL
                        - r.SOURCE_OPERATION_COUNT * 100.0 / r.SOURCE_TOTAL
                    ),
                    r.NUM_CONTA_PARTICIPANTE is not None,
                    r.NUM_CONTA_PARTICIPANTE or "",
                ),
            )
            for row in rows:
                for field in ("SOURCE_PCT", "SYNTHETIC_PCT", "DELTA_PP"):
                    assert row[field] is None or row[field] == round(row[field], 6)


@pytest.mark.parametrize(
    "source,run,message",
    [
        ("duplicate-ops", "run", "source NUM_ID_OPERACAO"),
        ("duplicate-if", "run", "source_if NUM_IF"),
        ("invalid-if", "run", "DAT_EXCLUSAO"),
        ("missing-source", "run", "OPERACAO"),
        ("missing-if", "run", "INSTRUMENTO_FINANCEIRO"),
        ("export", "missing-run", "OPERACAO"),
        ("export", "duplicate-synthetic", "synthetic NUM_ID_OPERACAO"),
    ],
)
def test_input_errors_have_context_and_cleanup(parquet_inputs, guarded_main, source, run, message):
    with pytest.raises(RuntimeError, match=message) as exc:
        cli.main(
            [
                "--source-base-uri",
                str(parquet_inputs / source),
                "--synthetic-run-base-uri",
                str(parquet_inputs / run),
                "--baseline",
                "full_export",
                "--product",
                PRODUCTS[0],
            ]
        )
    assert PRODUCTS[0] in str(exc.value)
    assert str(parquet_inputs / source / "OPERACAO") in str(exc.value)
    assert exc.value.__cause__ is not None


def test_display_failure_releases_distribution_and_inputs(
    parquet_inputs, guarded_main, monkeypatch
):
    def broken_show(*args, **kwargs):
        raise ValueError("display failed")

    monkeypatch.setattr(guarded_main["frame_type"], "show", broken_show)
    with pytest.raises(RuntimeError, match="display failed"):
        cli.main(
            [
                "--source-base-uri",
                str(parquet_inputs / "export"),
                "--synthetic-run-base-uri",
                str(parquet_inputs / "run"),
                "--baseline",
                "full_export",
                "--product",
                PRODUCTS[0],
            ]
        )
    assert len(guarded_main["owned"]) == 4


def test_later_product_failure_cleans_up_and_stops_processing(
    parquet_inputs, guarded_main, monkeypatch
):
    compare = cli.compare_operation_accounts

    def fail_second(source, synthetic, source_if, if_type):
        if if_type == 50:
            raise ValueError("second product failed")
        return compare(source, synthetic, source_if, if_type)

    monkeypatch.setattr(cli, "compare_operation_accounts", fail_second)
    with pytest.raises(RuntimeError, match="second product failed") as exc:
        cli.main(
            [
                "--source-base-uri",
                str(parquet_inputs / "export"),
                "--synthetic-run-base-uri",
                str(parquet_inputs / "run"),
                "--baseline",
                "full_export",
                "--product",
                PRODUCTS[0],
                "--product",
                PRODUCTS[3],
                "--product",
                PRODUCTS[1],
            ]
        )
    assert f"product={PRODUCTS[3]}" in str(exc.value)
    assert len(guarded_main["calls"]) == 1 and len(guarded_main["owned"]) == 5
