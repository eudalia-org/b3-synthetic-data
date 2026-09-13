import hashlib
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from scripts import compare_if_account_distribution as cli

CATALOG = Path(__file__).parents[1] / "datagen/queries_produtos.sql"
PRODUCTS = {
    "cdb_simplificado": (49, "sim"),
    "cdb_resgate": (49, "res"),
    "cdb_escalonamento": (49, "esc"),
    "rdb_inclusao": (50, "incl"),
    "rdb_resgate": (50, "rres"),
}
OP_SCHEMA = (
    "NUM_ID_OPERACAO string, NUM_IF string, NUM_CONTA_PARTICIPANTE_P1 string, "
    "NUM_CONTA_PARTICIPANTE_P2 string"
)
IF_SCHEMA = "NUM_IF string, NUM_TIPO_IF string, DAT_EXCLUSAO string"


def block(sql, product="cdb_simplificado"):
    return f"-- BEGIN QUERY: {product}\n{sql}\n-- END QUERY: {product}\n"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("product-query-account-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("canonical-account-queries")
    source_base = root / "export"
    cases = [(name, name, None) for _, name in PRODUCTS.values()]
    cases += [(f"{name}_bad_route", name, "route") for name in ("res", "esc", "rres")]
    cases += [
        (f"sim_no_{table}", "sim", table)
        for table in (
            "TITULO",
            "CONDICAO_IF",
            "RESGATE",
            "DEPOSITO_AUTOMATICO_IF",
            "DADO_OPERACAO",
            "LANCAMENTO",
            "ESPECIFICACAO",
            "ESPECIFICACAO_COMITENTE",
            "registration",
            "flags",
        )
    ]
    cases += [("sim_deleted", "sim", "deleted")]
    tables = {
        name: []
        for name in (
            "INSTRUMENTO_FINANCEIRO",
            "OPERACAO",
            "TITULO",
            "CONDICAO_IF",
            "RESGATE",
            "DEPOSITO_AUTOMATICO_IF",
            "DADO_OPERACAO",
            "LANCAMENTO",
            "ESPECIFICACAO",
            "ESPECIFICACAO_COMITENTE",
        )
    }
    for key, kind, missing in cases:
        if_type = "50" if kind in {"incl", "rres"} else "49"
        route = "5177" if if_type == "50" else "4509"
        if missing == "registration":
            route = "999"
        secondary_route = "999" if kind in {"sim", "incl"} or missing == "route" else route
        tables["INSTRUMENTO_FINANCEIRO"].append(
            (key, if_type, "deleted" if missing == "deleted" else None)
        )
        tables["OPERACAO"].extend(
            [
                (key + "q", key, " A ", "X", route, "43", "10-1", "40-1"),
                (key + "s", key, None, "Y", secondary_route, "0", "other", "other"),
            ]
        )
        if missing != "TITULO":
            tables["TITULO"].append((key, "step" if kind == "esc" else None, "0"))
        if missing != "CONDICAO_IF":
            tables["CONDICAO_IF"].append(
                (key, key + "c", "20" if missing == "flags" else "1", None)
            )
        if missing != "RESGATE":
            tables["RESGATE"].append(
                (key + "c", "MERCADO" if kind in {"res", "rres"} else "SEM TABELA", None)
            )
        if missing != "DEPOSITO_AUTOMATICO_IF":
            tables["DEPOSITO_AUTOMATICO_IF"].append((key,))
        # The child-complete operation is NOT the operation qualifying FILTRO_BASE.
        for table in ("DADO_OPERACAO", "LANCAMENTO"):
            if missing != table:
                tables[table].append((key + "s",))
        if missing != "ESPECIFICACAO":
            tables["ESPECIFICACAO"].append((key + "s", key + "e"))
        if missing != "ESPECIFICACAO_COMITENTE":
            tables["ESPECIFICACAO_COMITENTE"].extend([(key + "e",), (key + "e",)])
    schemas = {
        "INSTRUMENTO_FINANCEIRO": IF_SCHEMA,
        "OPERACAO": OP_SCHEMA
        + ", NUM_ID_TIPO_OPER_OBJETO_SERV string, COD_SITUACAO_OPERACAO string, "
        "COD_CONTA_PARTE string, COD_CONTA_CONTRAPARTE string",
        "TITULO": "NUM_IF string, COD_TIPO_ESCALONAMENTO string, QTD_RESGATADA string",
        "CONDICAO_IF": "NUM_IF string, NUM_CONDICAO_IF string, "
        "COD_TIPO_CONDICAO_IF string, DAT_EXCLUSAO string",
        "RESGATE": "NUM_CONDICAO_IF string, COD_COND_RESGATE string, DAT_EXCLUSAO string",
        "DEPOSITO_AUTOMATICO_IF": "NUM_IF string",
        "DADO_OPERACAO": "NUM_ID_OPERACAO string",
        "LANCAMENTO": "NUM_ID_OPERACAO string",
        "ESPECIFICACAO": "NUM_ID_OPERACAO string, NUM_ID_ESPECIFICACAO string",
        "ESPECIFICACAO_COMITENTE": "NUM_ID_ESPECIFICACAO string",
    }
    for name, rows in tables.items():
        spark.createDataFrame(rows, schemas[name]).write.parquet(str(source_base / name))
    spark.createDataFrame(
        [("4509", "1", "44", " S "), ("5177", "1", "45", "S"), ("999", "2", "44", "S")],
        "NUM_ID_TIPO_OPER_OBJETO_SERV string, NUM_ID_TIPO_OPERACAO string, "
        "NUM_ID_OBJETO_SERVICO string, IND_DISPONIVEL_IDENTIFICACAO string",
    ).write.parquet(str(source_base / "TIPO_OPER_OBJETO_SERV"))
    spark.createDataFrame(
        [("1", " 1 "), ("2", "2")],
        "NUM_ID_TIPO_OPERACAO string, COD_TIPO_OPERACAO string",
    ).write.parquet(str(source_base / "TIPO_OPERACAO"))
    synthetic = spark.createDataFrame(
        [("new1", "not-a-source-root", "A", "Y"), ("new2", None, "Z", None)],
        OP_SCHEMA,
    )
    for product in PRODUCTS:
        synthetic.write.parquet(str(root / "run/products" / product / "synthetic/OPERACAO"))
    return root


@pytest.mark.parametrize("product", PRODUCTS)
def test_actual_catalog_domains_and_all_operations_not_only_qualifying_rows(spark, raw, product):
    if_type, expected = PRODUCTS[product]
    source_base = str(raw / "export")
    metadata = spark.read.parquet(source_base + "/INSTRUMENTO_FINANCEIRO")
    roots = cli.product_query_if_ids(
        spark,
        source_base,
        CATALOG.read_bytes().decode("utf-8"),
        product,
        metadata,
        if_type,
    )
    assert roots.is_cached
    try:
        assert roots.dtypes == [("NUM_IF", "string")]
        assert [r.NUM_IF for r in roots.collect()] == [expected]
        source = spark.read.parquet(source_base + "/OPERACAO")
        synthetic = spark.read.parquet(str(raw / "run/products" / product / "synthetic/OPERACAO"))
        result = cli.compare_operation_accounts(
            source, synthetic, metadata, if_type, query_matched_ifs=roots
        )
        rows = result["distribution"].where("BASELINE = 'product_query_matched'").collect()
        assert {r.SOURCE_TOTAL for r in rows} == {2}
        assert {r.SYNTHETIC_TOTAL for r in rows} == {2}
        assert {
            (r.ROLE, r.NUM_CONTA_PARTICIPANTE): r.SOURCE_OPERATION_COUNT
            for r in rows
            if r.SOURCE_OPERATION_COUNT
        } == {
            ("P1", "A"): 1,
            ("P1", None): 1,
            ("P2", "X"): 1,
            ("P2", "Y"): 1,
        }
        assert all(r.SOURCE_PCT == 50 for r in rows if r.SOURCE_OPERATION_COUNT)
        assert any(
            r.NUM_CONTA_PARTICIPANTE == "Z" and r.SYNTHETIC_OPERATION_COUNT == 1 for r in rows
        )
        assert roots.is_cached
    finally:
        roots.unpersist(blocking=True)


@pytest.mark.parametrize("uri_kind", ["path", "file"])
def test_reader_preserves_bytes_hash_and_local_uri(spark, tmp_path, capsys, uri_kind):
    data = block("SELECT '1' AS NUM_IF;").replace("\n", "\r\n").encode("utf-8")
    path = tmp_path / "query catalog.sql"
    path.write_bytes(data)
    uri = str(path) if uri_kind == "path" else path.as_uri()
    assert cli.read_product_query_catalog(spark, uri).encode("utf-8") == data
    out = capsys.readouterr().out
    assert uri in out and hashlib.sha256(data).hexdigest() in out


@pytest.mark.parametrize("kind", ["missing", "directory", "oversize", "utf8"])
def test_reader_rejects_invalid_catalogs_without_sql(spark, tmp_path, monkeypatch, kind):
    path = tmp_path / "catalog.sql"
    if kind == "directory":
        path.mkdir()
    elif kind == "oversize":
        path.write_bytes(b"x" * (1024 * 1024 + 1))
    elif kind == "utf8":
        path.write_bytes(b"\xff")
    monkeypatch.setattr(spark, "sql", lambda *args: pytest.fail("must not execute SQL"))
    with pytest.raises(ValueError, match="Cannot read query catalog"):
        cli.read_product_query_catalog(spark, str(path))


@pytest.mark.parametrize(
    "text,message",
    [
        ("SELECT 1", "Missing query block"),
        (block("SELECT 1") * 2, "Duplicate"),
        ("-- BEGIN QUERY: cdb_simplificado\nSELECT 1", "Missing END"),
        ("-- END QUERY: cdb_simplificado\n", "Mismatched"),
        (
            block("SELECT 1").replace("END QUERY: cdb_simplificado", "END QUERY: rdb_resgate"),
            "Mismatched",
        ),
        (block("SELECT 1").replace("BEGIN QUERY:", "BEGIN QUERY"), "Malformed"),
        (block(block("SELECT 1", "other")), "nested"),
    ],
)
def test_catalog_block_contract(text, message):
    with pytest.raises(ValueError, match=message):
        cli._product_query(text, "cdb_simplificado", "/raw")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 AS NUM_IF; SELECT 2 AS NUM_IF",
        "SELECT 1 AS NUM_IF;;",
        "DROP TABLE x",
        "WITH x AS (SELECT 1) INSERT INTO target SELECT * FROM x",
        "WITH x AS (SELECT 1) INSERT OVERWRITE DIRECTORY '/tmp/no' SELECT * FROM x",
        "WITH x AS (SELECT 1) CREATE TABLE target AS SELECT * FROM x",
        "SELECT reflect('java.lang.System', 'setProperty', 'x', 'y') AS NUM_IF",
        "SELECT '${value}' AS NUM_IF; DELETE FROM x",
        "SELECT 'unclosed AS NUM_IF",
        "SELECT `unclosed AS NUM_IF",
        "SELECT NUM_IF FROM {{NOT_RAW_IF}}",
        "SELECT 1 /* outer /* inner */ */ AS NUM_IF",
    ],
)
def test_non_query_or_malformed_sql_rejected_before_execution(spark, monkeypatch, sql):
    metadata = spark.createDataFrame([], IF_SCHEMA)
    monkeypatch.setattr(spark, "sql", lambda *args: pytest.fail("guard must precede spark.sql"))
    with pytest.raises(ValueError):
        cli.product_query_if_ids(spark, "/raw", block(sql), "cdb_simplificado", metadata, 49)


def test_spark_plan_guard_rejects_command_inside_with_before_sql(spark, monkeypatch):
    sql = "WITH x AS (SELECT '1' AS NUM_IF) INSERT INTO target SELECT * FROM x"
    monkeypatch.setattr(cli, "_product_query", lambda *args: (sql, "unused", []))
    monkeypatch.setattr(spark, "sql", lambda *args: pytest.fail("must not execute a command plan"))
    with pytest.raises(ValueError, match="read-only SELECT/WITH plan"):
        cli.product_query_if_ids(
            spark,
            "/unused",
            "unused",
            "cdb_simplificado",
            spark.createDataFrame([], IF_SCHEMA),
            49,
        )


@pytest.mark.parametrize("relation", ["external", "literal_source", "temp_view", "qualified"])
def test_relations_without_raw_placeholder_rejected_even_with_valid_overlapping_ids(
    spark,
    tmp_path,
    monkeypatch,
    relation,
):
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    source_base = tmp_path / "export"
    external_base = tmp_path / "other-export"
    target = external_base if relation == "external" else source_base
    metadata.write.parquet(str(target / "INSTRUMENTO_FINANCEIRO"))
    metadata.createOrReplaceTempView("comparison_untrusted_if")
    reference = {
        "external": f"parquet.`{external_base}/INSTRUMENTO_FINANCEIRO`",
        "literal_source": f"parquet.`{source_base}/INSTRUMENTO_FINANCEIRO`",
        "temp_view": "comparison_untrusted_if",
        "qualified": "default.comparison_untrusted_if",
    }[relation]
    sql = f"SELECT NUM_IF FROM {reference}"
    if relation == "external":
        sql = "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) " + sql
    monkeypatch.setattr(
        spark, "sql", lambda *args: pytest.fail("relation guard must precede spark.sql")
    )
    try:
        with pytest.raises(ValueError, match="generated RAW placeholder path or an in-scope CTE"):
            cli.product_query_if_ids(
                spark,
                str(source_base),
                block(sql),
                "cdb_simplificado",
                metadata,
                49,
            )
    finally:
        spark.catalog.dropTempView("comparison_untrusted_if")


@pytest.mark.parametrize(
    "sql",
    [
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT NUM_IF FROM seed WHERE EXISTS (SELECT 1 FROM comparison_hidden)",
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT (SELECT NUM_IF FROM comparison_hidden) AS NUM_IF FROM seed",
        "WITH wrapper AS (WITH comparison_hidden AS "
        "(SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) SELECT NUM_IF FROM comparison_hidden) "
        "SELECT NUM_IF FROM comparison_hidden",
        "WITH wrapper AS (WITH comparison_hidden AS "
        "(SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT NUM_IF FROM comparison_hidden), "
        "sibling AS (SELECT NUM_IF FROM comparison_hidden) SELECT NUM_IF FROM sibling",
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT NUM_IF FROM seed WHERE EXISTS (WITH comparison_hidden AS "
        "(SELECT NUM_IF FROM seed) SELECT 1 FROM comparison_hidden) "
        "AND EXISTS (SELECT 1 FROM comparison_hidden)",
        "WITH earlier AS (SELECT NUM_IF FROM comparison_hidden), comparison_hidden AS "
        "(SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) SELECT NUM_IF FROM earlier",
        "WITH comparison_hidden AS (SELECT NUM_IF FROM comparison_hidden) "
        "SELECT NUM_IF FROM comparison_hidden",
    ],
)
def test_cte_scope_leaks_and_subquery_physical_fallbacks_rejected_before_sql(
    spark, monkeypatch, sql
):
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    metadata.createOrReplaceTempView("comparison_hidden")
    monkeypatch.setattr(
        spark, "sql", lambda *args: pytest.fail("must not resolve a physical fallback")
    )
    try:
        with pytest.raises(ValueError, match="generated RAW placeholder path or an in-scope CTE"):
            cli.product_query_if_ids(spark, "/unused", block(sql), "cdb_simplificado", metadata, 49)
    finally:
        spark.catalog.dropTempView("comparison_hidden")


@pytest.mark.parametrize(
    "sql",
    [
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}), "
        "result AS (SELECT NUM_IF FROM seed) SELECT NUM_IF FROM result "
        "WHERE EXISTS (SELECT 1 FROM seed WHERE seed.NUM_IF = result.NUM_IF)",
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT (SELECT MAX(NUM_IF) FROM seed) AS NUM_IF",
        "WITH comparison_hidden AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT NUM_IF FROM comparison_hidden",
        "WITH seed AS (SELECT 'not-a-source-id' AS NUM_IF), result AS "
        "(WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}) "
        "SELECT NUM_IF FROM seed) "
        "SELECT NUM_IF FROM result",
        "WITH seed AS (SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}}), result AS "
        "(WITH local_seed AS (SELECT NUM_IF FROM seed) SELECT NUM_IF FROM local_seed) "
        "SELECT NUM_IF FROM result",
    ],
)
def test_scoped_ctes_subqueries_and_shadowing_retain_legitimate_raw_roots(spark, tmp_path, sql):
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    metadata.write.parquet(str(tmp_path / "INSTRUMENTO_FINANCEIRO"))
    spark.createDataFrame([("not-a-source-id",)], "NUM_IF string").createOrReplaceTempView(
        "comparison_hidden"
    )
    policy = spark.conf.get("spark.sql.legacy.ctePrecedencePolicy")
    spark.conf.set("spark.sql.legacy.ctePrecedencePolicy", "CORRECTED")
    try:
        roots = cli.product_query_if_ids(
            spark,
            str(tmp_path),
            block(sql),
            "cdb_simplificado",
            metadata,
            49,
        )
        try:
            assert [r.NUM_IF for r in roots.collect()] == ["1"]
        finally:
            roots.unpersist(blocking=True)
    finally:
        spark.conf.set("spark.sql.legacy.ctePrecedencePolicy", policy)
        spark.catalog.dropTempView("comparison_hidden")


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_cte_resolution_respects_session_case_sensitivity(spark, monkeypatch, case_sensitive):
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    before = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
    query = block("WITH MixedCase AS (SELECT '1' AS NUM_IF) SELECT NUM_IF FROM mixedcase")
    try:
        if case_sensitive:
            monkeypatch.setattr(
                spark, "sql", lambda *args: pytest.fail("out-of-scope case must fail before SQL")
            )
            with pytest.raises(ValueError, match="in-scope CTE"):
                cli.product_query_if_ids(spark, "/unused", query, "cdb_simplificado", metadata, 49)
        else:
            roots = cli.product_query_if_ids(
                spark, "/unused", query, "cdb_simplificado", metadata, 49
            )
            try:
                assert [r.NUM_IF for r in roots.collect()] == ["1"]
            finally:
                roots.unpersist(blocking=True)
    finally:
        spark.conf.set("spark.sql.caseSensitive", before)


def test_backtick_export_path_executes_without_sql_dialect_changes(spark, tmp_path):
    base = tmp_path / "raw`export"
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    metadata.write.parquet(str(base / "INSTRUMENTO_FINANCEIRO"))
    before = spark.conf.get("spark.sql.ansi.enabled")
    roots = cli.product_query_if_ids(
        spark,
        str(base),
        block('SELECT NUM_IF FROM {{RAW_INSTRUMENTO_FINANCEIRO}} WHERE "10-1" LIKE "%10-%"'),
        "cdb_simplificado",
        metadata,
        49,
    )
    try:
        assert [r.NUM_IF for r in roots.collect()] == ["1"]
        assert spark.conf.get("spark.sql.ansi.enabled") == before
    finally:
        roots.unpersist(blocking=True)


def test_render_quotes_comments_hash_and_only_actual_placeholders():
    query = (
        "-- {{RAW_IGNORED}} DELETE;\n"
        "SELECT `NUM_IF` FROM {{raw_instrumento_financeiro}} "
        "WHERE 'it''s; DROP' <> 'x' AND \"%10-%\" LIKE '%10-%' "
        "AND '{{RAW_STRING}}' = '{{RAW_STRING}}' /* INSERT; */;\n"
    )
    sql, digest, tables = cli._product_query(block(query), "cdb_simplificado", "/raw`path")
    assert tables == ["INSTRUMENTO_FINANCEIRO"]
    assert "parquet.`/raw``path/INSTRUMENTO_FINANCEIRO`" in sql
    assert "'it''s; DROP'" in sql and '"%10-%"' in sql
    assert "{{RAW_STRING}}" in sql and "RAW_IGNORED" not in sql
    assert digest == hashlib.sha256((query + "\n").encode()).hexdigest()


@pytest.mark.parametrize(
    "sql,message",
    [
        ("SELECT '1' AS OTHER", "only NUM_IF"),
        ("SELECT '1' AS NUM_IF, 'x' AS extra", "only NUM_IF"),
        ("SELECT CAST(NULL AS STRING) AS NUM_IF", "non-null and nonblank"),
        ("SELECT '  ' AS NUM_IF", "non-null and nonblank"),
        ("SELECT 'missing' AS NUM_IF", "must exist"),
        ("SELECT '2' AS NUM_IF", "NUM_TIPO_IF=49"),
    ],
)
def test_root_schema_and_coverage_errors_release_cache(spark, monkeypatch, sql, message):
    metadata = spark.createDataFrame([("1", "49", None), ("2", "50", None)], IF_SCHEMA)
    frame_type = type(metadata)
    persist = frame_type.persist
    caches = []

    def track(frame, level):
        caches.append(frame)
        return persist(frame, level)

    monkeypatch.setattr(frame_type, "persist", track)
    with pytest.raises(ValueError, match=message):
        cli.product_query_if_ids(spark, "/unused", block(sql), "cdb_simplificado", metadata, 49)
    assert all(not frame.is_cached for frame in caches)


@pytest.mark.parametrize("empty", [False, True])
def test_normalized_duplicate_roots_no_fanout_and_empty_is_not_fallback(spark, empty):
    metadata = spark.createDataFrame([("1", "49", None)], IF_SCHEMA)
    query = "SELECT ' 1.00 ' AS NUM_IF UNION ALL SELECT '1' AS NUM_IF"
    if empty:
        query = "SELECT '1' AS NUM_IF WHERE FALSE"
    roots = cli.product_query_if_ids(
        spark, "/unused", block(query), "cdb_simplificado", metadata, 49
    )
    try:
        assert roots.count() == (0 if empty else 1)
        source = spark.createDataFrame([("a", "1", "A", None), ("b", "1", "B", "X")], OP_SCHEMA)
        synthetic = spark.createDataFrame([("new", "999", "C", "X")], OP_SCHEMA)
        result = cli.compare_operation_accounts(
            source, synthetic, metadata, 49, query_matched_ifs=roots
        )
        rows = result["summary"].collect()
        assert len(rows) == 8
        for row in rows:
            assert row.SYNTHETIC_TOTAL == 1
            assert row.SOURCE_TOTAL == (
                0 if empty and row.BASELINE == "product_query_matched" else 2
            )
            if empty and row.BASELINE == "product_query_matched":
                assert row.TOTAL_VARIATION_PCT is None
    finally:
        roots.unpersist(blocking=True)


@pytest.mark.parametrize("baseline,fail_display", [(None, False), ("all", False), (None, True)])
def test_main_full_catalog_default_and_all_keep_roots_cached(
    spark,
    raw,
    monkeypatch,
    capfd,
    baseline,
    fail_display,
):
    frame_type = type(spark.range(1))
    original_collect, persist, unpersist = (
        frame_type.collect,
        frame_type.persist,
        frame_type.unpersist,
    )
    active, seen, reads, stops = [], [], [], []
    reader = cli.read_product_query_catalog

    def read_catalog(session, uri):
        reads.append(uri)
        return reader(session, uri)

    def track_persist(frame, level):
        active.append(frame)
        seen.append(frame)
        return persist(frame, level)

    def track_unpersist(frame, blocking=False):
        assert blocking
        active[:] = [f for f in active if f is not frame]
        return unpersist(frame, blocking)

    def show(frame, n, truncate):
        assert len(active) == 5
        roots = [f for f in active if f.columns == ["NUM_IF"]]
        assert len(roots) == 1 and roots[0].is_cached
        if fail_display:
            raise ValueError("display failed with roots cached")
        rows = original_collect(frame.limit(n))
        assert all(r.SYNTHETIC_TOTAL == 2 for r in rows)
        assert all(r.SOURCE_TOTAL == 2 for r in rows if r.BASELINE == "product_query_matched")
        if "TOTAL_VARIATION_PCT" in frame.columns:
            assert n == 4 and len(rows) == 4

    def stop(session):
        assert active == []
        stops.append(session)

    monkeypatch.setattr(cli, "read_product_query_catalog", read_catalog)
    monkeypatch.setattr(frame_type, "persist", track_persist)
    monkeypatch.setattr(frame_type, "unpersist", track_unpersist)
    monkeypatch.setattr(frame_type, "show", show)
    monkeypatch.setattr(SparkSession, "stop", stop)
    args = [
        "--source-base-uri",
        str(raw / "export"),
        "--synthetic-run-base-uri",
        str(raw / "run"),
        "--queries-uri",
        str(CATALOG),
        "--product",
        "rdb_inclusao",
        "--top-n",
        "2",
    ]
    if baseline:
        args += ["--baseline", baseline]
    monkeypatch.setattr(sys, "argv", [str(cli.__file__), *args])
    if fail_display:
        with pytest.raises(RuntimeError, match="display failed with roots cached"):
            cli.main()
    else:
        cli.main()
    out = capfd.readouterr().out
    assert reads == [str(CATALOG)] and stops == [spark]
    assert all(not frame.is_cached for frame in seen)
    if fail_display:
        assert len(seen) == 5
        return
    assert out.count("Summary: all four baselines") == 2
    assert out.count("Baseline: product_query_matched") == 2
    assert out.count("Baseline:") == (8 if baseline == "all" else 2)
    assert hashlib.sha256(CATALOG.read_bytes()).hexdigest() in out
    assert "Product query roots: rdb_inclusao count=1" in out
    assert "no Python pruning, Oracle admission, or historical catalog version proof" in out


def test_catalog_validation_precedes_source_reads_and_stops_spark(spark, tmp_path, monkeypatch):
    path = tmp_path / "invalid.sql"
    path.write_text(block("SELECT '1' AS NUM_IF", "cdb_simplificado"))
    stops = []
    monkeypatch.setattr(SparkSession, "stop", lambda session: stops.append(session))
    monkeypatch.setattr(
        type(spark.read), "parquet", lambda *args: pytest.fail("must validate all blocks first")
    )
    with pytest.raises(RuntimeError, match="Missing query block: rdb_inclusao"):
        cli.main(
            [
                "--source-base-uri",
                "/unused",
                "--synthetic-run-base-uri",
                "/unused-run",
                "--queries-uri",
                str(path),
                "--product",
                "cdb_simplificado",
                "--product",
                "rdb_inclusao",
            ]
        )
    assert stops == [spark]


def test_actual_catalog_empty_domain_has_zero_baseline(spark, raw, tmp_path):
    catalog = CATALOG.read_bytes().decode("utf-8")
    _, _, tables = cli._product_query(catalog, "cdb_simplificado", str(raw / "export"))
    for table in tables:
        frame = spark.read.parquet(str(raw / "export" / table))
        if table == "DEPOSITO_AUTOMATICO_IF":
            frame = frame.where(F.lit(False))
        frame.write.parquet(str(tmp_path / table))
    metadata = spark.read.parquet(str(tmp_path / "INSTRUMENTO_FINANCEIRO"))
    roots = cli.product_query_if_ids(
        spark, str(tmp_path), catalog, "cdb_simplificado", metadata, 49
    )
    try:
        assert roots.count() == 0
        result = cli.compare_operation_accounts(
            spark.read.parquet(str(tmp_path / "OPERACAO")),
            spark.read.parquet(str(raw / "run/products/cdb_simplificado/synthetic/OPERACAO")),
            metadata,
            49,
            query_matched_ifs=roots,
        )
        rows = result["summary"].where("BASELINE = 'product_query_matched'").collect()
        assert len(rows) == 2
        assert all(r.SOURCE_TOTAL == 0 and r.SYNTHETIC_TOTAL == 2 for r in rows)
        assert all(r.TOTAL_VARIATION_PCT is None for r in rows)
    finally:
        roots.unpersist(blocking=True)
