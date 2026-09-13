"""Distributed OPERACAO P1/P2 account distributions using PySpark only.

The historical filename is retained because the deployed OCI Data Flow application
references it. The public API compares operation accounts, not IF accounts.

Source baselines describe the provided export, NOT the exact admitted generator pool:
full_export includes all operations, including null/unmatched NUM_IF; same_type
semi-joins root-type-matched IF keys; active_same_type additionally requires null
IF DAT_EXCLUSAO. product_query_matched uses the supplied canonical SQL root domain,
without Python pruning or Oracle admission. All operations under matched roots are
included, not just operations qualifying the SQL. Every synthetic operation
is retained in every baseline. Marginal distributions are not per-operation
mutation proof; no operation correspondence is inferred.

Usage (standalone OCI Data Flow application; SQL catalog is runtime data)::

    compare_if_account_distribution.py --source-base-uri oci://bucket@namespace/export \
        --synthetic-run-base-uri oci://bucket@namespace/runs/run-id \
        --queries-uri oci://bucket@namespace/config/queries_produtos.sql \
        --product cdb_simplificado --baseline all --top-n 30

Reads source OPERACAO and INSTRUMENTO_FINANCEIRO metadata, and only
products/<product>/synthetic/OPERACAO from the run. With --queries-uri, canonical
SQL also reads its required full RAW schemas directly from the source export.
Repeat --product or omit it for all five products. Output is bounded stdout only.
Data Flow supplies Spark configuration and the OCI connector/authentication.
"""

import argparse
import hashlib
import re
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_product_query_catalog(spark, uri):
    """Read one UTF-8 catalog (at most 1 MiB) through managed Hadoop filesystems.

    Supports OCI, local paths and file:// with the session's existing connector
    configuration. Returns text, preserving bytes for SHA256 provenance.
    """
    limit = 1024 * 1024
    try:
        location = spark._jvm.java.net.URI(uri) if uri.startswith("file:") else uri
        path = spark._jvm.org.apache.hadoop.fs.Path(location)
        fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        status = fs.getFileStatus(path)
        if not status.isFile() or status.getLen() > limit:
            raise ValueError("catalog must be one file of at most 1 MiB")
        stream = fs.open(path)
        try:
            # Bound the actual read too, in case the object changed after stat.
            data = bytes(stream.readNBytes(limit + 1))
        finally:
            stream.close()
        if len(data) > limit:
            raise ValueError("catalog exceeds 1 MiB")
        text = data.decode("utf-8")
    except Exception as exc:
        raise ValueError(f"Cannot read query catalog {uri}: {exc}") from exc
    print(f"Query catalog: {uri} SHA256={hashlib.sha256(data).hexdigest()}", flush=True)
    return text


def _product_query(catalog_text, product, source_base):
    """Extract generator-style blocks; render only executable RAW placeholders.

    This is a conservative canonical-catalog contract, not arbitrary SQL support
    or a sandbox for untrusted UDFs. Quotes retain Spark's existing SQL dialect.
    """
    blocks, current, body = {}, None, []
    for line in catalog_text.splitlines(keepends=True):
        marker = re.fullmatch(
            r"[ \t]*--[ \t]*(BEGIN|END) QUERY:[ \t]*([a-z][a-z0-9_]*)[ \t]*\r?\n?", line
        )
        if marker:
            kind, name = marker.groups()
            if kind == "BEGIN":
                if current is not None or name in blocks:
                    raise ValueError(f"Duplicate or nested query block: {name}")
                current, body = name, []
            else:
                if current != name:
                    raise ValueError(f"Mismatched END QUERY: {name} (open: {current})")
                blocks[name] = "".join(body)
                current = None
        elif re.match(r"[ \t]*--[ \t]*(BEGIN|END) QUERY\b", line):
            raise ValueError(f"Malformed query block marker: {line.strip()}")
        elif current is not None:
            body.append(line)
    if current is not None:
        raise ValueError(f"Missing END QUERY: {current}")
    if product not in blocks:
        raise ValueError(f"Missing query block: {product}")
    query = blocks[product]
    # Lex before substitution: quoted text/comments cannot introduce placeholders
    # or statements, and a backtick in an export path is escaped as an identifier.
    lexer = re.compile(
        r"\s+|--[^\r\n]*|/\*.*?\*/|'(?:[^'\\]|\\.|'')*'|"
        r'"(?:[^"\\]|\\.|"")*"|`(?:[^`]|``)*`|'
        r"\{\{RAW_([A-Z][A-Z0-9_]*)\}\}|[A-Z_][A-Z0-9_]*|"
        r"\d+(?:\.\d+)?(?:E[+-]?\d+)?|<=>|<>|!=|<=|>=|==|\|\||.",
        re.IGNORECASE | re.DOTALL,
    )
    tokens, words, tables = [], [], set()
    for match in lexer.finditer(query):
        token = match.group()
        if token.isspace() or token.startswith("--"):
            continue
        if token.startswith("/*"):
            if "/*" in token[2:]:
                raise ValueError("Nested SQL comments are not supported")
            continue
        if token in {"'", '"', "`", "{", "}", "$"}:
            raise ValueError("Malformed SQL quoting, placeholder or substitution")
        if match.group(1):
            table = match.group(1).upper()
            tables.add(table)
            path = (source_base.rstrip("/") + "/" + table).replace("`", "``")
            token = f"parquet.`{path}`"
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
            words.append(token.upper())
        tokens.append(token)
    if tokens and tokens[-1] == ";":
        tokens.pop()
    forbidden = set(
        "INSERT UPDATE DELETE MERGE CREATE REPLACE DROP ALTER TRUNCATE LOAD OVERWRITE "
        "INTO CACHE UNCACHE REFRESH SET RESET USE SHOW DESCRIBE EXPLAIN ANALYZE REPAIR "
        "OPTIMIZE VACUUM GRANT REVOKE CALL ADD LIST DFS MSCK EXECUTE TRANSFORM "
        "REFLECT JAVA_METHOD".split()
    )
    if (
        not tokens
        or tokens[0].upper() not in {"SELECT", "WITH"}
        or ";" in tokens
        or forbidden.intersection(words)
    ):
        raise ValueError(f"Query {product} must be one read-only SELECT/WITH statement")
    return " ".join(tokens), hashlib.sha256(query.encode("utf-8")).hexdigest(), sorted(tables)


def product_query_if_ids(spark, source_base, catalog_text, product, source_if, if_type):
    """Return materialized, normalized distinct SQL roots. Caller MUST unpersist.

    On failure this helper releases its own cache. Keep successful roots cached
    until every downstream comparison action finishes. No business rules are
    duplicated here: source existence/type checks validate only the SQL contract.
    """
    if not isinstance(if_type, int) or isinstance(if_type, bool):
        raise ValueError("if_type must be an integer")
    _require_columns(source_if, ["NUM_IF", "NUM_TIPO_IF"], "source_if")
    sql, digest, tables = _product_query(catalog_text, product, source_base)
    # Parsing has no execution side effects. Fail closed on non-query plans,
    # including WITH ... INSERT; the lexical guard also covers nested statements.
    plan = spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)
    pending = [(plan, frozenset())]
    raw_relations = {("parquet", source_base.rstrip("/") + "/" + table) for table in tables}
    case_sensitive = spark.conf.get("spark.sql.caseSensitive", "false").lower() == "true"
    allowed = {
        "UnresolvedWith",
        "Project",
        "Distinct",
        "Filter",
        "Join",
        "SubqueryAlias",
        "UnresolvedRelation",
        "UnresolvedSubqueryColumnAliases",
        "Union",
        "Except",
        "Intersect",
        "Aggregate",
        "Sort",
        "GlobalLimit",
        "LocalLimit",
        "OneRowRelation",
        "UnresolvedInlineTable",
        "UnresolvedHaving",
        "WithWindowDefinition",
    }
    while pending:
        node, scope = pending.pop()
        kind = node.getClass().getSimpleName()
        if kind not in allowed:
            raise ValueError(
                f"Query {product} is not a supported read-only SELECT/WITH plan: {node.nodeName()}"
            )
        if kind == "UnresolvedWith":
            # A definition sees outer/prior CTEs, not itself or later siblings.
            # Nested definitions must not leak into their enclosing query scope.
            definitions, local_names = node.cteRelations(), set()
            for i in range(definitions.size()):
                definition = definitions.apply(i)
                name = definition._1()
                name = name if case_sensitive else name.lower()
                if name in local_names:
                    raise ValueError(f"Query {product} has duplicate CTE name: {name}")
                pending.append((definition._2(), scope))
                local_names.add(name)
                scope = scope | {name}
            pending.append((node.child(), scope))
            continue
        if kind == "UnresolvedRelation":
            identifier = node.multipartIdentifier()
            parts = tuple(identifier.apply(i) for i in range(identifier.size()))
            name = parts[0] if case_sensitive else parts[0].lower()
            if parts not in raw_relations and not (len(parts) == 1 and name in scope):
                raise ValueError(
                    f"Query {product} relation {parts!r} must be a generated RAW placeholder "
                    "path or an in-scope CTE"
                )
        # QueryPlan.innerChildren includes expression subquery plans, which must
        # inherit this scope just like relational children (never a global set).
        for children in (node.children(), node.innerChildren()):
            pending.extend((children.apply(i), scope) for i in range(children.size()))
    print(f"Product query: {product} SHA256={digest} RAW tables={','.join(tables)}", flush=True)
    print(
        "Scope: product_query_matched is SQL-only; no Python pruning, Oracle admission, "
        "or historical catalog version proof. "
        "All source operations under matched roots are included.",
        flush=True,
    )
    result = spark.sql(sql)
    if result.columns != ["NUM_IF"]:
        raise ValueError(f"Query {product} must return only NUM_IF; got {result.columns}")
    roots = result.select(_identifier("NUM_IF").alias("NUM_IF")).distinct()
    try:
        roots.persist(StorageLevel.MEMORY_AND_DISK)
        if roots.where(F.col("NUM_IF").isNull()).agg(F.count(F.lit(1)).alias("n")).first()["n"]:
            raise ValueError(f"Query {product} NUM_IF must be non-null and nonblank")
        metadata = source_if.select(
            _identifier("NUM_IF").alias("NUM_IF"),
            _identifier("NUM_TIPO_IF").alias("NUM_TIPO_IF"),
        )
        _unique(metadata, "NUM_IF", "source_if")
        invalid = roots.join(
            metadata.where(F.col("NUM_TIPO_IF") == str(if_type)).select("NUM_IF"),
            "NUM_IF",
            "left_anti",
        )
        if invalid.agg(F.count(F.lit(1)).alias("n")).first()["n"]:
            raise ValueError(
                f"Query {product} NUM_IF must exist in source_if with NUM_TIPO_IF={if_type}"
            )
        count = roots.agg(F.count(F.lit(1)).alias("n")).first()["n"]
        print(f"Product query roots: {product} count={count}", flush=True)
        return roots
    except Exception:
        roots.unpersist(blocking=True)
        raise


def _identifier(name):
    value = F.regexp_replace(F.trim(F.col(name).cast("string")), r"\.0+$", "")
    return F.when(value != "", value)


def _require_columns(frame, columns, label):
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _unique(frame, key, label):
    # Only a scalar aggregate crosses to the driver, never offending input rows.
    invalid = frame.groupBy(key).count().where(F.col(key).isNull() | (F.col("count") > 1))
    if invalid.agg(F.count(F.lit(1)).alias("n")).first()["n"]:
        raise ValueError(f"{label} {key} must be non-null, valid and unique")


def _account_counts(frame, count_name):
    return (
        frame.select(
            F.explode(
                F.array(
                    *[
                        F.struct(
                            F.lit(role).alias("ROLE"),
                            F.col(f"NUM_CONTA_PARTICIPANTE_{role}").alias("NUM_CONTA_PARTICIPANTE"),
                        )
                        for role in ("P1", "P2")
                    ]
                )
            ).alias("side")
        )
        .select("side.*")
        .groupBy("ROLE", "NUM_CONTA_PARTICIPANTE")
        .agg(F.count(F.lit(1)).alias(count_name))
    )


def compare_operation_accounts(
    source: DataFrame,
    synthetic: DataFrame,
    source_if: DataFrame,
    if_type: int,
    *,
    query_matched_ifs: DataFrame | None = None,
) -> dict:
    """Return lazy, uncached distribution and summary DataFrames.

    Validation actions reject blank/duplicate operation IDs on either side and
    blank/duplicate source IF IDs (after normalization). Identifiers/accounts are
    trimmed strings without zero decimal suffixes, never converted through floats.
    Blank accounts share the null bucket. IF metadata uses only NUM_IF, NUM_TIPO_IF,
    DAT_EXCLUSAO, never its account. NUM_IF on operations may be null or unmatched.
    Each role's denominator is its cohort's operation count, not twice that count.
    Shares use 0..100 units; zero totals give null shares/deltas/total variation.
    Callers own reads, caching and actions and must keep inputs stable.
    Optional query_matched_ifs is the validated root frame from product_query_if_ids;
    it adds a fourth cohort without filtering synthetic rows or individual operations.
    """
    if not isinstance(if_type, int) or isinstance(if_type, bool):
        raise ValueError("if_type must be an integer")
    columns = [
        "NUM_ID_OPERACAO",
        "NUM_IF",
        "NUM_CONTA_PARTICIPANTE_P1",
        "NUM_CONTA_PARTICIPANTE_P2",
    ]
    metadata = ["NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"]
    _require_columns(source, columns, "source")
    _require_columns(synthetic, columns, "synthetic")
    _require_columns(source_if, metadata, "source_if")
    source = source.select(*[_identifier(c).alias(c) for c in columns])
    synthetic = synthetic.select(*[_identifier(c).alias(c) for c in columns])
    source_if = source_if.select(
        _identifier("NUM_IF").alias("NUM_IF"),
        _identifier("NUM_TIPO_IF").alias("NUM_TIPO_IF"),
        "DAT_EXCLUSAO",
    )
    _unique(source, "NUM_ID_OPERACAO", "source")
    _unique(synthetic, "NUM_ID_OPERACAO", "synthetic")
    _unique(source_if, "NUM_IF", "source_if")
    same_type = source_if.where(F.col("NUM_TIPO_IF") == str(if_type))
    cohorts = {
        "full_export": source,
        "same_type": source.join(same_type.select("NUM_IF"), "NUM_IF", "left_semi"),
        "active_same_type": source.join(
            same_type.where(F.col("DAT_EXCLUSAO").isNull()).select("NUM_IF"),
            "NUM_IF",
            "left_semi",
        ),
    }
    if query_matched_ifs is not None:
        _require_columns(query_matched_ifs, ["NUM_IF"], "query_matched_ifs")
        cohorts["product_query_matched"] = source.join(
            query_matched_ifs.select(_identifier("NUM_IF").alias("NUM_IF")),
            "NUM_IF",
            "left_semi",
        )
    baselines = source.sparkSession.createDataFrame(
        [(name,) for name in cohorts], "BASELINE string"
    )
    roles = source.sparkSession.createDataFrame([("P1",), ("P2",)], "ROLE string")
    keys = ["BASELINE", "ROLE"]
    account = "NUM_CONTA_PARTICIPANTE"
    source_counts = reduce(
        DataFrame.unionByName,
        (
            _account_counts(frame, "SOURCE_OPERATION_COUNT").withColumn("BASELINE", F.lit(name))
            for name, frame in cohorts.items()
        ),
    ).alias("s")
    synthetic_counts = (
        _account_counts(synthetic, "SYNTHETIC_OPERATION_COUNT").crossJoin(baselines).alias("y")
    )
    counts = source_counts.join(
        synthetic_counts,
        (F.col("s.BASELINE") == F.col("y.BASELINE"))
        & (F.col("s.ROLE") == F.col("y.ROLE"))
        & F.col(f"s.{account}").eqNullSafe(F.col(f"y.{account}")),
        "full_outer",
    ).select(
        *[F.coalesce(f"s.{key}", f"y.{key}").alias(key) for key in keys],
        F.coalesce(f"s.{account}", f"y.{account}").alias(account),
        F.coalesce("SOURCE_OPERATION_COUNT", F.lit(0)).alias("SOURCE_OPERATION_COUNT"),
        F.coalesce("SYNTHETIC_OPERATION_COUNT", F.lit(0)).alias("SYNTHETIC_OPERATION_COUNT"),
    )
    has_source = F.col("SOURCE_OPERATION_COUNT") > 0
    has_synthetic = F.col("SYNTHETIC_OPERATION_COUNT") > 0
    totals = (
        baselines.crossJoin(roles)
        .join(
            counts.groupBy(*keys).agg(
                F.sum("SOURCE_OPERATION_COUNT").alias("SOURCE_TOTAL"),
                F.sum("SYNTHETIC_OPERATION_COUNT").alias("SYNTHETIC_TOTAL"),
                F.sum(has_source.cast("long")).alias("SOURCE_ACCOUNT_BUCKETS"),
                F.sum(has_synthetic.cast("long")).alias("SYNTHETIC_ACCOUNT_BUCKETS"),
                F.sum((has_source & ~has_synthetic).cast("long")).alias("SOURCE_ONLY_BUCKETS"),
                F.sum((has_synthetic & ~has_source).cast("long")).alias("SYNTHETIC_ONLY_BUCKETS"),
            ),
            keys,
            "left",
        )
        .fillna(0)
    )
    distribution = (
        counts.join(totals.select(*keys, "SOURCE_TOTAL", "SYNTHETIC_TOTAL"), keys)
        .withColumn(
            "SOURCE_PCT",
            F.when(
                F.col("SOURCE_TOTAL") > 0,
                F.col("SOURCE_OPERATION_COUNT") * 100.0 / F.col("SOURCE_TOTAL"),
            ),
        )
        .withColumn(
            "SYNTHETIC_PCT",
            F.when(
                F.col("SYNTHETIC_TOTAL") > 0,
                F.col("SYNTHETIC_OPERATION_COUNT") * 100.0 / F.col("SYNTHETIC_TOTAL"),
            ),
        )
        .withColumn("DELTA_PP", F.col("SYNTHETIC_PCT") - F.col("SOURCE_PCT"))
        .withColumn(
            "PRESENCE",
            F.when(~has_source, "synthetic_only")
            .when(~has_synthetic, "source_only")
            .otherwise("both"),
        )
        .select(
            *keys,
            account,
            "SOURCE_OPERATION_COUNT",
            "SYNTHETIC_OPERATION_COUNT",
            "SOURCE_TOTAL",
            "SYNTHETIC_TOTAL",
            "SOURCE_PCT",
            "SYNTHETIC_PCT",
            "DELTA_PP",
            "PRESENCE",
        )
    )
    variation = distribution.groupBy(*keys).agg(
        (F.sum(F.abs(F.col("DELTA_PP"))) / 2.0).alias("TOTAL_VARIATION_PCT")
    )
    summary = totals.join(variation, keys, "left").select(
        *keys,
        "SOURCE_TOTAL",
        "SYNTHETIC_TOTAL",
        "SOURCE_ACCOUNT_BUCKETS",
        "SYNTHETIC_ACCOUNT_BUCKETS",
        "SOURCE_ONLY_BUCKETS",
        "SYNTHETIC_ONLY_BUCKETS",
        "TOTAL_VARIATION_PCT",
    )
    return {"distribution": distribution, "summary": summary}


def main(argv=None):
    product_types = {
        "cdb_simplificado": 49,
        "cdb_resgate": 49,
        "cdb_escalonamento": 49,
        "rdb_resgate": 50,
        "rdb_inclusao": 50,
    }
    baselines = ["full_export", "same_type", "active_same_type"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-base-uri", required=True)
    parser.add_argument("--synthetic-run-base-uri", required=True)
    parser.add_argument("--product", action="append", choices=product_types)
    parser.add_argument(
        "--queries-uri", default=None, help="Canonical SQL catalog URI/path (max 1 MiB)"
    )
    parser.add_argument(
        "--baseline",
        choices=baselines + ["product_query_matched", "all"],
        default="product_query_matched",
    )
    parser.add_argument("--top-n", type=int, default=30)
    args = parser.parse_args(argv)
    if args.queries_uri is not None:
        args.queries_uri = args.queries_uri.strip()
        if not args.queries_uri:
            parser.error("--queries-uri must be nonempty")
    if args.baseline in {"product_query_matched", "all"} and not args.queries_uri:
        parser.error(f"--baseline {args.baseline} requires --queries-uri (canonical SQL catalog)")
    if args.queries_uri:
        baselines.append("product_query_matched")
    for name in ("source_base_uri", "synthetic_run_base_uri"):
        value = getattr(args, name).strip()
        if not value:
            parser.error(f"--{name.replace('_', '-')} must be nonempty")
        setattr(args, name, value)
    products = args.product or list(product_types)
    if len(products) != len(set(products)):
        parser.error("duplicate --product values are not allowed")
    if not 1 <= args.top_n <= 1000:
        parser.error("--top-n must be an integer in 1..1000")
    chosen = baselines if args.baseline == "all" else [args.baseline]
    source_path = args.source_base_uri.rstrip("/") + "/OPERACAO"
    metadata_path = args.source_base_uri.rstrip("/") + "/INSTRUMENTO_FINANCEIRO"
    print("Operation account distribution", flush=True)
    print(
        f"Source: {source_path}\nIF metadata: {metadata_path}\nRun: {args.synthetic_run_base_uri}",
        flush=True,
    )
    print(
        "Caveat: full_export includes all source operations, including null/unmatched IF. "
        "same_type/active_same_type are root-type-matched, not the exact SQL eligibility pool; "
        "no individual operation status/TOS filters apply in aggregation. "
        "Marginal distributions are not "
        "per-operation mutation proof. Differences are analytics, not job failures.",
        flush=True,
    )
    spark = SparkSession.builder.appName("compare-operation-account-distribution").getOrCreate()
    source_caches = []
    context = f"products={','.join(products)} source={source_path} metadata={metadata_path}"
    try:
        catalog_text = None
        if args.queries_uri:
            catalog_text = read_product_query_catalog(spark, args.queries_uri)
            for product in products:
                _product_query(catalog_text, product, args.source_base_uri)
        columns = [
            "NUM_ID_OPERACAO",
            "NUM_IF",
            "NUM_CONTA_PARTICIPANTE_P1",
            "NUM_CONTA_PARTICIPANTE_P2",
        ]
        source = spark.read.parquet(source_path).select(*columns)
        source_caches.append(source)
        source.persist(StorageLevel.MEMORY_AND_DISK)
        source_if = spark.read.parquet(metadata_path).select(
            "NUM_IF", "NUM_TIPO_IF", "DAT_EXCLUSAO"
        )
        source_caches.append(source_if)
        source_if.persist(StorageLevel.MEMORY_AND_DISK)
        for product in products:
            base = f"{args.synthetic_run_base_uri.rstrip('/')}/products/{product}/synthetic"
            synthetic_path = base + "/OPERACAO"
            context = (
                f"product={product} source={source_path} metadata={metadata_path} "
                f"synthetic={synthetic_path}"
            )
            print(
                f"Product: {product} (source NUM_TIPO_IF={product_types[product]})\n"
                f"Synthetic: {synthetic_path}",
                flush=True,
            )
            caches = []
            try:
                synthetic = spark.read.parquet(synthetic_path).select(*columns)
                caches.append(synthetic)
                synthetic.persist(StorageLevel.MEMORY_AND_DISK)
                options = {}
                if catalog_text is not None:
                    roots = product_query_if_ids(
                        spark,
                        args.source_base_uri,
                        catalog_text,
                        product,
                        source_if,
                        product_types[product],
                    )
                    caches.append(roots)
                    options["query_matched_ifs"] = roots
                result = compare_operation_accounts(
                    source, synthetic, source_if, product_types[product], **options
                )
                distribution = result["distribution"]
                caches.append(distribution)
                distribution.persist(StorageLevel.MEMORY_AND_DISK)
                for role in ("P1", "P2"):
                    print(f"Role: {role} (OPERACAO.NUM_CONTA_PARTICIPANTE_{role})", flush=True)
                    print(
                        f"Summary: all {'four' if catalog_text is not None else 'three'} baselines",
                        flush=True,
                    )
                    result["summary"].where(F.col("ROLE") == role).withColumn(
                        "TOTAL_VARIATION_PCT", F.round("TOTAL_VARIATION_PCT", 6)
                    ).orderBy("BASELINE").show(n=len(baselines), truncate=100)
                    for baseline in chosen:
                        print(
                            f"Baseline: {baseline} (top {args.top_n} by absolute DELTA_PP)",
                            flush=True,
                        )
                        preview = distribution.where(
                            (F.col("BASELINE") == baseline) & (F.col("ROLE") == role)
                        ).orderBy(
                            F.abs(F.col("DELTA_PP")).desc_nulls_last(), "NUM_CONTA_PARTICIPANTE"
                        )
                        preview.select(
                            *[
                                F.round(c, 6).alias(c)
                                if c in {"SOURCE_PCT", "SYNTHETIC_PCT", "DELTA_PP"}
                                else F.col(c)
                                for c in preview.columns
                            ]
                        ).show(n=args.top_n, truncate=100)
            finally:
                for frame in reversed(caches):
                    frame.unpersist(blocking=True)
    except Exception as exc:
        raise RuntimeError(f"Operation account comparison failed ({context}): {exc}") from exc
    finally:
        try:
            for frame in reversed(source_caches):
                frame.unpersist(blocking=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
