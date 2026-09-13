"""Agreed generation normalization, not a reproduction of the two production IFs."""

import ast
import inspect
import random
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F
from test_validate_cdb_variants import (
    by_id,
    valid_escalonamento_tables,
    validator,
)
from test_validate_cdb_variants import spark as spark

from datagen import engorda_tables as engorda

START = "DAT_INICIO_CONDICAO_IF"
END = "DAT_FIM_CONDICAO_IF"
ROOT = "INSTRUMENTO_FINANCEIRO"


def cloned_tables(spark):
    source = valid_escalonamento_tables(spark)
    source["CONDICAO_IF"] = (
        source["CONDICAO_IF"]
        .withColumn(
            START, F.when(F.col("NUM_CONDICAO_IF") == 11, "2026-08-01").otherwise(F.col(START))
        )
        .withColumn(END, F.lit("2028-06-01"))
    )
    map_if = spark.createDataFrame(
        [(1, 1, 101), (1, 2, 202)], "old_NUM_IF long, __k int, new_NUM_IF long"
    )
    map_cond = spark.createDataFrame(
        [(old, k, old * 100 + k) for old in (11, 12, 13) for k in (1, 2)],
        "old_NUM_CONDICAO_IF long, __k int, new_NUM_CONDICAO_IF long",
    )
    tables = {}
    for name, frame in source.items():
        if name == "CONDICAO_RESGATE":
            tables[name] = frame
            continue
        key = "NUM_IF" if name in (ROOT, "TITULO") else "NUM_CONDICAO_IF"
        mapping = map_if if key == "NUM_IF" else map_cond
        joined = frame.join(mapping, frame[key] == mapping[f"old_{key}"])
        if name == "CONDICAO_IF":
            joined = (
                joined.join(map_if, ["__k"])
                .drop("NUM_IF")
                .withColumnRenamed("new_NUM_IF", "NUM_IF")
            )
        tables[name] = joined.select(
            *[F.col(f"new_{key}").alias(col) if col == key else F.col(col) for col in frame.columns]
        )
    tables[ROOT], _ = engorda.aplica_regras_engorda(
        tables[ROOT],
        ROOT,
        engorda_ts=datetime(2026, 9, 13, 15),
        controle_operacional_date=date(2026, 9, 1),
    )
    tables["CONDICAO_IF"], _ = engorda.ajusta_datas_condicao_if(
        tables["CONDICAO_IF"], source[ROOT], tables[ROOT], map_if
    )
    return tables, source, map_if, map_cond


def findings(tables):
    return by_id(validator.check_cdb_variant_rules(tables, 2, validator.VALIDATION_PROFILES["cdb"]))


def test_late_unique_first_segment_passes_existing_validator(spark):
    tables, source, map_if, map_cond = cloned_tables(spark)
    before = tables["CONDICAO_IF"].collect()
    assert findings(tables)["2b.escalonamento_dates"].severity == validator.SEV_ERROR

    tables["CONDICAO_IF"] = engorda.ajusta_inicio_escalonamento_emissao(
        tables["CONDICAO_IF"],
        tables[ROOT],
        source["TITULO"],
        source["JUROS_FLUTUANTE"],
        map_if,
        map_cond,
    ).localCheckpoint(eager=True)
    assert findings(tables)["2b.escalonamento_dates"].passed
    after = tables["CONDICAO_IF"].collect()
    assert len(after) == len(before) == 6
    expected = {row.NUM_CONDICAO_IF: row.asDict() for row in before}
    for key in (1101, 1102):
        expected[key][START] = "2026-09-01"
    assert {row.NUM_CONDICAO_IF: row.asDict() for row in after} == expected


def small_inputs(spark, date_type="string"):
    """Two clones with keys above double precision and mixed physical map types."""
    base = 9007199254740993
    roots = spark.createDataFrame(
        [(Decimal(base + k), 49, None, datetime(2026, 9, k)) for k in (1, 2)],
        "NUM_IF decimal(22,2), NUM_TIPO_IF int, DAT_EXCLUSAO string, DAT_EMISSAO timestamp",
    )
    rows = [
        (
            Decimal(base + 10 * k + i),
            Decimal(base + k),
            3,
            "  ",
            start,
            "2027-01-01",
            Decimal("123.45"),
            "sentinel",
        )
        for k in (1, 2)
        for i, start in enumerate(("2026-09-10 17:00:00", "2026-10-01 13:00:00"))
    ]
    random.Random(17).shuffle(rows)
    conditions = spark.createDataFrame(
        rows,
        "NUM_CONDICAO_IF decimal(22,2), NUM_IF decimal(22,2), "
        "COD_TIPO_CONDICAO_IF int, DAT_EXCLUSAO string, DAT_INICIO_CONDICAO_IF string, "
        "DAT_FIM_CONDICAO_IF string, VAL_FINANCEIRO decimal(10,2), __row string",
    ).withColumn(START, F.col(START).cast(date_type))
    titles = spark.createDataFrame(
        [(Decimal(7), " EMISSAO ", " ")] * 2,
        "NUM_IF decimal(22,2), COD_TIPO_ESCALONAMENTO string, DAT_EXCLUSAO string",
    )
    floats = spark.createDataFrame(
        [(Decimal(i), "2025-01-01") for i in (71, 72)] * 2,
        "NUM_CONDICAO_IF decimal(22,2), DAT_EXCLUSAO string",
    )
    map_if = spark.createDataFrame(
        [(" 7.00 ", str(base + k) + ".00", k) for k in (1, 2)] * 2,
        "old_NUM_IF string, new_NUM_IF string, __k int",
    )
    map_cond = spark.createDataFrame(
        [(f" {71 + i}.00 ", str(base + 10 * k + i) + ".00", k) for k in (1, 2) for i in (0, 1)] * 2,
        "old_NUM_CONDICAO_IF string, new_NUM_CONDICAO_IF string, __k int",
    )
    return [conditions, roots, titles, floats, map_if, map_cond]


@pytest.mark.parametrize("date_type", ["string", "date", "timestamp"])
def test_types_decimal_keys_duplicates_and_shuffled_order(spark, date_type):
    inputs = small_inputs(spark, date_type)
    before = inputs[0].collect()
    expected = {row.NUM_CONDICAO_IF: row.asDict() for row in before}
    base = 9007199254740993
    for k in (1, 2):
        value = f"2026-09-0{k}"
        if date_type == "date":
            value = date.fromisoformat(value)
        elif date_type == "timestamp":
            value = datetime.fromisoformat(value)
        expected[Decimal(base + 10 * k)][START] = value

    for reverse in (False, True):
        inputs[0] = inputs[0].orderBy(
            F.col("NUM_CONDICAO_IF").desc() if reverse else F.col("NUM_CONDICAO_IF")
        )
        corrected = engorda.ajusta_inicio_escalonamento_emissao(*inputs)
        assert corrected.schema == inputs[0].schema
        after = corrected.collect()
        assert len(after) == len(before) == 4
        assert {row.NUM_CONDICAO_IF: row.asDict() for row in after} == expected
        assert "SinglePartition" not in corrected._jdf.queryExecution().executedPlan().toString()


@pytest.mark.parametrize(
    "title_code,condition_code,eligible",
    [
        ("EMISSAO", "3", True),
        (" EMISSAO ", "3.0", True),
        ("emissao", "3", False),
        ("EMISSAO.0", "3", True),
        ("EMISSAO", "3.00", False),
    ],
)
def test_segment_scope_matches_validator_code_normalization(
    spark, title_code, condition_code, eligible
):
    inputs = small_inputs(spark)
    inputs[0] = inputs[0].withColumn("COD_TIPO_CONDICAO_IF", F.lit(condition_code))
    inputs[2] = inputs[2].withColumn("COD_TIPO_ESCALONAMENTO", F.lit(title_code))
    expected = {r.NUM_CONDICAO_IF: r.asDict() for r in inputs[0].collect()}
    if eligible:
        for k in (1, 2):
            expected[Decimal(9007199254740993 + 10 * k)][START] = f"2026-09-0{k}"
    corrected = engorda.ajusta_inicio_escalonamento_emissao(*inputs)
    assert {r.NUM_CONDICAO_IF: r.asDict() for r in corrected.collect()} == expected


@pytest.mark.parametrize(
    "case",
    [
        "inactive_root",
        "inactive_title",
        "inactive_condition",
        "other_condition_type",
        "non_emissao",
        "rdb",
        "missing_float",
        "null_emission",
        "invalid_emission",
        "null_starts",
        "invalid_starts",
        "tied_calendar_minimum",
    ],
)
def test_out_of_scope_or_ambiguous_inputs_are_unchanged(spark, case):
    inputs = small_inputs(spark)
    if case.startswith("inactive_"):
        index = {"inactive_root": 1, "inactive_title": 2, "inactive_condition": 0}[case]
        inputs[index] = inputs[index].withColumn("DAT_EXCLUSAO", F.lit("2025-01-01"))
    elif case == "other_condition_type":
        inputs[0] = inputs[0].withColumn("COD_TIPO_CONDICAO_IF", F.lit(20))
    elif case == "non_emissao":
        inputs[2] = inputs[2].withColumn("COD_TIPO_ESCALONAMENTO", F.lit("VENCIMENTO"))
    elif case == "rdb":
        inputs[1] = inputs[1].withColumn("NUM_TIPO_IF", F.lit(50))
    elif case == "missing_float":
        inputs[3] = inputs[3].limit(0)
    elif case in ("null_emission", "invalid_emission"):
        inputs[1] = inputs[1].withColumn(
            "DAT_EMISSAO", F.lit(None if case == "null_emission" else "not-a-date").cast("string")
        )
    elif case in ("null_starts", "invalid_starts"):
        inputs[0] = inputs[0].withColumn(
            START, F.lit(None if case == "null_starts" else "not-a-date").cast("string")
        )
    else:
        inputs[0] = inputs[0].withColumn(START, F.regexp_replace(START, "2026-10-01", "2026-09-10"))
    before = inputs[0].collect()
    after = engorda.ajusta_inicio_escalonamento_emissao(*inputs).collect()
    assert sorted(after) == sorted(before)


def test_skips_ineligible_earlier_rows_but_repairs_unique_parseable_start(spark):
    inputs = small_inputs(spark)
    original = inputs[0]
    extras = []
    for key, start, kind, deleted in (
        (1, None, 3, None),
        (2, "not-a-date", 3, None),
        (3, "2026-08-01", 3, "2025-01-01"),
        (4, "2026-08-02", 20, None),
        (5, "2026-08-03", 3, None),
    ):
        extra = original.limit(1).withColumn("NUM_CONDICAO_IF", F.lit(key).cast("decimal(22,2)"))
        extra = extra.withColumn(START, F.lit(start).cast("string"))
        extra = extra.withColumn("COD_TIPO_CONDICAO_IF", F.lit(kind))
        extras.append(extra.withColumn("DAT_EXCLUSAO", F.lit(deleted).cast("string")))
    for extra in extras:
        inputs[0] = inputs[0].unionByName(extra)
    # Null/invalid and excluded/other-type rows have subtype membership; row 5 does not.
    inputs[3] = inputs[3].unionByName(
        spark.createDataFrame([(Decimal(i), None) for i in (1, 2, 3, 4)], inputs[3].schema)
    )
    inputs[5] = inputs[5].unionByName(
        spark.createDataFrame([(str(i), str(i), 1) for i in (1, 2, 3, 4)], inputs[5].schema)
    )
    after = engorda.ajusta_inicio_escalonamento_emissao(*inputs).collect()
    expected = engorda.ajusta_inicio_escalonamento_emissao(original, *inputs[1:]).collect()
    for extra in extras:
        expected.extend(extra.collect())
    assert sorted(after) == sorted(expected)


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize("missing", ["frame", "column"])
def test_missing_required_structure_fails_clearly(spark, index, missing):
    inputs = small_inputs(spark)
    inputs[index] = None if missing == "frame" else inputs[index].drop(inputs[index].columns[0])
    with pytest.raises(ValueError, match="CDB EMISSAO: .*colunas obrigatorias"):
        engorda.ajusta_inicio_escalonamento_emissao(*inputs)


@pytest.mark.parametrize("index", range(6))
def test_typed_empty_frames_are_valid_noops(spark, index):
    inputs = small_inputs(spark)
    inputs[index] = inputs[index].limit(0)
    before = inputs[0].collect()
    after = engorda.ajusta_inicio_escalonamento_emissao(*inputs).collect()
    assert sorted(after) == sorted(before)


def test_optional_exclusion_columns_and_temporary_name_collisions(spark):
    inputs = small_inputs(spark)
    for index in (0, 1, 2):
        inputs[index] = inputs[index].drop("DAT_EXCLUSAO")
    for column in ("__if", "__cond", "__start", "__eligible", "__emission", "odd`field"):
        inputs[0] = inputs[0].withColumn(column, F.lit("untouched"))
    before = inputs[0].collect()
    corrected = engorda.ajusta_inicio_escalonamento_emissao(*inputs)
    assert corrected.schema == inputs[0].schema
    expected = {r.NUM_CONDICAO_IF: r.asDict() for r in before}
    for k in (1, 2):
        expected[Decimal(9007199254740993 + 10 * k)][START] = f"2026-09-0{k}"
    assert {r.NUM_CONDICAO_IF: r.asDict() for r in corrected.collect()} == expected


@pytest.mark.parametrize(
    "case",
    [
        "duplicate",
        "later_after_maturity",
        "end_before_issuance",
        "null_start",
        "invalid_start",
        "missing_float",
    ],
)
def test_existing_validators_still_reject_unrepaired_errors(spark, case):
    tables, source, map_if, map_cond = cloned_tables(spark)
    conditions = tables["CONDICAO_IF"]
    if case == "duplicate":
        conditions = conditions.withColumn(
            START, F.when(F.col("COD_TIPO_CONDICAO_IF") == 3, "2026-09-16").otherwise(F.col(START))
        )
    elif case == "later_after_maturity":
        conditions = conditions.withColumn(
            START,
            F.when(F.col("NUM_CONDICAO_IF").isin(1301, 1302), "2030-01-01").otherwise(F.col(START)),
        )
    elif case == "end_before_issuance":
        conditions = conditions.withColumn(
            END,
            F.when(F.col("NUM_CONDICAO_IF").isin(1101, 1102), "2026-08-31").otherwise(F.col(END)),
        )
    elif case in ("null_start", "invalid_start"):
        conditions = conditions.withColumn(
            START,
            F.when(
                F.col("NUM_CONDICAO_IF").isin(1101, 1102),
                F.lit(None if case == "null_start" else "not-a-date"),
            ).otherwise(F.col(START)),
        )
    else:
        source["JUROS_FLUTUANTE"] = source["JUROS_FLUTUANTE"].limit(0)
        tables["JUROS_FLUTUANTE"] = tables["JUROS_FLUTUANTE"].limit(0)
    before = conditions.collect()
    tables["CONDICAO_IF"] = engorda.ajusta_inicio_escalonamento_emissao(
        conditions, tables[ROOT], source["TITULO"], source["JUROS_FLUTUANTE"], map_if, map_cond
    ).localCheckpoint(eager=True)
    actual = findings(tables)
    if case == "end_before_issuance":
        assert actual["2b.escalonamento_dates"].passed
        meta = validator.Metadata(set(), {}, {}, {}, {})
        date_checks = validator.check_dates(tables, meta, 2)
        assert any(
            f.table == "CONDICAO_IF" and f.severity == validator.SEV_ERROR for f in date_checks
        )
    else:
        check = {
            "duplicate": "2b.escalonamento_unique_dates",
            "missing_float": "2b.escalonamento_coverage",
        }.get(case, "2b.escalonamento_dates")
        assert actual[check].severity == validator.SEV_ERROR
    after = {r.NUM_CONDICAO_IF: r.asDict() for r in tables["CONDICAO_IF"].collect()}
    for row in before:
        expected = row.asDict()
        if case not in ("duplicate", "missing_float"):
            expected[START] = after[row.NUM_CONDICAO_IF][START]
        assert after[row.NUM_CONDICAO_IF] == expected


def test_job_call_uses_frozen_sources_and_maps_before_children_and_validation():
    tree = ast.parse(inspect.getsource(engorda))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "ajusta_inicio_escalonamento_emissao"
    ]
    assert len(calls) == 1
    call = calls[0]
    assert [ast.unparse(arg) for arg in call.args] == [
        "clones",
        "resultados[TABELA_RAIZ][0]",
        "lotes.get('TITULO')",
        "lotes.get('JUROS_FLUTUANTE')",
        "mapeamentos[TABELA_RAIZ]",
        "mapeamentos[CONDICAO_IF_TABLE]",
    ]
    guards = [
        ast.unparse(n.test)
        for n in ast.walk(tree)
        if isinstance(n, ast.If) and n.lineno < call.lineno <= n.end_lineno
    ]
    assert "product_profile.date_strategy == 'standard'" in guards
    assert "product_profile.name == 'cdb_escalonamento'" in guards
    assert "t == CONDICAO_IF_TABLE and TABELA_RAIZ in resultados" in guards
    loop = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.For) and n.lineno < call.lineno <= n.end_lineno
    )
    assert ast.unparse(loop.target) == "t"
    assert ast.unparse(loop.iter) == "ordem"
    statements = {
        ast.unparse(n.targets[0]): n.lineno for n in ast.walk(loop) if isinstance(n, ast.Assign)
    }
    assert statements["mapeamentos[t]"] < call.lineno < statements["resultados[t]"]
    positions = {
        n.func.id if isinstance(n.func, ast.Name) else n.func.attr: n.lineno
        for n in ast.walk(loop)
        if isinstance(n, ast.Call) and isinstance(n.func, (ast.Name, ast.Attribute))
    }
    assert positions["ajusta_datas_condicao_if"] < call.lineno
    for name in (
        "aplica_nulificacao_faltantes",
        "aplica_nulificacao",
        "localCheckpoint",
        "valida_tabela",
    ):
        assert call.lineno < positions[name]


@pytest.mark.parametrize(
    "product,strategy",
    [
        ("cdb_escalonamento", "standard"),
        ("cdb_escalonamento", None),
        ("cdb_resgate", "standard"),
        ("rdb_inclusao", "standard"),
    ],
)
def test_actual_job_date_block_without_synthetic_title_or_float(spark, product, strategy):
    tables, source, map_if, map_cond = cloned_tables(spark)
    tree = ast.parse(inspect.getsource(engorda))
    date_block = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and ast.unparse(n.test) == "product_profile.date_strategy == 'standard'"
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "ajusta_datas_condicao_if"
            for c in ast.walk(n)
        )
    )
    # This is the real job block, with only roots completed and the current CIF map ready.
    namespace = dict(
        vars(engorda),
        product_profile=SimpleNamespace(name=product, date_strategy=strategy),
        t="CONDICAO_IF",
        clones=tables["CONDICAO_IF"],
        lotes=source,
        resultados={ROOT: (tables[ROOT], 1)},
        mapeamentos={ROOT: map_if, "CONDICAO_IF": map_cond},
        engorda_ts=datetime(2026, 9, 13),
        controle_operacional_date=date(2026, 9, 1),
        prazo_vencimento_dias=None,
    )
    expected = tables["CONDICAO_IF"]
    if strategy == "standard":
        expected, _ = engorda.ajusta_datas_condicao_if(expected, source[ROOT], tables[ROOT], map_if)
    expected_rows = {r.NUM_CONDICAO_IF: r.asDict() for r in expected.collect()}
    if product == "cdb_escalonamento" and strategy == "standard":
        for key in (1101, 1102):
            expected_rows[key][START] = "2026-09-01"
    exec(
        compile(ast.Module(body=[date_block], type_ignores=[]), engorda.__file__, "exec"), namespace
    )
    actual = namespace["clones"].collect()
    assert {r.NUM_CONDICAO_IF: r.asDict() for r in actual} == expected_rows
    assert namespace["cols_data"].count(START) == (1 if strategy == "standard" else 0)
