import copy
import dataclasses
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from datagen import engorda_tables as eng


class _ResultSet:
    def __init__(self, rows):
        self.rows = rows
        self.index = -1
        self.closed = False

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def getString(self, position):
        return self.rows[self.index][position - 1]

    def close(self):
        self.closed = True


class _Statement:
    def __init__(self, sql, rows):
        self.sql = sql
        self.rows = rows
        self.binds = {}
        self.fetch_size = None
        self.closed = False

    def setString(self, position, value):
        self.binds[position] = value

    def setFetchSize(self, value):
        self.fetch_size = value

    def executeQuery(self):
        return _ResultSet(self.rows)

    def close(self):
        self.closed = True


class _Connection:
    def __init__(self, batches):
        self.batches = iter(batches)
        self.statements = []
        self.closed = False

    def prepareStatement(self, sql):
        statement = _Statement(sql, next(self.batches))
        self.statements.append(statement)
        return statement

    def close(self):
        self.closed = True


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("engorda-target-fk-admission-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_oracle_parent_lookup_batches_composite_keys_and_closes(monkeypatch):
    connection = _Connection([
        [("1.0", "A")],
        [("3", "C")],
    ])
    monkeypatch.setattr(eng, "_open_oracle_connection", lambda *_args: connection)

    found = eng._oracle_existing_parent_keys(
        object(),
        ("url", "user", "password"),
        "PARENT",
        ("ID", "KIND"),
        [("1", "A"), ("2", "B"), ("3", "C")],
        numeric_flags=(True, False),
        batch_size=2,
    )

    assert found == {("1", "A"), ("3", "C")}
    assert len(connection.statements) == 2
    assert connection.statements[0].sql == (
        "SELECT DISTINCT ID, KIND FROM CETIP.PARENT "
        "WHERE (ID, KIND) IN ((?, ?), (?, ?))"
    )
    assert connection.statements[0].binds == {1: "1", 2: "A", 3: "2", 4: "B"}
    assert all(statement.closed for statement in connection.statements)
    assert connection.closed


def test_live_oracle_pk_max_raises_reservation_floor(monkeypatch):
    connection = _Connection([[('150',)]])
    monkeypatch.setattr(eng, "_open_oracle_connection", lambda *_args: connection)
    plan = eng.PlanoTabela(
        "OPERACAO",
        ("NUM_ID_OPERACAO",),
        pk_regra="OFFSET_PROPRIO",
        pk_start=100,
    )

    eng._apply_oracle_pk_floors(
        object(), ("url", "user", "password"), {"OPERACAO": plan}
    )

    assert plan.pk_start == 151
    assert connection.statements[0].sql == (
        "SELECT MAX(NUM_ID_OPERACAO) FROM CETIP.OPERACAO"
    )
    assert connection.closed


def _fixture(spark):
    sources = {
        eng.TABELA_RAIZ: spark.createDataFrame(
            [(1,), (2,), (3,), (4,)], "NUM_IF long"
        ),
        "OPERACAO": spark.createDataFrame(
            [(11, 1), (12, 2), (13, 3), (14, 4)],
            "NUM_ID_OPERACAO long, NUM_IF long",
        ),
        "ESPECIFICACAO": spark.createDataFrame(
            [(21, 11), (22, 12), (23, 13), (24, 14)],
            "NUM_ID_ESPECIFICACAO long, NUM_ID_OPERACAO long",
        ),
        "ESPECIFICACAO_COMITENTE": spark.createDataFrame(
            [(31, 21, 100), (32, 22, 901), (33, 23, 100), (34, 24, 100)],
            "NUM_ID_ESPECIFICACAO_COMITENTE long, "
            "NUM_ID_ESPECIFICACAO long, NUM_ID_ENTIDADE long",
        ),
        "CARTEIRA_COMITENTE": spark.createDataFrame(
            [(41, 1, 900), (42, 2, 100), (43, 3, 100), (44, 4, 100)],
            "NUM_CARTEIRA_COMITENTE long, NUM_IF long, NUM_ID_ENTIDADE long",
        ),
    }
    spec = {
        eng.TABELA_RAIZ: {
            "pk_cols": ["NUM_IF"],
            "foreign_keys": [],
            "static": False,
        },
        "OPERACAO": {
            "pk_cols": ["NUM_ID_OPERACAO"],
            "foreign_keys": [{
                "columns": ["NUM_IF"],
                "parent_table": eng.TABELA_RAIZ,
                "parent_columns": ["NUM_IF"],
            }],
            "static": False,
        },
        "ESPECIFICACAO": {
            "pk_cols": ["NUM_ID_ESPECIFICACAO"],
            "foreign_keys": [{
                "columns": ["NUM_ID_OPERACAO"],
                "parent_table": "OPERACAO",
                "parent_columns": ["NUM_ID_OPERACAO"],
            }],
            "static": False,
        },
        "ESPECIFICACAO_COMITENTE": {
            "pk_cols": ["NUM_ID_ESPECIFICACAO_COMITENTE"],
            "foreign_keys": [
                {
                    "columns": ["NUM_ID_ESPECIFICACAO"],
                    "parent_table": "ESPECIFICACAO",
                    "parent_columns": ["NUM_ID_ESPECIFICACAO"],
                },
                {
                    "columns": ["NUM_ID_ENTIDADE"],
                    "parent_table": "COMITENTE",
                    "parent_columns": ["NUM_ID_ENTIDADE"],
                },
            ],
            "not_null_cols": ["NUM_ID_ENTIDADE"],
            "static": False,
        },
        "CARTEIRA_COMITENTE": {
            "pk_cols": ["NUM_CARTEIRA_COMITENTE"],
            "foreign_keys": [
                {
                    "columns": ["NUM_IF"],
                    "parent_table": eng.TABELA_RAIZ,
                    "parent_columns": ["NUM_IF"],
                },
                {
                    "columns": ["NUM_ID_ENTIDADE"],
                    "parent_table": "COMITENTE",
                    "parent_columns": ["NUM_ID_ENTIDADE"],
                },
            ],
            "not_null_cols": ["NUM_ID_ENTIDADE"],
            "static": False,
        },
        "COMITENTE": {
            "pk_cols": ["NUM_ID_ENTIDADE"],
            "foreign_keys": [],
            "static": True,
        },
    }
    plans = {
        eng.TABELA_RAIZ: eng.PlanoTabela(eng.TABELA_RAIZ, ("NUM_IF",)),
        "OPERACAO": eng.PlanoTabela(
            "OPERACAO",
            ("NUM_ID_OPERACAO",),
            [eng.FkRemap(("NUM_IF",), eng.TABELA_RAIZ, ("NUM_IF",), True)],
        ),
        "ESPECIFICACAO": eng.PlanoTabela(
            "ESPECIFICACAO",
            ("NUM_ID_ESPECIFICACAO",),
            [eng.FkRemap(
                ("NUM_ID_OPERACAO",), "OPERACAO", ("NUM_ID_OPERACAO",), True
            )],
        ),
        "ESPECIFICACAO_COMITENTE": eng.PlanoTabela(
            "ESPECIFICACAO_COMITENTE",
            ("NUM_ID_ESPECIFICACAO_COMITENTE",),
            [eng.FkRemap(
                ("NUM_ID_ESPECIFICACAO",),
                "ESPECIFICACAO",
                ("NUM_ID_ESPECIFICACAO",),
                True,
            )],
        ),
        "CARTEIRA_COMITENTE": eng.PlanoTabela(
            "CARTEIRA_COMITENTE",
            ("NUM_CARTEIRA_COMITENTE",),
            [eng.FkRemap(("NUM_IF",), eng.TABELA_RAIZ, ("NUM_IF",), True)],
        ),
    }
    profile = dataclasses.replace(
        eng.get_product_profile("cdb_simplificado"),
        integrity=eng.IntegrityPolicy(),
    )
    return sources, spec, plans, profile


def test_target_fk_admission_streams_all_child_edges_once(
    spark, monkeypatch
):
    child = spark.createDataFrame(
        [(1, 10, "A", Decimal("1.5000"), "X")],
        "ID long, FK_A long, FK_B string, FK_N decimal(10,4), FK_KIND string",
    )
    provenance = spark.createDataFrame(
        [(1, 100)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    spec = {
        "CHILD": {
            "pk_cols": ["ID"],
            "foreign_keys": [
                {
                    "columns": ["FK_A"],
                    "parent_table": "PARENT_A",
                    "parent_columns": ["ID"],
                },
                {
                    "columns": ["FK_B"],
                    "parent_table": "PARENT_B",
                    "parent_columns": ["CODE"],
                },
                {
                    "columns": ["FK_N", "FK_KIND"],
                    "parent_table": "PARENT_COMPOSITE",
                    "parent_columns": ["N", "KIND"],
                },
            ],
        }
    }
    plans = {"CHILD": eng.PlanoTabela("CHILD", ("ID",))}
    iterator_calls = 0
    frame_class = type(child)
    original_iterator = frame_class.toLocalIterator

    def tracked_iterator(frame, *args, **kwargs):
        nonlocal iterator_calls
        iterator_calls += 1
        return original_iterator(frame, *args, **kwargs)

    monkeypatch.setattr(frame_class, "toLocalIterator", tracked_iterator)
    probes = []

    def existing_keys(table, columns, keys, numeric_flags):
        probes.append((table, columns, list(keys), numeric_flags))
        return set(keys)

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        spec,
        plans,
        {"CHILD": child},
        {"CHILD": provenance},
        ["CHILD"],
        frozenset(),
        {},
        existing_keys,
    )

    assert iterator_calls == 1
    assert probes == [
        ("PARENT_A", ("ID",), [("10",)], (True,)),
        ("PARENT_B", ("CODE",), [("A",)], (False,)),
        (
            "PARENT_COMPOSITE",
            ("N", "KIND"),
            [("1.5", "X")],
            (True, False),
        ),
    ]
    assert rejected == set()
    assert reasons == {}
    assert missing is None


def test_target_fk_admission_streams_multiple_parent_column_sets_once(
    spark, monkeypatch
):
    parent = spark.createDataFrame(
        [(1, 10, "A"), (2, 20, "B")],
        "ID long, PARENT_ID long, PARENT_CODE string",
    )
    parent_provenance = spark.createDataFrame(
        [(1, 101), (2, 202)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    child = spark.createDataFrame(
        [(11, 10, "A"), (12, 20, "B")],
        "ID long, FK_ID long, FK_CODE string",
    )
    child_provenance = spark.createDataFrame(
        [(11, 101), (12, 202)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    projections = []
    frame_class = type(child)
    original_iterator = frame_class.toLocalIterator

    def tracked_iterator(frame, *args, **kwargs):
        projections.append(tuple(frame.columns))
        return original_iterator(frame, *args, **kwargs)

    monkeypatch.setattr(frame_class, "toLocalIterator", tracked_iterator)
    spec = {
        "PARENT": {"pk_cols": ["ID"], "foreign_keys": []},
        "CHILD": {
            "pk_cols": ["ID"],
            "foreign_keys": [
                {
                    "columns": ["FK_ID"],
                    "parent_table": "PARENT",
                    "parent_columns": ["PARENT_ID"],
                },
                {
                    "columns": ["FK_CODE"],
                    "parent_table": "PARENT",
                    "parent_columns": ["PARENT_CODE"],
                },
            ],
        },
    }
    plans = {
        "PARENT": eng.PlanoTabela("PARENT", ("ID",)),
        "CHILD": eng.PlanoTabela(
            "CHILD",
            ("ID",),
            [
                eng.FkRemap(("FK_ID",), "PARENT", ("PARENT_ID",), False),
                eng.FkRemap(("FK_CODE",), "PARENT", ("PARENT_CODE",), False),
            ],
        ),
    }

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        spec,
        plans,
        {"PARENT": parent, "CHILD": child},
        {"PARENT": parent_provenance, "CHILD": child_provenance},
        ["PARENT", "CHILD"],
        frozenset(),
        {},
        lambda *_args: pytest.fail("internally satisfied keys reached Oracle"),
    )

    assert projections.count((
        eng.ROOT_PROVENANCE_COL, "PARENT_ID", "PARENT_CODE"
    )) == 1
    assert projections.count((eng.ROOT_PROVENANCE_COL, "FK_ID", "FK_CODE")) == 1
    assert rejected == set()
    assert reasons == {}
    assert missing is None


def test_target_fk_admission_keeps_selective_and_hard_edges_isolated(spark):
    child = spark.createDataFrame(
        [(1, "S1", "H1"), (2, "S2", "H2")],
        "ID long, FK_SELECTIVE string, FK_HARD string",
    )
    provenance = spark.createDataFrame(
        [(1, 101), (2, 202)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    spec = {
        "CHILD": {
            "pk_cols": ["ID"],
            "foreign_keys": [
                {
                    "columns": ["FK_SELECTIVE"],
                    "parent_table": "SELECTIVE_PARENT",
                    "parent_columns": ["CODE"],
                },
                {
                    "columns": ["FK_HARD"],
                    "parent_table": "HARD_PARENT",
                    "parent_columns": ["CODE"],
                },
            ],
        }
    }

    def existing_keys(table, _columns, keys, _numeric_flags):
        missing = {("S1",)} if table == "SELECTIVE_PARENT" else {("H2",)}
        return set(keys) - missing

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        spec,
        {"CHILD": eng.PlanoTabela("CHILD", ("ID",))},
        {"CHILD": child},
        {"CHILD": provenance},
        ["CHILD"],
        frozenset({("CHILD", "FK_SELECTIVE")}),
        {},
        existing_keys,
    )

    assert rejected == {202}
    assert reasons == {
        202: {
            "FK CHILD.['FK_HARD'] -> HARD_PARENT.['CODE']"
        }
    }
    assert [tuple(row) for row in missing.collect()] == [
        (101, "CHILD", "FK_SELECTIVE", "S1")
    ]


def test_target_fk_admission_mixed_edges_have_exact_outputs(spark):
    parent = spark.createDataFrame(
        [
            (1, Decimal("1.5000"), "I"),
            (2, Decimal("2.0000"), "I"),
        ],
        "ID long, PARENT_NUMBER decimal(10,4), PARENT_KIND string",
    )
    parent_provenance = spark.createDataFrame(
        [(1, 101), (2, 202)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    child = spark.createDataFrame(
        [
            (11, Decimal("1.5000"), "I", Decimal("10.5000"), "X", "S1", "H1"),
            (12, Decimal("3.0000"), "I", Decimal("20.0000"), "Y", "S2", "H2"),
        ],
        "ID long, FK_INTERNAL_NUMBER decimal(10,4), FK_INTERNAL_KIND string, "
        "FK_EXTERNAL_NUMBER decimal(10,4), FK_EXTERNAL_KIND string, "
        "FK_SELECTIVE string, FK_HARD string",
    )
    child_provenance = spark.createDataFrame(
        [(11, 101), (12, 202)], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )
    spec = {
        "INTERNAL_PARENT": {"pk_cols": ["ID"], "foreign_keys": []},
        "CHILD": {
            "pk_cols": ["ID"],
            "foreign_keys": [
                {
                    "columns": ["FK_INTERNAL_NUMBER", "FK_INTERNAL_KIND"],
                    "parent_table": "INTERNAL_PARENT",
                    "parent_columns": ["PARENT_NUMBER", "PARENT_KIND"],
                },
                {
                    "columns": ["FK_EXTERNAL_NUMBER", "FK_EXTERNAL_KIND"],
                    "parent_table": "EXTERNAL_PARENT",
                    "parent_columns": ["PARENT_NUMBER", "PARENT_KIND"],
                },
                {
                    "columns": ["FK_SELECTIVE"],
                    "parent_table": "SELECTIVE_PARENT",
                    "parent_columns": ["CODE"],
                },
                {
                    "columns": ["FK_HARD"],
                    "parent_table": "HARD_PARENT",
                    "parent_columns": ["CODE"],
                },
            ],
        },
    }
    plans = {
        "INTERNAL_PARENT": eng.PlanoTabela("INTERNAL_PARENT", ("ID",)),
        "CHILD": eng.PlanoTabela(
            "CHILD",
            ("ID",),
            [eng.FkRemap(
                ("FK_INTERNAL_NUMBER", "FK_INTERNAL_KIND"),
                "INTERNAL_PARENT",
                ("PARENT_NUMBER", "PARENT_KIND"),
                False,
            )],
        ),
    }
    probes = []

    def existing_keys(table, columns, keys, numeric_flags):
        probes.append((table, columns, list(keys), numeric_flags))
        missing_by_table = {
            "SELECTIVE_PARENT": {("S1",)},
            "HARD_PARENT": {("H2",)},
        }
        return set(keys) - missing_by_table.get(table, set())

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        spec,
        plans,
        {"INTERNAL_PARENT": parent, "CHILD": child},
        {"INTERNAL_PARENT": parent_provenance, "CHILD": child_provenance},
        ["INTERNAL_PARENT", "CHILD"],
        frozenset({("CHILD", "FK_SELECTIVE")}),
        {},
        existing_keys,
    )

    assert probes == [
        (
            "INTERNAL_PARENT",
            ("PARENT_NUMBER", "PARENT_KIND"),
            [("3", "I")],
            (True, False),
        ),
        (
            "EXTERNAL_PARENT",
            ("PARENT_NUMBER", "PARENT_KIND"),
            [("10.5", "X"), ("20", "Y")],
            (True, False),
        ),
        ("SELECTIVE_PARENT", ("CODE",), [("S1",), ("S2",)], (False,)),
        ("HARD_PARENT", ("CODE",), [("H1",), ("H2",)], (False,)),
    ]
    assert rejected == {202}
    assert reasons == {
        202: {"FK CHILD.['FK_HARD'] -> HARD_PARENT.['CODE']"}
    }
    assert [tuple(row) for row in missing.collect()] == [
        (101, "CHILD", "FK_SELECTIVE", "S1")
    ]
    assert [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in missing.schema.fields
    ] == [
        (eng.ROOT_PROVENANCE_COL, "bigint", True),
        ("TABELA", "string", False),
        ("COLUNA", "string", False),
        ("VALOR", "string", True),
    ]


def test_target_fk_admission_keeps_100k_keys_in_900_batches_without_collect(
    spark, monkeypatch
):
    child = spark.range(100_000).selectExpr("id AS ID", "id AS FK")
    provenance = child.selectExpr(
        "ID", f"ID AS {eng.ROOT_PROVENANCE_COL}"
    )
    batch_sizes = []
    frame_class = type(child)

    def forbidden_collect(*_args, **_kwargs):
        pytest.fail("FK admission must stream instead of collect")

    monkeypatch.setattr(frame_class, "collect", forbidden_collect)

    def existing_keys(_table, _columns, keys, _numeric_flags):
        batch_sizes.append(len(keys))
        return set(keys)

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        {
            "CHILD": {
                "pk_cols": ["ID"],
                "foreign_keys": [{
                    "columns": ["FK"],
                    "parent_table": "PARENT",
                    "parent_columns": ["ID"],
                }],
            }
        },
        {"CHILD": eng.PlanoTabela("CHILD", ("ID",))},
        {"CHILD": child},
        {"CHILD": provenance},
        ["CHILD"],
        frozenset(),
        {},
        existing_keys,
    )

    assert sum(batch_sizes) == 100_000
    assert max(batch_sizes) == 900
    assert len(batch_sizes) == 112
    assert rejected == set()
    assert reasons == {}
    assert missing is None


def test_target_fk_admission_empty_child_has_no_oracle_work(spark):
    child = spark.createDataFrame([], "ID long, FK string")
    provenance = spark.createDataFrame(
        [], f"ID long, {eng.ROOT_PROVENANCE_COL} long"
    )

    rejected, reasons, missing = eng._target_fk_rejections(
        spark,
        {
            "CHILD": {
                "pk_cols": ["ID"],
                "foreign_keys": [{
                    "columns": ["FK"],
                    "parent_table": "PARENT",
                    "parent_columns": ["CODE"],
                }],
            }
        },
        {"CHILD": eng.PlanoTabela("CHILD", ("ID",))},
        {"CHILD": child},
        {"CHILD": provenance},
        ["CHILD"],
        frozenset(),
        {},
        lambda *_args: pytest.fail("empty child reached Oracle"),
    )

    assert rejected == set()
    assert reasons == {}
    assert missing is None


def test_target_fk_admission_refills_direct_and_transitive_orphans(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )
    probes = []

    def existing_keys(_table, _columns, keys, _numeric_flags):
        probes.extend(keys)
        return {key for key in keys if key not in {("900",), ("901",)}}

    selection = eng.seleciona_instrumentos_destino(
        spark,
        {},
        spec,
        num_ifs=None,
        n_instrumentos=2,
        seed=42,
        profile=profile,
        planos=plans,
        ordem=list(plans),
        max_passadas=6,
        existing_key_lookup=existing_keys,
        poda_subtipo=False,
    )

    assert selection.values == [3, 4]
    assert selection.missing_keys is None
    assert {
        row.NUM_IF
        for row in selection.lotes[eng.TABELA_RAIZ].select("NUM_IF").collect()
    } == {3, 4}
    assert ("900",) in probes and ("901",) in probes
    assert set(probes) <= {("100",), ("900",), ("901",)}


def test_target_fk_admission_does_not_replace_explicit_instruments(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )

    def existing_keys(_table, _columns, keys, _numeric_flags):
        return {key for key in keys if key != ("900",)}

    with pytest.raises(ValueError, match=r"FK.*NUM_IF.*1"):
        eng.seleciona_instrumentos_destino(
            spark,
            {},
            spec,
            num_ifs=[1, 3],
            n_instrumentos=None,
            seed=42,
            profile=profile,
            planos=plans,
            ordem=list(plans),
            max_passadas=6,
            existing_key_lookup=existing_keys,
            poda_subtipo=False,
        )


def test_target_fk_admission_fails_closed_when_lookup_fails(spark, monkeypatch):
    sources, spec, plans, profile = _fixture(spark)
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )

    def unavailable(*_args):
        raise RuntimeError("oracle unavailable")

    with pytest.raises(RuntimeError, match="oracle unavailable"):
        eng.seleciona_instrumentos_destino(
            spark,
            {},
            spec,
            num_ifs=[3],
            n_instrumentos=None,
            seed=42,
            profile=profile,
            planos=plans,
            ordem=list(plans),
            max_passadas=6,
            existing_key_lookup=unavailable,
            poda_subtipo=False,
        )


def test_target_fk_admission_preserves_nullable_allowlisted_fk(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    spec["ESPECIFICACAO_COMITENTE"]["not_null_cols"] = []
    profile = dataclasses.replace(
        profile,
        integrity=eng.IntegrityPolicy(
            selective_missing_keys=frozenset({
                ("ESPECIFICACAO_COMITENTE", "NUM_ID_ENTIDADE")
            })
        ),
    )
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )

    def existing_keys(_table, _columns, keys, _numeric_flags):
        return {key for key in keys if key != ("901",)}

    selection = eng.seleciona_instrumentos_destino(
        spark,
        {},
        spec,
        num_ifs=[2, 3],
        n_instrumentos=None,
        seed=42,
        profile=profile,
        planos=plans,
        ordem=list(plans),
        max_passadas=6,
        existing_key_lookup=existing_keys,
        poda_subtipo=False,
    )

    assert selection.values == [2, 3]
    assert [tuple(row) for row in selection.missing_keys.collect()] == [
        ("ESPECIFICACAO_COMITENTE", "NUM_ID_ENTIDADE", "901")
    ]


def test_target_fk_admission_fails_without_partial_selection(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )

    with pytest.raises(ValueError, match=r"2 instrumento.*pedi 3"):
        eng.seleciona_instrumentos_destino(
            spark,
            {},
            spec,
            num_ifs=None,
            n_instrumentos=3,
            seed=42,
            profile=profile,
            planos=plans,
            ordem=list(plans),
            max_passadas=6,
            existing_key_lookup=lambda _table, _columns, keys, _numeric_flags: {
                key for key in keys if key not in {("900",), ("901",)}
            },
            poda_subtipo=False,
        )


def test_target_fk_admission_skips_fks_nullified_before_write(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    spec["CARTEIRA_COMITENTE"]["not_null_cols"] = []
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )
    probes = []

    def existing_keys(_table, _columns, keys, _numeric_flags):
        probes.extend(keys)
        return set(keys)

    selection = eng.seleciona_instrumentos_destino(
        spark,
        {},
        spec,
        num_ifs=[1, 3],
        n_instrumentos=None,
        seed=42,
        profile=profile,
        planos=plans,
        ordem=list(plans),
        max_passadas=6,
        existing_key_lookup=existing_keys,
        poda_subtipo=False,
        nullify_columns={"CARTEIRA_COMITENTE": ("NUM_ID_ENTIDADE",)},
    )

    assert selection.values == [1, 3]
    assert ("900",) not in probes


def test_target_fk_admission_does_not_skip_not_null_nullification(
    spark, monkeypatch
):
    sources, spec, plans, profile = _fixture(spark)
    domain = sources[eng.TABELA_RAIZ].select(eng.COL_NUM_IF)
    monkeypatch.setattr(eng, "_read_source", lambda _spark, _config, table: sources[table])
    monkeypatch.setattr(
        eng, "_dominio_num_if_produto", lambda *_args, **_kwargs: domain
    )

    with pytest.raises(ValueError, match=r"FK.*NUM_IF 1"):
        eng.seleciona_instrumentos_destino(
            spark,
            {},
            spec,
            num_ifs=[1, 3],
            n_instrumentos=None,
            seed=42,
            profile=profile,
            planos=plans,
            ordem=list(plans),
            max_passadas=6,
            existing_key_lookup=lambda _table, _columns, keys, _numeric_flags: {
                key for key in keys if key != ("900",)
            },
            poda_subtipo=False,
            nullify_columns={"CARTEIRA_COMITENTE": ("NUM_ID_ENTIDADE",)},
        )


def test_selective_nullification_uses_numeric_fk_canonicalization(spark):
    source = spark.createDataFrame(
        [(1, Decimal("1.5000"))], "ID long, FK decimal(10,4)"
    )
    missing = spark.createDataFrame(
        [("CHILD", "FK", "1.5")], "TABELA string, COLUNA string, VALOR string"
    )

    result, columns, _ = eng.aplica_nulificacao_faltantes(
        source,
        "CHILD",
        missing,
        frozenset({("CHILD", "FK")}),
    )

    assert result.first().FK is None
    assert columns == ["FK"]


def test_offline_faltantes_preserve_text_key_identity(spark, monkeypatch):
    domain = spark.createDataFrame([(1,), (2,)], "NUM_IF long")
    child = spark.createDataFrame(
        [(1, "A.0"), (2, "A")], "NUM_IF long, FK string"
    )
    missing = spark.createDataFrame(
        [("CHILD", "FK", "A.0")], "TABELA string, COLUNA string, VALOR string"
    )
    monkeypatch.setattr(eng, "_read_source", lambda *_args: child)

    excluded = eng._num_if_excluidos_por_faltantes(
        spark,
        {},
        {"CHILD": {}},
        missing,
        domain,
        frozenset(),
    )

    assert {row.NUM_IF for row in excluded.collect()} == {1}


def test_key_canonicalization_is_type_aware():
    assert eng._canon_oracle_key("1.5000", numeric=True) == "1.5"
    assert eng._canon_oracle_key("A.0", numeric=False) == "A.0"


def test_materialize_job_consumes_frozen_plan_without_resampling(tmp_path, monkeypatch):
    plan_body = {
        "artifact_type": eng.ENGORDA_PLAN_ARTIFACT,
        "schema_version": eng.ENGORDA_PLAN_SCHEMA_VERSION,
        "product": "cdb_simplificado",
        "selected_num_ifs": [10, 20],
        "fator_k": 3,
        "seed": 7,
        "engorda_timestamp": "2026-08-20T10:11:12",
        "controle_operacional_date": "2026-08-20",
        "raw_uri": "oci://raw@ns/run/RAW",
        "output_uri": "oci://out@ns/run/synthetic/cdb",
        "specs_uri": "oci://cfg@ns/spec.json",
        "spec_sha256": "a" * 64,
        "faltantes_uri": "oci://cfg@ns/faltantes",
        "query_num_if_uri": "queries_produtos.sql",
        "selected_lote": {
            "artifact_type": eng.ENGORDA_SELECTED_LOTE_ARTIFACT,
            "schema_version": eng.ENGORDA_SELECTED_LOTE_SCHEMA_VERSION,
            "snapshot_id": "00000000-0000-4000-8000-000000000001",
            "snapshot_uri": "snapshot",
            "table_set": [eng.TABELA_RAIZ],
            "tables": {
                eng.TABELA_RAIZ: {
                    "path": "snapshot/tables/INSTRUMENTO_FINANCEIRO",
                    "row_count": 2,
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {"name": "NUM_IF", "type": "long", "nullable": True,
                             "metadata": {}},
                            {"name": "NUM_TIPO_IF", "type": "long", "nullable": True,
                             "metadata": {}},
                        ],
                    },
                }
            },
            "selective_missing": {
                "present": False,
                "path": None,
                "row_count": 0,
                "schema": None,
            },
        },
        "tables": {
            eng.TABELA_RAIZ: {
                "source_count": 2,
                "synthetic_count": 6,
                "pk": {
                    "rule": "OFFSET_PROPRIO",
                    "count_demand": 6,
                    "step": 1,
                    "minimum_start": 100,
                },
            }
        },
        "cod_if": {"count": 6, "oracle_type": 49},
        "cod_operacao": {"count": 0},
        "meu_numero": {"ordinal_count_demand": 0},
    }
    plan = {**plan_body, "plan_id": eng._plan_id(plan_body)}
    reservation = {
        "artifact_type": eng.ENGORDA_RESERVATION_ARTIFACT,
        "schema_version": eng.ENGORDA_RESERVATION_SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "product": "cdb_simplificado",
        "table_pks": {
            eng.TABELA_RAIZ: {"count": 6, "start": 200, "end": 205, "step": 1}
        },
        "cod_operacao": {"strategy": "oracle_allocator", "count": 0},
        "meu_numero": {"prefix": None, "count": 0, "start": None, "end": None},
    }
    plan_path = tmp_path / "plan.json"
    reservation_path = tmp_path / "reservation.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    reservation_path.write_text(json.dumps(reservation), encoding="utf-8")

    class FakeSpark:
        def stop(self):
            pass

    captured = {}
    monkeypatch.setattr(eng, "create_spark_session", lambda *_args: FakeSpark())
    monkeypatch.setattr(eng, "load_specs", lambda *_args: {"SPEC": {}})
    frozen_lotes = {eng.TABELA_RAIZ: object()}
    monkeypatch.setattr(
        eng,
        "_load_selected_lote_snapshot",
        lambda *_args, **_kwargs: (frozen_lotes, None, {eng.TABELA_RAIZ: 2}),
    )
    monkeypatch.setattr(
        eng,
        "get_engorda_env",
        lambda *_args, **_kwargs: {
            "DATAGEN_RAW_BASE_URI": "oci://raw@ns/run/RAW",
            "DATAGEN_RAW_PREFIX": "",
            "DATAGEN_SYNTHETIC_BASE_URI": "oci://out@ns/run/synthetic/cdb",
            "DATAGEN_SYNTHETIC_PREFIX": "",
            "DATAGEN_CLONE_PREFIX": "ignored",
            "DATAGEN_OUTPUT_URI": "oci://out@ns/run/synthetic/cdb",
            "DATAGEN_SPECS_URI": "oci://cfg@ns/spec.json",
        },
    )
    monkeypatch.setattr(
        eng,
        "executa_clonagem",
        lambda *_args, **kwargs: captured.update(kwargs) or {},
    )

    eng.executar_job(eng.EngordaJob(
        produto="cdb_simplificado",
        phase="materialize",
        plan_uri=str(plan_path),
        reservation_uri=str(reservation_path),
        raw_uri="oci://raw@ns/run/RAW",
        output_uri="oci://out@ns/run/synthetic/cdb",
        specs_uri="oci://cfg@ns/spec.json",
        faltantes_parquet="oci://cfg@ns/faltantes",
    ))

    assert captured["num_ifs"] is None
    assert captured["n_instrumentos"] is None
    assert captured["fator_k"] == 3
    assert captured["seed"] == 7
    assert captured["tipo_oracle"] == 49
    assert captured["phase"] == "materialize"
    assert captured["planned_artifact"] == plan
    assert captured["reservation"] == reservation
    assert captured["snapshot_lotes"] == frozen_lotes
    assert captured["snapshot_faltantes"] is None
    assert captured["snapshot_lote_counts"] == {eng.TABELA_RAIZ: 2}


def test_exact_pipeline_output_uri_passes_destination_safety_check():
    config = {
        "DATAGEN_RAW_BASE_URI": "oci://bucket@ns/run/raw",
        "DATAGEN_RAW_PREFIX": "",
        "DATAGEN_SYNTHETIC_BASE_URI": "oci://bucket@ns/run/synthetic/product",
        "DATAGEN_SYNTHETIC_PREFIX": "",
        "DATAGEN_CLONE_PREFIX": "ignored",
        "DATAGEN_OUTPUT_URI": "oci://bucket@ns/run/synthetic/product",
    }

    assert eng._valida_destino(config) == config["DATAGEN_OUTPUT_URI"]


def test_local_plan_artifact_is_immutable(tmp_path):
    path = tmp_path / "plan.json"
    eng._write_json_artifact(object(), str(path), {"version": 1})

    with pytest.raises(ValueError, match="imutável já existe"):
        eng._write_json_artifact(object(), str(path), {"version": 2})

    assert json.loads(path.read_text(encoding="utf-8")) == {"version": 1}


def test_selected_lote_snapshot_roundtrip_preserves_empty_and_selective_missing(
    spark, tmp_path, monkeypatch
):
    root = spark.createDataFrame(
        [(10, 49, "A"), (20, 49, "B")],
        "NUM_IF long, NUM_TIPO_IF long, VALUE string",
    )
    empty = spark.createDataFrame([], "ID long, NUM_IF long, NOTE string")
    missing = spark.createDataFrame(
        [("CHILD", "FK", "900")],
        "TABELA string, COLUNA string, VALOR string",
    )
    plan_uri = str(tmp_path / "plan.json")

    descriptor = eng._create_selected_lote_snapshot(
        spark,
        plan_uri,
        {eng.TABELA_RAIZ: root, "CHILD": empty},
        missing,
        selected_num_ifs=[10, 20],
        selective_keys=frozenset({("CHILD", "FK")}),
    )
    count_actions = 0
    selective_collects = 0
    original_count_final_lotes = eng._count_final_lotes
    frame_class = type(root)
    original_collect = frame_class.collect
    original_count = frame_class.count
    original_drop_duplicates = frame_class.dropDuplicates

    def tracked_count_final_lotes(frames):
        nonlocal count_actions
        count_actions += 1
        return original_count_final_lotes(frames)

    def tracked_collect(frame):
        nonlocal selective_collects
        if frame.columns == ["TABELA", "COLUNA", "VALOR"]:
            selective_collects += 1
        return original_collect(frame)

    def forbidden_selective_count(frame):
        if frame.columns == ["TABELA", "COLUNA", "VALOR"]:
            raise AssertionError("selective_missing must be validated by one collect")
        return original_count(frame)

    def forbidden_selective_dedup(frame, *args, **kwargs):
        if frame.columns == ["TABELA", "COLUNA", "VALOR"]:
            raise AssertionError("selective_missing dedup must be driver-side")
        return original_drop_duplicates(frame, *args, **kwargs)

    monkeypatch.setattr(eng, "_count_final_lotes", tracked_count_final_lotes)
    monkeypatch.setattr(frame_class, "collect", tracked_collect)
    monkeypatch.setattr(frame_class, "count", forbidden_selective_count)
    monkeypatch.setattr(frame_class, "dropDuplicates", forbidden_selective_dedup)
    lotes, loaded_missing, counts = eng._load_selected_lote_snapshot(
        spark,
        plan_uri,
        descriptor,
        expected_tables={eng.TABELA_RAIZ, "CHILD"},
        selected_num_ifs=[10, 20],
        selective_keys=frozenset({("CHILD", "FK")}),
    )
    assert count_actions == 1
    assert selective_collects == 1

    assert descriptor["artifact_type"] == eng.ENGORDA_SELECTED_LOTE_ARTIFACT
    assert descriptor["schema_version"] == eng.ENGORDA_SELECTED_LOTE_SCHEMA_VERSION
    assert descriptor["table_set"] == ["CHILD", eng.TABELA_RAIZ]
    assert descriptor["tables"]["CHILD"]["row_count"] == 0
    assert counts == {"CHILD": 0, eng.TABELA_RAIZ: 2}
    assert lotes[eng.TABELA_RAIZ].orderBy("NUM_IF").collect() == root.orderBy("NUM_IF").collect()
    assert lotes["CHILD"].count() == 0
    assert loaded_missing.collect() == missing.collect()

    with pytest.raises(Exception, match="exist"):
        eng._write_selected_lote_datasets(
            descriptor,
            {eng.TABELA_RAIZ: root, "CHILD": empty},
            missing,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value["table_set"].append("GHOST"), "table_set"),
        (
            lambda value: value["tables"]["CHILD"].update(row_count=1),
            "row_count",
        ),
        (
            lambda value: value["tables"]["CHILD"]["schema"]["fields"][0].update(
                nullable=False
            ),
            "schema",
        ),
        (
            lambda value: value["selective_missing"].update(row_count=2),
            "selective_missing.*row_count",
        ),
    ],
)
def test_selected_lote_snapshot_rejects_malformed_descriptor_before_use(
    spark, tmp_path, mutation, message
):
    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")
    empty = spark.createDataFrame([], "ID long")
    missing = spark.createDataFrame(
        [("CHILD", "FK", "900")],
        "TABELA string, COLUNA string, VALOR string",
    )
    plan_uri = str(tmp_path / "plan.json")
    descriptor = eng._create_selected_lote_snapshot(
        spark,
        plan_uri,
        {eng.TABELA_RAIZ: root, "CHILD": empty},
        missing,
        selected_num_ifs=[10],
        selective_keys=frozenset({("CHILD", "FK")}),
    )
    malformed = copy.deepcopy(descriptor)
    mutation(malformed)

    with pytest.raises(ValueError, match=message):
        eng._load_selected_lote_snapshot(
            spark,
            plan_uri,
            malformed,
            expected_tables={eng.TABELA_RAIZ, "CHILD"},
            selected_num_ifs=[10],
            selective_keys=frozenset({("CHILD", "FK")}),
        )


def test_selected_lote_snapshot_rejects_root_ids_and_selective_pairs(spark, tmp_path):
    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")
    missing = spark.createDataFrame(
        [("CHILD", "FK", "900")],
        "TABELA string, COLUNA string, VALOR string",
    )
    plan_uri = str(tmp_path / "plan.json")
    descriptor = eng._create_selected_lote_snapshot(
        spark,
        plan_uri,
        {eng.TABELA_RAIZ: root},
        missing,
        selected_num_ifs=[10],
        selective_keys=frozenset({("CHILD", "FK")}),
    )

    with pytest.raises(ValueError, match="root NUM_IF"):
        eng._load_selected_lote_snapshot(
            spark,
            plan_uri,
            descriptor,
            expected_tables={eng.TABELA_RAIZ},
            selected_num_ifs=[20],
            selective_keys=frozenset({("CHILD", "FK")}),
        )
    with pytest.raises(ValueError, match="selective_missing.*allowlist"):
        eng._load_selected_lote_snapshot(
            spark,
            plan_uri,
            descriptor,
            expected_tables={eng.TABELA_RAIZ},
            selected_num_ifs=[10],
            selective_keys=frozenset(),
        )


def test_selected_lote_snapshot_rejects_null_selective_value(spark, tmp_path):
    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")
    missing = spark.createDataFrame(
        [("CHILD", "FK", None)],
        "TABELA string, COLUNA string, VALOR string",
    )
    plan_uri = str(tmp_path / "plan.json")

    with pytest.raises(ValueError, match="selective_missing.*VALOR.*nulo"):
        eng._create_selected_lote_snapshot(
            spark,
            plan_uri,
            {eng.TABELA_RAIZ: root},
            missing,
            selected_num_ifs=[10],
            selective_keys=frozenset({("CHILD", "FK")}),
        )


def test_selected_lote_snapshot_rejects_duplicate_selective_rows(spark, tmp_path):
    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")
    missing = spark.createDataFrame(
        [("CHILD", "FK", "900"), ("CHILD", "FK", "900")],
        "TABELA string, COLUNA string, VALOR string",
    )

    with pytest.raises(ValueError, match="selective_missing.*duplicatas"):
        eng._create_selected_lote_snapshot(
            spark,
            str(tmp_path / "plan.json"),
            {eng.TABELA_RAIZ: root},
            missing,
            selected_num_ifs=[10],
            selective_keys=frozenset({("CHILD", "FK")}),
        )


def test_monta_plano_reconstructs_frozen_pk_plan_without_raw_reads(spark, monkeypatch):
    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("materialize read RAW")

    monkeypatch.setattr(eng, "read_parquet", forbidden)
    monkeypatch.setattr(eng, "_read_pk_max", forbidden)
    monkeypatch.setattr(eng, "_read_source", forbidden)

    planos = eng.monta_plano(
        spark,
        {},
        {
            eng.TABELA_RAIZ: {
                "pk_cols": [eng.COL_NUM_IF],
                "foreign_keys": [],
                "static": False,
            }
        },
        set(),
        0,
        0,
        None,
        10,
        source_frames={eng.TABELA_RAIZ: root},
        frozen_table_plans={
            eng.TABELA_RAIZ: {
                "source_count": 1,
                "synthetic_count": 3,
                "pk": {
                    "rule": "OFFSET_PROPRIO",
                    "count_demand": 3,
                    "step": 7,
                    "minimum_start": 500,
                },
            }
        },
    )

    assert planos[eng.TABELA_RAIZ].pk_regra == "OFFSET_PROPRIO"
    assert planos[eng.TABELA_RAIZ].pk_start == 500
    assert planos[eng.TABELA_RAIZ].pk_passo == 7


def test_materialize_rejects_reservation_below_current_oracle_floor():
    planos = {
        eng.TABELA_RAIZ: eng.PlanoTabela(
            eng.TABELA_RAIZ,
            (eng.COL_NUM_IF,),
            pk_regra="OFFSET_PROPRIO",
            pk_start=151,
        )
    }
    reservation = {
        "table_pks": {
            eng.TABELA_RAIZ: {"start": 150},
        }
    }

    with pytest.raises(ValueError, match="mínimo live seguro 151"):
        eng._validate_reservation_live_pk_floors(planos, reservation)


def test_materialize_uses_frozen_lotes_and_rejects_spec_hash_divergence_before_side_effects(
    spark, monkeypatch
):
    class AllocatorReached(RuntimeError):
        pass

    root = spark.createDataFrame(
        [(10, 49, "OLD")], "NUM_IF long, NUM_TIPO_IF long, COD_IF string"
    )
    profile = eng.get_product_profile("cdb_simplificado")
    profile = dataclasses.replace(
        profile,
        business_keys=dataclasses.replace(profile.business_keys, operation=None),
    )
    config = {
        "DATAGEN_RAW_BASE_URI": "oci://raw@ns/run/RAW",
        "DATAGEN_RAW_PREFIX": "",
        "DATAGEN_SYNTHETIC_BASE_URI": "oci://out@ns/run/synthetic/cdb",
        "DATAGEN_SYNTHETIC_PREFIX": "",
        "DATAGEN_CLONE_PREFIX": "ignored",
        "DATAGEN_OUTPUT_URI": "oci://out@ns/run/synthetic/cdb",
        "DATAGEN_SPECS_URI": "oci://cfg@ns/spec.json",
    }
    snapshot_id = "00000000-0000-4000-8000-000000000001"
    snapshot_uri = f"plan.json.selected-lote/{snapshot_id}"
    descriptor = {
        "artifact_type": eng.ENGORDA_SELECTED_LOTE_ARTIFACT,
        "schema_version": eng.ENGORDA_SELECTED_LOTE_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "snapshot_uri": snapshot_uri,
        "table_set": [eng.TABELA_RAIZ],
        "tables": {
            eng.TABELA_RAIZ: {
                "path": f"{snapshot_uri}/tables/{eng.TABELA_RAIZ}",
                "row_count": 1,
                "schema": root.schema.jsonValue(),
            }
        },
        "selective_missing": {
            "present": False,
            "path": None,
            "row_count": 0,
            "schema": None,
        },
    }
    root_plan = eng.PlanoTabela(
        eng.TABELA_RAIZ,
        (eng.COL_NUM_IF,),
        pk_regra="OFFSET_PROPRIO",
        pk_start=100,
        pk_passo=1,
    )
    engorda_ts = datetime(2026, 8, 20, 10, 11, 12)
    spec = {
        eng.TABELA_RAIZ: {
            "pk_cols": [eng.COL_NUM_IF],
            "foreign_keys": [],
            "static": False,
        }
    }
    normalized_spec = eng.normalize_specs(spec)
    spec_sha256 = hashlib.sha256(
        eng._canonical_json(normalized_spec).encode("ascii")
    ).hexdigest()
    plan = eng._build_engorda_plan(
        config=config,
        specs_uri=config["DATAGEN_SPECS_URI"],
        spec_sha256=spec_sha256,
        product_profile=profile,
        valores=[10],
        fator_k=1,
        seed=42,
        engorda_ts=engorda_ts,
        controle_operacional_date=date(2026, 8, 20),
        tipo_derivado=49,
        planos={eng.TABELA_RAIZ: root_plan},
        lotes={eng.TABELA_RAIZ: root},
        lote_counts={eng.TABELA_RAIZ: 1},
        faltantes_uri=None,
        query_num_if_uri=profile.query_filename,
        selected_lote=descriptor,
        anular_cols=profile.integrity.nullify_mapping(),
    )
    reservation = {
        "artifact_type": eng.ENGORDA_RESERVATION_ARTIFACT,
        "schema_version": eng.ENGORDA_RESERVATION_SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "product": profile.name,
        "table_pks": {
            eng.TABELA_RAIZ: {"count": 1, "start": 200, "end": 200, "step": 1}
        },
        "cod_operacao": {"strategy": "oracle_allocator", "count": 0},
        "meu_numero": {"prefix": None, "count": 0, "start": None, "end": None},
    }
    forbidden = (
        "_carrega_faltantes",
        "_dominio_num_if_produto",
        "_num_if_inconsistentes_subtipo",
        "seleciona_instrumentos",
        "seleciona_instrumentos_destino",
        "calcula_lotes",
        "_deriva_tipo_oracle",
        "_target_fk_rejections",
        "read_parquet",
        "_read_pk_max",
        "_read_source",
    )
    for name in forbidden:
        monkeypatch.setattr(
            eng,
            name,
            lambda *_args, _name=name, **_kwargs: (_ for _ in ()).throw(
                AssertionError(f"materialize called {_name}")
            ),
        )
    monkeypatch.setitem(
        eng.TABELAS_ENGORDA_POR_PRODUTO,
        "cdb_simplificado",
        (eng.TABELA_RAIZ,),
    )
    monkeypatch.setattr(eng, "_valida_contrato_nulificacao_seletiva", lambda *_: None)
    monkeypatch.setattr(eng, "_oracle_credentials", lambda *_: ("url", "user", "pw"))
    events = []

    def apply_live_floor(_jvm, _credentials, planos):
        events.append("pk_floor")
        planos[eng.TABELA_RAIZ].pk_start = 150

    original_validate_reservation = eng._validate_reservation_artifact

    def validate_reservation(*args):
        events.append("current_plan_matched")
        return original_validate_reservation(*args)

    def assert_absent(*_args):
        events.append("output_absent")

    def stage_and_publish(_spark, _path, prepare, *, require_absent=False):
        assert require_absent is True
        events.append("staging")
        prepare("staging")

    def allocator(*_args, **_kwargs):
        events.append("allocator")
        raise AllocatorReached()

    monkeypatch.setattr(eng, "_apply_oracle_pk_floors", apply_live_floor)
    monkeypatch.setattr(eng, "_validate_reservation_artifact", validate_reservation)
    monkeypatch.setattr(eng, "_assert_exact_output_absent", assert_absent)
    monkeypatch.setattr(eng, "_stage_and_publish", stage_and_publish)
    monkeypatch.setattr(eng, "_materialize_code_map", allocator)
    monkeypatch.setattr(eng, "loga_chaves_amostra", lambda *_: None)
    monkeypatch.setattr(eng, "valida_tabela", lambda *_: [])
    monkeypatch.setattr(
        eng,
        "_count_final_lotes",
        lambda *_: (_ for _ in ()).throw(
            AssertionError("materialize recounted frozen snapshot")
        ),
    )

    with pytest.raises(AllocatorReached):
        eng.executa_clonagem(
            spark,
            config,
            spec,
            product_profile=profile,
            fator_k=1,
            seed=42,
            engorda_ts=engorda_ts,
            phase="materialize",
            planned_artifact=plan,
            reservation=reservation,
            snapshot_lotes={eng.TABELA_RAIZ: root},
            snapshot_faltantes=None,
            snapshot_lote_counts={eng.TABELA_RAIZ: 1},
            controle_operacional_date=date(2026, 8, 20),
            specs_uri=config["DATAGEN_SPECS_URI"],
        )

    assert events == [
        "pk_floor",
        "current_plan_matched",
        "output_absent",
        "staging",
        "allocator",
    ]

    events.clear()
    changed_spec = copy.deepcopy(spec)
    changed_spec[eng.TABELA_RAIZ]["mutable_metadata"] = "changed"
    with pytest.raises(ValueError, match="materialize divergiu do plano congelado"):
        eng.executa_clonagem(
            spark,
            config,
            changed_spec,
            product_profile=profile,
            fator_k=1,
            seed=42,
            engorda_ts=engorda_ts,
            phase="materialize",
            planned_artifact=plan,
            reservation=reservation,
            snapshot_lotes={eng.TABELA_RAIZ: root},
            snapshot_faltantes=None,
            snapshot_lote_counts={eng.TABELA_RAIZ: 1},
            controle_operacional_date=date(2026, 8, 20),
            specs_uri=config["DATAGEN_SPECS_URI"],
        )

    assert events == ["pk_floor"]


def test_phase_all_performs_no_snapshot_io(spark, monkeypatch):
    class InputsResolved(RuntimeError):
        pass

    root = spark.createDataFrame([(10, 49)], "NUM_IF long, NUM_TIPO_IF long")
    profile = eng.get_product_profile("cdb_simplificado")
    monkeypatch.setitem(
        eng.TABELAS_ENGORDA_POR_PRODUTO,
        "cdb_simplificado",
        (eng.TABELA_RAIZ,),
    )
    monkeypatch.setattr(eng, "_valida_contrato_nulificacao_seletiva", lambda *_: None)
    monkeypatch.setattr(eng, "_carrega_faltantes", lambda *_: None)
    monkeypatch.setattr(eng, "_oracle_credentials", lambda *_: None)
    monkeypatch.setattr(eng, "monta_plano", lambda *_args, **_kwargs: {
        eng.TABELA_RAIZ: eng.PlanoTabela(
            eng.TABELA_RAIZ,
            (eng.COL_NUM_IF,),
            pk_regra="OFFSET_PROPRIO",
            pk_start=100,
        )
    })
    monkeypatch.setattr(eng, "ordem_topologica", lambda *_: [eng.TABELA_RAIZ])
    monkeypatch.setattr(eng, "seleciona_instrumentos", lambda *_args, **_kwargs: [10])
    monkeypatch.setattr(eng, "_deriva_tipo_oracle", lambda *_: 49)
    monkeypatch.setattr(eng, "calcula_lotes", lambda *_args, **_kwargs: {
        eng.TABELA_RAIZ: root
    })
    monkeypatch.setattr(
        eng,
        "_create_selected_lote_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("phase all wrote snapshot")
        ),
    )
    monkeypatch.setattr(
        eng,
        "_load_selected_lote_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("phase all read snapshot")
        ),
    )
    monkeypatch.setattr(
        eng,
        "_count_final_lotes",
        lambda *_: (_ for _ in ()).throw(InputsResolved()),
    )

    with pytest.raises(InputsResolved):
        eng.executa_clonagem(
            spark,
            {"DATAGEN_SPECS_URI": "spec.json"},
            {
                eng.TABELA_RAIZ: {
                    "pk_cols": [eng.COL_NUM_IF],
                    "foreign_keys": [],
                    "static": False,
                }
            },
            product_profile=profile,
            meu_numero_prefix="321",
            num_ifs=[10],
            dry_run=True,
            phase="all",
            controle_operacional_date=date(2026, 8, 20),
        )


def test_exact_pipeline_output_refuses_overwrite():
    class FileSystem:
        @staticmethod
        def exists(_path):
            return True

    class Path:
        def __init__(self, value):
            self.value = value

        @staticmethod
        def getFileSystem(_configuration):
            return FileSystem()

    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                org=SimpleNamespace(
                    apache=SimpleNamespace(
                        hadoop=SimpleNamespace(fs=SimpleNamespace(Path=Path))
                    )
                )
            ),
            _jsc=SimpleNamespace(hadoopConfiguration=lambda: object()),
        )
    )

    with pytest.raises(ValueError, match="imutável já existe"):
        eng._assert_exact_output_absent(
            spark, "oci://bucket@ns/run/synthetic/product"
        )


def test_require_absent_rechecks_after_staging_and_preserves_raced_output(
    spark, tmp_path
):
    final_path = tmp_path / "final"

    def prepare(staging_path):
        staging = Path(staging_path)
        staging.mkdir(parents=True)
        (staging / "new.marker").write_text("new", encoding="ascii")
        final_path.mkdir()
        (final_path / "winner.marker").write_text("winner", encoding="ascii")

    with pytest.raises(ValueError, match="imutável já existe"):
        eng._stage_and_publish(
            spark,
            str(final_path),
            prepare,
            require_absent=True,
        )

    assert (final_path / "winner.marker").read_text(encoding="ascii") == "winner"
    assert not (final_path / "new.marker").exists()


def test_require_absent_never_replaces_output_raced_after_recheck():
    class RacingFileSystem:
        def __init__(self):
            self.entries = {"staging": "new", "final": "winner"}
            self.renames = []

        def exists(self, path):
            return str(path) in self.entries

        def rename(self, source, target):
            self.renames.append((str(source), str(target)))
            return False

    fs = RacingFileSystem()

    with pytest.raises(ValueError, match="race de publicação"):
        eng._promote_staging_paths(
            fs,
            "staging",
            "final",
            "backup",
            require_absent=True,
        )

    assert fs.entries["final"] == "winner"
    assert fs.renames == []


def test_reservation_must_honor_requested_meu_numero_prefix():
    plan = {
        "plan_id": "plan",
        "product": "cdb_simplificado",
        "tables": {},
        "cod_operacao": {"count": 0},
        "meu_numero": {
            "ordinal_count_demand": 1,
            "requested_prefix": "321",
        },
    }
    reservation = {
        "artifact_type": eng.ENGORDA_RESERVATION_ARTIFACT,
        "schema_version": eng.ENGORDA_RESERVATION_SCHEMA_VERSION,
        "plan_id": "plan",
        "product": "cdb_simplificado",
        "table_pks": {},
        "cod_operacao": {"strategy": "oracle_allocator", "count": 0},
        "meu_numero": {"prefix": "322", "count": 1, "start": 1, "end": 1},
    }

    with pytest.raises(ValueError, match="requested_prefix"):
        eng._validate_reservation_artifact(plan, reservation)
