import dataclasses
import json
from decimal import Decimal
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
        "schema_version": eng.ENGORDA_ARTIFACT_SCHEMA_VERSION,
        "product": "cdb_simplificado",
        "selected_num_ifs": [10, 20],
        "fator_k": 3,
        "seed": 7,
        "engorda_timestamp": "2026-08-20T10:11:12",
        "controle_operacional_date": "2026-08-20",
        "raw_uri": "oci://raw@ns/run/RAW",
        "output_uri": "oci://out@ns/run/synthetic/cdb",
        "specs_uri": "oci://cfg@ns/spec.json",
        "faltantes_uri": "oci://cfg@ns/faltantes",
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
        "schema_version": eng.ENGORDA_ARTIFACT_SCHEMA_VERSION,
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

    assert captured["num_ifs"] == [10, 20]
    assert captured["n_instrumentos"] is None
    assert captured["fator_k"] == 3
    assert captured["seed"] == 7
    assert captured["tipo_oracle"] == 49
    assert captured["phase"] == "materialize"
    assert captured["planned_artifact"] == plan
    assert captured["reservation"] == reservation


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
        "schema_version": eng.ENGORDA_ARTIFACT_SCHEMA_VERSION,
        "plan_id": "plan",
        "product": "cdb_simplificado",
        "table_pks": {},
        "cod_operacao": {"strategy": "oracle_allocator", "count": 0},
        "meu_numero": {"prefix": "322", "count": 1, "start": 1, "end": 1},
    }

    with pytest.raises(ValueError, match="requested_prefix"):
        eng._validate_reservation_artifact(plan, reservation)
