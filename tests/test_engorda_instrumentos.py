import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parent))
import engorda_instrumentos as eng  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]")
               .appName("engorda-instrumentos-test")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_k2_business_codes_are_unique_and_mapping_is_repartition_stable(spark):
    clones = spark.createDataFrame(
        [(102, "OLD"), (100, "OLD"), (101, "OLD"), (103, "OLD")],
        "NUM_IF long, COD_IF string",
    )

    def mapping(df):
        slots = eng._code_slots(df, "NUM_IF", "COD_IF", "NUM_IF_NOVO", "COD_IF_ORIG")
        return eng._materialize_code_map(
            spark, slots, code_kind="COD_IF", generated_alias="COD_IF_GERADO",
            out_path=None, dry_run=True, credentials=None, batch_size=2,
            engorda_date=date(2026, 7, 19),
        )

    first = {(r.NUM_IF_NOVO, r.COD_IF_GERADO) for r in mapping(clones.repartition(1)).collect()}
    second = {(r.NUM_IF_NOVO, r.COD_IF_GERADO) for r in mapping(clones.repartition(3)).collect()}
    assert first == second
    assert len({code for _, code in first}) == 4
    assert all(__import__("re").fullmatch(eng.COD_IF_PATTERN, code) for _, code in first)


@pytest.mark.parametrize("factor", [1, 2])
def test_clone_helper_regenerates_k1_and_k2_rows(spark, factor):
    lote = spark.createDataFrame([(7, "OLD")], "NUM_IF long, COD_IF string")
    plano = eng.PlanoTabela(
        name=eng.TABELA_RAIZ, pk_cols=("NUM_IF",), pk_regra="OFFSET_PROPRIO",
        pk_start=100, pk_passo=1,
    )
    clones, _ = eng.clona_tabela(spark, plano, lote, factor, {})
    slots = eng._code_slots(clones, "NUM_IF", "COD_IF", "NUM_IF_NOVO", "COD_IF_ORIG")
    mapping = eng._materialize_code_map(
        spark, slots, code_kind="COD_IF", generated_alias="COD_IF_GERADO",
        out_path=None, dry_run=True, credentials=None, batch_size=50,
        engorda_date=date(2026, 7, 19),
    )
    out = eng._attach_generated_code(
        clones, mapping, pk_col="NUM_IF", new_pk_alias="NUM_IF_NOVO",
        code_col="COD_IF", generated_alias="COD_IF_GERADO")
    rows = out.orderBy("NUM_IF").collect()
    assert [r.NUM_IF for r in rows] == list(range(100, 100 + factor))
    assert len({r.COD_IF for r in rows}) == factor
    assert all(r.COD_IF != "OLD" for r in rows)


def test_dry_run_placeholders_have_valid_formats_without_jdbc_or_paths(spark, monkeypatch):
    path_calls = []
    monkeypatch.setattr(
        eng, "_iter_oracle_code_batches",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("JDBC called")),
    )
    monkeypatch.setattr(eng, "_delete_path", lambda *args: path_calls.append(("delete", args)))
    monkeypatch.setattr(eng, "escreve_tabela", lambda *args: path_calls.append(("write", args)))
    slots = spark.createDataFrame([(1, 10, "OLD"), (2, 11, "OLD")],
                                  "ORDINAL long, PK long, OLD string")
    cod_if = eng._materialize_code_map(
        spark, slots, code_kind="COD_IF", generated_alias="CODE", out_path=None,
        dry_run=True, credentials=None, batch_size=1, engorda_date=date(2026, 7, 19))
    cod_op = eng._materialize_code_map(
        spark, slots, code_kind="COD_OPERACAO", generated_alias="CODE", out_path=None,
        dry_run=True, credentials=None, batch_size=1, engorda_date=date(2026, 7, 19))
    assert all(__import__("re").fullmatch(eng.COD_IF_PATTERN, r.CODE)
               for r in cod_if.collect())
    assert all(__import__("re").fullmatch(eng.COD_OPERACAO_PATTERN, r.CODE)
               for r in cod_op.collect())
    assert path_calls == []


class FakeResultSet:
    def __init__(self, rows):
        self.rows = rows
        self.index = -1
        self.closed = False

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def getInt(self, position):
        return self.rows[self.index][position - 1]

    def getString(self, position):
        return self.rows[self.index][position - 1]

    def getTimestamp(self, position):
        return self.rows[self.index][position - 1]

    def close(self):
        self.closed = True


class FakeStatement:
    def __init__(self, sql, rows):
        self.sql = sql
        self.rows = rows
        self.binds = {}
        self.fetch_size = None
        self.closed = False

    def setString(self, position, value):
        self.binds[position] = value

    def executeQuery(self):
        return FakeResultSet(self.rows)

    def setFetchSize(self, value):
        self.fetch_size = value

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, batches):
        self.batches = iter(batches)
        self.statements = []
        self.closed = False

    def prepareStatement(self, sql):
        statement = FakeStatement(sql, next(self.batches))
        self.statements.append(statement)
        return statement

    def close(self):
        self.closed = True


def test_open_oracle_connection_uses_spark_context_classloader_not_driver_manager():
    expected = object()

    class Properties(dict):
        def setProperty(self, key, value):
            self[key] = value

    class Driver:
        def connect(self, url, properties):
            assert url == "jdbc:oracle:thin:@target"
            assert properties == {"user": "alice", "password": "secret"}
            return expected

    class DriverClass:
        @staticmethod
        def newInstance():
            return Driver()

    class Loader:
        @staticmethod
        def loadClass(name):
            assert name == "oracle.jdbc.OracleDriver"
            return DriverClass()

    class CurrentThread:
        @staticmethod
        def getContextClassLoader():
            return Loader()

    class Thread:
        @staticmethod
        def currentThread():
            return CurrentThread()

    class DriverManager:
        @staticmethod
        def getConnection(*_):
            raise AssertionError("DriverManager must not be used")

    jvm = SimpleNamespace(java=SimpleNamespace(
        lang=SimpleNamespace(Thread=Thread),
        util=SimpleNamespace(Properties=Properties),
        sql=SimpleNamespace(DriverManager=DriverManager),
    ))

    assert eng._open_oracle_connection(
        jvm, "jdbc:oracle:thin:@target", "alice", "secret"
    ) is expected


def test_oracle_allocator_batches_and_uses_exact_function_sql(monkeypatch):
    connection = FakeConnection([
        [(1, "CDB10000001"), (2, "CDB10000002")],
        [(1, "CDB10000003")],
    ])
    calls = []
    monkeypatch.setattr(
        eng, "_open_oracle_connection",
        lambda *args: calls.append(args) or connection,
    )
    batches = list(eng._iter_oracle_code_batches(
        object(), "jdbc-secret", "user", "password", code_kind="COD_IF",
        total=3, batch_size=2, engorda_date=date(2026, 7, 19)))
    assert batches == [[(1, "CDB10000001"), (2, "CDB10000002")],
                       [(3, "CDB10000003")]]
    assert len(calls) == 1
    assert connection.statements[0].sql == (
        "SELECT LEVEL ordinal, "
        "CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(49, TO_DATE(?, 'YYYY-MM-DD')) code "
        "FROM dual CONNECT BY LEVEL <= 2")
    assert connection.statements[1].sql.endswith("CONNECT BY LEVEL <= 1")
    assert all(s.binds == {1: "2026-07-19"} and s.closed for s in connection.statements)
    assert connection.closed
    assert eng._allocation_sql("COD_OPERACAO", 5) == (
        "SELECT LEVEL ordinal, CETIP.GET_COD_OPERACAO code FROM dual "
        "CONNECT BY LEVEL <= 5")


def test_preflight_oracle_query_streams_timestamp_and_tos(spark, tmp_path, monkeypatch):
    connection = FakeConnection([[
        ("2026-07-19 12:34:56", "10", "3210000001", "4509"),
    ]])
    monkeypatch.setattr(eng, "_open_oracle_connection", lambda *args: connection)
    rows = eng._read_existing_meu_tuples(
        spark, ("url", "user", "password"), engorda_date=date(2026, 7, 19),
        prefix="321", temp_path=str(tmp_path / "preflight"), chunk_size=1)
    assert rows.first().DAT_OPERACAO == "2026-07-19 12:34:56"
    assert rows.first().NUM_ID_TIPO_OPER_OBJETO_SERV == "4509"
    statement = connection.statements[0]
    assert statement.sql.count("NUM_ID_TIPO_OPER_OBJETO_SERV") >= 3
    assert statement.fetch_size == 1 and statement.closed and connection.closed


@pytest.mark.parametrize("rows", [
    [(1, "bad")],
    [(1, "0000000000000001"), (2, "0000000000000001")],
    [(1, "0000000000000001")],
])
def test_oracle_allocator_aborts_malformed_duplicate_or_short_without_retry(monkeypatch, rows):
    connection = FakeConnection([rows])
    opened = []
    monkeypatch.setattr(
        eng, "_open_oracle_connection",
        lambda *args: opened.append(1) or connection,
    )
    with pytest.raises(ValueError):
        list(eng._iter_oracle_code_batches(
            object(), "url", "user", "password", code_kind="COD_OPERACAO",
            total=2 if len(rows) != 1 or rows[0][1] != "bad" else 1,
            batch_size=2, engorda_date=date(2026, 7, 19)))
    assert opened == [1]
    assert connection.closed


def test_materialized_map_rejects_cross_batch_duplicate(spark, tmp_path, monkeypatch):
    slots = spark.createDataFrame([(1, 10, "OLD"), (2, 11, "OLD")],
                                  "ORDINAL long, PK long, OLD string")
    monkeypatch.setattr(eng, "_iter_oracle_code_batches", lambda *args, **kwargs: iter([
        [(1, "0000000000000001")],
        [(2, "0000000000000001")],
    ]))
    with pytest.raises(ValueError, match="mapa Parquet"):
        eng._materialize_code_map(
            spark, slots, code_kind="COD_OPERACAO", generated_alias="CODE",
            out_path=str(tmp_path / "map"), dry_run=False,
            credentials=("url", "user", "password"), batch_size=1,
            engorda_date=date(2026, 7, 19))


def test_multiple_batches_join_slots_once(spark, tmp_path, monkeypatch):
    slots = spark.createDataFrame(
        [(i, 100 + i, "OLD") for i in range(1, 6)],
        "ORDINAL long, PK long, OLD string",
    )
    monkeypatch.setattr(eng, "_iter_oracle_code_batches", lambda *args, **kwargs: iter([
        [(1, "0000000000000001"), (2, "0000000000000002")],
        [(3, "0000000000000003"), (4, "0000000000000004")],
        [(5, "0000000000000005")],
    ]))
    joins = []
    original_join = eng._join_code_chunks

    def counted_join(*args):
        joins.append(1)
        return original_join(*args)

    monkeypatch.setattr(eng, "_join_code_chunks", counted_join)
    mapping = eng._materialize_code_map(
        spark, slots, code_kind="COD_OPERACAO", generated_alias="CODE",
        out_path=str(tmp_path / "map"), dry_run=False,
        credentials=("url", "user", "password"), batch_size=2,
        engorda_date=date(2026, 7, 19))
    assert mapping.count() == 5
    assert joins == [1]


def _operation_df(spark):
    return spark.createDataFrame([
        (20, "2020-01-01", "old1", "old2", "keep1", "keep2", "1", "2", "4509",
         1, "CDB10000001"),
        (10, "2020-01-01", "old3", "old4", "keep3", "keep4", "3", "3", "4509",
         1, "CDB10000001"),
    ], """NUM_ID_OPERACAO long, DAT_OPERACAO string,
           NUM_CONTROLE_LANCAMENTO_P1 string, NUM_CONTROLE_LANCAMENTO_P2 string,
           NUM_CONTROLE_LANCAMENTO_P1_ORIGINAL string,
           NUM_CONTROLE_LANCAMENTO_P2_ORIGINAL string,
           NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string,
           NUM_ID_TIPO_OPER_OBJETO_SERV string, NUM_IF long, COD_IF string""")


@pytest.mark.parametrize("factor", [1, 2])
def test_operation_cod_if_is_propagated_by_normalized_num_if_and_preserves_rows(
        spark, factor):
    instrumentos = spark.createDataFrame(
        [(index, f"CDB{index:08d}", None) for index in range(1, factor + 1)],
        "NUM_IF long, COD_IF string, DAT_EXCLUSAO string",
    )
    operacoes = spark.createDataFrame(
        [
            (index * 10 + suffix, Decimal(f"{index}.000"), "OLD", "LEGACY", suffix)
            for index in range(1, factor + 1)
            for suffix in (1, 2)
        ],
        "NUM_ID_OPERACAO long, NUM_IF decimal(38,9), COD_IF string, "
        "COD_ANTIGO_IF string, PAYLOAD long",
    )

    rows = eng._propagate_root_cod_if(instrumentos, operacoes).orderBy(
        "NUM_ID_OPERACAO"
    ).collect()

    assert [row.COD_IF for row in rows] == [
        f"CDB{index:08d}" for index in range(1, factor + 1) for _ in (1, 2)
    ]
    assert [(row.COD_ANTIGO_IF, row.PAYLOAD) for row in rows] == [
        ("LEGACY", suffix)
        for _ in range(1, factor + 1)
        for suffix in (1, 2)
    ]
    assert eng._propagate_root_cod_if(
        instrumentos.repartition(2), operacoes.repartition(2)
    ).orderBy("NUM_ID_OPERACAO").collect() == rows


@pytest.mark.parametrize(
    "failure", ["missing", "operation_missing", "duplicate", "blank", "inactive", "unmatched"]
)
def test_operation_cod_if_propagation_rejects_invalid_root_mapping(spark, failure):
    instrumentos = spark.createDataFrame([(1, "CDB10000001", None)],
                                         "NUM_IF long, COD_IF string, DAT_EXCLUSAO string")
    operacoes = spark.createDataFrame([(10, 1, "OLD")],
                                      "NUM_ID_OPERACAO long, NUM_IF long, COD_IF string")
    if failure == "missing":
        instrumentos = instrumentos.drop("COD_IF")
    elif failure == "operation_missing":
        operacoes = operacoes.drop("COD_IF")
    elif failure == "duplicate":
        instrumentos = instrumentos.unionByName(instrumentos)
    elif failure == "blank":
        instrumentos = instrumentos.withColumn("COD_IF", F.lit("  "))
    elif failure == "inactive":
        instrumentos = instrumentos.withColumn("DAT_EXCLUSAO", F.lit("2026-07-19"))
    else:
        operacoes = operacoes.withColumn("NUM_IF", F.lit(2))

    with pytest.raises(ValueError):
        eng._propagate_root_cod_if(instrumentos, operacoes)


def test_operation_cod_if_propagation_rejects_numeric_cod_if_type(spark):
    instrumentos = spark.createDataFrame(
        [(1, "CDB10000001", None)], "NUM_IF long, COD_IF string, DAT_EXCLUSAO string"
    )
    operacoes = spark.createDataFrame(
        [(10, 1, 0)], "NUM_ID_OPERACAO long, NUM_IF long, COD_IF long"
    )

    with pytest.raises(ValueError, match="StringType"):
        eng._propagate_root_cod_if(instrumentos, operacoes)


@pytest.mark.parametrize("prefix", ["", "012", "12", "1234", "A12"])
def test_meu_prefix_is_mandatory_three_nonzero_leading_digits(prefix):
    with pytest.raises(Exception):
        eng._validate_meu_numero_prefix(prefix)


def test_meu_generation_share_differ_same_account_separate_and_preserve_originals(spark):
    out = eng._generate_meu_numeros(_operation_df(spark), "321", date(2026, 7, 19))
    rows = {r.NUM_ID_OPERACAO: r for r in out.collect()}
    different = rows[20]
    same = rows[10]
    assert different.NUM_CONTROLE_LANCAMENTO_P1 == different.NUM_CONTROLE_LANCAMENTO_P2
    assert same.NUM_CONTROLE_LANCAMENTO_P1 != same.NUM_CONTROLE_LANCAMENTO_P2
    assert same.NUM_CONTROLE_LANCAMENTO_P1 == "3210000001"
    assert same.NUM_CONTROLE_LANCAMENTO_P2 == "3210000002"
    assert different.NUM_CONTROLE_LANCAMENTO_P1 == "3210000003"
    controls = {
        different.NUM_CONTROLE_LANCAMENTO_P1,
        same.NUM_CONTROLE_LANCAMENTO_P1,
        same.NUM_CONTROLE_LANCAMENTO_P2,
    }
    assert all(len(value) == 10 and value.startswith("321") and value.isdigit()
               for value in controls)
    assert len(controls) == 3
    assert same.NUM_CONTROLE_LANCAMENTO_P1_ORIGINAL == "keep3"
    assert different.NUM_CONTROLE_LANCAMENTO_P2_ORIGINAL == "keep2"
    assert {r.DAT_OPERACAO for r in rows.values()} == {"2026-07-19"}
    assert eng._flatten_meu_tuples(out).count() == eng._flatten_meu_tuples(
        out).dropDuplicates().count()


def test_meu_ordinals_are_repartition_stable(spark):
    source = _operation_df(spark)

    def controls(df):
        return {
            (r.NUM_ID_OPERACAO, r.NUM_CONTROLE_LANCAMENTO_P1,
             r.NUM_CONTROLE_LANCAMENTO_P2)
            for r in eng._generate_meu_numeros(df, "321", date(2026, 7, 19)).collect()
        }

    assert controls(source.repartition(1)) == controls(source.repartition(4))


def test_meu_capacity_aborts_above_9_999_999(spark, monkeypatch):
    eng._validate_meu_capacity(9_999_999)
    with pytest.raises(ValueError, match="capacidade"):
        eng._validate_meu_capacity(10_000_000)
    monkeypatch.setattr(eng, "MAX_MEU_NUMERO_ORDINAL", 2)
    with pytest.raises(ValueError, match="capacidade"):
        eng._generate_meu_numeros(_operation_df(spark), "321", date(2026, 7, 19))


def test_target_preflight_catches_collision_and_accepts_empty(spark):
    generated = eng._generate_meu_numeros(_operation_df(spark), "321", date(2026, 7, 19))
    first = eng._flatten_meu_tuples(generated).first()
    existing = spark.createDataFrame([tuple(first)], eng._flatten_meu_tuples(generated).schema)
    with pytest.raises(ValueError, match="colisão"):
        eng._assert_no_meu_collisions(generated, existing)
    eng._assert_no_meu_collisions(generated, spark.createDataFrame([], existing.schema))


def test_target_preflight_normalizes_numeric_account_representations(spark):
    generated = eng._generate_meu_numeros(_operation_df(spark), "321", date(2026, 7, 19))
    first = eng._flatten_meu_tuples(generated).first()
    existing = spark.createDataFrame([
        (str(first.DAT_OPERACAO), Decimal(first.NUM_CONTA_PARTICIPANTE + ".000"),
         first.NUM_CONTROLE_LANCAMENTO, Decimal(first.NUM_ID_TIPO_OPER_OBJETO_SERV + ".000")),
    ], "DAT_OPERACAO string, NUM_CONTA_PARTICIPANTE decimal(38,9), "
       "NUM_CONTROLE_LANCAMENTO string, NUM_ID_TIPO_OPER_OBJETO_SERV decimal(38,9)")
    with pytest.raises(ValueError, match="colisão"):
        eng._assert_no_meu_collisions(generated, existing)


def test_target_preflight_distinguishes_tos_and_exact_timestamp(spark):
    generated = eng._generate_meu_numeros(_operation_df(spark), "321", date(2026, 7, 19))
    first = eng._flatten_meu_tuples(generated).first()
    schema = ("DAT_OPERACAO string, NUM_CONTA_PARTICIPANTE string, "
              "NUM_CONTROLE_LANCAMENTO string, NUM_ID_TIPO_OPER_OBJETO_SERV string")
    different_tos = spark.createDataFrame([(
        str(first.DAT_OPERACAO), first.NUM_CONTA_PARTICIPANTE,
        first.NUM_CONTROLE_LANCAMENTO, "4510",
    )], schema)
    different_time = spark.createDataFrame([(
        "2026-07-19 12:00:00", first.NUM_CONTA_PARTICIPANTE,
        first.NUM_CONTROLE_LANCAMENTO, first.NUM_ID_TIPO_OPER_OBJETO_SERV,
    )], schema)
    eng._assert_no_meu_collisions(generated, different_tos)
    eng._assert_no_meu_collisions(generated, different_time)
    exact = spark.createDataFrame([(
        str(first.DAT_OPERACAO), first.NUM_CONTA_PARTICIPANTE,
        first.NUM_CONTROLE_LANCAMENTO, first.NUM_ID_TIPO_OPER_OBJETO_SERV,
    )], schema)
    with pytest.raises(ValueError, match="colisão"):
        eng._assert_no_meu_collisions(generated, exact)


def test_business_validation_rejects_composite_collision(spark):
    instrumentos = spark.createDataFrame(
        [(1, "CDB10000001", None)], "NUM_IF long, COD_IF string, DAT_EXCLUSAO string"
    )
    op = _operation_df(spark).withColumn(
        "COD_OPERACAO", F.lpad(F.col("NUM_ID_OPERACAO"), 16, "0"))
    generated = eng._generate_meu_numeros(op, "321", date(2026, 7, 19))
    eng._validate_business_keys(instrumentos, generated)
    duplicate = generated.withColumn(
        "NUM_CONTA_PARTICIPANTE_P2",
        F.col("NUM_CONTA_PARTICIPANTE_P1"),
    ).withColumn(
        "NUM_CONTROLE_LANCAMENTO_P2",
        F.col("NUM_CONTROLE_LANCAMENTO_P1"),
    )
    with pytest.raises(ValueError, match="colisão"):
        eng._validate_business_keys(instrumentos, duplicate)


@pytest.mark.parametrize("failure", ["mismatch", "missing", "duplicate", "unmatched"])
def test_business_validation_rejects_invalid_operation_root_cod_if_mapping(spark, failure):
    instrumentos = spark.createDataFrame(
        [(1, "CDB10000001", None)], "NUM_IF long, COD_IF string, DAT_EXCLUSAO string"
    )
    op = (_operation_df(spark)
          .withColumn("COD_OPERACAO", F.lpad(F.col("NUM_ID_OPERACAO"), 16, "0"))
          .withColumn("COD_IF", F.lit("CDB10000001")))
    if failure == "mismatch":
        op = op.withColumn("COD_IF", F.lit("CDB20000002"))
    elif failure == "missing":
        instrumentos = instrumentos.drop("DAT_EXCLUSAO")
    elif failure == "duplicate":
        instrumentos = instrumentos.unionByName(instrumentos)
    else:
        op = op.withColumn("NUM_IF", F.lit(2))
    generated = eng._generate_meu_numeros(op, "321", date(2026, 7, 19))

    with pytest.raises(ValueError, match="Validação final"):
        eng._validate_business_keys(instrumentos, generated)


def test_strict_domain_excludes_any_invalid_operation_and_optional_account_refs(spark):
    candidates = spark.createDataFrame([(str(i),) for i in range(1, 14)], "NUM_IF string")
    operations = spark.createDataFrame([
        ("1", "100", "10", "11"),
        ("2", "100", "10", "11"), ("2", "101", "10", "11"),
        ("3", "102", "10", "11"),
        ("4", "103", "10", "11"),
        ("5", "100", "12", "11"),
        ("6", "100", "13", "11"),
        ("7", None, "10", "11"),
        ("8", "100", None, "11"),
        ("9", "100", "10", "11"),
        ("10", "100", "10", "11"),
        ("11", "100", "10", "11"),
        ("12", "100", "10", "999"),
        ("13", "", "", "11"),
    ], "NUM_IF string, NUM_ID_TIPO_OPER_OBJETO_SERV string, "
       "NUM_CONTA_PARTICIPANTE_P1 string, NUM_CONTA_PARTICIPANTE_P2 string")
    tos = spark.createDataFrame([
        ("100", "200", "44", " S "),
        ("101", "201", "44", "S"),
        ("102", "200", "45", "S"),
        ("103", "200", "44", "N"),
    ], "NUM_ID_TIPO_OPER_OBJETO_SERV string, NUM_ID_TIPO_OPERACAO string, "
       "NUM_ID_OBJETO_SERVICO string, IND_DISPONIVEL_IDENTIFICACAO string")
    top = spark.createDataFrame([("200", "1"), ("201", "2")],
                                "NUM_ID_TIPO_OPERACAO string, COD_TIPO_OPERACAO string")
    accounts = spark.createDataFrame([
        ("10", "1", "12345.40-1"),
        ("11", "1", "54321.10-9"),
        ("12", "2", "12345.40-2"),
        ("13", "1", "MALFORMED"),
    ], "NUM_CONTA_PARTICIPANTE string, NUM_ID_SITUACAO_CONTA string, "
       "COD_CONTA_PARTICIPANTE string")
    titulo = spark.createDataFrame([("9", "999"), ("11", None)],
                                   "NUM_IF string, NUM_CONTA_PARTICIPANTE string")
    deposito = spark.createDataFrame([("10", "12"), ("11", None)],
                                     "NUM_IF string, NUM_CONTA_PARTICIPANTE string")
    result = eng._strict_lookup_eligible_domain(
        candidates, operations, tos, top, accounts, titulo, deposito)
    assert {r.NUM_IF for r in result.collect()} == {"1", "11"}

    numeric_columns = {
        "NUM_ID_TIPO_OPER_OBJETO_SERV", "NUM_CONTA_PARTICIPANTE_P1",
        "NUM_CONTA_PARTICIPANTE_P2", "NUM_ID_TIPO_OPERACAO",
        "NUM_ID_OBJETO_SERVICO", "COD_TIPO_OPERACAO",
        "NUM_CONTA_PARTICIPANTE", "NUM_ID_SITUACAO_CONTA",
    }

    def decimalize(df):
        for column in numeric_columns & set(df.columns):
            df = df.withColumn(column, F.col(column).cast("decimal(38,9)"))
        return df

    decimal_result = eng._strict_lookup_eligible_domain(
        candidates, decimalize(operations), decimalize(tos), decimalize(top),
        decimalize(accounts), decimalize(titulo), decimalize(deposito))
    assert {r.NUM_IF for r in decimal_result.collect()} == {"1", "11"}


def test_filter_reference_contains_strict_instrument_level_policy():
    text = (Path(__file__).with_name("filtro.txt").read_text(encoding="utf-8").upper())
    assert "NOT EXISTS" in text
    assert "NUM_CONTA_PARTICIPANTE_P2" in text
    assert "CETIP.TITULO" in text and "CETIP.DEPOSITO_AUTOMATICO_IF" in text
    assert "NUM_ID_SITUACAO_CONTA <> 1" in text
    assert "NUM_ID_SITUACAO_CONTA IN (1, 2)" not in text
    assert "NUM_ID_OBJETO_SERVICO <> 44" in text
    assert "COD_TIPO_OPERACAO <> 1" in text
    assert "IND_DISPONIVEL_IDENTIFICACAO" in text


class FakePublicationFs:
    def __init__(self, entries, failed_renames=()):
        self.entries = dict(entries)
        self.failed_renames = set(failed_renames)
        self.renames = []
        self.deletes = []

    def exists(self, path):
        return str(path) in self.entries

    def rename(self, source, target):
        pair = (str(source), str(target))
        self.renames.append(pair)
        if pair in self.failed_renames or pair[0] not in self.entries:
            return False
        self.entries[pair[1]] = self.entries.pop(pair[0])
        return True

    def delete(self, path, recursive):
        path = str(path)
        self.deletes.append((path, recursive))
        if path not in self.entries:
            return False
        del self.entries[path]
        return True


def test_publication_success_verifies_final_then_removes_backup():
    fs = FakePublicationFs({"staging": "new", "final": "old"})
    eng._promote_staging_paths(fs, "staging", "final", "final.__previous_run")
    assert fs.entries == {"final": "new"}
    assert fs.renames == [
        ("final", "final.__previous_run"),
        ("staging", "final"),
    ]
    assert fs.deletes == [("final.__previous_run", True)]


def test_promotion_failure_restores_and_verifies_old_final():
    fs = FakePublicationFs(
        {"staging": "new", "final": "old"},
        failed_renames={("staging", "final")},
    )
    with pytest.raises(ValueError, match="restaurado e verificado"):
        eng._promote_staging_paths(fs, "staging", "final", "final.__previous_run")
    assert fs.entries["final"] == "old"
    assert "final.__previous_run" not in fs.entries
    assert fs.renames[-1] == ("final.__previous_run", "final")


def test_promotion_and_restore_failure_retains_and_reports_backup():
    backup = "final.__previous_manual_recovery"
    fs = FakePublicationFs(
        {"staging": "new", "final": "old"},
        failed_renames={("staging", "final"), (backup, "final")},
    )
    with pytest.raises(RuntimeError, match=rf"CRÍTICO.*{backup}"):
        eng._promote_staging_paths(fs, "staging", "final", backup)
    assert fs.entries[backup] == "old"
    assert "final" not in fs.entries


@pytest.mark.parametrize("failure_phase", ["allocator", "preflight"])
def test_prepublication_failure_preserves_existing_final_output(
        spark, tmp_path, monkeypatch, failure_phase):
    final_path = tmp_path / "final"
    final_path.mkdir()
    marker = final_path / "previous.marker"
    marker.write_text("previous-output", encoding="utf-8")

    if failure_phase == "allocator":
        def failing_batches(*args, **kwargs):
            yield [(1, "0000000000000001")]
            raise RuntimeError("allocator failed")

        monkeypatch.setattr(
            eng, "_iter_oracle_code_batches",
            failing_batches,
        )
        slots = spark.createDataFrame([(1, 10, "OLD"), (2, 11, "OLD")],
                                      "ORDINAL long, PK long, OLD string")

        def prepare(staging):
            eng._materialize_code_map(
                spark, slots, code_kind="COD_OPERACAO", generated_alias="CODE",
                out_path=f"{staging}/map", dry_run=False,
                credentials=("url", "user", "password"), batch_size=1,
                engorda_date=date(2026, 7, 19))
    else:
        monkeypatch.setattr(
            eng, "_read_existing_meu_tuples",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("preflight failed")),
        )

        def prepare(staging):
            eng._read_existing_meu_tuples(
                spark, ("url", "user", "password"), engorda_date=date(2026, 7, 19),
                prefix="321", temp_path=f"{staging}/preflight")

    with pytest.raises(RuntimeError, match=failure_phase):
        eng._stage_and_publish(spark, str(final_path), prepare)
    assert marker.read_text(encoding="utf-8") == "previous-output"


def test_zero_row_table_and_mapping_are_schema_correct_and_readable(spark, tmp_path):
    schema = T.StructType([
        T.StructField("ID", T.LongType(), False),
        T.StructField("VALUE", T.StringType(), True),
    ])
    lote = spark.createDataFrame([], schema)
    plano = eng.PlanoTabela(
        name="EMPTY", pk_cols=("ID",), pk_regra="OFFSET_PROPRIO", pk_start=10,
    )
    clones, mapping = eng.clona_tabela(spark, plano, lote, 2, {})
    assert clones.count() == 0 and mapping.count() == 0
    path = str(tmp_path / "EMPTY")
    eng.escreve_tabela(spark, clones, path)
    readback = spark.read.parquet(path)
    assert readback.count() == 0
    assert [(field.name, field.dataType) for field in readback.schema] == [
        (field.name, field.dataType) for field in clones.schema]
