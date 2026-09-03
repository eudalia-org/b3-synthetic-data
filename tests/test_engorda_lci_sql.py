import json
import re
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datagen import engorda_tables as eng

ROOT = Path(__file__).resolve().parents[1]
QUERY_CATALOG = ROOT / "datagen" / "queries_produtos.sql"
SPEC_PATH = ROOT / "datagen" / "spec_config_mais_produtos.json"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("engorda-lci-sql-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _query_block(product: str) -> str:
    catalog = QUERY_CATALOG.read_text(encoding="utf-8")
    match = re.search(
        rf"-- BEGIN QUERY: {re.escape(product)}\s+(.*?)"
        rf"-- END QUERY: {re.escape(product)}",
        catalog,
        re.DOTALL,
    )
    assert match is not None
    return match.group(1)


@pytest.mark.parametrize(
    ("product", "allowed_route", "invalid_cte"),
    [
        ("cdb_resgate", "4509", "OPERACAO_FORA_ROTA"),
        ("cdb_escalonamento", "4509", "OPERACAO_FORA_ROTA"),
        ("ccb_pppre", "871", "OPERACAO_INVALIDA"),
        ("ccb_pfpre", "871", "OPERACAO_FORA_ROTA"),
        ("ccb_favcp", "871", "OPERACAO_FORA_ROTA"),
    ],
)
def test_flagged_queries_reject_any_root_with_an_operation_outside_route(
    product, allowed_route, invalid_cte
):
    query = _query_block(product)

    assert f"OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> {allowed_route}" in query
    assert f"LEFT ANTI JOIN {invalid_cte}" in query
    assert "O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL" in query
    assert "INNER JOIN OPER_REGISTRO" in query


def test_only_flagged_ccb_queries_get_all_operation_route_filter():
    assert "OPERACAO_FORA_ROTA" not in _query_block("ccb_pgrpre")
    assert "OPERACAO_FORA_ROTA" not in _query_block("ccb_fapre")


def test_pppre_rejects_any_nonapproved_operation_status():
    query = _query_block("ccb_pppre")

    assert "O.COD_SITUACAO_OPERACAO IS NULL" in query
    assert "OR O.COD_SITUACAO_OPERACAO <> 43" in query


def test_cdb_escalonamento_requires_numeric_nonnull_zero_redeemed_quantity():
    assert (
        "TRY_CAST(TIT.QTD_RESGATADA AS DECIMAL(38, 18)) = 0"
        in _query_block("cdb_escalonamento")
    )


def test_gravame_rejects_any_operation_outside_approved_route_set():
    query = _query_block("gravame")

    assert "O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL" in query
    assert "OR O.NUM_ID_TIPO_OPER_OBJETO_SERV NOT IN (15394, 15512)" in query
    assert "LEFT ANTI JOIN OPERACAO_FORA_ROTA" in query
    assert "INNER JOIN OPER_REGISTRO" in query


def test_lci_query_requires_active_scr_master_with_id_linked_history():
    query = _query_block("lci")

    assert "INNER JOIN {{RAW_HISTORICO_CREDITO_SCR}} H" in query
    assert "ON H.NUM_ID_CREDITO_SCR = S.NUM_ID_CREDITO_SCR" in query
    assert "WHERE S.DAT_EXCLUSAO IS NULL" in query


def test_scr_history_fk_is_explicit_in_product_spec():
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    expected = {
        "columns": ["NUM_ID_CREDITO_SCR"],
        "parent_table": "CREDITO_SCR",
        "parent_columns": ["NUM_ID_CREDITO_SCR"],
    }

    assert spec["HISTORICO_CREDITO_SCR"]["foreign_keys"].count(expected) == 1


def test_live_selection_forwards_product_and_lateral_cap(spark, monkeypatch):
    domain = spark.createDataFrame([(1,)], "NUM_IF long")
    captured = {}
    monkeypatch.setattr(
        eng,
        "_dominio_instrumentos_elegiveis",
        lambda *_args, **_kwargs: (domain, domain),
    )

    def closure(
        _spark,
        _config,
        _spec,
        _plans,
        _order,
        candidates,
        _max_passadas,
        **kwargs,
    ):
        captured.update(kwargs)
        root = spark.createDataFrame([(value,) for value in candidates], domain.schema)
        provenance = root.select(
            eng.COL_NUM_IF,
            F.col(eng.COL_NUM_IF).alias(eng.ROOT_PROVENANCE_COL),
        )
        return {eng.TABELA_RAIZ: root}, {eng.TABELA_RAIZ: provenance}

    monkeypatch.setattr(eng, "_calcula_lotes_com_proveniencia", closure)
    monkeypatch.setattr(
        eng,
        "_target_fk_rejections",
        lambda *_args, **_kwargs: (set(), {}, None),
    )
    selection = eng.seleciona_instrumentos_destino(
        spark,
        {},
        {eng.TABELA_RAIZ: {"pk_cols": [eng.COL_NUM_IF], "foreign_keys": []}},
        num_ifs=[1],
        n_instrumentos=None,
        seed=42,
        profile=eng.get_product_profile("lci"),
        planos={
            eng.TABELA_RAIZ: eng.PlanoTabela(
                eng.TABELA_RAIZ, (eng.COL_NUM_IF,)
            )
        },
        ordem=[eng.TABELA_RAIZ],
        max_passadas=6,
        existing_key_lookup=lambda *_args: set(),
        produto="lci",
        lastros_por_lote=3,
    )

    assert selection.values == [1]
    assert captured["produto"] == "lci"
    assert captured["lastros_por_lote"] == 3
    for frame in selection.lotes.values():
        frame.unpersist(blocking=False)


def test_lci_lateral_cap_ignores_incomplete_master_and_keeps_all_histories(
    spark, monkeypatch
):
    sources = {
        eng.TABELA_RAIZ: spark.createDataFrame(
            [(1, 100)], "NUM_IF long, NUM_ID_LOTE long"
        ),
        "CREDITO_SCR": spark.createDataFrame(
            [
                (10, 100, "INCOMPLETE", None),
                (11, 100, "COMPLETE", None),
                (12, 100, "DELETED", "2026-01-01"),
            ],
            "NUM_ID_CREDITO_SCR long, NUM_ID_LOTE long, "
            "COD_CREDITO_SCR string, DAT_EXCLUSAO string",
        ),
        "HISTORICO_CREDITO_SCR": spark.createDataFrame(
            [(101, 11), (102, 11), (103, 12), (104, 999)],
            "NUM_ID_HISTORICO_CREDITO_SCR long, NUM_ID_CREDITO_SCR long",
        ),
    }
    monkeypatch.setattr(
        eng, "_read_source", lambda _spark, _config, table: sources[table]
    )
    plans = {
        eng.TABELA_RAIZ: eng.PlanoTabela(
            eng.TABELA_RAIZ, (eng.COL_NUM_IF,)
        ),
        "CREDITO_SCR": eng.PlanoTabela(
            "CREDITO_SCR", ("NUM_ID_CREDITO_SCR",)
        ),
        "HISTORICO_CREDITO_SCR": eng.PlanoTabela(
            "HISTORICO_CREDITO_SCR",
            ("NUM_ID_HISTORICO_CREDITO_SCR",),
            [
                eng.FkRemap(
                    ("NUM_ID_CREDITO_SCR",),
                    "CREDITO_SCR",
                    ("NUM_ID_CREDITO_SCR",),
                    True,
                )
            ],
        ),
    }

    lots, provenances = eng._calcula_lotes_com_proveniencia(
        spark,
        {},
        {},
        plans,
        list(plans),
        [1],
        max_passadas=2,
        produto="lci",
        lastros_por_lote=1,
    )

    assert [row.NUM_ID_CREDITO_SCR for row in lots["CREDITO_SCR"].collect()] == [11]
    assert {
        row.NUM_ID_HISTORICO_CREDITO_SCR
        for row in lots["HISTORICO_CREDITO_SCR"].collect()
    } == {101, 102}
    assert {
        row.NUM_ID_HISTORICO_CREDITO_SCR
        for row in provenances["HISTORICO_CREDITO_SCR"].collect()
    } == {101, 102}
    for frame in (*lots.values(), *provenances.values()):
        frame.unpersist(blocking=False)


def test_lateral_plan_without_product_fails_before_source_reads(spark, monkeypatch):
    monkeypatch.setattr(
        eng,
        "_read_source",
        lambda *_args: pytest.fail("source read before lateral product validation"),
    )

    with pytest.raises(ValueError, match="Tabelas laterais planejadas sem produto"):
        eng._calcula_lotes_com_proveniencia(
            spark,
            {},
            {},
            {
                eng.TABELA_RAIZ: eng.PlanoTabela(
                    eng.TABELA_RAIZ, (eng.COL_NUM_IF,)
                ),
                "CREDITO_SCR": eng.PlanoTabela(
                    "CREDITO_SCR", ("NUM_ID_CREDITO_SCR",)
                ),
            },
            [eng.TABELA_RAIZ, "CREDITO_SCR"],
            [1],
            max_passadas=2,
        )


def test_lca_uses_code_linked_history_without_requiring_an_id_fk(spark, monkeypatch):
    sources = {
        eng.TABELA_RAIZ: spark.createDataFrame(
            [(1, 200)], "NUM_IF long, NUM_ID_LOTE long"
        ),
        "CREDITO_DC": spark.createDataFrame(
            [(30, 200, "DC-30", None)],
            "NUM_ID_CREDITO_DC long, NUM_ID_LOTE long, COD_CREDITO_DC string, "
            "DAT_EXCLUSAO string",
        ),
        "HISTORICO_CREDITO_DC": spark.createDataFrame(
            [(301, 200, "DC-30")],
            "NUM_ID_HISTORICO_CREDITO_DC long, NUM_ID_LOTE long, "
            "COD_CREDITO_DC string",
        ),
    }
    monkeypatch.setattr(
        eng, "_read_source", lambda _spark, _config, table: sources[table]
    )
    plans = {
        eng.TABELA_RAIZ: eng.PlanoTabela(eng.TABELA_RAIZ, (eng.COL_NUM_IF,)),
        "CREDITO_DC": eng.PlanoTabela("CREDITO_DC", ("NUM_ID_CREDITO_DC",)),
        "HISTORICO_CREDITO_DC": eng.PlanoTabela(
            "HISTORICO_CREDITO_DC", ("NUM_ID_HISTORICO_CREDITO_DC",)
        ),
    }

    lots, provenances = eng._calcula_lotes_com_proveniencia(
        spark, {}, {}, plans, list(plans), [1], max_passadas=2,
        produto="lca", lastros_por_lote=1,
    )

    assert [row.NUM_ID_CREDITO_DC for row in lots["CREDITO_DC"].collect()] == [30]
    assert [
        row.NUM_ID_HISTORICO_CREDITO_DC
        for row in lots["HISTORICO_CREDITO_DC"].collect()
    ] == [301]
    for frame in (*lots.values(), *provenances.values()):
        frame.unpersist(blocking=False)


def test_final_writer_coalesces_small_shuffled_operacao_output(spark, tmp_path):
    operations = spark.range(17).repartition(17)
    output = tmp_path / "OPERACAO"

    eng.escreve_tabela(spark, operations, str(output), expected_rows=17)

    assert spark.read.parquet(str(output)).count() == 17
    assert len(list(output.glob("part-*.parquet"))) == 1
