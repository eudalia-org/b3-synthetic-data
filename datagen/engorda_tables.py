from __future__ import annotations

import argparse
import copy
import json
import logging
import math
import os
import re
import sys
import time
import warnings
import zlib
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import reduce
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    # OCI Data Flow expõe o log do driver em spark_application_stdout; o default
    # do logging é stderr (spark_application_stderr). Direciona para stdout para
    # que INFO/DEBUG (inclusive os relatórios de debug) apareçam onde se olha.
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Debug mode.
#
# Ativado por --debug (CLI) ou pela env var DATAGEN_DEBUG in {"1","true","yes",
# "on"}. Quando ligado:
#   * o nível de log global sobe para DEBUG;
#   * `engorda` roda um relatório de integridade de FK (nulos + órfãos por
#     coluna) ENTRE cada estágio do pipeline (leitura -> filtro de domínio ->
#     amostra referencial -> fecho ascendente -> neutralização -> síntese ->
#     bind -> null_orphan), para localizar EM QUE ESTÁGIO cada coluna começa a
#     apresentar nulos/órfãos — que é exatamente a informação que falta para
#     diagnosticar as falhas do pre-append check (ver DEBUG_WATCH_COLUMNS).
#
# O relatório é a única coisa que dispara ações Spark extras; para não pesar no
# run completo, ele só roda quando o debug está ligado.
# ---------------------------------------------------------------------------
DEBUG_ENABLED = os.environ.get("DATAGEN_DEBUG", "").strip().lower() in (
    "1", "true", "yes", "on"
)

# Colunas sob suspeita no pre-append check. O relatório de debug SEMPRE inclui
# todas as colunas de FK do componente, mas destaca (marca com "<<<") estas
# para facilitar a leitura do log. Ajuste conforme novos achados.
DEBUG_WATCH_COLUMNS = {
    ("OPERACAO", "NUM_CONTA_PARTICIPANTE_P2"),
    ("OPERACAO", "NUM_CONTA_PARTICIPANTE_P1"),
    ("LANCAMENTO", "NUM_ID_ENTIDADE"),
    ("ESPECIFICACAO_COMITENTE", "NUM_ID_ENTIDADE"),
    ("CARTEIRA_COMITENTE", "NUM_ID_ENTIDADE"),
    ("CONDICAO_IF", "NUM_IF"),
}


def _ensure_stdout_logging() -> None:
    """Garante que o log do driver saia em stdout (spark_application_stdout).

    logging.basicConfig é no-op se o root logger já tiver handler (a plataforma
    do Data Flow pode instalar um, tipicamente em stderr). Aqui apontamos os
    handlers existentes para stdout; se não houver nenhum, criamos um.
    """
    root = logging.getLogger()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
    if stream_handlers:
        for h in stream_handlers:
            h.setStream(sys.stdout)
            # O handler não pode filtrar acima de DEBUG, senão os relatórios
            # [DEBUG N] deste módulo (que propagam para os handlers do root)
            # seriam descartados na saída mesmo com o logger em DEBUG.
            if h.level > logging.DEBUG:
                h.setLevel(logging.DEBUG)
    else:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(fmt)
        h.setLevel(logging.DEBUG)
        root.addHandler(h)


# Loggers de terceiros que explodem em DEBUG e soterram os relatórios: py4j
# loga CADA chamada JVM ("Answer received", "Command to send"); Spark e o
# conector OCI (BMC) também são verbosos. Mantidos em WARNING mesmo no modo
# debug — só o log DESTE módulo sobe para DEBUG.
_NOISY_DEBUG_LOGGERS = (
    "py4j", "py4j.java_gateway", "py4j.clientserver",
    "pyspark", "org.apache.spark", "com.oracle.bmc", "oci",
)


def _set_debug(enabled: bool) -> None:
    """Liga/desliga o modo debug em tempo de execução (usado pela flag CLI).

    Sobe SÓ o logger deste módulo para DEBUG. O root fica em INFO e os loggers
    ruidosos (py4j/Spark/OCI) são fixados em WARNING — senão o DEBUG global
    despeja o protocolo py4j inteiro no stdout e esconde os relatórios de FK.
    """
    global DEBUG_ENABLED
    DEBUG_ENABLED = enabled
    if enabled:
        _ensure_stdout_logging()
        # NÃO sobe o root para DEBUG (evita o dilúvio do py4j). Só este módulo.
        logging.getLogger().setLevel(logging.INFO)
        logger.setLevel(logging.DEBUG)
        for noisy in _NOISY_DEBUG_LOGGERS:
            logging.getLogger(noisy).setLevel(logging.WARNING)
        logger.debug("Debug mode ATIVADO: relatórios de integridade por estágio ligados.")


def _dbg() -> bool:
    return DEBUG_ENABLED


# Se True, o run é uma AMOSTRA (--limit): as chaves de pai são pequenas, então
# o relatório de debug computa órfãos e usa broadcast nos joins com segurança.
# Se False (run COMPLETO em tabelas de 50M–1B linhas), o relatório PULA a
# contagem de órfãos — um dropna().distinct().left_anti contra um pai gigante é
# caro/arriscado (OOM de executor) — e reporta só nulos, que são baratos.
DEBUG_SAMPLED = False


def _set_debug_sampled(sampled: bool) -> None:
    global DEBUG_SAMPLED
    DEBUG_SAMPLED = sampled


REQUIRED_ENV_VARS = (
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
    "DATAGEN_SPECS_URI",
)
DEFAULT_SCALE_FACTOR = 2.0
DEFAULT_SEED = 42

# ---------------------------------------------------------------------------
# Filtro de domínio: CDB simplificado.
#
# O produto CDB simplificado é definido por um conjunto de PREDICADOS DE FONTE
# por tabela (FILTROS_FONTE). Esses predicados são a PRIMEIRA etapa do
# pipeline: são aplicados na LEITURA de cada Parquet, ANTES de qualquer
# amostragem, propagação referencial ou síntese. A partir daí não existe mais
# "Parquet completo sem filtro" para essas tabelas — toda leitura de fonte
# passa por `_read_source`, então nenhuma etapa posterior (nem o fecho de pais
# em `completa_pais_referenciados`) consegue re-injetar uma linha que o filtro
# removeu.
#
# Regras do CDB simplificado (imagem do produto):
#   INSTRUMENTO_FINANCEIRO : NUM_TIPO_IF = 49 E DAT_EXCLUSAO IS NULL
#   RESGATE                : DAT_EXCLUSAO IS NULL
#   CONDICAO_IF            : DAT_EXCLUSAO IS NULL
#
# Propagação por chave (inalterada): a raiz do universo continua sendo
# INSTRUMENTO_FINANCEIRO (TABELAS_RAIZ_FILTRO) e o pertencimento desce a árvore
# de FK por semi-join em `referential_sample`, restrito a `_dominio_spine`. Os
# predicados de fonte acima são uma poda ADICIONAL em cima disso: cada tabela
# nasce já com suas próprias linhas fora do produto descartadas, e a descida
# por chave apenas restringe mais. Como a fonte já vem filtrada em TODO ponto
# de leitura, filha e pai não podem divergir por re-injeção — se um pai é
# removido pelo seu filtro, ele simplesmente não existe para ninguém, e a
# filha órfã cai na neutralização normal (null_orphan_fks anula FK nullable;
# drop_orphan_rows dropa quando a FK é NOT NULL).
#
# FK de domínio vs FK de integridade: durante a descida em `referential_sample`
# a poda por semi-join NÃO pode usar TODAS as FKs de uma tabela indistintamente.
# Uma tabela orientada a instrumento financeiro (TITULO, CARTEIRA_COMITENTE,
# CARTEIRA_PARTICIPANTE, CREDITO, DEPOSITO_AUTOMATICO_IF, CONDICAO_IF etc. —
# e a própria INSTRUMENTO_FINANCEIRO) também carrega FKs LATERAIS para tabelas
# de referência compartilhadas (CONTA_PARTICIPANTE, PARTICIPANTE, tabelas de
# lookup...) que nada têm a ver com o domínio e cuja amostra, sendo
# independente, não intersecta necessariamente as linhas válidas do domínio.
# Usar essas FKs laterais como critério de sobrevivência na MESMA passada
# zera artificialmente linhas que são perfeitamente válidas no domínio — e,
# como a poda acontece ANTES do fecho ascendente, a linha descartada não pode
# mais ser recuperada por `completa_pais_referenciados` (que só completa PAIS
# de filhas que sobreviveram). `_dominio_spine` resolve isso calculando, só a
# partir do grafo de FKs, o conjunto de tabelas cuja amostra é garantidamente
# consistente com o domínio (a raiz + tudo alcançável dela por uma cadeia de
# FKs); a poda em `referential_sample` só usa FKs cujo parent_table esteja
# nesse conjunto. As FKs laterais (para fora do conjunto) NÃO podam nada na
# descida — ficam para `completa_pais_referenciados` (fecho de integridade
# para cima) e `neutraliza_orfaos_na_fonte` (só então descarta/anula órfão
# real de produção) resolverem depois.
#
# Complementarmente, `completa_pais_referenciados` fecha o universo PARA
# CIMA: toda chave de FK presente numa filha mantida passa a existir no pai
# amostrado, puxando as linhas de pai referenciadas ausentes. Essa leitura
# TAMBÉM passa por `_read_source` (fonte filtrada): um pai que não pertence ao
# produto NÃO volta — a filha que o referenciava vira órfã e é neutralizada.
#
# IMPORTANTE: a leitura de max(pk) em compute_pk_maxes NÃO usa estes filtros
# de propósito — ela precisa do max real da tabela inteira para que as PKs
# sintéticas não colidam com linhas de produção de OUTROS registros (outros
# NUM_TIPO_IF, linhas excluídas etc.). Por isso ela lê via `read_parquet`
# direto, não via `_read_source`.
# ---------------------------------------------------------------------------
FILTRO_TIPO_IF_COLUMN = "NUM_TIPO_IF"
FILTRO_TIPO_IF_VALUE = 49 #filtro cdb simplificado

# Predicados de fonte por tabela para o CDB simplificado. Cada predicado é uma
# tupla (coluna, op, valor):
#   ("==", v)      -> col == v            (igualdade exata)
#   (">", v)       -> col > v             (maior que; linha com col NULL sai)
#   ("ieq", v)     -> upper(trim(col)) == v  (igualdade string case/space-insensitive)
#   ("isnull", _)  -> col IS NULL
# Aplicados em AND. Um predicado cuja coluna não exista no schema da tabela é
# ignorado (defensivo contra variação de schema); ver `_aplica_filtros_fonte`.
FILTROS_FONTE: dict[str, list[tuple[str, str, object]]] = {
    "INSTRUMENTO_FINANCEIRO": [
        (FILTRO_TIPO_IF_COLUMN, "==", FILTRO_TIPO_IF_VALUE),
        ("DAT_EXCLUSAO", "isnull", None),
    ],
    "RESGATE": [
        # 'SEM TABELA' com upper+trim por segurança contra caixa/espaços.
        ("DAT_EXCLUSAO", "isnull", None),
    ],
    "CONDICAO_IF": [
        ("DAT_EXCLUSAO", "isnull", None),
    ],
    "CARTEIRA_COMITENTE": [
        ("QTD_CARTEIRA_COMITENTE", ">", 0),
    ],
    "CARTEIRA_PARTICIPANTE": [
        ("QTD_CARTEIRA_PARTICIPANTE", ">", 0),
    ],
}
# Únicas tabelas filtradas pela coluna NUM_TIPO_IF; a raiz do universo. O
# restante do domínio é derivado por chave (semi-joins descendo + fecho
# subindo) e, adicionalmente, podado pelos predicados de FILTROS_FONTE.
TABELAS_RAIZ_FILTRO = frozenset({"INSTRUMENTO_FINANCEIRO"})

# ---------------------------------------------------------------------------
# Polimorfismo de CONDICAO_IF (joined-subclass do Hibernate SEM discriminador).
#
# CONDICAO_IF é uma superclasse cujo tipo concreto (juros fixo, flutuante,
# resgate, ...) é resolvido pelo Hibernate SOMENTE por QUAL tabela-subtipo
# física contém a linha daquele NUM_CONDICAO_IF — não há coluna discriminadora.
# A coluna COD_TIPO_CONDICAO_IF do PAI é o que a aplicação lê para decidir o
# tipo e fazer o cast (ex.: tipo=2 -> (JurosFixoDO) ...). A invariante que a
# aplicação assume é, para cada NUM_CONDICAO_IF:
#   (a) existe EXATAMENTE UMA linha-subtipo;
#   (b) na tabela indicada por COD_TIPO_CONDICAO_IF;
#   (c) e em nenhuma outra tabela-subtipo.
# Violá-la é o que produz o ClassCastException
# "JurosFlutuanteDO cannot be cast to JurosFixoDO" no batch da NoMe.
#
# NUM_CONDICAO_IF é, em cada subtipo, PK **e** FK-para-o-pai (shared-key 1:1);
# por isso o alinhamento é feito em bind_shared_key_children, que agora usa
# este mapa para vincular cada subtipo APENAS às chaves do seu próprio tipo.
# Mapa COD_TIPO_CONDICAO_IF -> nome da tabela-subtipo (de TipoCondicaoIFDO +
# CondicaoIFDO.hbm.xml; espelha SUBTYPE_BY_TIPO em valida_regras_aplicacao.py).
CONDICAO_IF_TABLE = "CONDICAO_IF"
CONDICAO_IF_PK = "NUM_CONDICAO_IF"
CONDICAO_IF_TIPO_COL = "COD_TIPO_CONDICAO_IF"
SUBTYPE_BY_TIPO: dict[str, str] = {
    "1": "AMORTIZACAO",
    "2": "JUROS_FIXO",
    "3": "JUROS_FLUTUANTE",
    "4": "ATUALIZACAO_POS",
    "5": "SPREAD",
    "6": "PARTICIPACAO_LUCROS",
    "7": "PREMIO",
    "14": "ATUALIZACAO_PRE",
    "15": "PREMIO_OPCAO",
    "16": "TERMO",
    "17": "PARAMETRO_LIMITE",
    "20": "RESGATE",
    "21": "PREMIO_CONTRATO",
    "22": "OPCAO",
    "23": "RESET",
    "24": "DESDOBRAMENTO",
}
# Tabelas-subtipo -> COD_TIPO_CONDICAO_IF esperado (inverso de SUBTYPE_BY_TIPO).
TIPO_BY_SUBTYPE: dict[str, str] = {v: k for k, v in SUBTYPE_BY_TIPO.items()}

# ---------------------------------------------------------------------------
# Regras de engorda por coluna.
#
# Data de engorda = instante em que este script começa a executar. A mesma data
# é reutilizada para todas as tabelas do run, evitando pequenas diferenças de
# timestamp entre componentes ou ações Spark.
#
# Regras aplicadas quando a coluna existir na tabela:
#   DAT_INCLUSAO              -> data/hora da engorda (timestamp)
#   DAT_ALTERACAO             -> mesma data/hora de DAT_INCLUSAO (timestamp)
#   DAT_INCLUSAO_REGISTRO     -> mesma data/hora de DAT_INCLUSAO (timestamp)
#   DAT_EMISSAO               -> data da engorda, sem timestamp
#   DAT_VENCIMENTO            -> data da engorda + prazo, sem timestamp
#
# NUM_ID_CERTIFICACAO_CETIP NÃO entra aqui: ela é PK e a geração de PK
# (compute_pk_maxes + _set_unique_pk_column) já a faz incremental acima do max
# real da tabela inteira, com a folga do --pk-safety-band. Tratá-la de novo
# desfazia a folga e relia o max sem necessidade.
#
# Para DAT_VENCIMENTO, se não for informado um prazo fixo por tabela, o código
# preserva o prazo original da linha bootstrapada: DAT_VENCIMENTO - DAT_EMISSAO.
# Se esse prazo não existir, for inválido ou <= 0, usa 365 dias por segurança.
#
# NB (correção): as colunas de emissão/vencimento no schema real são
# DAT_EMISSAO / DAT_VENCIMENTO (prefixo DAT_). Versões anteriores procuravam
# DT_EMISSAO / DT_VENCIMENTO, que não casavam com o dado -> a regra virava
# no-op silencioso (a função é tolerante a coluna ausente).
# ---------------------------------------------------------------------------
ENGORDA_COL_DAT_INCLUSAO = "DAT_INCLUSAO"
ENGORDA_COL_DAT_ALTERACAO = "DAT_ALTERACAO"
ENGORDA_COL_DAT_INCLUSAO_REGISTRO = "DAT_INCLUSAO_REGISTRO"
ENGORDA_COL_DAT_VENCIMENTO = "DAT_VENCIMENTO"
ENGORDA_COL_DAT_EMISSAO = "DAT_EMISSAO"
# Colunas que recebem o MESMO timestamp único da engorda. Declarativo: para
# tratar mais uma coluna de timestamp, basta adicioná-la aqui.
ENGORDA_COLS_TIMESTAMP = (
    ENGORDA_COL_DAT_INCLUSAO,
    ENGORDA_COL_DAT_ALTERACAO,
    ENGORDA_COL_DAT_INCLUSAO_REGISTRO,
)
DEFAULT_DT_VENCIMENTO_PRAZO_DIAS = 30
MIN_DT_VENCIMENTO_PRAZO_DIAS = 1

NullableFkPolicy = Literal["allow_any_null", "allow_all_null", "invalid_null"]


ValidateMode = Literal["none", "full"]


RelationshipPolicy = Literal["warn_and_skip", "raise"]


SaveErrorPolicy = Literal["warn_and_continue", "raise"]


@dataclass(frozen=True)
class ForeignKeySpec:
    columns: Tuple[str, ...]
    parent_table: str
    parent_columns: Tuple[str, ...]


PostProcessor = Callable[[DataFrame, Mapping[str, DataFrame]], DataFrame]


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk_cols: Tuple[str, ...]
    foreign_keys: Tuple[ForeignKeySpec, ...] = field(default_factory=tuple)
    static: bool = False
    postprocess: Optional[PostProcessor] = None


def _stable_seed(base_seed: int, *parts: object) -> int:
    txt = "|".join(str(p) for p in (base_seed,) + parts)
    return int(zlib.crc32(txt.encode("utf-8")) % 2_000_000_000)


def _is_integer_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType))


def _is_float_type(dt: T.DataType) -> bool:
    """FloatType/DoubleType. Comuns quando CSV é lido com inferSchema=True."""
    return isinstance(dt, (T.FloatType, T.DoubleType))


def _is_decimal_type(dt: T.DataType) -> bool:
    return isinstance(dt, T.DecimalType)


def _is_numeric_pk_type(dt: T.DataType) -> bool:
    return _is_integer_type(dt) or _is_float_type(dt) or _is_decimal_type(dt)


def _is_string_type(dt: T.DataType) -> bool:
    return isinstance(dt, T.StringType)


def _is_safe_pk_type(dt: T.DataType) -> bool:
    return _is_numeric_pk_type(dt) or _is_string_type(dt)


def _get_field_type(df: DataFrame, col_name: str) -> T.DataType:
    for f in df.schema.fields:
        if f.name == col_name:
            return f.dataType
    raise ValueError(f"Coluna `{col_name}` não existe no DataFrame.")


def _has_column(df: DataFrame, col_name: str) -> bool:
    return col_name in df.columns


def _normalize_engorda_ts(value: Optional[datetime]) -> datetime:
    """Retorna o timestamp único do run de engorda."""
    if value is None:
        return datetime.now().replace(microsecond=0)

    if isinstance(value, datetime):
        return value.replace(microsecond=0)

    raise TypeError("engorda_ts deve ser datetime ou None.")


def _engorda_date(value: datetime) -> date:
    return value.date()


def _timestamp_literal_for_type(value: datetime, dt: T.DataType):
    """Literal de timestamp respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(F.lit(value).cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
    return F.lit(value).cast(dt)


def _date_literal_for_type(value: date, dt: T.DataType):
    """Literal de data sem hora respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(F.lit(value).cast("date"), "yyyy-MM-dd")
    if isinstance(dt, T.TimestampType):
        # Sem timestamp/hora útil: grava a data à meia-noite se a coluna física
        # for TimestampType no metastore/origem.
        return F.lit(value).cast("timestamp").cast(dt)
    return F.lit(value).cast(dt)


def _date_expression_for_type(expr, dt: T.DataType):
    """Expressão de data sem hora respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(expr.cast("date"), "yyyy-MM-dd")
    if isinstance(dt, T.TimestampType):
        return expr.cast("date").cast("timestamp").cast(dt)
    return expr.cast(dt)


def _apply_engorda_business_rules(
    work: DataFrame,
    *,
    engorda_ts: datetime,
    dt_vencimento_prazo_dias: Optional[int] = None,
    default_dt_vencimento_prazo_dias: int = DEFAULT_DT_VENCIMENTO_PRAZO_DIAS,
) -> DataFrame:
    """
    Aplica as regras de DATA do engorda às colunas existentes na tabela.

    Regras (aplicadas só quando a coluna existir; tipo físico preservado):
      DAT_INCLUSAO          -> timestamp da engorda
      DAT_ALTERACAO         -> mesmo timestamp de DAT_INCLUSAO
      DAT_INCLUSAO_REGISTRO -> mesmo timestamp de DAT_INCLUSAO
      DAT_EMISSAO           -> data da engorda, sem hora
      DAT_VENCIMENTO        -> data da engorda + prazo, sem hora

    NUM_ID_CERTIFICACAO_CETIP NÃO é tratada aqui: é PK, e a geração de PK
    (compute_pk_maxes + _set_unique_pk_column) já a faz incremental acima do max
    real da tabela inteira, com a folga do --pk-safety-band. Reescrevê-la aqui
    desfazia a folga e relia o max desnecessariamente.

    A função é tolerante: se uma coluna não existir, não altera a tabela.
    """
    engorda_dt = _engorda_date(engorda_ts)

    # 1) Calcula prazo de vencimento ANTES de sobrescrever DAT_EMISSAO.
    tmp_prazo_col = "__engorda_prazo_dias"
    while tmp_prazo_col in work.columns:
        tmp_prazo_col = f"_{tmp_prazo_col}"

    has_vencimento = ENGORDA_COL_DAT_VENCIMENTO in work.columns
    has_emissao = ENGORDA_COL_DAT_EMISSAO in work.columns

    if has_vencimento:
        if dt_vencimento_prazo_dias is not None:
            prazo_expr = F.lit(int(dt_vencimento_prazo_dias)).cast("int")
        elif has_emissao:
            prazo_expr = F.datediff(
                F.to_date(F.col(ENGORDA_COL_DAT_VENCIMENTO)),
                F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)),
            ).cast("int")
        else:
            prazo_expr = F.lit(int(default_dt_vencimento_prazo_dias)).cast("int")

        prazo_expr = F.coalesce(
            prazo_expr,
            F.lit(int(default_dt_vencimento_prazo_dias)).cast("int"),
        )
        prazo_expr = F.when(
            prazo_expr < F.lit(MIN_DT_VENCIMENTO_PRAZO_DIAS),
            F.lit(int(default_dt_vencimento_prazo_dias)).cast("int"),
        ).otherwise(prazo_expr)

        work = work.withColumn(tmp_prazo_col, prazo_expr)

    # 2) DAT_INCLUSAO, DAT_ALTERACAO e DAT_INCLUSAO_REGISTRO usam exatamente o
    #    mesmo timestamp único da engorda (ENGORDA_COLS_TIMESTAMP).
    for col_name in ENGORDA_COLS_TIMESTAMP:
        if col_name in work.columns:
            work = work.withColumn(
                col_name,
                _timestamp_literal_for_type(
                    engorda_ts,
                    _get_field_type(work, col_name),
                ),
            )

    # 3) DAT_EMISSAO = data da engorda sem timestamp.
    if has_emissao:
        work = work.withColumn(
            ENGORDA_COL_DAT_EMISSAO,
            _date_literal_for_type(
                engorda_dt,
                _get_field_type(work, ENGORDA_COL_DAT_EMISSAO),
            ),
        )

    # 4) DAT_VENCIMENTO = data da engorda + prazo, sem timestamp.
    if has_vencimento:
        venc_expr = F.expr(
            f"date_add(DATE '{engorda_dt.isoformat()}', CAST({tmp_prazo_col} AS INT))"
        )
        work = work.withColumn(
            ENGORDA_COL_DAT_VENCIMENTO,
            _date_expression_for_type(
                venc_expr,
                _get_field_type(work, ENGORDA_COL_DAT_VENCIMENTO),
            ),
        ).drop(tmp_prazo_col)

    return work


def _persist(df: DataFrame, storage_level: StorageLevel) -> DataFrame:
    return df.persist(storage_level)


def _materialize(
    df: DataFrame, storage_level: StorageLevel, truncate_lineage: bool
) -> DataFrame:
    """Materialize a stage. When truncate_lineage is set, use an eager
    localCheckpoint so the LOGICAL plan is replaced by a shallow RDD leaf —
    a persist keeps the cached plan tree, which the analyzer still traverses on
    every dependent query, so deep FK-mapping chains compound until the driver
    OOMs analyzing them. Used for --limit, where the referential-sample chain
    makes the plans deep; the full run keeps persist (recomputable on
    executor loss, which localCheckpoint is not)."""
    if truncate_lineage:
        return df.localCheckpoint(eager=True)
    out = _persist(df, storage_level)
    out.count()
    return out


def _safe_unpersist(df: Optional[DataFrame]) -> None:
    if df is None:
        return
    try:
        df.unpersist()
    except Exception:
        pass


def _warn_or_raise(message: str, *, policy: RelationshipPolicy = "warn_and_skip") -> None:
    """
    Centraliza a política para relacionamento inválido.

    policy="raise": mantém comportamento estrito.
    policy="warn_and_skip": emite warning e deixa o processamento continuar.
    """
    if policy == "raise":
        raise ValueError(message)

    if policy == "warn_and_skip":
        warnings.warn(message, UserWarning, stacklevel=2)
        return

    raise ValueError(f"relationship_policy inválida: {policy!r}")


def _format_fk(child_table: str, fk: ForeignKeySpec) -> str:
    return (
        f"{child_table}.{list(fk.columns)} -> "
        f"{fk.parent_table}.{list(fk.parent_columns)}"
    )


def _sanitize_specs_against_known_tables(
    specs: Mapping[str, TableSpec],
    known_tables: Mapping[str, Any],
    *,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
) -> Dict[str, TableSpec]:
    """
    Remove FKs que apontam para parent_table inexistente em specs/known_tables.

    Usada antes de ler/processar dados, principalmente em run_synthesis_from_paths.
    Não valida colunas, pois os DataFrames ainda podem não ter sido lidos.
    """
    if relationship_policy not in ("warn_and_skip", "raise"):
        raise ValueError("relationship_policy deve ser 'warn_and_skip' ou 'raise'.")

    sanitized: Dict[str, TableSpec] = {}

    for name, spec in specs.items():
        valid_fks: List[ForeignKeySpec] = []

        for fk in spec.foreign_keys:
            problems: List[str] = []

            if fk.parent_table == name and set(fk.columns) & set(fk.parent_columns):
                # Self-reference GENUÍNA (FK -> PK da mesma tabela, colunas
                # distintas) é suportada: o loop de síntese remapeia com o
                # mapping old->new da própria tabela, preservando estrutura
                # e auto-loops. Só o caso degenerado é rejeitado: coluna FK
                # de si mesma, cujo remap sobrescreveria a própria origem.
                problems.append(
                    "self-reference com sobreposição entre columns e "
                    "parent_columns (coluna FK de si mesma) não é suportada"
                )

            if fk.parent_table not in specs:
                problems.append(
                    f"parent_table `{fk.parent_table}` não existe em specs_config/specs"
                )

            if fk.parent_table not in known_tables:
                problems.append(
                    f"parent_table `{fk.parent_table}` não existe em table_paths/tables"
                )

            if len(fk.columns) != len(fk.parent_columns):
                problems.append(
                    f"quantidade de columns {list(fk.columns)} difere de "
                    f"parent_columns {list(fk.parent_columns)}"
                )

            if problems:
                _warn_or_raise(
                    "Relacionamento ignorado: "
                    f"{_format_fk(name, fk)}. Motivo(s): "
                    + "; ".join(problems)
                    + ". As tabelas serão geradas sem preservar essa FK.",
                    policy=relationship_policy,
                )
                continue

            valid_fks.append(fk)

        sanitized[name] = TableSpec(
            name=spec.name,
            pk_cols=spec.pk_cols,
            foreign_keys=tuple(valid_fks),
            static=spec.static,
            postprocess=spec.postprocess,
        )

    return sanitized


def _fk_has_data_problem(
    tables: Mapping[str, DataFrame],
    child_table: str,
    fk: ForeignKeySpec,
    *,
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
) -> Optional[str]:
    """
    Verifica se a FK declarada existe logicamente nos dados de entrada.

    Retorna:
        None se o relacionamento parece válido.
        Uma string com o motivo se deve ser ignorado.

    Regras:
        - Se não houver nenhuma chave FK para validar, não considera problema.
        - Se houver zero matches com o pai, ignora a relação.
        - Se houver valores órfãos, ignora a relação para evitar falha posterior.
    """
    child_df_raw = tables[child_table]
    parent_df = tables[fk.parent_table]

    child_df = _filter_child_fk_for_validation(
        child_df_raw,
        fk,
        nullable_fk_policy,
    )

    child_keys = child_df.select(*fk.columns).dropDuplicates()

    total_child_keys = child_keys.count()
    if total_child_keys == 0:
        return None

    parent_keys = parent_df.select(
        *[
            F.col(parent_col).alias(child_col)
            for child_col, parent_col in zip(fk.columns, fk.parent_columns)
        ]
    ).dropDuplicates()

    matched_keys = child_keys.join(
        parent_keys,
        on=list(fk.columns),
        how="inner",
    ).count()

    if matched_keys == 0:
        return (
            f"nenhum valor da FK {list(fk.columns)} da tabela `{child_table}` "
            f"encontrou correspondência no pai `{fk.parent_table}` "
            f"pelas colunas {list(fk.parent_columns)}"
        )

    invalid_keys = child_keys.join(
        parent_keys,
        on=list(fk.columns),
        how="left_anti",
    ).count()

    if invalid_keys > 0:
        return (
            f"existem {invalid_keys} chave(s) FK órfã(s) em `{child_table}` "
            f"para o pai `{fk.parent_table}`"
        )

    return None


def _sanitize_specs_for_available_relationships(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
    check_relationship_values: bool = True,
) -> Dict[str, TableSpec]:
    """
    Remove FKs inválidas sem impedir a geração das tabelas.

    O que continua sendo erro fatal:
        - specs vazio;
        - tabela declarada em specs inexistente em tables;
        - PK inexistente.

    O que vira warning + FK ignorada:
        - parent_table ausente;
        - coluna FK ausente;
        - parent_column ausente;
        - self-reference;
        - tamanhos diferentes de FK;
        - mesma coluna usada em mais de uma FK;
        - FK sem match com o pai;
        - FK com órfãos.
    """
    if not specs:
        raise ValueError("`specs` está vazio.")

    if relationship_policy not in ("warn_and_skip", "raise"):
        raise ValueError("relationship_policy deve ser 'warn_and_skip' ou 'raise'.")

    sanitized: Dict[str, TableSpec] = {}

    for name, spec in specs.items():
        if name not in tables:
            raise ValueError(f"Tabela `{name}` está em specs, mas não está em tables.")

        if spec.name != name:
            raise ValueError(
                f"Inconsistência: chave specs=`{name}`, mas TableSpec.name=`{spec.name}`."
            )

        if not spec.pk_cols:
            raise ValueError(f"Tabela `{name}` precisa ter pelo menos uma coluna de PK.")

        df_cols = set(tables[name].columns)

        for pk in spec.pk_cols:
            if pk not in df_cols:
                raise ValueError(
                    f"PK col `{pk}` não existe na tabela `{name}`. "
                    "Sem PK válida não é seguro gerar a tabela."
                )

        seen_fk_cols: set = set()
        valid_fks: List[ForeignKeySpec] = []

        for fk in spec.foreign_keys:
            problems: List[str] = []

            if not fk.columns:
                problems.append("FK vazia")

            if len(fk.columns) != len(fk.parent_columns):
                problems.append(
                    f"quantidade de columns {list(fk.columns)} difere de "
                    f"parent_columns {list(fk.parent_columns)}"
                )

            if fk.parent_table == name and set(fk.columns) & set(fk.parent_columns):
                # Mesmo critério do sanitizador estrutural: self-reference
                # genuína fica ATIVA (remap in-loop com o mapping da própria
                # tabela); só a degenerada é rejeitada. A checagem de órfãos
                # (_fk_has_data_problem) roda normalmente para a self-FK:
                # a fonte chega aqui já fechada (fecho ascendente com ponto
                # fixo intra-tabela) e neutralizada, então passa.
                problems.append(
                    "self-reference com sobreposição entre columns e "
                    "parent_columns (coluna FK de si mesma) não é suportada"
                )

            if fk.parent_table not in specs:
                problems.append(
                    f"parent_table `{fk.parent_table}` não existe em specs"
                )

            if fk.parent_table not in tables:
                problems.append(
                    f"parent_table `{fk.parent_table}` não existe em tables"
                )

            for c in fk.columns:
                if c not in df_cols:
                    problems.append(
                        f"coluna FK `{c}` não existe na tabela filha `{name}`"
                    )

                if c in seen_fk_cols:
                    problems.append(
                        f"coluna `{c}` participa de mais de uma FK; "
                        "remapeamento ambíguo"
                    )

            if fk.parent_table in tables:
                parent_cols = set(tables[fk.parent_table].columns)
                for pc in fk.parent_columns:
                    if pc not in parent_cols:
                        problems.append(
                            f"parent_column `{pc}` não existe no pai `{fk.parent_table}`"
                        )

            if not problems and check_relationship_values:
                data_problem = _fk_has_data_problem(
                    tables,
                    name,
                    fk,
                    nullable_fk_policy=nullable_fk_policy,
                )
                if data_problem:
                    problems.append(data_problem)

            if problems:
                # Log INFO explícito (além do warning): no modo debug o stdout
                # mostra QUAIS FKs a síntese descartou e por quê — a FK ignorada
                # sai sem remap e vira órfã/nula no estágio 4, então saber disso
                # aqui liga o "4.after_synthesis" à causa.
                logger.info("FK descartada na síntese: %s. Motivo(s): %s",
                            _format_fk(name, fk), "; ".join(problems))
                _warn_or_raise(
                    "Relacionamento ignorado: "
                    f"{_format_fk(name, fk)}. Motivo(s): "
                    + "; ".join(problems)
                    + ". As tabelas serão geradas sem preservar essa FK.",
                    policy=relationship_policy,
                )
                continue

            for c in fk.columns:
                seen_fk_cols.add(c)

            valid_fks.append(fk)

        sanitized[name] = TableSpec(
            name=spec.name,
            pk_cols=spec.pk_cols,
            foreign_keys=tuple(valid_fks),
            static=spec.static,
            postprocess=spec.postprocess,
        )

    return sanitized


def _validate_relationship_columns(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
    check_relationship_values: bool = True,
) -> Dict[str, TableSpec]:
    """
    Confirma PK/FK declaradas contra schemas reais.

    Agora retorna specs saneadas. FKs inválidas viram warning e são ignoradas.
    PK inválida continua sendo erro fatal.
    """
    return _sanitize_specs_for_available_relationships(
        tables,
        specs,
        relationship_policy=relationship_policy,
        nullable_fk_policy=nullable_fk_policy,
        check_relationship_values=check_relationship_values,
    )


def _validate_specs(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
) -> None:
    """
    Validação estrita das specs já saneadas.

    Esta função mantém o nome original, mas agora deve receber specs sem FKs
    inválidas. A sanitização acontece antes dela dentro de synthesize_multitable_spark.
    """
    if not specs:
        raise ValueError("`specs` está vazio.")

    for name, spec in specs.items():
        if name not in tables:
            raise ValueError(f"Tabela `{name}` está em specs, mas não está em tables.")

        if spec.name != name:
            raise ValueError(
                f"Inconsistência: chave specs=`{name}`, mas TableSpec.name=`{spec.name}`."
            )

        if not spec.pk_cols:
            raise ValueError(f"Tabela `{name}` precisa ter pelo menos uma coluna de PK.")

        df_cols = set(tables[name].columns)

        for pk in spec.pk_cols:
            if pk not in df_cols:
                raise ValueError(f"PK col `{pk}` não existe na tabela `{name}`.")

        seen_fk_cols: set = set()

        for fk in spec.foreign_keys:
            if not fk.columns:
                raise ValueError(f"FK vazia declarada na tabela `{name}`.")

            if len(fk.columns) != len(fk.parent_columns):
                raise ValueError(f"FK inválida em `{name}`: tamanhos diferentes.")

            if fk.parent_table == name and set(fk.columns) & set(fk.parent_columns):
                raise ValueError(
                    f"Self-reference degenerada em `{name}`: coluna FK de si "
                    "mesma (columns sobrepõe parent_columns)."
                )

            if fk.parent_table not in specs:
                raise ValueError(
                    f"FK em `{name}` referencia `{fk.parent_table}` ausente em specs."
                )

            if fk.parent_table not in tables:
                raise ValueError(
                    f"FK em `{name}` referencia `{fk.parent_table}` ausente em tables."
                )

            for c in fk.columns:
                if c not in df_cols:
                    raise ValueError(f"FK col `{c}` não existe na filha `{name}`.")

                if c in seen_fk_cols:
                    raise ValueError(f"Coluna `{c}` em `{name}` participa de mais de uma FK.")

                seen_fk_cols.add(c)

            parent_cols = set(tables[fk.parent_table].columns)
            for pc in fk.parent_columns:
                if pc not in parent_cols:
                    raise ValueError(
                        f"FK em `{name}` referencia `{pc}` ausente no pai `{fk.parent_table}`."
                    )


def _toposort_break_cycles(
    deps: Mapping[str, set],
    *,
    on_cycle: Optional[Callable[[set, Mapping[str, set], set], None]] = None,
) -> List[str]:
    """Order nodes so every node follows all of its `deps`, returning each node
    exactly once.

    Cycles are not fatal: when no node is ready, the cycle is broken by forcing
    the node with the fewest still-unresolved deps (ties broken by name) so the
    result is deterministic. `on_cycle(remaining, deps, done)` is invoked the
    first time a break is forced, for callers that want to warn.
    """
    remaining = set(deps)
    done: set = set()
    order: List[str] = []
    cycle_reported = False

    while remaining:
        ready = sorted(n for n in remaining if deps[n] <= done)

        if not ready:
            if on_cycle is not None and not cycle_reported:
                on_cycle(remaining, deps, done)
                cycle_reported = True
            ready = [min(remaining, key=lambda n: (len(deps[n] - done), n))]

        for name in ready:
            order.append(name)
            done.add(name)
            remaining.discard(name)

    return order


def _topological_order(specs: Mapping[str, TableSpec]) -> List[str]:
    """Parents before children. Shares topo_order_tables' cycle policy: cycles
    are broken (with a warning) rather than raised, since sanitize/validate have
    already removed self-refs and missing parents by the time this runs."""
    deps: Dict[str, set] = {
        name: {
            fk.parent_table
            for fk in spec.foreign_keys
            if fk.parent_table != name and fk.parent_table in specs
        }
        for name, spec in specs.items()
    }

    def _warn(remaining: set, deps: Mapping[str, set], done: set) -> None:
        unresolved = {t: sorted(deps[t] - done) for t in sorted(remaining) if deps[t] - done}
        warnings.warn(
            "Ciclo de FK detectado; quebrando arbitrariamente para ordenar. "
            f"Pendências: {unresolved}",
            UserWarning,
            stacklevel=2,
        )

    return _toposort_break_cycles(deps, on_cycle=_warn)


def _referenced_parent_columns(specs: Mapping[str, TableSpec]) -> Dict[str, set]:
    refs: Dict[str, set] = {}

    for child_spec in specs.values():
        for fk in child_spec.foreign_keys:
            refs.setdefault(fk.parent_table, set()).add(tuple(fk.parent_columns))

    return refs


def _with_contiguous_row_id(df: DataFrame, id_col: str) -> DataFrame:
    """
    Adiciona um identificador contíguo 0..N-1 de forma paralela.

    Substitui a versão anterior que usava Window.orderBy() sem partitionBy,
    o que forçava toda a tabela em uma única tarefa (single-task sort). Em
    tabelas de 600M+ linhas isso era um gargalo serial intransponível.

    Algoritmo:
        1. mid = monotonically_increasing_id() — ordem determinística por
           partição (codifica (partition_id, counter) nos bits altos/baixos).
        2. part = spark_partition_id() — id da partição de origem.
        3. part_row = row_number() over (partitionBy part orderBy mid) —
           contador local, totalmente paralelo (sem shuffle entre partições).
        4. sizes = groupBy(part).agg(count(*)) — uma linha por partição.
           O map-side combine reduz N linhas a ~num_partições linhas antes
           do shuffle, então a etapa é barota mas leve.
        5. offset = soma cumulativa de sizes, calculada no DRIVER. `sizes` tem
           uma linha por partição e já é pequeno; coletá-lo e fazer o prefix-sum
           em Python evita um Window sem partitionBy, que o Spark executa como
           Exchange SinglePartition (serial, trava em tabelas grandes).
        6. id_col = offset + part_row - 1, com offset trazido por broadcast.

    Equivalência com a versão anterior:
        monotonically_increasing_id() ordena por (partition_id, counter).
        A ordenação global anterior era: partição 0 em ordem de counter,
        depois partição 1, etc. Esta versão reproduz exatamente essa ordem
        mas sem mover dados entre partições — cada partição calcula seu
        row_number localmente e recebe apenas seu offset por broadcast.

    Determinismo:
        A leitura Parquet é determinística (ordem de linhas por arquivo é
        estável), então mid_col tem a mesma ordem nas duas materializações
        (uma para sizes, outra para o join final). part_row é consistente
        porque depende apenas da ordem de mid dentro de cada partição.
    """
    part_col = f"__{id_col}_part"
    while part_col in df.columns:
        part_col = f"_{part_col}"

    part_row_col = f"__{id_col}_prow"
    while part_row_col in df.columns:
        part_row_col = f"_{part_row_col}"

    part_size_col = f"__{id_col}_psize"
    while part_size_col in df.columns:
        part_size_col = f"_{part_size_col}"

    offset_col = f"__{id_col}_poff"
    while offset_col in df.columns:
        offset_col = f"_{offset_col}"

    mid_col = f"__{id_col}_mid"
    while mid_col in df.columns:
        mid_col = f"_{mid_col}"

    df = (
        df
        .withColumn(mid_col, F.monotonically_increasing_id())
        .withColumn(part_col, F.spark_partition_id())
    )

    w_part = Window.partitionBy(part_col).orderBy(F.col(mid_col))
    df = df.withColumn(part_row_col, F.row_number().over(w_part))

    sizes = (
        df.groupBy(part_col)
        .agg(F.count(F.lit(1)).cast("long").alias(part_size_col))
    )

    # Prefix-sum no driver: uma linha por partição. Coletar é o mesmo custo do
    # broadcast a seguir e evita o Window sem partitionBy (SinglePartition).
    spark = df.sparkSession
    ordered_sizes = sorted(
        ((row[part_col], row[part_size_col]) for row in sizes.collect()),
        key=lambda pair: pair[0],
    )
    running = 0
    offset_rows: List[Tuple[int, int]] = []
    for part_value, size in ordered_sizes:
        offset_rows.append((part_value, running))
        running += size

    offset_schema = T.StructType(
        [
            T.StructField(part_col, T.IntegerType(), False),
            T.StructField(offset_col, T.LongType(), False),
        ]
    )
    offsets = spark.createDataFrame(offset_rows, schema=offset_schema)

    df = df.join(F.broadcast(offsets), on=part_col, how="left")

    df = df.withColumn(
        id_col,
        (F.col(offset_col) + F.col(part_row_col) - F.lit(1)).cast("long"),
    )

    return df.drop(mid_col, part_col, part_row_col, offset_col)


def _bootstrap_rows_exact(
    src_indexed: DataFrame,
    n_rows: int,
    *,
    src_count: int,
    seed: int,
    spark: SparkSession,
    keep_all_source_rows: bool,
) -> DataFrame:
    if n_rows < 0:
        raise ValueError("n_rows deve ser >= 0.")

    src_cols = [c for c in src_indexed.columns if c != "__src_row_id"]

    if n_rows == 0:
        # Evita spark.createDataFrame([], schema=...), que pode acionar
        # cloudpickle em algumas versões do PySpark/Python.
        return src_indexed.limit(0).select(
            F.lit(None).cast("long").alias("__synthetic_pos"),
            F.lit(None).cast("long").alias("__orig_src_row_id"),
            *[F.col(c) for c in src_cols],
        )

    if src_count == 0:
        raise ValueError("Fonte vazia mas n_rows > 0.")

    if keep_all_source_rows:
        if n_rows < src_count:
            raise ValueError(
                f"Pai precisa n_rows >= src_count. n_rows={n_rows}, src_count={src_count}."
            )

        base_keep = (
            src_indexed
            .withColumn("__synthetic_pos", F.col("__src_row_id"))
            .withColumn("__orig_src_row_id", F.col("__src_row_id"))
            .select("__synthetic_pos", "__orig_src_row_id", *src_cols)
        )

        extra_n = n_rows - src_count
        if extra_n == 0:
            return base_keep

        extra_positions = (
            spark.range(src_count, n_rows)
            .withColumnRenamed("id", "__synthetic_pos")
            .withColumn(
                "__lookup_src_row_id",
                F.floor(F.rand(seed) * F.lit(src_count)).cast("long"),
            )
        )

        extra = (
            extra_positions
            .join(
                src_indexed,
                extra_positions["__lookup_src_row_id"] == src_indexed["__src_row_id"],
                "left",
            )
            .withColumn("__orig_src_row_id", F.col("__src_row_id"))
            .select("__synthetic_pos", "__orig_src_row_id", *src_cols)
        )

        return base_keep.unionByName(extra)

    positions = (
        spark.range(0, n_rows)
        .withColumnRenamed("id", "__synthetic_pos")
        .withColumn(
            "__lookup_src_row_id",
            F.floor(F.rand(seed) * F.lit(src_count)).cast("long"),
        )
    )

    return (
        positions
        .join(
            src_indexed,
            positions["__lookup_src_row_id"] == src_indexed["__src_row_id"],
            "left",
        )
        .withColumn("__orig_src_row_id", F.col("__src_row_id"))
        .select("__synthetic_pos", "__orig_src_row_id", *src_cols)
    )


_INT_TYPE_LIMITS = (
    (T.ByteType, 127),
    (T.ShortType, 32_767),
    (T.IntegerType, 2_147_483_647),
)


_FLOAT_EXACT_INT_LIMIT = 16_777_216            # 2^24 (float 32 bits)


_DOUBLE_EXACT_INT_LIMIT = 9_007_199_254_740_992  # 2^53 (double 64 bits)


def _max_pk_value(df_cached: DataFrame, pk: str) -> Optional[int]:
    """
    Retorna o maior valor atual da PK como int.

    v4: também funciona para PK double/float/decimal (caso típico de CSV lido
    com inferSchema=True). Valores NaN são ignorados via floor seguro.
    """
    row = df_cached.agg(F.max(F.col(pk)).alias("max_pk")).collect()[0]
    value = row["max_pk"]

    if value is None:
        return None

    value_f = float(value)

    # NaN não é comparável; trata como inexistente para não propagar lixo.
    if math.isnan(value_f):
        return None

    return int(math.floor(value_f))


def _set_unique_pk_column(
    work: DataFrame,
    source_cached: DataFrame,
    pk: str,
    *,
    append_after_max: bool,
    target_n: int,
    offset: int = 0,
    pk_max_override: Optional[int] = None,
) -> DataFrame:
    # When pk_max_override is given, append after THIS max instead of the one
    # observed in source_cached. Used so a --limit'd (sampled) source still gets
    # PKs above the table's TRUE max, computed from the full Parquet by engorda.
    dt = _get_field_type(source_cached, pk)

    if _is_integer_type(dt):
        observed_max = (
            pk_max_override if pk_max_override is not None
            else _max_pk_value(source_cached, pk)
        )
        start = (observed_max or 0) + 1 if append_after_max else 1
        highest = start + target_n - 1 + offset

        for type_cls, limit in _INT_TYPE_LIMITS:
            if isinstance(dt, type_cls) and highest > limit:
                raise OverflowError(
                    f"PK `{pk}` {type_cls.__name__} estoura limite {limit:,} "
                    f"(max {highest:,})."
                )

        return work.withColumn(
            pk,
            (F.col("__synthetic_pos") + F.lit(start + offset)).cast(dt),
        )

    # ---- NOVO na v4: PK em ponto flutuante (double/float) -----------------
    # Cenário típico: CSV lido com inferSchema=True infere IDs como double.
    # Estratégia: gerar a mesma sequência inteira e castar para o tipo
    # original, garantindo que os valores fiquem na faixa de inteiros
    # representáveis de forma exata (2^53 para double, 2^24 para float).
    if _is_float_type(dt):
        observed_max = (
            pk_max_override if pk_max_override is not None
            else _max_pk_value(source_cached, pk)
        )
        start = (observed_max or 0) + 1 if append_after_max else 1
        highest = start + target_n - 1 + offset

        exact_limit = (
            _DOUBLE_EXACT_INT_LIMIT
            if isinstance(dt, T.DoubleType)
            else _FLOAT_EXACT_INT_LIMIT
        )

        if highest > exact_limit:
            raise OverflowError(
                f"PK `{pk}` ({type(dt).__name__}) atingiria {highest:,}, acima do "
                f"limite de inteiro exato {exact_limit:,}. Acima disso valores "
                "consecutivos colidem e a PK deixaria de ser única. "
                "Sugestão: converta a coluna para LongType na leitura."
            )

        return work.withColumn(
            pk,
            (F.col("__synthetic_pos") + F.lit(start + offset)).cast(dt),
        )

    # ---- NOVO na v4: PK decimal -------------------------------------------
    if _is_decimal_type(dt):
        observed_max = (
            pk_max_override if pk_max_override is not None
            else _max_pk_value(source_cached, pk)
        )
        start = (observed_max or 0) + 1 if append_after_max else 1
        highest = start + target_n - 1 + offset

        # Dígitos inteiros disponíveis = precision - scale.
        int_digits = dt.precision - dt.scale
        decimal_limit = (10 ** int_digits) - 1 if int_digits > 0 else 0

        if highest > decimal_limit:
            raise OverflowError(
                f"PK `{pk}` Decimal({dt.precision},{dt.scale}) estoura o limite "
                f"de {decimal_limit:,} (max {highest:,})."
            )

        return work.withColumn(
            pk,
            (F.col("__synthetic_pos") + F.lit(start + offset)).cast(dt),
        )

    if _is_string_type(dt):
        return work.withColumn(
            pk,
            F.concat(
                F.lit(f"SYN_{pk}_"),
                F.lpad(
                    (F.col("__synthetic_pos") + F.lit(offset)).cast("string"),
                    14,
                    "0",
                ),
            ).cast(dt),
        )

    raise TypeError(
        f"PK `{pk}` tipo {dt!r} sem estratégia segura. "
        "Tipos suportados: inteiro, double, float, decimal e string. "
        "Sugestão: faça cast da coluna para um desses tipos antes da síntese."
    )


def _generate_pk_columns(
    work: DataFrame,
    source_cached: DataFrame,
    spec: TableSpec,
    *,
    append_after_max: bool,
    target_n: int,
    pk_max_override: Optional[int] = None,
) -> DataFrame:
    if len(spec.pk_cols) == 1:
        return _set_unique_pk_column(
            work,
            source_cached,
            spec.pk_cols[0],
            append_after_max=append_after_max,
            target_n=target_n,
            pk_max_override=pk_max_override,
        )

    last_pk = spec.pk_cols[-1]
    last_type = _get_field_type(source_cached, last_pk)

    if not _is_safe_pk_type(last_type):
        raise TypeError(
            f"PK composta `{spec.name}` última col `{last_pk}` tipo {last_type!r} inseguro. "
            "Tipos suportados: inteiro, double, float, decimal e string."
        )

    return _set_unique_pk_column(
        work,
        source_cached,
        last_pk,
        append_after_max=append_after_max,
        target_n=target_n,
        pk_max_override=pk_max_override,
    )


def _build_mapping_for_parent_cols(
    work_cached: DataFrame,
    parent_cols: Tuple[str, ...],
    storage_level: StorageLevel,
) -> DataFrame:
    old_cols = [f"__old__{c}" for c in parent_cols]
    missing_old = [c for c in old_cols if c not in work_cached.columns]

    if missing_old:
        raise ValueError(f"Mapping: colunas antigas ausentes: {missing_old}")

    mapping = work_cached.select(
        *[
            F.col(old_cols[i]).alias(f"__old_{i}")
            for i in range(len(parent_cols))
        ],
        *[
            F.col(parent_cols[i]).alias(f"__new_{i}")
            for i in range(len(parent_cols))
        ],
        F.col("__synthetic_pos"),
    )

    partition_cols = [F.col(f"__old_{i}") for i in range(len(parent_cols))]
    w = Window.partitionBy(*partition_cols).orderBy(F.col("__synthetic_pos"))

    mapping = mapping.withColumn(
        "__candidate_rank",
        F.row_number().over(w).cast("long"),
    )

    counts = mapping.groupBy(
        *[F.col(f"__old_{i}") for i in range(len(parent_cols))]
    ).agg(
        F.count(F.lit(1)).cast("long").alias("__candidate_count")
    )

    mapping = mapping.join(
        counts,
        on=[f"__old_{i}" for i in range(len(parent_cols))],
        how="left",
    )

    return _persist(mapping, storage_level)


def _fk_join_condition(
    left_df: DataFrame,
    left_cols: List[str],
    right_df: DataFrame,
    right_cols: List[str],
):
    conditions = [
        left_df[left_cols[i]].eqNullSafe(right_df[right_cols[i]])
        for i in range(len(left_cols))
    ]
    return reduce(lambda a, b: a & b, conditions)


def _apply_fk_mapping(
    work: DataFrame,
    fk: ForeignKeySpec,
    mapping: DataFrame,
    *,
    seed: int,
    broadcast_fk_counts: bool,
    fk_index: int = 0,
) -> DataFrame:
    fk_tag = (
        f"__fk{fk_index}_{fk.parent_table}_"
        f"{_stable_seed(seed, fk.parent_table, fk.columns, fk.parent_columns)}"
    )
    n = len(fk.columns)

    counts = mapping.select(
        *[
            F.col(f"__old_{i}").alias(f"{fk_tag}_old_{i}")
            for i in range(n)
        ],
        F.col("__candidate_count").alias(f"{fk_tag}_count"),
    ).dropDuplicates([f"{fk_tag}_old_{i}" for i in range(n)])

    count_old_cols = [f"{fk_tag}_old_{i}" for i in range(n)]
    cond_counts = _fk_join_condition(
        work,
        list(fk.columns),
        counts,
        count_old_cols,
    )

    if broadcast_fk_counts:
        work = work.join(F.broadcast(counts), cond_counts, "left")
    else:
        work = work.join(counts, cond_counts, "left")

    work = work.withColumn(
        f"{fk_tag}_rank",
        F.when(
            F.col(f"{fk_tag}_count").isNull(),
            F.lit(None).cast("long"),
        ).otherwise(
            F.floor(
                F.rand(_stable_seed(seed, fk_tag, "rank"))
                * F.col(f"{fk_tag}_count")
            ).cast("long") + F.lit(1)
        ),
    )

    m = mapping.select(
        *[
            F.col(f"__old_{i}").alias(f"{fk_tag}_map_old_{i}")
            for i in range(n)
        ],
        *[
            F.col(f"__new_{i}").alias(f"{fk_tag}_new_{i}")
            for i in range(n)
        ],
        F.col("__candidate_rank").alias(f"{fk_tag}_map_rank"),
    )

    map_old_cols = [f"{fk_tag}_map_old_{i}" for i in range(n)]
    cond_map_key = _fk_join_condition(work, list(fk.columns), m, map_old_cols)
    cond_map = cond_map_key & (work[f"{fk_tag}_rank"] == m[f"{fk_tag}_map_rank"])

    work = work.join(m, cond_map, "left")

    for i, child_col in enumerate(fk.columns):
        child_type = _get_field_type(work, child_col)
        work = work.withColumn(
            child_col,
            F.col(f"{fk_tag}_new_{i}").cast(child_type),
        )

    drop_cols = (
        [f"{fk_tag}_old_{i}" for i in range(n)]
        + [f"{fk_tag}_map_old_{i}" for i in range(n)]
        + [f"{fk_tag}_new_{i}" for i in range(n)]
        + [f"{fk_tag}_count", f"{fk_tag}_rank", f"{fk_tag}_map_rank"]
    )

    return work.drop(*drop_cols)


def _rows_to_spark_df(
    spark: SparkSession,
    rows: List[Tuple[Any, ...]],
    columns: List[Tuple[str, str]],
) -> DataFrame:
    """
    Cria um DataFrame pequeno de diagnóstico sem spark.createDataFrame(rows).

    Motivo:
        Em alguns ambientes, spark.createDataFrame(lista_python) pode acionar
        cloudpickle.dumps e falhar com:

            IndexError: tuple index out of range
            Could not serialize object

        Para evitar isso, montamos cada linha usando spark.range(1).select(lit(...)).
        Assim não serializamos função Python nem lista de Row para os executores.

    Args:
        spark: SparkSession.
        rows: lista de tuplas com os valores.
        columns: lista de pares (nome_coluna, tipo_spark_sql), ex.:
                 [("table", "string"), ("total_rows", "long")].
    """
    select_exprs_empty = [
        F.lit(None).cast(dtype).alias(name)
        for name, dtype in columns
    ]

    if not rows:
        return spark.range(0).select(*select_exprs_empty)

    df_out: Optional[DataFrame] = None

    for row in rows:
        if len(row) != len(columns):
            raise ValueError(
                f"Linha de diagnóstico possui {len(row)} valores, mas eram esperados "
                f"{len(columns)}: {row!r}"
            )

        exprs = [
            F.lit(value).cast(dtype).alias(name)
            for value, (name, dtype) in zip(row, columns)
        ]
        one = spark.range(1).select(*exprs)
        df_out = one if df_out is None else df_out.unionByName(one)

    return df_out


def validate_primary_keys(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
) -> DataFrame:
    spark = next(iter(tables.values())).sparkSession
    rows = []

    for name, spec in specs.items():
        df = tables[name]
        total_rows = df.count()
        distinct_pk = df.select(*spec.pk_cols).dropDuplicates().count()

        null_condition = reduce(
            lambda a, b: a | b,
            [F.col(c).isNull() for c in spec.pk_cols],
        )
        null_pk_rows = df.where(null_condition).count()

        rows.append(
            (
                name,
                ",".join(spec.pk_cols),
                int(total_rows),
                int(distinct_pk),
                int(null_pk_rows),
                int(total_rows - distinct_pk),
            )
        )

    return _rows_to_spark_df(
        spark,
        rows,
        columns=[
            ("table", "string"),
            ("pk_cols", "string"),
            ("total_rows", "long"),
            ("distinct_pk", "long"),
            ("null_pk_rows", "long"),
            ("duplicate_pk_rows", "long"),
        ],
    )


def _filter_child_fk_for_validation(
    child_df: DataFrame,
    fk: ForeignKeySpec,
    nullable_fk_policy: NullableFkPolicy,
) -> DataFrame:
    if nullable_fk_policy == "invalid_null":
        return child_df

    any_null = reduce(
        lambda a, b: a | b,
        [F.col(c).isNull() for c in fk.columns],
    )

    all_null = reduce(
        lambda a, b: a & b,
        [F.col(c).isNull() for c in fk.columns],
    )

    if nullable_fk_policy == "allow_any_null":
        return child_df.where(~any_null)

    if nullable_fk_policy == "allow_all_null":
        return child_df.where(~all_null)

    raise ValueError(f"nullable_fk_policy inválida: {nullable_fk_policy}")


def validate_foreign_keys(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
    relationship_policy: RelationshipPolicy = "warn_and_skip",
) -> DataFrame:
    """
    Valida FKs que ainda estão ativas em specs.

    Se por algum motivo uma FK inválida chegar aqui e relationship_policy for
    warn_and_skip, ela entra no relatório como invalid_fk=-1 e gera warning,
    sem quebrar a execução.
    """
    spark = next(iter(tables.values())).sparkSession
    rows = []

    for child_name, child_spec in specs.items():
        child_df_raw = tables[child_name]

        for fk in child_spec.foreign_keys:
            if fk.parent_table not in tables:
                _warn_or_raise(
                    "Validação FK ignorada: "
                    f"{_format_fk(child_name, fk)}. Pai não existe em tables.",
                    policy=relationship_policy,
                )
                rows.append(
                    (
                        child_name,
                        ",".join(fk.columns),
                        fk.parent_table,
                        ",".join(fk.parent_columns),
                        0,
                        -1,
                    )
                )
                continue

            parent_df = tables[fk.parent_table]
            child_df = _filter_child_fk_for_validation(
                child_df_raw,
                fk,
                nullable_fk_policy,
            )

            child_keys = child_df.select(*fk.columns).dropDuplicates()
            parent_keys = parent_df.select(
                *[
                    F.col(parent_col).alias(child_col)
                    for child_col, parent_col in zip(fk.columns, fk.parent_columns)
                ]
            ).dropDuplicates()

            invalid = child_keys.join(
                parent_keys,
                on=list(fk.columns),
                how="left_anti",
            ).count()

            total_distinct = child_keys.count()

            rows.append(
                (
                    child_name,
                    ",".join(fk.columns),
                    fk.parent_table,
                    ",".join(fk.parent_columns),
                    int(total_distinct),
                    int(invalid),
                )
            )

    return _rows_to_spark_df(
        spark,
        rows,
        columns=[
            ("child_table", "string"),
            ("fk_cols", "string"),
            ("parent_table", "string"),
            ("parent_cols", "string"),
            ("distinct_child_fk", "long"),
            ("invalid_fk", "long"),
        ],
    )


def _run_validation_or_raise(
    result: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    nullable_fk_policy: NullableFkPolicy,
) -> None:
    pk_report = validate_primary_keys(result, specs)
    fk_report = validate_foreign_keys(
        result,
        specs,
        nullable_fk_policy=nullable_fk_policy,
        relationship_policy="warn_and_skip",
    )

    pk_problems = pk_report.where(
        "null_pk_rows > 0 OR duplicate_pk_rows > 0"
    ).count()

    fk_problems = fk_report.where(
        "invalid_fk > 0"
    ).count()

    if pk_problems or fk_problems:
        print(">>> FALHA NA VALIDAÇÃO")
        pk_report.show(truncate=False)
        fk_report.show(truncate=False)
        raise RuntimeError(
            f"Validação falhou: {pk_problems} PK, {fk_problems} FK."
        )


def run_validation_or_raise(
    result: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
) -> None:
    _run_validation_or_raise(
        result,
        specs,
        nullable_fk_policy=nullable_fk_policy,
    )


def synthesize_multitable_spark(
    tables: Mapping[str, DataFrame],
    specs: Mapping[str, TableSpec],
    n_rows_by_table: Optional[Mapping[str, int]] = None,
    *,
    seed: int = 42,
    append_after_max_pk: bool = True,
    pk_max_by_table: Optional[Mapping[str, int]] = None,
    engorda_ts: Optional[datetime] = None,
    dt_vencimento_prazo_dias_by_table: Optional[Mapping[str, int]] = None,
    default_dt_vencimento_prazo_dias: int = DEFAULT_DT_VENCIMENTO_PRAZO_DIAS,
    validate_mode: ValidateMode = "full",
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
    broadcast_fk_counts: bool = False,
    storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    verbose: bool = False,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
    check_relationship_values: bool = True,
    truncate_lineage: bool = False,
) -> Dict[str, DataFrame]:
    """
    Gera dados sintéticos multi-tabela.

    Novo comportamento:
        FKs inválidas/ausentes são ignoradas com warning quando
        relationship_policy="warn_and_skip".

    Para recuperar comportamento estrito antigo:
        relationship_policy="raise"
    """
    if validate_mode not in ("none", "full"):
        raise ValueError("validate_mode deve ser 'none' ou 'full'.")

    if relationship_policy not in ("warn_and_skip", "raise"):
        raise ValueError("relationship_policy deve ser 'warn_and_skip' ou 'raise'.")

    engorda_ts = _normalize_engorda_ts(engorda_ts)
    dt_vencimento_prazo_dias_by_table = dict(dt_vencimento_prazo_dias_by_table or {})

    # Saneia specs antes da validação/topologia/mapping.
    active_specs = _sanitize_specs_for_available_relationships(
        tables,
        specs,
        relationship_policy=relationship_policy,
        nullable_fk_policy=nullable_fk_policy,
        check_relationship_values=check_relationship_values,
    )

    _validate_specs(tables, active_specs)

    n_rows_by_table = dict(n_rows_by_table or {})
    order = _topological_order(active_specs)
    parent_refs = _referenced_parent_columns(active_specs)

    result: Dict[str, DataFrame] = {}
    mappings: Dict[Tuple[str, Tuple[str, ...]], DataFrame] = {}
    intermediates: List[DataFrame] = []
    # FKs cujo mapping do pai ainda não existia na visita da filha (aresta
    # quebrada por CICLO: a filha foi forçada antes do pai na ordem
    # topológica). São remapeadas num passe adiado após o loop principal,
    # quando todos os mappings existem. Sem isso a coluna sai com os valores
    # ANTIGOS do pai e null_orphan_fks a anula por completo (100% órfã
    # contra as PKs sintéticas novas).
    deferred_fks: List[Tuple[str, ForeignKeySpec, int]] = []

    # How many child tables still need each parent mapping. Once a parent's last
    # consumer is synthesized, its mapping is unpersisted instead of being held
    # for the whole component — large components otherwise keep every table's
    # bootstrapped frames + mappings cached at once, which is the memory wall.
    mapping_consumers: Dict[Tuple[str, Tuple[str, ...]], int] = {}
    for child_name in order:
        for fk in active_specs[child_name].foreign_keys:
            key = (fk.parent_table, tuple(fk.parent_columns))
            mapping_consumers[key] = mapping_consumers.get(key, 0) + 1

    def _release_mapping_consumer(key: Tuple[str, Tuple[str, ...]]) -> None:
        remaining = mapping_consumers.get(key, 0) - 1
        mapping_consumers[key] = remaining
        if remaining <= 0:
            _safe_unpersist(mappings.pop(key, None))

    if verbose:
        print("Specs ativas após saneamento de relacionamentos:")
        for table_name, spec in active_specs.items():
            if spec.foreign_keys:
                for fk in spec.foreign_keys:
                    print("  OK:", _format_fk(table_name, fk))
            else:
                print(f"  {table_name}: sem FK ativa")

    try:
        for table_name in order:
            source = tables[table_name]
            spec = active_specs[table_name]
            spark = source.sparkSession
            original_cols = source.columns
            target_n_raw = n_rows_by_table.get(table_name)

            ref_col_sets = parent_refs.get(table_name, set())
            ref_cols = sorted(
                set(c for cols in ref_col_sets for c in cols)
                | set(spec.pk_cols)
            )

            src_indexed: Optional[DataFrame] = None

            if spec.static:
                src_count = source.count()

                if target_n_raw is not None and int(target_n_raw) != src_count:
                    warnings.warn(
                        f"`{table_name}` static; n_rows ignorado.",
                        UserWarning,
                        stacklevel=2,
                    )

                if verbose:
                    print(f"[{table_name}] STATIC | {src_count} linhas")

                work = (
                    _with_contiguous_row_id(source, "__synthetic_pos")
                    .withColumn("__orig_src_row_id", F.col("__synthetic_pos"))
                )

                for c in ref_cols:
                    work = work.withColumn(f"__old__{c}", F.col(c))

                if spec.postprocess is not None:
                    work = spec.postprocess(work, result)

                work = _apply_engorda_business_rules(
                    work,
                    engorda_ts=engorda_ts,
                    dt_vencimento_prazo_dias=dt_vencimento_prazo_dias_by_table.get(table_name),
                    default_dt_vencimento_prazo_dias=default_dt_vencimento_prazo_dias,
                )

                work = _materialize(work, storage_level, truncate_lineage)
                intermediates.append(work)

            else:
                src_indexed = _with_contiguous_row_id(source, "__src_row_id")
                src_indexed = _persist(src_indexed, storage_level)
                src_count = src_indexed.count()
                intermediates.append(src_indexed)

                target_n = int(target_n_raw if target_n_raw is not None else src_count)
                keep_all = table_name in parent_refs

                if verbose:
                    print(
                        f"[{table_name}] {'PAI' if keep_all else 'FILHO'} | "
                        f"{src_count}->{target_n}"
                    )

                work = _bootstrap_rows_exact(
                    src_indexed,
                    target_n,
                    src_count=src_count,
                    seed=_stable_seed(seed, table_name, "bootstrap"),
                    spark=spark,
                    keep_all_source_rows=keep_all,
                )

                for c in ref_cols:
                    work = work.withColumn(f"__old__{c}", F.col(c))

                work = _generate_pk_columns(
                    work,
                    src_indexed,
                    spec,
                    append_after_max=append_after_max_pk,
                    target_n=target_n,
                    pk_max_override=(pk_max_by_table or {}).get(table_name),
                )

                # Self-FK genuína: o mapping PK_antiga -> PK_nova da PRÓPRIA
                # tabela será derivado deste work. Materializa antes, para o
                # mapping e o frame final enxergarem EXATAMENTE as mesmas
                # linhas bootstrapadas (e para a cadeia de bootstrap não ser
                # computada duas vezes).
                has_self_fk = any(f.parent_table == table_name
                                  for f in spec.foreign_keys)
                if has_self_fk:
                    work = _materialize(work, storage_level, truncate_lineage)
                    intermediates.append(work)

                for fk_idx, fk in enumerate(spec.foreign_keys):
                    key = (fk.parent_table, tuple(fk.parent_columns))

                    if fk.parent_table == table_name:
                        # ---- Self-reference (ex.: CONTA_PARTICIPANTE.
                        # NUM_CONTA_PARTICIPANTE_CETIP -> NUM_CONTA_
                        # PARTICIPANTE). O remap por VALOR com o mapping da
                        # própria tabela preserva a ESTRUTURA: a cópia de X
                        # aponta para uma cópia do MESMO Y que X apontava na
                        # origem. Nulos permanecem nulos (taxa preservada);
                        # valores sem candidato no mapping viram NULL.
                        if set(fk.columns) & set(spec.pk_cols):
                            _warn_or_raise(
                                "Self-FK com coluna dentro da PK: "
                                f"{_format_fk(table_name, fk)}. Remapear "
                                "tocaria a PK; delegada aos passes "
                                "bind/null_orphan.",
                                policy=relationship_policy,
                            )
                            _release_mapping_consumer(key)
                            continue

                        # Marca ANTES do remap os auto-loops literais
                        # (fk_antiga == pk_antiga, o padrão "conta própria"):
                        # o remap genérico apontaria para uma cópia QUALQUER
                        # da linha original; aqui garantimos fk_nova :=
                        # pk_nova DA PRÓPRIA LINHA.
                        selfloop_col = f"__selfloop_{fk_idx}"
                        eh_selfloop = reduce(
                            lambda a, b: a & b,
                            [work[c] == work[f"__old__{p}"]
                             for c, p in zip(fk.columns, fk.parent_columns)],
                        )
                        work = work.withColumn(selfloop_col, eh_selfloop)

                        self_mapping = _build_mapping_for_parent_cols(
                            work,
                            tuple(fk.parent_columns),
                            storage_level=storage_level,
                        )
                        self_mapping.count()
                        intermediates.append(self_mapping)

                        work = _apply_fk_mapping(
                            work,
                            fk,
                            self_mapping,
                            seed=_stable_seed(
                                seed,
                                table_name,
                                fk.parent_table,
                                fk.columns,
                                fk.parent_columns,
                            ),
                            broadcast_fk_counts=broadcast_fk_counts,
                            fk_index=fk_idx,
                        )

                        for c, p in zip(fk.columns, fk.parent_columns):
                            child_type = _get_field_type(work, c)
                            work = work.withColumn(
                                c,
                                F.when(F.col(selfloop_col),
                                       F.col(p).cast(child_type))
                                 .otherwise(F.col(c)),
                            )
                        work = work.drop(selfloop_col)
                        logger.info(
                            "Self-FK remapeada estruturalmente: %s "
                            "(estrutura e auto-loops preservados).",
                            _format_fk(table_name, fk))
                        _release_mapping_consumer(key)
                        continue

                    if key not in mappings:
                        if fk.parent_table not in result:
                            # Pai ainda não sintetizado -> aresta quebrada
                            # por ciclo. Adia o remap para depois do loop;
                            # NÃO libera o consumer, para o mapping do pai
                            # (construído quando ele for visitado) ficar
                            # vivo até o passe adiado.
                            logger.info(
                                "FK adiada (ciclo): %s — remapeada após a "
                                "síntese do pai.", _format_fk(table_name, fk))
                            deferred_fks.append((table_name, fk, fk_idx))
                            continue
                        _warn_or_raise(
                            "Mapping ausente para relacionamento ativo: "
                            f"{_format_fk(table_name, fk)}. "
                            "A FK será mantida sem remapeamento nesta tabela.",
                            policy=relationship_policy,
                        )
                        _release_mapping_consumer(key)
                        continue

                    work = _apply_fk_mapping(
                        work,
                        fk,
                        mappings[key],
                        seed=_stable_seed(
                            seed,
                            table_name,
                            fk.parent_table,
                            fk.columns,
                            fk.parent_columns,
                        ),
                        broadcast_fk_counts=broadcast_fk_counts,
                        fk_index=fk_idx,
                    )
                    _release_mapping_consumer(key)

                if spec.postprocess is not None:
                    work = spec.postprocess(work, result)

                work = _apply_engorda_business_rules(
                    work,
                    engorda_ts=engorda_ts,
                    dt_vencimento_prazo_dias=dt_vencimento_prazo_dias_by_table.get(table_name),
                    default_dt_vencimento_prazo_dias=default_dt_vencimento_prazo_dias,
                )

                work = _materialize(work, storage_level, truncate_lineage)
                intermediates.append(work)

            if table_name in parent_refs:
                for cols in parent_refs[table_name]:
                    key = (table_name, tuple(cols))
                    if mapping_consumers.get(key, 0) <= 0:
                        # Únicos consumidores eram self-FKs desta tabela, já
                        # atendidas com mapping efêmero no loop acima — não
                        # há filha futura para consumir; economiza o build.
                        continue
                    mapping_df = _build_mapping_for_parent_cols(
                        work,
                        tuple(cols),
                        storage_level=storage_level,
                    )
                    mapping_df.count()
                    mappings[key] = mapping_df
                    intermediates.append(mapping_df)

            synth = work.select(*original_cols)
            synth = _persist(synth, storage_level)
            synth.count()
            result[table_name] = synth

            # synth and any parent mapping are now materialized with their own
            # cached blocks, so the bulky bootstrapped frames for this table are
            # no longer needed — free them now instead of at end-of-component.
            _safe_unpersist(work)
            _safe_unpersist(src_indexed)

        # Passe adiado: remapeia as FKs de arestas de ciclo agora que TODOS
        # os mappings existem. _apply_fk_mapping opera por VALOR nas colunas
        # da FK (não precisa de __synthetic_pos no lado da filha), então
        # funciona direto sobre o frame final em `result`. A mesma seed do
        # caminho inline mantém o resultado determinístico.
        for child_name, fk, fk_idx in deferred_fks:
            key = (fk.parent_table, tuple(fk.parent_columns))
            mapping = mappings.get(key)
            if mapping is None:
                _warn_or_raise(
                    "Mapping ausente mesmo após o loop para FK adiada: "
                    f"{_format_fk(child_name, fk)}. "
                    "A FK será mantida sem remapeamento nesta tabela.",
                    policy=relationship_policy,
                )
                _release_mapping_consumer(key)
                continue
            repaired = _apply_fk_mapping(
                result[child_name],
                fk,
                mapping,
                seed=_stable_seed(
                    seed,
                    child_name,
                    fk.parent_table,
                    fk.columns,
                    fk.parent_columns,
                ),
                broadcast_fk_counts=broadcast_fk_counts,
                fk_index=fk_idx,
            )
            # Materializa ANTES de liberar o mapping: o plano do frame
            # reparado referencia o mapping; persistir+count corta a
            # dependência de recompute para os consumidores downstream
            # (bind/remap_self/null_orphan/gravação).
            repaired = _persist(repaired, storage_level)
            repaired.count()
            _safe_unpersist(result[child_name])
            result[child_name] = repaired
            logger.info(
                "FK adiada remapeada: %s.", _format_fk(child_name, fk))
            _release_mapping_consumer(key)

        if validate_mode == "full":
            if verbose:
                print("Validando...")

            _run_validation_or_raise(
                result,
                active_specs,
                nullable_fk_policy=nullable_fk_policy,
            )

            if verbose:
                print("Validação OK.")

        return result

    except Exception:
        for df in result.values():
            _safe_unpersist(df)
        raise

    finally:
        for df in intermediates:
            _safe_unpersist(df)


def _normalize_cols(value: Any, *, field_name: str, table_name: str) -> Tuple[str, ...]:
    if value is None:
        raise ValueError(f"Tabela `{table_name}`: `{field_name}` é obrigatório.")

    if isinstance(value, str):
        value = [value]

    if not isinstance(value, (list, tuple)):
        raise TypeError(
            f"Tabela `{table_name}`: `{field_name}` deve ser string/list/tuple. "
            f"Recebido: {type(value).__name__}."
        )

    out = tuple(str(c).strip() for c in value if str(c).strip())

    if not out:
        raise ValueError(f"Tabela `{table_name}`: `{field_name}` não pode ser vazio.")

    return out


def _try_normalize_cols(
    value: Any,
    *,
    field_name: str,
    table_name: str,
    fk_index: Optional[int] = None,
) -> Optional[Tuple[str, ...]]:
    """
    Versão tolerante de _normalize_cols.

    Retorna None quando o campo não existe ou está vazio.
    Isso permite avisar e ignorar apenas a FK problemática sem parar o código.
    """
    try:
        return _normalize_cols(value, field_name=field_name, table_name=table_name)
    except Exception as exc:
        suffix = f" FK #{fk_index}" if fk_index is not None else ""
        warnings.warn(
            f"Configuração de relacionamento ignorada em `{table_name}`{suffix}: "
            f"campo `{field_name}` inválido. Motivo: {exc}",
            UserWarning,
            stacklevel=2,
        )
        return None


def _infer_parent_table_from_config(
    specs_config: Mapping[str, Mapping],
    *,
    child_table: str,
    parent_columns: Optional[Tuple[str, ...]],
    child_fk_columns: Optional[Tuple[str, ...]],
) -> Tuple[Optional[str], Optional[Tuple[str, ...]], str]:
    """
    Tenta inferir parent_table quando ele não foi informado na FK.

    Estratégia:
        1. Se parent_columns foi informado, procura uma tabela cuja pk_cols seja
           exatamente igual a parent_columns.
        2. Se parent_columns não foi informado, usa child_fk_columns como pista
           e procura uma tabela cuja pk_cols seja exatamente igual a child_fk_columns.
        3. Se houver match único, retorna a tabela inferida.
        4. Se houver zero ou múltiplos matches, retorna None e motivo amigável.

    Observação:
        A inferência é conservadora de propósito. Se ficar ambígua, a FK é ignorada
        com warning para evitar relacionamento errado.
    """
    candidates: List[str] = []
    target_cols = parent_columns or child_fk_columns

    if not target_cols:
        return None, None, "não foi possível inferir: parent_table e parent_columns ausentes"

    for candidate_table, cfg in specs_config.items():
        if candidate_table == child_table:
            continue
        if not isinstance(cfg, ABCMapping):
            continue
        raw_pk = cfg.get("pk_cols")
        if raw_pk is None:
            continue
        try:
            pk_cols = _normalize_cols(
                raw_pk,
                field_name="pk_cols",
                table_name=str(candidate_table),
            )
        except Exception:
            continue
        if tuple(pk_cols) == tuple(target_cols):
            candidates.append(str(candidate_table))

    if len(candidates) == 1:
        inferred_parent_table = candidates[0]
        inferred_parent_columns = tuple(target_cols)
        return (
            inferred_parent_table,
            inferred_parent_columns,
            f"parent_table inferido automaticamente como `{inferred_parent_table}` "
            f"porque pk_cols={list(inferred_parent_columns)}",
        )

    if not candidates:
        return (
            None,
            None,
            "parent_table ausente e nenhuma tabela candidata foi encontrada "
            f"com pk_cols={list(target_cols)}",
        )

    return (
        None,
        None,
        "parent_table ausente e a inferência ficou ambígua; "
        f"candidatos com pk_cols={list(target_cols)}: {candidates}",
    )


def _build_specs_from_config(
    specs_config: Mapping[str, Mapping],
    postprocess_by_table: Optional[Mapping[str, PostProcessor]] = None,
    *,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
) -> Dict[str, TableSpec]:
    """
    Converte dicionário declarativo em specs tipados.

    Regras desta versão:
        - pk_cols continua obrigatório por tabela.
        - foreign_keys é opcional.
        - Dentro de cada FK, parent_table NÃO é mais obrigatório.
        - Se parent_table não vier, o código tenta inferir pelo pk_cols do pai.
        - Se não conseguir inferir, avisa e ignora somente aquela FK.
        - Se coluna FK/parent_column não existir depois no schema, avisa e ignora
          somente aquela FK na etapa de saneamento.

    Formatos aceitos:
        {
            "tabela_filha": {
                "pk_cols": ["ID_FILHO"],
                "foreign_keys": [
                    {
                        "columns": ["ID_PAI"],
                        # parent_table e parent_columns sao opcionais
                        "parent_table": "tabela_pai",
                        "parent_columns": ["ID_PAI"]
                    }
                ]
            }
        }
    """
    if not isinstance(specs_config, ABCMapping) or not specs_config:
        raise ValueError("`specs_config` deve ser um dicionário não vazio.")

    if relationship_policy not in ("warn_and_skip", "raise"):
        raise ValueError("relationship_policy deve ser 'warn_and_skip' ou 'raise'.")

    postprocess_by_table = dict(postprocess_by_table or {})
    specs: Dict[str, TableSpec] = {}

    for name, cfg in specs_config.items():
        name = str(name).strip()

        if not isinstance(cfg, ABCMapping):
            raise TypeError(
                f"Config da tabela `{name}` deve ser um dict, recebido {type(cfg)!r}."
            )

        # PK é estrutural para a geração. Sem PK não é seguro sintetizar.
        pk_cols = _normalize_cols(
            cfg.get("pk_cols"),
            field_name="pk_cols",
            table_name=name,
        )

        raw_fks = cfg.get("foreign_keys") or cfg.get("fks") or []

        if isinstance(raw_fks, ABCMapping):
            raw_fks = [raw_fks]

        if not isinstance(raw_fks, (list, tuple)):
            _warn_or_raise(
                f"Tabela `{name}`: `foreign_keys` deveria ser lista/tupla de dicts, "
                f"mas veio {type(raw_fks).__name__}. Todas as FKs dessa tabela serão ignoradas.",
                policy=relationship_policy,
            )
            raw_fks = []

        fks: List[ForeignKeySpec] = []

        for i, fk in enumerate(raw_fks):
            if isinstance(fk, ForeignKeySpec):
                # Se vier objeto pronto e completo, mantém.
                # Se estiver incompleto, saneamento posterior trata.
                fks.append(fk)
                continue

            if not isinstance(fk, ABCMapping):
                _warn_or_raise(
                    f"Relacionamento ignorado em `{name}` FK #{i}: esperado dict, recebido {fk!r}.",
                    policy=relationship_policy,
                )
                continue

            # columns continua necessário para saber qual coluna da filha participaria da FK.
            cols = _try_normalize_cols(
                fk.get("columns"),
                field_name="foreign_keys.columns",
                table_name=name,
                fk_index=i,
            )
            if not cols:
                _warn_or_raise(
                    f"Relacionamento ignorado em `{name}` FK #{i}: `columns` não foi informado. "
                    "A tabela será sintetizada sem essa FK.",
                    policy=relationship_policy,
                )
                continue

            raw_parent_table = fk.get("parent_table")
            parent_table = str(raw_parent_table).strip() if raw_parent_table is not None else ""

            parent_cols = _try_normalize_cols(
                fk.get("parent_columns"),
                field_name="foreign_keys.parent_columns",
                table_name=name,
                fk_index=i,
            ) if fk.get("parent_columns") is not None else None

            # Se parent_columns não foi informado, mas parent_table existe, usa pk_cols do pai.
            if parent_cols is None and parent_table:
                parent_cfg = specs_config.get(parent_table)
                if isinstance(parent_cfg, ABCMapping) and parent_cfg.get("pk_cols") is not None:
                    parent_cols = _normalize_cols(
                        parent_cfg.get("pk_cols"),
                        field_name="pk_cols",
                        table_name=parent_table,
                    )
                    warnings.warn(
                        f"Relacionamento `{name}` FK #{i}: `parent_columns` não informado. "
                        f"Usando pk_cols da tabela pai `{parent_table}`: {list(parent_cols)}.",
                        UserWarning,
                        stacklevel=2,
                    )
                else:
                    _warn_or_raise(
                        f"Relacionamento ignorado em `{name}` FK #{i}: `parent_columns` ausente "
                        f"e não foi possível obter pk_cols do parent_table `{parent_table}`.",
                        policy=relationship_policy,
                    )
                    continue

            # Se parent_table não foi informado, tenta inferir por parent_columns ou columns.
            if not parent_table:
                inferred_parent, inferred_parent_cols, reason = _infer_parent_table_from_config(
                    specs_config,
                    child_table=name,
                    parent_columns=parent_cols,
                    child_fk_columns=cols,
                )

                if inferred_parent and inferred_parent_cols:
                    parent_table = inferred_parent
                    parent_cols = inferred_parent_cols
                    warnings.warn(
                        f"Relacionamento `{name}` FK #{i}: `parent_table` não informado. {reason}.",
                        UserWarning,
                        stacklevel=2,
                    )
                else:
                    _warn_or_raise(
                        f"Relacionamento ignorado em `{name}` FK #{i}: {reason}. "
                        "A tabela será sintetizada sem essa FK.",
                        policy=relationship_policy,
                    )
                    continue

            if parent_cols is None:
                _warn_or_raise(
                    f"Relacionamento ignorado em `{name}` FK #{i}: `parent_columns` não informado "
                    "e não foi possível inferir. A tabela será sintetizada sem essa FK.",
                    policy=relationship_policy,
                )
                continue

            fks.append(
                ForeignKeySpec(
                    columns=tuple(cols),
                    parent_table=parent_table,
                    parent_columns=tuple(parent_cols),
                )
            )

        specs[name] = TableSpec(
            name=name,
            pk_cols=pk_cols,
            foreign_keys=tuple(fks),
            static=bool(cfg.get("static", False)),
            postprocess=postprocess_by_table.get(name),
        )

    return specs


def build_specs_from_config(
    specs_config: Mapping[str, Mapping],
    postprocess_by_table: Optional[Mapping[str, PostProcessor]] = None,
    *,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
) -> Dict[str, TableSpec]:
    return _build_specs_from_config(
        specs_config,
        postprocess_by_table,
        relationship_policy=relationship_policy,
    )


def _normalize_save_path(save_path: str) -> str:
    """
    Normaliza o caminho de saída:
        - expande "~";
        - remove "/" final para evitar caminhos com "//";
        - mantém esquemas remotos (oci://, s3://, hdfs://, dbfs:/) intactos.
    """
    path = str(save_path).strip()

    has_scheme = "://" in path or path.startswith("dbfs:/")

    if not has_scheme:
        path = os.path.expanduser(path)

    while len(path) > 1 and path.endswith("/"):
        path = path[:-1]

    return path


def _is_local_path(path: str) -> bool:
    return "://" not in path and not path.startswith("dbfs:/")


_INVALID_COL_CHARS_PATTERN = re.compile(r"[ ,;{}()\n\t=]")


def _sanitize_columns_for_save(df: DataFrame, table_name: str) -> DataFrame:
    """
    Renomeia colunas com caracteres inválidos para escrita em Parquet
    (espaço, vírgula, ponto-e-vírgula, chaves, parênteses, '=', tab, newline).

    Cada caractere inválido vira "_". Se houver colisão de nomes após o
    rename, adiciona sufixo numérico. Emite warning listando os renames,
    para o rename ficar auditável.
    """
    renames: List[Tuple[str, str]] = []
    new_names: List[str] = []
    used: set = set()

    for col_name in df.columns:
        new_name = _INVALID_COL_CHARS_PATTERN.sub("_", col_name)

        if not new_name.strip():
            new_name = "col"

        base = new_name
        suffix = 1
        while new_name in used:
            new_name = f"{base}_{suffix}"
            suffix += 1

        used.add(new_name)
        new_names.append(new_name)

        if new_name != col_name:
            renames.append((col_name, new_name))

    if not renames:
        return df

    warnings.warn(
        f"Tabela `{table_name}`: colunas renomeadas para gravação por conterem "
        f"caracteres inválidos para Parquet: {renames}.",
        UserWarning,
        stacklevel=2,
    )

    return df.toDF(*new_names)


def _save_hint_for_error(exc: Exception, out_path: str) -> str:
    """
    Gera dica prática conforme o tipo de erro de gravação.
    """
    msg = str(exc)
    hints: List[str] = []

    lowered = msg.lower()

    if "permission" in lowered or "denied" in lowered or "errno 13" in lowered:
        hints.append(
            "Parece falta de permissão de escrita. Caminhos como '/csv/' apontam "
            "para a RAIZ do filesystem; use um caminho relativo (ex.: './csv') ou "
            "absoluto dentro do seu usuário (ex.: '~/csv' ou '/home/usuario/csv')."
        )

    if "invalid character" in lowered or "attribute name" in lowered:
        hints.append(
            "Nome de coluna inválido para o formato. A sanitização automática "
            "deveria ter tratado; verifique se há colunas com caracteres exóticos."
        )

    if "already exists" in lowered:
        hints.append(
            "O destino já existe e o modo de escrita não permitiu sobrescrever."
        )

    if "winutils" in lowered or "hadoop_home" in lowered:
        hints.append(
            "Ambiente Windows sem winutils.exe/HADOOP_HOME configurado. "
            "Configure o winutils compatível com a versão do Hadoop do Spark."
        )

    if not hints:
        hints.append(
            f"Verifique se o diretório pai de `{out_path}` existe e se o processo "
            "do Spark tem permissão de escrita nele."
        )

    return " ".join(hints)


def run_synthesis_from_tables(
    tables: Mapping[str, DataFrame],
    specs_config: Mapping[str, Mapping],
    *,
    n_rows_by_table: Optional[Mapping[str, int]] = None,
    scale_factor: Optional[float] = None,
    seed: int = 42,
    append_after_max_pk: bool = True,
    pk_max_by_table: Optional[Mapping[str, int]] = None,
    engorda_ts: Optional[datetime] = None,
    dt_vencimento_prazo_dias_by_table: Optional[Mapping[str, int]] = None,
    default_dt_vencimento_prazo_dias: int = DEFAULT_DT_VENCIMENTO_PRAZO_DIAS,
    validate_mode: ValidateMode = "full",
    nullable_fk_policy: NullableFkPolicy = "allow_any_null",
    broadcast_fk_counts: bool = False,
    storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    postprocess_by_table: Optional[Mapping[str, PostProcessor]] = None,
    save_path: Optional[str] = None,
    save_format: Literal["csv", "parquet"] = "parquet",
    save_options: Optional[Mapping[str, object]] = None,
    save_single_file: bool = False,
    save_error_policy: SaveErrorPolicy = "warn_and_continue",
    save_mode: str = "overwrite",
    verbose: bool = True,
    relationship_policy: RelationshipPolicy = "warn_and_skip",
    check_relationship_values: bool = True,
    truncate_lineage: bool = False,
) -> Dict[str, DataFrame]:
    """
    Runner para quando os DataFrames já estão carregados.

    v4: agora também aceita save_path/save_format (e opções de gravação),
    com gravação robusta igual à de run_synthesis_from_paths.

    v5: aceita `save_mode` e `oci`. Se `oci` for um dict (ex.: {"auth": "config_file"}),
    configura o conector OCI no Spark antes de gravar. Para Data Flow/resource
    principal já ativo, pode passar oci={"auth": "none"} ou simplesmente omitir.
    """
    specs = _build_specs_from_config(
        specs_config,
        postprocess_by_table,
        relationship_policy=relationship_policy,
    )

    specs = _sanitize_specs_against_known_tables(
        specs,
        tables,
        relationship_policy=relationship_policy,
    )

    specs = _validate_relationship_columns(
        tables,
        specs,
        relationship_policy=relationship_policy,
        nullable_fk_policy=nullable_fk_policy,
        check_relationship_values=check_relationship_values,
    )

    if n_rows_by_table is None:
        effective_n_rows: Dict[str, int] = {}
        for name in specs:
            base = tables[name].count()
            if specs[name].static:
                effective_n_rows[name] = base
            elif scale_factor:
                effective_n_rows[name] = int(round(base * scale_factor))
            else:
                effective_n_rows[name] = base
    else:
        effective_n_rows = dict(n_rows_by_table)

    if verbose:
        print("Ordem topológica:", " -> ".join(_topological_order(specs)))
        print("n_rows_by_table:", effective_n_rows)

    synthetic = synthesize_multitable_spark(
        tables=tables,
        specs=specs,
        n_rows_by_table=effective_n_rows,
        seed=seed,
        append_after_max_pk=append_after_max_pk,
        pk_max_by_table=pk_max_by_table,
        engorda_ts=engorda_ts,
        dt_vencimento_prazo_dias_by_table=dt_vencimento_prazo_dias_by_table,
        default_dt_vencimento_prazo_dias=default_dt_vencimento_prazo_dias,
        validate_mode=validate_mode,
        nullable_fk_policy=nullable_fk_policy,
        broadcast_fk_counts=broadcast_fk_counts,
        storage_level=storage_level,
        verbose=verbose,
        relationship_policy=relationship_policy,
        check_relationship_values=False,
        truncate_lineage=truncate_lineage,
    )

    if save_path:
        save_synthetic_tables(
            synthetic,
            save_path,
            save_format=save_format,
            save_options=save_options,
            save_single_file=save_single_file,
            save_error_policy=save_error_policy,
            save_mode=save_mode,          # <-- ADD THIS LINE (upstream omits it)
            verbose=verbose,
        )

    return synthetic


def save_synthetic_tables(
    synthetic: Mapping[str, DataFrame],
    save_path: str,
    *,
    save_format: Literal["csv", "parquet"] = "parquet",
    save_options: Optional[Mapping[str, object]] = None,
    save_single_file: bool = False,
    save_error_policy: SaveErrorPolicy = "warn_and_continue",
    save_mode: str = "overwrite",
    verbose: bool = True,
) -> Dict[str, str]:
    """
    Grava as tabelas sintéticas em disco de forma resiliente.

    save_mode (NOVO na v5): modo de escrita do Spark por tabela.
        "overwrite" (default) substitui o diretório da tabela.
        "append" acrescenta; "ignore" não grava se já existir; "errorifexists".
        Obs.: o `existing_data_behavior="overwrite_or_ignore"` do pyarrow não tem
        equivalente exato no Spark — o mais próximo de "sobrescrever sempre" é
        "overwrite". Funciona com destinos locais e oci:// igualmente.

    Comportamento:
        - Normaliza o caminho e cria o diretório base se for filesystem local.
        - Sanitiza nomes de coluna inválidos para Parquet (warning auditável).
        - Cada tabela é gravada dentro de try/except: a falha de UMA tabela não
          impede a gravação das demais.
        - Ao final, se houve falhas:
            save_error_policy="warn_and_continue": emite warning com resumo;
            save_error_policy="raise": levanta RuntimeError com resumo.

    Retorna:
        dict {tabela: caminho_gravado} apenas com as tabelas gravadas com sucesso.
    """
    if save_error_policy not in ("warn_and_continue", "raise"):
        raise ValueError(
            "save_error_policy deve ser 'warn_and_continue' ou 'raise'."
        )

    fmt = (save_format or "parquet").lower()
    base_path = _normalize_save_path(save_path)
    options = dict(save_options or {})

    # Em filesystem local, garante que o diretório base exista e detecta
    # problemas de permissão ANTES de disparar jobs Spark.
    if _is_local_path(base_path):
        try:
            os.makedirs(base_path, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(
                f"Não foi possível criar o diretório de saída `{base_path}`: {exc}. "
                + _save_hint_for_error(exc, base_path)
            ) from exc

    saved: Dict[str, str] = {}
    failures: List[Tuple[str, str, str]] = []  # (tabela, caminho, erro+dica)

    for name, df in synthetic.items():
        out_path = f"{base_path}/{name}"

        try:
            # CSV aceita espaço/parênteses no header; só sanitiza nos formatos
            # que proíbem (parquet/orc/etc.), preservando os nomes originais
            # do metadado na saída CSV.
            df_out = df if fmt == "csv" else _sanitize_columns_for_save(df, name)

            if save_single_file:
                df_out = df_out.coalesce(1)

            writer = df_out.write.mode(save_mode)

            for k, v in options.items():
                writer = writer.option(k, v)

            if fmt == "csv":
                writer.option("header", options.get("header", True)).csv(out_path)
            elif fmt == "parquet":
                writer.parquet(out_path)
            else:
                writer.format(fmt).save(out_path)

            saved[name] = out_path

            if verbose:
                print(f"[salvo] {name} -> {out_path} ({fmt})")

        except Exception as exc:
            hint = _save_hint_for_error(exc, out_path)
            failures.append((name, out_path, f"{exc} | Dica: {hint}"))

            if verbose:
                print(f"[FALHA ao salvar] {name} -> {out_path}: {exc}")

    if failures:
        resumo = "; ".join(
            f"`{name}` em `{path}`: {erro}" for name, path, erro in failures
        )
        mensagem = (
            f"Falha ao gravar {len(failures)} de {len(synthetic)} tabela(s) "
            f"em `{base_path}` (formato {fmt}): {resumo}"
        )

        if save_error_policy == "raise":
            raise RuntimeError(mensagem)

        warnings.warn(mensagem, UserWarning, stacklevel=2)

    elif verbose and saved:
        print(f"Dados sintéticos salvos em: {base_path} ({len(saved)} tabela(s), formato {fmt})")

    return saved



def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def raw_path(config: dict[str, str], table: str) -> str:
    parts = [config["DATAGEN_RAW_BASE_URI"]]
    if config.get("DATAGEN_RAW_PREFIX"):
        parts.append(config["DATAGEN_RAW_PREFIX"])
    parts.append(table_path_name(table))
    return "/".join(parts)


def synthetic_base_path(config: dict[str, str]) -> str:
    base = config["DATAGEN_SYNTHETIC_BASE_URI"]
    prefix = config.get("DATAGEN_SYNTHETIC_PREFIX")
    return f"{base}/{prefix}" if prefix else base


def get_engorda_env() -> dict[str, str]:
    config: dict[str, str] = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        sys.exit(1)
    config["DATAGEN_RAW_PREFIX"] = os.environ.get("DATAGEN_RAW_PREFIX", "").strip("/")
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", ""
    ).strip("/")
    return config


def _fk_identidade_degenerada(table: str, fk: dict) -> bool:
    """True para FK auto-referente IDENTIDADE: mesma tabela e cada coluna
    apontando para si mesma (ex.: CONTA_PARTICIPANTE.NUM_CONTA_PARTICIPANTE_
    CETIP -> CONTA_PARTICIPANTE.NUM_CONTA_PARTICIPANTE_CETIP). Trivialmente
    satisfeita por construção (todo valor existe na própria coluna) — é
    artefato de geração de spec, não um relacionamento. Removida em
    normalize_specs para não gerar warning nem trabalho em nenhum consumidor.
    """
    if fk.get("parent_table") != table:
        return False
    cols = list(fk.get("columns") or [])
    pcols = list(fk.get("parent_columns") or [])
    return bool(cols) and len(cols) == len(pcols) and all(
        c == p for c, p in zip(cols, pcols))


def normalize_specs(specs: dict) -> dict:
    out: dict = {}
    for raw_name, cfg in specs.items():
        name = table_path_name(str(raw_name))
        if name in out:
            raise ValueError(
                f"Spec key collision after schema stripping: `{raw_name}` reduces to "
                f"`{name}`, which is already present."
            )
        new_cfg = copy.deepcopy(dict(cfg))
        for fk_key in ("foreign_keys", "fks"):
            fks = new_cfg.get(fk_key)
            if not isinstance(fks, (list, tuple)):
                continue
            for fk in fks:
                if isinstance(fk, dict) and fk.get("parent_table"):
                    fk["parent_table"] = table_path_name(str(fk["parent_table"]))
            # FKs auto-referentes identidade são spec-lixo: satisfeitas por
            # construção, só geravam o warning "self-reference não é
            # suportado" e joins inúteis nos passes de órfãos. Removidas
            # aqui, ANTES de qualquer consumidor (referential_sample,
            # sanitização de síntese, null_orphan_fks).
            filtradas = [fk for fk in fks
                         if not (isinstance(fk, dict)
                                 and _fk_identidade_degenerada(name, fk))]
            if len(filtradas) != len(fks):
                logger.info(
                    "normalize_specs: %d FK(s) identidade auto-referente(s) "
                    "removida(s) de %s (trivialmente satisfeitas).",
                    len(fks) - len(filtradas), name)
            new_cfg[fk_key] = filtradas
        out[name] = new_cfg
    return out


def connected_components(specs: dict) -> list[list[str]]:
    parent: dict[str, str] = {t: t for t in specs}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        parent[find(a)] = find(b)

    for table, cfg in specs.items():
        for fk_key in ("foreign_keys", "fks"):
            for fk in cfg.get(fk_key) or []:
                if not isinstance(fk, dict):
                    continue
                p = fk.get("parent_table")
                if p in specs:
                    union(table, p)

    groups: dict[str, list[str]] = {}
    for table in specs:
        groups.setdefault(find(table), []).append(table)
    return [sorted(g) for g in groups.values()]


def _fk_parent_tables(specs: dict) -> set[str]:
    parents: set[str] = set()
    for cfg in specs.values():
        for fk_key in ("foreign_keys", "fks"):
            for fk in cfg.get(fk_key) or []:
                if isinstance(fk, dict) and fk.get("parent_table") in specs:
                    parents.add(fk["parent_table"])
    return parents


def _fk_list(cfg: dict) -> list[dict]:
    fks = cfg.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = cfg.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def _not_null_cols(cfg: dict) -> set[str]:
    """Colunas NOT NULL declaradas no spec (gera_spec_config lê do cols_real).

    Vazio quando o spec foi gerado sem cols_real.csv -> o engorda cai no
    comportamento antigo (anula FK órfã sempre), que pode violar NOT NULL no
    append. Com a lista presente, uma FK órfã cuja coluna é NOT NULL é DROPADA
    em vez de anulada (ver neutraliza_orfaos_na_fonte / null_orphan_fks).
    """
    raw = cfg.get("not_null_cols") or []
    return {str(c) for c in raw if isinstance(c, str)}


def _warn_filtros_fonte_sem_not_null(specs: dict) -> None:
    """Alerta quando os filtros do produto podem gerar ORA-01400 silencioso.

    Os predicados de FILTROS_FONTE podem remover uma linha-PAI que uma filha
    referencia (ex.: CONDICAO_IF com DAT_EXCLUSAO não-nula, referida por um
    RESGATE válido). Como o fecho de pais lê a fonte JÁ FILTRADA, o pai não é
    re-injetado e a filha vira órfã. A neutralização então DROPA a linha se a
    FK for NOT NULL — MAS só sabe que é NOT NULL se `not_null_cols` estiver no
    spec (gerado com cols_real.csv). Sem essa lista, a FK órfã é ANULADA e, se
    a coluna for NOT NULL no Oracle, o append falha com ORA-01400 sem que
    assert_not_null_ok consiga barrar (ele também depende de not_null_cols).

    Portanto: quando há tabela de FILTROS_FONTE no run mas alguma tabela que a
    referencia não tem `not_null_cols`, emite um WARNING claro de que
    cols_real.csv passou a ser efetivamente obrigatório para este produto.
    """
    filtradas = {t for t in FILTROS_FONTE if t in specs}
    if not filtradas:
        return
    afetadas = []
    for table, cfg in specs.items():
        parents = {fk.get("parent_table") for fk in _fk_list(cfg)}
        if parents & filtradas and not _not_null_cols(cfg):
            afetadas.append(table)
    if afetadas:
        logger.warning(
            "FILTROS_FONTE ativos em %s, mas estas tabelas que as referenciam "
            "NÃO têm not_null_cols no spec: %s. Um pai removido pelo filtro "
            "torna a filha órfã; sem not_null_cols a FK órfã é ANULADA (não "
            "dropada) e pode violar NOT NULL no append (ORA-01400) sem ser "
            "barrada por assert_not_null_ok. Gere o spec COM cols_real.csv "
            "(ver gera_spec_config.py) para este produto.",
            sorted(filtradas), sorted(afetadas))


def _fk_is_whole_pk(pk_cols: list[str], fk: dict) -> bool:
    """True when a FK's columns are exactly the child's primary key.

    These are 1:1 "shared-key" extension tables (e.g. JUROS_FLUTUANTE keyed by
    NUM_CONDICAO_IF, which is also its FK to CONDICAO_IF). The synthesizer's FK
    remap can leave such a column NULL or non-unique; bind_shared_key_children
    rebinds it to distinct parent keys instead.
    """
    cols = fk.get("columns") or []
    return bool(pk_cols) and bool(cols) and sorted(cols) == sorted(pk_cols)


# FKs de AUDITORIA (quem incluiu/alterou o registro) apontam quase o schema
# inteiro para USUARIO, e USUARIO -> ENTIDADE -> USUARIO fecha ciclo: o SCC
# resultante engoliu ~metade das tabelas do CDB e forçava
# _toposort_break_cycles a quebrar arestas ESTRUTURAIS arbitrariamente —
# fecho/neutralização/null_orphan processavam filha ANTES do pai (caso real:
# CONTA_PARTICIPANTE neutralizada às 18:12:22, PARTICIPANTE dropada às
# 18:12:38 -> 9.751 órfãs re-criadas). As arestas para estes pais continuam
# NEUTRALIZADAS normalmente (anula/dropa/rebind); só não participam da
# ORDENAÇÃO topológica.
PARENTS_FORA_DA_ORDENACAO: frozenset = frozenset({"USUARIO"})


def topo_order_tables(comp_specs: dict) -> list[str]:
    """Order a component's tables so every parent comes before its children.

    Used by referential sampling (sample parents first, then keep only children
    whose FK lands in the sampled parents). Self-references are ignored; cycles
    are broken arbitrarily so the function always returns every table once.

    Arestas cujo pai está em PARENTS_FORA_DA_ORDENACAO (FKs de auditoria) não
    entram no grafo de dependências: elas criavam um SCC gigante e a quebra
    arbitrária de ciclo invalidava a garantia pai-antes-da-filha justamente
    nas arestas estruturais. Órfãos nessas arestas seguem tratados pelos
    passes de neutralização (que agora rodam a ponto fixo).
    """
    deps: dict[str, set[str]] = {t: set() for t in comp_specs}
    for table, cfg in comp_specs.items():
        for fk in _fk_list(cfg):
            parent = fk.get("parent_table")
            if (parent in comp_specs and parent != table
                    and parent not in PARENTS_FORA_DA_ORDENACAO):
                deps[table].add(parent)

    return _toposort_break_cycles(deps)


def effective_n_rows(
    specs: dict, source_counts: dict[str, int], scale_factor: float
) -> dict[str, int]:
    parents = _fk_parent_tables(specs)
    targets: dict[str, int] = {}
    for table, cfg in specs.items():
        count = int(source_counts[table])
        static = bool(cfg.get("static", False))
        override = cfg.get("n_rows")
        if count == 0:
            target = 0
        elif static:
            target = count  # static is terminal; override ignored (see warn in engorda)
        elif override is not None:
            target = int(override)
        else:
            target = int(round(count * scale_factor))
        if not static and count > 0 and table in parents:
            target = max(target, count)  # parent floor: keep_all_source_rows needs target >= count
        targets[table] = target
    return targets


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic relational Parquet from ingested raw Parquet."
    )
    parser.add_argument("--scale-factor", type=float, default=DEFAULT_SCALE_FACTOR,
                        help="Global row-count multiplier for non-static tables.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help="Synthesis seed.")
    parser.add_argument("--continue-on-error", action="store_true",
                        help="Continue with remaining components after a failure, "
                             "then exit non-zero.")
    parser.add_argument("--limit", type=positive_int, default=None,
                        help="Sample at most this many rows per table for a fast test run. "
                             "Sampling is referential (parents first, children kept only when "
                             "their FK lands in a sampled parent), so FKs stay consistent — but "
                             "child counts come out smaller than the limit. Omit for full data.")
    parser.add_argument("--pk-offset", type=positive_int, default=None,
                        help="Floor for synthetic PK starts. By default engorda reads each table's "
                             "TRUE max(pk) from the full Parquet and generates PKs as true_max+1, "
                             "etc (safe with --limit, collision-free vs the real table). Pass "
                             "--pk-offset N to start at max(true_max, N) instead, e.g. to reserve "
                             "a band well above all real PKs. FKs are remapped to match.")
    parser.add_argument("--pk-safety-band", type=positive_int, default=None,
                        help="Safety gap added above each table's true max(pk): synthetic PKs "
                             "start at true_max + band + 1. Leaves headroom so the real table can "
                             "grow between the max read and the load without colliding. Default: "
                             "no gap (start right after true_max).")
    parser.add_argument("--dt-vencimento-prazo-dias", type=positive_int, default=None,
                        help="Prazo fixo em dias para DAT_VENCIMENTO = data da engorda + X. "
                             "Se omitido, preserva o prazo original da linha quando possível; "
                             "se o prazo original for inválido, usa 365 dias.")
    parser.add_argument("--specs", default=None,
                        help="Override DATAGEN_SPECS_URI (URI of a single specs.json object).")
    parser.add_argument("--debug", action="store_true", default=DEBUG_ENABLED,
                        help="Modo debug: sobe o log para DEBUG e emite um relatório de "
                             "integridade de FK (nulos + órfãos por coluna) ENTRE cada estágio "
                             "do pipeline (amostra -> fecho -> neutralização -> síntese -> bind "
                             "-> null_orphan), para localizar em que estágio uma coluna começa a "
                             "ter nulos/órfãos. Também ligável via DATAGEN_DEBUG=1. Dispara "
                             "actions Spark extras; use em runs de diagnóstico (idealmente com "
                             "--limit).")
    return parser.parse_args()


def read_parquet(spark: SparkSession, path: str, limit: int | None = None) -> DataFrame:
    df = spark.read.parquet(path)
    return df.limit(limit) if limit is not None else df


def _aplica_filtros_fonte(df: DataFrame, table: str) -> DataFrame:
    """Aplica os predicados de fonte do CDB simplificado (FILTROS_FONTE[table]).

    PRIMEIRA etapa do pipeline: recorta a fonte de cada tabela para o produto
    ANTES de amostragem/propagação/síntese. Os predicados de uma tabela são
    combinados em AND. Um predicado cuja coluna não exista no schema é
    ignorado (defensivo): o produto é definido pelas colunas presentes, e uma
    coluna ausente não deve zerar a tabela nem quebrar a leitura.

    Operadores suportados (ver FILTROS_FONTE):
      "=="     -> col == valor              (igualdade exata, tipada)
      ">"      -> col > valor               (maior que; linha com col NULL sai)
      "ieq"    -> upper(trim(col)) == valor (string case/space-insensitive)
      "isnull" -> col IS NULL

    NÃO é usado por compute_pk_maxes: o max(pk) precisa da tabela inteira para
    que as PKs sintéticas não colidam com linhas de produção de OUTROS
    registros (fora do produto). Ver comentário no topo do arquivo.
    """
    preds = FILTROS_FONTE.get(table)
    if not preds:
        return df
    cols = set(df.columns)
    for col, op, valor in preds:
        if col not in cols:
            logger.warning(
                "_aplica_filtros_fonte: coluna %s ausente em %s; predicado "
                "(%s %s %r) ignorado", col, table, col, op, valor)
            continue
        if op == "isnull":
            df = df.where(F.col(col).isNull())
        elif op == "ieq":
            df = df.where(F.upper(F.trim(F.col(col))) == F.lit(valor))
        elif op == "==":
            df = df.where(F.col(col) == F.lit(valor))
        elif op == ">":
            # NULL > valor é NULL (não TRUE) -> linha com col NULL é descartada,
            # que é a semântica desejada de "maior que zero".
            df = df.where(F.col(col) > F.lit(valor))
        else:
            raise ValueError(
                f"_aplica_filtros_fonte: operador desconhecido {op!r} "
                f"para {table}.{col}")
    if _dbg():
        # Só a contagem PÓS-filtro (um único scan, com pushdown). Confere o
        # efeito de cada predicado — em especial o match de string 'SEM
        # TABELA' em RESGATE: se vier 0, suspeite de caixa/espaço no dado.
        # NÃO contamos o total PRÉ-filtro de propósito: seria um segundo scan
        # da tabela INTEIRA sem pushdown, caro em tabelas de 50M-1B linhas e
        # repetido a cada chamada de _read_source (inclusive dentro do loop de
        # fecho de pais em completa_pais_referenciados).
        logger.debug(
            "[DEBUG filtros_fonte] %s: %d linha(s) após predicados %s",
            table, df.count(),
            [(c, o, v) for c, o, v in preds])
    return df


def _read_source(spark, config, table: str, limit: int | None = None) -> DataFrame:
    """Leitura ÚNICA e canônica da fonte de uma tabela, já filtrada pelo produto.

    Todo ponto que consome dados de produção como FONTE de síntese
    (referential_sample e o fecho de pais em completa_pais_referenciados) deve
    ler por aqui — nunca por read_parquet direto — para que os predicados de
    FILTROS_FONTE valham em TODAS as etapas e nenhum passo consiga re-injetar
    uma linha fora do produto. A única exceção deliberada é compute_pk_maxes,
    que precisa do max(pk) da tabela inteira (ver topo do arquivo).
    """
    return _aplica_filtros_fonte(
        read_parquet(spark, raw_path(config, table), limit), table)


def _remove_linhas_pk_nula(df: DataFrame, cfg: dict, table: str) -> DataFrame:
    """Remove linhas com coluna de PK nula/vazia — artefato de extração.

    No Oracle uma PK é NOT NULL por definição, então linha sem PK completa
    não existe na origem real: é lixo do processo de extração (ex.: linha em
    branco de um CSV->Parquet). Ela não é apendável (ORA-01400), não é
    referenciável por FK e, numa tabela static, seria copiada como está para
    a saída (caso real: VEICULO_GARANTIDOR com 2 linhas na fonte, uma delas
    toda nula/vazia, reprovada no pre-append check). Para coluna string vale
    a semântica do Oracle: '' também conta como NULL.
    """
    pk_cols = [c for c in (cfg.get("pk_cols") or []) if _has_column(df, c)]
    if not pk_cols:
        return df
    preds = []
    for c in pk_cols:
        ok = F.col(c).isNotNull()
        if _is_string_type(_get_field_type(df, c)):
            ok = ok & (F.trim(F.col(c)) != F.lit(""))
        preds.append(ok)
    filtrado = df.where(reduce(lambda a, b: a & b, preds))
    if _dbg():
        antes, depois = df.count(), filtrado.count()
        if antes != depois:
            logger.debug(
                "[DEBUG pk_nula] %s: %d linha(s) com PK nula/vazia removida(s) "
                "na leitura da fonte.", table, antes - depois)
    return filtrado


def _repara_not_null_origem(df: DataFrame, cfg: dict, table: str) -> DataFrame:
    """Preenche, na leitura da fonte, colunas NOT NULL não-FK efetivamente nulas.

    Caso real: lookups static trazem da extração uma linha com PK VÁLIDA mas
    NULL/'' numa coluna que o Oracle declara NOT NULL (DETENTOR_IF.
    NOM_DETENTOR_IF, MOTIVO_SITUACAO_IF.COD/DES_MOTIVO_SITUACAO_IF,
    UNIDADE_MEDIDA.COD_UNIDADE_MEDIDA). Essa combinação não existe na origem
    real (o Oracle guarda '' como NULL e a coluna é NOT NULL): é artefato de
    extração, primo da linha de PK nula de _remove_linhas_pk_nula. Sem
    conserto, a linha atravessa o pipeline intacta (nenhum passe de FK a toca)
    e assert_not_null_ok aborta o componente — rodar de novo falha igual.

    DROPAR a linha (como se faz com PK nula) seria PIOR que preencher: a PK é
    válida e pode ser referenciada por FK NOT NULL de tabela-alvo (ex.:
    INSTRUMENTO_FINANCEIRO.NUM_ID_MOTIVO_SITUACAO_IF -> MOTIVO_SITUACAO_IF);
    o pai dropado tornaria essas filhas órfãs e a neutralização as DROPARIA em
    cascata (linhas VÁLIDAS do domínio perdidas). Preencher preserva a linha,
    a identidade da PK e toda a integridade referencial.

    Escopo deliberado (só o que nenhum outro passe cobre):
      - PK fica fora — linha de PK nula é dropada por _remove_linhas_pk_nula;
      - coluna de FK fica fora — NULL em FK é assunto dos passes de órfão
        (fecho/neutralização/rebind); inventar uma referência aqui criaria
        órfão novo;
      - só colunas declaradas em not_null_cols (spec gerado com cols_real.csv)
        e presentes no DataFrame.

    Valor de preenchimento: o MENOR valor válido do tipo, porque o spec não
    conhece a largura física da coluna no Oracle — string vira "0" (1 char:
    cabe em qualquer CHAR/VARCHAR2, inclusive indicadores CHAR(1), sem risco
    de ORA-12899; exceção: DAT_* fisicamente string recebe data formatada),
    número vira 0, data/timestamp viram o instante corrente (coerente com a
    data de engorda). Tipos além desses ficam como estão e
    assert_not_null_ok continua barrando, apontando para cá.

    Custo: projeção pura (um when/otherwise por coluna reparável), sem scan
    nem agg extra; contagem de reparos só sob --debug.
    """
    pk_set = set(cfg.get("pk_cols") or [])
    fk_cols = {c for fk in _fk_list(cfg) for c in (fk.get("columns") or [])}
    alvo = [c for c in sorted(_not_null_cols(cfg))
            if _has_column(df, c) and c not in pk_set and c not in fk_cols]
    if not alvo:
        return df
    if _dbg():
        nulos = _null_efetivo_counts(df, alvo)
        for c, n in nulos.items():
            if n:
                logger.debug(
                    "[DEBUG repara_not_null] %s.%s: %d valor(es) efetivamente "
                    "nulo(s) (NULL/'') preenchido(s) na leitura da fonte.",
                    table, c, n)
    for c in alvo:
        dt = _get_field_type(df, c)
        col = F.col(c)
        if _is_string_type(dt):
            eff_null = col.isNull() | (F.trim(col) == F.lit(""))
            if c.startswith("DAT_"):
                # Coluna de data fisicamente STRING (CSV lido com inferSchema):
                # "0" quebraria o append com ORA-01858; usa o mesmo formato de
                # _timestamp_literal_for_type para coluna string.
                fill = F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
            else:
                fill = F.lit("0")
        elif _is_numeric_pk_type(dt):
            eff_null = col.isNull()
            fill = F.lit(0).cast(dt)
        elif isinstance(dt, T.DateType):
            eff_null = col.isNull()
            fill = F.current_date()
        elif isinstance(dt, T.TimestampType):
            eff_null = col.isNull()
            fill = F.current_timestamp()
        else:
            continue  # tipo não reparável -> assert_not_null_ok segue barrando
        df = df.withColumn(c, F.when(eff_null, fill).otherwise(col))
    return df


def _saneia_fonte(df: DataFrame, cfg: dict, table: str) -> DataFrame:
    """Saneamento canônico da fonte já filtrada, num único ponto de entrada:
    (1) remove linhas de PK nula/vazia e (2) preenche colunas NOT NULL não-FK
    efetivamente nulas — os dois artefatos de extração que tornariam a linha
    não-apendável (ORA-01400). Todo ponto que consome fonte para amostra/fecho
    deve ler por aqui, para que síntese, bootstrap e cópia static enxerguem a
    MESMA fonte saneada."""
    return _repara_not_null_origem(
        _remove_linhas_pk_nula(df, cfg, table), cfg, table)


def _read_pk_max(spark, path: str, pk_col: str):
    """max(pk_col) from the full Parquet at `path` (footer-fast with pushdown)."""
    row = read_parquet(spark, path).agg(F.max(F.col(pk_col))).first()
    return row[0] if row is not None else None


def _pk_capacity(spark, path: str, pk_col: str):
    """Largest integer the PK column's type can hold (None for string/unknown)."""
    dt = read_parquet(spark, path).schema[pk_col].dataType
    if isinstance(dt, T.DecimalType):
        int_digits = dt.precision - dt.scale
        return (10 ** int_digits) - 1 if int_digits > 0 else 0
    if isinstance(dt, T.ByteType):
        return 127
    if isinstance(dt, T.ShortType):
        return 32_767
    if isinstance(dt, T.IntegerType):
        return 2**31 - 1
    if isinstance(dt, T.LongType):
        return 2**63 - 1
    if isinstance(dt, T.DoubleType):
        return 2**53
    if isinstance(dt, T.FloatType):
        return 2**24
    return None


def compute_pk_maxes(spark, config, comp_specs, floor: int = 0, band: int = 0,
                     n_rows: dict | None = None) -> dict[str, int]:
    """Per-table starting max for synthetic PKs, read from the FULL Parquet.

    For each non-static numeric-PK table the start is ``max(true_max + band, floor)``:
      - ``true_max`` = max(pk) from the full Parquet (footer-fast with
        spark.sql.parquet.aggregatePushdown=true), so synthetic PKs land above
        the real max even under --limit;
      - ``band`` = safety gap added above true_max, leaving room for the real
        table to grow between this read and the load without colliding;
      - ``floor`` = absolute minimum (the --pk-offset reserved band).

    The band/floor are then CLAMPED to the PK column's domain so a tight type
    (e.g. Decimal(3,0), max 999) can't overflow: start is capped at
    ``capacity - n_rows`` (and never below true_max). A table whose own growth
    already exceeds its PK domain is warned about (mark it static / scale down).

    Tables that are static, PK-less, or whose max is unreadable/non-numeric are
    omitted; the synthesizer falls back to append_after_max on the data.
    """
    n_rows = n_rows or {}
    out: dict[str, int] = {}
    for table, cfg in comp_specs.items():
        if cfg.get("static"):
            continue
        pk_cols = cfg.get("pk_cols") or []
        if not pk_cols:
            continue
        pk_col = pk_cols[-1]  # the synthesizer generates the last PK column
        try:
            # NB: max(pk) é lido da tabela INTEIRA (read_parquet direto, NÃO
            # _read_source), sem NENHUM dos predicados de FILTROS_FONTE, de
            # propósito. As PKs sintéticas precisam ficar acima do max real de
            # TODAS as linhas de produção (todos os NUM_TIPO_IF, inclusive
            # linhas excluídas / fora do produto) para não colidirem com dados
            # reais (ver validate_collision_producao). Filtrar aqui reduziria o
            # max e poderia gerar PKs que colidem com produção.
            raw_max = _read_pk_max(spark, raw_path(config, table), pk_col)
            true_max = int(raw_max) if raw_max is not None else None
            cap = _pk_capacity(spark, raw_path(config, table), pk_col)
        except Exception as exc:
            logger.warning("Could not read max(%s) for %s: %s", pk_col, table, exc)
            true_max, cap = None, None
        if true_max is None:
            continue
        start = max(true_max + band, floor)
        if cap is not None:
            headroom = cap - int(n_rows.get(table, 0))  # max start so start + n_rows <= cap
            if headroom < true_max:
                logger.warning(
                    "Table %s: PK domain (max %d) cannot hold %s new row(s) above %d; "
                    "mark it static or reduce scale.",
                    table, cap, n_rows.get(table, 0), true_max)
            elif start > headroom:
                logger.info("Table %s: clamping synthetic PK start %d -> %d to fit PK domain (%d)",
                            table, start, headroom, cap)
            start = max(true_max, min(start, headroom))
        out[table] = start
    return out


def _dominio_spine(comp_specs: Mapping[str, Any]) -> frozenset:
    """Tabelas cuja amostra, ao final da descida em `referential_sample`, é
    garantidamente consistente com o domínio (NUM_TIPO_IF == 49): a raiz
    (TABELAS_RAIZ_FILTRO) e toda tabela alcançável a partir dela por uma
    cadeia de FKs — mesmo que a tabela tenha OUTRAS FKs laterais (para
    referências compartilhadas como CONTA_PARTICIPANTE, PARTICIPANTE,
    tabelas de lookup etc.) que não entram nessa cadeia.

    Só uma FK cujo parent_table esteja neste conjunto pode ser usada para
    podar linhas durante a descida. Não é preciso que TODAS as FKs de uma
    tabela sejam para o domínio: a poda usa só as que forem; o restante
    (FK lateral) não poda nada nessa etapa e é resolvido depois por
    `completa_pais_referenciados` / `neutraliza_orfaos_na_fonte`. Ver
    comentário no topo do arquivo (FK de domínio vs FK de integridade).

    Ponto fixo sobre o grafo de FKs de `comp_specs`, sem tocar em dados: uma
    tabela entra no conjunto assim que tiver PELO MENOS UMA FK (não-self)
    cujo pai já esteja no conjunto — não precisa ser a FK direta para a
    raiz. Isso propaga corretamente por cadeias de 1 FK só (ex.:
    JUROS_FLUTUANTE/RESGATE -> CONDICAO_IF -> INSTRUMENTO_FINANCEIRO) sem
    arrastar tabelas cujo único vínculo é lateral (ex.: CONTA_PARTICIPANTE,
    que não tem nenhuma FK própria apontando para o domínio).
    """
    spine = {t for t in TABELAS_RAIZ_FILTRO if t in comp_specs}
    changed = True
    while changed:
        changed = False
        for table, cfg in comp_specs.items():
            if table in spine:
                continue
            for fk in _fk_list(cfg):
                parent = fk.get("parent_table")
                if parent != table and parent in spine:
                    spine.add(table)
                    changed = True
                    break
    return frozenset(spine)


def referential_sample(spark, config, comp_specs, limit: int | None) -> dict:
    """Subset referencial pais-antes-de-filhos, mantendo a FK consistente.

    Percorre o componente pais-antes-de-filhos: lê cada tabela JÁ FILTRADA
    pelos predicados de fonte do CDB simplificado (`_read_source` /
    FILTROS_FONTE) e mantém só as linhas-filhas cuja FK DE DOMÍNIO
    (parent_table em `_dominio_spine`) cai num pai já subsetado (ou é NULL).
    FKs LATERAIS (para tabelas fora do espinhaço de domínio, ex.:
    CONTA_PARTICIPANTE, PARTICIPANTE, tabelas de lookup) NÃO podam nada nesta
    etapa — podar por elas aqui zerava artificialmente linhas válidas do
    domínio sempre que a amostra independente do pai lateral não intersectava
    a filha, e a linha descartada não podia mais ser recuperada pelo fecho
    ascendente. Essas FKs continuam sendo resolvidas (pai completado ou órfão
    neutralizado) pelos passos seguintes.

    Duas camadas de recorte combinam aqui:
      (a) predicados de fonte (FILTROS_FONTE), aplicados na leitura de CADA
          tabela — a primeira etapa do pipeline; e
      (b) propagação por CHAVE a partir da raiz (TABELAS_RAIZ_FILTRO),
          descendo a árvore por semi-join sobre `_dominio_spine`.
    Como a fonte já vem filtrada em TODO ponto de leitura, pai e filha não
    podem divergir por re-injeção: um pai fora do produto simplesmente não
    existe para ninguém.

    Ao final, `completa_pais_referenciados` fecha o universo PARA CIMA: os
    valores de FK que sobraram em filhas mantidas mas cujo pai não sobreviveu
    ao filtro/poda (FKs ausentes no spec, arestas quebradas por ciclo, FKs
    laterais não usadas na poda) são completados puxando as linhas de pai da
    FONTE JÁ FILTRADA (`_read_source`). O resultado é FK-fechado por
    construção; os únicos órfãos restantes são os que já eram órfãos na
    produção OU cujo pai foi removido pelos predicados de fonte — ambos
    tratados por neutraliza_orfaos_na_fonte / null_orphan_fks (anula FK
    nullable; dropa linha quando a FK é NOT NULL).

    limit:
        int  -> também limita cada tabela a `limit` linhas (teste rápido). Os
                conjuntos de chave do pai são pequenos -> broadcast é seguro.
                NB: o fecho ascendente pode deixar tabelas-PAI com mais de
                `limit` linhas — é intencional (integridade > cap).
        None -> run COMPLETO: sem cap de linhas. Os conjuntos de chave do pai
                podem ser grandes -> o join de chave NÃO usa F.broadcast (deixa
                o AQE decidir) para evitar OOM no driver.
    """
    order = topo_order_tables(comp_specs)
    spine = _dominio_spine(comp_specs)
    sampled: dict = {}
    broadcast_keys = limit is not None
    for table in order:
        # Fonte JÁ FILTRADA pelos predicados do CDB simplificado
        # (FILTROS_FONTE), a primeira etapa. A propagação referencial abaixo
        # restringe adicionalmente por chave a partir da raiz, e a
        # consistência de FK é calculada sobre esse subconjunto. Linhas com
        # PK nula/vazia saem já na leitura e colunas NOT NULL não-FK
        # efetivamente nulas são preenchidas (artefatos de extração).
        df = _saneia_fonte(
            _read_source(spark, config, table), comp_specs[table], table)
        # Baseline BRUTO (pós-filtros de fonte, PRÉ-poda por semi-join): nulos
        # das colunas de FK direto da origem. Comparar com o estágio
        # "1.after_descending_sample" isola o que já vinha nulo na origem vs. o
        # que a poda tornou órfão. Só custa um agg de contagem de nulos.
        if _dbg():
            fk_cols_raw: list[str] = []
            for _fk in _fk_list(comp_specs[table]):
                for _c in (_fk.get("columns") or []):
                    if _c not in fk_cols_raw:
                        fk_cols_raw.append(_c)
            if fk_cols_raw:
                nulls_raw = _dbg_null_counts(df, fk_cols_raw)
                logger.debug(
                    "[DEBUG 0.raw_source] %s: nulos por coluna de FK na origem "
                    "(pós-filtros de fonte, pré-poda) = %s", table, nulls_raw)
        for fk in _fk_list(comp_specs[table]):
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            if (parent == table or parent not in sampled or not cols
                    or len(cols) != len(pcols) or parent not in spine):
                continue  # self-ref / out-of-component / malformed / FK lateral (fora do domínio) -> completada/neutralizada depois
            keys = (sampled[parent]
                    .select(*[F.col(pc).alias(f"__k{i}") for i, pc in enumerate(pcols)])
                    .dropna().distinct())
            cond = reduce(lambda a, b: a & b,
                          [df[cols[i]] == keys[f"__k{i}"] for i in range(len(cols))])
            # Broadcast só no caminho --limit (chaves pequenas). No full, as
            # chaves distintas de um pai grande estouram o broadcast -> deixa o
            # AQE escolher a estratégia de join.
            keys_side = F.broadcast(keys) if broadcast_keys else keys
            joined = df.join(keys_side, cond, "left")
            all_fk_null = reduce(lambda a, b: a & b, [F.col(c).isNull() for c in cols])
            df = (joined
                  .where(F.col("__k0").isNotNull() | all_fk_null)
                  .drop(*[f"__k{i}" for i in range(len(pcols))]))
        if limit is not None:
            df = df.limit(limit)
        # localCheckpoint (eager) nos dois caminhos: cada filho abaixo referencia
        # sampled[parent], então sem truncar o plano lógico a cadeia de joins se
        # acumula por toda a ordem topológica e todo consumidor downstream
        # (validação de FK, síntese) reanalisa um plano fundo o bastante para
        # OOMar o driver -> "SparkContext shutdown". persist() não ajuda — o
        # analyzer ainda percorre a árvore de plano cacheada; localCheckpoint a
        # substitui por uma folha RDD rasa.
        sampled[table] = df.localCheckpoint(eager=True)

    # Estado logo após a descida (filtros de fonte + poda por semi-join),
    # ANTES de qualquer fecho/neutralização: mostra os órfãos "estruturais"
    # herdados do subset (pais podados) vs. os que sobrarão como órfãos após
    # o fecho.
    debug_fk_integrity_report("1.after_descending_sample", sampled, comp_specs)

    # Passo ascendente: garante que toda chave de FK presente numa filha
    # mantida exista no pai amostrado, puxando da FONTE JÁ FILTRADA
    # (`_read_source`) as linhas de pai referenciadas que a poda removeu.
    # Um pai que os predicados de FILTROS_FONTE excluíram NÃO é puxado.
    sampled = completa_pais_referenciados(
        spark, config, comp_specs, sampled, broadcast_missing=broadcast_keys
    )
    # Após o fecho ascendente, os órfãos que RESTAREM têm DUAS origens
    # possíveis (ambas esperadas, não falha de amostragem):
    #   (a) órfão de PRODUÇÃO — a chave não existe nem na tabela completa; e
    #   (b) pai REMOVIDO pelos predicados de FILTROS_FONTE — a chave existe no
    #       Parquet completo, mas o pai não pertence ao produto CDB
    #       simplificado (ex.: CONDICAO_IF/INSTRUMENTO_FINANCEIRO com
    #       DAT_EXCLUSAO não-nula), então o fecho não o re-injeta.
    # Ao investigar um órfão aqui, distinga (a) de (b) antes de concluir que é
    # dado de origem: (b) é consequência intencional do filtro do produto.
    debug_fk_integrity_report("2.after_completa_pais", sampled, comp_specs)
    # Os órfãos remanescentes (produção OU pai fora do produto) são
    # neutralizados na fonte para que _fk_has_data_problem encontre zero
    # órfãos e a FK seja PRESERVADA na síntese — antes, um único órfão
    # descartava a FK inteira e null_orphan_fks anulava a coluna toda.
    sampled = neutraliza_orfaos_na_fonte(comp_specs, sampled)
    # Fonte final entregue à síntese. Aqui a contagem de órfãos DEVE ser 0
    # para toda FK do domínio; nulos são esperados (FK opcional / órfão de
    # produção anulado na fonte). Nulos altos numa coluna do pre-append check
    # aqui = origem já vinha nula OU neutraliza anulou muitos órfãos de
    # produção -> a coluna nasce nula, não é a síntese que a quebra.
    debug_fk_integrity_report("3.source_for_synthesis", sampled, comp_specs)
    return sampled


def neutraliza_orfaos_na_fonte(comp_specs, sampled: dict) -> dict:
    """Anula (ou dropa) na FONTE amostrada os valores de FK sem pai.

    Motivação: `_fk_has_data_problem` descarta o relacionamento INTEIRO se
    existir UM órfão sequer — o sintetizador então gera a coluna sem remap e
    `null_orphan_fks` a anula por completo. Um punhado de órfãos de produção
    (ex.: 69k chaves em CONDICAO_IF -> INSTRUMENTO_FINANCEIRO) destruía a FK
    para os milhões de linhas válidas. Aqui o órfão é neutralizado LINHA a
    LINHA na fonte, e o relacionamento sobrevive para o resto.

    Executa APÓS completa_pais_referenciados: o fecho ascendente já puxou do
    Parquet completo todo pai que existia; o que resta referenciado sem pai
    não existe em lugar nenhum (órfão de produção) e não há o que preservar.

    Regras por FK (espelham null_orphan_fks, mas na fonte):
      - linhas com QUALQUER coluna da FK nula nunca são tocadas (MATCH
        SIMPLE: FK parcialmente nula não é checada pelo banco);
      - anular exige ao menos UMA coluna da FK que seja ANULÁVEL — isto é,
        não-PK E não declarada em not_null_cols. Basta uma coluna nula para
        desligar a checagem composta, então anulamos essas;
      - se NENHUMA coluna da FK for anulável (todas PK e/ou NOT NULL), não dá
        para desligar a checagem sem violar NOT NULL: a linha órfã é DROPADA,
        com warning. O drop cascateia para os descendentes porque o passe
        corre em ordem topológica (pais antes de filhas) E repete a PONTO
        FIXO: em ciclo de FK a ordem é quebrada arbitrariamente e um pai pode
        ser encolhido DEPOIS de a filha já ter sido vista (caso real:
        PARTICIPANTE dropada após CONTA_PARTICIPANTE -> 9.751 órfãs chegavam
        à síntese e derrubavam a FK inteira via _fk_has_data_problem); a
        rodada seguinte revisita a filha contra o pai já encolhido. Rodada
        sem órfã custa só o anti-join da guarda por FK.

    NOT NULL vem de `not_null_cols` (spec gerado com cols_real.csv). Sem essa
    lista, cai no critério antigo (só PK bloqueia a anulação) — que pode anular
    uma coluna NOT NULL e quebrar o append com ORA-01400; a validação final
    antes da escrita (assert_not_null_ok) é a rede de segurança nesse caso.

    A ordem topológica também dá, de brinde, a checagem das arestas puladas
    pela quebra de ciclo na descida: aqui TODAS as tabelas já estão em
    `sampled`, então nenhuma aresta é pulada por `parent not in sampled`.

    Self-FKs (parent == table) TAMBÉM são neutralizadas: após o fecho
    intra-tabela de completa_pais_referenciados, um valor auto-referente sem
    correspondente é órfão de produção como qualquer outro — e, como a
    self-FK agora fica ATIVA nas specs de síntese, `_fk_has_data_problem`
    a checaria e descartaria por um órfão residual. O pai da checagem é o
    PRÓPRIO df corrente. No caso raro de self-FK dentro da PK (drop), o
    drop pode encadear (a linha dropada era pai de outra): itera a ponto
    fixo, limitado.

    Custo: por FK, um anti-join de chaves DISTINTAS + isEmpty (barato); o
    rewrite da filha (join contra as chaves órfãs, tipicamente poucas) e o
    novo localCheckpoint só acontecem quando há órfão de fato.
    """
    MAX_ITER_SELF_DROP = 10
    MAX_RODADAS_NEUTRALIZA = 10
    order = topo_order_tables(comp_specs)
    for rodada_global in range(1, MAX_RODADAS_NEUTRALIZA + 1):
        mudou_algo = False
        for table in order:
            df = sampled.get(table)
            if df is None:
                continue
            cfg = comp_specs[table]
            pk_set = set(cfg.get("pk_cols") or [])
            nn_set = _not_null_cols(cfg)
            changed = False
            for fk in _fk_list(cfg):
                parent = fk.get("parent_table")
                cols = list(fk.get("columns") or [])
                pcols = list(fk.get("parent_columns") or [])
                eh_self = parent == table
                if (parent not in sampled or not cols or len(cols) != len(pcols)
                        or (eh_self and set(cols) & set(pcols))):
                    continue  # fora do componente / malformada / self degenerada
                # Anulável = não-PK E não NOT NULL. Anular uma coluna NOT NULL
                # trocaria ORA-02291 (FK órfã) por ORA-01400 (NOT NULL); nesse
                # caso a linha é dropada (nullable_cols vazio -> ramo do drop).
                nullable_cols = [c for c in cols if c not in pk_set and c not in nn_set]
                # self + drop pode encadear (linha dropada era pai de outra);
                # nos demais casos uma rodada basta.
                rodadas = MAX_ITER_SELF_DROP if (eh_self and not nullable_cols) else 1
                for _ in range(rodadas):
                    base_pai = df if eh_self else sampled[parent]
                    parent_keys = (base_pai
                                   .select(*[F.col(p).alias(c)
                                             for c, p in zip(cols, pcols)])
                                   .distinct())
                    orfas = (df.select(*cols).dropna().distinct()
                             .join(parent_keys, on=cols, how="left_anti"))
                    if orfas.isEmpty():
                        break
                    # Join por igualdade nas colunas da FK: linhas com FK nula
                    # nunca casam com `orfas` (null != null) -> ficam intactas.
                    joined = df.join(orfas.withColumn("__orf", F.lit(True)),
                                     on=cols, how="left")
                    if nullable_cols:
                        logger.warning(
                            "neutraliza_orfaos_na_fonte: %s.%s -> %s%s: anulando "
                            "FK de linhas órfãs de produção (chave inexistente "
                            "no Parquet completo do pai).",
                            table, ",".join(cols), parent,
                            " (self)" if eh_self else "")
                        for c in nullable_cols:
                            joined = joined.withColumn(
                                c, F.when(F.col("__orf"),
                                          F.lit(None).cast(df.schema[c].dataType))
                                    .otherwise(F.col(c)))
                        df = joined.drop("__orf")
                    else:
                        # FK inteira dentro da PK: NOT NULL impede anular -> dropa.
                        logger.warning(
                            "neutraliza_orfaos_na_fonte: %s.%s -> %s%s: FK é "
                            "parte da PK (NOT NULL); DROPANDO linhas órfãs de "
                            "produção.",
                            table, ",".join(cols), parent,
                            " (self)" if eh_self else "")
                        df = joined.where(F.col("__orf").isNull()).drop("__orf")
                    changed = True
                    if eh_self and not nullable_cols:
                        # o drop mudou o conjunto de PKs: reavalia contra o df
                        # corrente na próxima rodada (plano raso via checkpoint).
                        df = df.localCheckpoint(eager=True)
                        continue
                    break
            if changed:
                # Checkpoint por tabela alterada: filhas mais abaixo na ordem
                # topológica leem sampled[table] já neutralizado (necessário para
                # o cascateamento do caso de drop) sem reanálise de plano fundo.
                sampled[table] = df.localCheckpoint(eager=True)
                mudou_algo = True
        if not mudou_algo:
            if rodada_global > 1:
                logger.info(
                    "neutraliza_orfaos_na_fonte: convergiu na rodada %d.",
                    rodada_global)
            break
    else:
        logger.warning(
            "neutraliza_orfaos_na_fonte: NÃO convergiu em %d rodadas — ainda "
            "havia mudança na última. Órfãos residuais ficam para "
            "null_orphan_fks (rede final).", MAX_RODADAS_NEUTRALIZA)
    return sampled


def completa_pais_referenciados(
    spark, config, comp_specs, sampled: dict, broadcast_missing: bool = False
) -> dict:
    """Fecho ascendente: todo valor de FK numa filha mantida passa a existir
    no pai amostrado.

    Percorre o componente em ordem topológica REVERSA (filhas -> pais): para
    cada FK de uma filha mantida, calcula as chaves referenciadas que NÃO
    estão no pai amostrado e puxa essas linhas da FONTE JÁ FILTRADA do pai
    (`_read_source` / FILTROS_FONTE), NÃO do Parquet completo. Um pai que não
    pertence ao produto CDB simplificado (ex.: CONDICAO_IF com DAT_EXCLUSAO
    não-nula) portanto NÃO é re-injetado aqui: a chave permanece faltante, a
    filha que a referenciava vira órfã e é neutralizada depois
    (null_orphan_fks anula FK nullable; drop_orphan_rows dropa quando NOT
    NULL). Assim os predicados de fonte valem também no fecho ascendente —
    nenhuma etapa consegue trazer de volta uma linha fora do produto.

    A ordem reversa resolve necessidades transitivas em UMA passada num DAG:
    as linhas adicionadas a um pai ainda terão as PRÓPRIAS FKs completadas
    quando esse pai for visitado depois (pais vêm antes das filhas na ordem
    topológica, logo depois delas na reversa). Arestas removidas pela quebra
    de ciclo em _toposort_break_cycles podem deixar resíduo transitivo; esse
    resíduo (raro) segue coberto por null_orphan_fks na síntese.

    Auto-referências (parent == child) são fechadas DENTRO da própria tabela,
    por iteração a ponto fixo ANTES das FKs normais da mesma tabela: uma
    linha mantida cuja self-FK aponte para uma linha podada puxa-a de volta
    da fonte JÁ FILTRADA; a linha puxada pode referenciar outra, e assim por
    diante — a iteração converge em ~profundidade-da-hierarquia passos
    (limitada por MAX_ITER_FECHO_SELF; hierarquias reais são rasas). O
    critério de parada por CONTAGEM estagnada (e não só por vazio) evita
    loop infinito quando restam apenas chaves inexistentes no Parquet
    completo (órfãs de produção), que ficam para neutraliza_orfaos_na_fonte.
    Rodar o fecho self ANTES das FKs normais garante que as linhas puxadas
    também tenham seus pais externos completados no mesmo passe.

    Custo: `faltantes` é checado com isEmpty()/count antes de tocar o
    Parquet do pai — no caso comum (poda descendente já garantiu a FK) a
    passada custa apenas um anti-join de chaves distintas, sem leitura extra
    nem novo checkpoint. O union + localCheckpoint só acontece quando há de
    fato linhas a completar, mantendo o plano do pai raso para os
    consumidores downstream (síntese, validação).

    broadcast_missing:
        True (caminho --limit) -> `faltantes` é pequeno por construção
        (limitado pelas chaves distintas de uma filha com <= limit linhas);
        broadcast no left_semi evita shuffle do Parquet completo do pai.
        False (full run) -> deixa o AQE decidir, como no passo descendente.
    """
    MAX_ITER_FECHO_SELF = 20
    order = topo_order_tables(comp_specs)
    for child in reversed(order):
        child_df = sampled.get(child)
        if child_df is None:
            continue
        fks = _fk_list(comp_specs[child])

        # ---- 1) fecho intra-tabela (self-FKs), a ponto fixo -------------
        for fk in fks:
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            if (parent != child or not cols or len(cols) != len(pcols)
                    or set(cols) & set(pcols)):
                continue  # não-self / malformada / degenerada
            n_anterior = -1
            for _ in range(MAX_ITER_FECHO_SELF):
                ref_keys = (child_df
                            .select(*[F.col(c).alias(p)
                                      for c, p in zip(cols, pcols)])
                            .dropna().distinct())
                faltantes = ref_keys.join(
                    child_df.select(*pcols).distinct(),
                    on=pcols, how="left_anti")
                n_faltantes = faltantes.count()
                if n_faltantes == 0 or n_faltantes == n_anterior:
                    # convergiu, ou só restam chaves inexistentes na fonte
                    # JÁ FILTRADA — órfãs de produção OU pais removidos pelos
                    # predicados de fonte; ambos ficam para a neutralização.
                    break
                n_anterior = n_faltantes
                faltantes_side = (F.broadcast(faltantes)
                                  if broadcast_missing else faltantes)
                extra = (_saneia_fonte(
                             _read_source(spark, config, child),
                             comp_specs[child], child)
                         .join(faltantes_side, on=pcols, how="left_semi"))
                logger.info(
                    "completa_pais_referenciados: %s.%s -> %s (self): "
                    "puxando %d linha(s) referenciada(s) podada(s)",
                    child, ",".join(cols), child, n_faltantes)
                child_df = (child_df.unionByName(extra)
                            .localCheckpoint(eager=True))
            sampled[child] = child_df

        # ---- 2) FKs normais (filha -> pai externo) ----------------------
        for fk in fks:
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            if (parent == child or parent not in sampled
                    or not cols or len(cols) != len(pcols)):
                continue  # self já tratada acima / fora do componente
            # Chaves referenciadas pela filha, já no nome das colunas do pai.
            # dropna(): FK com QUALQUER coluna NULL não exige pai (MATCH
            # SIMPLE); os casos parciais seguem com null_orphan_fks.
            ref_keys = (child_df
                        .select(*[F.col(c).alias(p) for c, p in zip(cols, pcols)])
                        .dropna()
                        .distinct())
            faltantes = ref_keys.join(
                sampled[parent].select(*pcols).distinct(),
                on=pcols, how="left_anti")
            # Ação barata (isEmpty) que evita, no caso comum de zero
            # faltantes, a leitura da fonte do pai, o union e um novo
            # localCheckpoint.
            if faltantes.isEmpty():
                continue
            # Puxa da FONTE JÁ FILTRADA do pai (`_read_source` / FILTROS_FONTE),
            # NÃO do Parquet completo: um pai fora do produto CDB simplificado
            # não é re-injetado; a chave faltante remanescente torna a filha
            # órfã e a neutralização (null_orphan_fks / drop_orphan_rows) cuida.
            faltantes_side = F.broadcast(faltantes) if broadcast_missing else faltantes
            extra = (_saneia_fonte(
                         _read_source(spark, config, parent),
                         comp_specs[parent], parent)
                     .join(faltantes_side, on=pcols, how="left_semi"))
            logger.info(
                "completa_pais_referenciados: %s.%s -> %s: completando pai com "
                "linhas referenciadas ausentes",
                child, ",".join(cols), parent)
            # localCheckpoint eager: sampled[parent] pode ser aumentado por
            # várias filhas e consumido por vários downstreams; sem truncar,
            # o plano cresce a cada union.
            sampled[parent] = (sampled[parent]
                               .unionByName(extra)
                               .localCheckpoint(eager=True))
    return sampled


def _is_condicao_if_subtype(child: str, parent: str, cols: list[str]) -> bool:
    """True se (child, parent, FK) é uma tabela-subtipo de CONDICAO_IF conhecida.

    Ou seja: o pai é CONDICAO_IF, a FK shared-key é sobre NUM_CONDICAO_IF, e o
    filho é uma das tabelas-subtipo mapeadas em SUBTYPE_BY_TIPO. Só nesse caso o
    bind precisa particionar as chaves do pai por COD_TIPO_CONDICAO_IF.
    """
    return (parent == CONDICAO_IF_TABLE
            and child in TIPO_BY_SUBTYPE
            and [c.upper() for c in cols] == [CONDICAO_IF_PK])


def _condicao_if_keys_for_subtype(
    parent_df: DataFrame, pcols: list[str], subtype_table: str
) -> Optional[DataFrame]:
    """Chaves do CONDICAO_IF pai cujo COD_TIPO_CONDICAO_IF casa com `subtype_table`.

    Retorna o DF de chaves (aliased __np{i}) restrito ao tipo do subtipo, ou None
    se o pai não tiver a coluna de tipo (aí o caller cai no comportamento antigo:
    usar todas as chaves). Particionar por tipo garante fatias DISJUNTAS entre
    subtipos -> nenhum NUM_CONDICAO_IF cai em dois subtipos (ambíguo), cada linha
    fica no subtipo do tipo certo (mismatch), e o 1:1 por fatia mantém unicidade.
    """
    if not _has_column(parent_df, CONDICAO_IF_TIPO_COL):
        return None
    expected_tipo = TIPO_BY_SUBTYPE.get(subtype_table)
    if expected_tipo is None:
        return None
    # Normaliza o código do pai a string trimada sem ".0" (IDs numéricos lidos
    # como double/decimal viram "2.0"); compara com o código esperado do mapa.
    filtered = parent_df.where(
        _condicao_if_tipo_norm_expr() == F.lit(expected_tipo))
    return (filtered
            .select(*[F.col(pc).alias(f"__np{i}") for i, pc in enumerate(pcols)])
            .dropna().distinct())


def bind_shared_key_children(synthetic: dict, comp_specs: dict) -> dict:
    """Rebind 1:1 shared-key children (PK == FK) to distinct synthetic parent keys.

    For a table whose primary key IS its FK to a parent (e.g. JUROS_FLUTUANTE /
    RESGATE keyed by NUM_CONDICAO_IF -> CONDICAO_IF), the synthesizer's FK remap
    can leave the column NULL or non-unique — fatal because it's a NOT NULL PK.
    Here we overwrite those columns with a distinct slice of the parent's
    synthetic keys, guaranteeing valid, unique, non-null keys. Child rows beyond
    the number of parent keys are dropped (1:1 cardinality).

    SUBTYPE-AWARE (correção do polimorfismo CONDICAO_IF): quando o par
    (child, parent) é uma tabela-subtipo de CONDICAO_IF (ver
    _is_condicao_if_subtype), a fatia de chaves do pai NÃO é "todas as chaves
    0..N", e sim APENAS as chaves cujo COD_TIPO_CONDICAO_IF corresponde àquele
    subtipo (ex.: JUROS_FIXO recebe só as chaves de tipo 2). Como cada subtipo
    consome uma fatia DISJUNTA do espaço de chaves do pai, garante-se de uma vez:
      (a) 1:1 por fatia -> chaves únicas (sem shared_key_dup);
      (b) exatamente uma tabela por chave -> sem ambiguidade (o
          ClassCastException) e sem "subtype_mismatch".
    Resíduos possíveis e quem os resolve:
      * subtipo NO run mas com MENOS linhas que as chaves do pai daquele tipo
        (inclusive ZERO linhas — domínio/amostra sem o subtipo, caso típico do
        CDB simplificado com AMORTIZACAO/PARTICIPACAO_LUCROS/RESET/
        DESDOBRAMENTO): as chaves de pai não cobertas ficariam dangling; são
        REMOVIDAS do CONDICAO_IF por alinha_condicao_if_aos_subtipos logo após
        este bind, e as filhas que as referenciavam são neutralizadas por
        null_orphan_fks (ordem topológica).
      * subtipo que NÃO está no run com linhas de pai daquele tipo: continua
        sendo erro de spec — resolvido incluindo o subtipo em TABELAS_ALVO
        (gera_spec_config.py) e barrado por assert_polymorphism_ok antes da
        gravação.
    FKs shared-key que não são subtipos de CONDICAO_IF mantêm o
    comportamento antigo (todas as chaves do pai).

    DUAS REGRAS DE SEGURANÇA (correção das duplicidades/órfãos em
    PARTICIPANTE/CONTA_PARTICIPANTE):
      * tabela STATIC nunca é rebindada — é referência copiada da fonte, com
        chaves já consistentes; reescrevê-las embaralha a identidade das
        linhas e torna órfã toda FK externa que aponta para ela. Órfã
        residual em static (cascata de drops da neutralização) é dropada por
        null_orphan_fks, preservando o pareamento original.
      * iteração em ORDEM TOPOLÓGICA (pais antes de filhas), não em ordem de
        dict: numa cadeia shared-key (ex.: ENTIDADE <- PESSOA_JURIDICA <-
        PARTICIPANTE), vincular a filha ANTES de o pai ter as próprias chaves
        reescritas invalidava o bind da filha (era a ordem alfabética que
        fazia PARTICIPANTE receber chaves antigas de PESSOA_JURIDICA).
    """
    for child in topo_order_tables(comp_specs):
        cfg = comp_specs[child]
        child_df = synthetic.get(child)
        if child_df is None:
            continue
        if cfg.get("static"):
            continue  # referência copiada da fonte: chaves intocáveis
        pk_cols = list(cfg.get("pk_cols") or [])
        for fk in _fk_list(cfg):
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            parent = fk.get("parent_table")
            parent_df = synthetic.get(parent)
            if (parent_df is None or parent == child or len(cols) != len(pcols)
                    or not _fk_is_whole_pk(pk_cols, fk)):
                continue

            # Fatia de chaves do pai. Para subtipos de CONDICAO_IF, restringe ao
            # COD_TIPO_CONDICAO_IF do subtipo (fatias disjuntas por tipo). Para
            # os demais shared-key, todas as chaves do pai (comportamento antigo).
            keys = None
            if _is_condicao_if_subtype(child, parent, cols):
                keys = _condicao_if_keys_for_subtype(parent_df, pcols, child)
                if keys is not None:
                    logger.info(
                        "bind_shared_key_children: %s é subtipo de CONDICAO_IF "
                        "(tipo=%s); vinculando SÓ às chaves desse tipo.",
                        child, TIPO_BY_SUBTYPE.get(child))
            if keys is None:
                # Numbering em PARALELO via _with_contiguous_row_id em vez de
                # row_number() sobre Window sem partitionBy. O padrão antigo virava
                # Exchange SinglePartition (sort serial numa única task) e estourava
                # num pai grande tipo CONDICAO_IF; o lado do filho, com
                # orderBy(monotonically_increasing_id()), ainda era NÃO-determinístico
                # (mid é reavaliado por estágio), variando o pareamento entre runs.
                # _with_contiguous_row_id dá id contíguo 0..N-1 bijetivo dos dois
                # lados; o inner join pareia 1:1 e descarta filhos acima do nº de
                # chaves do pai (mesma cardinalidade documentada), sem sort global.
                keys = (parent_df
                        .select(*[F.col(pc).alias(f"__np{i}") for i, pc in enumerate(pcols)])
                        .dropna().distinct())
            # MATERIALIZA cada lado após numerar. _with_contiguous_row_id usa
            # monotonically_increasing_id() — não-determinístico entre
            # avaliações. Sem congelar, o inner join por __bind_rn reavalia cada
            # lado independentemente e o pareamento 1:1 vira instável (mesma
            # classe de bug do rebind: numeração diverge entre avaliações do
            # plano lazy). localCheckpoint fixa __bind_rn dos dois lados, então
            # o join pareia de forma estável e reprodutível dentro do run.
            keys = _with_contiguous_row_id(keys, "__bind_rn").localCheckpoint(eager=True)
            numbered = _with_contiguous_row_id(child_df, "__bind_rn").localCheckpoint(eager=True)

            joined = numbered.join(keys, "__bind_rn", "inner")
            for i, c in enumerate(cols):
                joined = joined.withColumn(c, F.col(f"__np{i}"))
            child_df = joined.drop("__bind_rn", *[f"__np{i}" for i in range(len(pcols))])
        synthetic[child] = child_df
    return synthetic


def _condicao_if_tipo_norm_expr():
    """COD_TIPO_CONDICAO_IF normalizado a string trimada sem ".0" (IDs lidos
    como double/decimal viram "2.0"); mesma normalização usada no bind e nos
    asserts, para as comparações de tipo serem consistentes entre os passes."""
    return F.regexp_replace(
        F.trim(F.col(CONDICAO_IF_TIPO_COL).cast("string")), r"\.0$", "")


def _subtipo_vazio_e_legitimo(synthetic: dict, table: str) -> bool:
    """True se `table` é subtipo de CONDICAO_IF e o pai sintético NÃO tem
    nenhuma linha do COD_TIPO correspondente.

    Nesse caso a tabela-subtipo vazia é o resultado CORRETO — a invariante do
    polimorfismo (exatamente uma linha-subtipo por pai de tipo concreto) vale
    trivialmente: zero pais do tipo -> zero linhas no subtipo. É o que acontece
    no domínio CDB simplificado com tipos que não se aplicam a CDB (ex.:
    AMORTIZACAO/PARTICIPACAO_LUCROS/RESET/DESDOBRAMENTO), onde a fonte
    filtrada/amostrada simplesmente não tem linhas do subtipo. Abortar o run
    por isso (como o vazias_alvo fazia) transforma um dado VÁLIDO em falha.
    """
    tipo = TIPO_BY_SUBTYPE.get(table)
    if tipo is None:
        return False
    cond = synthetic.get(CONDICAO_IF_TABLE)
    if cond is None or not _has_column(cond, CONDICAO_IF_TIPO_COL):
        return False
    return cond.where(_condicao_if_tipo_norm_expr() == F.lit(tipo)).isEmpty()


def alinha_condicao_if_aos_subtipos(synthetic: dict, comp_specs: dict) -> dict:
    """Remove do CONDICAO_IF sintético as linhas de tipo concreto que ficaram
    SEM linha-subtipo (dangling) após bind_shared_key_children.

    Por que existe: o bind vincula cada subtipo APENAS às chaves do pai com o
    seu COD_TIPO_CONDICAO_IF, num pareamento 1:1 por posição. Quando o subtipo
    tem MENOS linhas que as chaves do pai daquele tipo — inclusive ZERO linhas,
    caso típico do CDB simplificado, cujo domínio não tem AMORTIZACAO/
    PARTICIPACAO_LUCROS/RESET/DESDOBRAMENTO — as chaves excedentes do pai ficam
    sem linha-subtipo. O Hibernate não conseguiria tipá-las (dangling), o
    assert_polymorphism_ok abortaria e, no caso de subtipo 100% vazio, o
    vazias_alvo do assert_not_null_ok abortava antes. Como não dá para FABRICAR
    linhas-subtipo (não há linha de fonte para bootstrapar), a única saída
    consistente é encolher o pai para as chaves efetivamente cobertas.

    O que faz, para cada tabela-subtipo PRESENTE no run (em synthetic):
      1. coleta as chaves NUM_CONDICAO_IF cobertas pelo subtipo (pós-bind);
      2. mantém no pai apenas linhas cujo (chave, tipo) está coberto;
      3. linhas de tipo NULL, de tipo fora do mapa curado ou de subtipo que
         NÃO está no run passam intactas — para subtipo fora do run a política
         continua sendo a do assert_polymorphism_ok (erro de spec: inclua a
         tabela em TABELAS_ALVO).

    DEVE rodar ANTES de null_orphan_fks: as filhas de CONDICAO_IF que
    referenciavam chaves removidas são então neutralizadas pela rede de
    segurança existente (anulação/rebind em ordem topológica).
    """
    cfg = comp_specs.get(CONDICAO_IF_TABLE)
    cond = synthetic.get(CONDICAO_IF_TABLE)
    if (cfg is None or cfg.get("static") or cond is None
            or not _has_column(cond, CONDICAO_IF_PK)
            or not _has_column(cond, CONDICAO_IF_TIPO_COL)):
        return synthetic

    covered = None
    aligned_tipos: list[str] = []
    for subtype, tipo in TIPO_BY_SUBTYPE.items():
        sdf = synthetic.get(subtype)
        if sdf is None or not _has_column(sdf, CONDICAO_IF_PK):
            continue
        aligned_tipos.append(tipo)
        piece = (sdf.select(F.col(CONDICAO_IF_PK).cast("string").alias("__al_key"))
                    .dropna().distinct()
                    .withColumn("__al_tipo", F.lit(tipo)))
        covered = piece if covered is None else covered.unionByName(piece)

    if covered is None:
        return synthetic

    work = (cond
            .withColumn("__al_key", F.col(CONDICAO_IF_PK).cast("string"))
            .withColumn("__al_tipo", _condicao_if_tipo_norm_expr()))
    hit = covered.withColumn("__al_hit", F.lit(True))
    # `covered` é distinct por (chave, tipo) -> o left join não multiplica
    # linhas do pai.
    joined = work.join(hit, ["__al_key", "__al_tipo"], "left")
    # keep nunca é NULL: os dois primeiros disjuntos são is[Not]Null (sempre
    # booleanos) e ~isin só é NULL com tipo NULL, caso já coberto pelo isNull.
    keep = (
        F.col("__al_hit").isNotNull()
        | F.col("__al_tipo").isNull()
        | ~F.col("__al_tipo").isin(aligned_tipos)
    )

    stats = (joined.where(~keep)
             .groupBy("__al_tipo")
             .agg(F.count(F.lit(1)).alias("__al_n"))
             .collect())
    if not stats:
        return synthetic

    total = sum(int(r["__al_n"]) for r in stats)
    detalhe = ", ".join(
        f"tipo={r['__al_tipo']} ({SUBTYPE_BY_TIPO.get(str(r['__al_tipo']), '?')}): "
        f"{r['__al_n']}"
        for r in sorted(stats, key=lambda r: str(r["__al_tipo"])))
    logger.warning(
        "alinha_condicao_if_aos_subtipos: removendo %d linha(s) de CONDICAO_IF "
        "sem linha-subtipo correspondente (dangling apos o bind): %s. Filhas "
        "que referenciavam essas chaves serao neutralizadas por "
        "null_orphan_fks.", total, detalhe)

    synthetic[CONDICAO_IF_TABLE] = (
        joined.where(keep).drop("__al_key", "__al_tipo", "__al_hit"))
    return synthetic


def _rebind_orphan_fk_to_valid_parent(
    child_df: DataFrame, parent_df: DataFrame,
    cols: list[str], pcols: list[str], *, seed: int,
    allow_reuse: bool = True) -> DataFrame:
    """Reaponta linhas órfãs de uma FK NOT NULL para chaves VÁLIDAS do pai.

    Preserva a linha (não dropa, não anula): a FK órfã recebe uma chave que
    existe no pai sintético, escolhida de forma determinística. Cada linha órfã
    é pareada por posição contígua (0..N-1) com uma chave do pai.

    allow_reuse:
        True  (FK comum, N:1) -> se há mais órfãs que chaves distintas, as
              chaves são recicladas por módulo — aceitável para dado sintético
              (o vínculo original já não existia).
        False (FK que participa da PK da filha, ex.: shared-key 1:1) ->
              reciclar chave criaria PK DUPLICADA (a chave reciclada colide
              com linha não-órfã; caso real: PARTICIPANTE.NUM_ID_ENTIDADE com
              5.952 duplicidades). Cada órfã recebe uma chave do pai que
              NINGUÉM usa (chaves do pai MENOS as já usadas pelas linhas
              não-órfãs); órfãs além do estoque de chaves livres são DROPADAS
              — não há como preservá-las sem violar a unicidade da PK.

    Só é chamado quando o pai TEM ao menos uma chave; o caller trata o caso de
    pai sem chave nenhuma (drop inevitável).
    """
    n = len(cols)
    # Chaves distintas do pai, numeradas 0..K-1.
    pkeys = (parent_df
             .select(*[F.col(pc).alias(f"__rb_np{i}") for i, pc in enumerate(pcols)])
             .dropna().distinct())
    pkeys = _with_contiguous_row_id(pkeys, "__rb_k")
    k = pkeys.count()
    if k == 0:
        return child_df  # sem chave no pai: caller decide (drop)

    # Marca órfãs: FK não-nula sem correspondência no pai.
    match = (pkeys.select(*[F.col(f"__rb_np{i}").alias(f"__rb_m{i}") for i in range(n)])
                  .withColumn("__rb_hit", F.lit(True)))
    cond = reduce(lambda a, b: a & b,
                  [child_df[cols[i]] == match[f"__rb_m{i}"] for i in range(n)])
    j = child_df.join(match, cond, "left")
    # MATCH SIMPLE: só é órfã a linha com TODAS as colunas da FK preenchidas e
    # sem correspondência no pai. Uma FK composta PARCIALMENTE nula não é
    # checada pelo banco (a checagem composta é desligada), então NÃO é órfã e
    # NÃO deve ser tocada — antes usava `any_fk_set` (qualquer col preenchida),
    # o que reescrevia indevidamente linhas válidas de FK composta parcial.
    all_fk_set = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in cols])
    is_orphan = F.col("__rb_hit").isNull() & all_fk_set

    # Congela is_orphan ANTES de fatiar em orf/nonorf. `j` é um join lazy; se
    # não materializar, cada ramo (orf, nonorf) reavalia `j` de forma
    # independente e não-determinística (o join + a numeração adiante embutem
    # ordem instável), e uma mesma linha poderia cair em orf numa avaliação e
    # em nonorf noutra — ou em nenhuma. localCheckpoint fixa a marcação de órfã
    # de uma vez, garantindo partição exata e sem perda/duplicação de linha.
    j = j.withColumn("__rb_isorf", is_orphan).localCheckpoint(eager=True)
    orf = j.where(F.col("__rb_isorf"))
    nonorf = j.where(~F.col("__rb_isorf"))
    if orf.isEmpty():
        drop_tmp = (["__rb_hit", "__rb_isorf"]
                    + [f"__rb_m{i}" for i in range(n)])
        return j.drop(*drop_tmp)

    orf = _with_contiguous_row_id(orf, "__rb_rn")
    if allow_reuse:
        # chave do pai = (rn + seed) % k. O seed (estável por (child,parent,cols) via
        # _stable_seed no caller) desloca o início do ciclo, então a distribuição das
        # órfãs entre as chaves do pai não é sempre "as primeiras k órfãs -> chaves
        # 0..k-1". NB: isto NÃO torna o rebind reprodutível ENTRE runs — a numeração
        # __rb_rn vem de monotonically_increasing_id, instável entre execuções; o
        # determinismo é garantido só DENTRO do run (após o localCheckpoint acima).
        seed_off = int(seed) % k if k else 0
        orf = orf.withColumn(
            "__rb_pick", ((F.col("__rb_rn") + F.lit(seed_off)) % F.lit(k)).cast("long"))
        picks = pkeys  # tem __rb_k (0..k-1) e __rb_np{i}
    else:
        # Sem reciclagem: destino = chaves do pai que NENHUMA linha não-órfã
        # usa. Congela a numeração das órfãs (o count/where/join abaixo fazem
        # múltiplas leituras e __rb_rn vem de monotonically_increasing_id).
        orf = orf.localCheckpoint(eager=True)
        usadas = (nonorf
                  .select(*[F.col(c).alias(f"__rb_np{i}") for i, c in enumerate(cols)])
                  .dropna().distinct())
        livres = (pkeys.drop("__rb_k")
                  .join(usadas, on=[f"__rb_np{i}" for i in range(n)], how="left_anti"))
        livres = _with_contiguous_row_id(livres, "__rb_k").localCheckpoint(eager=True)
        m = livres.count()
        drop_tmp = ["__rb_hit", "__rb_isorf"] + [f"__rb_m{i}" for i in range(n)]
        if m == 0:
            logger.warning(
                "_rebind_orphan_fk_to_valid_parent: FK %s participa da PK e o "
                "pai não tem NENHUMA chave livre; DROPANDO todas as órfãs "
                "(reciclar chave duplicaria a PK).", ",".join(cols))
            return nonorf.drop(*drop_tmp)
        n_orf = orf.count()
        if n_orf > m:
            logger.warning(
                "_rebind_orphan_fk_to_valid_parent: FK %s participa da PK; "
                "%d órfã(s) para %d chave(s) livre(s) do pai — DROPANDO %d "
                "órfã(s) excedente(s) (preservá-las duplicaria a PK).",
                ",".join(cols), n_orf, m, n_orf - m)
            orf = orf.where(F.col("__rb_rn") < F.lit(m))
        seed_off = int(seed) % m
        orf = orf.withColumn(
            "__rb_pick", ((F.col("__rb_rn") + F.lit(seed_off)) % F.lit(m)).cast("long"))
        picks = livres  # tem __rb_k (0..m-1) e __rb_np{i}
    orf = orf.join(F.broadcast(picks), orf["__rb_pick"] == picks["__rb_k"], "left")
    for i, c in enumerate(cols):
        ctype = _get_field_type(orf, c)
        orf = orf.withColumn(c, F.col(f"__rb_np{i}").cast(ctype))
    orf = orf.drop("__rb_rn", "__rb_pick", "__rb_k",
                   *[f"__rb_np{i}" for i in range(n)])

    drop_tmp = ["__rb_hit", "__rb_isorf"] + [f"__rb_m{i}" for i in range(n)]
    orf = orf.drop(*drop_tmp)
    nonorf = nonorf.drop(*drop_tmp)
    return nonorf.unionByName(orf)


def _tem_orfa(child_df: DataFrame, parent_df: DataFrame,
              cols: list[str], pcols: list[str]) -> bool:
    """Guarda barata: existe órfã sob MATCH SIMPLE? Anti-join de chaves
    DISTINTAS com TODAS as colunas da FK preenchidas + isEmpty — não
    materializa nada e é idempotente (linha anulada/dropada sai do conjunto na
    rodada seguinte), o que torna a detecção de mudança do ponto fixo exata."""
    ck = child_df.select(*cols).dropna().distinct()
    pk = (parent_df
          .select(*[F.col(p).alias(c) for c, p in zip(cols, pcols)])
          .dropna().distinct())
    return not ck.join(pk, on=cols, how="left_anti").isEmpty()


def _drop_orphan_rows(child_df: DataFrame, parent_df: DataFrame,
                      cols: list[str], pcols: list[str]) -> DataFrame:
    """Remove as linhas órfãs de uma FK. MATCH SIMPLE: só é órfã a linha com
    TODAS as colunas da FK preenchidas e sem correspondência no pai — FK
    parcialmente nula não é checada pelo banco e fica intacta."""
    keys = (parent_df
            .select(*[F.col(pc).alias(f"__pk{i}") for i, pc in enumerate(pcols)])
            .dropna().distinct().withColumn("__match", F.lit(True)))
    cond = reduce(lambda a, b: a & b,
                  [child_df[cols[i]] == keys[f"__pk{i}"] for i in range(len(cols))])
    joined = child_df.join(keys, cond, "left")
    all_fk_set = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in cols])
    is_orphan = F.col("__match").isNull() & all_fk_set
    return (joined.where(~is_orphan)
            .drop("__match", *[f"__pk{i}" for i in range(len(pcols))]))


def null_orphan_fks(synthetic: dict, comp_specs: dict) -> dict:
    """Neutraliza FK órfã pós-síntese, a PONTO FIXO, SEM esvaziar tabela.

    Rede de segurança final para referências que o sintetizador não conseguiu
    remapear — self-refs, relações ignoradas (órfão de fonte / pai ausente),
    órfãos residuais de amostragem — para o load não bater em ORA-02291.

    Decisão por FK (todas sob MATCH SIMPLE: linha com QUALQUER coluna da FK
    nula não é checada pelo banco e não é tocada):
      - colunas ANULÁVEIS (não-PK E não NOT NULL) -> anuladas nas linhas órfãs
        (uma coluna nula desliga a checagem composta);
      - tabela STATIC com FK NOT NULL órfã -> DROP da linha. Static é
        referência copiada da fonte: rebindar reescreveria chaves reais e
        embaralharia a identidade das linhas (foi a origem das duplicidades de
        PARTICIPANTE e dos órfãos de CONTA_PARTICIPANTE);
      - FK NOT NULL de tabela sintetizada -> REBIND determinístico das linhas
        órfãs para uma chave VÁLIDA do pai (preserva a linha). Se a FK
        participa da PK, o rebind NÃO recicla chave (allow_reuse=False:
        reciclar duplicaria a PK — só usa chaves livres, dropando excedentes).
        Só quando o pai não tem NENHUMA chave é que as órfãs são dropadas
        (inevitável) — com warning.

    PONTO FIXO em vez de passada única: a garantia "pai antes da filha" da
    ordem topológica NÃO existe em ciclo de FK (quebrado arbitrariamente), e
    um pai alterado DEPOIS da filha re-cria órfãs que a passada única não
    revisitava (caso real: REBIND de CONTA_PARTICIPANTE às 18:24:16 invalidado
    pelo REBIND de PARTICIPANTE às 18:24:22 -> 29.880 órfãs na saída). O passe
    repete até nenhuma tabela mudar; cada aresta custa só o anti-join da
    guarda quando já está sã, então rodadas extras são baratas. Tabela
    alterada é congelada (localCheckpoint) ANTES de ser lida como pai — regra
    do arquivo: resultado de _with_contiguous_row_id/mono_id deve ser
    materializado antes de múltiplas leituras.
    """
    MAX_RODADAS = 10
    order = topo_order_tables(comp_specs)
    for rodada in range(1, MAX_RODADAS + 1):
        mudou_algo = False
        for child in order:
            cfg = comp_specs.get(child)
            if cfg is None:
                continue
            child_df = synthetic.get(child)
            if child_df is None:
                continue
            pk_set = set(cfg.get("pk_cols") or [])
            nn_set = _not_null_cols(cfg)
            estatica = bool(cfg.get("static"))
            mudou_tabela = False
            for fk in _fk_list(cfg):
                parent = fk.get("parent_table")
                parent_df = child_df if parent == child else synthetic.get(parent)
                cols = list(fk.get("columns") or [])
                pcols = list(fk.get("parent_columns") or [])
                if parent_df is None or not cols or len(cols) != len(pcols):
                    continue
                if not _tem_orfa(child_df, parent_df, cols, pcols):
                    continue  # aresta sã -> nada a fazer (e nada muda)

                nullable_cols = [c for c in cols
                                 if c not in pk_set and c not in nn_set]

                if nullable_cols:
                    # Anula só as colunas anuláveis nas linhas órfãs.
                    logger.info(
                        "null_orphan_fks: %s.%s -> %s: anulando FK de "
                        "linha(s) órfã(s).", child, ",".join(cols), parent)
                    keys = (parent_df
                            .select(*[F.col(pc).alias(f"__pk{i}")
                                      for i, pc in enumerate(pcols)])
                            .dropna().distinct()
                            .withColumn("__match", F.lit(True)))
                    cond = reduce(
                        lambda a, b: a & b,
                        [child_df[cols[i]] == keys[f"__pk{i}"]
                         for i in range(len(cols))])
                    joined = child_df.join(keys, cond, "left")
                    all_fk_set = reduce(
                        lambda a, b: a & b,
                        [F.col(c).isNotNull() for c in cols])
                    is_orphan = F.col("__match").isNull() & all_fk_set
                    for c in nullable_cols:
                        joined = joined.withColumn(
                            c, F.when(is_orphan,
                                      F.lit(None).cast(child_df.schema[c].dataType))
                                .otherwise(F.col(c)))
                    child_df = joined.drop(
                        "__match", *[f"__pk{i}" for i in range(len(pcols))])
                elif estatica:
                    # Static: nunca rebindar (ver docstring). Drop preserva o
                    # pareamento original das linhas que ficam.
                    logger.warning(
                        "null_orphan_fks: %s.%s -> %s: FK NOT NULL órfã em "
                        "tabela STATIC; DROPANDO linha(s) órfã(s) (static não "
                        "é rebindada).", child, ",".join(cols), parent)
                    child_df = _drop_orphan_rows(child_df, parent_df, cols, pcols)
                else:
                    # FK NOT NULL órfã: rebind para chave válida do pai
                    # (preserva a linha). Se o pai não tem chave nenhuma,
                    # dropa (inevitável).
                    parent_has_key = not (
                        parent_df.select(*pcols).dropna().isEmpty())
                    if parent_has_key:
                        reuse = not (set(cols) & pk_set)
                        logger.info(
                            "null_orphan_fks: %s.%s -> %s: FK NOT NULL órfã; "
                            "REBIND para chave válida do pai (%s).",
                            child, ",".join(cols), parent,
                            "linha preservada" if reuse
                            else "FK participa da PK: só chaves livres, sem reciclagem")
                        child_df = _rebind_orphan_fk_to_valid_parent(
                            child_df, parent_df, cols, pcols,
                            seed=_stable_seed(0, child, parent, tuple(cols)),
                            allow_reuse=reuse)
                    else:
                        logger.warning(
                            "null_orphan_fks: %s.%s -> %s: FK NOT NULL órfã e pai "
                            "SEM chave sintética; DROPANDO linha(s) órfã(s) "
                            "(inevitável).", child, ",".join(cols), parent)
                        child_df = _drop_orphan_rows(child_df, parent_df, cols, pcols)
                mudou_tabela = True
            if mudou_tabela:
                # Congela ANTES de qualquer filha ler esta tabela como pai (e
                # antes do write): rebind/anula embutem joins sobre numeração
                # instável; sem materializar, leituras subsequentes poderiam
                # reavaliar o plano e ver dado diferente do que será gravado.
                synthetic[child] = child_df.localCheckpoint(eager=True)
                mudou_algo = True
        if not mudou_algo:
            if rodada > 1:
                logger.info(
                    "null_orphan_fks: convergiu na rodada %d.", rodada)
            break
    else:
        logger.warning(
            "null_orphan_fks: NÃO convergiu em %d rodadas — ainda havia "
            "mudança na última. Órfãos residuais possíveis; verifique ciclos "
            "de FK whole-PK entre as tabelas alteradas.", MAX_RODADAS)
    return synthetic


def _null_efetivo_counts(df: DataFrame, cols: list[str]) -> dict[str, int]:
    """Contagem de nulos EFETIVOS por coluna, numa única passada (um agg).

    Nulo efetivo = NULL do Spark e, para coluna STRING, também string vazia /
    só espaços: o Oracle armazena '' como NULL, então uma coluna NOT NULL
    string com '' passa no isNull() do Spark mas quebra o append com
    ORA-01400 (caso real: linha "vazia" vinda da origem em lookups static —
    DETENTOR_IF.NOM_DETENTOR_IF, UNIDADE_MEDIDA.COD_UNIDADE_MEDIDA etc. — que
    o assert com isNull() puro deixava passar e o validador pós-escrita
    acusava como "NULL/empty"; hoje essas linhas são consertadas na leitura
    da fonte por _repara_not_null_origem)."""
    present = [c for c in cols if c in df.columns]
    if not present:
        return {}
    aggs = []
    for c in present:
        eff = F.col(c).isNull()
        if _is_string_type(_get_field_type(df, c)):
            eff = eff | (F.trim(F.col(c)) == F.lit(""))
        aggs.append(F.count(F.when(eff, F.lit(1))).alias(c))
    row = df.agg(*aggs).first()
    return {c: int(row[c]) for c in present}


def assert_not_null_ok(synthetic: dict, comp_specs: dict) -> None:
    """Barra a escrita se alguma coluna NOT NULL ficou nula após todos os passes.

    Rede de segurança final contra ORA-01400: percorre as colunas declaradas em
    not_null_cols de cada tabela e conta nulos. Se qualquer uma tiver nulo,
    levanta ValueError com a tabela/coluna/contagem — melhor abortar o
    componente com log claro do que gravar dado que o append vai rejeitar.

    Só custa um agg de contagem por tabela com not_null_cols; tabelas sem a
    lista (spec gerado sem cols_real.csv) são puladas — nesse caso não há como
    validar e o comportamento antigo prevalece.

    C1 — distingue a NATUREZA da coluna NOT NULL nula, porque o CONSERTO é em
    lugar diferente:
      - NOT NULL que É FK -> algum passe (rebind/fecho/neutralização) deveria
        tê-la resolvido; se sobrou nula, é bug DESSES passes ou pai sem chave.
      - NOT NULL que NÃO é FK -> nenhum passe deste pipeline toca (rebind/anula
        só mexem em coluna de FK). Ela nasceu nula na SÍNTESE/bootstrap/
        postprocess ou já vinha nula da origem. Insistir no null_orphan não
        resolve — o conserto é na geração da coluna ou no dado de entrada.
    Ambos os casos ABORTAM (não gravar dado que o append rejeita), mas com
    mensagens separadas apontando o lugar certo, para não virar "beco sem
    saída" silencioso (rodar de novo sem mudar nada falharia igual).
    """
    problemas_fk: list[str] = []
    problemas_naofk: list[str] = []
    vazias_alvo: list[str] = []
    for table, cfg in comp_specs.items():
        df = synthetic.get(table)
        if df is None:
            continue
        # Tabela ALVO (não-static) que ficou VAZIA após os passes = sinal de que
        # um drop apagou tudo (FK NOT NULL 100% órfã). Melhor abortar com log do
        # que gravar tabela vazia silenciosamente (o rebind deve evitar isso;
        # esta é a rede de segurança). EXCEÇÃO legítima: subtipo de CONDICAO_IF
        # cujo tipo não tem NENHUMA linha no pai sintético — aí vazio é o
        # resultado correto (domínio/amostra sem o subtipo; ver
        # _subtipo_vazio_e_legitimo), não um drop acidental.
        if not cfg.get("static") and df.isEmpty():
            if _subtipo_vazio_e_legitimo(synthetic, table):
                logger.warning(
                    "assert_not_null_ok: %s vazia e LEGITIMA — subtipo de "
                    "CONDICAO_IF (tipo=%s) sem nenhuma linha do pai desse "
                    "tipo no dominio/amostra; tabela sera gravada vazia.",
                    table, TIPO_BY_SUBTYPE.get(table))
            else:
                vazias_alvo.append(table)
        nn = [c for c in _not_null_cols(cfg) if c in df.columns]
        if not nn:
            continue
        # Conjunto das colunas que participam de ALGUMA FK desta tabela.
        fk_cols = {c for fk in _fk_list(cfg) for c in (fk.get("columns") or [])}
        # Nulo EFETIVO (NULL ou '' em coluna string): é o que o Oracle grava
        # como NULL no append — isNull() puro deixava '' passar.
        counts = _null_efetivo_counts(df, nn)
        for col, n in counts.items():
            if n > 0:
                if col in fk_cols:
                    problemas_fk.append(f"{table}.{col}={n} nulo(s)")
                else:
                    problemas_naofk.append(f"{table}.{col}={n} nulo(s)")
    msgs: list[str] = []
    if problemas_fk:
        msgs.append(
            "Colunas NOT NULL de FK ficaram nulas (NULL ou '' string) após os "
            "passes (o append quebraria com ORA-01400): "
            + "; ".join(sorted(problemas_fk))
            + ". CONSERTO: verifique rebind/fecho dessas FKs (pai sem chave "
            "sintética? aresta de ciclo?), não a origem.")
    if problemas_naofk:
        msgs.append(
            "Colunas NOT NULL que NÃO são FK ficaram nulas (NULL ou '' string; "
            "o append quebraria com ORA-01400): "
            + "; ".join(sorted(problemas_naofk))
            + ". CONSERTO: _repara_not_null_origem preenche essas colunas na "
            "leitura da fonte (string->'0', número->0, data->hoje); se ainda "
            "assim ficou nulo, ou o tipo físico da coluna não é reparável, ou "
            "o nulo foi introduzido DEPOIS da leitura (síntese/postprocess/"
            "bind) — investigue esse passe (ou reveja not_null_cols do spec).")
    if vazias_alvo:
        msgs.append(
            "Tabela(s)-alvo ficaram VAZIAS após a síntese (provável drop de FK "
            "NOT NULL 100% órfã): " + ", ".join(sorted(vazias_alvo))
            + ". Rode com --limit maior (amostra pegou poucas linhas do pai) ou "
            "verifique o fecho/rebind dessas FKs.")
    if msgs:
        raise ValueError(" | ".join(msgs))


def assert_polymorphism_ok(synthetic: dict, comp_specs: dict) -> None:
    """Barra a escrita se o polimorfismo de CONDICAO_IF ficou inconsistente.

    Rede de segurança final contra o ClassCastException do batch da NoMe
    (JurosFlutuanteDO cannot be cast to JurosFixoDO). Após bind_shared_key_children,
    verifica as três invariantes que a aplicação assume para cada NUM_CONDICAO_IF
    (o subtipo é resolvido pela tabela física que contém a linha):

      1a.dangling  — CONDICAO_IF sem NENHUMA linha em tabela-subtipo alguma:
                     tipo CONHECIDO (SUBTYPE_BY_TIPO) sem linha na tabela do
                     seu tipo (some ao incluir o subtipo em TABELAS_ALVO,
                     gera_spec_config.py) e também tipo FORA do mapa curado
                     (paridade com o validador pós-escrita, que não filtra
                     por tipo conhecido — nesse caso o conserto é curar
                     SUBTYPE_BY_TIPO nos dois lados).
      1a.ambiguous — mesmo NUM_CONDICAO_IF presente em MAIS DE UMA tabela-subtipo.
      1b.mismatch  — subtipo presente numa tabela que NÃO corresponde ao
                     COD_TIPO_CONDICAO_IF do pai.

    Só roda quando CONDICAO_IF está no componente. Tabelas-subtipo ausentes do
    run não geram dangling para os tipos que elas representam SE nenhuma linha
    do pai tiver aquele COD_TIPO — mas se houver, é dangling e abortamos, porque
    o append passaria e o batch quebraria depois (falha silenciosa pior).
    """
    cond = synthetic.get(CONDICAO_IF_TABLE)
    if cond is None or not _has_column(cond, CONDICAO_IF_PK):
        return
    if not _has_column(cond, CONDICAO_IF_TIPO_COL):
        logger.warning(
            "assert_polymorphism_ok: CONDICAO_IF sem %s; checagem de "
            "polimorfismo pulada.", CONDICAO_IF_TIPO_COL)
        return

    cond_norm = cond.select(
        F.col(CONDICAO_IF_PK).cast("string").alias("__cnci"),
        _condicao_if_tipo_norm_expr().alias("__ctipo"),
    )

    # Membership de cada NUM_CONDICAO_IF nas tabelas-subtipo presentes no run.
    memb = None
    for subtype in TIPO_BY_SUBTYPE:  # só tabelas-subtipo conhecidas
        sdf = synthetic.get(subtype)
        if sdf is None or not _has_column(sdf, CONDICAO_IF_PK):
            continue
        piece = (sdf.select(F.col(CONDICAO_IF_PK).cast("string").alias("__nci"))
                    .withColumn("__tbl", F.lit(subtype)))
        memb = piece if memb is None else memb.unionByName(piece)

    problemas: list[str] = []

    # Tipos concretos conhecidos presentes no pai mas SEM subtipo no run:
    # dangling garantido para essas linhas. Tipos FORA do mapa curado são
    # tratados adiante por membership (nenhuma tabela do run pode contê-los
    # -> dangling também; o validador pós-escrita reprova igual).
    tipos_presentes = {str(r["__ctipo"]) for r in
                       cond_norm.select("__ctipo").distinct().collect()
                       if r["__ctipo"] is not None}
    tipos_desconhecidos = sorted(
        t for t in tipos_presentes if t not in SUBTYPE_BY_TIPO)
    for tipo in sorted(tipos_presentes):
        subtype = SUBTYPE_BY_TIPO.get(tipo)
        if subtype is None:
            continue  # fora do mapa curado -> checado por membership abaixo
        if synthetic.get(subtype) is None:
            n = cond_norm.where(F.col("__ctipo") == F.lit(tipo)).count()
            if n:
                problemas.append(
                    f"1a.dangling: {n} CONDICAO_IF com COD_TIPO={tipo} "
                    f"({subtype}) mas a tabela {subtype} não está no run")

    if memb is None and tipos_desconhecidos:
        # Sem NENHUMA tabela-subtipo no run, toda linha de tipo desconhecido é
        # dangling por definição (as de tipo conhecido já foram acusadas acima).
        n = cond_norm.where(F.col("__ctipo").isin(tipos_desconhecidos)).count()
        if n:
            problemas.append(
                f"1a.dangling: {n} CONDICAO_IF com COD_TIPO fora do mapa "
                f"curado ({tipos_desconhecidos}) e nenhuma tabela-subtipo no "
                "run — o Hibernate não consegue tipá-las e o validador "
                "pós-escrita reprova (1a.dangling); se o tipo tem tabela "
                "física, adicione-a a SUBTYPE_BY_TIPO (aqui e no validador) "
                "e a TABELAS_ALVO")

    if memb is not None:
        agg = memb.groupBy("__nci").agg(F.collect_set("__tbl").alias("__tbls"))
        joined = cond_norm.join(agg, cond_norm["__cnci"] == agg["__nci"], "left")
        joined = joined.withColumn(
            "__n_tbls",
            F.when(F.col("__tbls").isNull(), F.lit(0)).otherwise(F.size(F.col("__tbls"))))

        # 1a.dangling: pai de tipo concreto conhecido sem subtipo algum.
        map_pairs: list = []
        for k, v in SUBTYPE_BY_TIPO.items():
            map_pairs += [F.lit(k), F.lit(v)]
        expected = F.create_map(*map_pairs)[F.col("__ctipo")]
        joined = joined.withColumn("__expected", expected)

        n_dangling = joined.where(
            (F.col("__n_tbls") == 0) & F.col("__expected").isNotNull()).count()
        if n_dangling:
            problemas.append(
                f"1a.dangling: {n_dangling} CONDICAO_IF de tipo concreto sem "
                "nenhuma linha-subtipo (Hibernate não consegue tipá-los)")

        # PARIDADE com o validador pós-escrita: lá o 1a.dangling NÃO filtra
        # por tipo conhecido — linha de COD_TIPO fora do mapa curado sem
        # linha-subtipo em NENHUMA tabela do run também é ERROR. Sem este
        # check o engorda gravava e o validador reprovava depois.
        n_unk = joined.where(
            (F.col("__n_tbls") == 0) & F.col("__expected").isNull()).count()
        if n_unk:
            problemas.append(
                f"1a.dangling: {n_unk} CONDICAO_IF com COD_TIPO fora do mapa "
                f"curado ({tipos_desconhecidos}) e sem linha em nenhuma "
                "tabela-subtipo do run; se o tipo tem tabela física, "
                "adicione-a a SUBTYPE_BY_TIPO (aqui e no validador) e a "
                "TABELAS_ALVO")

        # 1a.ambiguous: presente em mais de uma tabela-subtipo.
        n_amb = joined.where(F.col("__n_tbls") > 1).count()
        if n_amb:
            problemas.append(
                f"1a.ambiguous: {n_amb} NUM_CONDICAO_IF em MAIS DE UMA "
                "tabela-subtipo (causa direta do ClassCastException)")

        # 1b.mismatch: subtipo único, mas não o que COD_TIPO indica.
        n_mismatch = joined.where(
            (F.col("__n_tbls") == 1)
            & F.col("__expected").isNotNull()
            & (F.col("__tbls")[0] != F.col("__expected"))).count()
        if n_mismatch:
            problemas.append(
                f"1b.mismatch: {n_mismatch} CONDICAO_IF cuja tabela-subtipo não "
                "corresponde ao COD_TIPO_CONDICAO_IF")

    if problemas:
        raise ValueError(
            "Polimorfismo de CONDICAO_IF inconsistente (o batch da NoMe "
            "quebraria com ClassCastException): " + " | ".join(problemas)
            + ". CONSERTO: garanta que todo subtipo de CDB está em TABELAS_ALVO "
            "(gera_spec_config.py) e que bind_shared_key_children vinculou cada "
            "subtipo só às chaves do seu COD_TIPO_CONDICAO_IF.")


def release(*dataframes) -> None:
    for df in dataframes:
        if df is None:
            continue
        try:
            df.unpersist()
        except Exception:
            pass


def _delete_path(spark: SparkSession, path: str) -> None:
    """Recursively delete exactly `path` via the Hadoop FileSystem API.

    Scoped to a single table prefix. Used instead of Spark's mode("overwrite"),
    whose delete-before-write removes the shared parent prefix on the OCI HDFS
    connector and clobbers sibling tables.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    if fs.exists(jpath):
        fs.delete(jpath, True)


def write_synthetic_table(spark: SparkSession, df: DataFrame, out_path: str) -> None:
    """Write one synthetic table to its own prefix without touching siblings.

    Delete only this table's prefix, then append. Equivalent to per-table
    overwrite, but the destructive step is scoped to exactly `out_path`.
    """
    table_name = out_path.rstrip("/").rsplit("/", 1)[-1]
    df_out = _sanitize_columns_for_save(df, table_name)
    _delete_path(spark, out_path)
    df_out.write.mode("append").parquet(out_path)


def load_specs(spark: SparkSession, specs_uri: str) -> dict:
    records = spark.sparkContext.wholeTextFiles(specs_uri).collect()
    if len(records) != 1:
        raise ValueError(
            f"Expected exactly one specs object at `{specs_uri}`, found {len(records)}. "
            "DATAGEN_SPECS_URI must point at a single specs.json file, not a prefix."
        )
    try:
        parsed = json.loads(records[0][1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"specs.json at `{specs_uri}` is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"specs.json at `{specs_uri}` must be a non-empty object.")
    return normalize_specs(parsed)


def _dbg_null_counts(df: DataFrame, cols: list[str]) -> dict[str, int]:
    """Contagem de nulos por coluna numa única passada (um só agg/action)."""
    present = [c for c in cols if c in df.columns]
    if not present:
        return {}
    row = df.agg(*[
        F.count(F.when(F.col(c).isNull(), F.lit(1))).alias(c) for c in present
    ]).first()
    return {c: int(row[c]) for c in present}


def _dbg_orphan_count(child_df: DataFrame, parent_df: DataFrame,
                      cols: list[str], pcols: list[str],
                      *, broadcast_parent: bool = False) -> int:
    """Chaves-filhas distintas, NÃO nulas, sem correspondência no pai.

    Espelha MATCH SIMPLE: linhas com QUALQUER coluna da FK nula não são
    contadas como órfãs (a checagem de FK composta é desligada por elas). É
    exatamente a definição que o pre-append check / o load Oracle aplicam.

    broadcast_parent: só sob --limit, quando as chaves do pai são pequenas.
    No run completo o build side (chaves distintas de um pai de 50M–1B linhas)
    estoura o broadcast -> deixa sem, e o AQE decide.
    """
    child_keys = (child_df.select(*cols).dropna().distinct())
    parent_keys = (parent_df
                   .select(*[F.col(p).alias(c) for c, p in zip(cols, pcols)])
                   .dropna().distinct())
    if broadcast_parent:
        parent_keys = F.broadcast(parent_keys)
    return child_keys.join(parent_keys, on=cols, how="left_anti").count()


def debug_fk_integrity_report(stage: str, tables: dict, comp_specs: dict,
                              *, label: str = "") -> None:
    """Loga nulos + órfãos por coluna de FK, para TODAS as FKs do componente.

    Executado ENTRE estágios do pipeline (só quando o debug está ligado) para
    revelar em QUE estágio uma coluna passa a ter nulos ou órfãos. `tables` é o
    dict {nome_tabela: DataFrame} do estágio; `comp_specs` são as specs (forma
    dict) do componente. `parent` pode ou não estar em `tables` — se não
    estiver, a contagem de órfãos é pulada (reportada como n/d) mas os nulos da
    coluna ainda são reportados.

    O relatório NÃO altera nenhum DataFrame; apenas dispara actions de
    contagem. Colunas em DEBUG_WATCH_COLUMNS são marcadas com "<<<".
    """
    if not _dbg():
        return
    prefix = f"[DEBUG {stage}]" + (f" {{{label}}}" if label else "")
    logger.debug("%s — relatório de integridade de FK (nulos / órfãos)", prefix)
    for table in sorted(tables):
        df = tables.get(table)
        if df is None:
            continue
        cfg = comp_specs.get(table)
        if cfg is None:
            continue
        fks = _fk_list(cfg)
        try:
            total = df.count()
        except Exception as exc:  # pragma: no cover - defensivo
            logger.debug("%s   %s: falha ao contar linhas: %s", prefix, table, exc)
            continue
        if not fks:
            logger.debug("%s   %s: %d linha(s), sem FK", prefix, table, total)
            continue
        # Todas as colunas de FK da tabela, para um único agg de nulos.
        all_fk_cols: list[str] = []
        for fk in fks:
            for c in (fk.get("columns") or []):
                if c not in all_fk_cols:
                    all_fk_cols.append(c)
        nulls = _dbg_null_counts(df, all_fk_cols)
        logger.debug("%s   %s: %d linha(s)", prefix, table, total)
        for fk in fks:
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            if not cols or len(cols) != len(pcols):
                logger.debug("%s     FK malformada -> %s: cols=%s pcols=%s",
                             prefix, parent, cols, pcols)
                continue
            watch = any((table, c) in DEBUG_WATCH_COLUMNS for c in cols)
            mark = " <<<" if watch else ""
            null_desc = ", ".join(
                f"{c}={nulls.get(c, 'n/d')}"
                + (f"({100.0 * nulls[c] / total:.1f}%)" if total and c in nulls else "")
                for c in cols
            )
            parent_df = tables.get(parent)
            if parent_df is None:
                orphan_desc = "órfãos=n/d (pai fora do estágio)"
            elif not DEBUG_SAMPLED:
                # Run completo (50M–1B linhas): o anti-join de órfãos é
                # caro demais. Reporta só nulos; use --limit para os órfãos.
                orphan_desc = "órfãos=pulado (full run; rode com --limit)"
            else:
                try:
                    n_orphan = _dbg_orphan_count(df, parent_df, cols, pcols,
                                                 broadcast_parent=True)
                    orphan_desc = f"órfãos={n_orphan}"
                except Exception as exc:  # pragma: no cover - defensivo
                    orphan_desc = f"órfãos=ERRO({exc})"
            logger.debug("%s     %s.%s -> %s.%s | nulos: %s | %s%s",
                         prefix, table, ",".join(cols), parent, ",".join(pcols),
                         null_desc, orphan_desc, mark)


def engorda(spark, config, specs, scale_factor, seed, continue_on_error,
            limit=None, pk_offset=None, pk_safety_band=None,
            dt_vencimento_prazo_dias=None) -> None:
    components = connected_components(specs)
    save_base = synthetic_base_path(config)
    total = len(components)
    # Órfãos no relatório de debug só são computados sob --limit (chaves de pai
    # pequenas). No full run, o report emite só nulos (ver debug_fk_integrity_report).
    _set_debug_sampled(limit is not None)
    if _dbg():
        logger.info("Debug ATIVO. Relatório de FK por estágio %s.",
                    "com nulos+órfãos (amostra)" if limit is not None
                    else "SÓ com nulos (full run; use --limit para incluir órfãos)")
    if limit is not None:
        logger.info("Input limit active: reading at most %d row(s) per raw table", limit)
    if pk_offset is not None:
        logger.info("PK offset floor active: synthetic PKs start at >= %d", pk_offset)
    if pk_safety_band is not None:
        logger.info("PK safety band active: synthetic PKs start at true_max + %d", pk_safety_band)
    logger.info("Loaded %d table(s) in %d component(s)", len(specs), total)
    _warn_filtros_fonte_sem_not_null(specs)
    run_started = time.perf_counter()
    failures: list[str] = []
    engorda_ts = _normalize_engorda_ts(None)
    logger.info("Data engorda do run: %s", engorda_ts.strftime("%Y-%m-%d %H:%M:%S"))

    for index, comp in enumerate(sorted(components, key=lambda c: sorted(c)[0]), start=1):
        comp_specs = {t: specs[t] for t in comp}
        label = ",".join(sorted(comp))
        comp_tables = {}
        synthetic = {}
        try:
            started = time.perf_counter()
            if limit is not None:
                # Referential sampling: parent rows first, then keep only children
                # whose FK lands in a sampled parent -> FK-consistent under --limit.
                comp_tables = referential_sample(spark, config, comp_specs, limit)
            else:
                # Run COMPLETO: MESMA propagação referencial do filtro de
                # domínio, agora sem cap de linhas. Os predicados de fonte do
                # CDB simplificado (FILTROS_FONTE) são aplicados na leitura de
                # CADA tabela (primeira etapa) e a raiz (TABELAS_RAIZ_FILTRO)
                # ancora a descida por chave pela árvore de FK. Ao final, o
                # fecho ascendente (completa_pais_referenciados) puxa da FONTE
                # JÁ FILTRADA as linhas de pai referenciadas por filhas
                # mantidas -> um pai fora do produto NÃO volta; o subset sai
                # FK-fechado dentro do produto e os órfãos remanescentes
                # (pai fora do produto ou órfão de produção) são neutralizados.
                comp_tables = referential_sample(spark, config, comp_specs, None)
            counts = {t: comp_tables[t].count() for t in comp}
            for t in comp:
                if comp_specs[t].get("static") and comp_specs[t].get("n_rows") is not None:
                    logger.warning("Table %s is static; ignoring n_rows override", t)
            n_rows = effective_n_rows(comp_specs, counts, scale_factor)
            logger.info("[%d/%d] Component {%s}: n_rows=%s", index, total, label, n_rows)
            pk_max = compute_pk_maxes(spark, config, comp_specs,
                                      floor=(pk_offset or 0), band=(pk_safety_band or 0),
                                      n_rows=n_rows)
            if pk_max:
                logger.info("[%d/%d] true PK max per table: %s", index, total, pk_max)
            prazo_vencimento_por_tabela: dict[str, int] = {}
            for t, cfg in comp_specs.items():
                cfg_prazo = cfg.get("dt_vencimento_prazo_dias")
                if cfg_prazo is not None:
                    prazo_vencimento_por_tabela[t] = int(cfg_prazo)
                elif dt_vencimento_prazo_dias is not None:
                    prazo_vencimento_por_tabela[t] = int(dt_vencimento_prazo_dias)
            # Synthesize (validate_mode="none": we make FKs load-safe ourselves
            # via null_orphan_fks instead of failing the whole component on an
            # orphan), then write each table with a scoped delete (Spark's
            # overwrite clobbers siblings on the OCI connector).
            synthetic = run_synthesis_from_tables(
                comp_tables, comp_specs,
                n_rows_by_table=n_rows, seed=seed,
                pk_max_by_table=pk_max,
                engorda_ts=engorda_ts,
                dt_vencimento_prazo_dias_by_table=prazo_vencimento_por_tabela,
                validate_mode="none", verbose=False,
                # Under --limit the referential-sample chain makes synthesis plans
                # deep enough to OOM the driver; truncate work lineage via eager
                # localCheckpoint. The full run keeps persist (recomputable).
                truncate_lineage=(limit is not None),
            )
            # Logo após a síntese (bootstrap + remap de FK + geração de PK),
            # ANTES de bind/null_orphan. Órfãos aqui = o remap de FK não
            # encontrou o pai sintético (aresta quebrada por ciclo, mapping
            # ausente, FK não preservada por _fk_has_data_problem). É o ponto
            # em que a síntese "quebra" uma coluna que estava íntegra na fonte.
            debug_fk_integrity_report("4.after_synthesis", synthetic, comp_specs,
                                      label=label)
            synthetic = bind_shared_key_children(synthetic, comp_specs)
            debug_fk_integrity_report("5.after_bind_shared_key", synthetic,
                                      comp_specs, label=label)
            # Alinha o pai aos subtipos: remove de CONDICAO_IF as linhas de
            # tipo concreto que ficaram sem linha-subtipo após o bind (subtipo
            # com menos linhas que chaves do pai — inclusive zero, caso do CDB
            # simplificado sem AMORTIZACAO/PARTICIPACAO_LUCROS/RESET/
            # DESDOBRAMENTO). Precisa vir ANTES de null_orphan_fks, que então
            # neutraliza as filhas que referenciavam as chaves removidas.
            synthetic = alinha_condicao_if_aos_subtipos(synthetic, comp_specs)
            debug_fk_integrity_report("5b.after_align_condicao_if", synthetic,
                                      comp_specs, label=label)
            # Self-FKs genuínas são remapeadas DENTRO da síntese (mapping
            # old->new da própria tabela), preservando estrutura e
            # auto-loops; null_orphan_fks segue como rede de segurança.
            synthetic = null_orphan_fks(synthetic, comp_specs)
            # MATERIALIZA o resultado do rebind ANTES de qualquer leitura.
            # null_orphan_fks/_rebind usam _with_contiguous_row_id, que depende
            # de monotonically_increasing_id() — NÃO-determinístico entre
            # avaliações. Sem congelar aqui, o plano lazy é reavaliado 3x
            # (debug 6, assert, write), cada vez com numeração diferente: o
            # pareamento órfã->chave muda, então linhas rebindadas numa
            # avaliação voltam a ser órfãs em outra. Era ISSO que fazia o
            # "[DEBUG 6] nulos=0" divergir do "assert=13k nulos" (mesmo plano,
            # resultados diferentes) — e o que seria ESCRITO era uma 3ª versão.
            # localCheckpoint(eager) congela o resultado: debug, assert e write
            # passam a ver EXATAMENTE o mesmo dado.
            #
            # SÓ materializa tabelas que bind/null_orphan PODEM ter tocado —
            # isto é, que têm ao menos uma FK. Tabela sem FK não passa por
            # rebind/anulação/bind, então seu synthetic[t] já veio materializado
            # da síntese (synth = _persist + count); recheckpointá-la só
            # gastaria disco à toa. Isso reduz a pressão de scratch disk no full
            # run (tabelas de 50M–1B linhas sem FK não são reescritas aqui).
            for _name in list(synthetic):
                _df = synthetic.get(_name)
                if _df is None:
                    continue
                _cfg = comp_specs.get(_name) or {}
                if not _fk_list(_cfg):
                    continue  # sem FK -> não foi tocada por bind/null_orphan
                synthetic[_name] = _df.localCheckpoint(eager=True)
            # Estado FINAL escrito no destino = o que o pre-append check lê.
            # Agora congelado: os nulos aqui são os mesmos do assert e do write.
            debug_fk_integrity_report("6.final_before_write", synthetic,
                                      comp_specs, label=label)
            # Rede de segurança: aborta o componente ANTES de gravar se alguma
            # coluna NOT NULL ficou nula (evita ORA-01400 no append). Só valida
            # tabelas cujo spec tem not_null_cols (gerado com cols_real.csv).
            assert_not_null_ok(synthetic, comp_specs)
            # Rede de segurança do polimorfismo: aborta ANTES de gravar se
            # CONDICAO_IF ficou com subtipo dangling/ambíguo/mismatch (evita o
            # ClassCastException do batch da NoMe, que o append NÃO detecta).
            assert_polymorphism_ok(synthetic, comp_specs)
            for name, df in synthetic.items():
                out_path = f"{save_base}/{name}"
                logger.info("[%d/%d] writing %s -> %s", index, total, name, out_path)
                write_synthetic_table(spark, df, out_path)
            logger.info("[%d/%d] Component {%s} done in %.1fs",
                        index, total, label, time.perf_counter() - started)
        except Exception as exc:
            logger.exception("[%d/%d] Component {%s} failed: %s", index, total, label, exc)
            failures.append(label)
            if not continue_on_error:
                raise
        finally:
            release(*comp_tables.values(), *synthetic.values())
            try:
                spark.catalog.clearCache()
            except Exception:
                pass

    logger.info("Finished: %d/%d component(s) in %.1fs",
                total - len(failures), total, time.perf_counter() - run_started)
    if failures:
        logger.error("Failed component(s): %s", "; ".join(failures))
        sys.exit(1)


# Workload-level Spark settings, independent of cluster shape. Driver/executor
# OCPU, memory and count are tuned via the OCI Data Flow UI, not here.
_STATIC_SPARK_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    # Answer max(pk) from Parquet footer stats (metadata only, no scan) so
    # computing each table's true max PK stays fast even under --limit.
    "spark.sql.parquet.aggregatePushdown": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    # Tolerate long GC pauses on large (fat) executors instead of declaring them
    # lost — losing 1 of few executors triggers expensive recompute cascades.
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "30s",
    # Survive transient shuffle-block unavailability (a GC pause makes an executor
    # briefly unreachable) instead of failing the fetch -> with few executors a
    # FetchFailed forces a full map-stage recompute, and 4 of them abort the job.
    "spark.shuffle.io.maxRetries": "10",
    "spark.shuffle.io.retryWait": "15s",
    # Overhead as a fraction of executor memory (Spark 3.3+), so it auto-scales
    # with whatever shape is picked in the Data Flow UI. 0.2 (~20%) suits PySpark
    # + shuffle-heavy work; the 0.1 default gets containers RM-killed at scale.
    # NOTE: do NOT also set the absolute spark.executor.memoryOverhead in the UI
    # — the absolute wins over the factor and would pin overhead to one shape.
    "spark.executor.memoryOverheadFactor": "0.2",
}

# Adaptive Query Execution + shuffle sizing. These are runtime SQL confs, so we
# also re-apply them to an already-active session: on Data Flow the context may
# be created by the platform before this runs, which would ignore builder confs.
_RUNTIME_SPARK_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # AQE coalesces post-shuffle partitions toward this target size, so the
    # high partition count below never lands as giant reducer tasks.
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
    # Fewer, larger MAP tasks (default is 128m) -> fewer map outputs. Total
    # shuffle blocks = map_tasks x shuffle.partitions, and a huge block count is
    # what causes FetchFailedException at scale. 512m cuts map tasks ~4x.
    "spark.sql.files.maxPartitionBytes": "512m",
    # Initial/max REDUCE partition count. Balances two forces: large enough that
    # no partition is oversized (AQE only MERGES, never SPLITS outside skewed
    # joins), but not so large that map_tasks x this explodes the block count and
    # triggers FetchFailed. ~0.5-1GB partitions on 128GB executors; AQE coalesces
    # small components back down via the advisory size above.
    "spark.sql.shuffle.partitions": "8000",
}


def create_spark_session(app_name: str) -> SparkSession:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in {**_STATIC_SPARK_CONF, **_RUNTIME_SPARK_CONF}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

    # Guarantee the AQE/shuffle confs apply even if the session pre-existed.
    for key, value in _RUNTIME_SPARK_CONF.items():
        spark.conf.set(key, value)
    return spark


def main() -> None:
    args = parse_arguments()
    _set_debug(bool(args.debug))
    config = get_engorda_env()
    spark = create_spark_session("DataGenEngordaTables")
    try:
        specs_uri = args.specs or config["DATAGEN_SPECS_URI"]
        specs = load_specs(spark, specs_uri)
        engorda(spark, config, specs, args.scale_factor, args.seed,
                args.continue_on_error, args.limit, args.pk_offset, args.pk_safety_band,
                args.dt_vencimento_prazo_dias)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
