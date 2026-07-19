#!/usr/bin/env python3
"""
clona_instrumentos.py — Etapa 2: clonagem por entidade (clone-and-remap).

Em vez de sintetizar tabela a tabela (bootstrap do engorda_tables.py, que
perde o fan-out por instrumento e recombina colunas em combinações de negócio
inválidas), este job clona INSTRUMENTOS INTEIROS: seleciona N valores de
NUM_IF do domínio do produto (query de validação do notebook — ver
_dominio_num_if_produto), copia todas as linhas do fecho
referencial que pertencem a esses instrumentos e reescreve APENAS chaves:

  1. NUM_IF (PK de INSTRUMENTO_FINANCEIRO) -> NUM_IF novo, acima do max real;
  2. PKs surrogate de cada tabela -> offset próprio por tabela, acima do max
     real daquela tabela (mesma lógica do compute_pk_maxes do engorda);
  3. PKs compartilhadas (shared-key, ex.: subtipos de CONDICAO_IF) -> seguem
     o mapeamento do pai;
  4. FKs para tabelas clonadas -> reescritas pelo mapeamento do pai.

Além das chaves, um conjunto FECHADO de colunas de DATA recebe a data do run
(regras de engorda — ver ENGORDA_COL_*): DAT_INCLUSAO, DAT_ALTERACAO,
DAT_INCLUSAO_REGISTRO, DAT_EMISSAO e DAT_VENCIMENTO. TODAS as demais colunas
ficam intocadas — é isso que preserva as combinações de negócio e o
polimorfismo Hibernate de CONDICAO_IF (a linha-subtipo é copiada junto, na
tabela certa, por construção; nada é recombinado).

DIRIGIDO SÓ PELO spec_config.json (o mesmo do engorda_tables.py, gerado por
gera_spec_config.py). Não usa arquivo de decisões nem conexão Oracle.

POLÍTICAS PADRÃO (onde haveria decisão manual, vale a regra abaixo — cada uma
é logada por tabela para auditoria):

  * FK para tabela clonada: reescreve SE o registro referenciado está no lote
    de clonagem; senão MANTÉM o valor original (que continua existindo no
    banco -> FK válida). Isso cobre self-references (NUM_IF_ORIGEM) e ligações
    entre instrumentos sem decisão manual.
  * Pertencimento ao lote ("de quem é esta linha?"): desce a árvore a partir
    de INSTRUMENTO_FINANCEIRO pelas FKs de VÍNCULO PRINCIPAL — aquelas cujas
    colunas na filha têm o MESMO NOME das colunas da PK do pai (convenção do
    schema CETIP: NUM_IF -> NUM_IF, NUM_CONDICAO_IF -> NUM_CONDICAO_IF). FKs
    com nome divergente (ex.: NUM_IF_ORIGEM) são LATERAIS: não puxam linhas
    para o lote, só são remapeadas-se-no-lote. Self-FKs nunca expandem o lote.
  * PK surrogate: coluna única, numérica e fora de qualquer FK -> offset
    próprio acima do max real (com --pk-safety-band). PK com componente de FK
    para pai clonado -> segue o pai. PK sem regra possível -> ABORTA listando
    as tabelas (use --tratar-como-static para excluí-las da clonagem).
  * Chaves únicas de NEGÓCIO (unique constraints fora da PK, ex.: COD_IF):
    copiadas como estão. Risco residual: ORA-00001 na carga se existir unique
    policiada sobre coluna não remapeada. Evidência a favor do padrão: o
    bootstrap do engorda já copia esses códigos duplicados hoje e a carga
    passa. Se a carga acusar ORA-00001, rode o diagnostica_clonagem.py para
    identificar a chave e definimos a regra de regeneração.
  * Tabelas static do spec: não são clonadas nem escritas; FKs para elas
    mantêm o valor original (o pai static continua existindo).

VALIDAÇÕES PRÉ-ESCRITA (abortam o job, nada é gravado parcial por tabela):
  * count(clones) == count(lote) * K por tabela;
  * PK nova: sem duplicata interna e (para offset) acima do max real do
    Parquet COMPLETO da tabela (todas as linhas de produção, sem filtro);
  * colunas NOT NULL do spec sem nulo efetivo (NULL ou '' string) nos clones.

SAÍDA: Parquet por tabela em
    {DATAGEN_SYNTHETIC_BASE_URI}/{DATAGEN_CLONE_PREFIX}/{TABELA}
(mesmo layout de saída do engorda — o processo de carga existente lê e faz o
append no Oracle). Também grava MAPA_CLONE_NUM_IF (NUM_IF_ORIG, K, NUM_IF_NOVO)
para auditoria. Com --dry-run nada é gravado: só validações e relatório.

USO (OCI Data Flow — mesmas envs do engorda_tables.py):
    envs: DATAGEN_RAW_BASE_URI, DATAGEN_SPECS_URI, DATAGEN_SYNTHETIC_BASE_URI
          (+ opcionais DATAGEN_RAW_PREFIX, DATAGEN_SYNTHETIC_PREFIX,
           DATAGEN_CLONE_PREFIX — default "clones_instrumentos")
    argumentos:
      --num-ifs 12345,67890         # lista explícita (aceita 1 só), OU
      --n-instrumentos 5 --seed 42  # amostra do domínio do produto (query)
      --fator-k 3                   # clones por instrumento (default 1)
      --dry-run                     # valida e loga, não grava
      --pk-safety-band 100000       # folga acima do max real (default 0)
      --pk-passo 10                 # folga ENTRE PKs novas consecutivas (default 1)
      --offset-num-if 900000000     # início explícito p/ NUM_IF novo (opcional)
      --data-engorda 2026-07-19     # data/hora do run (default: agora)
      --prazo-vencimento-dias 30    # DAT_VENCIMENTO = data + N (default: prazo original)
      --tratar-como-static TAB1,TAB2  # excluir tabela(s) da clonagem
      --specs oci://.../spec_config.json  # override de DATAGEN_SPECS_URI

Em notebook: from clona_instrumentos import executa_clonagem (ver main()).

Comentários e logs em português; helpers copiados do engorda_tables.py estão
marcados como tal (arquivo único e autocontido, como o Data Flow espera).
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Mesmo padrão do engorda_tables.py: log do driver em stdout
# (spark_application_stdout no OCI Data Flow).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domínio (espelha engorda_tables.py — manter em sincronia).
# ---------------------------------------------------------------------------
TABELA_RAIZ = "INSTRUMENTO_FINANCEIRO"
COL_NUM_IF = "NUM_IF"
FILTRO_TIPO_IF_COLUMN = "NUM_TIPO_IF"
FILTRO_TIPO_IF_VALUE = 49  # CDB simplificado

# Predicados de fonte por tabela (cópia de engorda_tables.FILTROS_FONTE): o
# lote de clonagem nasce do produto JÁ FILTRADO, como no engorda — uma linha
# fora do produto não entra no clone.
FILTROS_FONTE: dict[str, list[tuple[str, str, object]]] = {
    "INSTRUMENTO_FINANCEIRO": [
        (FILTRO_TIPO_IF_COLUMN, "==", FILTRO_TIPO_IF_VALUE),
        ("DAT_EXCLUSAO", "isnull", None),
    ],
    "RESGATE": [
        ("COD_COND_RESGATE", "ieq", "SEM TABELA"),
        ("DAT_EXCLUSAO", "isnull", None),
    ],
    "TITULO": [
        ("COD_TIPO_ESCALONAMENTO", "isnull", None),
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
# As PKs NÃO entram aqui (nem NUM_ID_CERTIFICACAO_CETIP): quem as reescreve é o
# plano de clonagem (monta_plano + _monta_mapeamento_pk), incremental acima do
# max real da tabela INTEIRA, com a folga do --pk-safety-band e o passo do
# --pk-passo. Tratá-las de novo aqui desfazia a folga e relia o max sem
# necessidade.
#
# Para DAT_VENCIMENTO, se não for informado um prazo fixo (por tabela em
# ENGORDA_PRAZO_DIAS_POR_TABELA, ou global via --prazo-vencimento-dias), o
# código preserva o prazo original da linha clonada:
# DAT_VENCIMENTO - DAT_EMISSAO. Se esse prazo não existir, for inválido ou
# < MIN_DT_VENCIMENTO_PRAZO_DIAS, usa DEFAULT_DT_VENCIMENTO_PRAZO_DIAS.
#
# NB: as colunas de emissão/vencimento no schema real são DAT_EMISSAO /
# DAT_VENCIMENTO (prefixo DAT_) — não DT_. A aplicação é tolerante a coluna
# ausente (no-op), então um nome errado aqui vira regra silenciosamente morta.
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
# Prazo FIXO de DAT_VENCIMENTO por tabela (dias). Vazio por padrão: sem entrada
# aqui, vale o --prazo-vencimento-dias e, na falta dele, o prazo original da
# linha clonada. Entrada aqui tem precedência sobre a CLI.
ENGORDA_PRAZO_DIAS_POR_TABELA: dict[str, int] = {}
# Coluna temporária do prazo calculado (checada contra colisão em tempo de execução).
ENGORDA_PRAZO_TMP_COL = "__engorda_prazo_dias"

REQUIRED_ENV_VARS = (
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
    "DATAGEN_SPECS_URI",
)
DEFAULT_CLONE_PREFIX = "clones_instrumentos"
DEFAULT_SEED = 42
MAPA_NUM_IF_TABLE = "MAPA_CLONE_NUM_IF"

# Coluna temporária com o índice do clone (1..K). Sufixo improvável de
# colidir com colunas reais; ainda assim é checado em tempo de execução.
K_COL = "__clone_k"


# ---------------------------------------------------------------------------
# Helpers copiados/adaptados de engorda_tables.py (arquivo autocontido).
# ---------------------------------------------------------------------------
def table_path_name(table: str) -> str:
    return table.split(".", 1)[1] if "." in table else table


def raw_path(config: dict[str, str], table: str) -> str:
    parts = [config["DATAGEN_RAW_BASE_URI"]]
    if config.get("DATAGEN_RAW_PREFIX"):
        parts.append(config["DATAGEN_RAW_PREFIX"])
    parts.append(table_path_name(table))
    return "/".join(parts)


def clone_base_path(config: dict[str, str]) -> str:
    base = config["DATAGEN_SYNTHETIC_BASE_URI"]
    prefix = config.get("DATAGEN_CLONE_PREFIX") or DEFAULT_CLONE_PREFIX
    return f"{base}/{prefix}"


def get_engorda_env() -> dict[str, str]:
    """Mesmas envs do engorda_tables.py + DATAGEN_CLONE_PREFIX opcional, para
    que a configuração do Data Flow seja idêntica entre os dois jobs."""
    config: dict[str, str] = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Env var(s) obrigatória(s) ausente(s): %s", ", ".join(missing))
        sys.exit(1)
    config["DATAGEN_RAW_PREFIX"] = os.environ.get("DATAGEN_RAW_PREFIX", "").strip("/")
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", "").strip("/")
    config["DATAGEN_CLONE_PREFIX"] = os.environ.get(
        "DATAGEN_CLONE_PREFIX", DEFAULT_CLONE_PREFIX).strip("/")
    return config


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def _aplica_filtros_fonte(df: DataFrame, table: str) -> DataFrame:
    """Cópia de engorda_tables._aplica_filtros_fonte (predicados do produto)."""
    preds = FILTROS_FONTE.get(table)
    if not preds:
        return df
    cols = set(df.columns)
    for col, op, valor in preds:
        if col not in cols:
            logger.warning("_aplica_filtros_fonte: coluna %s ausente em %s; "
                           "predicado (%s %s %r) ignorado", col, table, col, op, valor)
            continue
        if op == "isnull":
            df = df.where(F.col(col).isNull())
        elif op == "ieq":
            df = df.where(F.upper(F.trim(F.col(col))) == F.lit(valor))
        elif op == "==":
            df = df.where(F.col(col) == F.lit(valor))
        elif op == ">":
            df = df.where(F.col(col) > F.lit(valor))
        else:
            raise ValueError(f"operador desconhecido {op!r} para {table}.{col}")
    return df


def _read_source(spark, config, table: str) -> DataFrame:
    """Leitura canônica da fonte, já filtrada pelo produto (como no engorda)."""
    return _aplica_filtros_fonte(read_parquet(spark, raw_path(config, table)), table)


def _fk_list(cfg: dict) -> list[dict]:
    fks = cfg.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = cfg.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def _fk_identidade_degenerada(table: str, fk: dict) -> bool:
    """Cópia de engorda_tables: FK auto-referente identidade (cada coluna
    apontando para si mesma) é lixo de spec — removida na normalização."""
    if fk.get("parent_table") != table:
        return False
    cols = list(fk.get("columns") or [])
    pcols = list(fk.get("parent_columns") or [])
    return bool(cols) and len(cols) == len(pcols) and all(
        c == p for c, p in zip(cols, pcols))


def normalize_specs(specs: dict) -> dict:
    """Versão enxuta de engorda_tables.normalize_specs: strip de schema nas
    chaves e nos parent_table, uppercase, remoção de self-FK identidade."""
    out: dict = {}
    for raw_name, cfg in specs.items():
        name = table_path_name(str(raw_name).strip().upper())
        if name in out:
            raise ValueError(f"Colisão de chave no spec após strip de schema: `{name}`.")
        new_cfg = copy.deepcopy(dict(cfg))
        fks_norm: list[dict] = []
        for fk in _fk_list(new_cfg):
            fk = dict(fk)
            fk["columns"] = [str(c).strip().upper() for c in (fk.get("columns") or [])]
            fk["parent_columns"] = [str(c).strip().upper()
                                    for c in (fk.get("parent_columns") or [])]
            fk["parent_table"] = table_path_name(
                str(fk.get("parent_table", "")).strip().upper())
            if not _fk_identidade_degenerada(name, fk):
                fks_norm.append(fk)
        new_cfg["foreign_keys"] = fks_norm
        new_cfg.pop("fks", None)
        new_cfg["pk_cols"] = [str(c).strip().upper()
                              for c in (new_cfg.get("pk_cols") or [])]
        out[name] = new_cfg
    return out


def load_specs(spark: SparkSession, specs_uri: str) -> dict:
    """Cópia de engorda_tables.load_specs (specs.json único via wholeTextFiles)."""
    records = spark.sparkContext.wholeTextFiles(specs_uri).collect()
    if len(records) != 1:
        raise ValueError(
            f"Esperado exatamente um specs.json em `{specs_uri}`, achei {len(records)}.")
    parsed = json.loads(records[0][1])
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"specs.json em `{specs_uri}` precisa ser objeto não-vazio.")
    return normalize_specs(parsed)


def _not_null_cols(cfg: dict) -> set[str]:
    raw = cfg.get("not_null_cols") or []
    return {str(c).strip().upper() for c in raw if isinstance(c, str)}


def _is_numeric_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType,
                           T.FloatType, T.DoubleType, T.DecimalType))


def _read_pk_max(spark, path: str, pk_col: str):
    """max(pk_col) do Parquet COMPLETO (footer-fast com aggregatePushdown) —
    cópia de engorda_tables._read_pk_max. Sem filtros de produto de propósito:
    a PK nova precisa ficar acima de TODAS as linhas de produção."""
    row = read_parquet(spark, path).agg(F.max(F.col(pk_col))).first()
    return row[0] if row is not None else None


def _pk_capacity_of(dt: T.DataType) -> Optional[int]:
    """Maior inteiro que o tipo físico da PK comporta (None = desconhecido)."""
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


def _with_contiguous_row_id(df: DataFrame, id_col: str) -> DataFrame:
    """Cópia de engorda_tables._with_contiguous_row_id: id contíguo 0..N-1 sem
    Window.orderBy global (single-task sort). row_number só DENTRO de cada
    partição + prefix-sum dos tamanhos no driver."""
    part_col, prow_col, off_col, mid_col = (
        f"__{id_col}_part", f"__{id_col}_prow", f"__{id_col}_poff", f"__{id_col}_mid")
    colisao = [c for c in (part_col, prow_col, off_col, mid_col) if c in df.columns]
    if colisao:
        raise ValueError(f"colisão de coluna temporária: {colisao}")

    df = (df.withColumn(mid_col, F.monotonically_increasing_id())
            .withColumn(part_col, F.spark_partition_id()))
    w_part = Window.partitionBy(part_col).orderBy(F.col(mid_col))
    df = df.withColumn(prow_col, F.row_number().over(w_part))

    sizes = df.groupBy(part_col).agg(F.count(F.lit(1)).cast("long").alias("__sz"))
    spark = df.sparkSession
    ordered = sorted(((r[part_col], r["__sz"]) for r in sizes.collect()),
                     key=lambda p: p[0])
    running = 0
    offsets: List[Tuple[int, int]] = []
    for pid, size in ordered:
        offsets.append((pid, running))
        running += size
    schema = T.StructType([T.StructField(part_col, T.IntegerType(), False),
                           T.StructField(off_col, T.LongType(), False)])
    off_df = spark.createDataFrame(offsets, schema=schema)
    df = df.join(F.broadcast(off_df), on=part_col, how="left")
    df = df.withColumn(id_col, (F.col(off_col) + F.col(prow_col) - F.lit(1)).cast("long"))
    return df.drop(mid_col, part_col, prow_col, off_col)


def _toposort_break_cycles(deps: Mapping[str, set]) -> List[str]:
    """Cópia (enxuta) de engorda_tables._toposort_break_cycles: pais antes de
    filhos; ciclo é quebrado deterministicamente com warning."""
    remaining = set(deps)
    done: set = set()
    order: List[str] = []
    warned = False
    while remaining:
        ready = sorted(n for n in remaining if deps[n] <= done)
        if not ready:
            if not warned:
                pend = {t: sorted(deps[t] - done) for t in sorted(remaining)}
                logger.warning("Ciclo de FK entre tabelas clonáveis; quebrando "
                               "deterministicamente. Pendências: %s", pend)
                warned = True
            ready = [min(remaining, key=lambda n: (len(deps[n] - done), n))]
        for name in ready:
            order.append(name)
            done.add(name)
            remaining.discard(name)
    return order


def _null_efetivo_pred(df: DataFrame, col: str):
    """NULL efetivo: NULL, ou '' em coluna string (o Oracle grava '' como NULL)."""
    pred = F.col(col).isNull()
    if isinstance(df.schema[col].dataType, T.StringType):
        pred = pred | (F.trim(F.col(col)) == F.lit(""))
    return pred


# ---------------------------------------------------------------------------
# Regras de engorda (datas) — ver bloco de constantes ENGORDA_COL_* no topo.
# ---------------------------------------------------------------------------
def _normalize_engorda_ts(value: Optional[datetime]) -> datetime:
    """Timestamp único do run. Microssegundos zerados: o Oracle guarda DATE com
    precisão de segundo e um resíduo de microssegundo só criaria diferença
    entre o Parquet e o que a carga grava."""
    if value is None:
        return datetime.now().replace(microsecond=0)
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    raise TypeError("engorda_ts deve ser datetime ou None.")


def _tipo_data_engordavel(dt: T.DataType) -> bool:
    """Só data/hora/string recebem literal de data. Um DAT_* numérico (schema
    inesperado) seria NULADO pelo cast — melhor pular com aviso."""
    return isinstance(dt, (T.DateType, T.TimestampType, T.StringType))


def _timestamp_literal_for_type(value: datetime, dt: T.DataType):
    """Literal de timestamp respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(F.lit(value).cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
    return F.lit(value).cast(dt)


def _date_literal_for_type(value: date, dt: T.DataType):
    """Literal de data SEM hora respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(F.lit(value).cast("date"), "yyyy-MM-dd")
    if isinstance(dt, T.TimestampType):
        # Coluna física é timestamp: grava a data à meia-noite (sem hora útil).
        return F.lit(value).cast("date").cast(dt)
    return F.lit(value).cast(dt)


def _date_expression_for_type(expr, dt: T.DataType):
    """Expressão de data SEM hora respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(expr.cast("date"), "yyyy-MM-dd")
    if isinstance(dt, T.TimestampType):
        return expr.cast("date").cast(dt)
    return expr.cast(dt)


def aplica_regras_engorda(df: DataFrame, tabela: str, *, engorda_ts: datetime,
                          prazo_vencimento_dias: Optional[int] = None,
                          ) -> Tuple[DataFrame, List[str]]:
    """Aplica as regras de DATA do engorda aos clones de UMA tabela.

    Devolve (df, colunas efetivamente reescritas). Tolerante por construção: a
    regra de uma coluna ausente no schema é no-op — as tabelas do fecho têm
    subconjuntos bem diferentes dessas colunas.

    O prazo de DAT_VENCIMENTO é calculado ANTES de DAT_EMISSAO ser
    sobrescrita; senão o "prazo original da linha clonada" viraria
    (vencimento_original - data_do_run), que é outro número.
    """
    engorda_dt = engorda_ts.date()
    tipos = {f.name: f.dataType for f in df.schema.fields}
    aplicadas: List[str] = []

    def _tipo_ok(col: str) -> bool:
        if _tipo_data_engordavel(tipos[col]):
            return True
        logger.warning("%s.%s: tipo %s não é data/hora/string; regra de engorda "
                       "IGNORADA (a coluna mantém o valor clonado).",
                       tabela, col, tipos[col].simpleString())
        return False

    tem_venc = ENGORDA_COL_DAT_VENCIMENTO in tipos and _tipo_ok(ENGORDA_COL_DAT_VENCIMENTO)
    tem_emissao = ENGORDA_COL_DAT_EMISSAO in tipos and _tipo_ok(ENGORDA_COL_DAT_EMISSAO)

    # 1) Prazo de vencimento (dias), ainda com os valores ORIGINAIS na mão.
    if tem_venc:
        if ENGORDA_PRAZO_TMP_COL in df.columns:
            raise ValueError(
                f"{tabela}: colisão de coluna temporária {ENGORDA_PRAZO_TMP_COL}.")
        padrao = F.lit(int(DEFAULT_DT_VENCIMENTO_PRAZO_DIAS)).cast("int")
        prazo_fixo = ENGORDA_PRAZO_DIAS_POR_TABELA.get(tabela, prazo_vencimento_dias)
        if prazo_fixo is not None:
            prazo_expr = F.lit(int(prazo_fixo)).cast("int")
        elif tem_emissao:
            prazo_expr = F.datediff(
                F.to_date(F.col(ENGORDA_COL_DAT_VENCIMENTO)),
                F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)),
            ).cast("int")
        else:
            prazo_expr = padrao
        # Prazo nulo (data ilegível/ausente) ou não-positivo cai no default: um
        # vencimento anterior à emissão quebraria a regra de negócio na NoMe.
        prazo_expr = F.coalesce(prazo_expr, padrao)
        prazo_expr = F.when(
            prazo_expr < F.lit(MIN_DT_VENCIMENTO_PRAZO_DIAS), padrao
        ).otherwise(prazo_expr)
        df = df.withColumn(ENGORDA_PRAZO_TMP_COL, prazo_expr)

    # 2) Colunas de timestamp: TODAS com o mesmo instante do run.
    for col in ENGORDA_COLS_TIMESTAMP:
        if col not in tipos or not _tipo_ok(col):
            continue
        df = df.withColumn(col, _timestamp_literal_for_type(engorda_ts, tipos[col]))
        aplicadas.append(col)

    # 3) DAT_EMISSAO = data do run, sem hora.
    if tem_emissao:
        df = df.withColumn(
            ENGORDA_COL_DAT_EMISSAO,
            _date_literal_for_type(engorda_dt, tipos[ENGORDA_COL_DAT_EMISSAO]))
        aplicadas.append(ENGORDA_COL_DAT_EMISSAO)

    # 4) DAT_VENCIMENTO = data do run + prazo, sem hora.
    if tem_venc:
        venc_expr = F.expr(
            f"date_add(DATE '{engorda_dt.isoformat()}', "
            f"CAST({ENGORDA_PRAZO_TMP_COL} AS INT))")
        df = df.withColumn(
            ENGORDA_COL_DAT_VENCIMENTO,
            _date_expression_for_type(venc_expr, tipos[ENGORDA_COL_DAT_VENCIMENTO]),
        ).drop(ENGORDA_PRAZO_TMP_COL)
        aplicadas.append(ENGORDA_COL_DAT_VENCIMENTO)

    return df, aplicadas


# ---------------------------------------------------------------------------
# Plano de clonagem: classificação de tabelas/PKs/FKs a partir do spec + dos
# schemas Parquet. Nenhuma decisão implícita: o que não tem regra ABORTA.
# ---------------------------------------------------------------------------
@dataclass
class FkRemap:
    """FK desta tabela para a PK de um pai clonado (grupo de constraint do
    spec, colunas alinhadas por posição com a PK do pai)."""
    columns: Tuple[str, ...]
    parent_table: str
    parent_columns: Tuple[str, ...]
    principal: bool  # colunas com MESMO NOME da PK do pai -> vínculo principal


@dataclass
class PlanoTabela:
    name: str
    pk_cols: Tuple[str, ...]
    fks_remap: List[FkRemap] = field(default_factory=list)
    pk_regra: str = ""              # OFFSET_PROPRIO | VIA_PAI
    pk_start: Optional[int] = None  # início da PK nova (só OFFSET_PROPRIO)
    pk_passo: int = 1               # folga ENTRE PKs novas consecutivas


def _fks_para_pais_clonados(spec: dict, tabela: str,
                            clonaveis: Set[str]) -> List[FkRemap]:
    out: List[FkRemap] = []
    for fk in _fk_list(spec[tabela]):
        pai = fk.get("parent_table")
        if pai not in clonaveis:
            continue  # pai static/fora: FK mantém valor original (política padrão)
        pk_pai = tuple(spec[pai].get("pk_cols") or [])
        cols = tuple(fk.get("columns") or [])
        pcols = tuple(fk.get("parent_columns") or [])
        if not pk_pai or pcols != pk_pai or len(cols) != len(pcols):
            logger.warning(
                "FK %s.%s -> %s.%s não aponta para a PK do pai; sem remap "
                "definido — colunas mantêm o valor original.",
                tabela, list(cols), pai, list(pcols))
            continue
        principal = (cols == pcols) and (pai != tabela)
        out.append(FkRemap(columns=cols, parent_table=pai,
                           parent_columns=pcols, principal=principal))
    return out


def monta_plano(spark, config, spec: dict, estaticas_extra: Set[str],
                pk_floor: int, pk_band: int, offset_num_if: Optional[int],
                n_clones_estimado: int,
                pk_passo: int = 1) -> Dict[str, PlanoTabela]:
    """Classifica cada tabela clonável e define a regra de PK. Aborta (com
    lista completa) se alguma tabela ficar sem regra — nada de chute.

    Duas folgas independentes na PK nova (ambas só valem para OFFSET_PROPRIO;
    VIA_PAI apenas segue o mapeamento do pai):
      pk_band  -> distância entre o max REAL da tabela e a primeira PK nova;
      pk_passo -> distância entre duas PKs novas consecutivas (default 1,
                  contíguo). Serve para deixar buracos reserváveis entre os
                  registros clonados.
    """
    if pk_passo < 1:
        raise ValueError("pk_passo deve ser >= 1.")
    estaticas = {t for t, cfg in spec.items() if cfg.get("static")} | estaticas_extra
    clonaveis = {t for t in spec if t not in estaticas}
    if TABELA_RAIZ not in clonaveis:
        raise ValueError(f"{TABELA_RAIZ} precisa ser clonável (não-static) no spec.")

    planos: Dict[str, PlanoTabela] = {}
    problemas: List[str] = []

    for t in sorted(clonaveis):
        pk = tuple(spec[t].get("pk_cols") or [])
        if not pk:
            problemas.append(f"{t}: sem pk_cols no spec")
            continue
        fks = _fks_para_pais_clonados(spec, t, clonaveis)
        plano = PlanoTabela(name=t, pk_cols=pk, fks_remap=fks)

        # Regra da PK: componente coberto por FK de pai clonado -> segue o pai;
        # senão, surrogate única/numérica fora de FK -> offset próprio.
        cols_fk_qualquer = {c for fk in _fk_list(spec[t])
                            for c in (fk.get("columns") or [])}
        cols_fk_remap = {c for fk in fks for c in fk.columns}
        if any(c in cols_fk_remap for c in pk):
            plano.pk_regra = "VIA_PAI"
        elif len(pk) == 1 and pk[0] not in cols_fk_qualquer:
            try:
                dt = read_parquet(spark, raw_path(config, t)).schema[pk[0]].dataType
            except Exception as exc:
                problemas.append(f"{t}: não li o schema Parquet ({exc})")
                continue
            if not _is_numeric_type(dt):
                problemas.append(
                    f"{t}: PK {pk[0]} não numérica ({dt.simpleString()}) e sem "
                    "FK de pai clonado — sem regra de remap")
                continue
            plano.pk_regra = "OFFSET_PROPRIO"
        else:
            problemas.append(
                f"{t}: PK {list(pk)} sem componente de FK de pai clonado e não "
                "elegível a offset (composta e/ou participa de FK para pai não "
                "clonado) — sem regra de remap")
            continue

        # Vínculo principal para o pertencimento: exigido de toda tabela
        # clonável exceto a raiz.
        if t != TABELA_RAIZ and not any(fk.principal for fk in fks):
            problemas.append(
                f"{t}: nenhuma FK de VÍNCULO PRINCIPAL (colunas com mesmo nome "
                "da PK de um pai clonado) — não sei ligar as linhas ao "
                "instrumento. Marque static (--tratar-como-static) ou corrija o spec.")
            continue
        planos[t] = plano

    if problemas:
        raise ValueError(
            "Tabela(s) clonável(is) sem regra segura — resolva antes de rodar "
            "(--tratar-como-static as exclui da clonagem):\n  - "
            + "\n  - ".join(problemas))

    # Início da PK nova para as tabelas OFFSET_PROPRIO (max real do Parquet
    # COMPLETO + band, com clamp de capacidade — padrão compute_pk_maxes).
    for t, plano in sorted(planos.items()):
        if plano.pk_regra != "OFFSET_PROPRIO":
            continue
        plano.pk_passo = pk_passo
        pk_col = plano.pk_cols[0]
        raw_max = _read_pk_max(spark, raw_path(config, t), pk_col)
        if raw_max is None:
            raise ValueError(f"{t}: não li max({pk_col}) do Parquet completo.")
        true_max = int(raw_max)
        if t == TABELA_RAIZ and offset_num_if is not None:
            if offset_num_if <= true_max:
                raise ValueError(
                    f"--offset-num-if {offset_num_if} <= max real de "
                    f"{COL_NUM_IF} ({true_max}); colidiria com produção.")
            # Início EXPLÍCITO é inclusivo: o primeiro NUM_IF novo É o valor
            # informado (semântica documentada no --help).
            plano.pk_start = offset_num_if
        else:
            # Primeira PK nova = max real + band + 1.
            plano.pk_start = max(true_max + pk_band, pk_floor) + 1
        dt = read_parquet(spark, raw_path(config, t)).schema[pk_col].dataType
        cap = _pk_capacity_of(dt)
        # O passo multiplica o alcance: a última PK nova é
        # pk_start + (n_clones - 1) * pk_passo.
        alcance = n_clones_estimado * pk_passo
        if cap is not None and plano.pk_start + alcance > cap:
            logger.warning(
                "%s: início %d + ~%d clone(s) × passo %d pode estourar o "
                "domínio da PK (cap %d). Reduza o lote/K/passo ou trate a "
                "tabela como static.",
                t, plano.pk_start, n_clones_estimado, pk_passo, cap)
        logger.info("Plano %s: PK %s OFFSET_PROPRIO a partir de %d "
                    "(max real %d, band %d, passo %d)", t, pk_col,
                    plano.pk_start, true_max, pk_band, pk_passo)
    for t, plano in sorted(planos.items()):
        if plano.pk_regra == "VIA_PAI":
            logger.info("Plano %s: PK %s VIA_PAI (segue o mapeamento do pai)",
                        t, list(plano.pk_cols))
        principais = [f"{list(fk.columns)}->{fk.parent_table}"
                      for fk in plano.fks_remap if fk.principal]
        laterais = [f"{list(fk.columns)}->{fk.parent_table}"
                    for fk in plano.fks_remap if not fk.principal]
        logger.info("Plano %s: vínculo principal %s | laterais (remap-se-no-lote) %s",
                    t, principais or "-", laterais or "-")
    return planos


def ordem_topologica(planos: Dict[str, PlanoTabela]) -> List[str]:
    deps = {t: {fk.parent_table for fk in p.fks_remap
                if fk.parent_table != t and fk.parent_table in planos}
            for t, p in planos.items()}
    return _toposort_break_cycles(deps)


# ---------------------------------------------------------------------------
# Seleção do lote de instrumentos.
# ---------------------------------------------------------------------------
def _dominio_num_if_produto(spark, config) -> DataFrame:
    """Domínio de NUM_IF elegível à amostragem/validação do lote — reprodução
    FIEL da query de validação do produto (notebook), que é a FONTE DA VERDADE
    do que é o CDB simplificado. Query de referência (manter em sincronia):

        SELECT DISTINCT ife.NUM_IF
        FROM INSTRUMENTO_FINANCEIRO ife
        INNER JOIN TITULO tit          ON ife.NUM_IF = tit.NUM_IF
        INNER JOIN CREDITO cre         ON ife.NUM_IF = cre.NUM_IF
        INNER JOIN CONDICAO_IF cif     ON ife.NUM_IF = cif.NUM_IF
        INNER JOIN EVENTO eve          ON ife.NUM_IF = eve.NUM_IF
        LEFT JOIN RESGATE res          ON cif.NUM_CONDICAO_IF = res.NUM_CONDICAO_IF
        LEFT JOIN JUROS_FLUTUANTE jfl  ON cif.NUM_CONDICAO_IF = jfl.NUM_CONDICAO_IF
        WHERE ife.NUM_TIPO_IF = 49
          AND ife.DAT_EXCLUSAO IS NULL
          AND (res.COD_COND_RESGATE = 'SEM TABELA' OR res.COD_COND_RESGATE IS NULL)
          AND tit.COD_TIPO_ESCALONAMENTO IS NULL
          AND cif.DAT_EXCLUSAO IS NULL
          AND eve.DAT_EXCLUSAO IS NULL

    Implementação: cadeia de LEFT SEMI joins sobre o raw — resultado IDÊNTICO
    ao da query. Cada predicado do WHERE toca um único alias, então "existe
    combinação de linhas satisfazendo tudo" fatoriza em um EXISTS independente
    por tabela; o semi join É esse EXISTS, e dispensa o DISTINCT sobre o
    fan-out multiplicativo tit×cre×cif×eve×res do join literal.

    Notas de fidelidade (decisões conscientes, não simplificações):
      * RESGATE é condição POR CONDICAO_IF: left join + (cod = 'SEM TABELA'
        OR cod IS NULL). Condição sem resgate passa (NULL do left join);
        condição com >= 1 resgate de código 'SEM TABELA' ou NULL passa.
        Comparação EXATA e SEM filtro de DAT_EXCLUSAO, como na query — de
        propósito DIFERENTE de FILTROS_FONTE["RESGATE"] (ieq + DAT_EXCLUSAO
        nulo), que decide quais linhas de RESGATE entram no CLONE, não quem
        pertence ao domínio de amostragem.
      * JUROS_FLUTUANTE: LEFT JOIN sem predicado no WHERE não restringe o
        conjunto DISTINCT de NUM_IF — omitido. Se a query ganhar predicado
        de jfl, replique aqui.
      * Coluna/tabela ausente no raw ABORTA (erro do Spark): o domínio do
        produto não pode ser aproximado em silêncio.
    """
    # ife: NUM_TIPO_IF = 49 AND DAT_EXCLUSAO IS NULL
    dom = (read_parquet(spark, raw_path(config, TABELA_RAIZ))
           .where((F.col(FILTRO_TIPO_IF_COLUMN) == F.lit(FILTRO_TIPO_IF_VALUE))
                  & F.col("DAT_EXCLUSAO").isNull())
           .select(COL_NUM_IF))

    # INNER JOIN TITULO ... AND tit.COD_TIPO_ESCALONAMENTO IS NULL
    tit = (read_parquet(spark, raw_path(config, "TITULO"))
           .where(F.col("COD_TIPO_ESCALONAMENTO").isNull())
           .select(COL_NUM_IF))
    dom = dom.join(tit, on=COL_NUM_IF, how="left_semi")

    # INNER JOIN CREDITO (sem predicado adicional no WHERE)
    cre = read_parquet(spark, raw_path(config, "CREDITO")).select(COL_NUM_IF)
    dom = dom.join(cre, on=COL_NUM_IF, how="left_semi")

    # INNER JOIN EVENTO ... AND eve.DAT_EXCLUSAO IS NULL
    eve = (read_parquet(spark, raw_path(config, "EVENTO"))
           .where(F.col("DAT_EXCLUSAO").isNull())
           .select(COL_NUM_IF))
    dom = dom.join(eve, on=COL_NUM_IF, how="left_semi")

    # INNER JOIN CONDICAO_IF (cif.DAT_EXCLUSAO IS NULL)
    # LEFT JOIN RESGATE ON cif.NUM_CONDICAO_IF = res.NUM_CONDICAO_IF
    # WHERE (res.COD_COND_RESGATE = 'SEM TABELA' OR res.COD_COND_RESGATE IS NULL)
    cif = (read_parquet(spark, raw_path(config, "CONDICAO_IF"))
           .where(F.col("DAT_EXCLUSAO").isNull())
           .select(COL_NUM_IF, "NUM_CONDICAO_IF"))
    res = (read_parquet(spark, raw_path(config, "RESGATE"))
           .select("NUM_CONDICAO_IF", "COD_COND_RESGATE"))
    cif_ok = (cif.join(res, on="NUM_CONDICAO_IF", how="left")
              .where((F.col("COD_COND_RESGATE") == F.lit("SEM TABELA"))
                     | F.col("COD_COND_RESGATE").isNull())
              .select(COL_NUM_IF))
    dom = dom.join(cif_ok, on=COL_NUM_IF, how="left_semi")

    # SELECT DISTINCT ife.NUM_IF — os semi joins preservam as linhas de ife
    # (PK NUM_IF já única); o dropDuplicates é cinto e suspensório.
    return dom.dropDuplicates([COL_NUM_IF])


def seleciona_instrumentos(spark, config, num_ifs: Optional[List[int]],
                           n_instrumentos: Optional[int], seed: int) -> List:
    """Devolve a lista de valores de NUM_IF do lote (coletada no driver — o
    lote é pequeno por definição). Tanto o SORTEIO quanto a VALIDAÇÃO de
    lista explícita rodam contra o domínio do produto — a query de validação
    do notebook (ver _dominio_num_if_produto), não só a raiz filtrada.
    Aborta se algum NUM_IF pedido estiver fora desse domínio."""
    if num_ifs is not None and not num_ifs:
        # Defensivo (o argparse já barra): lista vazia NÃO pode virar sorteio.
        raise ValueError("Lista de NUM_IF vazia; informe valores ou use "
                         "--n-instrumentos.")
    fonte = _dominio_num_if_produto(spark, config)
    logger.info("Domínio de amostragem/validação de NUM_IF: query do produto "
                "(fecho TITULO+CREDITO+CONDICAO_IF+EVENTO + regra de RESGATE).")
    if num_ifs:
        pedidos = spark.createDataFrame([(v,) for v in num_ifs], [COL_NUM_IF])
        achados = {r[0] for r in fonte.join(
            F.broadcast(pedidos.select(F.col(COL_NUM_IF).cast(
                fonte.schema[COL_NUM_IF].dataType))),
            on=COL_NUM_IF, how="left_semi").collect()}
        faltando = [v for v in num_ifs if v not in {int(a) for a in achados}]
        if faltando:
            raise ValueError(
                "NUM_IF(s) fora do domínio do produto (query de validação — "
                f"ver _dominio_num_if_produto): {faltando}")
        valores = sorted(achados)
    else:
        n = int(n_instrumentos or 1)
        valores = [r[0] for r in
                   fonte.orderBy(F.rand(seed)).limit(n).collect()]
        if len(valores) < n:
            raise ValueError(
                f"Domínio tem só {len(valores)} instrumento(s); pedi {n}.")
        valores = sorted(valores)
    logger.info("Lote: %d instrumento(s) NUM_IF=%s", len(valores),
                valores if len(valores) <= 20 else f"{valores[:20]}... (+{len(valores)-20})")
    return valores


# ---------------------------------------------------------------------------
# Pertencimento (membership): que linhas de cada tabela pertencem ao lote.
# ---------------------------------------------------------------------------
def calcula_lotes(spark, config, spec: dict, planos: Dict[str, PlanoTabela],
                  ordem: List[str], num_if_valores: List,
                  max_passadas: int) -> Dict[str, DataFrame]:
    """Desce a árvore a partir da raiz pelas FKs de vínculo principal,
    pais-antes-de-filhos; repete a passada até estabilizar (ciclos), até
    max_passadas. Cada lote é pequeno (linhas de N instrumentos) -> persist +
    localCheckpoint para cortar a linhagem entre passadas."""
    fontes: Dict[str, DataFrame] = {}
    lotes: Dict[str, DataFrame] = {}
    contagens: Dict[str, int] = {}

    raiz_src = _read_source(spark, config, TABELA_RAIZ)
    sel = spark.createDataFrame([(v,) for v in num_if_valores], [COL_NUM_IF])
    sel = sel.select(F.col(COL_NUM_IF).cast(raiz_src.schema[COL_NUM_IF].dataType))
    lote_raiz = raiz_src.join(F.broadcast(sel), on=COL_NUM_IF, how="left_semi")
    lotes[TABELA_RAIZ] = lote_raiz.localCheckpoint(eager=True)
    contagens[TABELA_RAIZ] = lotes[TABELA_RAIZ].count()
    if contagens[TABELA_RAIZ] != len(num_if_valores):
        raise ValueError(
            f"{TABELA_RAIZ}: lote com {contagens[TABELA_RAIZ]} linha(s) para "
            f"{len(num_if_valores)} NUM_IF — PK duplicada ou seleção inconsistente.")

    for passada in range(1, max_passadas + 1):
        cresceu = False
        for t in ordem:
            if t == TABELA_RAIZ:
                continue
            plano = planos[t]
            fks_uteis = [fk for fk in plano.fks_remap
                         if fk.principal and fk.parent_table in lotes]
            if not fks_uteis:
                continue  # pai ainda sem lote nesta passada (ciclo); tenta na próxima
            if t not in fontes:
                fontes[t] = _read_source(spark, config, t)
            src = fontes[t]
            partes: List[DataFrame] = []
            for fk in fks_uteis:
                chaves_pai = (lotes[fk.parent_table]
                              .select(*[F.col(pc).alias(cc) for cc, pc
                                        in zip(fk.columns, fk.parent_columns)])
                              .dropDuplicates())
                partes.append(src.join(F.broadcast(chaves_pai),
                                       on=list(fk.columns), how="left_semi"))
            lote_t = partes[0]
            for extra in partes[1:]:
                lote_t = lote_t.unionByName(extra)
            lote_t = lote_t.dropDuplicates(list(plano.pk_cols))
            lote_t = lote_t.localCheckpoint(eager=True)
            n = lote_t.count()
            if contagens.get(t) != n:
                # PRIMEIRA atribuição também conta como mudança: num ciclo de
                # FKs principais o lote da passada 1 pode estar incompleto
                # (pai que vem depois na ordem ainda sem lote) — só a passada
                # de confirmação sem NENHUMA mudança prova o ponto fixo.
                cresceu = True
                lotes[t] = lote_t
                contagens[t] = n
        faltando = [t for t in ordem if t not in lotes]
        if not cresceu and not faltando:
            logger.info("Pertencimento estabilizou na passada %d.", passada)
            break
        if passada == max_passadas and (cresceu or faltando):
            # Gravar um fecho INCOMPLETO em silêncio seria pior que falhar:
            # clones sem parte das linhas mudam o comportamento da NoMe.
            raise ValueError(
                f"Pertencimento não estabilizou em {max_passadas} passada(s)"
                + (f"; tabela(s) sem lote: {faltando}" if faltando else "")
                + ". Aumente --max-passadas (ciclos de FK profundos no fecho).")
    for t in ordem:
        if t not in lotes:
            # Sem caminho principal até a raiz nesta execução: nada a clonar.
            lotes[t] = fontes.get(t, _read_source(spark, config, t)).limit(0)
            contagens[t] = 0
        logger.info("Lote %s: %d linha(s).", t, contagens[t])
    return lotes


# ---------------------------------------------------------------------------
# Clonagem: lote × K, mapeamento de PK e reescrita de FKs.
# ---------------------------------------------------------------------------
def _k_df(spark, fator_k: int) -> DataFrame:
    return (spark.range(1, fator_k + 1)
            .select(F.col("id").cast("int").alias(K_COL)))


def _copia_independente(df: DataFrame) -> DataFrame:
    """Projeção com alias coluna a coluna: regenera os exprIds do plano lógico,
    de modo que cada join use uma cópia INDEPENDENTE do mapeamento. Sem isso,
    juntar o mesmo mapa mais de uma vez na mesma linhagem (passo 1 da PK +
    self-FK no passo 2) dispara DetectAmbiguousSelfJoin -> AnalysisException
    ("Column ... are ambiguous") — o localCheckpoint preserva os exprIds de
    saída, então ele sozinho NÃO quebra essa identidade."""
    return df.select(*[F.col(c).alias(c) for c in df.columns])


def _monta_mapeamento_pk(clones: DataFrame, plano: PlanoTabela,
                         mapeamentos: Dict[str, DataFrame]) -> DataFrame:
    """DataFrame de mapeamento da PK desta tabela: colunas old_<pk_i>, K_COL,
    new_<pk_i>. OFFSET_PROPRIO gera valores acima do max real, espaçados de
    plano.pk_passo (1 = contíguo); VIA_PAI deriva do mapeamento do(s) pai(s) —
    componentes não cobertos por FK remapeada ficam com o valor original
    (new == old).

    A saída passa por _copia_independente: os exprIds não podem coincidir com
    os de `clones`, senão os joins seguintes viram self-join ambíguo."""
    pk = list(plano.pk_cols)

    if plano.pk_regra == "OFFSET_PROPRIO":
        base = clones.select(*pk, K_COL).dropDuplicates(pk + [K_COL])
        pk_col = pk[0]
        dt = clones.schema[pk_col].dataType
        com_id = _with_contiguous_row_id(base, "__pk_rid")
        passo = max(1, int(plano.pk_passo))
        mapa = (com_id
                .withColumn(f"new_{pk_col}",
                            (F.lit(int(plano.pk_start))
                             + F.col("__pk_rid") * F.lit(passo))
                            .cast(dt))
                .drop("__pk_rid")
                .select(*[F.col(c).alias(f"old_{c}") for c in pk],
                        F.col(K_COL).alias(K_COL),
                        F.col(f"new_{pk_col}").alias(f"new_{pk_col}")))
        return _copia_independente(mapa)

    # VIA_PAI: aplica cada FK remapeável que cobre componentes da PK. A base
    # carrega TAMBÉM as colunas dessas FKs que ficam FORA da PK: uma FK
    # composta (A, B) -> PK do pai onde a PK local é só (A) precisa de old_B
    # para o join com o mapeamento do pai. Como a PK é única por linha,
    # dropDuplicates por (pk, K) mantém uma linha por chave.
    fks_cobrem_pk = [fk for fk in plano.fks_remap
                     if any(c in plano.pk_cols for c in fk.columns)]
    cols_base = list(dict.fromkeys(
        pk + [c for fk in fks_cobrem_pk for c in fk.columns]))
    base = clones.select(*cols_base, K_COL).dropDuplicates(pk + [K_COL])
    out = base.select(*[F.col(c).alias(f"old_{c}") for c in cols_base], K_COL,
                      *[F.col(c).alias(f"new_{c}") for c in pk])
    for fk in fks_cobrem_pk:
        mapa_pai = mapeamentos.get(fk.parent_table)
        if mapa_pai is None:
            continue  # ciclo sem raiz — já teria abortado no plano
        mapa_pai = _copia_independente(mapa_pai)
        cond = [out[f"old_{cc}"] == mapa_pai[f"old_{pc}"]
                for cc, pc in zip(fk.columns, fk.parent_columns)]
        cond.append(out[K_COL] == mapa_pai[K_COL])
        joined = out.join(F.broadcast(mapa_pai), on=cond, how="left")
        proj = []
        for c in pk:
            if c in fk.columns:
                pc = fk.parent_columns[list(fk.columns).index(c)]
                proj.append(F.coalesce(mapa_pai[f"new_{pc}"],
                                       out[f"new_{c}"]).alias(f"new_{c}"))
            else:
                proj.append(out[f"new_{c}"].alias(f"new_{c}"))
        out = joined.select(
            *[out[f"old_{cb}"].alias(f"old_{cb}") for cb in cols_base],
            out[K_COL].alias(K_COL), *proj)
    mapa = out.select(*[F.col(f"old_{c}").alias(f"old_{c}") for c in pk],
                      F.col(K_COL).alias(K_COL),
                      *[F.col(f"new_{c}").alias(f"new_{c}") for c in pk])
    return _copia_independente(mapa)


def _aplica_remap_fk(clones: DataFrame, fk: FkRemap, mapa_pai: DataFrame,
                     orig: Dict[str, str]) -> DataFrame:
    """Reescreve as colunas do grupo de FK via mapeamento do pai, juntando
    pelos valores ORIGINAIS congelados (__orig_*). LEFT join + coalesce(new,
    atual): alvo fora do lote (ou FK NULL) mantém o valor — a política padrão
    é remapeia-se-no-lote."""
    # Cópia independente: numa self-FK o mapa já está na linhagem de `clones`
    # (passo 1); reutilizar as MESMAS referências seria self-join ambíguo.
    mapa_pai = _copia_independente(mapa_pai)
    cond = [clones[orig[cc]] == mapa_pai[f"old_{pc}"]
            for cc, pc in zip(fk.columns, fk.parent_columns)]
    cond.append(clones[K_COL] == mapa_pai[K_COL])
    joined = clones.join(F.broadcast(mapa_pai), on=cond, how="left")
    proj = []
    for c in clones.columns:
        if c in fk.columns:
            pc = fk.parent_columns[list(fk.columns).index(c)]
            proj.append(F.coalesce(mapa_pai[f"new_{pc}"], clones[c])
                        .cast(clones.schema[c].dataType).alias(c))
        else:
            proj.append(clones[c].alias(c))
    return joined.select(*proj)


def clona_tabela(spark, plano: PlanoTabela, lote: DataFrame, fator_k: int,
                 mapeamentos: Dict[str, DataFrame]) -> Tuple[DataFrame, DataFrame]:
    """Clona uma tabela: lote × K, mapeia a própria PK e reescreve as FKs.
    Devolve (clones prontos SEM colunas temporárias, mapeamento da PK).

    Todos os joins de remap comparam contra CÓPIAS CONGELADAS dos valores
    originais (__orig_*): uma coluna pode participar da PK e de uma FK ao
    mesmo tempo (shared-key/composta) e, depois de reescrita pelo primeiro
    passo, seu valor novo não casaria mais com o lado old_ do mapeamento —
    o que perderia silenciosamente o remap das demais colunas do grupo."""
    if K_COL in lote.columns:
        raise ValueError(f"{plano.name}: coluna {K_COL} já existe na fonte.")
    clones = lote.crossJoin(F.broadcast(_k_df(spark, fator_k)))

    cols_de_join = sorted({*plano.pk_cols,
                           *(c for fk in plano.fks_remap for c in fk.columns)})
    orig = {c: f"__orig_{c}" for c in cols_de_join}
    colisao = [oc for oc in orig.values() if oc in clones.columns]
    if colisao:
        raise ValueError(f"{plano.name}: colisão de coluna temporária {colisao}.")
    for c, oc in orig.items():
        clones = clones.withColumn(oc, F.col(c))

    # Mapeamento da PK é montado ANTES de qualquer reescrita (valores ainda
    # originais) e congelado: os joins seguintes (inclusive self-FK) tratam o
    # mapa como fonte independente, sem linhagem comum com `clones`.
    mapa_pk = _monta_mapeamento_pk(clones, plano, mapeamentos)
    mapa_pk = mapa_pk.localCheckpoint(eager=True)

    # 1) PK própria: inner join pelos valores ORIGINAIS (toda linha mapeia,
    #    porque o mapa foi construído das próprias linhas do lote × K).
    #    Cópia independente do mapa: evita self-join ambíguo (ver helper).
    mapa_p1 = _copia_independente(mapa_pk)
    cond = [clones[orig[c]] == mapa_p1[f"old_{c}"] for c in plano.pk_cols]
    cond.append(clones[K_COL] == mapa_p1[K_COL])
    joined = clones.join(F.broadcast(mapa_p1), on=cond, how="inner")
    proj = []
    for c in clones.columns:
        if c in plano.pk_cols:
            proj.append(F.coalesce(mapa_p1[f"new_{c}"], clones[c])
                        .cast(clones.schema[c].dataType).alias(c))
        else:
            proj.append(clones[c].alias(c))
    clones = joined.select(*proj)

    # 2) FKs para pais clonados (inclui self-FKs e laterais): remap-se-no-lote,
    #    sempre juntando pelos __orig_*. FK totalmente contida na PK já foi
    #    reescrita pelo passo 1 com o MESMO mapeamento — pular evita join à toa.
    for fk in plano.fks_remap:
        if all(c in plano.pk_cols for c in fk.columns):
            continue
        mapa_pai = mapa_pk if fk.parent_table == plano.name \
            else mapeamentos.get(fk.parent_table)
        if mapa_pai is None:
            logger.warning("%s: mapeamento do pai %s indisponível (ciclo); "
                           "FK %s mantém valores originais.",
                           plano.name, fk.parent_table, list(fk.columns))
            continue
        clones = _aplica_remap_fk(clones, fk, mapa_pai, orig)

    return clones.drop(K_COL, *orig.values()), mapa_pk


# ---------------------------------------------------------------------------
# Relatório de conferência no log: chaves original -> nova de 1 instrumento.
# ---------------------------------------------------------------------------
def loga_chaves_amostra(ordem: List[str], planos: Dict[str, PlanoTabela],
                        lotes: Dict[str, DataFrame],
                        mapeamentos: Dict[str, DataFrame],
                        num_if_amostra, fator_k: int,
                        limite_por_tabela: int = 30) -> None:
    """Loga, por tabela, as chaves ORIGINAIS -> NOVAS (todas as cópias k) das
    linhas de UM instrumento do lote, para conferência manual contra o banco
    de origem: use os valores originais no DBeaver
    (SELECT * FROM CETIP.<TABELA> WHERE <PK> IN (...)) e compare com as
    linhas de chave nova após a carga dos clones. Tabela sem coluna NUM_IF
    (ex.: subtipos de CONDICAO_IF) mostra as chaves do lote inteiro, até o
    limite — num lote de 1 instrumento é a mesma coisa."""
    logger.info("=" * 78)
    logger.info("CHAVES DE CONFERÊNCIA — instrumento de amostra NUM_IF=%s × K=%d "
                "(mapa completo em %s)", num_if_amostra, fator_k, MAPA_NUM_IF_TABLE)
    logger.info("=" * 78)
    for t in ordem:
        plano = planos.get(t)
        mapa = mapeamentos.get(t)
        if plano is None or mapa is None:
            continue  # lote vazio: sem clones, nada a conferir
        lote = lotes[t]
        pk = list(plano.pk_cols)
        tem_num_if = COL_NUM_IF in lote.columns
        restrito = (lote.where(F.col(COL_NUM_IF) == F.lit(num_if_amostra))
                    if tem_num_if else lote)
        chaves = (restrito
                  .select(*[F.col(c).alias(f"old_{c}") for c in pk])
                  .dropDuplicates())
        cols_old = [f"old_{c}" for c in pk]
        amostra = (chaves.join(F.broadcast(_copia_independente(mapa)),
                               on=cols_old, how="inner")
                   .orderBy(*cols_old, K_COL)
                   .limit(limite_por_tabela + 1)
                   .collect())
        origem = ("linhas do instrumento de amostra" if tem_num_if
                  else "lote inteiro (tabela sem coluna NUM_IF)")
        logger.info("[%s] PK=(%s) — %s:", t, "+".join(pk), origem)
        for r in amostra[:limite_por_tabela]:
            olds = tuple(r[f"old_{c}"] for c in pk)
            news = tuple(r[f"new_{c}"] for c in pk)
            logger.info("    k=%s  %s -> %s", r[K_COL],
                        olds[0] if len(olds) == 1 else olds,
                        news[0] if len(news) == 1 else news)
        if len(amostra) > limite_por_tabela:
            logger.info("    ... truncado em %d chave(s); mapa completo desta "
                        "tabela sai só no log acima do lote.", limite_por_tabela)
    logger.info("Conferência no banco original: SELECT * FROM <owner>.<TABELA> "
                "WHERE <PK> IN (valores ORIGINAIS acima); após a carga, as "
                "mesmas linhas devem existir com as chaves NOVAS.")
    logger.info("=" * 78)


# ---------------------------------------------------------------------------
# Validações pré-escrita.
# ---------------------------------------------------------------------------
def valida_tabela(spec_cfg: dict, plano: PlanoTabela, clones: DataFrame,
                  n_lote: int, fator_k: int) -> List[str]:
    """Devolve a lista de ERROS da tabela (vazia = ok). Uma única passada de
    agregação por checagem; os clones são pequenos (lote × K)."""
    erros: List[str] = []
    esperado = n_lote * fator_k
    total = clones.count()
    if total != esperado:
        erros.append(f"contagem: {total} clone(s), esperado {esperado} "
                     f"(lote {n_lote} × K {fator_k})")

    if total:
        pk = list(plano.pk_cols)
        distintos = clones.select(*pk).dropDuplicates().count()
        if distintos != total:
            erros.append(f"PK nova duplicada: {total - distintos} colisão(ões) "
                         f"interna(s) em {pk}")
        if plano.pk_regra == "OFFSET_PROPRIO":
            pk_col = pk[0]
            minimo = clones.agg(F.min(F.col(pk_col))).first()[0]
            if minimo is not None and int(minimo) < int(plano.pk_start):
                erros.append(f"PK nova abaixo do início seguro: min({pk_col})="
                             f"{minimo} < {plano.pk_start}")

        nn = sorted(c for c in _not_null_cols(spec_cfg) if c in clones.columns)
        if nn:
            row = clones.agg(*[
                F.count(F.when(_null_efetivo_pred(clones, c), F.lit(1))).alias(c)
                for c in nn]).first()
            for c in nn:
                if int(row[c]) > 0:
                    erros.append(f"NOT NULL violado: {c} com {int(row[c])} "
                                 "nulo(s) efetivo(s) (ORA-01400 na carga)")
    return erros


# ---------------------------------------------------------------------------
# Escrita (mesmo padrão do write_synthetic_table do engorda: apaga SÓ o
# prefixo da própria tabela dentro do prefixo de clones e regrava).
# ---------------------------------------------------------------------------
def _delete_path(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    jsc = spark.sparkContext._jsc
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(jsc.hadoopConfiguration())
    if fs.exists(hpath):
        fs.delete(hpath, True)


def escreve_tabela(spark: SparkSession, df: DataFrame, out_path: str) -> None:
    _delete_path(spark, out_path)
    df.write.mode("append").parquet(out_path)


def _area(base: str, prefix: Optional[str]) -> str:
    """Caminho completo de uma área = base + prefixo (quando houver). O
    ambiente real usa UM bucket só, separado por prefixos (raw em
    onprem-export, sintético/clones em prefixos próprios) — por isso as
    comparações de segurança precisam considerar o PREFIXO, nunca só a base."""
    base = base.rstrip("/")
    prefix = (prefix or "").strip("/")
    return f"{base}/{prefix}" if prefix else base


def _mesmo_ou_ancestral(a: str, b: str) -> bool:
    """True se `a` == `b` ou `a` é ancestral de `b` (comparação por segmento:
    bucket/raw NÃO é ancestral de bucket/raw2)."""
    a, b = a.rstrip("/"), b.rstrip("/")
    return a == b or b.startswith(a + "/")


def _valida_destino(config: dict) -> str:
    """O prefixo de clones é EXCLUSIVO deste job e é apagado POR INTEIRO a
    cada execução (senão tabela com lote 0 num run novo deixaria clones do
    run anterior misturados, e o processo de carga os aplicaria de novo).
    Por isso o destino NÃO pode ser vazio, nem conter/estar contido na área
    raw, nem conter a área de saída do engorda."""
    prefix = (config.get("DATAGEN_CLONE_PREFIX") or "").strip("/")
    if not prefix:
        raise ValueError(
            "DATAGEN_CLONE_PREFIX vazio: o destino seria a RAIZ de "
            "DATAGEN_SYNTHETIC_BASE_URI, que é apagada por inteiro a cada run. "
            f"Defina um prefixo dedicado (default: {DEFAULT_CLONE_PREFIX}).")

    save_base = clone_base_path(config)
    raw_area = _area(config["DATAGEN_RAW_BASE_URI"],
                     config.get("DATAGEN_RAW_PREFIX"))
    engorda_area = _area(config["DATAGEN_SYNTHETIC_BASE_URI"],
                         config.get("DATAGEN_SYNTHETIC_PREFIX"))

    # Área raw: nem apagar por cima (save_base ancestral) nem escrever dentro
    # (misturar clones com snapshots que o engorda lê por nome de tabela).
    if _mesmo_ou_ancestral(save_base, raw_area) or _mesmo_ou_ancestral(raw_area, save_base):
        raise ValueError(
            f"Destino dos clones ({save_base}) sobrepõe a área raw "
            f"({raw_area}). Ajuste DATAGEN_CLONE_PREFIX/DATAGEN_RAW_PREFIX "
            "para áreas disjuntas.")
    # Área do engorda: só é problema se apagar save_base LEVAR JUNTO a área
    # do engorda (igual ou descendente). O contrário — clones DENTRO da base
    # sintética, em prefixo próprio — é o layout esperado.
    if _mesmo_ou_ancestral(save_base, engorda_area):
        raise ValueError(
            f"Destino dos clones ({save_base}) é igual/ancestral da área de "
            f"saída do engorda ({engorda_area}); apagá-lo destruiria a saída "
            "do engorda_tables. Use um prefixo dedicado aos clones.")
    return save_base


# ---------------------------------------------------------------------------
# Orquestração.
# ---------------------------------------------------------------------------
def executa_clonagem(spark, config, spec: dict, *,
                     num_ifs: Optional[List[int]] = None,
                     n_instrumentos: Optional[int] = None,
                     fator_k: int = 1,
                     seed: int = DEFAULT_SEED,
                     pk_offset: int = 0,
                     pk_safety_band: int = 0,
                     pk_passo: int = 1,
                     offset_num_if: Optional[int] = None,
                     tratar_como_static: Optional[Set[str]] = None,
                     max_passadas: int = 6,
                     engorda_ts: Optional[datetime] = None,
                     prazo_vencimento_dias: Optional[int] = None,
                     dry_run: bool = False) -> Dict[str, dict]:
    """Roda a clonagem fim a fim; devolve {tabela: estatísticas} (para uso em
    notebook). Aborta sem gravar NADA se qualquer validação falhar."""
    inicio = time.perf_counter()
    # UM instante para o run inteiro: tabelas diferentes não podem divergir no
    # timestamp só porque foram materializadas em ações Spark diferentes.
    engorda_ts = _normalize_engorda_ts(engorda_ts)
    logger.info("Data de engorda do run: %s (prazo de %s: %s)",
                engorda_ts.isoformat(sep=" "), ENGORDA_COL_DAT_VENCIMENTO,
                f"{prazo_vencimento_dias} dia(s) fixos"
                if prazo_vencimento_dias is not None
                else "preserva o prazo original da linha clonada")
    spec = normalize_specs(spec)
    estaticas_extra = {table_path_name(t.strip().upper())
                       for t in (tratar_como_static or set()) if t.strip()}
    if estaticas_extra:
        logger.info("Tratando como static por parâmetro: %s", sorted(estaticas_extra))

    valores = seleciona_instrumentos(spark, config, num_ifs, n_instrumentos, seed)
    if fator_k < 1:
        raise ValueError("--fator-k deve ser >= 1.")

    # n_clones_estimado: só alimenta o AVISO de capacidade da PK (fan-out por
    # instrumento é desconhecido antes do lote; 1000 linhas/instrumento é um
    # chute conservador — não afeta o cálculo do offset, apenas o warning).
    planos = monta_plano(spark, config, spec, estaticas_extra,
                         pk_offset, pk_safety_band, offset_num_if,
                         n_clones_estimado=len(valores) * fator_k * 1000,
                         pk_passo=pk_passo)
    ordem = ordem_topologica(planos)
    logger.info("Ordem de clonagem (%d tabela(s)): %s", len(ordem), ordem)

    lotes = calcula_lotes(spark, config, spec, planos, ordem, valores, max_passadas)

    mapeamentos: Dict[str, DataFrame] = {}
    resultados: Dict[str, Tuple[DataFrame, int]] = {}
    stats: Dict[str, dict] = {}
    erros_globais: List[str] = []

    for t in ordem:
        plano = planos[t]
        n_lote = lotes[t].count()
        if n_lote == 0:
            logger.info("[%s] lote vazio — nada a clonar.", t)
            stats[t] = {"lote": 0, "clones": 0, "colunas_remapeadas": [],
                        "colunas_data": []}
            continue
        clones, mapa_pk = clona_tabela(spark, plano, lotes[t], fator_k, mapeamentos)
        mapeamentos[t] = mapa_pk
        # Regras de data ANTES do checkpoint/validação: o NOT NULL precisa ser
        # conferido no valor que vai ser gravado, não no valor clonado.
        clones, cols_data = aplica_regras_engorda(
            clones, t, engorda_ts=engorda_ts,
            prazo_vencimento_dias=prazo_vencimento_dias)
        clones = clones.localCheckpoint(eager=True)  # congela p/ validar e gravar

        cols_remap = sorted({*plano.pk_cols,
                             *(c for fk in plano.fks_remap for c in fk.columns)}
                            & set(clones.columns))
        erros = valida_tabela(spec[t], plano, clones, n_lote, fator_k)
        stats[t] = {"lote": n_lote, "clones": n_lote * fator_k,
                    "colunas_remapeadas": cols_remap,
                    "colunas_data": cols_data, "erros": erros}
        logger.info("[%s] lote=%d clones=%d remapeadas=%s datas=%s %s",
                    t, n_lote, n_lote * fator_k, cols_remap, cols_data or "-",
                    "ERROS: " + "; ".join(erros) if erros else "OK")
        if erros:
            erros_globais.extend(f"{t}: {e}" for e in erros)
        resultados[t] = (clones, n_lote)

    if erros_globais:
        raise ValueError("Validação pré-escrita FALHOU (nada foi gravado):\n  - "
                         + "\n  - ".join(erros_globais))

    # Relatório de conferência (sai também no --dry-run): chaves original ->
    # nova de 1 instrumento do lote, por tabela, para checagem manual no
    # banco de origem via DBeaver.
    loga_chaves_amostra(ordem, planos, lotes, mapeamentos, valores[0], fator_k)

    save_base = clone_base_path(config)
    if dry_run:
        logger.info("--dry-run: validações OK; NADA gravado (destino seria %s).",
                    save_base)
    else:
        save_base = _valida_destino(config)
        # Apaga o prefixo de clones POR INTEIRO: o destino reflete exatamente
        # ESTA execução (tabela com lote 0 ou removida do plano não deixa
        # Parquet velho para o processo de carga reaplicar).
        logger.info("Limpando destino %s (prefixo exclusivo deste job)...", save_base)
        _delete_path(spark, save_base)
        for t in ordem:
            if t not in resultados:
                continue
            clones, _ = resultados[t]
            out_path = f"{save_base}/{t}"
            logger.info("Gravando %s -> %s", t, out_path)
            escreve_tabela(spark, clones, out_path)
        # Mapa NUM_IF para auditoria (NUM_IF_ORIG, K, NUM_IF_NOVO).
        mapa_if = (mapeamentos[TABELA_RAIZ]
                   .select(F.col(f"old_{COL_NUM_IF}").alias("NUM_IF_ORIG"),
                           F.col(K_COL).alias("K"),
                           F.col(f"new_{COL_NUM_IF}").alias("NUM_IF_NOVO")))
        escreve_tabela(spark, mapa_if, f"{save_base}/{MAPA_NUM_IF_TABLE}")
        logger.info("Mapa de NUM_IF gravado em %s/%s.", save_base, MAPA_NUM_IF_TABLE)

    logger.info("=" * 78)
    logger.info("RESUMO DA CLONAGEM (%.1fs) — %d instrumento(s) × K=%d, "
                "data de engorda %s, %s",
                time.perf_counter() - inicio, len(valores), fator_k,
                engorda_ts.isoformat(sep=" "),
                "DRY-RUN (nada gravado)" if dry_run else f"gravado em {save_base}")
    for t in ordem:
        s = stats.get(t, {})
        logger.info("  %-32s lote=%-8s clones=%-8s remap=%s datas=%s",
                    t, s.get("lote", "-"), s.get("clones", "-"),
                    ",".join(s.get("colunas_remapeadas", [])) or "-",
                    ",".join(s.get("colunas_data", [])) or "-")
    logger.info("=" * 78)
    return stats


# ---------------------------------------------------------------------------
# Spark session (cópia das confs do engorda_tables.py — Data Flow).
# ---------------------------------------------------------------------------
_STATIC_SPARK_CONF = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.aggregatePushdown": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.network.timeout": "600s",
    "spark.executor.heartbeatInterval": "30s",
    "spark.shuffle.io.maxRetries": "10",
    "spark.shuffle.io.retryWait": "15s",
    "spark.executor.memoryOverheadFactor": "0.2",
}
_RUNTIME_SPARK_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
    "spark.sql.files.maxPartitionBytes": "512m",
    # Os lotes de clone são pequenos (N instrumentos × K); um shuffle.partitions
    # gigante como o do engorda só criaria tasks vazias. AQE coalesce resolve.
    "spark.sql.shuffle.partitions": "512",
}


def create_spark_session(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in {**_STATIC_SPARK_CONF, **_RUNTIME_SPARK_CONF}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    for key, value in _RUNTIME_SPARK_CONF.items():
        spark.conf.set(key, value)
    return spark


# ---------------------------------------------------------------------------
# CLI (argumentos do Data Flow).
# ---------------------------------------------------------------------------
def _parse_num_ifs(txt: str) -> List[int]:
    try:
        valores = [int(v.strip()) for v in txt.split(",") if v.strip()]
    except ValueError:
        raise argparse.ArgumentTypeError(
            "--num-ifs deve ser lista de inteiros separados por vírgula")
    if not valores:
        # Lista vazia cairia em silêncio no ramo de SORTEIO (if num_ifs:) e
        # clonaria um instrumento aleatório — melhor abortar aqui.
        raise argparse.ArgumentTypeError("--num-ifs não pode ser vazio")
    return valores


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("deve ser > 0")
    return parsed


def nonneg_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("deve ser >= 0")
    return parsed


def _parse_data_engorda(txt: str) -> datetime:
    """Data/hora do run: 'YYYY-MM-DD' (meia-noite) ou 'YYYY-MM-DD HH:MM:SS'.
    Existe para tornar o run REPRODUZÍVEL (rodar de novo e obter exatamente as
    mesmas datas); sem a flag, vale o instante de início do script."""
    txt = txt.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        "--data-engorda deve ser 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clonagem por entidade (clone-and-remap) do CDB simplificado. "
                    "Dirigido pelo spec_config.json; sem conexão Oracle.")
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--num-ifs", type=_parse_num_ifs, default=None,
                       help="Lista explícita de NUM_IF (ex.: 123,456). Aceita 1 só.")
    grupo.add_argument("--n-instrumentos", type=positive_int, default=None,
                       help="Sorteia N instrumentos do domínio do produto "
                            "(query de validação; com --seed).")
    parser.add_argument("--fator-k", type=positive_int, default=1,
                        help="Clones por instrumento (default 1).")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED,
                        help="Semente do sorteio de instrumentos.")
    parser.add_argument("--pk-offset", type=nonneg_int, default=0,
                        help="Piso absoluto para as PKs novas (como no engorda). "
                             "Default 0 (sem piso).")
    parser.add_argument("--pk-safety-band", type=nonneg_int, default=0,
                        help="Folga acima do max real de cada PK: distância "
                             "entre o max real e a PRIMEIRA PK nova. Default 0.")
    parser.add_argument("--pk-passo", type=positive_int, default=1,
                        help="Folga ENTRE PKs novas consecutivas (incremento). "
                             "1 = contíguo (default); 10 deixa 9 valores livres "
                             "entre cada clone. Só vale para PK OFFSET_PROPRIO.")
    parser.add_argument("--offset-num-if", type=positive_int, default=None,
                        help="Início explícito do NUM_IF novo (> max real).")
    parser.add_argument("--data-engorda", type=_parse_data_engorda, default=None,
                        help="Data/hora do run usada nas colunas DAT_* "
                             "('YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'). "
                             "Default: instante de início do script.")
    parser.add_argument("--prazo-vencimento-dias", type=positive_int, default=None,
                        help=f"{ENGORDA_COL_DAT_VENCIMENTO} = data da engorda + N "
                             "dias. Default: preserva o prazo original da linha "
                             f"clonada ({ENGORDA_COL_DAT_VENCIMENTO} - "
                             f"{ENGORDA_COL_DAT_EMISSAO}); prazo inválido cai em "
                             f"{DEFAULT_DT_VENCIMENTO_PRAZO_DIAS} dias.")
    parser.add_argument("--tratar-como-static", default="",
                        help="Tabelas a excluir da clonagem (vírgula).")
    parser.add_argument("--max-passadas", type=positive_int, default=6,
                        help="Passadas máximas do pertencimento (ciclos de FK). "
                             "Não estabilizou -> aborta pedindo aumento. Default 6.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Valida e loga; não grava nada.")
    parser.add_argument("--specs", default=None,
                        help="Override de DATAGEN_SPECS_URI (specs.json único).")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    config = get_engorda_env()
    spark = create_spark_session("DataGenClonaInstrumentos")
    try:
        specs_uri = args.specs or config["DATAGEN_SPECS_URI"]
        spec = load_specs(spark, specs_uri)
        executa_clonagem(
            spark, config, spec,
            num_ifs=args.num_ifs,
            n_instrumentos=args.n_instrumentos,
            fator_k=args.fator_k,
            seed=args.seed,
            pk_offset=args.pk_offset,
            pk_safety_band=args.pk_safety_band,
            pk_passo=args.pk_passo,
            offset_num_if=args.offset_num_if,
            tratar_como_static={t for t in args.tratar_como_static.split(",") if t.strip()},
            max_passadas=args.max_passadas,
            engorda_ts=args.data_engorda,
            prazo_vencimento_dias=args.prazo_vencimento_dias,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
