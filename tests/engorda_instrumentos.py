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

DIRIGIDO pelo spec_config.json (o mesmo do engorda_tables.py, gerado por
gera_spec_config.py). Usa Oracle no driver apenas para alocar COD_IF,
COD_OPERACAO e fazer o preflight de colisão de meu-número; não cria objetos.

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
  * Chaves únicas de NEGÓCIO: COD_IF e COD_OPERACAO são alocados pelas funções
    oficiais do Oracle para TODO clone (inclusive K=1). Controles P1/P2 são
    gerados localmente com prefixo obrigatório e preflight no destino.
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
append no Oracle). Também grava MAPA_CLONE_NUM_IF, MAPA_CLONE_COD_IF e
MAPA_CLONE_COD_OPERACAO para auditoria. Com --dry-run nada é gravado nem
alocado no Oracle: usa placeholders locais só para validação. Fora do dry-run,
toda a árvore é validada em staging irmão antes de substituir a saída anterior.
A publicação por rename NÃO é atômica em object storage: se o processo cair
entre os renames, restaure manualmente o backup `<destino>.__previous_*` para
o caminho fixo `<destino>` antes de consumir a saída.

USO (OCI Data Flow — mesmas envs do engorda_tables.py):
    envs: DATAGEN_RAW_BASE_URI, DATAGEN_SPECS_URI, DATAGEN_SYNTHETIC_BASE_URI,
          DATAGEN_SOURCE_JDBC_URL, DATAGEN_SOURCE_DB_USER,
          DATAGEN_SOURCE_DB_PASSWORD (nomes legados: apontam para o Oracle
          receptor; as três últimas são dispensadas no --dry-run)
          (+ opcionais DATAGEN_RAW_PREFIX, DATAGEN_SYNTHETIC_PREFIX,
           DATAGEN_CLONE_PREFIX — default "clones_instrumentos")
    argumentos:
      --num-ifs 12345,67890         # lista explícita (aceita 1 só), OU
      --n-instrumentos 5 --seed 42  # amostra do domínio do produto (query)
      --fator-k 3                   # clones por instrumento (default 1)
      --meu-numero-prefix 321       # obrigatório; exatamente 3 dígitos, 1º não-zero
      --oracle-code-batch-size 50000  # códigos por round-trip Oracle
      --dry-run                     # valida e loga, não grava
      --pk-safety-band 100000       # folga acima do max real (default 0)
      --pk-passo 10                 # folga ENTRE PKs novas consecutivas (default 1)
      --offset-num-if 900000000     # início explícito p/ NUM_IF novo (opcional)
      --data-engorda 2026-07-19     # data/hora do run (default: agora)
      --prazo-vencimento-dias 30    # DAT_VENCIMENTO = data + N (default: prazo original)
      --tratar-como-static TAB1,TAB2  # excluir tabela(s) da clonagem
      --sem-poda-subtipo            # DESLIGA a poda do item 1 (dangling CONDICAO_IF)
      --faltantes-arg 'CARTEIRA_COMITENTE.NUM_ID_ENTIDADE=343..;...'
                                    # itens 3/4: poda NUM_IF que referenciam chave
                                    #   inexistente no destino (QAB), sem Oracle
      --faltantes-parquet oci://.../faltantes  # idem, TABELA/COLUNA/VALOR (listas grandes)
      --anular-cols 'TAB.COL,COL2;...'  # item 2 (extra): colunas nullable a anular
      --specs oci://.../spec_config.json  # override de DATAGEN_SPECS_URI

CORREÇÕES DE CARGA (saída carregável por construção — ver executa_clonagem):
  1. CONDICAO_IF dangling (Cat 1): poda do domínio os NUM_IF cujo subtipo não
     existe na origem; a amostragem repõe até fechar N (--sem-poda-subtipo desliga).
  2. NUM_ID_TRANSF_ARQ_P1/P2 órfãos: anulados nos clones de OPERACAO (nullable) —
     ver NULIFICA_COLS_POR_TABELA / --anular-cols.
  3/4. Comitente/conta inexistentes no destino: poda do domínio os NUM_IF que os
     referenciam, via --faltantes-arg/--faltantes-parquet (sem conexão Oracle).

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
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

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
# Poda de domínio (itens 1, 3 e 4) — instrumentos que o clone NÃO conseguiria
# deixar carregável são removidos do domínio ANTES da amostragem. A amostragem
# de N sorteia do domínio JÁ PODADO, então a contagem final continua N (cada
# instrumento podado é reposto por outra amostra válida — não sobra "buraco").
#
# Item 1 — polimorfismo CONDICAO_IF. COD_TIPO_CONDICAO_IF -> tabela-subtipo
# física (joined-subclass do Hibernate), igual ao SUBTYPE_BY_TIPO do
# validate_cdb_simplificado.py. Uma CONDICAO_IF ativa SEM a linha na sua
# tabela-subtipo fica "dangling": o Hibernate não consegue tipar a classe e o
# batch estoura ClassCastException (Cat 1 do validador). Como NUM_CONDICAO_IF é
# a PK de CONDICAO_IF (globalmente única), uma chave só pode viver na
# tabela-subtipo do seu próprio tipo — basta checar presença na UNIÃO das
# fontes-subtipo clonáveis (lidas com o MESMO _read_source do clone, então a
# checagem enxerga exatamente o que o clone produziria: p.ex. RESGATE só conta
# com COD_COND_RESGATE='SEM TABELA').
# ---------------------------------------------------------------------------
CONDICAO_IF_TABLE = "CONDICAO_IF"
CONDICAO_IF_PK = "NUM_CONDICAO_IF"
CONDICAO_IF_TIPO_COL = "COD_TIPO_CONDICAO_IF"
SUBTYPE_BY_TIPO: dict[str, str] = {
    "1": "AMORTIZACAO", "2": "JUROS_FIXO", "3": "JUROS_FLUTUANTE",
    "4": "ATUALIZACAO_POS", "5": "SPREAD", "6": "PARTICIPACAO_LUCROS",
    "7": "PREMIO", "14": "ATUALIZACAO_PRE", "15": "PREMIO_OPCAO",
    "16": "TERMO", "17": "PARAMETRO_LIMITE", "20": "RESGATE",
    "21": "PREMIO_CONTRATO", "22": "OPCAO", "23": "RESET", "24": "DESDOBRAMENTO",
}

# Item 2 — colunas nullable ANULADAS nos clones por serem drift entre o snapshot
# de origem e o destino (QAB): NUM_ID_TRANSF_ARQ_P1/P2 de OPERACAO apontam para
# TRANSFERENCIA_ARQUIVO inexistentes no destino. Como são nullable (não estão em
# not_null_cols), anular remove o órfão de FK sem perder a operação — a maioria
# das operações já as tem nulas. Declarativo {TABELA: (col, ...)}; --anular-cols
# acrescenta entradas em tempo de execução.
NULIFICA_COLS_POR_TABELA: dict[str, tuple[str, ...]] = {
    "OPERACAO": ("NUM_ID_TRANSF_ARQ_P1", "NUM_ID_TRANSF_ARQ_P2"),
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
# Nomes legados "SOURCE"; neste job apontam contratualmente para o Oracle
# receptor, usado no driver para alocação oficial e preflight antes da publicação.
ORACLE_ENV_VARS = (
    "DATAGEN_SOURCE_JDBC_URL",
    "DATAGEN_SOURCE_DB_USER",
    "DATAGEN_SOURCE_DB_PASSWORD",
)
DEFAULT_CLONE_PREFIX = "clones_instrumentos"
DEFAULT_SEED = 42
MAPA_NUM_IF_TABLE = "MAPA_CLONE_NUM_IF"
MAPA_COD_IF_TABLE = "MAPA_CLONE_COD_IF"
MAPA_COD_OPERACAO_TABLE = "MAPA_CLONE_COD_OPERACAO"
DEFAULT_ORACLE_CODE_BATCH_SIZE = 50_000
MAX_MEU_NUMERO_ORDINAL = 9_999_999
COD_IF_PATTERN = r"^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$"
COD_OPERACAO_PATTERN = r"^[0-9]{16}$"
MEU_PREFIX_PATTERN = re.compile(r"^[1-9][0-9]{2}$")
ACCOUNT_CODE_PATTERN = r"^[0-9]{5}\.(40|10)-[0-9]$"

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
    for name in ORACLE_ENV_VARS:
        value = os.environ.get(name)
        if value:
            config[name] = value
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
def _strict_lookup_eligible_domain(
    dominio: DataFrame,
    operacao: DataFrame,
    tipo_oper_objeto_serv: DataFrame,
    tipo_operacao: DataFrame,
    conta_participante: DataFrame,
    titulo: DataFrame,
    deposito: DataFrame,
) -> DataFrame:
    """Exclui o instrumento inteiro se qualquer referência clonada for inválida.

    O domínio usa somente tabelas raw disponíveis. As views adicionais continuam
    sob responsabilidade do validador Python conectado ao destino.
    """
    candidatos = dominio.select(COL_NUM_IF).dropDuplicates()
    op = operacao.join(candidatos, on=COL_NUM_IF, how="left_semi").alias("o")
    tos = tipo_oper_objeto_serv.alias("tos")
    top = tipo_operacao.alias("top")
    p1 = conta_participante.alias("p1")
    p2 = conta_participante.alias("p2")
    joined = (op
              .join(tos,
                    F.col("o.NUM_ID_TIPO_OPER_OBJETO_SERV") ==
                    F.col("tos.NUM_ID_TIPO_OPER_OBJETO_SERV"), "left")
              .join(top,
                    F.col("tos.NUM_ID_TIPO_OPERACAO") ==
                    F.col("top.NUM_ID_TIPO_OPERACAO"), "left")
              .join(p1,
                    F.col("o.NUM_CONTA_PARTICIPANTE_P1") ==
                    F.col("p1.NUM_CONTA_PARTICIPANTE"), "left")
              .join(p2,
                    F.col("o.NUM_CONTA_PARTICIPANTE_P2") ==
                    F.col("p2.NUM_CONTA_PARTICIPANTE"), "left"))

    def _nonblank(name: str):
        return F.col(name).isNotNull() & (F.trim(F.col(name).cast("string")) != "")

    def _exact_number(name: str, expected: str):
        return _norm_key_col(F.col(name)) == F.lit(expected)

    def _valid_account(raw_col: str, alias: str):
        return (_nonblank(raw_col)
                & F.col(f"{alias}.NUM_CONTA_PARTICIPANTE").isNotNull()
                & _exact_number(f"{alias}.NUM_ID_SITUACAO_CONTA", "1")
                & F.trim(F.col(f"{alias}.COD_CONTA_PARTICIPANTE").cast("string"))
                .rlike(ACCOUNT_CODE_PATTERN))

    valid_op = (
        _nonblank("o.NUM_ID_TIPO_OPER_OBJETO_SERV")
        & F.col("tos.NUM_ID_TIPO_OPER_OBJETO_SERV").isNotNull()
        & _exact_number("tos.NUM_ID_OBJETO_SERVICO", "44")
        & (F.trim(F.col("tos.IND_DISPONIVEL_IDENTIFICACAO")) == F.lit("S"))
        & F.col("top.NUM_ID_TIPO_OPERACAO").isNotNull()
        & _exact_number("top.COD_TIPO_OPERACAO", "1")
        & _valid_account("o.NUM_CONTA_PARTICIPANTE_P1", "p1")
        & _valid_account("o.NUM_CONTA_PARTICIPANTE_P2", "p2")
    )
    invalidos = joined.where(~F.coalesce(valid_op, F.lit(False))).select(
        F.col(f"o.{COL_NUM_IF}").alias(COL_NUM_IF))

    def _invalid_optional_account(rows: DataFrame) -> DataFrame:
        r = rows.join(candidatos, on=COL_NUM_IF, how="left_semi").alias("r")
        cp = conta_participante.alias("cp")
        checked = r.join(
            cp,
            F.col("r.NUM_CONTA_PARTICIPANTE") == F.col("cp.NUM_CONTA_PARTICIPANTE"),
            "left",
        )
        supplied = F.col("r.NUM_CONTA_PARTICIPANTE").isNotNull()
        valid = (_nonblank("r.NUM_CONTA_PARTICIPANTE")
                 & F.col("cp.NUM_CONTA_PARTICIPANTE").isNotNull()
                 & _exact_number("cp.NUM_ID_SITUACAO_CONTA", "1")
                 & F.trim(F.col("cp.COD_CONTA_PARTICIPANTE").cast("string"))
                 .rlike(ACCOUNT_CODE_PATTERN))
        return checked.where(supplied & ~F.coalesce(valid, F.lit(False))).select(
            F.col(f"r.{COL_NUM_IF}").alias(COL_NUM_IF))

    invalidos = (invalidos
                  .unionByName(_invalid_optional_account(titulo))
                  .unionByName(_invalid_optional_account(deposito))
                  .dropDuplicates())
    return candidatos.join(invalidos, on=COL_NUM_IF, how="left_anti")


def _dominio_num_if_produto(spark, config) -> DataFrame:
    """Domínio elegível: query oficial preservada, seguida da política raw hard.

    A base roda via spark.sql lendo os Parquet RAW diretamente
    (parquet.`<path>`), com paths montados por raw_path. Depois, anti-joins
    instrument-level removem qualquer operação/conta incompatível com a carga.

    Query oficial (manter em sincronia com nova1/nova2/nova3):

        WITH FILTRO_BASE AS (
            SELECT DISTINCT IFE.NUM_IF
            FROM CETIP.INSTRUMENTO_FINANCEIRO IFE
                INNER JOIN CETIP.TITULO      TIT ON TIT.NUM_IF = IFE.NUM_IF
                INNER JOIN CETIP.CONDICAO_IF CIF ON CIF.NUM_IF = IFE.NUM_IF
                INNER JOIN CETIP.RESGATE     RES ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
            WHERE IFE.NUM_TIPO_IF = 49
              AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
              AND RES.COD_COND_RESGATE = 'SEM TABELA'
              AND IFE.DAT_EXCLUSAO IS NULL
              AND CIF.DAT_EXCLUSAO IS NULL
              AND RES.DAT_EXCLUSAO IS NULL
        ),
        EVENTOS_IF AS (
            SELECT E.NUM_IF,
                   MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 83 THEN 1 ELSE 0 END) QE83,
                   MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 85 THEN 1 ELSE 0 END) QE85
            FROM CETIP.EVENTO E INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = E.NUM_IF
            GROUP BY E.NUM_IF
        ),
        FLAGS_IF AS (
            SELECT C.NUM_IF,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  1 THEN 1 ELSE 0 END) QC01,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  2 THEN 1 ELSE 0 END) QC02,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  3 THEN 1 ELSE 0 END) QC03,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  4 THEN 1 ELSE 0 END) QC04,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  5 THEN 1 ELSE 0 END) QC05,
                   MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF = 14 THEN 1 ELSE 0 END) QC14,
                   MAX(CASE WHEN JFL.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFL,
                   MAX(CASE WHEN JFI.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFI
            FROM CETIP.CONDICAO_IF C
                INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = C.NUM_IF
                LEFT JOIN CETIP.JUROS_FLUTUANTE JFL ON JFL.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
                LEFT JOIN CETIP.JUROS_FIXO      JFI ON JFI.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
            WHERE C.DAT_EXCLUSAO IS NULL AND C.COD_TIPO_CONDICAO_IF <> 20
            GROUP BY C.NUM_IF
        ),
        AGREGADO_BASE AS (
            SELECT COUNT(*) QTDE_BASE FROM FILTRO_BASE
        ),
        AGREGADO_FLAGS AS (
            SELECT SUM(NVL(F.QC01,0)) QC01, SUM(NVL(F.QC02,0)) QC02,
                   SUM(NVL(F.QC03,0)) QC03, SUM(NVL(F.QC04,0)) QC04,
                   SUM(NVL(F.QC05,0)) QC05, SUM(NVL(F.QC14,0)) QC14,
                   SUM(NVL(F.QJFL,0)) QJFL, SUM(NVL(F.QJFI,0)) QJFI,
                   SUM(NVL(E.QE83,0)) QE83, SUM(NVL(E.QE85,0)) QE85
            FROM FILTRO_BASE FB
                LEFT JOIN FLAGS_IF   F ON F.NUM_IF = FB.NUM_IF
                LEFT JOIN EVENTOS_IF E ON E.NUM_IF = FB.NUM_IF
        ),
        DEP_IF AS (
            SELECT COUNT(DISTINCT DP.NUM_IF) QDEP
            FROM CETIP.DEPOSITO_AUTOMATICO_IF DP
                JOIN FILTRO_BASE FB ON FB.NUM_IF = DP.NUM_IF
        ),
        COM_IF AS (
            SELECT COUNT(DISTINCT CM.NUM_IF) QCOM
            FROM CETIP.CARTEIRA_COMITENTE CM
                JOIN FILTRO_BASE FB ON FB.NUM_IF = CM.NUM_IF
            WHERE CM.QTD_CARTEIRA_COMITENTE > 0
        ),
        CPA_IF AS (
            SELECT COUNT(DISTINCT CP.NUM_IF) QCPA
            FROM CETIP.CARTEIRA_PARTICIPANTE CP
                JOIN FILTRO_BASE FB ON FB.NUM_IF = CP.NUM_IF
            WHERE CP.QTD_CARTEIRA_PARTICIPANTE > 0
        )
        SELECT DISTINCT f.NUM_IF
        FROM FLAGS_IF f
            INNER JOIN CETIP.OPERACAO             o  ON o.NUM_IF = f.NUM_IF
            INNER JOIN CETIP.DADO_OPERACAO        do ON do.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
            INNER JOIN CETIP.LANCAMENTO           l  ON l.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
            INNER JOIN CETIP.ESPECIFICACAO        e2 ON e2.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
            INNER JOIN CETIP.ESPECIFICACAO_COMITENTE ec
                       ON ec.NUM_ID_ESPECIFICACAO = e2.NUM_ID_ESPECIFICACAO

    Nota: EVENTOS_IF, AGREGADO_BASE, AGREGADO_FLAGS, DEP_IF, COM_IF e CPA_IF são
    declaradas na query oficial mas NÃO são referenciadas pelo SELECT final — o
    Spark não materializa CTE não referenciada. Mantidas idênticas à oficial (só
    CETIP.<TAB> vira parquet.`<path>`). O SELECT final agora faz INNER JOIN com a
    cadeia OPERACAO -> DADO_OPERACAO / LANCAMENTO / ESPECIFICACAO ->
    ESPECIFICACAO_COMITENTE, então o domínio efetivo é FILTRO_BASE ∩ instrumentos
    com CONDICAO_IF ativa de tipo <> 20 ∩ instrumentos que têm essa cadeia de
    operação/especificação completa (cada INNER JOIN restringe o domínio).
    """
    p_ife = raw_path(config, TABELA_RAIZ)
    p_tit = raw_path(config, "TITULO")
    p_cif = raw_path(config, "CONDICAO_IF")
    p_res = raw_path(config, "RESGATE")
    p_eve = raw_path(config, "EVENTO")
    p_jfl = raw_path(config, "JUROS_FLUTUANTE")
    p_jfi = raw_path(config, "JUROS_FIXO")
    p_dep = raw_path(config, "DEPOSITO_AUTOMATICO_IF")
    p_com = raw_path(config, "CARTEIRA_COMITENTE")
    p_cpa = raw_path(config, "CARTEIRA_PARTICIPANTE")
    p_ope = raw_path(config, "OPERACAO")
    p_dop = raw_path(config, "DADO_OPERACAO")
    p_lan = raw_path(config, "LANCAMENTO")
    p_esp = raw_path(config, "ESPECIFICACAO")
    p_epc = raw_path(config, "ESPECIFICACAO_COMITENTE")
    p_tos = raw_path(config, "TIPO_OPER_OBJETO_SERV")
    p_top = raw_path(config, "TIPO_OPERACAO")
    p_con = raw_path(config, "CONTA_PARTICIPANTE")
    sql = f"""
    WITH FILTRO_BASE AS (
        SELECT DISTINCT IFE.NUM_IF
        FROM parquet.`{p_ife}` IFE
            INNER JOIN parquet.`{p_tit}` TIT ON TIT.NUM_IF = IFE.NUM_IF
            INNER JOIN parquet.`{p_cif}` CIF ON CIF.NUM_IF = IFE.NUM_IF
            INNER JOIN parquet.`{p_res}` RES ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
        WHERE IFE.NUM_TIPO_IF = 49
            AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
            AND RES.COD_COND_RESGATE = 'SEM TABELA'
            AND IFE.DAT_EXCLUSAO IS NULL
            AND CIF.DAT_EXCLUSAO IS NULL
            AND RES.DAT_EXCLUSAO IS NULL
    ),
    EVENTOS_IF AS (
        SELECT E.NUM_IF,
               MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 83 THEN 1 ELSE 0 END) QE83,
               MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 85 THEN 1 ELSE 0 END) QE85
        FROM parquet.`{p_eve}` E
            INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = E.NUM_IF
        GROUP BY E.NUM_IF
    ),
    FLAGS_IF AS (
        SELECT C.NUM_IF,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  1 THEN 1 ELSE 0 END) QC01,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  2 THEN 1 ELSE 0 END) QC02,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  3 THEN 1 ELSE 0 END) QC03,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  4 THEN 1 ELSE 0 END) QC04,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  5 THEN 1 ELSE 0 END) QC05,
               MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF = 14 THEN 1 ELSE 0 END) QC14,
               MAX(CASE WHEN JFL.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFL,
               MAX(CASE WHEN JFI.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFI
        FROM parquet.`{p_cif}` C
            INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = C.NUM_IF
            LEFT JOIN parquet.`{p_jfl}` JFL ON JFL.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
            LEFT JOIN parquet.`{p_jfi}` JFI ON JFI.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
        WHERE C.DAT_EXCLUSAO IS NULL
            AND C.COD_TIPO_CONDICAO_IF <> 20
        GROUP BY C.NUM_IF
    ),
    AGREGADO_BASE AS (
        SELECT COUNT(*) QTDE_BASE FROM FILTRO_BASE
    ),
    AGREGADO_FLAGS AS (
        SELECT SUM(NVL(F.QC01,0)) QC01, SUM(NVL(F.QC02,0)) QC02,
               SUM(NVL(F.QC03,0)) QC03, SUM(NVL(F.QC04,0)) QC04,
               SUM(NVL(F.QC05,0)) QC05, SUM(NVL(F.QC14,0)) QC14,
               SUM(NVL(F.QJFL,0)) QJFL, SUM(NVL(F.QJFI,0)) QJFI,
               SUM(NVL(E.QE83,0)) QE83, SUM(NVL(E.QE85,0)) QE85
        FROM FILTRO_BASE FB
            LEFT JOIN FLAGS_IF   F ON F.NUM_IF = FB.NUM_IF
            LEFT JOIN EVENTOS_IF E ON E.NUM_IF = FB.NUM_IF
    ),
    DEP_IF AS (
        SELECT COUNT(DISTINCT DP.NUM_IF) QDEP
        FROM parquet.`{p_dep}` DP
            JOIN FILTRO_BASE FB ON FB.NUM_IF = DP.NUM_IF
    ),
    COM_IF AS (
        SELECT COUNT(DISTINCT CM.NUM_IF) QCOM
        FROM parquet.`{p_com}` CM
            JOIN FILTRO_BASE FB ON FB.NUM_IF = CM.NUM_IF
        WHERE CM.QTD_CARTEIRA_COMITENTE > 0
    ),
    CPA_IF AS (
        SELECT COUNT(DISTINCT CP.NUM_IF) QCPA
        FROM parquet.`{p_cpa}` CP
            JOIN FILTRO_BASE FB ON FB.NUM_IF = CP.NUM_IF
        WHERE CP.QTD_CARTEIRA_PARTICIPANTE > 0
    )
    SELECT DISTINCT f.NUM_IF
    FROM FLAGS_IF f
        INNER JOIN parquet.`{p_ope}` o  ON o.NUM_IF = f.NUM_IF
        INNER JOIN parquet.`{p_dop}` do ON do.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
        INNER JOIN parquet.`{p_lan}` l  ON l.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
        INNER JOIN parquet.`{p_esp}` e2 ON e2.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
        INNER JOIN parquet.`{p_epc}` ec ON ec.NUM_ID_ESPECIFICACAO = e2.NUM_ID_ESPECIFICACAO
    """
    base = spark.sql(sql)
    return _strict_lookup_eligible_domain(
        base,
        _read_source(spark, config, "OPERACAO"),
        read_parquet(spark, p_tos),
        read_parquet(spark, p_top),
        read_parquet(spark, p_con),
        _read_source(spark, config, "TITULO"),
        _read_source(spark, config, "DEPOSITO_AUTOMATICO_IF"),
    )


# ---------------------------------------------------------------------------
# Poda de domínio (itens 1, 3 e 4) e anulação de colunas de drift (item 2).
# ---------------------------------------------------------------------------
def _norm_key_col(col):
    """Normaliza uma chave para string comparável: valor numérico perde o '.0'
    final (a mesma chave pode chegar como decimal numa fonte e int noutra — ver
    harmoniza_tipos_fk_com_pai / os falsos fk_orphan por tipo físico)."""
    return F.regexp_replace(F.trim(col.cast("string")), r"\.0+$", "")


def _subtipos_clonaveis(spec: dict) -> List[str]:
    """Tabelas-subtipo de CONDICAO_IF que o clone realmente produz: presentes no
    spec e não-static. Uma condição de tipo concreto sem linha aqui vira dangling
    (Cat 1). Ordem estável, sem repetição."""
    return [s for s in dict.fromkeys(SUBTYPE_BY_TIPO.values())
            if s in spec and not spec[s].get("static")]


def _num_if_inconsistentes_subtipo(spark, config, spec, dominio: DataFrame) -> DataFrame:
    """NUM_IF do domínio cujo clone teria ao menos UMA CONDICAO_IF ativa sem a
    respectiva linha-subtipo clonável — os dangling da Cat 1 (item 1). Base da
    poda: excluídos do sorteio, o lote nasce sem ClassCastException.

    NUM_CONDICAO_IF é a PK de CONDICAO_IF (única): uma chave só pode viver na
    tabela-subtipo do seu próprio tipo, então "presente na UNIÃO das
    fontes-subtipo clonáveis" == "presente no subtipo esperado". As fontes são
    lidas com o MESMO _read_source do clone (FILTROS_FONTE incluso), de modo que
    a checagem enxerga exatamente o que o clone gravaria (ex.: RESGATE só entra
    com COD_COND_RESGATE='SEM TABELA')."""
    cond = (_read_source(spark, config, CONDICAO_IF_TABLE)
            .select(F.col(COL_NUM_IF).alias(COL_NUM_IF),
                    _norm_key_col(F.col(CONDICAO_IF_PK)).alias("__nci"))
            .join(dominio.select(COL_NUM_IF), on=COL_NUM_IF, how="left_semi"))
    presente = None
    for s in _subtipos_clonaveis(spec):
        try:
            sdf = _read_source(spark, config, s)
        except Exception as exc:  # fonte ausente: trata como sem chaves (conservador)
            logger.warning("poda subtipo: não li a fonte de %s (%s); condições "
                           "desse tipo entram como dangling.", s, exc)
            continue
        if CONDICAO_IF_PK not in sdf.columns:
            logger.warning("poda subtipo: %s sem coluna %s; ignorada.",
                           s, CONDICAO_IF_PK)
            continue
        piece = sdf.select(_norm_key_col(F.col(CONDICAO_IF_PK)).alias("__nci"))
        presente = piece if presente is None else presente.unionByName(piece)
    if presente is None:
        # Nenhuma tabela-subtipo clonável: toda condição concreta seria dangling.
        return cond.select(COL_NUM_IF).dropDuplicates()
    dangling = cond.join(presente.dropDuplicates(), on="__nci", how="left_anti")
    return dangling.select(COL_NUM_IF).dropDuplicates()


def _parse_faltantes_arg(txt: str) -> List[Tuple[str, str, List[str]]]:
    """'TABELA.COLUNA=v1,v2;TAB2.COL2=v3' -> [(TAB, COL, [v1, v2]), ...]."""
    out: List[Tuple[str, str, List[str]]] = []
    for grupo in txt.split(";"):
        grupo = grupo.strip()
        if not grupo:
            continue
        chave, sep, vals = grupo.partition("=")
        if not sep or "." not in chave:
            raise ValueError(f"--faltantes-arg: entrada inválida {grupo!r} "
                             "(use TABELA.COLUNA=v1,v2;...).")
        tab, _, col = chave.partition(".")
        valores = [v.strip() for v in vals.split(",") if v.strip()]
        if valores:
            out.append((table_path_name(tab.strip().upper()),
                        col.strip().upper(), valores))
    return out


def _carrega_faltantes(spark, config, faltantes_arg: Optional[str],
                       faltantes_parquet: Optional[str]) -> Optional[DataFrame]:
    """DataFrame [TABELA, COLUNA, VALOR] das chaves de referência que NÃO existem
    no destino (QAB) — itens 3/4, sem conexão Oracle. Vem de --faltantes-arg
    (inline) e/ou --faltantes-parquet (colunas TABELA/COLUNA/VALOR). VALOR é
    normalizado para string comparável. Sem nenhuma fonte -> None (sem poda 3/4)."""
    df: Optional[DataFrame] = None
    linhas: List[Tuple[str, str, str]] = []
    if faltantes_arg:
        for tab, col, valores in _parse_faltantes_arg(faltantes_arg):
            linhas.extend((tab, col, v) for v in valores)
    if linhas:
        df = spark.createDataFrame(linhas, ["TABELA", "COLUNA", "VALOR"])
    if faltantes_parquet:
        pq = read_parquet(spark, faltantes_parquet)
        cm = {c.upper(): c for c in pq.columns}
        need = ["TABELA", "COLUNA", "VALOR"]
        faltam = [n for n in need if n not in cm]
        if faltam:
            raise ValueError(f"--faltantes-parquet precisa das colunas {need}; "
                             f"faltam {faltam} (achei {pq.columns}).")
        pqn = pq.select(
            F.element_at(F.split(F.upper(F.trim(F.col(cm["TABELA"]))), r"\."), -1)
             .alias("TABELA"),
            F.upper(F.trim(F.col(cm["COLUNA"]))).alias("COLUNA"),
            F.col(cm["VALOR"]).cast("string").alias("VALOR"))
        df = pqn if df is None else df.unionByName(pqn)
    if df is None:
        return None
    return df.select(F.col("TABELA"), F.col("COLUNA"),
                     _norm_key_col(F.col("VALOR")).alias("VALOR")).dropDuplicates()


def _num_if_excluidos_por_faltantes(spark, config, spec, faltantes: DataFrame,
                                    dominio: DataFrame) -> DataFrame:
    """NUM_IF do domínio a podar (itens 3/4): instrumentos cujo cluster referencia
    uma chave inexistente no destino. Só tabelas COM coluna NUM_IF (o instrumento
    é alcançável direto) — ex.: CARTEIRA_COMITENTE carrega NUM_ID_ENTIDADE
    (comitente) e NUM_CONTA (conta) por NUM_IF. Tabela sem NUM_IF (ex.:
    ESPECIFICACAO_COMITENTE) é coberta transitivamente: o mesmo comitente sai do
    lote quando o instrumento é podado via CARTEIRA_COMITENTE."""
    pares = [(r["TABELA"], r["COLUNA"]) for r in
             faltantes.select("TABELA", "COLUNA").dropDuplicates().collect()]
    excl: Optional[DataFrame] = None
    for tab, col in pares:
        if tab not in spec:
            logger.warning("faltantes: tabela %s fora do spec; ignorada.", tab)
            continue
        try:
            src = _read_source(spark, config, tab)
        except Exception as exc:
            logger.warning("faltantes: não li a fonte de %s (%s); ignorada.", tab, exc)
            continue
        if COL_NUM_IF not in src.columns:
            logger.warning("faltantes: %s não tem %s — não dá p/ alcançar o "
                           "instrumento; use uma tabela com NUM_IF (ex.: "
                           "CARTEIRA_COMITENTE). Ignorada.", tab, COL_NUM_IF)
            continue
        if col not in src.columns:
            logger.warning("faltantes: %s sem coluna %s; ignorada.", tab, col)
            continue
        miss = (faltantes.where((F.col("TABELA") == F.lit(tab))
                                & (F.col("COLUNA") == F.lit(col)))
                .select(F.col("VALOR").alias("__v")).dropDuplicates())
        hit = (src.select(F.col(COL_NUM_IF).alias(COL_NUM_IF),
                          _norm_key_col(F.col(col)).alias("__v"))
               .join(F.broadcast(miss), on="__v", how="left_semi")
               .select(COL_NUM_IF).dropDuplicates())
        n = hit.count()
        logger.info("faltantes: %s.%s -> %d instrumento(s) do domínio a podar.",
                    tab, col, n)
        excl = hit if excl is None else excl.unionByName(hit)
    if excl is None:
        return dominio.select(COL_NUM_IF).limit(0)
    return (excl.join(dominio.select(COL_NUM_IF), on=COL_NUM_IF, how="left_semi")
            .dropDuplicates())


def aplica_nulificacao(df: DataFrame, tabela: str,
                       cols_por_tabela: Mapping[str, Sequence[str]],
                       not_null_cols: Optional[Set[str]] = None,
                       ) -> Tuple[DataFrame, List[str]]:
    """Anula (seta NULL) colunas nullable marcadas como drift de destino (item 2:
    NUM_ID_TRANSF_ARQ_P1/P2 de OPERACAO -> TRANSFERENCIA_ARQUIVO inexistente no
    QAB). Só atua em coluna existente e NÃO NOT NULL: anular uma NOT NULL apenas
    trocaria o fk_orphan por um ORA-01400, então essas são puladas com aviso (a
    validação pré-escrita pegaria de qualquer forma). Devolve (df, anuladas)."""
    nn = not_null_cols or set()
    anuladas: List[str] = []
    for c in (cols_por_tabela.get(tabela) or []):
        if c not in df.columns:
            continue
        if c in nn:
            logger.warning("%s.%s é NOT NULL no spec; anulação IGNORADA (anular "
                           "trocaria fk_orphan por ORA-01400).", tabela, c)
            continue
        df = df.withColumn(c, F.lit(None).cast(df.schema[c].dataType))
        anuladas.append(c)
    return df, anuladas


def seleciona_instrumentos(spark, config, spec, num_ifs: Optional[List[int]],
                           n_instrumentos: Optional[int], seed: int,
                           faltantes: Optional[DataFrame] = None,
                           poda_subtipo: bool = True) -> List:
    """Devolve a lista de valores de NUM_IF do lote (coletada no driver — o lote
    é pequeno por definição). O SORTEIO e a VALIDAÇÃO de lista explícita rodam
    contra o domínio do produto (ver _dominio_num_if_produto), JÁ PODADO:

      * item 1 (poda_subtipo): remove instrumentos que gerariam CONDICAO_IF
        dangling (subtipo sem linha na origem — Cat 1);
      * itens 3/4 (faltantes): remove instrumentos que referenciam comitente/
        conta/etc. inexistentes no destino (Cat 3/4, sem Oracle).

    A amostragem de N sorteia do domínio PODADO, então a contagem final continua
    N (cada instrumento podado é reposto por outra amostra válida — o que atende
    "no final tem que vir N NUM_IF"). Lista explícita NÃO é reposta: se algum
    pedido cair na poda, ABORTA dizendo qual e por quê (não troca em silêncio)."""
    if num_ifs is not None and not num_ifs:
        # Defensivo (o argparse já barra): lista vazia NÃO pode virar sorteio.
        raise ValueError("Lista de NUM_IF vazia; informe valores ou use "
                         "--n-instrumentos.")
    fonte = _dominio_num_if_produto(spark, config).select(COL_NUM_IF).dropDuplicates()
    logger.info("Domínio de amostragem/validação de NUM_IF: query do produto + "
                "política raw hard de TOS e contas (P1/P2/título/depósito).")

    # Poda de domínio: junta as exclusões dos itens 1/3/4 e tira do domínio.
    exclusoes: List[Tuple[str, DataFrame]] = []
    if poda_subtipo:
        exclusoes.append(("subtipo dangling (Cat 1)",
                          _num_if_inconsistentes_subtipo(spark, config, spec, fonte)))
    if faltantes is not None:
        exclusoes.append(("chave inexistente no destino (Cat 3/4)",
                          _num_if_excluidos_por_faltantes(spark, config, spec,
                                                          faltantes, fonte)))
    excluir: Optional[DataFrame] = None
    for rotulo, df in exclusoes:
        df = df.select(COL_NUM_IF).dropDuplicates().localCheckpoint(eager=True)
        logger.info("Poda de domínio [%s]: %d instrumento(s) removido(s).",
                    rotulo, df.count())
        excluir = df if excluir is None else excluir.unionByName(df)
    if excluir is not None:
        excluir = excluir.dropDuplicates().localCheckpoint(eager=True)
        valido = fonte.join(excluir, on=COL_NUM_IF, how="left_anti")
    else:
        valido = fonte
    valido = valido.localCheckpoint(eager=True)

    if num_ifs:
        pedidos = F.broadcast(
            spark.createDataFrame([(v,) for v in num_ifs], [COL_NUM_IF])
            .select(F.col(COL_NUM_IF).cast(fonte.schema[COL_NUM_IF].dataType)))
        no_dominio = {int(r[0]) for r in
                      fonte.join(pedidos, on=COL_NUM_IF, how="left_semi").collect()}
        validos = {int(r[0]) for r in
                   valido.join(pedidos, on=COL_NUM_IF, how="left_semi").collect()}
        fora = [v for v in num_ifs if v not in no_dominio]
        podados = [v for v in num_ifs if v in no_dominio and v not in validos]
        if fora or podados:
            partes = []
            if fora:
                partes.append(f"fora do domínio do produto: {fora}")
            if podados:
                partes.append("no domínio mas PODADOS (subtipo dangling e/ou "
                              f"chave inexistente no destino): {podados}")
            raise ValueError("NUM_IF(s) não clonáveis — " + "; ".join(partes))
        valores = sorted(validos)
    else:
        n = int(n_instrumentos or 1)
        valores = [r[0] for r in valido.orderBy(F.rand(seed)).limit(n).collect()]
        if len(valores) < n:
            raise ValueError(
                f"Domínio VÁLIDO após a poda (subtipo/destino) tem só "
                f"{len(valores)} instrumento(s); pedi {n}. Afrouxe os filtros "
                "(--sem-poda-subtipo / menos faltantes) ou reduza --n-instrumentos.")
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
# Chaves de negócio geradas pelo Oracle e meu-número da operação.
# ---------------------------------------------------------------------------
def _validate_meu_numero_prefix(value: str) -> str:
    if not MEU_PREFIX_PATTERN.fullmatch(value or ""):
        raise argparse.ArgumentTypeError(
            "--meu-numero-prefix deve ter exatamente 3 dígitos e começar em 1-9")
    return value


def _validate_meu_capacity(allocated: int) -> None:
    if allocated > MAX_MEU_NUMERO_ORDINAL:
        raise ValueError(
            f"meu-número excede capacidade: {allocated} > {MAX_MEU_NUMERO_ORDINAL}")


def _oracle_credentials(config: Mapping[str, str]) -> Tuple[str, str, str]:
    missing = [name for name in ORACLE_ENV_VARS if not config.get(name)]
    if missing:
        raise ValueError("Env var(s) Oracle obrigatória(s) ausente(s): " + ", ".join(missing))
    return tuple(config[name] for name in ORACLE_ENV_VARS)  # type: ignore[return-value]


def _open_oracle_connection(jvm, jdbc_url: str, user: str, password: str):
    try:
        # Spark carrega --jars no context classloader. DriverManager chamado via
        # Py4J pode usar outro loader e responder "No suitable driver" embora o
        # mesmo jar funcione no DataFrameReader JDBC do validador.
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        driver_class = loader.loadClass("oracle.jdbc.OracleDriver")
        driver = driver_class.newInstance()
        properties = jvm.java.util.Properties()
        properties.setProperty("user", user)
        properties.setProperty("password", password)
        connection = driver.connect(jdbc_url, properties)
        if connection is None:
            raise RuntimeError("OracleDriver não aceitou a JDBC URL")
        return connection
    except Exception:
        # A exceção Java pode incluir a URL; o erro público não replica credenciais.
        raise RuntimeError("falha ao abrir conexão Oracle no driver") from None


def _allocation_sql(code_kind: str, batch_count: int) -> str:
    if batch_count < 1:
        raise ValueError("batch_count deve ser >= 1")
    if code_kind == "COD_IF":
        expression = "CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21(49, TO_DATE(?, 'YYYY-MM-DD'))"
    elif code_kind == "COD_OPERACAO":
        expression = "CETIP.GET_COD_OPERACAO"
    else:
        raise ValueError(f"tipo de código desconhecido: {code_kind}")
    return (f"SELECT LEVEL ordinal, {expression} code FROM dual "
            f"CONNECT BY LEVEL <= {int(batch_count)}")


def _iter_oracle_code_batches(jvm, jdbc_url: str, user: str, password: str, *,
                              code_kind: str, total: int, batch_size: int,
                              engorda_date: date):
    """Entrega um lote por round-trip; não retém códigos de lotes anteriores."""
    if total < 0 or batch_size < 1:
        raise ValueError("total deve ser >= 0 e batch_size deve ser >= 1")
    connection = None
    try:
        connection = _open_oracle_connection(jvm, jdbc_url, user, password)
        offset = 0
        while offset < total:
            expected = min(batch_size, total - offset)
            statement = None
            result_set = None
            batch: List[Tuple[int, str]] = []
            try:
                statement = connection.prepareStatement(_allocation_sql(code_kind, expected))
                if code_kind == "COD_IF":
                    statement.setString(1, engorda_date.isoformat())
                result_set = statement.executeQuery()
                while result_set.next():
                    local_ordinal = int(result_set.getInt(1))
                    raw_code = result_set.getString(2)
                    code = "" if raw_code is None else str(raw_code).strip()
                    batch.append((offset + local_ordinal, code))
            finally:
                if result_set is not None:
                    result_set.close()
                if statement is not None:
                    statement.close()
            if len(batch) != expected:
                raise ValueError(
                    f"{code_kind}: Oracle retornou {len(batch)} código(s), esperado {expected}")
            local_ordinals = [ordinal - offset for ordinal, _ in batch]
            if local_ordinals != list(range(1, expected + 1)):
                raise ValueError(f"{code_kind}: ordinais Oracle incompletos/fora de ordem")
            codes = [code for _, code in batch]
            pattern = COD_IF_PATTERN if code_kind == "COD_IF" else COD_OPERACAO_PATTERN
            invalid = [code for code in codes if not re.fullmatch(pattern, code)]
            if invalid:
                raise ValueError(f"{code_kind}: Oracle retornou código vazio/malformado")
            if len(set(codes)) != expected:
                raise ValueError(f"{code_kind}: Oracle retornou código duplicado no lote")
            yield batch
            offset += expected
    finally:
        if connection is not None:
            connection.close()


def _with_distributed_ordinal(df: DataFrame, order_cols: Sequence[str],
                              ordinal_col: str = "ORDINAL") -> DataFrame:
    """Ordenação distribuída estável + índice contíguo, sem Window global."""
    if ordinal_col in df.columns:
        raise ValueError(f"colisão de coluna ordinal: {ordinal_col}")
    positions = [df.columns.index(column) for column in order_cols]
    partitions = max(2, df.rdd.getNumPartitions())
    ordered = df.rdd.sortBy(
        lambda row: tuple(row[position] for position in positions),
        ascending=True,
        numPartitions=partitions,
    )
    indexed = ordered.zipWithIndex().map(
        lambda pair: tuple(pair[0]) + (int(pair[1]) + 1,))
    schema = T.StructType(list(df.schema.fields) + [
        T.StructField(ordinal_col, T.LongType(), False)])
    return df.sparkSession.createDataFrame(indexed, schema)


def _code_slots(df: DataFrame, pk_col: str, old_code_col: str,
                new_pk_alias: str, old_code_alias: str) -> DataFrame:
    if pk_col not in df.columns or old_code_col not in df.columns:
        raise ValueError(f"colunas obrigatórias ausentes: {pk_col}, {old_code_col}")
    indexed = _with_distributed_ordinal(df.select(pk_col, old_code_col), [pk_col])
    return indexed.select(
        "ORDINAL", F.col(pk_col).alias(new_pk_alias),
        F.col(old_code_col).alias(old_code_alias))


def _dry_placeholder(kind: str):
    if kind == "COD_IF":
        return F.concat(F.lit("CDB100"),
                        F.upper(F.lpad(F.conv(F.col("ORDINAL"), 10, 36), 5, "0")))
    return F.lpad(F.col("ORDINAL").cast("string"), 16, "0")


def _join_code_chunks(slots: DataFrame, code_chunks: DataFrame,
                      generated_alias: str) -> DataFrame:
    return slots.join(code_chunks.select("ORDINAL", generated_alias), on="ORDINAL", how="inner")


def _materialize_code_map(spark: SparkSession, slots: DataFrame, *, code_kind: str,
                          generated_alias: str, out_path: Optional[str], dry_run: bool,
                          credentials: Optional[Tuple[str, str, str]], batch_size: int,
                          engorda_date: date) -> DataFrame:
    """Anexa códigos por ordinal, mantendo no driver somente o lote corrente."""
    total = slots.count()
    if dry_run:
        return slots.withColumn(generated_alias, _dry_placeholder(code_kind))
    if out_path is None or credentials is None:
        raise ValueError("destino e credenciais são obrigatórios fora do dry-run")
    jdbc_url, user, password = credentials
    if total == 0:
        empty = slots.withColumn(generated_alias, F.lit(None).cast("string"))
        empty.write.mode("overwrite").parquet(out_path)
        return spark.read.parquet(out_path)

    chunk_path = f"{out_path}.__code_chunks"
    for batch in _iter_oracle_code_batches(
            spark.sparkContext._jvm, jdbc_url, user, password,
            code_kind=code_kind, total=total, batch_size=batch_size,
            engorda_date=engorda_date):
        schema = T.StructType([
            T.StructField("ORDINAL", T.LongType(), False),
            T.StructField(generated_alias, T.StringType(), False)])
        spark.createDataFrame(batch, schema).write.mode("append").parquet(chunk_path)
    code_chunks = spark.read.parquet(chunk_path)
    mapping = _join_code_chunks(slots, code_chunks, generated_alias)
    mapping.write.mode("overwrite").parquet(out_path)
    _delete_path(spark, chunk_path)
    mapping = spark.read.parquet(out_path)
    pk_column = slots.columns[1]
    pattern = COD_IF_PATTERN if code_kind == "COD_IF" else COD_OPERACAO_PATTERN
    summary = mapping.agg(
        F.count(F.lit(1)).alias("total"),
        F.countDistinct("ORDINAL").alias("ordinals"),
        F.countDistinct(pk_column).alias("pks"),
        F.countDistinct(generated_alias).alias("codes"),
        F.count(F.when(~F.col(generated_alias).rlike(pattern), 1)).alias("invalid"),
    ).first()
    if any(int(summary[name]) != total
           for name in ("total", "ordinals", "pks", "codes")) or int(summary["invalid"]):
        raise ValueError(f"{code_kind}: mapa Parquet incompleto, duplicado ou malformado")
    return mapping


def _attach_generated_code(df: DataFrame, mapping: DataFrame, *, pk_col: str,
                           new_pk_alias: str, code_col: str,
                           generated_alias: str) -> DataFrame:
    right = mapping.select(new_pk_alias, generated_alias).alias("m")
    left = df.alias("d")
    joined = left.join(right, F.col(f"d.{pk_col}") == F.col(f"m.{new_pk_alias}"), "left")
    return joined.select(*[
        F.col(f"m.{generated_alias}").cast(df.schema[c].dataType).alias(c)
        if c == code_col else F.col(f"d.{c}").alias(c)
        for c in df.columns
    ])


def _generate_meu_numeros(operacoes: DataFrame, prefix: str,
                          engorda_date: date) -> DataFrame:
    _validate_meu_numero_prefix(prefix)
    required = {
        "NUM_ID_OPERACAO", "DAT_OPERACAO", "NUM_CONTA_PARTICIPANTE_P1",
        "NUM_CONTA_PARTICIPANTE_P2", "NUM_CONTROLE_LANCAMENTO_P1",
        "NUM_CONTROLE_LANCAMENTO_P2", "NUM_ID_TIPO_OPER_OBJETO_SERV",
    }
    missing = sorted(required - set(operacoes.columns))
    if missing:
        raise ValueError(f"OPERACAO sem coluna(s) para meu-número: {missing}")
    norm_p1 = _norm_key_col(F.col("NUM_CONTA_PARTICIPANTE_P1"))
    norm_p2 = _norm_key_col(F.col("NUM_CONTA_PARTICIPANTE_P2"))
    staged = operacoes.withColumn("__meu_same_account", norm_p1.eqNullSafe(norm_p2))
    allocations = (staged.select(
        "NUM_ID_OPERACAO", F.lit(1).cast("int").alias("__meu_side"))
        .unionByName(staged.where("__meu_same_account").select(
            "NUM_ID_OPERACAO", F.lit(2).cast("int").alias("__meu_side"))))
    allocated = allocations.count()
    _validate_meu_capacity(allocated)
    allocation_map = _with_distributed_ordinal(
        allocations, ["NUM_ID_OPERACAO", "__meu_side"], "__meu_ord"
    ).localCheckpoint(eager=True)
    p1_map = allocation_map.where(F.col("__meu_side") == 1).select(
        "NUM_ID_OPERACAO", F.col("__meu_ord").alias("__meu_p1_ord"))
    p2_map = allocation_map.where(F.col("__meu_side") == 2).select(
        "NUM_ID_OPERACAO", F.col("__meu_ord").alias("__meu_p2_allocated_ord"))
    staged = (staged.join(p1_map, on="NUM_ID_OPERACAO", how="inner")
              .join(p2_map, on="NUM_ID_OPERACAO", how="left")
              .withColumn(
                  "__meu_p2_ord",
                  F.coalesce(F.col("__meu_p2_allocated_ord"), F.col("__meu_p1_ord"))))

    def _control(ordinal_col: str):
        return F.concat(F.lit(prefix), F.lpad(F.col(ordinal_col).cast("string"), 7, "0"))

    dat_type = operacoes.schema["DAT_OPERACAO"].dataType
    return (staged
            .withColumn("DAT_OPERACAO", _date_literal_for_type(engorda_date, dat_type))
            .withColumn("NUM_CONTROLE_LANCAMENTO_P1", _control("__meu_p1_ord").cast(
                operacoes.schema["NUM_CONTROLE_LANCAMENTO_P1"].dataType))
            .withColumn("NUM_CONTROLE_LANCAMENTO_P2", _control("__meu_p2_ord").cast(
                operacoes.schema["NUM_CONTROLE_LANCAMENTO_P2"].dataType))
            .drop("__meu_p1_ord", "__meu_p2_ord", "__meu_p2_allocated_ord",
                  "__meu_same_account"))


def _flatten_meu_tuples(operacoes: DataFrame) -> DataFrame:
    pieces = []
    for side in ("P1", "P2"):
        pieces.append(operacoes.select(
            F.date_trunc("second", F.col("DAT_OPERACAO").cast("timestamp")).alias(
                "DAT_OPERACAO"),
            _norm_key_col(F.col(f"NUM_CONTA_PARTICIPANTE_{side}")).alias(
                "NUM_CONTA_PARTICIPANTE"),
            F.col(f"NUM_CONTROLE_LANCAMENTO_{side}").cast("string").alias(
                "NUM_CONTROLE_LANCAMENTO"),
            _norm_key_col(F.col("NUM_ID_TIPO_OPER_OBJETO_SERV")).alias(
                "NUM_ID_TIPO_OPER_OBJETO_SERV"),
        ))
    return pieces[0].unionByName(pieces[1])


def _validated_root_cod_if_map(instrumentos: DataFrame) -> DataFrame:
    required = {COL_NUM_IF, "COD_IF", "DAT_EXCLUSAO"}
    missing = sorted(required - set(instrumentos.columns))
    if missing:
        raise ValueError(f"INSTRUMENTO_FINANCEIRO sem coluna(s) para COD_IF: {missing}")

    roots = instrumentos.where(F.col("DAT_EXCLUSAO").isNull()).select(
        _norm_key_col(F.col(COL_NUM_IF)).alias("__num_if"),
        F.col("COD_IF").alias("__root_cod_if"),
    )
    invalid = roots.where(
        F.col("__num_if").isNull()
        | (F.col("__num_if") == "")
        | F.col("__root_cod_if").isNull()
        | (F.trim(F.col("__root_cod_if").cast("string")) == "")
    ).count()
    if invalid:
        raise ValueError(
            "INSTRUMENTO_FINANCEIRO: mapeamento NUM_IF -> COD_IF ausente/vazio"
        )
    duplicates = (roots.groupBy("__num_if").count()
                  .where(F.col("count") != 1).limit(1).count())
    if duplicates:
        raise ValueError("INSTRUMENTO_FINANCEIRO: mapeamento NUM_IF -> COD_IF duplicado")
    return roots


def _propagate_root_cod_if(instrumentos: DataFrame, operacoes: DataFrame) -> DataFrame:
    required = {COL_NUM_IF, "COD_IF"}
    missing = sorted(required - set(operacoes.columns))
    if missing:
        raise ValueError(f"OPERACAO sem coluna(s) para COD_IF: {missing}")
    if not isinstance(operacoes.schema["COD_IF"].dataType, T.StringType):
        raise ValueError("OPERACAO.COD_IF precisa ter tipo textual StringType")

    roots = _validated_root_cod_if_map(instrumentos).withColumn(
        "__root_found", F.lit(True)
    )
    joined = operacoes.withColumn(
        "__num_if", _norm_key_col(F.col(COL_NUM_IF))
    ).join(roots, "__num_if", "left")
    unmatched = joined.where(
        F.col("__num_if").isNull()
        | (F.col("__num_if") == "")
        | F.col("__root_found").isNull()
    ).limit(1).count()
    if unmatched:
        raise ValueError("OPERACAO: NUM_IF sem mapeamento ativo de COD_IF na raiz")

    propagated = joined.select(*[
        F.col("__root_cod_if").cast("string").alias(column)
        if column == "COD_IF" else F.col(column)
        for column in operacoes.columns
    ])
    compared = propagated.withColumn(
        "__num_if", _norm_key_col(F.col(COL_NUM_IF))
    ).join(roots.select("__num_if", "__root_cod_if"), "__num_if", "left")
    invalid = compared.where(
        F.col("COD_IF").isNull()
        | F.col("__root_cod_if").isNull()
        | (F.trim(F.col("COD_IF")) != F.trim(F.col("__root_cod_if").cast("string")))
    ).limit(1).count()
    if invalid:
        raise ValueError("OPERACAO.COD_IF não preservou exatamente o código textual da raiz")
    return propagated


def _validate_business_keys(instrumentos: DataFrame, operacoes: DataFrame) -> None:
    try:
        roots = _validated_root_cod_if_map(instrumentos)
        required = {COL_NUM_IF, "COD_IF"}
        missing = sorted(required - set(operacoes.columns))
        if missing:
            raise ValueError(f"OPERACAO sem coluna(s) para COD_IF: {missing}")
        if not isinstance(operacoes.schema["COD_IF"].dataType, T.StringType):
            raise ValueError("OPERACAO.COD_IF precisa ter tipo textual StringType")
    except ValueError as exc:
        raise ValueError(f"Validação final de chaves de negócio FALHOU: {exc}") from exc

    checks = (
        (instrumentos, "COD_IF", COD_IF_PATTERN),
        (operacoes, "COD_OPERACAO", COD_OPERACAO_PATTERN),
    )
    errors = []
    for df, column, pattern in checks:
        total = df.count()
        invalid = df.where(
            F.col(column).isNull() | ~F.trim(F.col(column)).rlike(pattern)).count()
        distinct = df.select(F.trim(F.col(column))).dropDuplicates().count()
        if invalid:
            errors.append(f"{column}: {invalid} valor(es) vazio(s)/malformado(s)")
        if distinct != total:
            errors.append(f"{column}: {total - distinct} duplicata(s)")
    tuples = _flatten_meu_tuples(operacoes)
    total_tuples = tuples.count()
    incomplete = tuples.where(
        F.col("DAT_OPERACAO").isNull()
        | F.col("NUM_CONTA_PARTICIPANTE").isNull()
        | (F.col("NUM_CONTA_PARTICIPANTE") == "")
        | F.col("NUM_CONTROLE_LANCAMENTO").isNull()
        | ~F.col("NUM_CONTROLE_LANCAMENTO").rlike(r"^[1-9][0-9]{9}$")
        | F.col("NUM_ID_TIPO_OPER_OBJETO_SERV").isNull()
        | (F.col("NUM_ID_TIPO_OPER_OBJETO_SERV") == "")
    ).count()
    distinct_tuples = tuples.dropDuplicates().count()
    if incomplete:
        errors.append(f"meu-número: {incomplete} tupla(s) incompleta(s)/malformada(s)")
    if distinct_tuples != total_tuples:
        errors.append(f"meu-número: {total_tuples - distinct_tuples} colisão(ões) interna(s)")
    compared = operacoes.withColumn(
        "__num_if", _norm_key_col(F.col(COL_NUM_IF))
    ).join(roots, "__num_if", "left")
    mismatches = compared.where(
        F.col("__root_cod_if").isNull()
        | F.col("COD_IF").isNull()
        | (F.trim(F.col("COD_IF")) != F.trim(F.col("__root_cod_if").cast("string")))
    ).count()
    if mismatches:
        errors.append(
            f"OPERACAO.COD_IF: {mismatches} valor(es) divergente(s) da raiz por NUM_IF"
        )
    if errors:
        raise ValueError("Validação final de chaves de negócio FALHOU: " + "; ".join(errors))


def _assert_no_meu_collisions(operacoes: DataFrame, existing: DataFrame) -> None:
    generated = _flatten_meu_tuples(operacoes).dropDuplicates()
    normalized_existing = existing.select(
        F.date_trunc("second", F.col("DAT_OPERACAO").cast("timestamp")).alias(
            "DAT_OPERACAO"),
        _norm_key_col(F.col("NUM_CONTA_PARTICIPANTE")).alias(
            "NUM_CONTA_PARTICIPANTE"),
        F.col("NUM_CONTROLE_LANCAMENTO").cast("string").alias(
            "NUM_CONTROLE_LANCAMENTO"),
        _norm_key_col(F.col("NUM_ID_TIPO_OPER_OBJETO_SERV")).alias(
            "NUM_ID_TIPO_OPER_OBJETO_SERV"),
    ).dropDuplicates()
    if generated.join(normalized_existing, generated.columns, "left_semi").limit(1).count():
        raise ValueError("preflight Oracle: colisão de tupla meu-número no destino")


def _read_existing_meu_tuples(spark: SparkSession, credentials: Tuple[str, str, str], *,
                              engorda_date: date, prefix: str, temp_path: str,
                              chunk_size: int = 50_000) -> DataFrame:
    jdbc_url, user, password = credentials
    sql = """
        SELECT DAT_OPERACAO, NUM_CONTA_PARTICIPANTE, NUM_CONTROLE_LANCAMENTO,
               NUM_ID_TIPO_OPER_OBJETO_SERV
        FROM (
            SELECT DAT_OPERACAO,
                   NUM_CONTA_PARTICIPANTE_P1 NUM_CONTA_PARTICIPANTE,
                   NUM_CONTROLE_LANCAMENTO_P1 NUM_CONTROLE_LANCAMENTO,
                   NUM_ID_TIPO_OPER_OBJETO_SERV
            FROM CETIP.OPERACAO
            WHERE DAT_OPERACAO >= TO_DATE(?, 'YYYY-MM-DD')
              AND DAT_OPERACAO < TO_DATE(?, 'YYYY-MM-DD') + 1
              AND NUM_CONTROLE_LANCAMENTO_P1 LIKE ?
            UNION ALL
            SELECT DAT_OPERACAO,
                   NUM_CONTA_PARTICIPANTE_P2 NUM_CONTA_PARTICIPANTE,
                   NUM_CONTROLE_LANCAMENTO_P2 NUM_CONTROLE_LANCAMENTO,
                   NUM_ID_TIPO_OPER_OBJETO_SERV
            FROM CETIP.OPERACAO
            WHERE DAT_OPERACAO >= TO_DATE(?, 'YYYY-MM-DD')
              AND DAT_OPERACAO < TO_DATE(?, 'YYYY-MM-DD') + 1
              AND NUM_CONTROLE_LANCAMENTO_P2 LIKE ?
        )
    """
    schema = T.StructType([
        T.StructField("DAT_OPERACAO", T.StringType(), False),
        T.StructField("NUM_CONTA_PARTICIPANTE", T.StringType(), False),
        T.StructField("NUM_CONTROLE_LANCAMENTO", T.StringType(), False),
        T.StructField("NUM_ID_TIPO_OPER_OBJETO_SERV", T.StringType(), False),
    ])
    connection = statement = result_set = None
    wrote = False
    try:
        connection = _open_oracle_connection(spark.sparkContext._jvm, jdbc_url, user, password)
        statement = connection.prepareStatement(sql)
        day = engorda_date.isoformat()
        for position, value in enumerate((day, day, f"{prefix}%", day, day, f"{prefix}%"), 1):
            statement.setString(position, value)
        statement.setFetchSize(chunk_size)
        result_set = statement.executeQuery()
        chunk: List[Tuple[str, str, str, str]] = []
        while result_set.next():
            timestamp = result_set.getTimestamp(1)
            timestamp_text = str(timestamp.toString()) if hasattr(timestamp, "toString") \
                else str(timestamp)
            values = (timestamp_text,) + tuple(result_set.getString(i) for i in range(2, 5))
            chunk.append(tuple(str(value) for value in values))
            if len(chunk) >= chunk_size:
                spark.createDataFrame(chunk, schema).write.mode("append").parquet(temp_path)
                wrote = True
                chunk = []
        if chunk:
            spark.createDataFrame(chunk, schema).write.mode("append").parquet(temp_path)
            wrote = True
        return spark.read.parquet(temp_path) if wrote else spark.createDataFrame([], schema)
    finally:
        if result_set is not None:
            result_set.close()
        if statement is not None:
            statement.close()
        if connection is not None:
            connection.close()


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
    if fs.exists(hpath) and not fs.delete(hpath, True):
        raise ValueError(f"não foi possível apagar caminho de trabalho: {path}")


def _promote_staging_paths(fs, staging, final, backup) -> None:
    """Promove staging com rollback explícito, sem alegar atomicidade.

    Object stores podem implementar rename como copy+delete. Existe uma janela
    de crash entre `final -> backup` e `staging -> final`; nesse caso, o operador
    deve restaurar manualmente o caminho `.__previous_*` informado no log.
    """
    staging_text, final_text, backup_text = map(str, (staging, final, backup))
    if not fs.exists(staging):
        raise ValueError(f"staging ausente antes da publicação: {staging_text}")
    had_previous = fs.exists(final)
    if had_previous:
        logger.warning(
            "Publicação object-store não atômica: se o processo cair entre renames, "
            "restaure manualmente %s para %s.", backup_text, final_text)
        try:
            preserved = bool(fs.rename(final, backup))
            backup_exists = bool(fs.exists(backup))
        except Exception:
            preserved = backup_exists = False
        if not preserved or not backup_exists:
            raise RuntimeError(
                "CRÍTICO: não foi possível verificar a preservação do destino anterior; "
                f"caminho de backup esperado: {backup_text}")

    try:
        promoted = bool(fs.rename(staging, final))
        final_exists = bool(fs.exists(final))
    except Exception:
        promoted = final_exists = False
    if not promoted or not final_exists:
        if not had_previous:
            raise ValueError("falha ao promover staging; não havia destino anterior")
        try:
            partial_exists = bool(fs.exists(final))
        except Exception:
            raise RuntimeError(
                "CRÍTICO: promoção falhou e o destino parcial não pôde ser verificado; "
                f"backup para recuperação manual: {backup_text}") from None
        if partial_exists:
            try:
                partial_deleted = bool(fs.delete(final, True))
            except Exception:
                partial_deleted = False
            if not partial_deleted:
                raise RuntimeError(
                    "CRÍTICO: promoção falhou e o destino parcial não pôde ser removido; "
                    f"backup para recuperação manual: {backup_text}")
        try:
            restored = bool(fs.rename(backup, final))
            restored_exists = bool(fs.exists(final))
        except Exception:
            restored = restored_exists = False
        if not restored or not restored_exists:
            raise RuntimeError(
                "CRÍTICO: promoção e restauração do destino anterior falharam; "
                f"recupere manualmente o backup {backup_text} para {final_text}")
        raise ValueError(
            "falha ao promover staging; destino anterior restaurado e verificado")

    if had_previous:
        try:
            deleted = bool(fs.delete(backup, True))
            backup_remains = bool(fs.exists(backup))
        except Exception:
            deleted, backup_remains = False, True
        if not deleted or backup_remains:
            logger.warning(
                "Destino novo publicado e verificado, mas a limpeza do backup falhou; "
                "remova manualmente %s. O destino final não foi alterado.", backup_text)


def _publish_staging(spark: SparkSession, staging_path: str, final_path: str) -> None:
    jvm = spark.sparkContext._jvm
    jsc = spark.sparkContext._jsc
    staging = jvm.org.apache.hadoop.fs.Path(staging_path)
    final = jvm.org.apache.hadoop.fs.Path(final_path)
    backup = jvm.org.apache.hadoop.fs.Path(f"{final_path}.__previous_{uuid.uuid4().hex}")
    fs = final.getFileSystem(jsc.hadoopConfiguration())
    _promote_staging_paths(fs, staging, final, backup)


def _stage_and_publish(spark: SparkSession, final_path: str,
                       prepare: Callable[[str], None]) -> None:
    staging_path = f"{final_path}.__staging_{uuid.uuid4().hex}"
    _delete_path(spark, staging_path)
    try:
        prepare(staging_path)
        _publish_staging(spark, staging_path, final_path)
    except Exception:
        logger.error(
            "Publicação abortada; verifique o erro e qualquer backup .__previous_* "
            "antes de consumir o destino fixo.")
        raise


def escreve_tabela(spark: SparkSession, df: DataFrame, out_path: str) -> None:
    _delete_path(spark, out_path)
    df.write.mode("append").parquet(out_path)
    expected = df.count()
    actual = spark.read.parquet(out_path).count()
    if actual != expected:
        raise ValueError(
            f"readback Parquet falhou em {out_path}: {actual} linha(s), esperado {expected}")


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
    """O prefixo de clones é EXCLUSIVO e substituído por staging completo.

    A saída anterior permanece publicada durante alocação, preflight, escrita e
    readback; só depois o staging validado é promovido como uma árvore completa.
    Por isso o destino NÃO pode ser vazio, nem conter/estar contido na área
    raw, nem conter a área de saída do engorda."""
    prefix = (config.get("DATAGEN_CLONE_PREFIX") or "").strip("/")
    if not prefix:
        raise ValueError(
            "DATAGEN_CLONE_PREFIX vazio: o destino seria a RAIZ de "
            "DATAGEN_SYNTHETIC_BASE_URI, que seria substituída por inteiro. "
            f"Defina um prefixo dedicado (default: {DEFAULT_CLONE_PREFIX}).")

    save_base = clone_base_path(config)
    raw_area = _area(config["DATAGEN_RAW_BASE_URI"],
                     config.get("DATAGEN_RAW_PREFIX"))
    engorda_area = _area(config["DATAGEN_SYNTHETIC_BASE_URI"],
                         config.get("DATAGEN_SYNTHETIC_PREFIX"))

    # Área raw: nem publicar por cima (save_base ancestral) nem escrever dentro
    # (misturar clones com snapshots que o engorda lê por nome de tabela).
    if _mesmo_ou_ancestral(save_base, raw_area) or _mesmo_ou_ancestral(raw_area, save_base):
        raise ValueError(
            f"Destino dos clones ({save_base}) sobrepõe a área raw "
            f"({raw_area}). Ajuste DATAGEN_CLONE_PREFIX/DATAGEN_RAW_PREFIX "
            "para áreas disjuntas.")
    # Área do engorda: só é problema se substituir save_base LEVAR JUNTO a área
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
                     meu_numero_prefix: str,
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
                     faltantes_arg: Optional[str] = None,
                     faltantes_parquet: Optional[str] = None,
                     poda_subtipo: bool = True,
                     anular_cols: Optional[Mapping[str, Sequence[str]]] = None,
                     oracle_code_batch_size: int = DEFAULT_ORACLE_CODE_BATCH_SIZE,
                     dry_run: bool = False) -> Dict[str, dict]:
    """Roda a clonagem fim a fim; devolve {tabela: estatísticas} (para uso em
    notebook). Aborta sem gravar NADA se qualquer validação falhar.

    Poda de domínio ANTES da amostragem (saída carregável por construção):
      * poda_subtipo (item 1): tira do domínio os NUM_IF que gerariam CONDICAO_IF
        dangling; a amostragem repõe até fechar N;
      * faltantes_arg/parquet (itens 3/4): tira os NUM_IF que referenciam chaves
        inexistentes no destino (QAB), sem conexão Oracle.
    Anulação de colunas de drift (item 2): anular_cols (default
    NULIFICA_COLS_POR_TABELA) seta NULL nas colunas nullable listadas por tabela."""
    inicio = time.perf_counter()
    _validate_meu_numero_prefix(meu_numero_prefix)
    if oracle_code_batch_size < 1:
        raise ValueError("--oracle-code-batch-size deve ser >= 1")
    credentials = None if dry_run else _oracle_credentials(config)
    anular_cols = NULIFICA_COLS_POR_TABELA if anular_cols is None else anular_cols
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

    faltantes = _carrega_faltantes(spark, config, faltantes_arg, faltantes_parquet)
    if faltantes is not None:
        logger.info("Filtro de faltantes (itens 3/4) ativo: %d chave(s) de "
                    "referência inexistentes no destino.", faltantes.count())
    if not poda_subtipo:
        logger.warning("Poda de subtipo (item 1) DESLIGADA (--sem-poda-subtipo): "
                       "clones podem ter CONDICAO_IF dangling (Cat 1).")
    valores = seleciona_instrumentos(spark, config, spec, num_ifs, n_instrumentos,
                                     seed, faltantes=faltantes,
                                     poda_subtipo=poda_subtipo)
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
            logger.info("[%s] lote vazio — materializando clone e mapa vazios.", t)
        clones, mapa_pk = clona_tabela(spark, plano, lotes[t], fator_k, mapeamentos)
        mapeamentos[t] = mapa_pk
        # Regras de data ANTES do checkpoint/validação: o NOT NULL precisa ser
        # conferido no valor que vai ser gravado, não no valor clonado.
        clones, cols_data = aplica_regras_engorda(
            clones, t, engorda_ts=engorda_ts,
            prazo_vencimento_dias=prazo_vencimento_dias)
        # Anulação de colunas de drift (item 2) ANTES do checkpoint/validação:
        # o NOT NULL e a gravação precisam enxergar o valor já nulo.
        clones, cols_anuladas = aplica_nulificacao(
            clones, t, anular_cols, _not_null_cols(spec[t]))
        clones = clones.localCheckpoint(eager=True)  # congela p/ validar e gravar

        cols_remap = sorted({*plano.pk_cols,
                             *(c for fk in plano.fks_remap for c in fk.columns)}
                            & set(clones.columns))
        erros = valida_tabela(spec[t], plano, clones, n_lote, fator_k)
        stats[t] = {"lote": n_lote, "clones": n_lote * fator_k,
                    "colunas_remapeadas": cols_remap,
                    "colunas_data": cols_data, "colunas_anuladas": cols_anuladas,
                    "erros": erros}
        logger.info("[%s] lote=%d clones=%d remapeadas=%s datas=%s anuladas=%s %s",
                    t, n_lote, n_lote * fator_k, cols_remap, cols_data or "-",
                    cols_anuladas or "-",
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
    if TABELA_RAIZ not in resultados or "OPERACAO" not in resultados:
        raise ValueError("plano precisa produzir INSTRUMENTO_FINANCEIRO e OPERACAO")

    def _prepare_business_outputs(output_base: Optional[str], is_dry_run: bool) -> None:
        instrumentos, n_raiz = resultados[TABELA_RAIZ]
        operacoes, n_operacoes = resultados["OPERACAO"]
        slots_if = _code_slots(
            instrumentos, COL_NUM_IF, "COD_IF", "NUM_IF_NOVO", "COD_IF_ORIG"
        ).localCheckpoint(eager=True)
        mapa_cod_if = _materialize_code_map(
            spark, slots_if, code_kind="COD_IF", generated_alias="COD_IF_GERADO",
            out_path=None if is_dry_run else f"{output_base}/{MAPA_COD_IF_TABLE}",
            dry_run=is_dry_run, credentials=credentials, batch_size=oracle_code_batch_size,
            engorda_date=engorda_ts.date())
        instrumentos = _attach_generated_code(
            instrumentos, mapa_cod_if, pk_col=COL_NUM_IF, new_pk_alias="NUM_IF_NOVO",
            code_col="COD_IF", generated_alias="COD_IF_GERADO").localCheckpoint(eager=True)
        resultados[TABELA_RAIZ] = (instrumentos, n_raiz)
        operacoes = _propagate_root_cod_if(instrumentos, operacoes)

        slots_operacao = _code_slots(
            operacoes, "NUM_ID_OPERACAO", "COD_OPERACAO",
            "NUM_ID_OPERACAO_NOVO", "COD_OPERACAO_ORIG").localCheckpoint(eager=True)
        mapa_cod_operacao = _materialize_code_map(
            spark, slots_operacao, code_kind="COD_OPERACAO",
            generated_alias="COD_OPERACAO_GERADO",
            out_path=None if is_dry_run else f"{output_base}/{MAPA_COD_OPERACAO_TABLE}",
            dry_run=is_dry_run, credentials=credentials, batch_size=oracle_code_batch_size,
            engorda_date=engorda_ts.date())
        operacoes = _attach_generated_code(
            operacoes, mapa_cod_operacao, pk_col="NUM_ID_OPERACAO",
            new_pk_alias="NUM_ID_OPERACAO_NOVO", code_col="COD_OPERACAO",
            generated_alias="COD_OPERACAO_GERADO")
        operacoes = _generate_meu_numeros(
            operacoes, meu_numero_prefix, engorda_ts.date()).localCheckpoint(eager=True)
        resultados["OPERACAO"] = (operacoes, n_operacoes)
        _validate_business_keys(instrumentos, operacoes)
        if is_dry_run:
            return

        preflight_path = f"{output_base}/__PREFLIGHT_MEU"
        try:
            existing = _read_existing_meu_tuples(
                spark, credentials, engorda_date=engorda_ts.date(),
                prefix=meu_numero_prefix, temp_path=preflight_path)
            _assert_no_meu_collisions(operacoes, existing)
        finally:
            _delete_path(spark, preflight_path)
        for t in ordem:
            clones, _ = resultados[t]
            out_path = f"{output_base}/{t}"
            logger.info("Gravando staging %s -> %s", t, out_path)
            escreve_tabela(spark, clones, out_path)
        mapa_if = (mapeamentos[TABELA_RAIZ]
                   .select(F.col(f"old_{COL_NUM_IF}").alias("NUM_IF_ORIG"),
                           F.col(K_COL).alias("K"),
                           F.col(f"new_{COL_NUM_IF}").alias("NUM_IF_NOVO")))
        escreve_tabela(spark, mapa_if, f"{output_base}/{MAPA_NUM_IF_TABLE}")

    if dry_run:
        _prepare_business_outputs(None, True)
        logger.info("--dry-run: validações OK; NADA gravado (destino seria %s).",
                    save_base)
    else:
        save_base = _valida_destino(config)
        _stage_and_publish(
            spark, save_base,
            lambda staging_base: _prepare_business_outputs(staging_base, False))
        logger.info("Staging validado e publicado em %s.", save_base)

    logger.info("=" * 78)
    logger.info("RESUMO DA CLONAGEM (%.1fs) — %d instrumento(s) × K=%d, "
                "data de engorda %s, %s",
                time.perf_counter() - inicio, len(valores), fator_k,
                engorda_ts.isoformat(sep=" "),
                "DRY-RUN (nada gravado)" if dry_run else f"gravado em {save_base}")
    for t in ordem:
        s = stats.get(t, {})
        logger.info("  %-32s lote=%-8s clones=%-8s remap=%s datas=%s anuladas=%s",
                    t, s.get("lote", "-"), s.get("clones", "-"),
                    ",".join(s.get("colunas_remapeadas", [])) or "-",
                    ",".join(s.get("colunas_data", [])) or "-",
                    ",".join(s.get("colunas_anuladas", [])) or "-")
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
    # Spark 3.5.0 (OCI Data Flow) + AQE + dados em cache PERDE LINHAS DE JOIN
    # silenciosamente (SPARK-45282, corrigido no 3.5.1). Manter AQE desligado
    # enquanto os apps rodarem < 3.5.1.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
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
                    "Dirigido pelo spec_config.json; Oracle aloca chaves de negócio.")
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--num-ifs", type=_parse_num_ifs, default=None,
                       help="Lista explícita de NUM_IF (ex.: 123,456). Aceita 1 só.")
    grupo.add_argument("--n-instrumentos", type=positive_int, default=None,
                       help="Sorteia N instrumentos do domínio do produto "
                            "(query de validação; com --seed).")
    parser.add_argument("--fator-k", type=positive_int, default=1,
                        help="Clones por instrumento (default 1).")
    parser.add_argument("--meu-numero-prefix", type=_validate_meu_numero_prefix,
                        required=True,
                        help="Prefixo obrigatório de 3 dígitos (primeiro 1-9) para "
                             "controles de lançamento.")
    parser.add_argument("--oracle-code-batch-size", type=positive_int,
                        default=DEFAULT_ORACLE_CODE_BATCH_SIZE,
                        help="Códigos Oracle por round-trip (default 50000).")
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
    parser.add_argument("--sem-poda-subtipo", action="store_true",
                        help="DESLIGA a poda de domínio do item 1 (por padrão os "
                             "NUM_IF que gerariam CONDICAO_IF dangling são tirados "
                             "do domínio e repostos por outra amostra). Use só p/ "
                             "depurar — o clone pode sair com dangling (Cat 1).")
    parser.add_argument("--faltantes-arg", default=None,
                        help="Itens 3/4: chaves de referência inexistentes no "
                             "destino (QAB), inline: "
                             "'TABELA.COLUNA=v1,v2;TAB2.COL2=v3'. Os NUM_IF que "
                             "as referenciam são podados do domínio. Ex.: "
                             "'CARTEIRA_COMITENTE.NUM_ID_ENTIDADE=343..;"
                             "CARTEIRA_COMITENTE.NUM_CONTA=95..'.")
    parser.add_argument("--faltantes-parquet", default=None,
                        help="Itens 3/4: Parquet com colunas TABELA/COLUNA/VALOR "
                             "das chaves inexistentes no destino (mesma poda do "
                             "--faltantes-arg, p/ listas grandes).")
    parser.add_argument("--anular-cols", default=None,
                        help="Item 2 (override/extra): colunas nullable a ANULAR "
                             "nos clones, formato 'TABELA.COL,COL2;TAB2.COL3'. "
                             f"Somam-se ao default {NULIFICA_COLS_POR_TABELA}.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Valida e loga; não grava nada.")
    parser.add_argument("--specs", default=None,
                        help="Override de DATAGEN_SPECS_URI (specs.json único).")
    return parser.parse_args()


def _merge_anular_cols(base: Mapping[str, Sequence[str]],
                       txt: Optional[str]) -> Dict[str, Tuple[str, ...]]:
    """Funde o default de anulação com o --anular-cols ('TAB.COL,COL2;TAB2.COL3'),
    preservando ordem e sem duplicar coluna por tabela."""
    merged: Dict[str, List[str]] = {t: list(cols) for t, cols in base.items()}
    for grupo in (txt or "").split(";"):
        grupo = grupo.strip()
        if not grupo:
            continue
        tab, sep, cols = grupo.partition(".")
        if not sep or not cols.strip():
            raise argparse.ArgumentTypeError(
                f"--anular-cols: entrada inválida {grupo!r} (use TABELA.COL,COL2;...).")
        tab = table_path_name(tab.strip().upper())
        alvo = merged.setdefault(tab, [])
        for c in cols.split(","):
            c = c.strip().upper()
            if c and c not in alvo:
                alvo.append(c)
    return {t: tuple(cols) for t, cols in merged.items()}


def main() -> None:
    args = parse_arguments()
    config = get_engorda_env()
    spark = create_spark_session("DataGenClonaInstrumentos")
    try:
        specs_uri = args.specs or config["DATAGEN_SPECS_URI"]
        spec = load_specs(spark, specs_uri)
        executa_clonagem(
            spark, config, spec,
            meu_numero_prefix=args.meu_numero_prefix,
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
            faltantes_arg=args.faltantes_arg,
            faltantes_parquet=args.faltantes_parquet,
            poda_subtipo=not args.sem_poda_subtipo,
            anular_cols=_merge_anular_cols(NULIFICA_COLS_POR_TABELA, args.anular_cols),
            oracle_code_batch_size=args.oracle_code_batch_size,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
