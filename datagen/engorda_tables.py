#!/usr/bin/env python3
"""
engorda_instrumentos.py — motor multi-produto de sintetização por entidade.

Em vez de sintetizar tabela a tabela (bootstrap do engorda_tables.py, que
perde o fan-out por instrumento e recombina colunas em combinações de negócio
inválidas), este job sintetiza INSTRUMENTOS INTEIROS: seleciona N valores de
NUM_IF do domínio definido pela query SQL do produto (ou por override), copia
todas as linhas do fecho
referencial que pertencem a esses instrumentos e reescreve APENAS chaves:

  1. NUM_IF (PK de INSTRUMENTO_FINANCEIRO) -> NUM_IF novo, acima do max real;
  2. PKs surrogate de cada tabela -> offset próprio por tabela, acima do max
     real daquela tabela (mesma lógica do compute_pk_maxes do engorda);
  3. PKs compartilhadas (shared-key, ex.: subtipos de CONDICAO_IF) -> seguem
     o mapeamento do pai;
  4. FKs para tabelas sintetizadas -> reescritas pelo mapeamento do pai.

Quando o perfil ativa a estratégia padrão, um conjunto FECHADO de colunas de
DATA recebe o timestamp do run ou a data do controle operacional (ver
ENGORDA_*_COLS). DAT_VENCIMENTO preserva o prazo original a partir da nova
DAT_EMISSAO; DAT_RESGATE do resgate e de seu cronograma acompanha o mesmo
deslocamento da emissão. TODAS as demais colunas ficam intocadas — é isso que
preserva as combinações de negócio e o polimorfismo Hibernate de CONDICAO_IF.

===========================================================================
MULTI-PRODUTO SEM EDITAR CÓDIGO
===========================================================================
Um produto novo NÃO exige mudança neste arquivo. O que o define é, por inteiro:

  * o catálogo de domínio -> queries_produtos.sql (bloco escolhido por --produto)
  * o spec do fecho       -> --specs (define QUAIS tabelas são engordadas:
                             o motor engorda exatamente as tabelas não-static
                             do spec)
  * o prefixo de saída    -> --clone-prefix (default:
                             sintetizacao_multiproduto/<produto>)
  * o rótulo do run       -> --produto (texto livre [a-z][a-z0-9_]*)

O TIPO DO INSTRUMENTO (NUM_TIPO_IF, ex.: 49=CDB, 50=RDB) NÃO É MAIS CONSTANTE
NO CÓDIGO nem parâmetro obrigatório: ele é DERIVADO das próprias linhas do lote
em INSTRUMENTO_FINANCEIRO, logo após a seleção dos NUM_IF e ANTES de qualquer
alocação no Oracle. Como o lote é o resultado do SQL do produto, o tipo usado
para alocar COD_IF é, por construção, o tipo dos instrumentos sintetizados —
o que torna IMPOSSÍVEL alocar código de um produto para instrumento de outro.

  * lote com mais de um NUM_TIPO_IF distinto -> ABORTA (o SQL do produto deve
    restringir o domínio a um tipo);
  * --tipo-oracle N (opcional) -> confere contra o derivado e ABORTA se
    divergir. Num lote legitimamente multi-tipo, é a saída manual explícita.

DIRIGIDO por REGRAS_SCHEMA_CETIP (perfil ÚNICO do schema, comum a todos os
produtos), query SQL e spec_config.json. Todo run gera COD_IF pelo alocador
oficial do Oracle; a geração de COD_OPERACAO, meu-número e o preflight de
OPERACAO são etapas da política de chaves de negócio do schema.

POLÍTICAS COMUNS (as demais pertencem explicitamente ao perfil do schema):

  * FK para tabela sintetizada: reescreve SE o registro referenciado está no lote
    de sintetização; senão MANTÉM o valor original (que continua existindo no
    banco -> FK válida). Antes da seleção final, todo valor preservado é verificado
    no Oracle receptor; o NUM_IF dono é rejeitado e reposto se o pai não existir.
  * Pertencimento ao lote ("de quem é esta linha?"): desce a árvore a partir
    de INSTRUMENTO_FINANCEIRO pelas FKs de VÍNCULO PRINCIPAL — aquelas cujas
    colunas na filha têm o MESMO NOME das colunas da PK do pai (convenção do
    schema CETIP: NUM_IF -> NUM_IF, NUM_CONDICAO_IF -> NUM_CONDICAO_IF). FKs
    com nome divergente (ex.: NUM_IF_ORIGEM) são LATERAIS: não puxam linhas
    para o lote, só são remapeadas-se-no-lote. Self-FKs nunca expandem o lote.
  * PK surrogate: coluna única, numérica e fora de qualquer FK -> offset
    próprio acima do max real (com --pk-safety-band). PK com componente de FK
    para pai sintetizado -> segue o pai. PK sem regra possível -> ABORTA listando
    as tabelas (use --tratar-como-static para excluí-las da sintetização).
  * COD_IF e COD_OPERACAO são alocados pelas funções oficiais do Oracle para
    TODO sintético (inclusive K=1). Controles P1/P2 são gerados localmente com
    prefixo obrigatório e preflight no destino.
  * Tabelas static do spec: não são sintetizadas nem escritas; FKs para elas
    mantêm o valor original depois de confirmar o pai no Oracle receptor.

VALIDAÇÕES PRÉ-ESCRITA (abortam o job, nada é gravado parcial por tabela):
  * count(sintéticos) == count(lote) * K por tabela;
  * PK nova: sem duplicata interna e (para offset) acima do max real do
    Parquet COMPLETO da tabela (todas as linhas de produção, sem filtro);
  * colunas NOT NULL do spec sem nulo efetivo (NULL ou '' string) nos sintéticos.

SAÍDA: Parquet por tabela em
    {DATAGEN_SYNTHETIC_BASE_URI}/{DATAGEN_CLONE_PREFIX}/{TABELA}
(mesmo layout de saída do engorda — o processo de carga existente lê e faz o
append no Oracle). Sempre grava MAPA_CLONE_NUM_IF, MAPA_CLONE_COD_IF e
MAPA_CLONE_COD_OPERACAO. Com --dry-run nada é gravado nem alocado no Oracle:
usa placeholders locais só para validação. Fora do dry-run, toda a árvore é
validada em staging irmão antes de substituir a saída anterior.
A publicação por rename NÃO é atômica em object storage: se o processo cair
entre os renames, restaure manualmente o backup `<destino>.__previous_*` para
o caminho fixo `<destino>` antes de consumir a saída.

USO RECOMENDADO:
    escolha os parâmetros em executar_engorda_multiproduto.py e execute:
    spark-submit --py-files engorda_instrumentos.py \
      --files queries_produtos.sql \
      executar_engorda_multiproduto.py

CLI DIRETA (OCI Data Flow — mesmas envs do engorda_tables.py):
    envs: DATAGEN_RAW_BASE_URI, DATAGEN_SPECS_URI, DATAGEN_SYNTHETIC_BASE_URI,
          DATAGEN_SOURCE_JDBC_URL, DATAGEN_SOURCE_DB_USER,
          DATAGEN_SOURCE_DB_PASSWORD (nomes legados: apontam para o Oracle
          receptor; as três últimas são dispensadas no --dry-run)
          (+ opcionais DATAGEN_RAW_PREFIX, DATAGEN_SYNTHETIC_PREFIX,
           DATAGEN_CLONE_PREFIX — default derivado do nome do produto)
    argumentos:
      --produto cdb_simplificado    # escolhe tabelas e query do produto
      --num-ifs 12345,67890         # lista explícita (aceita 1 só), OU
      --n-instrumentos 5 --seed 42  # amostra do domínio definido no SQL
      --query-num-if-sql queries_produtos.sql  # override opcional do catálogo
      --specs oci://.../spec_cdb.json  # define as tabelas engordadas
      --clone-prefix sintetizacao_multiproduto/cdb  # default: .../<produto>
      --fator-k 3                   # sintéticos por instrumento (default 1)
      --meu-numero-prefix 321       # obrigatório enquanto houver OPERACAO
      --tipo-oracle 49              # OPCIONAL: confere contra o derivado
      --cod-if-padrao '^CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}$'  # OPCIONAL: aperta
      --cod-if-dry-prefix CDB100    # OPCIONAL: prefixo do placeholder dry-run
      --oracle-code-batch-size 50000  # códigos por round-trip Oracle
      --dry-run                     # valida e loga, não grava
      --pk-safety-band 100000       # folga acima do max real (default 0)
      --pk-passo 10                 # folga ENTRE PKs novas consecutivas (default 1)
      --offset-num-if 900000000     # início explícito p/ NUM_IF novo (opcional)
      --data-engorda 2026-07-19     # data/hora do run (default: agora)
      --prazo-vencimento-dias 30    # DAT_VENCIMENTO = data + N (default: prazo original)
      --tratar-como-static TAB1,TAB2  # excluir tabela(s) da sintetização
      --sem-poda-subtipo            # DESLIGA a poda do item 1 (dangling CONDICAO_IF)
      --faltantes-arg 'CARTEIRA_COMITENTE.NUM_ID_ENTIDADE=343..;...'
                                    # hint offline para dry-run; execução real
                                    #   sempre usa o Oracle live
      --faltantes-parquet oci://.../faltantes  # idem, TABELA/COLUNA/VALOR
      --anular-cols 'TAB.COL,COL2;...'  # item 2 (extra): colunas nullable a anular

REGRAS DO SCHEMA:
  O dicionário REGRAS_SCHEMA_CETIP é a fonte única de configuração técnica do
  motor e é COMUM A TODOS OS PRODUTOS (antes era REGRAS_PRODUTO, com uma entrada
  por produto — as entradas eram deepcopy umas das outras, variando só em campos
  que hoje são derivados do dado ou parâmetros de CLI). Para desligar uma regra
  opcional, use o valor neutro indicado ao lado dela: ajuste_datas=None,
  subtipo=None, nulificar_colunas={}, faltantes_seletivos=() ou operacao=None.
  A geração segura de COD_IF é obrigatória e não pode ser desligada.

CORREÇÕES DE INTEGRIDADE (saída carregável por construção):
  1. CONDICAO_IF dangling (Cat 1): poda do domínio os NUM_IF cujo subtipo não
     existe na origem; a amostragem repõe até fechar N (--sem-poda-subtipo desliga).
  2. NUM_ID_TRANSF_ARQ_P1/P2 órfãos: anulados nos sintéticos de OPERACAO (nullable) —
     ver REGRAS_SCHEMA_CETIP / --anular-cols.
  3/4. Chaves inexistentes no destino: por padrão, poda do domínio os NUM_IF que
     as referenciam. A regra faltantes_seletivos preserva o instrumento e anula
     somente os valores listados nos sintéticos.

API: from engorda_instrumentos import EngordaJob, executar_job.

Comentários e logs em português; helpers copiados do engorda_tables.py estão
marcados como tal (arquivo único e autocontido, como o Data Flow espera).
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from functools import reduce
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from pyspark import SparkFiles
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


@contextmanager
def _perf_timer(phase: str, **fields: Any):
    started = time.perf_counter()
    try:
        yield
    finally:
        details = " ".join(f"{key}={value}" for key, value in fields.items())
        logger.info(
            "PERF %s seconds=%.3f%s",
            phase,
            time.perf_counter() - started,
            f" {details}" if details else "",
        )

# ---------------------------------------------------------------------------
# Domínio: a seleção de NUM_IF vive no catálogo SQL único, em um bloco por
# produto. A leitura das tabelas para montar o fecho é RAW; filtros posteriores
# neste módulo são de integridade.
# ---------------------------------------------------------------------------
TABELA_RAIZ = "INSTRUMENTO_FINANCEIRO"
COL_NUM_IF = "NUM_IF"
# Coluna do TIPO do instrumento na tabela raiz. É dela que sai o argumento do
# alocador oficial de COD_IF no Oracle — DERIVADO do lote, nunca constante no
# código (ver _deriva_tipo_oracle e o cabeçalho do módulo).
COL_NUM_TIPO_IF = "NUM_TIPO_IF"
# Quantos tipos distintos o diagnóstico de lote heterogêneo lista no erro.
MAX_TIPOS_DIAGNOSTICO = 10

PRODUTOS_COM_PODA_SUBTIPO = frozenset({
    'cdb_simplificado',
    'cdb_resgate',
    'cdb_escalonamento',
})

# ---------------------------------------------------------------------------
# Poda de domínio (itens 1, 3 e 4) — instrumentos que o sintético NÃO conseguiria
# deixar carregável são removidos do domínio ANTES da amostragem. A amostragem
# de N sorteia do domínio JÁ PODADO, então a contagem final continua N (cada
# instrumento podado é reposto por outra amostra válida — não sobra "buraco").
#
# Item 1 — polimorfismo CONDICAO_IF. COD_TIPO_CONDICAO_IF -> tabela-subtipo
# física (joined-subclass do Hibernate), conforme tabelas_por_tipo no
# REGRAS_SCHEMA_CETIP. Uma CONDICAO_IF ativa SEM a linha na sua
# tabela-subtipo fica "dangling": o Hibernate não consegue tipar a classe e o
# batch estoura ClassCastException (Cat 1 do validador). Como NUM_CONDICAO_IF é
# a PK de CONDICAO_IF (globalmente única), uma chave só pode viver na
# tabela-subtipo do seu próprio tipo — basta checar presença na UNIÃO das
# fontes-subtipo sintetizáveis. As fontes são RAW; a poda restringe explicitamente
# apenas o pai CONDICAO_IF às linhas ativas, pois essa é uma guarda de integridade
# do carregamento e não um filtro de negócio do fecho sintetizado.
#
# ATENÇÃO ao rodar produto novo: _subtipos_clonaveis só considera tabelas-subtipo
# PRESENTES e NÃO-STATIC no spec. Se o fecho do produto não trouxer, por exemplo,
# JUROS_FLUTUANTE, toda CONDICAO_IF daquele tipo vira dangling e os NUM_IF saem
# do domínio. O job avisa (log abaixo) e, se o domínio esvaziar, aborta em
# seleciona_instrumentos com a contagem — não há perda silenciosa.
# ---------------------------------------------------------------------------
CONDICAO_IF_TABLE = "CONDICAO_IF"
CONDICAO_IF_PK = "NUM_CONDICAO_IF"
CONDICAO_IF_TIPO_COL = "COD_TIPO_CONDICAO_IF"
# Item 2 — colunas nullable ANULADAS nos sintéticos por serem drift entre o snapshot
# de origem e o destino (QAB): NUM_ID_TRANSF_ARQ_P1/P2 de OPERACAO apontam para
# TRANSFERENCIA_ARQUIVO inexistentes no destino. Como são nullable (não estão em
# not_null_cols), anular remove o órfão de FK sem perder a operação — a maioria
# das operações já as tem nulas. Declarativo {TABELA: (col, ...)}; --anular-cols
# acrescenta entradas em tempo de execução.

# Exceção exata à poda por faltantes: metadados Oracle/QAB confirmam que esta
# FK filha é nullable. O contrato é revalidado contra o spec em todo startup;
# a exceção fica em faltantes_seletivos no dicionário do schema.
FALTANTES_SELECTIVE_KEY_COL = "__faltante_seletiva_key"
FALTANTES_SELECTIVE_MARKER_COL = "__faltante_seletiva_match"

# ---------------------------------------------------------------------------
# Regras de engorda por coluna.
#
# Timestamp de engorda = instante em que este script começa a executar. O mesmo
# valor é reutilizado em todas as tabelas. Datas de negócio marcadas abaixo usam
# CETIP.CONTROLE_OPERACIONAL.DAT_CTL_OPER (NUM_ORDEM=0, NUM_SISTEMA IS NULL).
#
# Regras aplicadas quando a coluna existir na tabela:
#   auditoria                 -> data/hora da engorda (timestamp)
#   datas operacionais        -> DAT_CTL_OPER, sem hora
#   DAT_VENCIMENTO            -> DAT_CTL_OPER + prazo, sem hora
#   datas derivadas do EVENTO -> DAT_LIQUIDACAO original
#
# As PKs NÃO entram aqui (nem NUM_ID_CERTIFICACAO_CETIP): quem as reescreve é o
# plano de sintetização (monta_plano + _monta_mapeamento_pk), incremental acima do
# max real da tabela INTEIRA, com a folga do --pk-safety-band e o passo do
# --pk-passo. Tratá-las de novo aqui desfazia a folga e relia o max sem
# necessidade.
#
# Para DAT_VENCIMENTO, se não for informado um prazo fixo (por tabela em
# ENGORDA_PRAZO_DIAS_POR_TABELA, ou global via --prazo-vencimento-dias), o
# código preserva o prazo original da linha sintetizada:
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
ENGORDA_COL_DAT_ATUALIZACAO_REGISTRO = "DAT_ATUALIZACAO_REGISTRO"
ENGORDA_COL_DAT_VENCIMENTO = "DAT_VENCIMENTO"
ENGORDA_COL_DAT_EMISSAO = "DAT_EMISSAO"
# Colunas de auditoria comuns que recebem o MESMO timestamp único da engorda.
ENGORDA_COLS_TIMESTAMP = (
    ENGORDA_COL_DAT_INCLUSAO,
    ENGORDA_COL_DAT_ALTERACAO,
    ENGORDA_COL_DAT_INCLUSAO_REGISTRO,
)
ENGORDA_TIMESTAMP_COLS_BY_TABLE = {
    "OPERACAO": (
        ENGORDA_COL_DAT_ATUALIZACAO_REGISTRO,
        "TSP_SITUACAO",
    ),
    "INSTRUMENTO_FINANCEIRO": (ENGORDA_COL_DAT_ATUALIZACAO_REGISTRO,),
    "TITULO": (ENGORDA_COL_DAT_ATUALIZACAO_REGISTRO,),
}
ENGORDA_FORMATTED_TIMESTAMP_COLS_BY_TABLE = {
    "OPERACAO": ("VAL_TIME_STAMP_ATUALIZACAO",),
}
ENGORDA_OPERATIONAL_DATE_COLS_BY_TABLE = {
    "INSTRUMENTO_FINANCEIRO": (
        ENGORDA_COL_DAT_EMISSAO,
        "DAT_REGISTRO",
        "DAT_VAL_NOMINAL_EM",
        "DAT_ULTIMA_CORRECAO",
        "DAT_PU_CURVA",
        "DAT_VAL_NOMINAL_EM_ORIG",
        "DAT_FATOR_JUR_FLUT_ACUM_CDB",
    ),
    "ESPECIFICACAO": (
        "DAT_LIMITE_IDENTIFICACAO",
        "DAT_SITUACAO",
    ),
}
ENGORDA_EVENT_LIQUIDATION_COL = "DAT_LIQUIDACAO"
ENGORDA_EVENT_DERIVED_COLS = (
    "DAT_OCORRENCIA_EVENTO",
    "DAT_ORIGINAL_EVENTO",
)

# ---------------------------------------------------------------------------
# Deslocamento Δ das datas de vigência de CONDICAO_IF.
#
# A raiz tem DAT_EMISSAO reescrita para a data operacional, mas as condições
# filhas ficavam com a data ORIGINAL — anos antes da nova emissão. Toda
# DAT_INICIO_CONDICAO_IF caía fora de [DAT_EMISSAO, DAT_VENCIMENTO]
# (2b.escalonamento_dates).
#
# Carimbar a data do run NÃO serve: um instrumento escalonado tem N segmentos
# com N datas distintas, e carimbar colapsaria os N num valor só, trocando um
# erro por outro (2b.escalonamento_unique_dates). A única regra que preserva a
# ordem E o vínculo com a emissão é o mesmo deslocamento relativo já usado em
# ajusta_datas_resgate:
#     Δ = nova DAT_EMISSAO - DAT_EMISSAO original (por instrumento)
# Assim min(DAT_INICIO) == DAT_EMISSAO continua valendo se valia na origem.
ENGORDA_CONDICAO_IF_SHIFT_COLS = (
    "DAT_INICIO_CONDICAO_IF",
    "DAT_FIM_CONDICAO_IF",
)
CONDICAO_IF_SHIFT_KEY_COL = "__shift_num_if"
CONDICAO_IF_SHIFT_DAYS_COL = "__shift_days_cif"

# ---------------------------------------------------------------------------
# Filtro de linhas logicamente excluídas no fecho.
#
# calcula_lotes desce a árvore por FK PURA, sem olhar DAT_EXCLUSAO. Uma
# CONDICAO_IF soft-deleted, seu RESGATE e o CONDICAO_RESGATE correspondente
# entram todos no lote. O validador descarta pais inativos, então esses filhos
# ficam órfãos POR CONSTRUÇÃO (resgate_schedule_parent / coverage).
COL_DAT_EXCLUSAO = "DAT_EXCLUSAO"
COL_IND_EXCLUIDO = "IND_EXCLUIDO"
# A coluna de exclusão é declarada POR TABELA e espelha exatamente o predicado
# que o validador usa. NÃO existe fallback entre as duas colunas: uma tabela que
# tenha DAT_EXCLUSAO e IND_EXCLUIDO e seja filtrada pela coluna "errada" descarta
# linhas que o validador considera ATIVAS — o pai COM TABELA fica sem cronograma
# e o erro migra de resgate_schedule_parent para resgate_schedule_coverage.
#   CONDICAO_IF / RESGATE   -> _active()  = DAT_EXCLUSAO nula/''
#   CONDICAO_RESGATE        -> IND_EXCLUIDO fora de {S,Y,1} (NULL conta ativo)
FECHO_COLUNA_EXCLUSAO_POR_TABELA: Dict[str, str] = {
    "CONDICAO_IF": COL_DAT_EXCLUSAO,
    "RESGATE": COL_DAT_EXCLUSAO,
    "CONDICAO_RESGATE": COL_IND_EXCLUIDO,
}

# ---------------------------------------------------------------------------
# Poda do cronograma de resgate.
#
# O FILTRO_BASE das queries qualifica o INSTRUMENTO por EXISTS ("tem ao menos um
# resgate SEM TABELA"), mas o fecho clona TODAS as CONDICAO_IF type-20 dele —
# inclusive as de outro COD_COND_RESGATE. O app só aceita cronograma sob resgate
# COM TABELA, então CONDICAO_RESGATE pendurada em SEM TABELA/MERCADO/ESPECIFICA
# é órfã lógica por construção (resgate_schedule_parent).
CRONOGRAMA_TABELA = "CONDICAO_RESGATE"
RESGATE_TABELA = "RESGATE"
COL_COD_COND_RESGATE = "COD_COND_RESGATE"
COD_COND_RESGATE_COM_TABELA = "COM TABELA"

CONTROLE_OPERACIONAL_DATE_SQL = (
    "SELECT DAT_CTL_OPER "
    "FROM CETIP.CONTROLE_OPERACIONAL "
    "WHERE NUM_ORDEM = 0 AND NUM_SISTEMA IS NULL AND ROWNUM = 1"
)
DEFAULT_DT_VENCIMENTO_PRAZO_DIAS = 30
MIN_DT_VENCIMENTO_PRAZO_DIAS = 1
# Prazo FIXO de DAT_VENCIMENTO por tabela (dias). Vazio por padrão: sem entrada
# aqui, vale o --prazo-vencimento-dias e, na falta dele, o prazo original da
# linha sintetizada. Entrada aqui tem precedência sobre a CLI.
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
DEFAULT_CLONE_PREFIX = "sintetizacao_multiproduto"
DEFAULT_QUERIES_FILENAME = "queries_produtos.sql"
DEFAULT_SEED = 42
MAPA_NUM_IF_TABLE = "MAPA_CLONE_NUM_IF"
MAPA_COD_IF_TABLE = "MAPA_CLONE_COD_IF"
MAPA_COD_OPERACAO_TABLE = "MAPA_CLONE_COD_OPERACAO"
DEFAULT_ORACLE_CODE_BATCH_SIZE = 50_000
MAX_MEU_NUMERO_ORDINAL = 9_999_999
MEU_PREFIX_PATTERN = re.compile(r"^[1-9][0-9]{2}$")
PRODUTO_NOME_RE = re.compile(r"[a-z][a-z0-9_]*")
QUERY_SECTION_RE = re.compile(
    r"^[ \t]*--[ \t]*BEGIN QUERY:[ \t]*"
    r"([a-z][a-z0-9_]*)[ \t]*\r?$"
    r"(.*?)"
    r"^[ \t]*--[ \t]*END QUERY:[ \t]*\1[ \t]*\r?$",
    re.MULTILINE | re.DOTALL,
)
RAW_SOURCE_PLACEHOLDER_RE = re.compile(
    r"\{\{RAW_([A-Z][A-Z0-9_]*)\}\}", re.IGNORECASE
)
SQL_PLACEHOLDER_RE = re.compile(r"\{\{[^{}]+\}\}")

# ---------------------------------------------------------------------------
# Pattern de COD_IF: guarda ESTRUTURAL, não de produto.
#
# O Oracle devolve o COD_IF a partir do tipo que o job passou para
# F_GETCODIGONOVOIF21. Um pattern com prefixo de produto (ex.: ^CDB...) só
# casaria se o tipo já estivesse correto — logo, ele NUNCA foi capaz de detectar
# tipo trocado; a proteção real contra isso é a DERIVAÇÃO do tipo a partir do
# lote (_deriva_tipo_oracle). A função efetiva do pattern, nos três pontos onde
# é aplicado (_iter_oracle_code_batches, _materialize_code_map e
# _validate_business_keys), é rejeitar retorno VAZIO ou MALFORMADO do Oracle —
# ver a própria mensagem de erro "código vazio/malformado".
#
# Por isso o default é estrutural e agnóstico de produto. Quem quiser rigor
# adicional num run específico usa --cod-if-padrao (e --cod-if-dry-prefix, cuja
# compatibilidade com o pattern é validada no startup).
# ---------------------------------------------------------------------------
DEFAULT_COD_IF_PATTERN = r"^[0-9A-Z]{6,20}$"
DEFAULT_COD_IF_DRY_PREFIX = "SYN100"
DEFAULT_COD_OPERACAO_PATTERN = r"^[0-9]{16}$"
ENGORDA_PLAN_SCHEMA_VERSION = 2
ENGORDA_SELECTED_LOTE_SCHEMA_VERSION = 1
ENGORDA_RESERVATION_SCHEMA_VERSION = 1
ENGORDA_PLAN_ARTIFACT = "engorda_plan"
ENGORDA_SELECTED_LOTE_ARTIFACT = "engorda_selected_lote"
ENGORDA_RESERVATION_ARTIFACT = "engorda_reservation"
ENGORDA_PHASES = ("all", "plan", "materialize")


# ---------------------------------------------------------------------------
# Perfil do SCHEMA. REGRAS_SCHEMA_CETIP é editável e declarativo; ProductProfile
# é a representação interna validada usada pelo motor. Não há inferência pelo SQL
# e não há mais um dicionário por produto: o que variava entre cdb /
# cdb_simplificado / rdb virou (a) valor derivado do dado — tipo do instrumento —
# ou (b) parâmetro de CLI — query, spec, prefixo de saída, pattern.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SubtypePolicy:
    condition_table: str
    condition_pk: str
    condition_type_column: str
    active_column: str
    subtype_by_type: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class IntegrityPolicy:
    subtype: Optional[SubtypePolicy] = None
    nullify_columns: Tuple[Tuple[str, Tuple[str, ...]], ...] = ()
    selective_missing_keys: frozenset[Tuple[str, str]] = frozenset()

    def nullify_mapping(self) -> Dict[str, Tuple[str, ...]]:
        return {table: tuple(columns) for table, columns in self.nullify_columns}


@dataclass(frozen=True)
class OperationKeyPolicy:
    strategy: str
    table: str
    code_pattern: str
    generate_meu_numero: bool


@dataclass(frozen=True)
class BusinessKeyPolicy:
    cod_if_allocator: str
    cod_if_pattern: str
    cod_if_dry_prefix: str
    # None = ainda NÃO resolvido. O valor concreto é derivado do lote em
    # _deriva_tipo_oracle e injetado por _resolve_business_policy antes de
    # qualquer round-trip de alocação no Oracle.
    cod_if_oracle_type: Optional[int] = None
    operation: Optional[OperationKeyPolicy] = None


@dataclass(frozen=True)
class ProductProfile:
    name: str
    query_filename: str
    default_clone_prefix: str
    integrity: IntegrityPolicy
    business_keys: BusinessKeyPolicy
    static_tables: Tuple[str, ...] = ()
    date_strategy: Optional[str] = "standard"


@dataclass(frozen=True)
class EngordaJob:
    produto: str
    num_ifs: Optional[Tuple[int, ...]] = None
    n_instrumentos: Optional[int] = None
    fator_k: int = 1
    meu_numero_prefix: Optional[str] = None
    query_num_if_path: Optional[str] = None
    seed: int = DEFAULT_SEED
    pk_offset: int = 0
    pk_safety_band: int = 0
    pk_passo: int = 1
    offset_num_if: Optional[int] = None
    tratar_como_static: Tuple[str, ...] = ()
    max_passadas: int = 6
    engorda_ts: Optional[datetime] = None
    controle_operacional_date: Optional[date] = None
    prazo_vencimento_dias: Optional[int] = None
    faltantes_arg: Optional[str] = None
    faltantes_parquet: Optional[str] = None
    poda_subtipo: bool = True
    somente_ativos: bool = True
    anular_cols: Optional[Mapping[str, Sequence[str]]] = None
    oracle_code_batch_size: int = DEFAULT_ORACLE_CODE_BATCH_SIZE
    dry_run: bool = False
    specs_uri: Optional[str] = None
    clone_prefix: Optional[str] = None
    # Confere (não define) o tipo derivado do lote. Ver _deriva_tipo_oracle.
    tipo_oracle: Optional[int] = None
    # Overrides estruturais do COD_IF; None = defaults agnósticos de produto.
    cod_if_pattern: Optional[str] = None
    cod_if_dry_prefix: Optional[str] = None
    phase: str = "all"
    plan_uri: Optional[str] = None
    reservation_uri: Optional[str] = None
    raw_uri: Optional[str] = None
    output_uri: Optional[str] = None


# Tabelas que devem ser engordadas por produto. As tabelas correspondentes são
# marcadas como static=False no spec único durante a execução.
TABELAS_ENGORDA_POR_PRODUTO: Dict[str, Tuple[str, ...]] = {
    "cdb_simplificado": (
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
        "CONDICAO_IF",
        "CREDITO",
        "DADO_OPERACAO",
        "DEPOSITO_AUTOMATICO_IF",
        "DESDOBRAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "EVENTO",
        "INSTRUMENTO_FINANCEIRO",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "LANCAMENTO",
        "OPERACAO",
        "PARTICIPACAO_LUCROS",
        "RESET",
        "RESGATE",
        "SPREAD",
        "TITULO",
    ),
    "cdb_resgate": (
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
        "CONDICAO_IF",
        "CREDITO",
        "DADO_OPERACAO",
        "DEPOSITO_AUTOMATICO_IF",
        "DESDOBRAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "EVENTO",
        "INSTRUMENTO_FINANCEIRO",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "LANCAMENTO",
        "OPERACAO",
        "PARTICIPACAO_LUCROS",
        "RESET",
        "RESGATE",
        "SPREAD",
        "TITULO",
        "CONDICAO_RESGATE",
        "PENDENCIA_IF",
    ),
    "cdb_escalonamento": (
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
        "CONDICAO_IF",
        "CREDITO",
        "DADO_OPERACAO",
        "DEPOSITO_AUTOMATICO_IF",
        "DESDOBRAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "EVENTO",
        "INSTRUMENTO_FINANCEIRO",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "LANCAMENTO",
        "OPERACAO",
        "PARTICIPACAO_LUCROS",
        "RESET",
        "RESGATE",
        "SPREAD",
        "TITULO",
        "CONDICAO_RESGATE",
        "PENDENCIA_IF",
    ),
    "rdb_inclusao": (
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
        "CONDICAO_IF",
        "CREDITO",
        "DADO_OPERACAO",
        "DEPOSITO_AUTOMATICO_IF",
        "DESDOBRAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "EVENTO",
        "INSTRUMENTO_FINANCEIRO",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "LANCAMENTO",
        "OPERACAO",
        "PARTICIPACAO_LUCROS",
        "RESET",
        "RESGATE",
        "SPREAD",
        "TITULO",
        "CONDICAO_RESGATE",
        "HISTORICO_PU_CURVA",
        "PENDENCIA_IF",
    ),
    "rdb_resgate": (
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
        "CONDICAO_IF",
        "CREDITO",
        "DADO_OPERACAO",
        "DEPOSITO_AUTOMATICO_IF",
        "DESDOBRAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "EVENTO",
        "INSTRUMENTO_FINANCEIRO",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "LANCAMENTO",
        "OPERACAO",
        "PARTICIPACAO_LUCROS",
        "RESET",
        "RESGATE",
        "SPREAD",
        "TITULO",
        "CONDICAO_RESGATE",
        "HISTORICO_PU_CURVA",
        "PENDENCIA_IF",
    ),
    "lci": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "JUROS_FIXO",
        "JUROS_FLUTUANTE",
        "ATUALIZACAO_POS",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "EVENTO",
        "DEPOSITO_AUTOMATICO_IF",
        "OPERACAO",
        "DADO_OPERACAO",
        "LANCAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
    ),
    "lca": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "IF_LCA",
        "CREDITO",
        "GARANTIA",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "JUROS_FLUTUANTE",
        "SPREAD",
        "RESGATE",
        "EVENTO",
        "DEPOSITO_AUTOMATICO_IF",
        "OPERACAO",
        "DADO_OPERACAO",
        "LANCAMENTO",
        "ESPECIFICACAO",
        "ESPECIFICACAO_COMITENTE",
        "CARTEIRA_COMITENTE",
        "CARTEIRA_PARTICIPANTE",
    ),
    "ccb_pppre": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "JUROS_FIXO",
        "SPREAD",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "HISTORICO_IF_TITULO",
        "ALTERACAO_IF",
        "TCTPIF_CCB",
        "TCTPCRONOGRAMA_CCB",
        "OPERACAO",
        "LANCAMENTO",
        "GARANTIA",
        "TCTPCADEIA_IPOC",
    ),
    "ccb_pfpre": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "JUROS_FIXO",
        "SPREAD",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "HISTORICO_IF_TITULO",
        "ALTERACAO_IF",
        "TCTPIF_CCB",
        "TCTPCRONOGRAMA_CCB",
        "OPERACAO",
        "LANCAMENTO",
        "GARANTIA",
        "TCTPCADEIA_IPOC",
    ),
    "ccb_pgrpre": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "JUROS_FIXO",
        "SPREAD",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "HISTORICO_IF_TITULO",
        "ALTERACAO_IF",
        "TCTPIF_CCB",
        "TCTPCRONOGRAMA_CCB",
        "OPERACAO",
        "LANCAMENTO",
        "GARANTIA",
        "TCTPCADEIA_IPOC",
    ),
    "ccb_favcp": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "JUROS_FIXO",
        "SPREAD",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "HISTORICO_IF_TITULO",
        "ALTERACAO_IF",
        "TCTPIF_CCB",
        "TCTPCRONOGRAMA_CCB",
        "OPERACAO",
        "LANCAMENTO",
        "GARANTIA",
        "TCTPCADEIA_IPOC",
    ),
    "ccb_fapre": (
        "INSTRUMENTO_FINANCEIRO",
        "TITULO",
        "CREDITO",
        "CONDICAO_IF",
        "AMORTIZACAO",
        "ATUALIZACAO_POS",
        "ATUALIZACAO_PRE",
        "JUROS_FIXO",
        "SPREAD",
        "RESGATE",
        "HISTORICO_PU_CURVA",
        "HISTORICO_IF_TITULO",
        "ALTERACAO_IF",
        "TCTPIF_CCB",
        "TCTPCRONOGRAMA_CCB",
        "OPERACAO",
        "LANCAMENTO",
        "GARANTIA",
        "TCTPCADEIA_IPOC",
    ),
    "gravame": (
        "INSTRUMENTO_FINANCEIRO",
        "COMPLEMENTO_CONTRATO",
        "IF_GRVM",
        "PARAMETRO_PONTA",
        "CONTA",
        "OPERACAO",
        "LANCAMENTO",
        "DADO_OPERACAO",
        "ARQUIVO_TRANSF",
        "ARQUIVO_TRANSF_CONTEUDO",
        "ARQUIVO_IF",
        "PROTOCOLO",
        "GRAVAME_GRAU_PENHOR",
        "ALERTA",
    ),
    "lastro": (
        "LOTE",
        "CREDITO_SCR",
        "HISTORICO_CREDITO_SCR",
    ),
    "direito_creditorio": (
        "LOTE",
        "CREDITO_DC",
        "HISTORICO_CREDITO_DC",
        "TCTPCHAV_IROP_ATIV",
        "TCTPDET_CHAV_IROP_CCB",
        "TCTPDET_CHAV_IROP_CMER",
        "TCTPIROP_ATIV",
        "TCTPSOLI_IROP_ATIV",
    ),
}


# Perfil ÚNICO do schema CETIP, comum a todos os produtos.
REGRAS_SCHEMA_CETIP: Dict[str, Any] = {
    # None desliga os ajustes DAT_*.
    "ajuste_datas": "standard",
    "tabelas_static": (),
    "integridade": {
        # None desliga a poda de subtipo.
        "subtipo": {
            "tabela_condicao": CONDICAO_IF_TABLE,
            "pk_condicao": CONDICAO_IF_PK,
            "coluna_tipo": CONDICAO_IF_TIPO_COL,
            "coluna_ativa": "DAT_EXCLUSAO",
            "tabelas_por_tipo": {
                "1": "AMORTIZACAO", "2": "JUROS_FIXO",
                "3": "JUROS_FLUTUANTE", "4": "ATUALIZACAO_POS",
                "5": "SPREAD", "6": "PARTICIPACAO_LUCROS",
                "7": "PREMIO", "14": "ATUALIZACAO_PRE",
                "15": "PREMIO_OPCAO", "16": "TERMO",
                "17": "PARAMETRO_LIMITE", "20": "RESGATE",
                "21": "PREMIO_CONTRATO", "22": "OPCAO",
                "23": "RESET", "24": "DESDOBRAMENTO",
            },
        },
        # {} desliga as nulificações integrais.
        "nulificar_colunas": {
            "OPERACAO": (
                "NUM_ID_TRANSF_ARQ_P1", "NUM_ID_TRANSF_ARQ_P2",
            ),
        },
        # () desliga as nulificações seletivas de faltantes.
        # P1 e P2 têm as MESMAS propriedades no spec: FK filha para
        # CONTEXTO_MENSAGEM (static) e ausentes de not_null_cols de OPERACAO.
        # Só P2 estava listada — daí os órfãos de NUM_ID_CTX_MSG_P1 (Cat 3).
        "faltantes_seletivos": (
            ("OPERACAO", "NUM_ID_CTX_MSG_P1"),
            ("OPERACAO", "NUM_ID_CTX_MSG_P2"),
        ),
    },
    "chaves_negocio": {
        # COD_IF é obrigatório e não possui flag enabled. tipo_oracle NÃO
        # aparece aqui: é derivado do lote em tempo de execução.
        "cod_if": {
            "alocador": "oracle_if21",
            "padrao": DEFAULT_COD_IF_PATTERN,
            "prefixo_dry_run": DEFAULT_COD_IF_DRY_PREFIX,
        },
        # None desliga toda a estratégia específica de OPERACAO.
        "operacao": {
            "estrategia": "cetip_operacao_v1",
            "tabela": "OPERACAO",
            "padrao_codigo": DEFAULT_COD_OPERACAO_PATTERN,
            "gerar_meu_numero": True,
        },
    },
}


def _rule_mapping(value: Any, expected: Set[str], context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{context} precisa ser um dicionário")
    keys = set(value)
    missing = sorted(expected - keys)
    unknown = sorted(keys - expected)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"ausentes={missing}")
        if unknown:
            details.append(f"desconhecidas={unknown}")
        raise ValueError(f"{context}: " + "; ".join(details))
    return value


def _rule_sequence(value: Any, context: str) -> Tuple[Any, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(
            value, (list, tuple, set, frozenset)):
        raise ValueError(f"{context} precisa ser uma sequência")
    return tuple(value)


RULE_IDENTIFIER_RE = re.compile(r"^[A-Z][A-Z0-9_$#]*$")


def _normalize_rule_identifier(value: Any, context: str, *,
                               table: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} precisa ser um identificador não vazio")
    normalized = value.strip().upper()
    if table:
        parts = normalized.split(".")
        if len(parts) > 2 or any(not part for part in parts):
            raise ValueError(f"{context} contém tabela inválida: {value!r}")
        if any(not RULE_IDENTIFIER_RE.fullmatch(part) for part in parts):
            raise ValueError(f"{context} contém tabela inválida: {value!r}")
        return parts[-1]
    if not RULE_IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{context} contém identificador inválido: {value!r}")
    return normalized


def _normalize_clone_prefix(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("clone_prefix precisa ser texto")
    prefix = value.strip().strip("/")
    if not prefix:
        raise ValueError("clone_prefix não pode ser vazio")
    if any(part in ("", ".", "..") for part in prefix.split("/")):
        raise ValueError("clone_prefix contém segmento inválido")
    return prefix


def _normalize_produto(name: Any) -> str:
    """Normaliza e valida um produto configurado para engorda.

    O nome seleciona as tabelas em TABELAS_ENGORDA_POR_PRODUTO, a query no
    catálogo SQL único e o default de --clone-prefix."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("produto precisa ser texto não vazio")
    normalized = name.strip().lower()
    if not PRODUTO_NOME_RE.fullmatch(normalized):
        raise ValueError(
            f"nome de produto inválido: {name!r} (use [a-z][a-z0-9_]*)")
    if normalized not in TABELAS_ENGORDA_POR_PRODUTO:
        raise ValueError(
            f"produto desconhecido: {name!r}; escolha entre: "
            + ", ".join(TABELAS_ENGORDA_POR_PRODUTO)
        )
    return normalized


def _build_product_profile(
    name: str,
    raw: Mapping[str, Any],
    *,
    query_filename: Optional[str] = None,
    clone_prefix: Optional[str] = None,
    cod_if_pattern: Optional[str] = None,
    cod_if_dry_prefix: Optional[str] = None,
    tipo_oracle: Optional[int] = None,
) -> ProductProfile:
    """Compila o perfil do schema para um rótulo de produto, aplicando os
    overrides de CLI. Sem overrides, tudo cai nos defaults derivados do nome."""
    produto = _normalize_produto(name)
    rules = _rule_mapping(raw, {
        "ajuste_datas", "tabelas_static", "integridade", "chaves_negocio",
    }, "REGRAS_SCHEMA_CETIP")
    integrity_raw = _rule_mapping(rules["integridade"], {
        "subtipo", "nulificar_colunas", "faltantes_seletivos",
    }, "integridade")

    subtype_raw = integrity_raw["subtipo"]
    subtype = None
    if subtype_raw is not None:
        subtype_cfg = _rule_mapping(subtype_raw, {
            "tabela_condicao", "pk_condicao", "coluna_tipo", "coluna_ativa",
            "tabelas_por_tipo",
        }, "integridade.subtipo")
        subtype_tables = subtype_cfg["tabelas_por_tipo"]
        if not isinstance(subtype_tables, Mapping):
            raise ValueError("integridade.subtipo.tabelas_por_tipo inválido")
        subtype_pairs: List[Tuple[str, str]] = []
        for raw_type, raw_table in subtype_tables.items():
            if not isinstance(raw_type, str) or not raw_type.strip():
                raise ValueError("integridade.subtipo contém tipo inválido")
            subtype_pairs.append((
                raw_type.strip(),
                _normalize_rule_identifier(
                    raw_table,
                    f"integridade.subtipo.tabelas_por_tipo[{raw_type!r}]",
                    table=True,
                ),
            ))
        subtype = SubtypePolicy(
            condition_table=_normalize_rule_identifier(
                subtype_cfg["tabela_condicao"],
                "integridade.subtipo.tabela_condicao",
                table=True,
            ),
            condition_pk=_normalize_rule_identifier(
                subtype_cfg["pk_condicao"],
                "integridade.subtipo.pk_condicao",
            ),
            condition_type_column=_normalize_rule_identifier(
                subtype_cfg["coluna_tipo"],
                "integridade.subtipo.coluna_tipo",
            ),
            active_column=_normalize_rule_identifier(
                subtype_cfg["coluna_ativa"],
                "integridade.subtipo.coluna_ativa",
            ),
            subtype_by_type=tuple(subtype_pairs),
        )

    nullify_raw = integrity_raw["nulificar_colunas"]
    if not isinstance(nullify_raw, Mapping):
        raise ValueError("integridade.nulificar_colunas inválido")
    normalized_nullify: Dict[str, List[str]] = {}
    for raw_table, raw_columns in nullify_raw.items():
        table = _normalize_rule_identifier(
            raw_table, "integridade.nulificar_colunas", table=True
        )
        target = normalized_nullify.setdefault(table, [])
        for raw_column in _rule_sequence(
                raw_columns, f"{table}.nulificar_colunas"):
            column = _normalize_rule_identifier(
                raw_column, f"{table}.nulificar_colunas"
            )
            if column not in target:
                target.append(column)
    nullify_columns = tuple(
        (table, tuple(columns))
        for table, columns in normalized_nullify.items()
    )

    normalized_selective: Set[Tuple[str, str]] = set()
    for item in _rule_sequence(
            integrity_raw["faltantes_seletivos"],
            "integridade.faltantes_seletivos"):
        pair = _rule_sequence(item, "faltantes_seletivos")
        if len(pair) != 2:
            raise ValueError(
                "faltantes_seletivos precisa conter pares (TABELA, COLUNA)"
            )
        normalized_selective.add((
            _normalize_rule_identifier(
                pair[0], "faltantes_seletivos.tabela", table=True
            ),
            _normalize_rule_identifier(
                pair[1], "faltantes_seletivos.coluna"
            ),
        ))
    selective_missing = frozenset(normalized_selective)

    business_raw = _rule_mapping(rules["chaves_negocio"], {
        "cod_if", "operacao",
    }, "chaves_negocio")
    cod_if = _rule_mapping(business_raw["cod_if"], {
        "alocador", "padrao", "prefixo_dry_run",
    }, "chaves_negocio.cod_if")
    operation_raw = business_raw["operacao"]
    operation = None
    if operation_raw is not None:
        operation_cfg = _rule_mapping(operation_raw, {
            "estrategia", "tabela", "padrao_codigo", "gerar_meu_numero",
        }, "chaves_negocio.operacao")
        operation = OperationKeyPolicy(
            strategy=operation_cfg["estrategia"],
            table=_normalize_rule_identifier(
                operation_cfg["tabela"],
                "chaves_negocio.operacao.tabela",
                table=True,
            ),
            code_pattern=operation_cfg["padrao_codigo"],
            generate_meu_numero=operation_cfg["gerar_meu_numero"],
        )

    if cod_if_pattern is not None and (
            not isinstance(cod_if_pattern, str) or not cod_if_pattern.strip()):
        raise ValueError("cod_if_pattern precisa ser texto não vazio")
    if cod_if_dry_prefix is not None and (
            not isinstance(cod_if_dry_prefix, str)
            or not cod_if_dry_prefix.strip()):
        raise ValueError("cod_if_dry_prefix precisa ser texto não vazio")

    return ProductProfile(
        name=produto,
        query_filename=(query_filename.strip()
                        if isinstance(query_filename, str) and query_filename.strip()
                        else DEFAULT_QUERIES_FILENAME),
        default_clone_prefix=_normalize_clone_prefix(
            clone_prefix if clone_prefix else f"{DEFAULT_CLONE_PREFIX}/{produto}"
        ),
        integrity=IntegrityPolicy(
            subtype=subtype,
            nullify_columns=nullify_columns,
            selective_missing_keys=selective_missing,
        ),
        business_keys=BusinessKeyPolicy(
            cod_if_allocator=cod_if["alocador"],
            cod_if_pattern=(cod_if_pattern.strip() if cod_if_pattern
                            else cod_if["padrao"]),
            cod_if_dry_prefix=(cod_if_dry_prefix.strip() if cod_if_dry_prefix
                               else cod_if["prefixo_dry_run"]),
            cod_if_oracle_type=tipo_oracle,
            operation=operation,
        ),
        static_tables=tuple(
            _normalize_rule_identifier(
                table, "tabelas_static", table=True
            )
            for table in _rule_sequence(
                rules["tabelas_static"], "tabelas_static"
            )
        ),
        date_strategy=rules["ajuste_datas"],
    )


def _validate_product_profile(profile: ProductProfile) -> None:
    _normalize_produto(profile.name)
    if (not isinstance(profile.query_filename, str)
            or not profile.query_filename.strip().lower().endswith(".sql")):
        raise ValueError(f"{profile.name}: query_filename precisa apontar para .sql")
    _normalize_clone_prefix(profile.default_clone_prefix)
    if profile.date_strategy not in (None, "standard"):
        raise ValueError(
            f"{profile.name}: date_strategy desconhecida {profile.date_strategy!r}"
        )
    if not isinstance(profile.integrity, IntegrityPolicy):
        raise ValueError(f"{profile.name}: integrity precisa ser IntegrityPolicy")
    if profile.integrity.subtype is not None:
        subtype = profile.integrity.subtype
        if not all((subtype.condition_table, subtype.condition_pk,
                    subtype.condition_type_column, subtype.active_column)):
            raise ValueError(f"{profile.name}: política de subtipo incompleta")
        if not subtype.subtype_by_type:
            raise ValueError(f"{profile.name}: política de subtipo sem tabelas")
        if any(not isinstance(tipo, str) or not tipo.strip()
               or not isinstance(table, str) or not table.strip()
               for tipo, table in subtype.subtype_by_type):
            raise ValueError(f"{profile.name}: mapa de subtipo contém entrada inválida")
        subtype_types = [str(tipo) for tipo, _ in subtype.subtype_by_type]
        if len(set(subtype_types)) != len(subtype_types):
            raise ValueError(f"{profile.name}: mapa de subtipo repete código de tipo")
    nullify_tables: Set[str] = set()
    for table, columns in profile.integrity.nullify_columns:
        normalized_table = _normalize_rule_identifier(
            table, f"{profile.name}.integrity.nullify_columns", table=True
        )
        if normalized_table != table or normalized_table in nullify_tables:
            raise ValueError(
                f"{profile.name}: tabela de nulificação inválida/duplicada {table!r}"
            )
        nullify_tables.add(normalized_table)
        if len(set(columns)) != len(columns):
            raise ValueError(
                f"{profile.name}.{table}: colunas de nulificação duplicadas"
            )
        for column in columns:
            if _normalize_rule_identifier(
                    column, f"{profile.name}.{table}.nullify_columns") != column:
                raise ValueError(
                    f"{profile.name}.{table}: coluna não normalizada {column!r}"
                )
    for table, column in profile.integrity.selective_missing_keys:
        if (_normalize_rule_identifier(
                table, f"{profile.name}.selective_missing.table", table=True
        ) != table or _normalize_rule_identifier(
                column, f"{profile.name}.selective_missing.column"
        ) != column):
            raise ValueError(
                f"{profile.name}: faltante seletivo não normalizado "
                f"{table}.{column}"
            )
    if any(not isinstance(table, str) or not table.strip()
           for table in profile.static_tables):
        raise ValueError(f"{profile.name}: static_tables contém nome inválido")
    static_tables = {
        table_path_name(table.strip().upper()) for table in profile.static_tables
    }
    if TABELA_RAIZ in static_tables:
        raise ValueError(f"{profile.name}: {TABELA_RAIZ} não pode ser static")

    policy = profile.business_keys
    if not isinstance(policy, BusinessKeyPolicy):
        raise ValueError(
            f"{profile.name}: política de COD_IF é obrigatória para evitar colisões"
        )
    if policy.cod_if_allocator != "oracle_if21":
        raise ValueError(
            f"{profile.name}: alocador COD_IF desconhecido "
            f"{policy.cod_if_allocator!r}"
        )
    # None é legítimo ANTES da derivação; se vier preenchido (--tipo-oracle ou
    # policy já resolvida), tem que ser inteiro positivo.
    if policy.cod_if_oracle_type is not None and (
            type(policy.cod_if_oracle_type) is not int
            or policy.cod_if_oracle_type < 1):
        raise ValueError(
            f"{profile.name}: cod_if_oracle_type deve ser inteiro positivo"
        )
    if not policy.cod_if_pattern:
        raise ValueError(f"{profile.name}: padrão de COD_IF é obrigatório")
    re.compile(policy.cod_if_pattern)
    dry_sample = policy.cod_if_dry_prefix + "00001"
    if not re.fullmatch(policy.cod_if_pattern, dry_sample):
        raise ValueError(
            f"{profile.name}: prefixo dry-run {policy.cod_if_dry_prefix!r} "
            f"não produz COD_IF compatível com o pattern "
            f"{policy.cod_if_pattern!r} (amostra: {dry_sample!r}). Ajuste "
            "--cod-if-dry-prefix junto com --cod-if-padrao."
        )
    operation = policy.operation
    if operation is not None:
        if not isinstance(operation, OperationKeyPolicy):
            raise ValueError(f"{profile.name}: política de operação inválida")
        if operation.strategy != "cetip_operacao_v1":
            raise ValueError(
                f"{profile.name}: estratégia de operação desconhecida "
                f"{operation.strategy!r}"
            )
        if operation.table != "OPERACAO":
            raise ValueError(
                f"{profile.name}: cetip_operacao_v1 exige tabela OPERACAO"
            )
        if not operation.code_pattern:
            raise ValueError(
                f"{profile.name}: padrão de COD_OPERACAO é obrigatório"
            )
        re.compile(operation.code_pattern)
        if not re.fullmatch(operation.code_pattern, "0000000000000001"):
            raise ValueError(
                f"{profile.name}: padrão de COD_OPERACAO não aceita o dry-run"
            )
        if type(operation.generate_meu_numero) is not bool:
            raise ValueError(
                f"{profile.name}: gerar_meu_numero precisa ser booleano"
            )
        if operation.table in static_tables:
            raise ValueError(
                f"{profile.name}: {operation.table} não pode ser static"
            )


def get_product_profile(
    name: str,
    *,
    query_filename: Optional[str] = None,
    clone_prefix: Optional[str] = None,
    cod_if_pattern: Optional[str] = None,
    cod_if_dry_prefix: Optional[str] = None,
    tipo_oracle: Optional[int] = None,
) -> ProductProfile:
    """Compila e valida o perfil para um produto configurado.

    O perfil técnico é único (REGRAS_SCHEMA_CETIP); as tabelas engordáveis são
    selecionadas por TABELAS_ENGORDA_POR_PRODUTO."""
    profile = _build_product_profile(
        name, REGRAS_SCHEMA_CETIP,
        query_filename=query_filename,
        clone_prefix=clone_prefix,
        cod_if_pattern=cod_if_pattern,
        cod_if_dry_prefix=cod_if_dry_prefix,
        tipo_oracle=tipo_oracle,
    )
    _validate_product_profile(profile)
    return profile


def _resolve_business_policy(policy: BusinessKeyPolicy,
                             tipo_oracle: int) -> BusinessKeyPolicy:
    """Injeta o tipo DERIVADO do lote na política de chaves de negócio.

    Chamado uma única vez por run, depois da seleção dos instrumentos e antes de
    qualquer round-trip de alocação. A partir daqui cod_if_oracle_type é um
    inteiro concreto e _allocation_sql pode montar a chamada do Oracle."""
    if type(tipo_oracle) is not int or tipo_oracle < 1:
        raise ValueError("tipo derivado deve ser inteiro positivo")
    if (policy.cod_if_oracle_type is not None
            and int(policy.cod_if_oracle_type) != int(tipo_oracle)):
        raise ValueError(
            f"tipo já fixado ({policy.cod_if_oracle_type}) diverge do derivado "
            f"({tipo_oracle})")
    return dataclasses.replace(policy, cod_if_oracle_type=int(tipo_oracle))


# Coluna temporária com o índice do sintético (1..K). Sufixo improvável de
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
    exact_output = config.get("DATAGEN_OUTPUT_URI")
    if exact_output:
        return exact_output.rstrip("/")
    base = config["DATAGEN_SYNTHETIC_BASE_URI"]
    prefix = config.get("DATAGEN_CLONE_PREFIX") or DEFAULT_CLONE_PREFIX
    return f"{base}/{prefix}"


def get_engorda_env(
    specs_uri_override: Optional[str] = None,
    raw_uri_override: Optional[str] = None,
    output_uri_override: Optional[str] = None,
) -> dict[str, str]:
    """Mesmas envs do engorda_tables.py + DATAGEN_CLONE_PREFIX opcional, para
    que a configuração do Data Flow seja idêntica entre os dois jobs."""
    config: dict[str, str] = {}
    missing = []
    for name in REQUIRED_ENV_VARS:
        if name == "DATAGEN_SPECS_URI" and specs_uri_override:
            value = specs_uri_override
        elif name == "DATAGEN_RAW_BASE_URI" and raw_uri_override:
            value = raw_uri_override
        elif name == "DATAGEN_SYNTHETIC_BASE_URI" and output_uri_override:
            value = output_uri_override
        else:
            value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")
    if missing:
        logger.error("Env var(s) obrigatória(s) ausente(s): %s", ", ".join(missing))
        sys.exit(1)
    config["DATAGEN_RAW_PREFIX"] = (
        "" if raw_uri_override
        else os.environ.get("DATAGEN_RAW_PREFIX", "").strip("/")
    )
    config["DATAGEN_SYNTHETIC_PREFIX"] = os.environ.get(
        "DATAGEN_SYNTHETIC_PREFIX", "").strip("/")
    config["DATAGEN_CLONE_PREFIX"] = os.environ.get(
        "DATAGEN_CLONE_PREFIX", DEFAULT_CLONE_PREFIX).strip("/")
    if output_uri_override:
        config["DATAGEN_OUTPUT_URI"] = output_uri_override.rstrip("/")
    for name in ORACLE_ENV_VARS:
        value = os.environ.get(name)
        if value:
            config[name] = value
    return config


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def _default_num_if_query_path(filename: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def _extract_num_if_query_for_product(catalog_text: str, produto: str,
                                      source: str) -> str:
    """Extrai do catálogo SQL único a query correspondente ao produto."""
    produto = _normalize_produto(produto)
    sections: Dict[str, str] = {}
    for match in QUERY_SECTION_RE.finditer(catalog_text):
        section_product = match.group(1)
        if section_product in sections:
            raise ValueError(
                f"catálogo de queries {source!r} repete o produto "
                f"{section_product!r}"
            )
        sections[section_product] = match.group(2).strip()

    expected = set(TABELAS_ENGORDA_POR_PRODUTO)
    missing = sorted(expected - set(sections))
    unknown = sorted(set(sections) - expected)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"produtos ausentes={missing}")
        if unknown:
            details.append(f"produtos desconhecidos={unknown}")
        raise ValueError(
            f"catálogo de queries inválido em {source!r}: " + "; ".join(details)
        )

    query = sections[produto]
    if not query:
        raise ValueError(
            f"produto {produto!r} ainda não possui query configurada em "
            f"{source!r}"
        )
    return query


def _read_num_if_query_text(spark: SparkSession,
                            query_path: Optional[str],
                            default_filename: Optional[str],
                            produto: str) -> Tuple[str, str]:
    """Lê o catálogo SQL e devolve somente a query do produto selecionado."""
    selected_path = query_path or default_filename
    if selected_path is None:
        raise ValueError("query de NUM_IF não informada e produto sem arquivo SQL")
    path = selected_path.strip()
    if not path:
        raise ValueError("caminho da query de NUM_IF está vazio")
    if "://" in path:
        rows = spark.read.text(path).collect()
        text = "\n".join(row["value"] for row in rows)
        resolved = path
    else:
        expanded = os.path.abspath(os.path.expanduser(path))
        if not os.path.isfile(expanded):
            beside_script = _default_num_if_query_path(path)
            localized = SparkFiles.get(os.path.basename(path))
            for candidate in (beside_script, localized):
                if os.path.isfile(candidate):
                    expanded = candidate
                    break
        try:
            with open(expanded, "r", encoding="utf-8-sig") as sql_file:
                text = sql_file.read()
        except OSError as exc:
            raise ValueError(
                f"não foi possível ler a query de NUM_IF em {expanded!r}: {exc}"
            ) from exc
        resolved = expanded
    if not text.strip():
        raise ValueError(f"catálogo de queries vazio em {resolved!r}")
    query = _extract_num_if_query_for_product(text, produto, resolved)
    return query, f"{resolved} [produto={produto}]"


def _render_num_if_query(sql_text: str, config: Mapping[str, str]) -> str:
    """Expande {{RAW_TABELA}} para parquet.`<path RAW da tabela>`."""
    tables: Set[str] = set()

    def replace_source(match: re.Match) -> str:
        table = table_path_name(match.group(1).upper())
        path = raw_path(dict(config), table)
        if "`" in path:
            raise ValueError(f"path RAW de {table} contém crase e não é SQL-safe")
        tables.add(table)
        return f"parquet.`{path}`"

    rendered = RAW_SOURCE_PLACEHOLDER_RE.sub(replace_source, sql_text).strip()
    unresolved = sorted(set(SQL_PLACEHOLDER_RE.findall(rendered)))
    if unresolved:
        raise ValueError(
            "placeholder(s) desconhecido(s) na query de NUM_IF: "
            + ", ".join(unresolved)
            + ". Use {{RAW_NOME_DA_TABELA}}."
        )
    if rendered.endswith(";"):
        rendered = rendered[:-1].rstrip()
    logger.info("Query de NUM_IF usa fonte(s) RAW: %s", sorted(tables) or "paths literais")
    return rendered


def _read_source(spark, config, table: str) -> DataFrame:
    """Leitura RAW; o domínio de NUM_IF é definido exclusivamente pela query."""
    return read_parquet(spark, raw_path(config, table))


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
    """Cópia de engorda_tables.load_specs (specs.json único via wholeTextFiles).

    O spec É a definição do conjunto de tabelas engordadas: o motor sintetiza
    exatamente as tabelas não-static presentes aqui. Produto novo = spec do
    fecho daquele produto, via --specs."""
    records = spark.sparkContext.wholeTextFiles(specs_uri).collect()
    if len(records) != 1:
        raise ValueError(
            f"Esperado exatamente um specs.json em `{specs_uri}`, achei {len(records)}.")
    parsed = json.loads(records[0][1])
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"specs.json em `{specs_uri}` precisa ser objeto não-vazio.")
    return normalize_specs(parsed)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _local_artifact_path(uri: str) -> Optional[str]:
    if uri.startswith("file://"):
        return uri[7:]
    return None if "://" in uri else uri


def _write_json_artifact(spark: SparkSession, uri: str,
                         artifact: Mapping[str, Any]) -> None:
    """Write one immutable deterministic JSON object locally or through Hadoop FS."""
    text = json.dumps(artifact, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    local_path = _local_artifact_path(uri)
    if local_path is not None:
        parent = os.path.dirname(os.path.abspath(local_path))
        os.makedirs(parent, exist_ok=True)
        if os.path.exists(local_path):
            raise ValueError(f"artefato JSON imutável já existe em {uri!r}")
        temporary = f"{local_path}.tmp-{uuid.uuid4().hex}"
        try:
            with open(temporary, "w", encoding="ascii", newline="\n") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, local_path)
            except FileExistsError as exc:
                raise ValueError(
                    f"artefato JSON imutável já existe em {uri!r}"
                ) from exc
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        raise ValueError(f"artefato JSON imutável já existe em {uri!r}")
    stream = fs.create(path, False)
    try:
        stream.write(bytearray(text.encode("ascii")))
    finally:
        stream.close()


def _read_json_artifact(spark: SparkSession, uri: str) -> dict[str, Any]:
    local_path = _local_artifact_path(uri)
    if local_path is not None:
        try:
            with open(local_path, "r", encoding="utf-8") as handle:
                parsed = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"artefato JSON inválido em {uri!r}: {exc}") from exc
    else:
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(path) or fs.getFileStatus(path).isDirectory():
            raise ValueError(f"esperado um objeto JSON em {uri!r}")
        stream = fs.open(path)
        try:
            text = jvm.org.apache.commons.io.IOUtils.toString(
                stream, jvm.java.nio.charset.StandardCharsets.UTF_8
            )
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"artefato JSON inválido em {uri!r}: {exc}") from exc
        finally:
            stream.close()
    if not isinstance(parsed, dict):
        raise ValueError(f"artefato JSON em {uri!r} precisa ser um objeto")
    return parsed


def _plan_id(plan_without_id: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(plan_without_id).encode("ascii")).hexdigest()


def _selected_lote_snapshot_uri(plan_uri: str, snapshot_id: str) -> str:
    return f"{plan_uri.rstrip('/')}.selected-lote/{snapshot_id}"


def _nonnegative_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{context} precisa ser inteiro >= 0")
    return value


def _validate_selected_lote_descriptor(
    descriptor: Mapping[str, Any],
    *,
    expected_tables: Set[str],
    plan_uri: Optional[str] = None,
) -> dict[str, Any]:
    if not isinstance(descriptor, Mapping):
        raise ValueError("selected_lote precisa ser um descriptor")
    expected_keys = {
        "artifact_type", "schema_version", "snapshot_id", "snapshot_uri",
        "table_set", "tables", "selective_missing",
    }
    if set(descriptor) != expected_keys:
        raise ValueError(
            "selected_lote possui campos inválidos: "
            f"esperado={sorted(expected_keys)}, recebido={sorted(descriptor)}"
        )
    if descriptor.get("artifact_type") != ENGORDA_SELECTED_LOTE_ARTIFACT:
        raise ValueError("selected_lote possui artifact_type inválido")
    if descriptor.get("schema_version") != ENGORDA_SELECTED_LOTE_SCHEMA_VERSION:
        raise ValueError("selected_lote possui schema_version incompatível")
    snapshot_id = descriptor.get("snapshot_id")
    try:
        parsed_snapshot_id = uuid.UUID(str(snapshot_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("selected_lote possui snapshot_id UUID inválido") from exc
    if str(parsed_snapshot_id) != snapshot_id:
        raise ValueError("selected_lote snapshot_id precisa ser UUID canônico")
    snapshot_uri = descriptor.get("snapshot_uri")
    if not isinstance(snapshot_uri, str) or not snapshot_uri:
        raise ValueError("selected_lote possui snapshot_uri inválido")
    if plan_uri is not None:
        expected_uri = _selected_lote_snapshot_uri(plan_uri, snapshot_id)
        if snapshot_uri != expected_uri:
            raise ValueError(
                "selected_lote snapshot_uri não deriva do plan_uri e snapshot_id"
            )

    table_set = descriptor.get("table_set")
    if (not isinstance(table_set, list)
            or any(not isinstance(table, str) or not table for table in table_set)
            or table_set != sorted(set(table_set))):
        raise ValueError("selected_lote table_set precisa ser lista exata e ordenada")
    if set(table_set) != expected_tables:
        raise ValueError(
            "selected_lote table_set diverge: "
            f"esperado={sorted(expected_tables)}, recebido={table_set}"
        )
    if TABELA_RAIZ not in expected_tables:
        raise ValueError(f"selected_lote table_set não contém {TABELA_RAIZ}")
    tables = descriptor.get("tables")
    if not isinstance(tables, Mapping) or set(tables) != expected_tables:
        raise ValueError("selected_lote tables diverge do table_set")
    for table in table_set:
        entry = tables[table]
        if not isinstance(entry, Mapping) or set(entry) != {"path", "row_count", "schema"}:
            raise ValueError(f"selected_lote tables.{table} inválido")
        if entry.get("path") != f"{snapshot_uri}/tables/{table}":
            raise ValueError(f"selected_lote tables.{table}.path inválido")
        _nonnegative_int(entry.get("row_count"), f"selected_lote tables.{table}.row_count")
        schema = entry.get("schema")
        if not isinstance(schema, dict):
            raise ValueError(f"selected_lote tables.{table}.schema inválido")
        try:
            parsed_schema = T.StructType.fromJson(schema)
        except (TypeError, ValueError, KeyError) as exc:
            raise ValueError(f"selected_lote tables.{table}.schema inválido") from exc
        if parsed_schema.jsonValue() != schema:
            raise ValueError(f"selected_lote tables.{table}.schema não é canônico")

    selective = descriptor.get("selective_missing")
    if (not isinstance(selective, Mapping)
            or set(selective) != {"present", "path", "row_count", "schema"}
            or type(selective.get("present")) is not bool):
        raise ValueError("selected_lote selective_missing inválido")
    if selective["present"]:
        if selective.get("path") != f"{snapshot_uri}/selective_missing":
            raise ValueError("selected_lote selective_missing.path inválido")
        _nonnegative_int(
            selective.get("row_count"),
            "selected_lote selective_missing.row_count",
        )
        schema = selective.get("schema")
        if not isinstance(schema, dict):
            raise ValueError("selected_lote selective_missing.schema inválido")
        try:
            parsed_schema = T.StructType.fromJson(schema)
        except (TypeError, ValueError, KeyError) as exc:
            raise ValueError("selected_lote selective_missing.schema inválido") from exc
        if parsed_schema.jsonValue() != schema:
            raise ValueError("selected_lote selective_missing.schema não é canônico")
    elif dict(selective) != {
        "present": False, "path": None, "row_count": 0, "schema": None,
    }:
        raise ValueError("selected_lote selective_missing ausente possui contrato inválido")
    return dict(descriptor)


def _write_selected_lote_datasets(
    descriptor: Mapping[str, Any],
    lotes: Mapping[str, DataFrame],
    selective_missing: Optional[DataFrame],
) -> None:
    table_set = descriptor["table_set"]
    if set(lotes) != set(table_set):
        raise ValueError("lotes para snapshot divergem do table_set")
    for table in table_set:
        lotes[table].write.mode("errorifexists").parquet(
            descriptor["tables"][table]["path"]
        )
    selective = descriptor["selective_missing"]
    if selective["present"]:
        if selective_missing is None:
            raise ValueError("snapshot exige DataFrame selective_missing")
        selective_missing.write.mode("errorifexists").parquet(selective["path"])
    elif selective_missing is not None:
        raise ValueError("snapshot não descreve o DataFrame selective_missing")


def _load_selected_lote_snapshot(
    spark: SparkSession,
    plan_uri: str,
    descriptor: Mapping[str, Any],
    *,
    expected_tables: Set[str],
    selected_num_ifs: Sequence[int],
    selective_keys: frozenset[Tuple[str, str]],
) -> Tuple[Dict[str, DataFrame], Optional[DataFrame], Dict[str, int]]:
    validated = _validate_selected_lote_descriptor(
        descriptor, expected_tables=expected_tables, plan_uri=plan_uri
    )
    lotes: Dict[str, DataFrame] = {}
    for table in validated["table_set"]:
        entry = validated["tables"][table]
        frame = spark.read.parquet(entry["path"])
        if frame.schema.jsonValue() != entry["schema"]:
            raise ValueError(f"selected_lote tables.{table}.schema diverge do Parquet")
        lotes[table] = frame
    counts = _count_final_lotes(lotes)
    for table in validated["table_set"]:
        expected_count = validated["tables"][table]["row_count"]
        if counts[table] != expected_count:
            raise ValueError(
                f"selected_lote tables.{table}.row_count={counts[table]}, "
                f"esperado {expected_count}"
            )

    root = lotes[TABELA_RAIZ]
    if COL_NUM_IF not in root.columns:
        raise ValueError(f"selected_lote root não expõe {COL_NUM_IF}")
    root_ids = sorted(int(row[COL_NUM_IF]) for row in root.select(COL_NUM_IF).collect())
    expected_root_ids = sorted(int(value) for value in selected_num_ifs)
    if root_ids != expected_root_ids:
        raise ValueError(
            f"selected_lote root NUM_IF diverge: {root_ids} != {expected_root_ids}"
        )

    selective = validated["selective_missing"]
    missing: Optional[DataFrame] = None
    if selective["present"]:
        missing = spark.read.parquet(selective["path"])
        if missing.schema.jsonValue() != selective["schema"]:
            raise ValueError("selected_lote selective_missing.schema diverge do Parquet")
        required_columns = ["TABELA", "COLUNA", "VALOR"]
        if missing.columns != required_columns:
            raise ValueError(
                "selected_lote selective_missing possui colunas inválidas; "
                f"esperado={required_columns}, recebido={missing.columns}"
            )
        rows = [tuple(row) for row in missing.select(*required_columns).collect()]
        if len(rows) != selective["row_count"]:
            raise ValueError(
                "selected_lote selective_missing.row_count="
                f"{len(rows)}, esperado {selective['row_count']}"
            )
        if any(value is None for _, _, value in rows):
            raise ValueError(
                "selected_lote selective_missing possui VALOR nulo"
            )
        if len(set(rows)) != len(rows):
            raise ValueError("selected_lote selective_missing possui duplicatas")
        invalid_pairs = sorted({(table, column) for table, column, _ in rows}
                               - set(selective_keys))
        if invalid_pairs:
            raise ValueError(
                "selected_lote selective_missing diverge da allowlist: "
                f"{invalid_pairs}"
            )
    return lotes, missing, counts


def _create_selected_lote_snapshot(
    spark: SparkSession,
    plan_uri: str,
    lotes: Mapping[str, DataFrame],
    selective_missing: Optional[DataFrame],
    *,
    selected_num_ifs: Sequence[int],
    selective_keys: frozenset[Tuple[str, str]],
    lote_counts: Optional[Mapping[str, int]] = None,
) -> dict[str, Any]:
    snapshot_id = str(uuid.uuid4())
    snapshot_uri = _selected_lote_snapshot_uri(plan_uri, snapshot_id)
    counts = (
        {table: int(count) for table, count in lote_counts.items()}
        if lote_counts is not None else _count_final_lotes(lotes)
    )
    descriptor: dict[str, Any] = {
        "artifact_type": ENGORDA_SELECTED_LOTE_ARTIFACT,
        "schema_version": ENGORDA_SELECTED_LOTE_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "snapshot_uri": snapshot_uri,
        "table_set": sorted(lotes),
        "tables": {
            table: {
                "path": f"{snapshot_uri}/tables/{table}",
                "row_count": counts[table],
                "schema": lotes[table].schema.jsonValue(),
            }
            for table in sorted(lotes)
        },
        "selective_missing": (
            {
                "present": True,
                "path": f"{snapshot_uri}/selective_missing",
                "row_count": selective_missing.count(),
                "schema": selective_missing.schema.jsonValue(),
            }
            if selective_missing is not None else {
                "present": False, "path": None, "row_count": 0, "schema": None,
            }
        ),
    }
    _validate_selected_lote_descriptor(
        descriptor, expected_tables=set(lotes), plan_uri=plan_uri
    )
    _write_selected_lote_datasets(descriptor, lotes, selective_missing)
    for table in descriptor["table_set"]:
        persisted_schema = spark.read.parquet(
            descriptor["tables"][table]["path"]
        ).schema
        if persisted_schema.simpleString() != lotes[table].schema.simpleString():
            raise ValueError(
                f"selected_lote tables.{table}.schema mudou na persistência"
            )
        descriptor["tables"][table]["schema"] = persisted_schema.jsonValue()
    if selective_missing is not None:
        persisted_schema = spark.read.parquet(
            descriptor["selective_missing"]["path"]
        ).schema
        if persisted_schema.simpleString() != selective_missing.schema.simpleString():
            raise ValueError(
                "selected_lote selective_missing.schema mudou na persistência"
            )
        descriptor["selective_missing"]["schema"] = persisted_schema.jsonValue()
    _load_selected_lote_snapshot(
        spark,
        plan_uri,
        descriptor,
        expected_tables=set(lotes),
        selected_num_ifs=selected_num_ifs,
        selective_keys=selective_keys,
    )
    return descriptor


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
                logger.warning("Ciclo de FK entre tabelas sintetizáveis; quebrando "
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
    """Timestamp único do run; preserva frações para o timestamp textual."""
    if value is None:
        return datetime.now()
    if isinstance(value, datetime):
        return value
    raise TypeError("engorda_ts deve ser datetime ou None.")


def _tipo_data_engordavel(dt: T.DataType) -> bool:
    """Só data/hora/string recebem literal de data. Um DAT_* numérico (schema
    inesperado) seria NULADO pelo cast — melhor pular com aviso."""
    return isinstance(dt, (T.DateType, T.TimestampType, T.StringType))


def _timestamp_literal_for_type(value: datetime, dt: T.DataType):
    """Literal de timestamp respeitando o tipo físico da coluna."""
    if isinstance(dt, T.StringType):
        return F.date_format(F.lit(value).cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
    # As colunas Oracle DATE guardam segundos; a fração só é usada pelo VAL_TIME_*.
    return F.lit(value.replace(microsecond=0)).cast(dt)


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
                          controle_operacional_date: date,
                          prazo_vencimento_dias: Optional[int] = None,
                          ) -> Tuple[DataFrame, List[str]]:
    """Aplica as regras de DATA do engorda aos sintéticos de UMA tabela.

    Devolve (df, colunas efetivamente reescritas). Tolerante por construção: a
    regra de uma coluna ausente no schema é no-op — as tabelas do fecho têm
    subconjuntos bem diferentes dessas colunas.

    O prazo de DAT_VENCIMENTO é calculado ANTES de DAT_EMISSAO ser
    sobrescrita; senão o "prazo original da linha sintetizada" viraria
    (vencimento_original - data_do_run), que é outro número.
    """
    tabela = table_path_name(tabela).upper()
    tipos = {f.name: f.dataType for f in df.schema.fields}
    aplicadas: List[str] = []

    def _tipo_ok(col: str) -> bool:
        if _tipo_data_engordavel(tipos[col]):
            return True
        logger.warning("%s.%s: tipo %s não é data/hora/string; regra de engorda "
                       "IGNORADA (a coluna mantém o valor sintetizado).",
                       tabela, col, tipos[col].simpleString())
        return False

    is_instrumento = tabela == TABELA_RAIZ
    tem_venc = (is_instrumento
                and ENGORDA_COL_DAT_VENCIMENTO in tipos
                and _tipo_ok(ENGORDA_COL_DAT_VENCIMENTO))
    tem_emissao = (is_instrumento
                   and ENGORDA_COL_DAT_EMISSAO in tipos
                   and _tipo_ok(ENGORDA_COL_DAT_EMISSAO))

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
    timestamp_cols = (*ENGORDA_COLS_TIMESTAMP,
                      *ENGORDA_TIMESTAMP_COLS_BY_TABLE.get(tabela, ()))
    for col in timestamp_cols:
        if col not in tipos or not _tipo_ok(col):
            continue
        df = df.withColumn(col, _timestamp_literal_for_type(engorda_ts, tipos[col]))
        aplicadas.append(col)

    # Timestamp legado textual de OPERACAO: yyyyMMddHHmmssSS (centésimos).
    formatted_ts = (engorda_ts.strftime("%Y%m%d%H%M%S")
                    + f"{engorda_ts.microsecond // 10_000:02d}")
    for col in ENGORDA_FORMATTED_TIMESTAMP_COLS_BY_TABLE.get(tabela, ()):
        if col not in tipos or not _tipo_ok(col):
            continue
        df = df.withColumn(col, F.lit(formatted_ts).cast(tipos[col]))
        aplicadas.append(col)

    # 3) Datas do controle operacional, à meia-noite.
    for col in ENGORDA_OPERATIONAL_DATE_COLS_BY_TABLE.get(tabela, ()):
        if col not in tipos or not _tipo_ok(col):
            continue
        df = df.withColumn(
            col, _date_literal_for_type(controle_operacional_date, tipos[col]))
        aplicadas.append(col)

    # 4) DAT_VENCIMENTO = data operacional + prazo original/fixo, sem hora.
    if tem_venc:
        venc_expr = F.expr(
            f"date_add(DATE '{controle_operacional_date.isoformat()}', "
            f"CAST({ENGORDA_PRAZO_TMP_COL} AS INT))")
        df = df.withColumn(
            ENGORDA_COL_DAT_VENCIMENTO,
            _date_expression_for_type(venc_expr, tipos[ENGORDA_COL_DAT_VENCIMENTO]),
        ).drop(ENGORDA_PRAZO_TMP_COL)
        aplicadas.append(ENGORDA_COL_DAT_VENCIMENTO)

    # 5) DAT_LIQUIDACAO permanece original; as datas equivalentes a copiam.
    if tabela == "EVENTO" and ENGORDA_EVENT_LIQUIDATION_COL in tipos:
        source = ENGORDA_EVENT_LIQUIDATION_COL
        if _tipo_ok(source):
            for col in ENGORDA_EVENT_DERIVED_COLS:
                if col not in tipos or not _tipo_ok(col):
                    continue
                df = df.withColumn(col, F.col(source).cast(tipos[col]))
                aplicadas.append(col)

    return df, aplicadas


def ajusta_datas_resgate(
    resultados: Mapping[str, Tuple[DataFrame, int]],
    lote_instrumentos: DataFrame,
    mapa_num_if: DataFrame,
    tabelas: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Tuple[DataFrame, int]], List[str]]:
    """Desloca resgate e cronograma pela mesma diferença aplicada à emissão raiz."""
    updated = dict(resultados)
    required_tables = (TABELA_RAIZ, CONDICAO_IF_TABLE)
    if any(table not in updated for table in required_tables):
        return updated, []

    roots = updated[TABELA_RAIZ][0]
    conditions = updated[CONDICAO_IF_TABLE][0]
    required_columns = {
        TABELA_RAIZ: {COL_NUM_IF, ENGORDA_COL_DAT_EMISSAO},
        CONDICAO_IF_TABLE: {CONDICAO_IF_PK, COL_NUM_IF},
    }
    frames = {TABELA_RAIZ: roots, CONDICAO_IF_TABLE: conditions}
    if any(required_columns[name] - set(frames[name].columns) for name in required_tables):
        return updated, []
    if {f"old_{COL_NUM_IF}", f"new_{COL_NUM_IF}"} - set(mapa_num_if.columns):
        return updated, []
    if {COL_NUM_IF, ENGORDA_COL_DAT_EMISSAO} - set(lote_instrumentos.columns):
        return updated, []

    original_roots = lote_instrumentos.select(
        _norm_key_col(F.col(COL_NUM_IF)).alias("__old_num_if"),
        F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)).alias("__old_emission"),
    ).dropDuplicates(["__old_num_if"])
    root_map = mapa_num_if.select(
        _norm_key_col(F.col(f"old_{COL_NUM_IF}")).alias("__old_num_if"),
        _norm_key_col(F.col(f"new_{COL_NUM_IF}")).alias("__new_num_if"),
    ).dropDuplicates(["__old_num_if", "__new_num_if"])
    shifted_roots = roots.select(
        _norm_key_col(F.col(COL_NUM_IF)).alias("__new_num_if"),
        F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)).alias("__new_emission"),
    ).dropDuplicates(["__new_num_if"])
    shifts = (
        root_map.join(original_roots, "__old_num_if", "inner")
        .join(shifted_roots, "__new_num_if", "inner")
        .where(F.col("__old_emission").isNotNull() & F.col("__new_emission").isNotNull())
        .select(
            "__new_num_if",
            F.datediff("__new_emission", "__old_emission").alias("__shift_days"),
        )
        .dropDuplicates(["__new_num_if"])
    )
    condition_shifts = (
        conditions.select(
            _norm_key_col(F.col(CONDICAO_IF_PK)).alias("__condition_key"),
            _norm_key_col(F.col(COL_NUM_IF)).alias("__new_num_if"),
        )
        .join(shifts, "__new_num_if", "inner")
        .select("__condition_key", "__shift_days")
        .dropDuplicates(["__condition_key"])
    )

    selected = {
        table_path_name(table).upper()
        for table in (tabelas or ("RESGATE", "CONDICAO_RESGATE"))
    }
    changed: List[str] = []
    for table in ("CONDICAO_RESGATE", "RESGATE"):
        if table not in selected or table not in updated:
            continue
        frame, source_rows = updated[table]
        if CONDICAO_IF_PK not in frame.columns or "DAT_RESGATE" not in frame.columns:
            continue
        date_type = frame.schema["DAT_RESGATE"].dataType
        source = frame.withColumn(
            "__condition_key", _norm_key_col(F.col(CONDICAO_IF_PK))
        ).alias("source")
        context = condition_shifts.alias("context")
        joined = source.join(F.broadcast(context), "__condition_key", "left")
        parsed = F.to_date(F.col("source.DAT_RESGATE"))
        shifted = F.date_add(parsed, F.col("context.__shift_days"))
        shifted_for_type = _date_expression_for_type(shifted, date_type)
        adjusted = joined.select(*[
            F.when(
                parsed.isNotNull() & F.col("context.__shift_days").isNotNull(),
                shifted_for_type,
            ).otherwise(F.col(f"source.{column}")).alias(column)
            if column == "DAT_RESGATE"
            else F.col(f"source.{column}").alias(column)
            for column in frame.columns
        ])
        updated[table] = (adjusted, source_rows)
        changed.append(table)

    return updated, changed


def ajusta_datas_condicao_if(
    clones: DataFrame,
    lote_instrumentos: DataFrame,
    raiz_sintetica: DataFrame,
    mapa_num_if: DataFrame,
) -> Tuple[DataFrame, List[str]]:
    """Desloca as datas de vigência de CONDICAO_IF pelo mesmo Δ da emissão.

    Mesma mecânica de ajusta_datas_resgate, porém direta: CONDICAO_IF já carrega
    NUM_IF, então não é preciso a ponte por NUM_CONDICAO_IF.

    Tolerante por construção: se faltar qualquer insumo (coluna ausente, mapa
    incompleto), devolve o DataFrame intacto e lista vazia — nunca corrompe."""
    cols_alvo = [c for c in ENGORDA_CONDICAO_IF_SHIFT_COLS if c in clones.columns]
    if not cols_alvo or COL_NUM_IF not in clones.columns:
        return clones, []
    if {f"old_{COL_NUM_IF}", f"new_{COL_NUM_IF}"} - set(mapa_num_if.columns):
        return clones, []
    if {COL_NUM_IF, ENGORDA_COL_DAT_EMISSAO} - set(lote_instrumentos.columns):
        return clones, []
    if {COL_NUM_IF, ENGORDA_COL_DAT_EMISSAO} - set(raiz_sintetica.columns):
        return clones, []
    colisao = [c for c in (CONDICAO_IF_SHIFT_KEY_COL, CONDICAO_IF_SHIFT_DAYS_COL)
               if c in clones.columns]
    if colisao:
        raise ValueError(
            f"{CONDICAO_IF_TABLE}: colisão de coluna temporária {colisao}.")

    tipos = {c: clones.schema[c].dataType for c in cols_alvo}
    nao_datavel = [c for c in cols_alvo if not _tipo_data_engordavel(tipos[c])]
    if nao_datavel:
        logger.warning("%s: coluna(s) %s não são data/hora/string; deslocamento "
                       "IGNORADO nelas.", CONDICAO_IF_TABLE, nao_datavel)
        cols_alvo = [c for c in cols_alvo if c not in nao_datavel]
        if not cols_alvo:
            return clones, []

    original_roots = lote_instrumentos.select(
        _norm_key_col(F.col(COL_NUM_IF)).alias("__old_num_if"),
        F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)).alias("__old_emission"),
    ).dropDuplicates(["__old_num_if"])
    root_map = mapa_num_if.select(
        _norm_key_col(F.col(f"old_{COL_NUM_IF}")).alias("__old_num_if"),
        _norm_key_col(F.col(f"new_{COL_NUM_IF}")).alias(CONDICAO_IF_SHIFT_KEY_COL),
    ).dropDuplicates(["__old_num_if", CONDICAO_IF_SHIFT_KEY_COL])
    shifted_roots = raiz_sintetica.select(
        _norm_key_col(F.col(COL_NUM_IF)).alias(CONDICAO_IF_SHIFT_KEY_COL),
        F.to_date(F.col(ENGORDA_COL_DAT_EMISSAO)).alias("__new_emission"),
    ).dropDuplicates([CONDICAO_IF_SHIFT_KEY_COL])
    shifts = (
        root_map.join(original_roots, "__old_num_if", "inner")
        .join(shifted_roots, CONDICAO_IF_SHIFT_KEY_COL, "inner")
        .where(F.col("__old_emission").isNotNull()
               & F.col("__new_emission").isNotNull())
        .select(
            CONDICAO_IF_SHIFT_KEY_COL,
            F.datediff("__new_emission", "__old_emission").alias(
                CONDICAO_IF_SHIFT_DAYS_COL),
        )
        .dropDuplicates([CONDICAO_IF_SHIFT_KEY_COL])
    )

    source = clones.withColumn(
        CONDICAO_IF_SHIFT_KEY_COL, _norm_key_col(F.col(COL_NUM_IF))
    ).alias("source")
    context = shifts.alias("context")
    joined = source.join(F.broadcast(context), CONDICAO_IF_SHIFT_KEY_COL, "left")

    projecao = []
    for column in clones.columns:
        if column in cols_alvo:
            parsed = F.to_date(F.col(f"source.{column}"))
            deslocada = _date_expression_for_type(
                F.date_add(parsed, F.col(f"context.{CONDICAO_IF_SHIFT_DAYS_COL}")),
                tipos[column],
            )
            projecao.append(
                F.when(
                    parsed.isNotNull()
                    & F.col(f"context.{CONDICAO_IF_SHIFT_DAYS_COL}").isNotNull(),
                    deslocada,
                ).otherwise(F.col(f"source.{column}")).alias(column)
            )
        else:
            projecao.append(F.col(f"source.{column}").alias(column))
    logger.info("%s: deslocamento Δ-emissão aplicado em %s.",
                CONDICAO_IF_TABLE, cols_alvo)
    return joined.select(*projecao), cols_alvo


# ---------------------------------------------------------------------------
# Plano de sintetização: classificação de tabelas/PKs/FKs a partir do spec + dos
# schemas Parquet. Nenhuma decisão implícita: o que não tem regra ABORTA.
# ---------------------------------------------------------------------------
@dataclass
class FkRemap:
    """FK desta tabela para a PK de um pai sintetizado (grupo de constraint do
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


ROOT_PROVENANCE_COL = "__root_num_if"


@dataclass
class TargetInstrumentSelection:
    values: List[int]
    missing_keys: Optional[DataFrame] = None
    lotes: Optional[Dict[str, DataFrame]] = None


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
                pk_passo: int = 1,
                source_frames: Optional[Mapping[str, DataFrame]] = None,
                frozen_table_plans: Optional[Mapping[str, Any]] = None,
                ) -> Dict[str, PlanoTabela]:
    """Classifica cada tabela sintetizável e define a regra de PK. Aborta (com
    lista completa) se alguma tabela ficar sem regra — nada de chute.

    O CONJUNTO de tabelas engordadas é, por definição, {tabelas do spec} menos
    {static}. Trocar o --specs troca o conjunto, sem tocar no código.

    Duas folgas independentes na PK nova (ambas só valem para OFFSET_PROPRIO;
    VIA_PAI apenas segue o mapeamento do pai):
      pk_band  -> distância entre o max REAL da tabela e a primeira PK nova;
      pk_passo -> distância entre duas PKs novas consecutivas (default 1,
                  contíguo). Serve para deixar buracos reserváveis entre os
                  registros sintetizados.
    """
    if pk_passo < 1:
        raise ValueError("pk_passo deve ser >= 1.")
    estaticas = {t for t, cfg in spec.items() if cfg.get("static")} | estaticas_extra
    clonaveis = {t for t in spec if t not in estaticas}
    if TABELA_RAIZ not in clonaveis:
        raise ValueError(f"{TABELA_RAIZ} precisa ser sintetizável (não-static) no spec.")
    frozen = source_frames is not None or frozen_table_plans is not None
    if frozen and (source_frames is None or frozen_table_plans is None):
        raise ValueError(
            "reconstrução congelada exige source_frames e frozen_table_plans"
        )
    if frozen and (set(source_frames) != clonaveis
                   or set(frozen_table_plans) != clonaveis):
        raise ValueError(
            "snapshot/frozen table_set diverge do conjunto sintetizável"
        )

    source_schemas: Dict[str, T.StructType] = {}

    def source_schema(table: str) -> T.StructType:
        if table not in source_schemas:
            source_schemas[table] = (
                source_frames[table].schema
                if source_frames is not None
                else read_parquet(spark, raw_path(config, table)).schema
            )
        return source_schemas[table]

    planos: Dict[str, PlanoTabela] = {}
    problemas: List[str] = []

    for t in sorted(clonaveis):
        pk = tuple(spec[t].get("pk_cols") or [])
        if not pk:
            problemas.append(f"{t}: sem pk_cols no spec")
            continue
        fks = _fks_para_pais_clonados(spec, t, clonaveis)
        plano = PlanoTabela(name=t, pk_cols=pk, fks_remap=fks)

        # Regra da PK: componente coberto por FK de pai sintetizado -> segue o pai;
        # senão, surrogate única/numérica fora de FK -> offset próprio.
        cols_fk_qualquer = {c for fk in _fk_list(spec[t])
                            for c in (fk.get("columns") or [])}
        cols_fk_remap = {c for fk in fks for c in fk.columns}
        if any(c in cols_fk_remap for c in pk):
            plano.pk_regra = "VIA_PAI"
        elif len(pk) == 1 and pk[0] not in cols_fk_qualquer:
            try:
                dt = source_schema(t)[pk[0]].dataType
            except Exception as exc:
                problemas.append(f"{t}: não li o schema Parquet ({exc})")
                continue
            if not _is_numeric_type(dt):
                problemas.append(
                    f"{t}: PK {pk[0]} não numérica ({dt.simpleString()}) e sem "
                    "FK de pai sintetizado — sem regra de remap")
                continue
            plano.pk_regra = "OFFSET_PROPRIO"
        else:
            problemas.append(
                f"{t}: PK {list(pk)} sem componente de FK de pai sintetizado e não "
                "elegível a offset (composta e/ou participa de FK para pai não "
                "sintetizado) — sem regra de remap")
            continue

        # Vínculo principal para o pertencimento: exigido de toda tabela
        # sintetizável exceto a raiz.
        if t != TABELA_RAIZ and not any(fk.principal for fk in fks):
            problemas.append(
                f"{t}: nenhuma FK de VÍNCULO PRINCIPAL (colunas com mesmo nome "
                "da PK de um pai sintetizado) — não sei ligar as linhas ao "
                "instrumento. Marque static (--tratar-como-static) ou corrija o spec.")
            continue
        planos[t] = plano

    if problemas:
        raise ValueError(
            "Tabela(s) sintetizável(is) sem regra segura — resolva antes de rodar "
            "(--tratar-como-static as exclui da sintetização):\n  - "
            + "\n  - ".join(problemas))

    if frozen_table_plans is not None:
        for table, plano in sorted(planos.items()):
            frozen_table = frozen_table_plans[table]
            frozen_pk = frozen_table.get("pk") if isinstance(frozen_table, Mapping) else None
            if not isinstance(frozen_pk, Mapping):
                raise ValueError(f"plano congelado {table}.pk inválido")
            if frozen_pk.get("rule") != plano.pk_regra:
                raise ValueError(
                    f"plano congelado {table}.pk.rule diverge da classificação atual"
                )
            step = frozen_pk.get("step")
            if isinstance(step, bool) or not isinstance(step, int) or step < 1:
                raise ValueError(f"plano congelado {table}.pk.step inválido")
            minimum_start = frozen_pk.get("minimum_start")
            if plano.pk_regra == "OFFSET_PROPRIO":
                if isinstance(minimum_start, bool) or not isinstance(minimum_start, int):
                    raise ValueError(
                        f"plano congelado {table}.pk.minimum_start inválido"
                    )
                plano.pk_start = minimum_start
            elif minimum_start is not None:
                raise ValueError(
                    f"plano congelado {table}.pk.minimum_start precisa ser nulo"
                )
            plano.pk_passo = step

    # Início da PK nova para as tabelas OFFSET_PROPRIO (max real do Parquet
    # COMPLETO + band, com clamp de capacidade — padrão compute_pk_maxes).
    for t, plano in sorted(planos.items()):
        if plano.pk_regra != "OFFSET_PROPRIO":
            continue
        pk_col = plano.pk_cols[0]
        if frozen_table_plans is None:
            plano.pk_passo = pk_passo
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
        dt = source_schema(t)[pk_col].dataType
        cap = _pk_capacity_of(dt)
        # O passo multiplica o alcance: a última PK nova é
        # pk_start + (n_clones - 1) * pk_passo.
        alcance = n_clones_estimado * plano.pk_passo
        if cap is not None and plano.pk_start + alcance > cap:
            logger.warning(
                "%s: início %d + ~%d sintético(s) × passo %d pode estourar o "
                "domínio da PK (cap %d). Reduza o lote/K/passo ou trate a "
                "tabela como static.",
                t, plano.pk_start, n_clones_estimado, pk_passo, cap)
        if frozen_table_plans is None:
            logger.info("Plano %s: PK %s OFFSET_PROPRIO a partir de %d "
                        "(max real %d, band %d, passo %d)", t, pk_col,
                        plano.pk_start, true_max, pk_band, pk_passo)
        else:
            logger.info(
                "Plano %s: PK %s OFFSET_PROPRIO congelado a partir de %d "
                "(passo %d)", t, pk_col, plano.pk_start, plano.pk_passo,
            )
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
def _dominio_num_if_produto(spark, config, profile: ProductProfile,
                             query_path: Optional[str] = None) -> DataFrame:
    """Executa a query do produto extraída do catálogo SQL único.

    O SQL deve ser um único SELECT e expor exatamente uma coluna chamada NUM_IF.
    Placeholders {{RAW_TABELA}} são resolvidos para o Parquet RAW correspondente.
    Nenhum filtro de produto é aplicado pelo Python depois dessa consulta — é
    aqui que moram NUM_TIPO_IF, COD_COND_RESGATE, COD_TIPO_ESCALONAMENTO e
    qualquer outro predicado de negócio do produto.
    """
    _validate_product_profile(profile)
    sql_text, resolved_path = _read_num_if_query_text(
        spark, query_path, profile.query_filename, profile.name
    )
    sql = _render_num_if_query(sql_text, config)
    logger.info("Produto %s: executando query de domínio de NUM_IF: %s",
                profile.name, resolved_path)
    queried = spark.sql(sql)

    num_if_columns = [c for c in queried.columns if c.upper() == COL_NUM_IF]
    if len(num_if_columns) != 1:
        raise ValueError(
            "query de NUM_IF deve retornar exatamente uma coluna chamada NUM_IF; "
            f"colunas encontradas: {queried.columns}"
        )
    base = queried.select(F.col(num_if_columns[0]).alias(COL_NUM_IF))
    if base.where(F.col(COL_NUM_IF).isNull()).limit(1).count():
        raise ValueError("query de NUM_IF retornou NUM_IF nulo")
    base = base.dropDuplicates()

    return base

# ---------------------------------------------------------------------------
# Poda de domínio (itens 1, 3 e 4) e duas políticas de anulação: drift integral
# (item 2) e faltantes seletivos para a allowlist nullable.
# ---------------------------------------------------------------------------
def _norm_key_col(col):
    """Normaliza uma chave para string comparável: valor numérico perde o '.0'
    final (a mesma chave pode chegar como decimal numa fonte e int noutra — ver
    harmoniza_tipos_fk_com_pai / os falsos fk_orphan por tipo físico)."""
    return F.regexp_replace(F.trim(col.cast("string")), r"\.0+$", "")


def _fk_key_col(col, data_type: T.DataType):
    text = col.cast("string")
    if isinstance(data_type, T.NumericType):
        text = F.trim(text)
        return F.regexp_replace(
            F.regexp_replace(text, r"(\.\d*?)0+$", "$1"), r"\.$", ""
        )
    return text


def _valida_contrato_nulificacao_seletiva(
    spec: dict, selective_keys: frozenset[Tuple[str, str]]
) -> None:
    """Aborta se a exceção nullable deixar de ser uma FK filha no spec."""
    problemas: List[str] = []
    for tabela, coluna in sorted(selective_keys):
        cfg = spec.get(tabela)
        if cfg is None:
            problemas.append(f"{tabela}.{coluna}: tabela ausente no spec")
            continue
        fk_filha = any(
            fk.get("parent_table") and coluna in (fk.get("columns") or [])
            for fk in _fk_list(cfg)
        )
        if not fk_filha:
            problemas.append(
                f"{tabela}.{coluna}: coluna ausente ou não é FK filha no spec"
            )
        if coluna in _not_null_cols(cfg):
            problemas.append(f"{tabela}.{coluna}: consta em not_null_cols")
        composite = [
            fk for fk in _fk_list(cfg)
            if coluna in (fk.get("columns") or [])
            and len(fk.get("columns") or []) != 1
        ]
        if composite:
            problemas.append(
                f"{tabela}.{coluna}: faltante seletivo em FK composta não é "
                "representável pelo contrato TABELA/COLUNA/VALOR"
            )
    if problemas:
        raise ValueError(
            "Contrato da nulificação seletiva de faltantes divergiu do metadata "
            "Oracle/QAB; corrija a allowlist/spec antes de sintetizar:\n  - "
            + "\n  - ".join(problemas)
        )
    logger.info(
        "Contrato de faltantes seletivos validado (FK filha nullable): %s",
        sorted(selective_keys),
    )


def _pred_faltante_seletivo(selective_keys: frozenset[Tuple[str, str]]):
    pred = F.lit(False)
    for tabela, coluna in sorted(selective_keys):
        pred = pred | (
            (F.col("TABELA") == F.lit(tabela))
            & (F.col("COLUNA") == F.lit(coluna))
        )
    return pred


def _subtipos_clonaveis(spec: dict,
                        policy: SubtypePolicy) -> List[Tuple[str, str]]:
    """Pares (tipo, tabela-subtipo) que a sintetização realmente produz.

    Loga os tipos SEM tabela-subtipo no spec: num produto novo, um subtipo fora
    do fecho faz toda CONDICAO_IF daquele tipo virar dangling e derruba os
    NUM_IF correspondentes na poda. Sem este aviso a causa fica invisível e o
    sintoma aparece só como "domínio válido menor que N"."""
    disponiveis = [
        (str(tipo), tabela)
        for tipo, tabela in policy.subtype_by_type
        if tabela in spec and not spec[tabela].get("static")
    ]
    ausentes = sorted(
        f"{tipo}->{tabela}"
        for tipo, tabela in policy.subtype_by_type
        if tabela not in spec or spec[tabela].get("static")
    )
    if ausentes:
        logger.warning(
            "poda subtipo: tipo(s) sem tabela-subtipo sintetizável no spec: %s. "
            "Toda %s desses tipos será tratada como dangling e os NUM_IF "
            "correspondentes saem do domínio. Se o produto usa esses tipos, "
            "inclua as tabelas no --specs.",
            ausentes, policy.condition_table)
    return disponiveis


def _num_if_inconsistentes_subtipo(spark, config, spec, dominio: DataFrame,
                                    policy: SubtypePolicy) -> DataFrame:
    """NUM_IF do domínio cujo sintético teria ao menos UMA CONDICAO_IF ativa sem a
    respectiva linha-subtipo sintetizável — os dangling da Cat 1 (item 1). Base da
    poda: excluídos do sorteio, o lote nasce sem ClassCastException.

    A presença é comparada pelo par (COD_TIPO_CONDICAO_IF, NUM_CONDICAO_IF),
    portanto uma PK existente na tabela do tipo errado continua dangling."""
    cond_source = _read_source(spark, config, policy.condition_table)
    required = {
        COL_NUM_IF, policy.condition_pk, policy.condition_type_column,
        policy.active_column,
    }
    missing = sorted(required - set(cond_source.columns))
    if missing:
        raise ValueError(
            f"{policy.condition_table}: coluna(s) obrigatória(s) ausente(s) "
            f"para poda de subtipo: {missing}"
        )
    cond = (cond_source.where(F.col(policy.active_column).isNull())
            .select(F.col(COL_NUM_IF).alias(COL_NUM_IF),
                    _norm_key_col(F.col(policy.condition_pk)).alias("__nci"),
                    _norm_key_col(F.col(policy.condition_type_column)).alias(
                        "__tipo"))
            .join(dominio.select(COL_NUM_IF), on=COL_NUM_IF, how="left_semi"))
    presente = None
    for tipo, s in _subtipos_clonaveis(spec, policy):
        try:
            sdf = _read_source(spark, config, s)
        except Exception as exc:  # fonte ausente: trata como sem chaves (conservador)
            logger.warning("poda subtipo: não li a fonte de %s (%s); condições "
                           "desse tipo entram como dangling.", s, exc)
            continue
        if policy.condition_pk not in sdf.columns:
            logger.warning("poda subtipo: %s sem coluna %s; ignorada.",
                           s, policy.condition_pk)
            continue
        piece = sdf.select(
            _norm_key_col(F.col(policy.condition_pk)).alias("__nci"),
            F.lit(tipo).alias("__tipo"),
        )
        presente = piece if presente is None else presente.unionByName(piece)
    if presente is None:
        # Nenhuma tabela-subtipo sintetizável: toda condição concreta seria dangling.
        return cond.select(COL_NUM_IF).dropDuplicates()
    dangling = cond.join(
        presente.dropDuplicates(), on=["__nci", "__tipo"], how="left_anti"
    )
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
                     F.col("VALOR").cast("string").alias("VALOR")).dropDuplicates()


def _num_if_excluidos_por_faltantes(spark, config, spec, faltantes: DataFrame,
                                    dominio: DataFrame,
                                    selective_keys: frozenset[Tuple[str, str]]) -> DataFrame:
    """NUM_IF do domínio a podar (itens 3/4): instrumentos cujo cluster referencia
    uma chave inexistente no destino. Só tabelas COM coluna NUM_IF (o instrumento
    é alcançável direto) — ex.: CARTEIRA_COMITENTE carrega NUM_ID_ENTIDADE
    (comitente) e NUM_CONTA (conta) por NUM_IF. Tabela sem NUM_IF (ex.:
    ESPECIFICACAO_COMITENTE) é coberta transitivamente: o mesmo comitente sai do
    lote quando o instrumento é podado via CARTEIRA_COMITENTE."""
    # A allowlist nullable tem outra política: preserva o instrumento e anula
    # apenas sintéticos cujo valor aparece em faltantes.
    faltantes_poda = faltantes.where(~_pred_faltante_seletivo(selective_keys))
    pares = [(r["TABELA"], r["COLUNA"]) for r in
             faltantes_poda.select("TABELA", "COLUNA").dropDuplicates().collect()]
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
        data_type = src.schema[col].dataType
        miss = (faltantes_poda.where((F.col("TABELA") == F.lit(tab))
                                      & (F.col("COLUNA") == F.lit(col)))
                .select(_fk_key_col(F.col("VALOR"), data_type).alias("__v"))
                .dropDuplicates())
        hit = (src.select(F.col(COL_NUM_IF).alias(COL_NUM_IF),
                          _fk_key_col(F.col(col), data_type).alias("__v"))
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


def aplica_nulificacao_faltantes(
    df: DataFrame,
    tabela: str,
    faltantes: Optional[DataFrame],
    selective_keys: frozenset[Tuple[str, str]],
) -> Tuple[DataFrame, List[str], Dict[str, Dict[str, int]]]:
    """Anula somente sintéticos casados com faltantes da allowlist nullable.

    As chaves ficam distribuídas: um broadcast left join com lado direito
    deduplicado marca os matches sem coletar valores no driver nem multiplicar
    linhas. Devolve também colunas efetivamente alteradas e contagens separadas.
    """
    if faltantes is None:
        return df, [], {}

    out = df
    anuladas: List[str] = []
    contagens: Dict[str, Dict[str, int]] = {}
    for tab, coluna in sorted(selective_keys):
        if tab != tabela:
            continue
        if coluna not in out.columns:
            raise ValueError(
                f"{tab}.{coluna}: coluna allowlisted não existe no schema dos sintéticos."
            )
        tipo = out.schema[coluna].dataType
        chaves = (
            faltantes.where(
                (F.col("TABELA") == F.lit(tab))
                & (F.col("COLUNA") == F.lit(coluna))
            )
            .select(_fk_key_col(F.col("VALOR"), tipo).alias(FALTANTES_SELECTIVE_KEY_COL))
            .where(F.col(FALTANTES_SELECTIVE_KEY_COL).isNotNull())
            .dropDuplicates()
        )
        n_chaves = chaves.count()
        if n_chaves == 0:
            logger.info(
                "faltantes seletivos: %s.%s -> 0 chave(s) distinta(s), 0 sintético(s) casado(s).",
                tab, coluna,
            )
            continue
        colisao = [
            c for c in (FALTANTES_SELECTIVE_KEY_COL, FALTANTES_SELECTIVE_MARKER_COL)
            if c in out.columns
        ]
        if colisao:
            raise ValueError(f"{tab}: colisão de coluna temporária {colisao}.")

        marcadores = chaves.withColumn(FALTANTES_SELECTIVE_MARKER_COL, F.lit(True))
        joined = (
            out.withColumn(
                FALTANTES_SELECTIVE_KEY_COL, _fk_key_col(F.col(coluna), tipo)
            )
            .join(F.broadcast(marcadores), on=FALTANTES_SELECTIVE_KEY_COL, how="left")
        )
        n_casados = joined.where(F.col(FALTANTES_SELECTIVE_MARKER_COL)).count()
        out = joined.select(
            *[
                F.when(
                    F.col(FALTANTES_SELECTIVE_MARKER_COL), F.lit(None).cast(tipo)
                ).otherwise(F.col(c)).alias(c)
                if c == coluna else F.col(c)
                for c in out.columns
            ]
        )
        logger.info(
            "faltantes seletivos: %s.%s -> %d chave(s) distinta(s) listada(s), "
            "%d linha(s) sintética(s) casada(s).",
            tab, coluna, n_chaves, n_casados,
        )
        contagens[coluna] = {
            "chaves_distintas_listadas": n_chaves,
            "linhas_clone_casadas": n_casados,
        }
        if n_casados:
            anuladas.append(coluna)
    return out, anuladas, contagens


def _colunas_anuladas_resumo(*grupos: Sequence[str]) -> List[str]:
    """Une políticas de anulação em ordem, sem repetir nomes no resumo."""
    return list(dict.fromkeys(coluna for grupo in grupos for coluna in grupo))


def _merge_nullification_mappings(
    base: Mapping[str, Sequence[str]],
    extra: Optional[Mapping[str, Sequence[str]]],
) -> Dict[str, Tuple[str, ...]]:
    """Soma extras ao perfil; a API nunca remove correções obrigatórias."""
    merged: Dict[str, List[str]] = {}
    for mapping in (base, extra or {}):
        if not isinstance(mapping, Mapping):
            raise TypeError("anular_cols precisa ser um mapping TABELA -> colunas")
        for raw_table, raw_columns in mapping.items():
            if not isinstance(raw_table, str) or not raw_table.strip():
                raise ValueError("anular_cols contém tabela inválida")
            if isinstance(raw_columns, (str, bytes)):
                raise TypeError(
                    f"anular_cols[{raw_table!r}] precisa ser uma sequência de colunas"
                )
            table = table_path_name(raw_table.strip().upper())
            target = merged.setdefault(table, [])
            for raw_column in raw_columns:
                if not isinstance(raw_column, str) or not raw_column.strip():
                    raise ValueError(f"{table}: nome de coluna inválido em anular_cols")
                column = raw_column.strip().upper()
                if column not in target:
                    target.append(column)
    return {table: tuple(columns) for table, columns in merged.items()}


def _dominio_instrumentos_elegiveis(
    spark,
    config,
    spec,
    profile: ProductProfile,
    query_num_if_path: Optional[str] = None,
    faltantes: Optional[DataFrame] = None,
    poda_subtipo: bool = True,
) -> Tuple[DataFrame, DataFrame]:
    fonte = (_dominio_num_if_produto(spark, config, profile, query_num_if_path)
             .select(COL_NUM_IF).dropDuplicates())
    logger.info("Produto %s: domínio de NUM_IF vindo integralmente da query.",
                profile.name)

    # Poda de domínio: junta as exclusões dos itens 1/3/4 e tira do domínio.
    exclusoes: List[Tuple[str, DataFrame]] = []
    subtype_policy = profile.integrity.subtype
    if (poda_subtipo
            and profile.name in PRODUTOS_COM_PODA_SUBTIPO
            and subtype_policy is not None):
        exclusoes.append(("subtipo dangling (Cat 1)",
                          _num_if_inconsistentes_subtipo(
                              spark, config, spec, fonte, subtype_policy)))
    if faltantes is not None:
        exclusoes.append(("chave inexistente no destino (Cat 3/4)",
                          _num_if_excluidos_por_faltantes(spark, config, spec,
                                                          faltantes, fonte,
                                                          profile.integrity.selective_missing_keys)))
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
    return fonte, valido


def seleciona_instrumentos(spark, config, spec, num_ifs: Optional[List[int]],
                           n_instrumentos: Optional[int], seed: int,
                           profile: ProductProfile,
                           query_num_if_path: Optional[str] = None,
                           faltantes: Optional[DataFrame] = None,
                           poda_subtipo: bool = True) -> List:
    """Seleciona NUM_IF no domínio podado; lista explícita nunca é substituída."""
    if (num_ifs is None) == (n_instrumentos is None):
        raise ValueError(
            "informe exatamente uma seleção: num_ifs ou n_instrumentos"
        )
    if num_ifs is not None and not num_ifs:
        raise ValueError("Lista de NUM_IF vazia; informe valores ou use "
                         "--n-instrumentos.")
    fonte, valido = _dominio_instrumentos_elegiveis(
        spark,
        config,
        spec,
        profile,
        query_num_if_path=query_num_if_path,
        faltantes=faltantes,
        poda_subtipo=poda_subtipo,
    )

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
            raise ValueError("NUM_IF(s) não sintetizáveis — " + "; ".join(partes))
        valores = sorted(validos)
    else:
        n = int(n_instrumentos)
        if n < 1:
            raise ValueError("n_instrumentos deve ser >= 1")
        valores = [r[0] for r in valido.orderBy(F.rand(seed)).limit(n).collect()]
        if len(valores) < n:
            raise ValueError(
                f"Domínio VÁLIDO após a poda (subtipo/destino) tem só "
                f"{len(valores)} instrumento(s); pedi {n}. Afrouxe os filtros "
                "(--sem-poda-subtipo / menos faltantes), reduza --n-instrumentos "
                "ou verifique os avisos de 'tipo(s) sem tabela-subtipo no spec'.")
        valores = sorted(valores)
    logger.info("Lote: %d instrumento(s) NUM_IF=%s", len(valores),
                valores if len(valores) <= 20 else f"{valores[:20]}... (+{len(valores)-20})")
    return valores


def _target_fk_rejections(
    spark,
    spec: dict,
    planos: Dict[str, PlanoTabela],
    lotes: Dict[str, DataFrame],
    proveniencias: Dict[str, DataFrame],
    materialization_order: Sequence[str],
    selective_keys: frozenset[Tuple[str, str]],
    nullify_columns: Mapping[str, Sequence[str]],
    existing_key_lookup: Callable[
        [str, Tuple[str, ...], List[Tuple[str, ...]], Tuple[bool, ...]],
        Set[Tuple[str, ...]],
    ],
) -> Tuple[Set[int], Dict[int, Set[str]], Optional[DataFrame]]:
    """Resolve every emitted FK against same-root synthetic parents or Oracle."""
    rejected: Set[int] = set()
    reasons: Dict[int, Set[str]] = {}
    selective_rows: List[Tuple[Any, str, str, str]] = []
    selective_root_field: Optional[T.StructField] = None
    selective_value_nullable = False
    order_index = {table: index for index, table in enumerate(materialization_order)}
    edges_by_child: Dict[str, List[dict[str, Any]]] = {}
    parent_requirements: Dict[
        str, Dict[Tuple[str, ...], Tuple[T.DataType, ...]]
    ] = {}
    parent_requirement_uses: Dict[Tuple[str, Tuple[str, ...]], int] = {}

    # Validate the complete edge plan before starting any Spark iterator or
    # Oracle probe. This keeps malformed later edges from producing partial work.
    for table in sorted(planos):
        child = lotes[table]
        for fk in _fk_list(spec[table]):
            child_cols = tuple(fk.get("columns") or ())
            parent_table = fk.get("parent_table")
            parent_cols = tuple(fk.get("parent_columns") or ())
            if (not parent_table or not child_cols or not parent_cols
                    or len(child_cols) != len(parent_cols)):
                raise ValueError(
                    f"FK inválida no spec: {table}.{list(child_cols)} -> "
                    f"{parent_table}.{list(parent_cols)}"
                )
            missing_child_cols = sorted(set(child_cols) - set(child.columns))
            if missing_child_cols:
                raise ValueError(
                    f"FK do spec usa coluna(s) ausente(s) em {table}: "
                    f"{missing_child_cols}"
                )
            nullable_nullifications = (
                set(child_cols)
                & set(nullify_columns.get(table, ()))
                - _not_null_cols(spec[table])
            )
            if nullable_nullifications:
                logger.info(
                    "Admissão FK: %s.%s será anulada integralmente; probe pulado.",
                    table,
                    list(child_cols),
                )
                continue

            child_types = tuple(child.schema[column].dataType for column in child_cols)
            numeric_flags = tuple(
                isinstance(data_type, T.NumericType) for data_type in child_types
            )
            remappable = any(
                edge.columns == child_cols
                and edge.parent_table == parent_table
                and edge.parent_columns == parent_cols
                for edge in planos[table].fks_remap
            )
            parent_map_available = (
                parent_table == table
                or order_index.get(parent_table, len(order_index)) < order_index[table]
            )
            internal_requirement = None
            if (remappable and parent_map_available
                    and parent_table in lotes and parent_table in proveniencias):
                parent = lotes[parent_table]
                missing_parent_cols = sorted(set(parent_cols) - set(parent.columns))
                if missing_parent_cols:
                    raise ValueError(
                        f"FK do spec usa coluna(s) pai ausente(s) em {parent_table}: "
                        f"{missing_parent_cols}"
                    )
                parent_types = tuple(
                    parent.schema[column].dataType for column in parent_cols
                )
                parent_requirements.setdefault(parent_table, {})[
                    parent_cols
                ] = parent_types
                internal_requirement = (parent_table, parent_cols)
                parent_requirement_uses[internal_requirement] = (
                    parent_requirement_uses.get(internal_requirement, 0) + 1
                )

            selective_indexes = [
                index for index, column in enumerate(child_cols)
                if (table, column) in selective_keys
            ]
            label = (
                f"FK {table}.{list(child_cols)} -> "
                f"{parent_table}.{list(parent_cols)}"
            )
            edges_by_child.setdefault(table, []).append({
                "child_cols": child_cols,
                "child_types": child_types,
                "parent_table": parent_table,
                "parent_cols": parent_cols,
                "numeric_flags": numeric_flags,
                "selective_indexes": selective_indexes,
                "label": label,
                "internal_requirement": internal_requirement,
                "residual_roots": {},
            })

    # Load each parent lazily on first use and release it after its last child.
    # This keeps one iterator per parent table without retaining the complete
    # multiproduct parent graph for the duration of admission.
    internal_sets: Dict[
        str, Dict[Tuple[str, ...], Set[Tuple[Any, Tuple[str, ...]]]]
    ] = {}

    def ensure_parent_sets(parent_table: str) -> None:
        if parent_table in internal_sets:
            return
        requirements = parent_requirements[parent_table]
        parent = lotes[parent_table]
        parent_pk = list(planos[parent_table].pk_cols)
        wide_columns = list(dict.fromkeys(
            column for columns in requirements for column in columns
        ))
        projected = (
            parent.join(proveniencias[parent_table], parent_pk, "inner")
            .select(
                ROOT_PROVENANCE_COL,
                *[
                    F.col(column).cast("string").alias(column)
                    for column in wide_columns
                ],
            )
        )
        table_sets = {
            columns: set() for columns in requirements
        }
        internal_sets[parent_table] = table_sets
        row_count = 0
        started = time.perf_counter()
        for row in projected.toLocalIterator(prefetchPartitions=False):
            row_count += 1
            root = row[ROOT_PROVENANCE_COL]
            for columns, data_types in requirements.items():
                values = tuple(row[column] for column in columns)
                # Spark equality never matches a tuple with a null component.
                if any(value is None for value in values):
                    continue
                key = tuple(
                    _canon_oracle_key(
                        value, isinstance(data_type, T.NumericType)
                    )
                    for value, data_type in zip(values, data_types)
                )
                table_sets[columns].add((root, key))
        logger.info(
            "PERF FK parent=%s rows=%d internal_distinct_keys=%d "
            "requirements=%d extraction_seconds=%.3f",
            parent_table,
            row_count,
            sum(len(keys) for keys in table_sets.values()),
            len(requirements),
            time.perf_counter() - started,
        )

    # Spark performs the cast once in the wide projection. Driver-side numeric
    # normalization then mirrors _fk_key_col without Python-specific formatting.
    for table in sorted(planos):
        edge_states = edges_by_child.get(table, [])
        if not edge_states:
            continue
        for state in edge_states:
            requirement = state["internal_requirement"]
            if requirement is not None:
                ensure_parent_sets(requirement[0])
        child = lotes[table]
        provenance = proveniencias[table]
        child_pk = list(planos[table].pk_cols)
        wide_columns = list(dict.fromkeys(
            column
            for state in edge_states
            for column in state["child_cols"]
        ))
        projected = (
            child.join(provenance, child_pk, "inner")
            .select(
                ROOT_PROVENANCE_COL,
                *[
                    F.col(column).cast("string").alias(column)
                    for column in wide_columns
                ],
            )
        )
        row_count = 0
        started = time.perf_counter()
        for row in projected.toLocalIterator(prefetchPartitions=False):
            row_count += 1
            root = row[ROOT_PROVENANCE_COL]
            for state in edge_states:
                values = tuple(row[column] for column in state["child_cols"])
                if any(
                    value is None
                    or (isinstance(data_type, T.StringType) and value == "")
                    for value, data_type in zip(values, state["child_types"])
                ):
                    continue
                key = tuple(
                    _canon_oracle_key(value, numeric)
                    for value, numeric in zip(values, state["numeric_flags"])
                )
                requirement = state["internal_requirement"]
                if requirement is not None:
                    parent_table, parent_cols = requirement
                    if (root, key) in internal_sets[parent_table][parent_cols]:
                        continue
                roots = state["residual_roots"].setdefault(key, set())
                roots.add(root)
        extraction_seconds = time.perf_counter() - started
        logger.info(
            "PERF FK table=%s rows=%d residual_distinct_keys=%d edges=%d "
            "extraction_seconds=%.3f",
            table,
            row_count,
            sum(len(state["residual_roots"]) for state in edge_states),
            len(edge_states),
            extraction_seconds,
        )

        for state in edge_states:
            residual_roots = state["residual_roots"]
            keys = sorted(residual_roots)
            missing_keys: Set[Tuple[str, ...]] = set()
            oracle_seconds = 0.0
            lookup_batches = 0
            for offset in range(0, len(keys), 900):
                batch = keys[offset:offset + 900]
                lookup_started = time.perf_counter()
                lookup_batches += 1
                try:
                    existing = existing_key_lookup(
                        state["parent_table"],
                        state["parent_cols"],
                        batch,
                        state["numeric_flags"],
                    )
                finally:
                    oracle_seconds += time.perf_counter() - lookup_started
                missing_keys.update(set(batch) - existing)

            missing_count = len(missing_keys)
            edge_bad_roots: Set[int] = set()
            if state["selective_indexes"]:
                # Composite selective edges are rejected by contract validation.
                index = state["selective_indexes"][0]
                if missing_count:
                    root_field = provenance.schema[ROOT_PROVENANCE_COL]
                    if selective_root_field is None:
                        selective_root_field = root_field
                    elif root_field.nullable and not selective_root_field.nullable:
                        selective_root_field = T.StructField(
                            ROOT_PROVENANCE_COL,
                            selective_root_field.dataType,
                            True,
                            selective_root_field.metadata,
                        )
                    selective_value_nullable = (
                        selective_value_nullable
                        or child.schema[state["child_cols"][index]].nullable
                    )
                for key in sorted(missing_keys):
                    for root in sorted(residual_roots[key]):
                        selective_rows.append((
                            root,
                            table,
                            state["child_cols"][index],
                            key[index],
                        ))
                if missing_count:
                    logger.info(
                        "Admissão FK seletiva: %s, %d chave(s) ausente(s); "
                        "a coluna nullable será anulada.",
                        state["label"],
                        missing_count,
                    )
            else:
                for key in missing_keys:
                    edge_bad_roots.update(
                        int(root) for root in residual_roots[key]
                    )
                rejected.update(edge_bad_roots)
                for root in edge_bad_roots:
                    reasons.setdefault(root, set()).add(state["label"])
                if missing_count:
                    logger.info(
                        "Admissão FK: %s, %d chave(s) ausente(s), "
                        "%d NUM_IF rejeitado(s).",
                        state["label"],
                        missing_count,
                        len(edge_bad_roots),
                    )
            logger.info(
                "PERF FK edge=%s residual_distinct_keys=%d "
                "oracle_lookup_seconds=%.3f oracle_batches=%d oracle_keys=%d",
                state["label"],
                len(keys),
                oracle_seconds,
                lookup_batches,
                len(keys),
            )
            residual_roots.clear()

        for state in edge_states:
            requirement = state["internal_requirement"]
            if requirement is None:
                continue
            remaining = parent_requirement_uses[requirement] - 1
            parent_requirement_uses[requirement] = remaining
            if remaining == 0:
                parent_table, parent_cols = requirement
                table_sets = internal_sets.get(parent_table)
                if table_sets is not None:
                    table_sets.pop(parent_cols, None)
                    if not table_sets:
                        internal_sets.pop(parent_table, None)

    selective_missing = None
    if selective_rows:
        assert selective_root_field is not None
        selective_missing = spark.createDataFrame(
            selective_rows,
            T.StructType([
                T.StructField(
                    ROOT_PROVENANCE_COL,
                    selective_root_field.dataType,
                    selective_root_field.nullable,
                    selective_root_field.metadata,
                ),
                T.StructField("TABELA", T.StringType(), False),
                T.StructField("COLUNA", T.StringType(), False),
                T.StructField("VALOR", T.StringType(), selective_value_nullable),
            ]),
        )
    return rejected, reasons, selective_missing


def seleciona_instrumentos_destino(
    spark,
    config,
    spec: dict,
    num_ifs: Optional[List[int]],
    n_instrumentos: Optional[int],
    seed: int,
    profile: ProductProfile,
    planos: Dict[str, PlanoTabela],
    ordem: List[str],
    max_passadas: int,
    existing_key_lookup: Callable[
        [str, Tuple[str, ...], List[Tuple[str, ...]], Tuple[bool, ...]],
        Set[Tuple[str, ...]],
    ],
    query_num_if_path: Optional[str] = None,
    faltantes: Optional[DataFrame] = None,
    poda_subtipo: bool = True,
    somente_ativos: bool = True,
    nullify_columns: Optional[Mapping[str, Sequence[str]]] = None,
) -> TargetInstrumentSelection:
    """Admite exactly N roots whose complete FK closure is loadable in Oracle."""
    if (num_ifs is None) == (n_instrumentos is None):
        raise ValueError(
            "informe exatamente uma seleção: num_ifs ou n_instrumentos"
        )
    if num_ifs is not None and not num_ifs:
        raise ValueError("Lista de NUM_IF vazia")

    with _perf_timer("domain_query_selection", product=profile.name):
        fonte, valid_domain = _dominio_instrumentos_elegiveis(
            spark,
            config,
            spec,
            profile,
            query_num_if_path=query_num_if_path,
            faltantes=faltantes,
            poda_subtipo=poda_subtipo,
        )
    requested = len(num_ifs) if num_ifs is not None else int(n_instrumentos)
    if requested < 1:
        raise ValueError("n_instrumentos deve ser >= 1")

    if num_ifs is not None:
        requested_df = F.broadcast(
            spark.createDataFrame([(value,) for value in num_ifs], [COL_NUM_IF])
            .select(F.col(COL_NUM_IF).cast(fonte.schema[COL_NUM_IF].dataType))
        )
        in_domain = {
            int(row[0]) for row in
            fonte.join(requested_df, COL_NUM_IF, "left_semi").collect()
        }
        eligible = {
            int(row[0]) for row in
            valid_domain.join(requested_df, COL_NUM_IF, "left_semi").collect()
        }
        outside = [value for value in num_ifs if value not in in_domain]
        pruned = [value for value in num_ifs if value in in_domain and value not in eligible]
        if outside or pruned:
            raise ValueError(
                "NUM_IF(s) não sintetizáveis antes da admissão FK: "
                f"fora_do_domínio={outside}, podados={pruned}"
            )
        candidate_pages = [sorted(eligible)]
    else:
        ranked = valid_domain.select(
            F.col(COL_NUM_IF),
            F.xxhash64(F.lit(seed), F.col(COL_NUM_IF).cast("string")).alias("__rank"),
        )
        candidate_pages = None

    accepted: List[int] = []
    all_reasons: Dict[int, Set[str]] = {}
    selective_missing: Optional[DataFrame] = None
    accepted_lotes: Dict[str, DataFrame] = {}
    cursor: Optional[Tuple[int, int]] = None

    while len(accepted) < requested:
        if candidate_pages is not None:
            candidates = candidate_pages.pop(0) if candidate_pages else []
        else:
            remaining = requested - len(accepted)
            page_size = remaining + max(100, (remaining + 9) // 10)
            page = ranked
            if cursor is not None:
                last_rank, last_num_if = cursor
                page = page.where(
                    (F.col("__rank") > F.lit(last_rank))
                    | (
                        (F.col("__rank") == F.lit(last_rank))
                        & (F.col(COL_NUM_IF) > F.lit(last_num_if))
                    )
                )
            rows = page.orderBy("__rank", COL_NUM_IF).limit(page_size).collect()
            candidates = [int(row[COL_NUM_IF]) for row in rows]
            if rows:
                cursor = (int(rows[-1]["__rank"]), int(rows[-1][COL_NUM_IF]))

        if not candidates:
            break
        with _perf_timer("closure", product=profile.name, roots=len(candidates)):
            lotes, proveniencias = _calcula_lotes_com_proveniencia(
                spark,
                config,
                spec,
                planos,
                ordem,
                candidates,
                max_passadas,
                somente_ativos=somente_ativos,
            )
        with _perf_timer(
            "live_fk_admission", product=profile.name, roots=len(candidates)
        ):
            rejected, reasons, page_selective = _target_fk_rejections(
                spark,
                spec,
                planos,
                lotes,
                proveniencias,
                ordem,
                profile.integrity.selective_missing_keys,
                nullify_columns or {},
                existing_key_lookup,
            )
        all_reasons.update(reasons)
        if num_ifs is not None and rejected:
            details = "; ".join(
                f"NUM_IF {root}: {', '.join(sorted(reasons[root]))}"
                for root in sorted(rejected)
            )
            for frame in lotes.values():
                frame.unpersist(blocking=False)
            for frame in proveniencias.values():
                frame.unpersist(blocking=False)
            raise ValueError(
                "Admissão FK rejeitou lista explícita; nenhum NUM_IF foi "
                f"substituído: {details}"
            )
        page_accepted = [
            candidate for candidate in candidates
            if candidate not in rejected
        ][:requested - len(accepted)]
        accepted.extend(page_accepted)
        if page_accepted:
            accepted_roots = spark.createDataFrame(
                [(value,) for value in page_accepted], [ROOT_PROVENANCE_COL]
            ).select(
                F.col(ROOT_PROVENANCE_COL).cast(
                    lotes[TABELA_RAIZ].schema[COL_NUM_IF].dataType
                )
            )
            if page_selective is not None:
                selected_missing = (
                    page_selective
                    .join(
                        F.broadcast(accepted_roots),
                        ROOT_PROVENANCE_COL,
                        "left_semi",
                    )
                    .select("TABELA", "COLUNA", "VALOR")
                )
                selective_missing = (
                    selected_missing if selective_missing is None
                    else selective_missing.unionByName(selected_missing)
                )
            for table in ordem:
                table_pk = list(planos[table].pk_cols)
                accepted_keys = (
                    proveniencias[table]
                    .join(F.broadcast(accepted_roots), ROOT_PROVENANCE_COL, "left_semi")
                    .select(*table_pk)
                    .dropDuplicates()
                )
                accepted_page_lote = (
                    lotes[table]
                    .join(accepted_keys, table_pk, "left_semi")
                    .localCheckpoint(eager=True)
                )
                if table in accepted_lotes:
                    previous_lote = accepted_lotes[table]
                    accepted_lotes[table] = (
                        previous_lote
                        .unionByName(accepted_page_lote)
                        .dropDuplicates(table_pk)
                        .localCheckpoint(eager=True)
                    )
                    previous_lote.unpersist(blocking=False)
                    accepted_page_lote.unpersist(blocking=False)
                else:
                    accepted_lotes[table] = accepted_page_lote
        for frame in lotes.values():
            frame.unpersist(blocking=False)
        for frame in proveniencias.values():
            frame.unpersist(blocking=False)
        if num_ifs is not None:
            break

    if len(accepted) < requested:
        raise ValueError(
            "Domínio esgotado pela admissão FK do destino: "
            f"{len(accepted)} instrumento(s) válido(s), pedi {requested}."
        )
    accepted = sorted(accepted[:requested])
    missing_df = (
        selective_missing.dropDuplicates().localCheckpoint(eager=True)
        if selective_missing is not None else None
    )
    logger.info(
        "Admissão FK concluída: %d instrumento(s) aceito(s), %d rejeitado(s).",
        len(accepted),
        len(all_reasons),
    )
    return TargetInstrumentSelection(accepted, missing_df, accepted_lotes)


def _deriva_tipo_oracle(spark, config, num_if_valores: List,
                         tipo_oracle_cli: Optional[int] = None) -> int:
    """Deriva o TIPO do instrumento (NUM_TIPO_IF) das próprias linhas do lote.

    Por que derivar em vez de configurar: o valor alimenta
    F_GETCODIGONOVOIF21(<tipo>, ...), que é quem decide QUAL código o Oracle
    aloca. Se o tipo viesse de um literal por produto (o antigo REGRAS_PRODUTO)
    ou de um flag digitado, um --produto trocado em relação ao SQL alocaria
    código do produto errado passando por TODAS as validações — o pattern de
    COD_IF não pega isso, porque ele valida a saída de uma função cujo input já
    estava errado. Lendo o tipo das linhas que o SQL do produto selecionou, o
    código alocado é o do produto sintetizado POR CONSTRUÇÃO.

    Semântica do --tipo-oracle (opcional):
      * ausente  -> deriva; mais de um tipo distinto no lote ABORTA;
      * presente -> confere contra o derivado e ABORTA se divergir. Num lote
                    legitimamente multi-tipo, é a escolha manual explícita.
    """
    src = _read_source(spark, config, TABELA_RAIZ)
    if COL_NUM_TIPO_IF not in src.columns:
        if tipo_oracle_cli is None:
            raise ValueError(
                f"{TABELA_RAIZ} não expõe a coluna {COL_NUM_TIPO_IF}: não há como "
                "derivar o tipo do lote. Informe --tipo-oracle explicitamente.")
        logger.warning(
            "%s sem coluna %s; usando --tipo-oracle=%d SEM conferência contra o "
            "dado.", TABELA_RAIZ, COL_NUM_TIPO_IF, int(tipo_oracle_cli))
        return int(tipo_oracle_cli)

    sel = spark.createDataFrame([(v,) for v in num_if_valores], [COL_NUM_IF])
    sel = sel.select(F.col(COL_NUM_IF).cast(src.schema[COL_NUM_IF].dataType))
    lote = src.join(F.broadcast(sel), on=COL_NUM_IF, how="left_semi")
    return _deriva_tipo_oracle_do_lote(lote, tipo_oracle_cli)


def _deriva_tipo_oracle_do_lote(
    lote_raiz: DataFrame,
    tipo_oracle_esperado: Optional[int] = None,
) -> int:
    """Deriva NUM_TIPO_IF do lote já admitido, sem consultar RAW."""
    if COL_NUM_TIPO_IF not in lote_raiz.columns:
        raise ValueError(
            f"snapshot de {TABELA_RAIZ} não expõe {COL_NUM_TIPO_IF}; "
            "gere novamente o plano"
        )
    brutos = [
        row["__tipo"] for row in
        (lote_raiz.select(F.col(COL_NUM_TIPO_IF).alias("__tipo"))
         .dropDuplicates()
         .orderBy("__tipo")
         .limit(MAX_TIPOS_DIAGNOSTICO + 1)
         .collect())
    ]
    if not brutos:
        raise ValueError(
            f"lote sem linhas em {TABELA_RAIZ}: não há {COL_NUM_TIPO_IF} a derivar.")
    if any(valor is None for valor in brutos):
        raise ValueError(
            f"{TABELA_RAIZ}.{COL_NUM_TIPO_IF} nulo em linha(s) do lote; corrija o "
            "domínio da query do produto.")

    tipos: List[int] = []
    for valor in brutos:
        try:
            tipo = int(valor)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{TABELA_RAIZ}.{COL_NUM_TIPO_IF} não é inteiro no lote: {valor!r}"
            ) from exc
        if tipo < 1:
            raise ValueError(
                f"{TABELA_RAIZ}.{COL_NUM_TIPO_IF} inválido no lote: {tipo}")
        tipos.append(tipo)
    tipos = sorted(set(tipos))

    if len(tipos) > 1:
        listados = (tipos if len(tipos) <= MAX_TIPOS_DIAGNOSTICO
                    else f"{tipos[:MAX_TIPOS_DIAGNOSTICO]}... (+)")
        if tipo_oracle_esperado is None:
            raise ValueError(
                f"Lote heterogêneo: {COL_NUM_TIPO_IF} distinto(s) {listados} nas "
                f"linhas selecionadas de {TABELA_RAIZ}. O tipo define qual COD_IF "
                "o Oracle aloca, então o SQL do produto precisa restringir o "
                "domínio a UM tipo. Se o lote for legitimamente multi-tipo, "
                "informe --tipo-oracle para escolher o tipo da alocação.")
        logger.warning(
            "Lote heterogêneo (%s=%s); alocando COD_IF com tipo congelado=%d por "
            "decisão explícita do plano.",
            COL_NUM_TIPO_IF, listados, int(tipo_oracle_esperado))
        return int(tipo_oracle_esperado)

    derivado = tipos[0]
    if tipo_oracle_esperado is not None and int(tipo_oracle_esperado) != derivado:
        raise ValueError(
            f"tipo Oracle esperado={int(tipo_oracle_esperado)} diverge do "
            f"{COL_NUM_TIPO_IF} "
            f"do lote ({derivado}). Isso indica query/produto trocados; corrija "
            "antes de alocar COD_IF no Oracle.")
    logger.info(
        "Tipo do instrumento DERIVADO do lote: %s=%d (%s).",
        COL_NUM_TIPO_IF, derivado,
        "confirmado pelo valor esperado" if tipo_oracle_esperado is not None
        else "sem override na CLI")
    return derivado


# ---------------------------------------------------------------------------
# Pertencimento (membership): que linhas de cada tabela pertencem ao lote.
# ---------------------------------------------------------------------------
def _filtra_ativos(df: DataFrame, tabela: str) -> Tuple[DataFrame, Optional[str]]:
    """Remove linhas logicamente excluídas. Devolve (df, coluna usada ou None).

    A coluna vem de FECHO_COLUNA_EXCLUSAO_POR_TABELA e NÃO tem fallback: filtrar
    pela coluna errada descarta linhas que o validador considera ativas (ver o
    comentário do mapa). Coluna declarada mas ausente no Parquet -> WARN e no-op,
    porque adivinhar a outra coluna é justamente o erro que se quer evitar."""
    coluna = FECHO_COLUNA_EXCLUSAO_POR_TABELA.get(tabela)
    if coluna is None:
        return df, None
    if coluna not in df.columns:
        logger.warning("fecho ativos: %s não expõe a coluna declarada %s; filtro "
                       "NÃO aplicado (sem fallback proposital).", tabela, coluna)
        return df, None
    if coluna == COL_DAT_EXCLUSAO:
        col = F.col(COL_DAT_EXCLUSAO)
        pred = col.isNull()
        if isinstance(df.schema[COL_DAT_EXCLUSAO].dataType, T.StringType):
            pred = pred | (F.trim(col) == F.lit(""))
        return df.where(pred), COL_DAT_EXCLUSAO
    norm = F.upper(F.trim(F.col(COL_IND_EXCLUIDO).cast("string")))
    return df.where(F.coalesce(~norm.isin("S", "Y", "1"), F.lit(True))), \
        COL_IND_EXCLUIDO


def _poda_cronograma_sem_tabela(lotes: Dict[str, DataFrame]) -> Optional[int]:
    """Remove do lote as CONDICAO_RESGATE cujo RESGATE pai não é COM TABELA.

    Devolve o nº de linhas podadas, ou None quando a poda não é aplicável
    (tabela fora do fecho / coluna ausente). Tolerante por construção."""
    cronograma = lotes.get(CRONOGRAMA_TABELA)
    resgate = lotes.get(RESGATE_TABELA)
    if cronograma is None or resgate is None:
        return None
    if CONDICAO_IF_PK not in cronograma.columns:
        return None
    if {CONDICAO_IF_PK, COL_COD_COND_RESGATE} - set(resgate.columns):
        logger.warning("poda cronograma: %s sem %s/%s; poda NÃO aplicada.",
                       RESGATE_TABELA, CONDICAO_IF_PK, COL_COD_COND_RESGATE)
        return None
    pais_com_tabela = (
        resgate.where(
            F.upper(F.trim(F.col(COL_COD_COND_RESGATE).cast("string")))
            == F.lit(COD_COND_RESGATE_COM_TABELA)
        )
        .select(_norm_key_col(F.col(CONDICAO_IF_PK)).alias("__cron_key"))
        .dropDuplicates()
    )
    marcado = cronograma.withColumn(
        "__cron_key", _norm_key_col(F.col(CONDICAO_IF_PK))
    )
    mantido = marcado.join(F.broadcast(pais_com_tabela), "__cron_key", "left_semi")
    mantido = mantido.select(*cronograma.columns).localCheckpoint(eager=True)
    antes = cronograma.count()
    depois = mantido.count()
    lotes[CRONOGRAMA_TABELA] = mantido
    return antes - depois


def _calcula_lotes_com_proveniencia(
    spark,
    config,
    spec: dict,
    planos: Dict[str, PlanoTabela],
    ordem: List[str],
    num_if_valores: List,
    max_passadas: int,
    somente_ativos: bool = True,
) -> Tuple[Dict[str, DataFrame], Dict[str, DataFrame]]:
    """Desce a árvore a partir da raiz pelas FKs de vínculo principal,
    pais-antes-de-filhos; repete a passada até estabilizar (ciclos), até
    max_passadas. Cada lote é pequeno (linhas de N instrumentos) -> persist +
    localCheckpoint para cortar a linhagem entre passadas.

    Com somente_ativos, as tabelas de FECHO_COLUNA_EXCLUSAO_POR_TABELA são lidas já
    sem as linhas logicamente excluídas. Sem isso o fecho puxa CONDICAO_IF /
    RESGATE soft-deleted e seus CONDICAO_RESGATE, que o validador descarta por
    inatividade do pai — gerando órfãos por construção. A raiz NÃO é filtrada
    aqui: o domínio da query já a restringe."""
    fontes: Dict[str, DataFrame] = {}
    lotes: Dict[str, DataFrame] = {}
    proveniencias: Dict[str, DataFrame] = {}
    contagens: Dict[str, int] = {}
    contagens_proveniencia: Dict[str, int] = {}

    def _fonte(tabela: str) -> DataFrame:
        if tabela in fontes:
            return fontes[tabela]
        src = _read_source(spark, config, tabela)
        if somente_ativos and tabela in FECHO_COLUNA_EXCLUSAO_POR_TABELA:
            src, coluna = _filtra_ativos(src, tabela)
            if coluna is not None:
                logger.info(
                    "fecho ativos [%s]: filtro por %s aplicado sem contagem RAW.",
                    tabela,
                    coluna,
                )
        fontes[tabela] = src
        return src

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
    proveniencias[TABELA_RAIZ] = (
        lotes[TABELA_RAIZ]
        .select(
            F.col(COL_NUM_IF),
            F.col(COL_NUM_IF).alias(ROOT_PROVENANCE_COL),
        )
        .dropDuplicates()
        .localCheckpoint(eager=True)
    )
    contagens_proveniencia[TABELA_RAIZ] = contagens[TABELA_RAIZ]

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
            src = _fonte(t)
            partes: List[DataFrame] = []
            partes_proveniencia: List[DataFrame] = []
            for fk in fks_uteis:
                child = src.alias("__child")
                parent = proveniencias[fk.parent_table].alias("__parent")
                join_condition = [
                    F.col(f"__child.{child_col}") == F.col(f"__parent.{parent_col}")
                    for child_col, parent_col in zip(
                        fk.columns, fk.parent_columns
                    )
                ]
                linked = child.join(F.broadcast(parent), join_condition, "inner")
                partes.append(linked.select("__child.*"))
                partes_proveniencia.append(
                    linked.select(
                        *[
                            F.col(f"__child.{pk_col}").alias(pk_col)
                            for pk_col in plano.pk_cols
                        ],
                        F.col(f"__parent.{ROOT_PROVENANCE_COL}").alias(
                            ROOT_PROVENANCE_COL
                        ),
                    ).dropDuplicates()
                )
            lote_t = partes[0]
            for extra in partes[1:]:
                lote_t = lote_t.unionByName(extra)
            lote_t = lote_t.dropDuplicates(list(plano.pk_cols))
            lote_t = lote_t.localCheckpoint(eager=True)
            proveniencia_t = partes_proveniencia[0]
            for extra in partes_proveniencia[1:]:
                proveniencia_t = proveniencia_t.unionByName(extra)
            proveniencia_t = (
                proveniencia_t.dropDuplicates(
                    [*plano.pk_cols, ROOT_PROVENANCE_COL]
                ).localCheckpoint(eager=True)
            )
            n = lote_t.count()
            n_proveniencia = proveniencia_t.count()
            changed = (
                contagens.get(t) != n
                or contagens_proveniencia.get(t) != n_proveniencia
            )
            if changed:
                # PRIMEIRA atribuição também conta como mudança: num ciclo de
                # FKs principais o lote da passada 1 pode estar incompleto
                # (pai que vem depois na ordem ainda sem lote) — só a passada
                # de confirmação sem NENHUMA mudança prova o ponto fixo.
                cresceu = True
                previous_lote = lotes.get(t)
                previous_provenance = proveniencias.get(t)
                lotes[t] = lote_t
                proveniencias[t] = proveniencia_t
                contagens[t] = n
                contagens_proveniencia[t] = n_proveniencia
                if previous_lote is not None:
                    previous_lote.unpersist(blocking=False)
                if previous_provenance is not None:
                    previous_provenance.unpersist(blocking=False)
            else:
                lote_t.unpersist(blocking=False)
                proveniencia_t.unpersist(blocking=False)
        faltando = [t for t in ordem if t not in lotes]
        if not cresceu and not faltando:
            logger.info("Pertencimento estabilizou na passada %d.", passada)
            break
        if passada == max_passadas and (cresceu or faltando):
            # Gravar um fecho INCOMPLETO em silêncio seria pior que falhar:
            # sintéticos sem parte das linhas mudam o comportamento da NoMe.
            raise ValueError(
                f"Pertencimento não estabilizou em {max_passadas} passada(s)"
                + (f"; tabela(s) sem lote: {faltando}" if faltando else "")
                + ". Aumente --max-passadas (ciclos de FK profundos no fecho).")
    for t in ordem:
        if t not in lotes:
            # Sem caminho principal até a raiz nesta execução: nada a sintetizar.
            lotes[t] = _fonte(t).limit(0)
            contagens[t] = 0
            proveniencias[t] = (
                lotes[t]
                .select(*planos[t].pk_cols)
                .withColumn(
                    ROOT_PROVENANCE_COL,
                    F.lit(None).cast(lotes[TABELA_RAIZ].schema[COL_NUM_IF].dataType),
                )
            )
            contagens_proveniencia[t] = 0
    if somente_ativos:
        podadas = _poda_cronograma_sem_tabela(lotes)
        if podadas is None:
            logger.info("poda cronograma: não aplicável neste fecho.")
        else:
            contagens[CRONOGRAMA_TABELA] = lotes[CRONOGRAMA_TABELA].count()
            cronograma_pk = list(planos[CRONOGRAMA_TABELA].pk_cols)
            previous_provenance = proveniencias[CRONOGRAMA_TABELA]
            proveniencias[CRONOGRAMA_TABELA] = (
                previous_provenance
                .join(
                    F.broadcast(
                        lotes[CRONOGRAMA_TABELA]
                        .select(*cronograma_pk)
                        .dropDuplicates()
                    ),
                    cronograma_pk,
                    "left_semi",
                )
                .localCheckpoint(eager=True)
            )
            previous_provenance.unpersist(blocking=False)
            logger.info("poda cronograma [%s]: %d linha(s) removida(s) por pai "
                        "%s <> '%s'; restam %d.", CRONOGRAMA_TABELA, podadas,
                        COL_COD_COND_RESGATE, COD_COND_RESGATE_COM_TABELA,
                        contagens[CRONOGRAMA_TABELA])
    for t in ordem:
        logger.info("Lote %s: %d linha(s).", t, contagens[t])
    return lotes, proveniencias


def calcula_lotes(spark, config, spec: dict, planos: Dict[str, PlanoTabela],
                  ordem: List[str], num_if_valores: List,
                  max_passadas: int,
                  somente_ativos: bool = True) -> Dict[str, DataFrame]:
    lotes, proveniencias = _calcula_lotes_com_proveniencia(
        spark,
        config,
        spec,
        planos,
        ordem,
        num_if_valores,
        max_passadas,
        somente_ativos=somente_ativos,
    )
    for provenance in proveniencias.values():
        provenance.unpersist(blocking=False)
    return lotes


# ---------------------------------------------------------------------------
# Sintetização: lote × K, mapeamento de PK e reescrita de FKs.
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
    """Sintetiza uma tabela: lote × K, mapeia a própria PK e reescreve as FKs.
    Devolve (sintéticos prontos SEM colunas temporárias, mapeamento da PK).

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

    # 2) FKs para pais sintetizados (inclui self-FKs e laterais): remap-se-no-lote,
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
    linhas de chave nova após a carga dos sintéticos. Tabela sem coluna NUM_IF
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
            continue  # lote vazio: sem sintéticos, nada a conferir
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
def _validate_meu_numero_prefix(value: Optional[str]) -> str:
    if not isinstance(value, str) or not MEU_PREFIX_PATTERN.fullmatch(value):
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


def _canon_oracle_key(value: Any, numeric: bool = False) -> str:
    text = "" if value is None else str(value)
    if numeric:
        text = text.strip()
    if numeric and re.fullmatch(r"-?\d+\.\d+", text):
        return text.rstrip("0").rstrip(".")
    return text


def _oracle_existing_parent_keys(
    jvm,
    credentials: Tuple[str, str, str],
    table: str,
    columns: Tuple[str, ...],
    keys: List[Tuple[str, ...]],
    numeric_flags: Optional[Tuple[bool, ...]] = None,
    batch_size: int = 900,
    connection=None,
) -> Set[Tuple[str, ...]]:
    """Return requested FK tuples that exist now, without scanning the parent."""
    if batch_size < 1 or batch_size > 900:
        raise ValueError("batch_size da admissão FK deve estar entre 1 e 900")
    table = _normalize_rule_identifier(table, "FK parent_table", table=True)
    columns = tuple(
        _normalize_rule_identifier(column, "FK parent_column")
        for column in columns
    )
    if not columns:
        raise ValueError("admissão FK exige ao menos uma coluna pai")
    numeric_flags = numeric_flags or tuple(False for _ in columns)
    if len(numeric_flags) != len(columns):
        raise ValueError("numeric_flags incompatível com as colunas pai")
    malformed = [key for key in keys if len(key) != len(columns)]
    if malformed:
        raise ValueError("tupla de FK incompatível com as colunas pai")
    if not keys:
        return set()

    owns_connection = connection is None
    if connection is None:
        connection = _open_oracle_connection(jvm, *credentials)
    existing: Set[Tuple[str, ...]] = set()
    try:
        selected = ", ".join(columns)
        for offset in range(0, len(keys), batch_size):
            batch = keys[offset:offset + batch_size]
            if len(columns) == 1:
                placeholders = ", ".join("?" for _ in batch)
                predicate = f"{columns[0]} IN ({placeholders})"
            else:
                tuple_placeholder = "(" + ", ".join("?" for _ in columns) + ")"
                placeholders = ", ".join(tuple_placeholder for _ in batch)
                predicate = f"({selected}) IN ({placeholders})"
            sql = (
                f"SELECT DISTINCT {selected} FROM CETIP.{table} "
                f"WHERE {predicate}"
            )
            statement = None
            result_set = None
            try:
                statement = connection.prepareStatement(sql)
                statement.setFetchSize(min(len(batch), 900))
                bind_index = 1
                for key in batch:
                    for value in key:
                        statement.setString(bind_index, value)
                        bind_index += 1
                result_set = statement.executeQuery()
                while result_set.next():
                    existing.add(tuple(
                        _canon_oracle_key(
                            result_set.getString(index + 1), numeric_flags[index]
                        ) for index in range(len(columns))
                    ))
            finally:
                if result_set is not None:
                    result_set.close()
                if statement is not None:
                    statement.close()
    finally:
        if owns_connection:
            connection.close()
    return existing


def _apply_oracle_pk_floors(
    jvm,
    credentials: Tuple[str, str, str],
    planos: Mapping[str, PlanoTabela],
) -> None:
    """Raise reservable PK minima above the live target, never only the RAW max."""
    connection = _open_oracle_connection(jvm, *credentials)
    try:
        for table in sorted(planos):
            plano = planos[table]
            if plano.pk_regra != "OFFSET_PROPRIO":
                continue
            if len(plano.pk_cols) != 1 or plano.pk_start is None:
                raise ValueError(f"{table}: OFFSET_PROPRIO exige PK simples e início")
            pk_col = _normalize_rule_identifier(
                plano.pk_cols[0], f"{table}.pk"
            )
            statement = None
            result_set = None
            try:
                statement = connection.prepareStatement(
                    f"SELECT MAX({pk_col}) FROM CETIP.{table}"
                )
                result_set = statement.executeQuery()
                raw_max = result_set.getString(1) if result_set.next() else None
            finally:
                if result_set is not None:
                    result_set.close()
                if statement is not None:
                    statement.close()
            if raw_max is not None:
                target_floor = int(Decimal(str(raw_max))) + 1
                plano.pk_start = max(int(plano.pk_start), target_floor)
    finally:
        connection.close()


def _read_controle_operacional_date(jvm, jdbc_url: str, user: str,
                                    password: str) -> date:
    """Lê a data operacional única usada por todo o run."""
    connection = _open_oracle_connection(jvm, jdbc_url, user, password)
    statement = None
    result_set = None
    try:
        statement = connection.prepareStatement(CONTROLE_OPERACIONAL_DATE_SQL)
        result_set = statement.executeQuery()
        if not result_set.next():
            raise ValueError(
                "CONTROLE_OPERACIONAL não retornou NUM_ORDEM=0/NUM_SISTEMA NULL")
        raw = result_set.getDate(1)
        if raw is None:
            raise ValueError("CONTROLE_OPERACIONAL.DAT_CTL_OPER está NULL")
        return date.fromisoformat(str(raw))
    finally:
        if result_set is not None:
            result_set.close()
        if statement is not None:
            statement.close()
        connection.close()


def _allocation_sql(code_kind: str, batch_count: int,
                    policy: BusinessKeyPolicy) -> str:
    if batch_count < 1:
        raise ValueError("batch_count deve ser >= 1")
    if code_kind == "COD_IF":
        if policy.cod_if_allocator != "oracle_if21":
            raise ValueError(
                f"alocador COD_IF não implementado: {policy.cod_if_allocator!r}"
            )
        # Defensivo: a policy só chega aqui depois de _resolve_business_policy.
        if policy.cod_if_oracle_type is None:
            raise ValueError(
                "tipo do instrumento não resolvido para a alocação de COD_IF "
                "(chame _resolve_business_policy com o tipo derivado do lote)"
            )
        expression = (
            "CETIP.PKG_CODIGO.F_GETCODIGONOVOIF21("
            f"{int(policy.cod_if_oracle_type)}, TO_DATE(?, 'YYYY-MM-DD'))"
        )
    elif code_kind == "COD_OPERACAO":
        if policy.operation is None:
            raise ValueError("perfil não habilita geração de COD_OPERACAO")
        if policy.operation.strategy != "cetip_operacao_v1":
            raise ValueError(
                f"estratégia não implementada: {policy.operation.strategy!r}"
            )
        expression = "CETIP.GET_COD_OPERACAO"
    else:
        raise ValueError(f"tipo de código desconhecido: {code_kind}")
    return (f"SELECT LEVEL ordinal, {expression} code FROM dual "
            f"CONNECT BY LEVEL <= {int(batch_count)}")


def _iter_oracle_code_batches(jvm, jdbc_url: str, user: str, password: str, *,
                              code_kind: str, total: int, batch_size: int,
                              engorda_date: date, policy: BusinessKeyPolicy):
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
                statement = connection.prepareStatement(
                    _allocation_sql(code_kind, expected, policy))
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
            pattern = (policy.cod_if_pattern if code_kind == "COD_IF"
                       else (policy.operation.code_pattern
                             if policy.operation is not None else None))
            if not pattern:
                raise ValueError(f"{code_kind}: pattern não configurado no perfil")
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


def _dry_placeholder(kind: str, policy: BusinessKeyPolicy):
    if kind == "COD_IF":
        return F.concat(F.lit(policy.cod_if_dry_prefix),
                        F.upper(F.lpad(F.conv(F.col("ORDINAL"), 10, 36), 5, "0")))
    if kind == "COD_OPERACAO" and policy.operation is not None:
        return F.lpad(F.col("ORDINAL").cast("string"), 16, "0")
    raise ValueError(f"tipo de código não habilitado no perfil: {kind}")


def _join_code_chunks(slots: DataFrame, code_chunks: DataFrame,
                      generated_alias: str) -> DataFrame:
    return slots.join(code_chunks.select("ORDINAL", generated_alias), on="ORDINAL", how="inner")


def _materialize_code_map(spark: SparkSession, slots: DataFrame, *, code_kind: str,
                          generated_alias: str, out_path: Optional[str], dry_run: bool,
                          credentials: Optional[Tuple[str, str, str]], batch_size: int,
                          engorda_date: date, policy: BusinessKeyPolicy) -> DataFrame:
    """Anexa códigos por ordinal, mantendo no driver somente o lote corrente."""
    total = slots.count()
    if dry_run:
        return slots.withColumn(generated_alias, _dry_placeholder(code_kind, policy))
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
            engorda_date=engorda_date, policy=policy):
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
    pattern = (policy.cod_if_pattern if code_kind == "COD_IF"
               else (policy.operation.code_pattern
                     if policy.operation is not None else None))
    if not pattern:
        raise ValueError(f"{code_kind}: pattern não configurado no perfil")
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


def _generate_meu_numeros(
    operacoes: DataFrame,
    prefix: str,
    engorda_date: date,
    *,
    ordinal_start: int = 1,
    ordinal_end: Optional[int] = None,
) -> DataFrame:
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
    if ordinal_start < 1:
        raise ValueError("meu-número: ordinal_start deve ser >= 1")
    expected_end = ordinal_start + allocated - 1
    if ordinal_end is not None and ordinal_end != expected_end:
        raise ValueError(
            f"meu-número: reserva {ordinal_start}..{ordinal_end} não atende "
            f"{allocated} alocação(ões)"
        )
    _validate_meu_capacity(expected_end)
    allocation_map = _with_distributed_ordinal(
        allocations, ["NUM_ID_OPERACAO", "__meu_side"], "__meu_ord"
    ).withColumn(
        "__meu_ord", F.col("__meu_ord") + F.lit(ordinal_start - 1)
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
    required = {COL_NUM_IF, "COD_IF"}
    missing = sorted(required - set(instrumentos.columns))
    if missing:
        raise ValueError(f"INSTRUMENTO_FINANCEIRO sem coluna(s) para COD_IF: {missing}")

    roots = instrumentos.select(
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


def _validate_business_keys(instrumentos: DataFrame,
                            operacoes: Optional[DataFrame],
                            policy: BusinessKeyPolicy) -> None:
    try:
        roots = _validated_root_cod_if_map(instrumentos)
    except ValueError as exc:
        raise ValueError(f"Validação final de chaves de negócio FALHOU: {exc}") from exc

    operation = policy.operation
    checks = [(instrumentos, "COD_IF", policy.cod_if_pattern)]
    if operation is not None:
        if operacoes is None:
            raise ValueError(
                "Validação final de chaves de negócio FALHOU: OPERACAO ausente"
            )
        required = {COL_NUM_IF, "COD_IF", "COD_OPERACAO"}
        missing = sorted(required - set(operacoes.columns))
        if missing:
            raise ValueError(
                "Validação final de chaves de negócio FALHOU: "
                f"OPERACAO sem coluna(s): {missing}"
            )
        if not isinstance(operacoes.schema["COD_IF"].dataType, T.StringType):
            raise ValueError(
                "Validação final de chaves de negócio FALHOU: "
                "OPERACAO.COD_IF precisa ter tipo textual StringType"
            )
        checks.append((operacoes, "COD_OPERACAO", operation.code_pattern))

    errors = []
    for df, column, pattern in checks:
        if not pattern:
            errors.append(f"{column}: pattern ausente no perfil")
            continue
        total = df.count()
        invalid = df.where(
            F.col(column).isNull() | ~F.trim(F.col(column)).rlike(pattern)).count()
        distinct = df.select(F.trim(F.col(column))).dropDuplicates().count()
        if invalid:
            errors.append(f"{column}: {invalid} valor(es) vazio(s)/malformado(s)")
        if distinct != total:
            errors.append(f"{column}: {total - distinct} duplicata(s)")
    if (operation is not None and operation.generate_meu_numero
            and operacoes is not None):
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
            errors.append(
                f"meu-número: {incomplete} tupla(s) incompleta(s)/malformada(s)"
            )
        if distinct_tuples != total_tuples:
            errors.append(
                f"meu-número: {total_tuples - distinct_tuples} colisão(ões) interna(s)"
            )
    if operation is not None and operacoes is not None:
        compared = operacoes.withColumn(
            "__num_if", _norm_key_col(F.col(COL_NUM_IF))
        ).join(roots, "__num_if", "left")
        mismatches = compared.where(
            F.col("__root_cod_if").isNull()
            | F.col("COD_IF").isNull()
            | (F.trim(F.col("COD_IF")) !=
               F.trim(F.col("__root_cod_if").cast("string")))
        ).count()
        if mismatches:
            errors.append(
                "OPERACAO.COD_IF: "
                f"{mismatches} valor(es) divergente(s) da raiz por NUM_IF"
            )
    if errors:
        raise ValueError("Validação final de chaves de negócio FALHOU: " + "; ".join(errors))


def _validate_disabled_operation_output(
    resultados: Mapping[str, Tuple[Any, int]],
    operation_policy: Optional[OperationKeyPolicy],
) -> None:
    """Impede que operacoes sintetizadas mantenham chaves de negócio da origem."""
    if operation_policy is not None:
        return
    operation_output = resultados.get("OPERACAO")
    if operation_output is None:
        return
    _, source_rows = operation_output
    if source_rows:
        raise ValueError(
            "A política de chaves de negócio desliga OPERACAO, mas o lote contém "
            f"{source_rows} linha(s) dessa tabela. Configure a estratégia em "
            "REGRAS_SCHEMA_CETIP ou exclua OPERACAO da sintetização com "
            "--tratar-como-static."
        )


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
    agregação por checagem; os sintéticos são pequenos (lote × K)."""
    erros: List[str] = []
    esperado = n_lote * fator_k
    total = clones.count()
    if total != esperado:
        erros.append(f"contagem: {total} sintético(s), esperado {esperado} "
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
# prefixo da própria tabela dentro do prefixo de sintéticos e regrava).
# ---------------------------------------------------------------------------
def _delete_path(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    jsc = spark.sparkContext._jsc
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(jsc.hadoopConfiguration())
    if fs.exists(hpath) and not fs.delete(hpath, True):
        raise ValueError(f"não foi possível apagar caminho de trabalho: {path}")


def _promote_staging_paths(
    fs, staging, final, backup, *, require_absent: bool = False
) -> None:
    """Promove staging com rollback explícito, sem alegar atomicidade.

    Object stores podem implementar rename como copy+delete. Existe uma janela
    de crash entre `final -> backup` e `staging -> final`; nesse caso, o operador
    deve restaurar manualmente o caminho `.__previous_*` informado no log.
    """
    staging_text, final_text, backup_text = map(str, (staging, final, backup))
    if not fs.exists(staging):
        raise ValueError(f"staging ausente antes da publicação: {staging_text}")
    had_previous = fs.exists(final)
    if had_previous and require_absent:
        raise ValueError(
            f"artefato sintético imutável já existe em {final_text!r}; "
            "não será substituído após race de publicação"
        )
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


def _publish_staging(
    spark: SparkSession,
    staging_path: str,
    final_path: str,
    *,
    require_absent: bool = False,
) -> None:
    jvm = spark.sparkContext._jvm
    jsc = spark.sparkContext._jsc
    staging = jvm.org.apache.hadoop.fs.Path(staging_path)
    final = jvm.org.apache.hadoop.fs.Path(final_path)
    backup = jvm.org.apache.hadoop.fs.Path(f"{final_path}.__previous_{uuid.uuid4().hex}")
    fs = final.getFileSystem(jsc.hadoopConfiguration())
    _promote_staging_paths(
        fs, staging, final, backup, require_absent=require_absent
    )


def _stage_and_publish(spark: SparkSession, final_path: str,
                       prepare: Callable[[str], None], *,
                       require_absent: bool = False) -> None:
    staging_path = f"{final_path}.__staging_{uuid.uuid4().hex}"
    _delete_path(spark, staging_path)
    try:
        prepare(staging_path)
        if require_absent:
            _assert_exact_output_absent(spark, final_path)
        _publish_staging(
            spark,
            staging_path,
            final_path,
            require_absent=require_absent,
        )
    except Exception:
        logger.error(
            "Publicação abortada; verifique o erro e qualquer backup .__previous_* "
            "antes de consumir o destino fixo.")
        raise


def _assert_exact_output_absent(spark: SparkSession, path: str) -> None:
    jvm = spark.sparkContext._jvm
    jsc = spark.sparkContext._jsc
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(jsc.hadoopConfiguration())
    if fs.exists(hpath):
        raise ValueError(
            f"artefato sintético imutável já existe em {path!r}; "
            "não será sobrescrito por retry"
        )


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
    onprem-export, sintético em prefixos próprios) — por isso as
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
    """O prefixo de sintéticos é EXCLUSIVO e substituído por staging completo.

    A saída anterior permanece publicada durante alocação, preflight, escrita e
    readback; só depois o staging validado é promovido como uma árvore completa.
    Por isso o destino NÃO pode ser vazio, nem conter/estar contido na área
    raw, nem conter a área de saída do engorda.

    Multi-produto: cada produto precisa do SEU prefixo (default
    sintetizacao_multiproduto/<produto>). Dois produtos com o mesmo
    --clone-prefix se sobrescrevem — o segundo run publica por cima do primeiro."""
    prefix = (config.get("DATAGEN_CLONE_PREFIX") or "").strip("/")
    exact_output = config.get("DATAGEN_OUTPUT_URI")
    if not prefix and not exact_output:
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
    # (misturar sintéticos com snapshots que o engorda lê por nome de tabela).
    if _mesmo_ou_ancestral(save_base, raw_area) or _mesmo_ou_ancestral(raw_area, save_base):
        raise ValueError(
            f"Destino dos sintéticos ({save_base}) sobrepõe a área raw "
            f"({raw_area}). Ajuste DATAGEN_CLONE_PREFIX/DATAGEN_RAW_PREFIX "
            "para áreas disjuntas.")
    # Área do engorda: só é problema se substituir save_base LEVAR JUNTO a área
    # do engorda (igual ou descendente). O contrário — sintéticos DENTRO da base
    # sintética, em prefixo próprio — é o layout esperado.
    if not exact_output and _mesmo_ou_ancestral(save_base, engorda_area):
        raise ValueError(
            f"Destino dos sintéticos ({save_base}) é igual/ancestral da área de "
            f"saída do engorda ({engorda_area}); apagá-lo destruiria a saída "
            "do engorda_tables. Use um prefixo dedicado aos sintéticos.")
    return save_base


# ---------------------------------------------------------------------------
# Contagens de diagnóstico do domínio (log-only; não altera a sintetização).
#
# O domínio da volumetria é o MAPA_CLONE_NUM_IF publicado pelo próprio run.
# Portanto NUM_TIPO_IF, COD_COND_RESGATE, COD_TIPO_ESCALONAMENTO e qualquer
# outro filtro de negócio permanecem exclusivamente no SQL de entrada. O mapa
# transporta para a saída exatamente os NUM_IF selecionados por esse SQL, sem
# tentar reconstruir ou interpretar seus predicados no Python.
# ---------------------------------------------------------------------------
# Colunas do SELECT final da query de contagens, na ordem em que são logadas.
_CONTAGENS_DOMINIO_COLS = (
    "QIFE", "QTIT", "QCRE", "QC20",
    "QC01", "QC02", "QC03", "QC04", "QC05", "QC14",
    "QRES", "QJFL", "QJFI", "QE83", "QE85",
    "QDEP", "QCOM", "QCPA", "QOPE",
    "QDOP", "QESP", "QECO", "QLAN",
)


def _monta_query_contagens_dominio(config: dict) -> str:
    """Monta a query de contagens do domínio sobre os Parquets ENGORDADOS
    (saída sintética em clone_base_path). Só lê os sintéticos recém-gravados e
    não altera nada.

    FILTRO_BASE nasce dos NUM_IF_NOVO distintos gravados em MAPA_CLONE_NUM_IF.
    Esse mapa é a parametrização exata do domínio definido pelo SQL de entrada:
    não há filtros de produto duplicados nesta query."""
    base = clone_base_path(config)

    def src(table: str) -> str:
        path = f"{base}/{table_path_name(table)}"
        if "`" in path:
            raise ValueError(f"path do sintético de {table} contém crase e não é SQL-safe")
        return f"parquet.`{path}`"

    return f"""
WITH FILTRO_BASE AS
(
    SELECT DISTINCT IFE.NUM_IF
    FROM {src(MAPA_NUM_IF_TABLE)} MIF
         INNER JOIN {src("INSTRUMENTO_FINANCEIRO")} IFE
                 ON IFE.NUM_IF = MIF.NUM_IF_NOVO
         INNER JOIN {src("TITULO")} TIT ON TIT.NUM_IF = IFE.NUM_IF
         INNER JOIN {src("CONDICAO_IF")} CIF ON CIF.NUM_IF = IFE.NUM_IF
         INNER JOIN {src("RESGATE")} RES ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.DAT_EXCLUSAO IS NULL
      AND CIF.DAT_EXCLUSAO IS NULL
      AND RES.DAT_EXCLUSAO IS NULL
),
EVENTOS_IF AS
(
    SELECT E.NUM_IF,
           MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 83 THEN 1 ELSE 0 END) QE83,
           MAX(CASE WHEN E.NUM_TIPO_EVENTO_LEGADO = 85 THEN 1 ELSE 0 END) QE85
      FROM {src("EVENTO")} E
           INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = E.NUM_IF
    GROUP BY E.NUM_IF
),
FLAGS_IF AS
(
    SELECT C.NUM_IF,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  1 THEN 1 ELSE 0 END) QC01,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  2 THEN 1 ELSE 0 END) QC02,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  3 THEN 1 ELSE 0 END) QC03,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  4 THEN 1 ELSE 0 END) QC04,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF =  5 THEN 1 ELSE 0 END) QC05,
           MAX(CASE WHEN C.COD_TIPO_CONDICAO_IF = 14 THEN 1 ELSE 0 END) QC14,
           MAX(CASE WHEN JFL.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFL,
           MAX(CASE WHEN JFI.NUM_CONDICAO_IF IS NOT NULL THEN 1 ELSE 0 END) QJFI
      FROM {src("CONDICAO_IF")} C
           INNER JOIN FILTRO_BASE FB ON FB.NUM_IF = C.NUM_IF
            LEFT JOIN {src("JUROS_FLUTUANTE")} JFL ON JFL.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
            LEFT JOIN {src("JUROS_FIXO")} JFI ON JFI.NUM_CONDICAO_IF = C.NUM_CONDICAO_IF
     WHERE C.DAT_EXCLUSAO IS NULL
       AND C.COD_TIPO_CONDICAO_IF <> 20
     GROUP BY C.NUM_IF
),
AGREGADO_BASE AS
(
    SELECT COUNT(*) QTDE_BASE
      FROM FILTRO_BASE
),
AGREGADO_FLAGS AS
(
    SELECT SUM(NVL(F.QC01,0)) QC01,
           SUM(NVL(F.QC02,0)) QC02,
           SUM(NVL(F.QC03,0)) QC03,
           SUM(NVL(F.QC04,0)) QC04,
           SUM(NVL(F.QC05,0)) QC05,
           SUM(NVL(F.QC14,0)) QC14,
           SUM(NVL(F.QJFL,0)) QJFL,
           SUM(NVL(F.QJFI,0)) QJFI,
           SUM(NVL(E.QE83,0)) QE83,
           SUM(NVL(E.QE85,0)) QE85
      FROM FILTRO_BASE FB
      LEFT JOIN FLAGS_IF F ON F.NUM_IF = FB.NUM_IF
      LEFT JOIN EVENTOS_IF E ON E.NUM_IF = FB.NUM_IF
),
DEP_IF AS
(
    SELECT COUNT(DISTINCT DP.NUM_IF) QDEP
      FROM {src("DEPOSITO_AUTOMATICO_IF")} DP
           JOIN FILTRO_BASE FB ON FB.NUM_IF = DP.NUM_IF
),
COM_IF AS
(
    SELECT COUNT(DISTINCT CM.NUM_IF) QCOM
      FROM {src("CARTEIRA_COMITENTE")} CM
           JOIN FILTRO_BASE FB ON FB.NUM_IF = CM.NUM_IF
     WHERE CM.QTD_CARTEIRA_COMITENTE > 0
),
CPA_IF AS
(
    SELECT COUNT(DISTINCT CP.NUM_IF) QCPA
      FROM {src("CARTEIRA_PARTICIPANTE")} CP
           JOIN FILTRO_BASE FB ON FB.NUM_IF = CP.NUM_IF
     WHERE CP.QTD_CARTEIRA_PARTICIPANTE > 0
),
OPE_IF AS
(
    SELECT COUNT(DISTINCT OPE.NUM_IF) QOPE
      FROM {src("OPERACAO")} OPE
           JOIN FILTRO_BASE FB ON FB.NUM_IF = OPE.NUM_IF
),
DOP_IF AS
(
    SELECT COUNT(DISTINCT OPE.NUM_IF) QDOP
      FROM {src("DADO_OPERACAO")} DOP
           JOIN {src("OPERACAO")} OPE
             ON OPE.NUM_ID_OPERACAO = DOP.NUM_ID_OPERACAO
           JOIN FILTRO_BASE FB ON FB.NUM_IF = OPE.NUM_IF
),
ESP_IF AS
(
    SELECT COUNT(DISTINCT OPE.NUM_IF) QESP
      FROM {src("ESPECIFICACAO")} ESP
           JOIN {src("OPERACAO")} OPE
             ON OPE.NUM_ID_OPERACAO = ESP.NUM_ID_OPERACAO
           JOIN FILTRO_BASE FB ON FB.NUM_IF = OPE.NUM_IF
),
ECO_IF AS
(
    SELECT COUNT(DISTINCT OPE.NUM_IF) QECO
      FROM {src("ESPECIFICACAO_COMITENTE")} ECO
           JOIN {src("ESPECIFICACAO")} ESP
             ON ESP.NUM_ID_ESPECIFICACAO = ECO.NUM_ID_ESPECIFICACAO
           JOIN {src("OPERACAO")} OPE
             ON OPE.NUM_ID_OPERACAO = ESP.NUM_ID_OPERACAO
           JOIN FILTRO_BASE FB ON FB.NUM_IF = OPE.NUM_IF
),
LAN_IF AS
(
    SELECT COUNT(DISTINCT OPE.NUM_IF) QLAN
      FROM {src("LANCAMENTO")} LAN
           JOIN {src("OPERACAO")} OPE
             ON OPE.NUM_ID_OPERACAO = LAN.NUM_ID_OPERACAO
           JOIN FILTRO_BASE FB ON FB.NUM_IF = OPE.NUM_IF
)
SELECT B.QTDE_BASE AS QIFE,
       B.QTDE_BASE AS QTIT,
       B.QTDE_BASE AS QCRE,
       B.QTDE_BASE AS QC20,
       F.QC01, F.QC02, F.QC03, F.QC04, F.QC05, F.QC14,
       B.QTDE_BASE AS QRES,
       F.QJFL, F.QJFI, F.QE83, F.QE85,
       DAI.QDEP, COM.QCOM, CPA.QCPA, OPE.QOPE,
       DOP.QDOP, ESP.QESP, ECO.QECO, LAN.QLAN
  FROM AGREGADO_BASE B
CROSS JOIN AGREGADO_FLAGS F
CROSS JOIN DEP_IF DAI
CROSS JOIN COM_IF COM
CROSS JOIN CPA_IF CPA
CROSS JOIN OPE_IF OPE
CROSS JOIN DOP_IF DOP
CROSS JOIN ESP_IF ESP
CROSS JOIN ECO_IF ECO
CROSS JOIN LAN_IF LAN
"""


def _loga_contagens_dominio(spark, config: dict,
                            dry_run: bool = False) -> None:
    """Roda a query de contagens do domínio nos Parquets ENGORDADOS (saída
    sintética) e escreve o resultado no log. É puramente diagnóstico: qualquer
    falha é logada como aviso e NÃO interrompe nem altera a sintetização. No
    --dry-run nada foi gravado, então as contagens são puladas."""
    if dry_run:
        logger.info("--dry-run: contagens do domínio (dados engordados) puladas "
                    "— nada foi gravado.")
        return
    try:
        sql = _monta_query_contagens_dominio(config)
        row = spark.sql(sql).first()
        logger.info("=" * 78)
        logger.info("CONTAGENS DO DOMÍNIO (dados engordados — diagnóstico; "
                    "domínio do SQL de entrada via %s):", MAPA_NUM_IF_TABLE)
        if row is None:
            logger.info("  (query não retornou linhas)")
        else:
            for col in _CONTAGENS_DOMINIO_COLS:
                logger.info("  %-6s = %s", col, row[col])
        logger.info("=" * 78)
    except Exception as exc:  # noqa: BLE001 — diagnóstico não pode quebrar o job
        logger.warning("Falha ao calcular as contagens do domínio (ignorado): %s", exc)


# ---------------------------------------------------------------------------
# Orquestração.
# ---------------------------------------------------------------------------
def _meu_numero_ordinal_demand(
    operacoes: DataFrame,
    fator_k: int,
    operation_count: Optional[int] = None,
) -> int:
    norm_p1 = _norm_key_col(F.col("NUM_CONTA_PARTICIPANTE_P1"))
    norm_p2 = _norm_key_col(F.col("NUM_CONTA_PARTICIPANTE_P2"))
    same_accounts = operacoes.where(norm_p1.eqNullSafe(norm_p2)).count()
    base_count = operacoes.count() if operation_count is None else int(operation_count)
    return (base_count + same_accounts) * fator_k


def _count_final_lotes(lotes: Mapping[str, DataFrame]) -> Dict[str, int]:
    """Count every final lote through one Spark action."""
    count_frames = [
        frame.agg(F.count(F.lit(1)).cast("long").alias("__count")).select(
            F.lit(table).alias("__table"), F.col("__count")
        )
        for table, frame in sorted(lotes.items())
    ]
    if not count_frames:
        return {}
    combined = reduce(lambda left, right: left.unionByName(right), count_frames)
    return {str(row["__table"]): int(row["__count"]) for row in combined.collect()}


def _build_engorda_plan(
    *,
    config: Mapping[str, str],
    specs_uri: str,
    spec_sha256: str,
    product_profile: ProductProfile,
    valores: Sequence[int],
    fator_k: int,
    seed: int,
    engorda_ts: datetime,
    controle_operacional_date: Optional[date],
    tipo_derivado: int,
    planos: Mapping[str, PlanoTabela],
    lotes: Mapping[str, DataFrame],
    faltantes_uri: Optional[str],
    query_num_if_uri: str,
    selected_lote: Mapping[str, Any],
    lote_counts: Optional[Mapping[str, int]] = None,
    frozen_table_plans: Optional[Mapping[str, Any]] = None,
    prazo_vencimento_dias: Optional[int] = None,
    anular_cols: Optional[Mapping[str, Sequence[str]]] = None,
    meu_numero_prefix: Optional[str] = None,
) -> dict[str, Any]:
    source_counts = (
        {table: int(count) for table, count in lote_counts.items()}
        if lote_counts is not None
        else {table: int(lotes[table].count()) for table in planos}
    )
    tables: dict[str, Any] = {}
    for table in sorted(planos):
        plano = planos[table]
        source_count = source_counts[table]
        synthetic_count = source_count * fator_k
        pk_plan = {
            "rule": plano.pk_regra,
            "count_demand": (
                synthetic_count if plano.pk_regra == "OFFSET_PROPRIO" else 0
            ),
            "step": plano.pk_passo,
            "minimum_start": plano.pk_start,
        }
        if frozen_table_plans is not None:
            frozen_table = frozen_table_plans[table]
            frozen_pk = frozen_table.get("pk") if isinstance(frozen_table, Mapping) else None
            if not isinstance(frozen_pk, Mapping):
                raise ValueError(f"plano congelado {table}.pk inválido")
            if (frozen_table.get("source_count") != source_count
                    or frozen_table.get("synthetic_count") != synthetic_count
                    or frozen_pk.get("rule") != pk_plan["rule"]
                    or frozen_pk.get("count_demand") != pk_plan["count_demand"]):
                raise ValueError(
                    f"plano congelado {table} diverge das contagens/classificação"
                )
            pk_plan = dict(frozen_pk)
        tables[table] = {
            "source_count": source_count,
            "synthetic_count": synthetic_count,
            "pk": pk_plan,
        }

    operation = product_profile.business_keys.operation
    operation_count = 0
    meu_demand = 0
    if operation is not None and operation.table in lotes:
        operation_source_count = source_counts[operation.table]
        operation_count = operation_source_count * fator_k
        if operation.generate_meu_numero:
            meu_demand = _meu_numero_ordinal_demand(
                lotes[operation.table],
                fator_k,
                operation_count=operation_source_count,
            )

    body: dict[str, Any] = {
        "artifact_type": ENGORDA_PLAN_ARTIFACT,
        "schema_version": ENGORDA_PLAN_SCHEMA_VERSION,
        "product": product_profile.name,
        "selected_num_ifs": sorted(int(value) for value in valores),
        "fator_k": fator_k,
        "seed": seed,
        "engorda_timestamp": engorda_ts.isoformat(),
        "controle_operacional_date": (
            controle_operacional_date.isoformat()
            if controle_operacional_date is not None else None
        ),
        "raw_uri": _area(
            config["DATAGEN_RAW_BASE_URI"], config.get("DATAGEN_RAW_PREFIX")
        ),
        "output_uri": clone_base_path(dict(config)),
        "specs_uri": specs_uri,
        "spec_sha256": spec_sha256,
        "faltantes_uri": faltantes_uri,
        "query_num_if_uri": query_num_if_uri,
        "selected_lote": dict(selected_lote),
        "prazo_vencimento_dias": prazo_vencimento_dias,
        "anular_cols": {
            table: list(columns)
            for table, columns in sorted((anular_cols or {}).items())
        },
        "tables": tables,
        "cod_if": {
            "count": tables[TABELA_RAIZ]["synthetic_count"],
            "oracle_type": tipo_derivado,
        },
        "cod_operacao": {"count": operation_count},
        "meu_numero": {
            "ordinal_count_demand": meu_demand,
            **({"requested_prefix": meu_numero_prefix}
               if meu_numero_prefix is not None else {}),
        },
    }
    return {**body, "plan_id": _plan_id(body)}


def _validate_plan_artifact(plan: Mapping[str, Any]) -> dict[str, Any]:
    if plan.get("artifact_type") != ENGORDA_PLAN_ARTIFACT:
        raise ValueError("artefato de plano possui artifact_type inválido")
    if plan.get("schema_version") == 1:
        raise ValueError(
            "plano schema_version=1 não possui snapshot imutável; gere novamente "
            "com phase plan antes de materializar"
        )
    if plan.get("schema_version") != ENGORDA_PLAN_SCHEMA_VERSION:
        raise ValueError("artefato de plano possui schema_version incompatível")
    plan_id = plan.get("plan_id")
    if not isinstance(plan_id, str) or not plan_id:
        raise ValueError("artefato de plano sem plan_id")
    body = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan_id != _plan_id(body):
        raise ValueError("plan_id não corresponde ao conteúdo do plano")
    required = {
        "product", "selected_num_ifs", "fator_k", "seed",
        "engorda_timestamp", "controle_operacional_date", "raw_uri",
        "output_uri", "specs_uri", "spec_sha256", "faltantes_uri", "tables", "cod_if",
        "cod_operacao", "meu_numero", "query_num_if_uri", "selected_lote",
    }
    missing = sorted(required - set(plan))
    if missing:
        raise ValueError(f"artefato de plano incompleto: {missing}")
    if (not isinstance(plan["spec_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", plan["spec_sha256"])):
        raise ValueError("artefato de plano possui spec_sha256 inválido")
    if not isinstance(plan["tables"], dict) or not plan["tables"]:
        raise ValueError("artefato de plano precisa conter tables")
    selected_lote = _validate_selected_lote_descriptor(
        plan["selected_lote"], expected_tables=set(plan["tables"])
    )
    for table, table_plan in plan["tables"].items():
        snapshot_count = selected_lote["tables"][table]["row_count"]
        if table_plan.get("source_count") != snapshot_count:
            raise ValueError(
                f"plano tables.{table}.source_count diverge do selected_lote"
            )
    return dict(plan)


def _reservation_range(section: Mapping[str, Any], context: str,
                       expected_count: int, *, step: int = 1) -> Tuple[int, int]:
    if section.get("count") != expected_count:
        raise ValueError(
            f"reserva {context}: count={section.get('count')!r}, "
            f"esperado {expected_count}"
        )
    start, end = section.get("start"), section.get("end")
    if any(type(value) is not int for value in (start, end)):
        raise ValueError(f"reserva {context}: start/end precisam ser inteiros")
    expected_end = start + (expected_count - 1) * step
    if end != expected_end:
        raise ValueError(
            f"reserva {context}: range {start}..{end} não atende "
            f"count={expected_count}, step={step}"
        )
    return start, end


def _validate_reservation_artifact(
    plan: Mapping[str, Any], reservation: Mapping[str, Any]
) -> dict[str, Any]:
    if reservation.get("artifact_type") != ENGORDA_RESERVATION_ARTIFACT:
        raise ValueError("artefato de reserva possui artifact_type inválido")
    if reservation.get("schema_version") != ENGORDA_RESERVATION_SCHEMA_VERSION:
        raise ValueError("artefato de reserva possui schema_version incompatível")
    if reservation.get("plan_id") != plan["plan_id"]:
        raise ValueError("reserva não está vinculada ao plan_id consumido")
    if reservation.get("product") != plan["product"]:
        raise ValueError("produto da reserva diverge do plano")

    table_pks = reservation.get("table_pks")
    if not isinstance(table_pks, dict):
        raise ValueError("reserva precisa conter table_pks")
    expected_tables = {
        table for table, table_plan in plan["tables"].items()
        if table_plan["pk"]["rule"] == "OFFSET_PROPRIO"
        and table_plan["pk"]["count_demand"] > 0
    }
    if set(table_pks) != expected_tables:
        raise ValueError(
            "reserva table_pks diverge do plano: "
            f"esperado={sorted(expected_tables)}, recebido={sorted(table_pks)}"
        )
    for table in sorted(expected_tables):
        table_plan = plan["tables"][table]["pk"]
        table_reservation = table_pks[table]
        if not isinstance(table_reservation, Mapping):
            raise ValueError(f"reserva table_pks.{table} inválida")
        if table_reservation.get("step") != table_plan["step"]:
            raise ValueError(f"reserva table_pks.{table}: step diverge do plano")
        start, _ = _reservation_range(
            table_reservation,
            f"table_pks.{table}",
            table_plan["count_demand"],
            step=table_plan["step"],
        )
        if start < table_plan["minimum_start"]:
            raise ValueError(
                f"reserva table_pks.{table}: start {start} abaixo do mínimo "
                f"seguro {table_plan['minimum_start']}"
            )

    cod_reservation = reservation.get("cod_operacao")
    if not isinstance(cod_reservation, Mapping):
        raise ValueError("reserva precisa conter cod_operacao")
    cod_count = plan["cod_operacao"]["count"]
    expected_cod = {"strategy": "oracle_allocator", "count": cod_count}
    if dict(cod_reservation) != expected_cod:
        raise ValueError("cod_operacao precisa permanecer no allocator oficial Oracle")

    meu_reservation = reservation.get("meu_numero")
    if not isinstance(meu_reservation, Mapping):
        raise ValueError("reserva precisa conter meu_numero")
    meu_count = plan["meu_numero"]["ordinal_count_demand"]
    if meu_count:
        try:
            _validate_meu_numero_prefix(meu_reservation.get("prefix"))
        except argparse.ArgumentTypeError as exc:
            raise ValueError(str(exc)) from exc
        requested_prefix = plan["meu_numero"].get("requested_prefix")
        if (requested_prefix is not None
                and meu_reservation.get("prefix") != requested_prefix):
            raise ValueError(
                "reserva meu_numero diverge do requested_prefix do plano"
            )
        start, end = _reservation_range(meu_reservation, "meu_numero", meu_count)
        if start < 1 or end > MAX_MEU_NUMERO_ORDINAL:
            raise ValueError("reserva meu_numero excede os ordinais de 1 a 9999999")
    elif dict(meu_reservation) != {
        "prefix": None, "count": 0, "start": None, "end": None
    }:
        raise ValueError("reserva meu_numero vazia possui contrato inválido")
    return dict(reservation)


def _inject_reserved_pk_starts(
    planos: Mapping[str, PlanoTabela], reservation: Mapping[str, Any]
) -> None:
    for table, table_reservation in reservation["table_pks"].items():
        planos[table].pk_start = int(table_reservation["start"])


def _validate_reservation_live_pk_floors(
    planos: Mapping[str, PlanoTabela], reservation: Mapping[str, Any]
) -> None:
    for table, table_reservation in reservation["table_pks"].items():
        live_minimum = planos[table].pk_start
        reserved_start = int(table_reservation["start"])
        if live_minimum is None or reserved_start < int(live_minimum):
            raise ValueError(
                f"reserva table_pks.{table}: start {reserved_start} abaixo do "
                f"mínimo live seguro {live_minimum}"
            )


def executa_clonagem(spark, config, spec: dict, *,
                     product_profile: ProductProfile,
                     meu_numero_prefix: Optional[str] = None,
                     num_ifs: Optional[List[int]] = None,
                     n_instrumentos: Optional[int] = None,
                     fator_k: int = 1,
                     seed: int = DEFAULT_SEED,
                     query_num_if_path: Optional[str] = None,
                     pk_offset: int = 0,
                     pk_safety_band: int = 0,
                     pk_passo: int = 1,
                     offset_num_if: Optional[int] = None,
                     tratar_como_static: Optional[Set[str]] = None,
                     max_passadas: int = 6,
                     engorda_ts: Optional[datetime] = None,
                     controle_operacional_date: Optional[date] = None,
                     prazo_vencimento_dias: Optional[int] = None,
                     faltantes_arg: Optional[str] = None,
                     faltantes_parquet: Optional[str] = None,
                     poda_subtipo: bool = True,
                     anular_cols: Optional[Mapping[str, Sequence[str]]] = None,
                      oracle_code_batch_size: int = DEFAULT_ORACLE_CODE_BATCH_SIZE,
                      tipo_oracle: Optional[int] = None,
                      somente_ativos: bool = True,
                      dry_run: bool = False,
                      phase: str = "all",
                      plan_uri: Optional[str] = None,
                       planned_artifact: Optional[Mapping[str, Any]] = None,
                       reservation: Optional[Mapping[str, Any]] = None,
                       snapshot_lotes: Optional[Mapping[str, DataFrame]] = None,
                       snapshot_faltantes: Optional[DataFrame] = None,
                       snapshot_lote_counts: Optional[Mapping[str, int]] = None,
                       specs_uri: Optional[str] = None) -> Dict[str, dict]:
    """Roda a sintetização fim a fim; devolve {tabela: estatísticas} (para uso em
    notebook). Aborta sem gravar NADA se qualquer validação falhar.

    Admissão ANTES da sintetização (saída carregável por construção):
      * poda_subtipo (item 1): tira do domínio os NUM_IF que gerariam CONDICAO_IF
        dangling; a amostragem repõe até fechar N;
      * em runs reais, todas as FKs do spec são resolvidas contra pais do mesmo
        cluster sintético e, no residual, contra o Oracle live. Raízes inválidas
        são repostas até fechar N; faltantes_arg/parquet é apenas hint de dry-run.

    O TIPO do instrumento é derivado do lote logo após a seleção e ANTES de
    qualquer alocação no Oracle (ver _deriva_tipo_oracle); tipo_oracle é apenas
    conferência opcional."""
    inicio = time.perf_counter()
    _validate_product_profile(product_profile)
    if phase not in ENGORDA_PHASES:
        raise ValueError(f"phase inválida: {phase!r}")
    if phase == "plan" and dry_run:
        raise ValueError("phase plan não aceita dry_run: a admissão Oracle é obrigatória")
    if phase != "materialize" and (num_ifs is None) == (n_instrumentos is None):
        raise ValueError(
            "informe exatamente uma seleção: num_ifs ou n_instrumentos"
        )
    if phase == "materialize" and (
        snapshot_lotes is None or snapshot_lote_counts is None
    ):
        raise ValueError(
            "materialize exige lotes e contagens carregados do snapshot validado"
        )
    if fator_k < 1:
        raise ValueError("--fator-k deve ser >= 1.")
    if oracle_code_batch_size < 1:
        raise ValueError("--oracle-code-batch-size deve ser >= 1")
    business_policy = product_profile.business_keys
    operation_policy = business_policy.operation
    meu_numero_ordinal_start = 1
    meu_numero_ordinal_end: Optional[int] = None
    requested_meu_numero_prefix = meu_numero_prefix if phase == "plan" else None
    if requested_meu_numero_prefix is not None:
        requested_meu_numero_prefix = _validate_meu_numero_prefix(
            requested_meu_numero_prefix
        )
    if phase == "materialize":
        if planned_artifact is None or reservation is None:
            raise ValueError("materialize exige plano e reserva validados")
        meu_reservation = reservation.get("meu_numero") or {}
        requested_meu_numero_prefix = (
            planned_artifact.get("meu_numero") or {}
        ).get("requested_prefix")
        if meu_reservation.get("count"):
            meu_numero_prefix = str(meu_reservation["prefix"])
            meu_numero_ordinal_start = int(meu_reservation["start"])
            meu_numero_ordinal_end = int(meu_reservation["end"])
    if (phase != "plan" and operation_policy is not None
            and operation_policy.generate_meu_numero):
        meu_numero_prefix = _validate_meu_numero_prefix(meu_numero_prefix)
    elif phase != "plan" and meu_numero_prefix is not None:
        logger.info(
            "Produto %s não gera meu-número; prefixo informado será ignorado.",
            product_profile.name,
        )
    credentials = None if dry_run else _oracle_credentials(config)
    anular_cols = _merge_nullification_mappings(
        product_profile.integrity.nullify_mapping(), anular_cols
    )
    # UM instante para o run inteiro: tabelas diferentes não podem divergir no
    # timestamp só porque foram materializadas em ações Spark diferentes.
    engorda_ts = _normalize_engorda_ts(engorda_ts)
    if product_profile.date_strategy == "standard":
        if credentials is not None:
            if controle_operacional_date is not None and phase != "materialize":
                raise ValueError(
                    "controle_operacional_date só pode ser informado no dry-run")
            if controle_operacional_date is None:
                controle_operacional_date = _read_controle_operacional_date(
                    spark._sc._jvm, *credentials)
        elif controle_operacional_date is None:
            controle_operacional_date = engorda_ts.date()
            logger.warning(
                "Dry-run sem --data-controle-operacional: usando a data da "
                "engorda (%s) apenas para simulação.", controle_operacional_date)
        logger.info("Timestamp de engorda: %s; data operacional: %s "
                    "(prazo de %s: %s)",
                    engorda_ts.isoformat(sep=" "), controle_operacional_date,
                    ENGORDA_COL_DAT_VENCIMENTO,
                    f"{prazo_vencimento_dias} dia(s) fixos"
                    if prazo_vencimento_dias is not None
                    else "preserva o prazo original da linha sintetizada")
    else:
        logger.info("Produto %s não altera colunas de data.", product_profile.name)
    spec = normalize_specs(spec)
    spec_sha256 = hashlib.sha256(_canonical_json(spec).encode("ascii")).hexdigest()
    logger.info("Spec carregado: %d tabela(s); engordáveis (não-static) antes dos "
                "parâmetros: %d.", len(spec),
                sum(1 for cfg in spec.values() if not cfg.get("static")))
    _valida_contrato_nulificacao_seletiva(
        spec, product_profile.integrity.selective_missing_keys
    )
    produto = _normalize_produto(product_profile.name)
    tabelas_produto = {
        table_path_name(table.strip().upper())
        for table in TABELAS_ENGORDA_POR_PRODUTO[produto]
        if table.strip()
    }
    tabelas_ausentes = sorted(tabelas_produto - set(spec))
    if tabelas_ausentes:
        raise ValueError(
            f"Produto {produto}: tabela(s) não encontrada(s) no "
            f"spec: {tabelas_ausentes}"
        )
    for table in tabelas_produto:
        spec[table]["static"] = False
    if tabelas_produto:
        logger.info("Tabelas engordáveis do produto %s: %s",
                    produto, sorted(tabelas_produto))
    estaticas_extra = {
        table_path_name(t.strip().upper())
        for t in (*product_profile.static_tables, *(tratar_como_static or set()))
        if t.strip()
    }
    if estaticas_extra:
        logger.info("Tratando como static por perfil/parâmetro: %s",
                    sorted(estaticas_extra))
        for table in estaticas_extra:
            if table in spec:
                spec[table]["static"] = True

    if phase == "materialize":
        faltantes = snapshot_faltantes
    else:
        faltantes = _carrega_faltantes(
            spark, config, faltantes_arg, faltantes_parquet
        )
        if faltantes is not None:
            logger.info(
                "Faltantes offline carregados: %d chave(s). Em run real o arquivo "
                "não decide a admissão; o Oracle live é autoritativo.",
                faltantes.count(),
            )
    if (not poda_subtipo
            and produto in PRODUTOS_COM_PODA_SUBTIPO
            and product_profile.integrity.subtype is not None):
        logger.warning("Poda de subtipo (item 1) DESLIGADA (--sem-poda-subtipo): "
                       "sintéticos podem ter CONDICAO_IF dangling (Cat 1).")

    if phase == "materialize":
        valores = [int(value) for value in planned_artifact["selected_num_ifs"]]
        requested_count = len(valores)
    else:
        requested_count = len(num_ifs) if num_ifs is not None else int(n_instrumentos)
    # O plano de pertencimento precisa existir durante a admissão FK para ligar
    # filhos transitivos ao NUM_IF dono. O tamanho solicitado basta para o aviso
    # conservador de capacidade; a seleção aceita exatamente esse total.
    planos = monta_plano(
        spark,
        config,
        spec,
        estaticas_extra,
        pk_offset,
        pk_safety_band,
        offset_num_if,
        n_clones_estimado=requested_count * fator_k * 1000,
        pk_passo=pk_passo,
        source_frames=(snapshot_lotes if phase == "materialize" else None),
        frozen_table_plans=(
            planned_artifact["tables"] if phase == "materialize" else None
        ),
    )
    ordem = ordem_topologica(planos)
    logger.info("Ordem de sintetização (%d tabela(s)): %s", len(ordem), ordem)

    selected_lotes: Optional[Dict[str, DataFrame]] = None
    if phase == "materialize":
        selected_lotes = dict(snapshot_lotes)
        if set(selected_lotes) != set(planos):
            raise ValueError(
                "materialize: table_set do snapshot diverge do spec atual: "
                f"snapshot={sorted(selected_lotes)}, spec={sorted(planos)}"
            )
    elif credentials is None:
        logger.warning(
            "Dry-run: admissão live de todas as FKs contra o Oracle não executada; "
            "o resultado é estruturalmente válido, mas parcial quanto a drift."
        )
        with _perf_timer("domain_query_selection", product=product_profile.name):
            valores = seleciona_instrumentos(
                spark,
                config,
                spec,
                num_ifs,
                n_instrumentos,
                seed,
                product_profile,
                query_num_if_path=query_num_if_path,
                faltantes=faltantes,
                poda_subtipo=poda_subtipo,
            )
    else:
        admission_connection = _open_oracle_connection(spark._sc._jvm, *credentials)
        try:
            selection = seleciona_instrumentos_destino(
                spark,
                config,
                spec,
                num_ifs,
                n_instrumentos,
                seed,
                product_profile,
                planos,
                ordem,
                max_passadas,
                existing_key_lookup=lambda table, columns, keys, numeric_flags: (
                    _oracle_existing_parent_keys(
                        spark._sc._jvm,
                        credentials,
                        table,
                        columns,
                        keys,
                        numeric_flags,
                        connection=admission_connection,
                    )
                ),
                query_num_if_path=query_num_if_path,
                # Um emit acumulado pode ficar obsoleto quando o pai chega ao destino.
                # A execução real não pode rejeitar raízes por esse estado histórico.
                faltantes=None,
                poda_subtipo=poda_subtipo,
                somente_ativos=somente_ativos,
                nullify_columns=anular_cols,
            )
        finally:
            admission_connection.close()
        valores = selection.values
        selected_lotes = selection.lotes
        # Também substitui o input offline para a nulificação seletiva: somente
        # ausências confirmadas live podem alterar o sintético desta execução.
        faltantes = selection.missing_keys

    # Tipo do instrumento DERIVADO do lote — antes de qualquer round-trip Oracle.
    # É isto que substitui o antigo literal por produto e o que impede alocar
    # COD_IF de um produto para instrumento de outro.
    if phase == "materialize":
        tipo_derivado = _deriva_tipo_oracle_do_lote(
            selected_lotes[TABELA_RAIZ],
            int(planned_artifact["cod_if"]["oracle_type"]),
        )
    else:
        tipo_derivado = _deriva_tipo_oracle(spark, config, valores, tipo_oracle)
    business_policy = _resolve_business_policy(business_policy, tipo_derivado)
    operation_policy = business_policy.operation

    if selected_lotes is not None:
        lotes = selected_lotes
    else:
        with _perf_timer("closure", product=product_profile.name, roots=len(valores)):
            lotes = calcula_lotes(
                spark,
                config,
                spec,
                planos,
                ordem,
                valores,
                max_passadas,
                somente_ativos=somente_ativos,
            )
    if credentials is not None:
        _apply_oracle_pk_floors(spark._sc._jvm, credentials, planos)

    if phase == "materialize":
        final_lote_counts = {
            table: int(count) for table, count in snapshot_lote_counts.items()
        }
        if set(final_lote_counts) != set(lotes):
            raise ValueError("materialize: contagens do snapshot divergem do table_set")
    else:
        with _perf_timer("final_lote_counts", product=product_profile.name):
            final_lote_counts = _count_final_lotes(lotes)

    current_plan: Optional[dict[str, Any]] = None
    if phase == "plan":
        if not plan_uri:
            raise ValueError("phase plan exige plan_uri")
        selected_lote_descriptor = _create_selected_lote_snapshot(
            spark,
            plan_uri,
            lotes,
            faltantes,
            selected_num_ifs=valores,
            selective_keys=product_profile.integrity.selective_missing_keys,
            lote_counts=final_lote_counts,
        )
    elif phase == "materialize":
        selected_lote_descriptor = planned_artifact["selected_lote"]

    if phase in {"plan", "materialize"}:
        current_plan = _build_engorda_plan(
            config=config,
            specs_uri=specs_uri or config["DATAGEN_SPECS_URI"],
            spec_sha256=spec_sha256,
            product_profile=product_profile,
            valores=valores,
            fator_k=fator_k,
            seed=seed,
            engorda_ts=engorda_ts,
            controle_operacional_date=controle_operacional_date,
            tipo_derivado=tipo_derivado,
            planos=planos,
            lotes=lotes,
            lote_counts=final_lote_counts,
            faltantes_uri=faltantes_parquet,
            query_num_if_uri=query_num_if_path or product_profile.query_filename,
            selected_lote=selected_lote_descriptor,
            frozen_table_plans=(
                planned_artifact["tables"] if phase == "materialize" else None
            ),
            prazo_vencimento_dias=prazo_vencimento_dias,
            anular_cols=anular_cols,
            meu_numero_prefix=requested_meu_numero_prefix,
        )
    if phase == "plan":
        with _perf_timer("plan_artifact_write", product=product_profile.name):
            _write_json_artifact(spark, plan_uri, current_plan)
        for frame in lotes.values():
            frame.unpersist(blocking=False)
        logger.info("Plano imutável %s gravado em %s.", current_plan["plan_id"], plan_uri)
        return {"plan": current_plan}
    if phase == "materialize":
        validated_plan = _validate_plan_artifact(planned_artifact)
        if current_plan != validated_plan:
            raise ValueError(
                "materialize divergiu do plano congelado; RAW/spec/destino/"
                "seleção ou cardinalidades mudaram"
            )
        validated_reservation = _validate_reservation_artifact(
            validated_plan, reservation
        )
        _validate_reservation_live_pk_floors(planos, validated_reservation)
        _inject_reserved_pk_starts(planos, validated_reservation)

    mapeamentos: Dict[str, DataFrame] = {}
    resultados: Dict[str, Tuple[DataFrame, int]] = {}
    stats: Dict[str, dict] = {}
    erros_globais: List[str] = []

    for t in ordem:
        plano = planos[t]
        n_lote = final_lote_counts[t]
        if n_lote == 0:
            logger.info("[%s] lote vazio — materializando sintético e mapa vazios.", t)
        clones, mapa_pk = clona_tabela(spark, plano, lotes[t], fator_k, mapeamentos)
        mapeamentos[t] = mapa_pk
        # Regras de data ANTES do checkpoint/validação: o NOT NULL precisa ser
        # conferido no valor que vai ser gravado, não no valor sintetizado.
        if product_profile.date_strategy == "standard":
            clones, cols_data = aplica_regras_engorda(
                clones, t, engorda_ts=engorda_ts,
                controle_operacional_date=controle_operacional_date,
                prazo_vencimento_dias=prazo_vencimento_dias)
            if t == CONDICAO_IF_TABLE and TABELA_RAIZ in resultados:
                clones, cols_shift = ajusta_datas_condicao_if(
                    clones,
                    lotes[TABELA_RAIZ],
                    resultados[TABELA_RAIZ][0],
                    mapeamentos[TABELA_RAIZ],
                )
                cols_data.extend(cols_shift)
            if t in {"RESGATE", "CONDICAO_RESGATE"}:
                date_context = dict(resultados)
                date_context[t] = (clones, n_lote)
                adjusted, changed = ajusta_datas_resgate(
                    date_context,
                    lotes[TABELA_RAIZ],
                    mapeamentos[TABELA_RAIZ],
                    tabelas=(t,),
                )
                if t in changed:
                    clones = adjusted[t][0]
                    cols_data.append("DAT_RESGATE")
        else:
            cols_data = []
        # Faltantes allowlisted são anulados seletivamente após todo remap e
        # antes de checkpoint, validação e geração de chaves de negócio.
        clones, cols_anuladas_seletivas, contagens_seletivas = (
            aplica_nulificacao_faltantes(
                clones, t, faltantes,
                product_profile.integrity.selective_missing_keys,
            )
        )
        # Anulação integral de colunas de drift (item 2), também antes do
        # checkpoint/validação: a gravação precisa enxergar o valor já nulo.
        clones, cols_anuladas_integrais = aplica_nulificacao(
            clones, t, anular_cols, _not_null_cols(spec[t]))
        cols_anuladas = _colunas_anuladas_resumo(
            cols_anuladas_integrais, cols_anuladas_seletivas
        )
        clones = clones.localCheckpoint(eager=True)  # congela p/ validar e gravar

        cols_remap = sorted({*plano.pk_cols,
                             *(c for fk in plano.fks_remap for c in fk.columns)}
                            & set(clones.columns))
        erros = valida_tabela(spec[t], plano, clones, n_lote, fator_k)
        stats[t] = {"lote": n_lote, "clones": n_lote * fator_k,
                    "colunas_remapeadas": cols_remap,
                    "colunas_data": cols_data, "colunas_anuladas": cols_anuladas,
                    "colunas_anuladas_seletivas": cols_anuladas_seletivas,
                    "faltantes_seletivos": contagens_seletivas,
                    "erros": erros}
        logger.info("[%s] lote=%d sinteticos=%d remapeadas=%s datas=%s anuladas=%s %s",
                    t, n_lote, n_lote * fator_k, cols_remap, cols_data or "-",
                    cols_anuladas or "-",
                    "ERROS: " + "; ".join(erros) if erros else "OK")
        if erros:
            erros_globais.extend(f"{t}: {e}" for e in erros)
        resultados[t] = (clones, n_lote)

    if erros_globais:
        raise ValueError("Validação pré-escrita FALHOU (nada foi gravado):\n  - "
                         + "\n  - ".join(erros_globais))
    _validate_disabled_operation_output(resultados, operation_policy)

    # Relatório de conferência (sai também no --dry-run): chaves original ->
    # nova de 1 instrumento do lote, por tabela, para checagem manual no
    # banco de origem via DBeaver.
    loga_chaves_amostra(ordem, planos, lotes, mapeamentos, valores[0], fator_k)

    save_base = clone_base_path(config)
    if TABELA_RAIZ not in resultados:
        raise ValueError(f"plano precisa produzir {TABELA_RAIZ}")
    if (operation_policy is not None
            and operation_policy.table not in resultados):
        raise ValueError(
            "política de chaves de negócio exige a tabela "
            f"{operation_policy.table} no spec (inclua-a no fecho do produto ou "
            "desligue a política em REGRAS_SCHEMA_CETIP)"
        )

    def _prepare_outputs(output_base: Optional[str], is_dry_run: bool) -> None:
        code_allocation_date = controle_operacional_date or engorda_ts.date()
        instrumentos, n_raiz = resultados[TABELA_RAIZ]
        slots_if = _code_slots(
            instrumentos, COL_NUM_IF, "COD_IF", "NUM_IF_NOVO", "COD_IF_ORIG"
        ).localCheckpoint(eager=True)
        mapa_cod_if = _materialize_code_map(
            spark, slots_if, code_kind="COD_IF",
            generated_alias="COD_IF_GERADO",
            out_path=(None if is_dry_run
                      else f"{output_base}/{MAPA_COD_IF_TABLE}"),
            dry_run=is_dry_run, credentials=credentials,
            batch_size=oracle_code_batch_size,
            engorda_date=code_allocation_date, policy=business_policy)
        instrumentos = _attach_generated_code(
            instrumentos, mapa_cod_if, pk_col=COL_NUM_IF,
            new_pk_alias="NUM_IF_NOVO", code_col="COD_IF",
            generated_alias="COD_IF_GERADO").localCheckpoint(eager=True)
        resultados[TABELA_RAIZ] = (instrumentos, n_raiz)

        operacoes: Optional[DataFrame] = None
        if operation_policy is not None:
            operation_table = operation_policy.table
            operacoes, n_operacoes = resultados[operation_table]
            operacoes = _propagate_root_cod_if(instrumentos, operacoes)

            slots_operacao = _code_slots(
                operacoes, "NUM_ID_OPERACAO", "COD_OPERACAO",
                "NUM_ID_OPERACAO_NOVO", "COD_OPERACAO_ORIG"
            ).localCheckpoint(eager=True)
            mapa_cod_operacao = _materialize_code_map(
                spark, slots_operacao, code_kind="COD_OPERACAO",
                generated_alias="COD_OPERACAO_GERADO",
                out_path=(None if is_dry_run
                          else f"{output_base}/{MAPA_COD_OPERACAO_TABLE}"),
                dry_run=is_dry_run, credentials=credentials,
                batch_size=oracle_code_batch_size,
                engorda_date=code_allocation_date, policy=business_policy)
            operacoes = _attach_generated_code(
                operacoes, mapa_cod_operacao, pk_col="NUM_ID_OPERACAO",
                new_pk_alias="NUM_ID_OPERACAO_NOVO", code_col="COD_OPERACAO",
                generated_alias="COD_OPERACAO_GERADO")
            if operation_policy.generate_meu_numero:
                operacoes = _generate_meu_numeros(
                    operacoes,
                    meu_numero_prefix,
                    engorda_ts.date(),
                    ordinal_start=meu_numero_ordinal_start,
                    ordinal_end=meu_numero_ordinal_end,
                )
            operacoes = operacoes.localCheckpoint(eager=True)
            resultados[operation_table] = (operacoes, n_operacoes)

        _validate_business_keys(instrumentos, operacoes, business_policy)

        if (not is_dry_run and operation_policy is not None
                and operation_policy.generate_meu_numero):
            if (credentials is None or meu_numero_prefix is None
                    or operacoes is None):
                raise RuntimeError(
                    "credenciais Oracle, prefixo e operações são obrigatórios "
                    "no preflight"
                )
            preflight_path = f"{output_base}/__PREFLIGHT_MEU"
            try:
                existing = _read_existing_meu_tuples(
                    spark, credentials, engorda_date=engorda_ts.date(),
                    prefix=meu_numero_prefix, temp_path=preflight_path)
                _assert_no_meu_collisions(operacoes, existing)
            finally:
                _delete_path(spark, preflight_path)

        if is_dry_run:
            return
        if output_base is None:
            raise RuntimeError("output_base ausente fora do dry-run")
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
        _prepare_outputs(None, True)
        logger.info("--dry-run: validações OK; NADA gravado (destino seria %s).",
                    save_base)
    else:
        save_base = _valida_destino(config)
        require_absent = bool(config.get("DATAGEN_OUTPUT_URI"))
        if require_absent:
            _assert_exact_output_absent(spark, save_base)
        _stage_and_publish(
            spark, save_base,
            lambda staging_base: _prepare_outputs(staging_base, False),
            require_absent=require_absent,
        )
        logger.info("Staging validado e publicado em %s.", save_base)

    logger.info("=" * 78)
    logger.info("RESUMO DA SINTETIZAÇÃO (%.1fs) — produto %s, %s=%d, "
                "%d instrumento(s) × K=%d, data de engorda %s, %s",
                time.perf_counter() - inicio, product_profile.name,
                COL_NUM_TIPO_IF, tipo_derivado, len(valores), fator_k,
                engorda_ts.isoformat(sep=" "),
                "DRY-RUN (nada gravado)" if dry_run else f"gravado em {save_base}")
    for t in ordem:
        s = stats.get(t, {})
        logger.info("  %-32s lote=%-8s sinteticos=%-8s remap=%s datas=%s anuladas=%s",
                    t, s.get("lote", "-"), s.get("clones", "-"),
                    ",".join(s.get("colunas_remapeadas", [])) or "-",
                    ",".join(s.get("colunas_data", [])) or "-",
                    ",".join(s.get("colunas_anuladas", [])) or "-")
    logger.info("=" * 78)
    _loga_contagens_dominio(spark, config, dry_run)
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
    # Os lotes de sintético são pequenos (N instrumentos × K); um shuffle.partitions
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
# API pública de execução. CLI e executar_engorda.py usam o mesmo caminho.
# ---------------------------------------------------------------------------
def _validate_engorda_job(job: EngordaJob) -> ProductProfile:
    if not isinstance(job, EngordaJob):
        raise TypeError("job precisa ser uma instância de EngordaJob")
    for field_name in ("query_num_if_path", "specs_uri", "clone_prefix",
                       "cod_if_pattern", "cod_if_dry_prefix", "plan_uri",
                       "reservation_uri", "raw_uri", "output_uri"):
        value = getattr(job, field_name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{field_name} precisa ser texto não vazio")
    if job.cod_if_pattern is not None:
        try:
            re.compile(job.cod_if_pattern)
        except re.error as exc:
            raise ValueError(f"cod_if_pattern inválido: {exc}") from exc
    if job.tipo_oracle is not None and (
            type(job.tipo_oracle) is not int or job.tipo_oracle < 1):
        raise ValueError("tipo_oracle deve ser inteiro >= 1")
    # O perfil é construído já com os overrides: assim a compatibilidade
    # pattern × prefixo dry-run falha no startup, e não no meio do run.
    profile = get_product_profile(
        job.produto,
        query_filename=job.query_num_if_path,
        clone_prefix=job.clone_prefix,
        cod_if_pattern=job.cod_if_pattern,
        cod_if_dry_prefix=job.cod_if_dry_prefix,
        tipo_oracle=job.tipo_oracle,
    )
    if job.phase not in ENGORDA_PHASES:
        raise ValueError(f"phase inválida: {job.phase!r}")
    if (job.phase in {"all", "plan"}
            and (job.num_ifs is None) == (job.n_instrumentos is None)):
        raise ValueError(
            "informe exatamente um entre num_ifs e n_instrumentos"
        )
    if job.phase == "materialize" and (
        job.num_ifs is not None or job.n_instrumentos is not None
    ):
        raise ValueError("materialize consome a seleção do plano e não reamostra")
    if job.phase in {"plan", "materialize"} and (
        not job.plan_uri or not job.raw_uri or not job.output_uri
    ):
        raise ValueError(f"phase {job.phase} exige plan_uri/raw_uri/output_uri")
    if job.phase == "materialize" and not job.reservation_uri:
        raise ValueError("phase materialize exige reservation_uri")
    if job.num_ifs is not None:
        if not isinstance(job.num_ifs, (tuple, list)):
            raise ValueError("num_ifs precisa ser uma sequência de inteiros")
        if not job.num_ifs:
            raise ValueError("num_ifs não pode ser vazio")
        if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0
               for value in job.num_ifs):
            raise ValueError("num_ifs deve conter apenas inteiros positivos")
        if len(set(job.num_ifs)) != len(job.num_ifs):
            raise ValueError("num_ifs não pode conter valores duplicados")
    if job.n_instrumentos is not None:
        if type(job.n_instrumentos) is not int or job.n_instrumentos < 1:
            raise ValueError("n_instrumentos deve ser inteiro >= 1")
    for field_name in ("fator_k", "pk_passo", "max_passadas",
                       "oracle_code_batch_size"):
        value = getattr(job, field_name)
        if type(value) is not int or value < 1:
            raise ValueError(f"{field_name} deve ser inteiro >= 1")
    for field_name in ("pk_offset", "pk_safety_band"):
        value = getattr(job, field_name)
        if type(value) is not int or value < 0:
            raise ValueError(f"{field_name} deve ser inteiro >= 0")
    if type(job.seed) is not int:
        raise ValueError("seed deve ser inteiro")
    for field_name in ("offset_num_if", "prazo_vencimento_dias"):
        value = getattr(job, field_name)
        if value is not None and (type(value) is not int or value < 1):
            raise ValueError(f"{field_name} deve ser inteiro >= 1")
    if not isinstance(job.tratar_como_static, (tuple, list, set)) or any(
            not isinstance(table, str) or not table.strip()
            for table in job.tratar_como_static):
        raise ValueError("tratar_como_static deve conter nomes de tabela")
    runtime_static = {
        table_path_name(table.strip().upper())
        for table in job.tratar_como_static
    }
    protected_tables = {TABELA_RAIZ}
    operation_policy = profile.business_keys.operation
    if operation_policy is not None:
        protected_tables.add(operation_policy.table)
    forbidden = sorted(runtime_static & protected_tables)
    if forbidden:
        raise ValueError(f"tabela(s) obrigatória(s) não podem ser static: {forbidden}")
    if job.engorda_ts is not None and not isinstance(job.engorda_ts, datetime):
        raise ValueError("engorda_ts precisa ser datetime")
    if (job.controle_operacional_date is not None
            and (not isinstance(job.controle_operacional_date, date)
                 or isinstance(job.controle_operacional_date, datetime))):
        raise ValueError("controle_operacional_date precisa ser date")
    if (job.controle_operacional_date is not None and not job.dry_run
            and job.phase != "materialize"):
        raise ValueError("controle_operacional_date só pode ser informado no dry-run")
    for field_name in ("poda_subtipo", "dry_run", "somente_ativos"):
        if type(getattr(job, field_name)) is not bool:
            raise ValueError(f"{field_name} precisa ser booleano")
    if job.anular_cols is not None:
        _merge_nullification_mappings(
            profile.integrity.nullify_mapping(), job.anular_cols
        )
    if (job.phase == "all" and operation_policy is not None
            and operation_policy.generate_meu_numero):
        try:
            _validate_meu_numero_prefix(job.meu_numero_prefix)
        except argparse.ArgumentTypeError as exc:
            raise ValueError(str(exc)) from exc
    return profile


def executar_job(job: EngordaJob) -> Dict[str, dict]:
    """Executa um job configurado sem duplicar bootstrap entre CLI e runner."""
    profile = _validate_engorda_job(job)
    config = dict(get_engorda_env(
        job.specs_uri,
        raw_uri_override=job.raw_uri,
        output_uri_override=job.output_uri,
    ))
    if job.clone_prefix is not None:
        config["DATAGEN_CLONE_PREFIX"] = _normalize_clone_prefix(job.clone_prefix)
    elif not os.environ.get("DATAGEN_CLONE_PREFIX"):
        config["DATAGEN_CLONE_PREFIX"] = _normalize_clone_prefix(
            profile.default_clone_prefix
        )
    else:
        config["DATAGEN_CLONE_PREFIX"] = _normalize_clone_prefix(
            config["DATAGEN_CLONE_PREFIX"]
        )
    logger.info(
        "Job produto=%s phase=%s query=%s specs=%s destino=%s tipo_oracle=%s "
        "dry_run=%s",
        profile.name,
        job.phase,
        job.query_num_if_path or profile.query_filename,
        job.specs_uri or config["DATAGEN_SPECS_URI"],
        clone_base_path(config),
        job.tipo_oracle if job.tipo_oracle is not None else "derivado do lote",
        job.dry_run,
    )

    spark = create_spark_session(f"DataGenEngorda_{profile.name}")
    try:
        specs_uri = job.specs_uri or config["DATAGEN_SPECS_URI"]
        planned_artifact = None
        reservation = None
        snapshot_lotes = None
        snapshot_faltantes = None
        snapshot_lote_counts = None
        num_ifs = list(job.num_ifs) if job.num_ifs is not None else None
        n_instrumentos = job.n_instrumentos
        fator_k = job.fator_k
        seed = job.seed
        engorda_ts = job.engorda_ts
        controle_operacional_date = job.controle_operacional_date
        tipo_oracle = job.tipo_oracle
        prazo_vencimento_dias = job.prazo_vencimento_dias
        anular_cols = job.anular_cols
        if job.phase == "materialize":
            planned_artifact = _validate_plan_artifact(
                _read_json_artifact(spark, job.plan_uri)
            )
            reservation = _validate_reservation_artifact(
                planned_artifact,
                _read_json_artifact(spark, job.reservation_uri),
            )
            if planned_artifact["product"] != profile.name:
                raise ValueError("produto do plano diverge do job")
            lineage = {
                "raw_uri": _area(
                    config["DATAGEN_RAW_BASE_URI"], config.get("DATAGEN_RAW_PREFIX")
                ),
                "output_uri": clone_base_path(config),
                "specs_uri": specs_uri,
                "faltantes_uri": job.faltantes_parquet,
                "query_num_if_uri": job.query_num_if_path or profile.query_filename,
            }
            mismatches = {
                key: (planned_artifact[key], value)
                for key, value in lineage.items()
                if planned_artifact[key] != value
            }
            if mismatches:
                raise ValueError(f"lineage do materialize diverge do plano: {mismatches}")
            snapshot_lotes, snapshot_faltantes, snapshot_lote_counts = (
                _load_selected_lote_snapshot(
                    spark,
                    job.plan_uri,
                    planned_artifact["selected_lote"],
                    expected_tables=set(planned_artifact["tables"]),
                    selected_num_ifs=planned_artifact["selected_num_ifs"],
                    selective_keys=profile.integrity.selective_missing_keys,
                )
            )
            num_ifs = None
            n_instrumentos = None
            fator_k = int(planned_artifact["fator_k"])
            seed = int(planned_artifact["seed"])
            engorda_ts = datetime.fromisoformat(planned_artifact["engorda_timestamp"])
            frozen_date = planned_artifact["controle_operacional_date"]
            controle_operacional_date = (
                date.fromisoformat(frozen_date) if frozen_date is not None else None
            )
            tipo_oracle = int(planned_artifact["cod_if"]["oracle_type"])
            prazo_vencimento_dias = planned_artifact.get("prazo_vencimento_dias")
            anular_cols = planned_artifact.get("anular_cols") or None
        spec = load_specs(spark, specs_uri)
        return executa_clonagem(
            spark, config, spec,
            product_profile=profile,
            meu_numero_prefix=job.meu_numero_prefix,
            num_ifs=num_ifs,
            n_instrumentos=n_instrumentos,
            fator_k=fator_k,
            seed=seed,
            query_num_if_path=job.query_num_if_path,
            pk_offset=job.pk_offset,
            pk_safety_band=job.pk_safety_band,
            pk_passo=job.pk_passo,
            offset_num_if=job.offset_num_if,
            tratar_como_static=set(job.tratar_como_static),
            max_passadas=job.max_passadas,
            engorda_ts=engorda_ts,
            controle_operacional_date=controle_operacional_date,
            prazo_vencimento_dias=prazo_vencimento_dias,
            faltantes_arg=job.faltantes_arg,
            faltantes_parquet=job.faltantes_parquet,
            poda_subtipo=job.poda_subtipo,
            anular_cols=_merge_nullification_mappings(
                profile.integrity.nullify_mapping(), anular_cols
            ),
            oracle_code_batch_size=job.oracle_code_batch_size,
            tipo_oracle=tipo_oracle,
            somente_ativos=job.somente_ativos,
            dry_run=job.dry_run,
            phase=job.phase,
            plan_uri=job.plan_uri,
            planned_artifact=planned_artifact,
            reservation=reservation,
            snapshot_lotes=snapshot_lotes,
            snapshot_faltantes=snapshot_faltantes,
            snapshot_lote_counts=snapshot_lote_counts,
            specs_uri=specs_uri,
        )
    finally:
        spark.stop()


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


def _parse_produto(value: str) -> str:
    """Produto presente em TABELAS_ENGORDA_POR_PRODUTO."""
    try:
        return _normalize_produto(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parse_regex(value: str) -> str:
    texto = value.strip()
    if not texto:
        raise argparse.ArgumentTypeError("padrão não pode ser vazio")
    try:
        re.compile(texto)
    except re.error as exc:
        raise argparse.ArgumentTypeError(f"regex inválida: {exc}") from exc
    return texto


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


def _parse_controle_operacional_date(txt: str) -> date:
    try:
        return date.fromisoformat(txt.strip())
    except ValueError:
        raise argparse.ArgumentTypeError(
            "--data-controle-operacional deve ser 'YYYY-MM-DD'") from None


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Motor multi-produto de sintetização por entidade "
                    "(synthesize-and-remap), dirigido por query SQL, "
                    "spec_config.json, produto e parâmetros.")
    parser.add_argument(
        "--produto", required=True, type=_parse_produto,
        help="Produto que seleciona a query no catálogo SQL, as tabelas "
             "engordáveis no spec único e o default de --clone-prefix "
             f"({DEFAULT_CLONE_PREFIX}/<produto>).",
    )
    parser.add_argument("--phase", choices=ENGORDA_PHASES, default="all")
    grupo = parser.add_mutually_exclusive_group(required=False)
    grupo.add_argument("--num-ifs", type=_parse_num_ifs, default=None,
                       help="Lista explícita de NUM_IF (ex.: 123,456). Aceita 1 só.")
    grupo.add_argument("--n-instrumentos", type=positive_int, default=None,
                       help="Sorteia N instrumentos do domínio definido pela "
                            "query SQL; usa --seed.")
    parser.add_argument("--query-num-if-sql", dest="query_num_if_path", default=None,
                        help="Override do catálogo SQL único de produtos; caminho "
                             "local ou URI. Default: queries_produtos.sql ao lado "
                             "do script. O bloco de --produto deve retornar NUM_IF.")
    parser.add_argument("--fator-k", type=positive_int, default=1,
                        help="Sintéticos por instrumento (default 1).")
    parser.add_argument("--meu-numero-prefix", type=_validate_meu_numero_prefix,
                        default=None,
                        help="Prefixo de 3 dígitos (primeiro 1-9); obrigatório "
                             "enquanto a política de OPERACAO gerar meu-número.")
    parser.add_argument("--tipo-oracle", type=positive_int, default=None,
                        help=f"OPCIONAL. O {COL_NUM_TIPO_IF} é DERIVADO das linhas "
                             "do lote; informe apenas para CONFERIR (diverge -> "
                             "aborta) ou para escolher o tipo da alocação num lote "
                             "legitimamente multi-tipo.")
    parser.add_argument("--cod-if-padrao", dest="cod_if_pattern",
                        type=_parse_regex, default=None,
                        help="OPCIONAL. Aperta a validação estrutural do COD_IF "
                             f"(default {DEFAULT_COD_IF_PATTERN!r}, agnóstico de "
                             "produto). Ajuste --cod-if-dry-prefix junto.")
    parser.add_argument("--cod-if-dry-prefix", dest="cod_if_dry_prefix",
                        default=None,
                        help="OPCIONAL. Prefixo do placeholder de COD_IF no "
                             f"--dry-run (default {DEFAULT_COD_IF_DRY_PREFIX!r}). "
                             "Precisa casar com --cod-if-padrao.")
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
                             "entre cada sintético. Só vale para PK OFFSET_PROPRIO.")
    parser.add_argument("--offset-num-if", type=positive_int, default=None,
                        help="Início explícito do NUM_IF novo (> max real).")
    parser.add_argument("--data-engorda", type=_parse_data_engorda, default=None,
                        help="Data/hora do run usada nas colunas DAT_* "
                             "('YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS'). "
                              "Default: instante de início do script.")
    parser.add_argument(
        "--data-controle-operacional",
        type=_parse_controle_operacional_date,
        default=None,
        help="Override de CETIP.CONTROLE_OPERACIONAL.DAT_CTL_OPER apenas para "
             "dry-run (YYYY-MM-DD). Execuções reais sempre consultam NUM_ORDEM=0 "
             "e NUM_SISTEMA IS NULL; dry-run sem valor usa a data da engorda.",
    )
    parser.add_argument("--prazo-vencimento-dias", type=positive_int, default=None,
                        help=f"{ENGORDA_COL_DAT_VENCIMENTO} = data operacional + N "
                             "dias. Default: preserva o prazo original da linha "
                             f"sintetizada ({ENGORDA_COL_DAT_VENCIMENTO} - "
                             f"{ENGORDA_COL_DAT_EMISSAO}); prazo inválido cai em "
                             f"{DEFAULT_DT_VENCIMENTO_PRAZO_DIAS} dias.")
    parser.add_argument("--tratar-como-static", default="",
                        help="Tabelas a excluir da sintetização (vírgula).")
    parser.add_argument("--max-passadas", type=positive_int, default=6,
                        help="Passadas máximas do pertencimento (ciclos de FK). "
                             "Não estabilizou -> aborta pedindo aumento. Default 6.")
    parser.add_argument("--sem-poda-subtipo", action="store_true",
                        help="DESLIGA a poda de domínio do item 1 (por padrão os "
                             "NUM_IF que gerariam CONDICAO_IF dangling são tirados "
                             "do domínio e repostos por outra amostra). Use só p/ "
                             "depurar — o sintético pode sair com dangling (Cat 1).")
    parser.add_argument("--sem-filtro-ativos", action="store_true",
                        help="DESLIGA o filtro de linhas logicamente excluídas no "
                             "fecho (por padrão CONDICAO_IF/RESGATE/CONDICAO_RESGATE "
                             "entram só com DAT_EXCLUSAO nula / IND_EXCLUIDO<>S). "
                             "Use só p/ depurar: sem o filtro, cronogramas de "
                             "resgate saem com pai inativo.")
    parser.add_argument("--faltantes-arg", default=None,
                        help="Hint offline de chaves inexistentes, usado na poda "
                             "do dry-run. Runs reais ignoram este estado histórico "
                             "e consultam todas as FKs no Oracle live. Formato: "
                             "'TABELA.COLUNA=v1,v2;TAB2.COL2=v3'. Os NUM_IF que "
                             "as referenciam são podados do domínio. Ex.: "
                             "'CARTEIRA_COMITENTE.NUM_ID_ENTIDADE=343..;"
                             "CARTEIRA_COMITENTE.NUM_CONTA=95..'.")
    parser.add_argument("--faltantes-parquet", default=None,
                        help="Hint offline Parquet TABELA/COLUNA/VALOR para dry-run; "
                             "não é autoritativo em execução real.")
    parser.add_argument("--anular-cols", default=None,
                        help="Item 2 (override/extra): colunas nullable a ANULAR "
                             "nos sintéticos, formato 'TABELA.COL,COL2;TAB2.COL3'. "
                             "Somam-se às colunas declaradas pelo schema.")
    parser.add_argument(
        "--clone-prefix", default=None,
        help="Prefixo exclusivo de saída. Precedência: argumento, env, "
             f"default {DEFAULT_CLONE_PREFIX}/<produto>. Dois produtos NÃO podem "
             "compartilhar o mesmo prefixo — o segundo run publica por cima.",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Valida e loga; não grava nada.")
    parser.add_argument("--specs", default=None,
                        help="Override de DATAGEN_SPECS_URI (specs.json único). "
                             "É o spec que DEFINE quais tabelas são engordadas: "
                             "as não-static presentes nele.")
    parser.add_argument("--plan-uri", default=None)
    parser.add_argument("--reservation-uri", default=None)
    parser.add_argument("--raw-uri", default=None)
    parser.add_argument("--output-uri", default=None)
    args = parser.parse_args(argv)
    has_selection = (args.num_ifs is not None) + (args.n_instrumentos is not None)
    if args.phase in {"all", "plan"} and has_selection != 1:
        parser.error(f"--phase {args.phase} exige exatamente uma seleção")
    if args.phase == "materialize" and has_selection:
        parser.error("--phase materialize usa o plano congelado e rejeita reamostragem")
    if args.phase in {"plan", "materialize"} and not args.plan_uri:
        parser.error(f"--phase {args.phase} exige --plan-uri")
    if args.phase == "materialize" and not args.reservation_uri:
        parser.error("--phase materialize exige --reservation-uri")
    if args.phase in {"plan", "materialize"} and (
        not args.raw_uri or not args.output_uri
    ):
        parser.error(f"--phase {args.phase} exige --raw-uri e --output-uri")
    return args


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


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_arguments(argv)
    executar_job(EngordaJob(
        produto=args.produto,
        num_ifs=tuple(args.num_ifs) if args.num_ifs is not None else None,
        n_instrumentos=args.n_instrumentos,
        fator_k=args.fator_k,
        meu_numero_prefix=args.meu_numero_prefix,
        query_num_if_path=args.query_num_if_path,
        seed=args.seed,
        pk_offset=args.pk_offset,
        pk_safety_band=args.pk_safety_band,
        pk_passo=args.pk_passo,
        offset_num_if=args.offset_num_if,
        tratar_como_static=tuple(
            table for table in args.tratar_como_static.split(",") if table.strip()
        ),
        max_passadas=args.max_passadas,
        engorda_ts=args.data_engorda,
        controle_operacional_date=args.data_controle_operacional,
        prazo_vencimento_dias=args.prazo_vencimento_dias,
        faltantes_arg=args.faltantes_arg,
        faltantes_parquet=args.faltantes_parquet,
        poda_subtipo=not args.sem_poda_subtipo,
        somente_ativos=not args.sem_filtro_ativos,
        anular_cols=(
            _merge_anular_cols({}, args.anular_cols)
            if args.anular_cols else None
        ),
        oracle_code_batch_size=args.oracle_code_batch_size,
        tipo_oracle=args.tipo_oracle,
        cod_if_pattern=args.cod_if_pattern,
        cod_if_dry_prefix=args.cod_if_dry_prefix,
        dry_run=args.dry_run,
        specs_uri=args.specs,
        clone_prefix=args.clone_prefix,
        phase=args.phase,
        plan_uri=args.plan_uri,
        reservation_uri=args.reservation_uri,
        raw_uri=args.raw_uri,
        output_uri=args.output_uri,
    ))


# ---------------------------------------------------------------------------
# Validação FAIL-AT-IMPORT: o perfil canônico é compilado e validado assim que o
# módulo carrega. Um erro em REGRAS_SCHEMA_CETIP quebra o import, não o meio do
# run — mesma garantia que existia com o registro por produto.
# ---------------------------------------------------------------------------
CDB_SIMPLIFICADO_PROFILE = get_product_profile("cdb_simplificado")


if __name__ == "__main__":
    main()
