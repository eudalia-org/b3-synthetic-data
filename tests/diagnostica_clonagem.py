#!/usr/bin/env python3
"""
diagnostica_clonagem.py — Etapa 1 (DIAGNÓSTICO) da clonagem por entidade.

Antes de escrever o clonador (clona_instrumentos.py), este script consulta o
DICIONÁRIO do Oracle (ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES,
ALL_IND_COLUMNS, ALL_IND_EXPRESSIONS, ALL_TAB_COLUMNS) para as tabelas do
fecho referencial (spec_config.json, gerado por gera_spec_config.py) e produz:

  (a) diag_unicidade.csv  — unique constraints e unique indexes que NÃO são a
      PK, com veredicto por chave: se alguma coluna da chave será remapeada
      pelo clone-and-remap (PK surrogate com offset ou FK para pai clonado),
      a unicidade é preservada por construção; senão, é coluna de código
      externo/negócio que estouraria ORA-00001 se apenas copiada e PRECISA de
      regra de regeneração na Etapa 2.

  (a2) diag_pks.csv — classificação da PK de CADA tabela do fecho: remapeada
      via FK (PK compartilhada/composta com componente de FK), surrogate
      numérica que recebe offset próprio (item 4 do plano), ou PK sem
      componente remapeado (ORA-00001 garantido se apenas copiada — decidir).

  (b) diag_referencias_instrumento.csv — self-references e FKs entre
      instrumentos: toda FK cujo pai é INSTRUMENTO_FINANCEIRO (ex.: colunas
      tipo NUM_IF_ORIGEM), FKs da própria INSTRUMENTO_FINANCEIRO para seus
      pais, e FKs auto-referentes de qualquer tabela do fecho — para decidir
      se o instrumento referenciado entra no clone ou mantém o valor original.

  (c) diag_fk_not_null.csv — colunas NOT NULL (dicionário) que participam de
      qualquer FK do fecho: NUNCA podem ser anuladas pelo clonador.

  (d) diag_divergencias.csv — divergências entre o spec_config.json (o que o
      clonador VAI remapear) e o dicionário (a verdade do banco): FK que só
      existe no dicionário (coluna NÃO seria remapeada -> órfã lógica ou
      colisão de PK composta), FK que só existe no spec, PK divergente, FK
      apontando para UNIQUE (não PK) de pai clonado, FK desabilitada e tabela
      do spec ausente no owner.

  Entradas do diagnóstico (exportadas do DBeaver no PASSO 2):
      dic_constraints_pk_uk_fk.csv, dic_indices_unicos.csv, dic_colunas.csv

FLUXO (SEM conexão programática ao Oracle — o acesso ao banco é só via
DBeaver; os DADOS estão clonados em Parquet no Object Storage, mas os
METADADOS de constraint/índice/NULLABLE só existem no dicionário do Oracle,
por isso estas três queries precisam rodar lá uma única vez):

  PASSO 1 — gerar as queries (nenhuma conexão é aberta):
      python diagnostica_clonagem.py --spec spec_config.json --owner CETIP
    Gera em ./sql_diagnostico/:
      01_constraints_pk_uk_fk.sql, 02_indices_unicos.sql, 03_colunas.sql
    (IN-list literal com as tabelas do fecho, prontas para abrir no DBeaver).

  PASSO 2 — rodar cada .sql no DBeaver e exportar o resultado como CSV:
    clique-direito no grid > Export resultset... > CSV, mantendo o CABEÇALHO;
    delimitador vírgula OU ponto-e-vírgula (ambos aceitos); NULL como vazio
    (default do DBeaver). Salvar na MESMA pasta com os nomes exatos:
      dic_constraints_pk_uk_fk.csv, dic_indices_unicos.csv, dic_colunas.csv

  PASSO 3 — análises offline (gera os diag_*.csv + resumo no log).
    Num NOTEBOOK (com spec_config.json e os 3 CSVs na mesma pasta do kernel):
      from diagnostica_clonagem import diagnostica
      resultado = diagnostica(spec="spec_config.json", owner="CETIP")
    A mesma chamada serve para os dois passos: se os 3 CSVs ainda NÃO
    existirem na pasta, ela gera os .sql do PASSO 1 e para; se existirem,
    roda as análises. Em terminal:
      python diagnostica_clonagem.py --spec spec_config.json --de-csvs <pasta>

GARANTIA: as queries geradas são apenas SELECT no dicionário (nenhuma linha
de DADOS das tabelas é lida) e nada é alterado no banco.

Comentários e logs em português, seguindo o padrão de engorda_tables.py (os
helpers de spec — table_path_name / _fk_list / static — espelham os de lá para
que o mesmo spec_config.json sirva aos dois sem conversão).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

# Mesmo padrão de logging de engorda_tables.py: INFO em stdout.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes do domínio (espelham engorda_tables.py / gera_spec_config.py).
# ---------------------------------------------------------------------------
TABELA_RAIZ = "INSTRUMENTO_FINANCEIRO"
COL_NUM_IF = "NUM_IF"

# Limite prático do Oracle para lista IN é 1000 expressões; 500 dá folga.
_CHUNK_IN = 500

# Nomes de objeto aceitos nas IN-lists (defensivo: os nomes vêm do spec, mas
# um nome exótico com aspas/minúsculas não deve virar SQL malformado).
_NOME_OBJETO_VALIDO = re.compile(r"^[A-Z0-9_\$#]+$")

# Tipos numéricos do Oracle elegíveis a PK surrogate com offset (item 4 do
# plano). BINARY_FLOAT/DOUBLE ficam de fora de propósito (imprecisão).
_TIPOS_NUMERICOS = {"NUMBER", "FLOAT", "INTEGER"}


# ---------------------------------------------------------------------------
# Leitura do spec_config.json (espelha normalize_specs/_fk_list de
# engorda_tables.py, sem depender de pyspark).
# ---------------------------------------------------------------------------
def table_path_name(table: str) -> str:
    """Remove prefixo de schema ("CETIP.TABELA" -> "TABELA") — igual a
    engorda_tables.table_path_name."""
    return table.split(".", 1)[1] if "." in table else table


def _fk_list(cfg: dict) -> List[dict]:
    """Lista de FKs do spec, aceitando as chaves 'foreign_keys' e 'fks' —
    igual a engorda_tables._fk_list."""
    fks = cfg.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = cfg.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def carrega_spec(caminho: str) -> Dict[str, dict]:
    """Carrega e normaliza o spec_config.json: chaves em MAIÚSCULAS e sem
    prefixo de schema, parent_table idem. Não valida FKs aqui — a comparação
    com o dicionário é justamente uma das saídas (diag_divergencias)."""
    with open(caminho, encoding="utf-8") as f:
        bruto = json.load(f)
    if not isinstance(bruto, dict) or not bruto:
        raise ValueError(f"spec `{caminho}` precisa ser um objeto JSON não-vazio.")

    spec: Dict[str, dict] = {}
    for nome_bruto, cfg in bruto.items():
        nome = table_path_name(str(nome_bruto).strip().upper())
        if nome in spec:
            raise ValueError(
                f"Colisão de chave no spec após remover prefixo de schema: `{nome}`.")
        novo = dict(cfg) if isinstance(cfg, dict) else {}
        fks_norm = []
        for fk in _fk_list(novo):
            fk = dict(fk)
            fk["columns"] = [str(c).strip().upper() for c in (fk.get("columns") or [])]
            fk["parent_columns"] = [
                str(c).strip().upper() for c in (fk.get("parent_columns") or [])]
            fk["parent_table"] = table_path_name(
                str(fk.get("parent_table", "")).strip().upper())
            fks_norm.append(fk)
        novo["foreign_keys"] = fks_norm
        novo.pop("fks", None)
        novo["pk_cols"] = [str(c).strip().upper() for c in (novo.get("pk_cols") or [])]
        spec[nome] = novo
    return spec


# ---------------------------------------------------------------------------
# Queries do dicionário (executadas POR VOCÊ no DBeaver — este script não
# abre conexão com o Oracle; ver FLUXO no docstring).
#
# Cada query recebe owner + IN-list literal das tabelas do fecho, em chunks
# de _CHUNK_IN. Os JOINs de resolução do pai (rc/rcc) NÃO são restritos à
# IN-list de propósito: um pai fora do fecho precisa aparecer (é justamente
# um dos achados — FK do dicionário cujo pai o spec não cobre).
# ---------------------------------------------------------------------------
def _sql_constraints(marcadores: str) -> str:
    # P/U/R das tabelas do fecho, uma linha por coluna, com o pai resolvido
    # (para R): rc = constraint referenciada (PK ou UNIQUE do pai), rcc = a
    # coluna do pai na MESMA posição da coluna filha.
    return f"""
SELECT
    c.table_name,
    c.constraint_name,
    c.constraint_type,
    c.status,
    c.validated,
    c.delete_rule,
    c.index_name,
    cc.column_name,
    cc.position,
    c.r_owner,
    rc.table_name        AS parent_table,
    rc.constraint_type   AS parent_constraint_type,
    rcc.column_name      AS parent_column
FROM all_constraints c
JOIN all_cons_columns cc
  ON cc.owner = c.owner
 AND cc.constraint_name = c.constraint_name
 AND cc.table_name = c.table_name
LEFT JOIN all_constraints rc
  ON rc.owner = c.r_owner
 AND rc.constraint_name = c.r_constraint_name
LEFT JOIN all_cons_columns rcc
  ON rcc.owner = rc.owner
 AND rcc.constraint_name = rc.constraint_name
 AND rcc.position = cc.position
WHERE c.owner = :owner
  AND c.constraint_type IN ('P', 'U', 'R')
  AND c.table_name IN ({marcadores})
ORDER BY c.table_name, c.constraint_name, cc.position
""".strip()


def _sql_indices_unicos(marcadores: str) -> str:
    # Índices UNIQUE das tabelas do fecho, uma linha por coluna. Índices que
    # apenas suportam a PK/UNIQUE constraint são excluídos DEPOIS, no Python
    # (via all_constraints.index_name e por igualdade do conjunto de colunas).
    # column_expression (LONG) traz a expressão de índice function-based —
    # nesse caso column_name vem como coluna virtual SYS_NC...
    return f"""
SELECT
    i.table_name,
    i.index_name,
    i.uniqueness,
    i.status,
    ic.column_name,
    ic.column_position,
    ie.column_expression
FROM all_indexes i
JOIN all_ind_columns ic
  ON ic.index_owner = i.owner
 AND ic.index_name = i.index_name
LEFT JOIN all_ind_expressions ie
  ON ie.index_owner = i.owner
 AND ie.index_name = i.index_name
 AND ie.column_position = ic.column_position
WHERE i.table_owner = :owner
  AND i.uniqueness = 'UNIQUE'
  AND i.table_name IN ({marcadores})
ORDER BY i.table_name, i.index_name, ic.column_position
""".strip()


def _sql_colunas(marcadores: str) -> str:
    # Todas as colunas das tabelas do fecho: NULLABLE alimenta (c) e os
    # veredictos de unicidade; DATA_TYPE/tamanhos ajudam a desenhar as regras
    # de regeneração de código de negócio na Etapa 2.
    return f"""
SELECT
    t.table_name,
    t.column_name,
    t.column_id,
    t.data_type,
    t.data_length,
    t.data_precision,
    t.data_scale,
    t.nullable
FROM all_tab_columns t
WHERE t.owner = :owner
  AND t.table_name IN ({marcadores})
ORDER BY t.table_name, t.column_id
""".strip()


# Nomes esperados dos dumps do dicionário (gerados pela conexão direta OU
# exportados manualmente das queries do --somente-sql).
_ARQ_CONSTRAINTS = "dic_constraints_pk_uk_fk.csv"
_ARQ_INDICES = "dic_indices_unicos.csv"
_ARQ_COLUNAS = "dic_colunas.csv"


def _chunks(itens: Sequence[str], tamanho: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(itens), tamanho):
        yield itens[i:i + tamanho]


def gera_arquivos_sql(tabelas: Sequence[str], owner: str, sql_dir: str) -> None:
    """PASSO 1 do fluxo: escreve as três queries (IN-list literal, owner
    resolvido) como arquivos .sql prontos para abrir e executar no DBeaver.
    Cada arquivo diz, no cabeçalho, com que nome salvar o CSV exportado."""
    owner_lit = owner.replace("'", "''")
    consultas = (
        ("01_constraints_pk_uk_fk.sql", "CONSTRAINTS P/U/R", _sql_constraints,
         _ARQ_CONSTRAINTS),
        ("02_indices_unicos.sql", "ÍNDICES UNIQUE", _sql_indices_unicos,
         _ARQ_INDICES),
        ("03_colunas.sql", "COLUNAS/NULLABLE", _sql_colunas, _ARQ_COLUNAS),
    )
    os.makedirs(sql_dir, exist_ok=True)
    for arquivo, titulo, monta, csv_destino in consultas:
        partes = list(_chunks(tabelas, _CHUNK_IN))
        blocos: List[str] = [
            f"-- diagnostica_clonagem.py — PASSO 1: {titulo}",
            f"-- Owner: {owner} | Tabelas do fecho: {len(tabelas)}",
            "-- Execute no DBeaver e exporte o resultado como CSV",
            "-- (Export resultset... > CSV, COM cabeçalho; NULL como vazio),",
            f"-- salvando com o nome EXATO: {csv_destino}",
        ]
        if len(partes) > 1:
            blocos.append(f"-- ATENÇÃO: {len(partes)} partes — execute cada uma "
                          "e una os resultados no MESMO CSV (cabeçalho só uma vez).")
        for n, chunk in enumerate(partes, start=1):
            lista = ", ".join(f"'{t}'" for t in chunk)
            sql = monta(lista).replace(":owner", f"'{owner_lit}'")
            if len(partes) > 1:
                blocos.append(f"\n-- parte {n}/{len(partes)}")
            blocos.append(sql + ";")
        caminho = os.path.join(sql_dir, arquivo)
        with open(caminho, "w", encoding="utf-8") as f:
            f.write("\n".join(blocos) + "\n")
        logger.info("  gerado %s", caminho)

    logger.info("PASSO 2: rode cada .sql no DBeaver e exporte os CSVs "
                "(%s, %s, %s) para uma pasta.",
                _ARQ_CONSTRAINTS, _ARQ_INDICES, _ARQ_COLUNAS)
    logger.info("PASSO 3: python diagnostica_clonagem.py --spec <spec> "
                "--de-csvs <pasta_dos_3_csvs>")


# ---------------------------------------------------------------------------
# Modo offline (--de-csvs): lê os três dumps exportados manualmente a partir
# das queries do --somente-sql e roda as MESMAS análises, sem conexão.
# ---------------------------------------------------------------------------
def _le_dump_csv(caminho: str, obrigatorias: Sequence[str],
                 inteiras: Sequence[str]) -> List[dict]:
    """Lê um dump do dicionário exportado como CSV (DBeaver: Export
    resultset... > CSV). Normaliza: delimitador detectado (vírgula ou
    ponto-e-vírgula, conforme a configuração do DBeaver), cabeçalho em
    maiúsculas, ""/"null" -> None, colunas de posição -> int (senão a
    ordenação por posição compararia strings: "10" < "2")."""
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        primeira = f.readline()
        delimitador = ";" if primeira.count(";") > primeira.count(",") else ","
        f.seek(0)
        reader = csv.DictReader(f, delimiter=delimitador)
        campos = [c.strip().upper() for c in (reader.fieldnames or [])]
        for col in obrigatorias:
            if col not in campos:
                raise ValueError(
                    f"{os.path.basename(caminho)}: coluna obrigatória `{col}` ausente "
                    f"(cabeçalho encontrado: {campos}). Exporte o resultado da query "
                    "do --somente-sql COM cabeçalho e sem renomear colunas.")
        linhas: List[dict] = []
        for bruto in reader:
            linha: dict = {}
            for chave, valor in bruto.items():
                if chave is None:
                    continue
                chave = chave.strip().upper()
                valor = valor.strip() if isinstance(valor, str) else valor
                # "" é o default do DBeaver para NULL; "null"/"NULL" cobre
                # export com "NULL string" configurado. Nenhuma coluna destes
                # dumps tem "NULL" como valor legítimo.
                if valor is None or valor == "" or str(valor).upper() == "NULL":
                    linha[chave] = None
                elif chave in inteiras:
                    try:
                        linha[chave] = int(float(valor))
                    except ValueError:
                        linha[chave] = None
                else:
                    linha[chave] = valor
            linhas.append(linha)
    return linhas


def carrega_dumps_csv(pasta: str) -> Tuple[List[dict], List[dict], List[dict]]:
    """Carrega os três dumps do dicionário a partir de `pasta` (modo --de-csvs)."""
    linhas_cons = _le_dump_csv(
        os.path.join(pasta, _ARQ_CONSTRAINTS),
        obrigatorias=("TABLE_NAME", "CONSTRAINT_NAME", "CONSTRAINT_TYPE",
                      "COLUMN_NAME", "POSITION"),
        inteiras=("POSITION",))
    linhas_idx = _le_dump_csv(
        os.path.join(pasta, _ARQ_INDICES),
        obrigatorias=("TABLE_NAME", "INDEX_NAME", "COLUMN_NAME", "COLUMN_POSITION"),
        inteiras=("COLUMN_POSITION",))
    linhas_cols = _le_dump_csv(
        os.path.join(pasta, _ARQ_COLUNAS),
        obrigatorias=("TABLE_NAME", "COLUMN_NAME", "NULLABLE"),
        inteiras=("COLUMN_ID", "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE"))
    return linhas_cons, linhas_idx, linhas_cols


# ---------------------------------------------------------------------------
# Estruturas parseadas do dicionário.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FkDicionario:
    child_table: str
    constraint_name: str
    status: str
    validated: str
    delete_rule: str
    columns: Tuple[str, ...]
    parent_owner: str
    parent_table: str
    parent_columns: Tuple[str, ...]
    parent_constraint_type: str  # 'P' (PK) ou 'U' (UNIQUE)


@dataclass(frozen=True)
class ChaveUnica:
    table: str
    origem: str  # 'CONSTRAINT_UNIQUE' | 'INDICE_UNIQUE'
    nome: str
    columns: Tuple[str, ...]
    # Expressão por posição (function-based index); None quando coluna normal.
    expressoes: Tuple[Optional[str], ...] = ()
    # Constraint: ENABLED/DISABLED (ALL_CONSTRAINTS.STATUS). Índice: VALID/
    # UNUSABLE (ALL_INDEXES.STATUS). Uma UK DISABLED (sem KEEP INDEX) perde o
    # índice: cópia idêntica NÃO gera ORA-00001 — o veredicto precisa disso.
    status: str = "ENABLED"

    @property
    def tem_expressao(self) -> bool:
        return any(e for e in self.expressoes)

    @property
    def policiada(self) -> bool:
        """False quando o banco NÃO policia a chave (constraint DISABLED)."""
        return self.status.upper() not in ("DISABLED",)


def _agrupa_constraints(linhas: List[dict]) -> Tuple[
        Dict[str, Tuple[str, ...]],      # pk_por_tabela
        List[ChaveUnica],                # unique constraints
        List[FkDicionario],              # FKs
        Set[Tuple[str, str]],            # (tabela, index_name) que suportam P/U
        Set[Tuple[str, Tuple[str, ...]]]]:  # (tabela, colunas) de P/U
    """Reagrupa as linhas coluna-a-coluna da query de constraints em objetos
    por constraint (mesma ideia do agrupamento por CONSTRAINT_NAME do
    gera_spec_config.py)."""
    por_constraint: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for linha in linhas:
        por_constraint[(linha["TABLE_NAME"], linha["CONSTRAINT_NAME"])].append(linha)

    pk_por_tabela: Dict[str, Tuple[str, ...]] = {}
    uniques: List[ChaveUnica] = []
    fks: List[FkDicionario] = []
    indices_pu: Set[Tuple[str, str]] = set()
    chaves_pu: Set[Tuple[str, Tuple[str, ...]]] = set()

    for (tabela, nome), regs in por_constraint.items():
        # .get() defensivo: no modo --de-csvs colunas opcionais podem faltar
        # se o export não trouxe todas as colunas da query.
        regs = sorted(regs, key=lambda r: r.get("POSITION") or 0)
        tipo = regs[0].get("CONSTRAINT_TYPE")
        cols = tuple(r.get("COLUMN_NAME") or "?" for r in regs)
        index_name = regs[0].get("INDEX_NAME")
        status = str(regs[0].get("STATUS") or "ENABLED")
        # Só constraint ENABLED entra nos conjuntos de exclusão: uma PK/UK
        # DISABLE KEEP INDEX deixa um índice UNIQUE físico que CONTINUA
        # policiando — ele deve ser reportado como INDICE_UNIQUE, não engolido.
        habilitada = status.upper() != "DISABLED"

        if tipo == "P":
            pk_por_tabela[tabela] = cols
            if habilitada:
                chaves_pu.add((tabela, cols))
                if index_name:
                    indices_pu.add((tabela, index_name))
        elif tipo == "U":
            uniques.append(ChaveUnica(
                table=tabela, origem="CONSTRAINT_UNIQUE", nome=nome, columns=cols,
                expressoes=tuple(None for _ in cols), status=status))
            if habilitada:
                chaves_pu.add((tabela, cols))
                if index_name:
                    indices_pu.add((tabela, index_name))
        elif tipo == "R":
            parent_table = regs[0].get("PARENT_TABLE") or "?DESCONHECIDO?"
            fks.append(FkDicionario(
                child_table=tabela,
                constraint_name=nome,
                status=str(regs[0].get("STATUS") or ""),
                validated=str(regs[0].get("VALIDATED") or ""),
                delete_rule=str(regs[0].get("DELETE_RULE") or ""),
                columns=cols,
                parent_owner=str(regs[0].get("R_OWNER") or ""),
                parent_table=parent_table,
                parent_columns=tuple(r.get("PARENT_COLUMN") or "?" for r in regs),
                parent_constraint_type=str(regs[0].get("PARENT_CONSTRAINT_TYPE") or "?"),
            ))
    return pk_por_tabela, uniques, fks, indices_pu, chaves_pu


def _agrupa_indices_unicos(linhas: List[dict],
                           indices_pu: Set[Tuple[str, str]],
                           chaves_pu: Set[Tuple[str, Tuple[str, ...]]]
                           ) -> List[ChaveUnica]:
    """Índices UNIQUE que NÃO são o suporte físico de uma PK/UNIQUE constraint
    (esses já foram reportados como constraint). Exclui por index_name e,
    defensivamente, por igualdade exata do conjunto ordenado de colunas."""
    por_indice: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for linha in linhas:
        por_indice[(linha["TABLE_NAME"], linha["INDEX_NAME"])].append(linha)

    out: List[ChaveUnica] = []
    for (tabela, nome), regs in sorted(por_indice.items()):
        if (tabela, nome) in indices_pu:
            continue
        regs = sorted(regs, key=lambda r: r.get("COLUMN_POSITION") or 0)
        cols = tuple(r.get("COLUMN_NAME") or "?" for r in regs)
        if (tabela, cols) in chaves_pu:
            continue
        expressoes = tuple(
            (str(r.get("COLUMN_EXPRESSION")).strip() if r.get("COLUMN_EXPRESSION") else None)
            for r in regs)
        out.append(ChaveUnica(table=tabela, origem="INDICE_UNIQUE", nome=nome,
                              columns=cols, expressoes=expressoes,
                              status=str(regs[0].get("STATUS") or "VALID")))
    return out


# ---------------------------------------------------------------------------
# Colunas remapeadas pelo clone-and-remap (a partir do SPEC, porque é o spec
# que dirige o clonador na Etapa 2) + classificação da PK de cada tabela.
# ---------------------------------------------------------------------------
def _fks_remapeaveis(spec: Dict[str, dict], tabela: str,
                     estaticas: Set[str]) -> List[Tuple[Tuple[str, ...], str, Tuple[str, ...]]]:
    """FKs do SPEC desta tabela que apontam para a PK de um pai CLONADO
    (não-static, dentro do fecho): são as únicas com remap definido no plano.
    FK para pai static não é remapeada (tabela de referência não é clonada);
    FK para UNIQUE que não é a PK do pai também não (diag_divergencias).
    Retorna tuplas (colunas_filha, pai, colunas_pai) alinhadas por posição."""
    out: List[Tuple[Tuple[str, ...], str, Tuple[str, ...]]] = []
    cfg = spec.get(tabela) or {}
    for fk in _fk_list(cfg):
        pai = fk.get("parent_table")
        if pai not in spec or pai in estaticas:
            continue
        pk_pai = tuple(spec[pai].get("pk_cols") or [])
        cols = tuple(fk.get("columns") or [])
        pcols = tuple(fk.get("parent_columns") or [])
        if not pk_pai or pcols != pk_pai or len(cols) != len(pcols):
            continue
        out.append((cols, pai, pcols))
    return out


def calcula_remap(spec: Dict[str, dict],
                  estaticas: Set[str],
                  dtypes: Dict[Tuple[str, str], str]
                  ) -> Tuple[Dict[str, Set[str]], Set[str], Dict[str, Set[str]], Dict[str, Set[str]]]:
    """Ponto fixo: quais colunas de cada tabela clonada têm o valor de fato
    REESCRITO pelo clone-and-remap.

    Semente (item 4 do plano): PK surrogate própria — coluna ÚNICA, numérica e
    que não participa de NENHUMA FK do spec (participar de FK, mesmo para pai
    static, veta o offset: reescrever quebraria a FK).

    Propagação (pareamento POSICIONAL coluna_filha[i] <-> pk_pai[i]): uma
    coluna de FK para a PK de pai clonado é reescrita SE E SOMENTE SE o
    componente correspondente da PK do pai é ele próprio reescrito. Uma FK
    composta NÃO reescreve as colunas pareadas com componentes não remapeados
    do pai — ex.: FK (NUM_IF, NUM_SEQ) -> PK (NUM_IF, NUM_SEQ) onde só NUM_IF
    é remapeado: o clone do pai mantém NUM_SEQ, logo a filha também mantém.

    Itera até estabilizar; ciclos de PK compartilhada sem raiz surrogate ficam
    sem remap (e a PK sai como VERIFICAR em diag_pks).

    Retorna (remap_por_tabela, tabelas_com_offset_proprio,
    pk_herdada_por_tabela, pk_em_fk_qualquer_por_tabela).
    """
    fks_por_tabela = {t: _fks_remapeaveis(spec, t, estaticas) for t in spec}

    # Componentes da PK herdados de pai clonado (participam de FK remapeável)
    # e componentes da PK que participam de QUALQUER FK do spec (inclusive
    # para pai static/fora do fecho — vetam o offset próprio).
    pk_herdada: Dict[str, Set[str]] = {}
    pk_em_fk_qualquer: Dict[str, Set[str]] = {}
    for t in spec:
        pk = set(spec[t].get("pk_cols") or [])
        pk_herdada[t] = {c for cols, _, _ in fks_por_tabela[t] for c in cols if c in pk}
        pk_em_fk_qualquer[t] = {
            c for fk in _fk_list(spec[t]) for c in (fk.get("columns") or []) if c in pk}

    remap: Dict[str, Set[str]] = {t: set() for t in spec}
    offsets: Set[str] = set()

    # Semente: surrogate própria.
    for t in spec:
        if t in estaticas:
            continue
        pk = tuple(spec[t].get("pk_cols") or [])
        if (len(pk) == 1 and pk[0] not in pk_em_fk_qualquer[t]
                and dtypes.get((t, pk[0]), "").upper() in _TIPOS_NUMERICOS):
            remap[t].add(pk[0])
            offsets.add(t)

    # Propagação até ponto fixo (monótona crescente -> termina).
    mudou = True
    while mudou:
        mudou = False
        for t in spec:
            if t in estaticas:
                continue
            for cols, pai, pcols in fks_por_tabela[t]:
                remap_pai = remap.get(pai) or set()
                for col_filha, col_pai in zip(cols, pcols):
                    if col_pai in remap_pai and col_filha not in remap[t]:
                        remap[t].add(col_filha)
                        mudou = True
    return remap, offsets, pk_herdada, pk_em_fk_qualquer


def analisa_pks(spec: Dict[str, dict],
                estaticas: Set[str],
                pk_dicionario: Dict[str, Tuple[str, ...]],
                dtypes: Dict[Tuple[str, str], str],
                tabelas_no_dicionario: Set[str]
                ) -> Tuple[List[dict], Dict[str, Set[str]]]:
    """Classifica a PK de cada tabela do fecho e devolve também, por tabela,
    o conjunto FINAL de colunas efetivamente remapeadas (ponto fixo de
    calcula_remap), usado nos veredictos de unicidade.

    A PK usada na lógica é a do SPEC (é ela que dirige o clonador); o
    dicionário entra como fallback e a divergência entre os dois é acusada em
    diag_divergencias (PK_DIVERGENTE, CRITICA)."""
    linhas: List[dict] = []
    remap_total, offsets, pk_herdada, pk_em_fk_qualquer = calcula_remap(
        spec, estaticas, dtypes)

    for tabela in sorted(spec):
        cfg = spec[tabela]
        eh_static = bool(cfg.get("static"))
        pk_spec = tuple(cfg.get("pk_cols") or [])
        pk = pk_spec or pk_dicionario.get(tabela) or ()
        fonte_pk = "SPEC" if pk_spec else (
            "DICIONARIO(fallback)" if pk else "")
        remap = remap_total.get(tabela, set())
        comp_remap = [c for c in pk if c in remap]

        if eh_static:
            classificacao = "STATIC_NAO_CLONADA"
            detalhe = "Tabela de referência: não é clonada; PK intocada."
        elif tabela not in tabelas_no_dicionario:
            classificacao = "TABELA_AUSENTE_NO_OWNER"
            detalhe = ("Tabela do spec não encontrada no owner informado — "
                       "verifique --owner (ver diag_divergencias).")
        elif not pk:
            classificacao = "SEM_PK"
            detalhe = "Sem PK no spec nem no dicionário; clonagem indefinida."
        elif tabela in offsets:
            classificacao = "OFFSET_PROPRIO"
            detalhe = ("PK surrogate numérica: recebe offset próprio por "
                       "tabela (item 4 do plano); FKs que a referenciam são "
                       "reescritas pelo mesmo mapeamento.")
        elif comp_remap:
            classificacao = "OK_REMAP_VIA_FK"
            detalhe = (f"Componente(s) {comp_remap} da PK remapeado(s) via FK "
                       "de pai clonado (mapeamento do pai é remapeado de "
                       "fato); unicidade preservada por construção.")
        elif pk_herdada.get(tabela):
            classificacao = "VERIFICAR_PK_HERDADA_SEM_REMAP"
            detalhe = (f"Componente(s) {sorted(pk_herdada[tabela])} da PK vêm "
                       "de pai clonado, mas NENHUM componente herdado é "
                       "efetivamente remapeado (cadeia de pais sem raiz "
                       "surrogate/offset) — apenas copiar garante ORA-00001; "
                       "precisa de decisão na Etapa 2.")
        elif any(c in pk_em_fk_qualquer.get(tabela, set()) for c in pk):
            classificacao = "VERIFICAR_PK_SEM_REMAP"
            detalhe = ("PK participa de FK para pai NÃO clonado (static ou "
                       "fora do remap): offset próprio quebraria a FK e a "
                       "cópia idêntica garante ORA-00001 — decidir na Etapa 2.")
        else:
            classificacao = "VERIFICAR_PK_SEM_REMAP"
            detalhe = ("PK sem componente remapeado e não elegível a offset "
                       "numérico simples (composta e/ou não numérica) — "
                       "apenas copiar garante ORA-00001; decidir na Etapa 2.")

        linhas.append({
            "TABELA": tabela,
            "STATIC": "S" if eh_static else "N",
            "PK": "+".join(pk) if pk else "",
            "FONTE_PK": fonte_pk,
            "TIPO_DADO_PK": "+".join(dtypes.get((tabela, c), "?") for c in pk),
            "CLASSIFICACAO": classificacao,
            "COLUNAS_REMAPEADAS": "+".join(sorted(remap)) if remap else "",
            "DETALHE": detalhe,
        })
    return linhas, remap_total


# ---------------------------------------------------------------------------
# (a) Unicidade fora da PK.
# ---------------------------------------------------------------------------
def analisa_unicidade(chaves: List[ChaveUnica],
                      spec: Dict[str, dict],
                      estaticas: Set[str],
                      remap_total: Dict[str, Set[str]],
                      nullable: Dict[Tuple[str, str], str]) -> List[dict]:
    linhas: List[dict] = []
    for ch in sorted(chaves, key=lambda c: (c.table, c.nome)):
        if ch.table not in spec:
            continue  # índice de tabela fora do fecho não interessa
        remap = remap_total.get(ch.table, set())
        remapeadas = [c for c in ch.columns if c in remap]
        negocio = [c for c in ch.columns if c not in remap]
        remap_not_null = [c for c in remapeadas
                          if nullable.get((ch.table, c), "?") == "N"]

        if ch.table in estaticas:
            veredicto = "STATIC_NAO_CLONADA"
            detalhe = "Tabela não é clonada; nenhuma linha nova -> sem risco."
        elif ch.origem == "CONSTRAINT_UNIQUE" and not ch.policiada:
            # UK DISABLED sem KEEP INDEX perde o índice: cópia idêntica NÃO
            # gera ORA-00001. Se houve DISABLE KEEP INDEX, o índice
            # remanescente aparece como INDICE_UNIQUE nesta mesma lista (os
            # conjuntos de exclusão só consideram constraints ENABLED).
            veredicto = "CONSTRAINT_DESABILITADA"
            detalhe = ("Constraint DISABLED: o banco não policia esta chave "
                       "(sem índice quando o DISABLE foi sem KEEP INDEX) — "
                       "sem risco de ORA-00001 por cópia. Índice único "
                       "remanescente, se existir, é reportado à parte como "
                       "INDICE_UNIQUE.")
        elif ch.tem_expressao:
            veredicto = "VERIFICAR_EXPRESSAO"
            exprs = [e for e in ch.expressoes if e]
            detalhe = (f"Índice function-based ({exprs}); análise automática "
                       "não cobre expressão — avaliar manualmente.")
        elif not remapeadas:
            veredicto = "RISCO_ORA00001"
            detalhe = ("Nenhuma coluna da chave é remapeada pelo clone — "
                       "cópia idêntica colide (ORA-00001). Precisa de regra "
                       f"de regeneração para {negocio} na Etapa 2.")
        elif remap_not_null:
            veredicto = "OK_REMAP"
            detalhe = (f"Coluna(s) remapeada(s) NOT NULL {remap_not_null} "
                       "garantem unicidade dos clones por construção.")
        elif not negocio:
            # Chave 100% remapeada, todas nullable. Semântica do índice único
            # do Oracle: chave TODA nula não é indexada (não colide); chave
            # com algum valor tem coluna remapeada não-nula reescrita
            # injetivamente -> não colide com o original nem entre clones.
            veredicto = "OK_REMAP"
            detalhe = (f"Todas as colunas da chave {remapeadas} são "
                       "remapeadas: chave toda-NULL não entra no índice único "
                       "e chave com valor tem coluna remapeada reescrita — "
                       "unicidade preservada por construção.")
        else:
            veredicto = "ATENCAO_REMAP_NULLABLE"
            detalhe = (f"Coluna(s) remapeada(s) {remapeadas} são todas "
                       f"NULLABLE e a chave tem coluna(s) de negócio {negocio} "
                       "apenas copiada(s): linha com as remapeadas NULL vira "
                       "chave PARCIALMENTE nula — que o Oracle indexa — e "
                       "colide com o original. Verificar se há NULL real "
                       "nessas colunas na fonte.")

        linhas.append({
            "TABELA": ch.table,
            "ORIGEM": ch.origem,
            "NOME": ch.nome,
            "STATUS": ch.status,
            "COLUNAS": "+".join(ch.columns),
            "COLUNAS_REMAPEADAS": "+".join(remapeadas),
            "COLUNAS_NEGOCIO": "+".join(negocio),
            "VEREDICTO": veredicto,
            "DETALHE": detalhe,
        })
    return linhas


def _pai_mesmo_owner(fk: FkDicionario, owner: str) -> bool:
    """True quando a constraint referencia chave do MESMO owner analisado.
    Uma FK para tabela HOMÔNIMA de outro schema não é o pai do fecho: tratá-la
    como interna faria o clonador remapear para uma PK que não existe no
    schema que a constraint valida (ORA-02291)."""
    return not fk.parent_owner or fk.parent_owner.upper() == owner.upper()


def _pai_no_fecho(fk: FkDicionario, spec: Dict[str, dict], owner: str) -> bool:
    return fk.parent_table in spec and _pai_mesmo_owner(fk, owner)


# ---------------------------------------------------------------------------
# (b) Self-references e FKs entre instrumentos.
# ---------------------------------------------------------------------------
def analisa_referencias_instrumento(fks: List[FkDicionario],
                                    spec: Dict[str, dict],
                                    estaticas: Set[str],
                                    owner: str) -> List[dict]:
    linhas: List[dict] = []

    fks_para_if_por_filho: Dict[str, int] = defaultdict(int)
    for fk in fks:
        if fk.parent_table == TABELA_RAIZ:
            fks_para_if_por_filho[fk.child_table] += 1

    for fk in sorted(fks, key=lambda f: (f.child_table, f.constraint_name)):
        self_ref = fk.child_table == fk.parent_table
        para_if = fk.parent_table == TABELA_RAIZ and not self_ref
        de_if = fk.child_table == TABELA_RAIZ and not self_ref and not para_if
        if not (self_ref or para_if or de_if):
            continue

        obs: List[str] = []
        if self_ref:
            direcao = "SELF_REFERENCE"
            if fk.columns == fk.parent_columns:
                obs.append("identidade degenerada (colunas FK de si mesmas; "
                           "engorda_tables ignora este padrão — ignorar aqui também)")
            else:
                obs.append("self-reference genuína: decidir se o registro "
                           "referenciado entra no clone ou mantém o original")
        elif para_if:
            direcao = "FILHA_DE_INSTRUMENTO"
            if list(fk.columns) != list(fk.parent_columns):
                obs.append(f"nome de coluna divergente {list(fk.columns)} -> "
                           f"{list(fk.parent_columns)}: provável ligação "
                           "ENTRE instrumentos (tipo NUM_IF_ORIGEM) — decidir "
                           "se o IF referenciado entra no clone")
            if fks_para_if_por_filho[fk.child_table] > 1:
                obs.append("tabela tem MAIS DE UMA FK para INSTRUMENTO_"
                           "FINANCEIRO: só uma pode ser o vínculo principal")
        else:
            direcao = "INSTRUMENTO_FILHO_DE"
            pai_static = fk.parent_table in estaticas
            if not _pai_no_fecho(fk, spec, owner):
                obs.append("pai FORA do fecho do spec (ver diag_divergencias)")
            elif pai_static:
                obs.append("pai static (referência): FK mantém valor original")
            else:
                obs.append("pai clonável: FK será remapeada")
        if not _pai_mesmo_owner(fk, owner):
            obs.append(f"pai em OUTRO owner ({fk.parent_owner}.{fk.parent_table}): "
                       "NÃO é a tabela do fecho, apenas homônima")
        if fk.status != "ENABLED":
            obs.append(f"constraint {fk.status}")

        linhas.append({
            "DIRECAO": direcao,
            "TABELA_FILHA": fk.child_table,
            "CONSTRAINT": fk.constraint_name,
            "COLUNAS_FILHA": "+".join(fk.columns),
            "TABELA_PAI": fk.parent_table,
            "COLUNAS_PAI": "+".join(fk.parent_columns),
            "STATUS": fk.status,
            "OBSERVACAO": "; ".join(obs),
        })
    return linhas


# ---------------------------------------------------------------------------
# (c) Colunas NOT NULL envolvidas em FK.
# ---------------------------------------------------------------------------
def analisa_fk_not_null(fks: List[FkDicionario],
                        spec: Dict[str, dict],
                        estaticas: Set[str],
                        nullable: Dict[Tuple[str, str], str],
                        owner: str) -> List[dict]:
    linhas: List[dict] = []
    for fk in sorted(fks, key=lambda f: (f.child_table, f.constraint_name)):
        for pos, col in enumerate(fk.columns):
            if nullable.get((fk.child_table, col), "?") != "N":
                continue
            pai_static = fk.parent_table in estaticas
            pai_no_fecho = _pai_no_fecho(fk, spec, owner)
            linhas.append({
                "TABELA": fk.child_table,
                "CONSTRAINT": fk.constraint_name,
                "COLUNA": col,
                "POSICAO": pos + 1,
                "TABELA_PAI": fk.parent_table,
                "COLUNA_PAI": fk.parent_columns[pos] if pos < len(fk.parent_columns) else "?",
                "PAI_STATIC": ("S" if pai_static else "N") if pai_no_fecho else "FORA_DO_FECHO",
                "STATUS": fk.status,
                "REGRA": "NUNCA anular esta coluna no clonador (ORA-01400).",
            })
    return linhas


# ---------------------------------------------------------------------------
# (d) Divergências spec × dicionário.
# ---------------------------------------------------------------------------
def _fk_chave(cols: Sequence[str], pai: str, pcols: Sequence[str]) -> Tuple:
    return (tuple(cols), pai, tuple(pcols))


def analisa_divergencias(spec: Dict[str, dict],
                         estaticas: Set[str],
                         pk_dicionario: Dict[str, Tuple[str, ...]],
                         fks_dicionario: List[FkDicionario],
                         nullable: Dict[Tuple[str, str], str],
                         tabelas_no_dicionario: Set[str],
                         owner: str) -> List[dict]:
    linhas: List[dict] = []

    def add(tipo: str, severidade: str, tabela: str, detalhe: str) -> None:
        linhas.append({"TIPO": tipo, "SEVERIDADE": severidade,
                       "TABELA": tabela, "DETALHE": detalhe})

    # Tabela do spec ausente no owner.
    for tabela in sorted(spec):
        if tabela not in tabelas_no_dicionario:
            add("TABELA_AUSENTE_NO_OWNER", "CRITICA", tabela,
                f"Tabela do spec não existe em ALL_TAB_COLUMNS para owner={owner} "
                "(owner errado? sinônimo/view? ambiente diferente do que gerou os CSVs?).")

    # PK divergente (dicionário é a verdade; o spec dirige o clonador).
    for tabela in sorted(spec):
        pk_spec = tuple(spec[tabela].get("pk_cols") or [])
        pk_dic = pk_dicionario.get(tabela)
        if pk_dic and pk_spec and set(pk_spec) != set(pk_dic):
            add("PK_DIVERGENTE", "CRITICA", tabela,
                f"PK no spec={list(pk_spec)} != PK no dicionário={list(pk_dic)}. "
                "O offset de PK do clonador seguiria a coluna errada.")
        elif not pk_dic and tabela in tabelas_no_dicionario:
            add("SEM_PK_NO_DICIONARIO", "ALTA", tabela,
                f"Sem PK no dicionário; spec declara {list(pk_spec)}. "
                "Conferir se é view/MV ou PK via índice único apenas.")

    # FKs: dicionário vs spec, por tabela filha. Pai de OUTRO owner entra na
    # chave de comparação QUALIFICADO (OWNER.TABELA) para nunca casar com uma
    # tabela homônima do spec — vira FK_SO_NO_DICIONARIO + PAI_FORA_DO_FECHO.
    fks_dic_por_filho: Dict[str, Dict[Tuple, FkDicionario]] = defaultdict(dict)
    for fk in fks_dicionario:
        nome_pai = (fk.parent_table if _pai_mesmo_owner(fk, owner)
                    else f"{fk.parent_owner}.{fk.parent_table}")
        fks_dic_por_filho[fk.child_table][
            _fk_chave(fk.columns, nome_pai, fk.parent_columns)] = fk

    for tabela in sorted(spec):
        pk_tab = set(pk_dicionario.get(tabela) or spec[tabela].get("pk_cols") or [])
        spec_fks = {_fk_chave(fk.get("columns") or [],
                              fk.get("parent_table") or "",
                              fk.get("parent_columns") or [])
                    for fk in _fk_list(spec[tabela])}
        dic_fks = fks_dic_por_filho.get(tabela, {})

        # FK só no dicionário -> o clonador NÃO remapearia essas colunas.
        for chave, fk in sorted(dic_fks.items()):
            if chave in spec_fks:
                continue
            nome_pai = chave[1]  # já qualificado com owner quando cross-owner
            cols = set(fk.columns)
            if cols & pk_tab:
                sev, consequencia = "CRITICA", ("coluna participa da PK: cópia sem "
                                                "remap colide (ORA-00001)")
            elif any(nullable.get((tabela, c)) == "N" for c in cols):
                sev, consequencia = "ALTA", ("coluna NOT NULL apontaria para o "
                                             "registro ORIGINAL (clone cruzado)")
            else:
                sev, consequencia = "MEDIA", "clone manteria referência ao original"
            add("FK_SO_NO_DICIONARIO", sev, tabela,
                f"{fk.constraint_name}: {list(fk.columns)} -> "
                f"{nome_pai}.{list(fk.parent_columns)} não está no spec; {consequencia}. "
                "Regerar o spec (fk_real.csv desatualizado?) ou justificar a ausência.")

        # FK só no spec -> spec inventou/herdou relação que o banco não tem.
        for chave in sorted(spec_fks):
            if chave in dic_fks:
                continue
            cols, pai, pcols = chave
            add("FK_SO_NO_SPEC", "MEDIA", tabela,
                f"{list(cols)} -> {pai}.{list(pcols)} não existe como constraint no "
                "dicionário (FK lógica?). O remap ainda é desejável para manter a "
                "consistência do clone, mas conferir a origem dessa entrada.")

    # FKs do dicionário com características que o clonador precisa conhecer.
    for fk in sorted(fks_dicionario, key=lambda f: (f.child_table, f.constraint_name)):
        if fk.parent_constraint_type == "U" and _pai_no_fecho(fk, spec, owner) \
                and fk.parent_table not in estaticas:
            add("FK_PARA_UNIQUE_DE_PAI_CLONADO", "ALTA", fk.child_table,
                f"{fk.constraint_name}: referencia UNIQUE (não PK) "
                f"{fk.parent_table}.{list(fk.parent_columns)}. O plano remapeia PKs; "
                "chave UNIQUE do pai não é remapeada -> definir tratamento na Etapa 2.")
        if fk.status != "ENABLED":
            add("FK_DESABILITADA", "INFO", fk.child_table,
                f"{fk.constraint_name} ({fk.status}/{fk.validated}) -> "
                f"{fk.parent_table}: o banco não valida, mas a aplicação provavelmente "
                "assume o vínculo; o clonador deve remapear mesmo assim.")
        if not _pai_no_fecho(fk, spec, owner):
            add("PAI_FORA_DO_FECHO", "ALTA", fk.child_table,
                f"{fk.constraint_name}: pai {fk.parent_owner}.{fk.parent_table} não está "
                "no fecho do spec (fecho incompleto, owner distinto ou tabela homônima "
                "de outro schema). FK ficaria sem regra de remap.")

    return linhas


# ---------------------------------------------------------------------------
# Saída: CSVs + resumo no log.
# ---------------------------------------------------------------------------
def escreve_csv(caminho: str, linhas: List[dict], cabecalho: Sequence[str]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(cabecalho))
        writer.writeheader()
        for linha in linhas:
            writer.writerow(linha)
    logger.info("  gravado %s (%d linha(s))", caminho, len(linhas))


def _resume(titulo: str, linhas: List[dict], destaque, limite: int = 20) -> None:
    """Bloco de resumo legível no log: conta e mostra até `limite` itens que
    passam no filtro `destaque` (os que exigem ação)."""
    itens = [l for l in linhas if destaque(l)]
    logger.info("--- %s: %d de %d exigem atenção ---", titulo, len(itens), len(linhas))
    for linha in itens[:limite]:
        logger.info("    %s", linha)
    if len(itens) > limite:
        logger.info("    ... e mais %d (ver CSV).", len(itens) - limite)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Etapa 1 do clone-and-remap: diagnóstico do dicionário Oracle "
                    "para as tabelas do fecho, SEM conexão ao banco (as queries "
                    "rodam no DBeaver; ver FLUXO no cabeçalho do arquivo).",
        epilog="Num notebook, chame diagnostica(spec=..., owner=...) direto — "
               "argparse não funciona sob kernel Jupyter.")
    parser.add_argument("--spec", default="spec_config.json",
                        help="Caminho do spec_config.json (fecho referencial). "
                             "Default: spec_config.json no diretório atual.")
    parser.add_argument("--owner", default=None,
                        help="Owner (schema) das tabelas no Oracle. Default: env "
                             "DIAG_ORACLE_OWNER ou CETIP.")
    parser.add_argument("--de-csvs", default=".", metavar="PASTA",
                        help="Pasta com os dumps exportados do DBeaver "
                             f"({_ARQ_CONSTRAINTS}, {_ARQ_INDICES}, {_ARQ_COLUNAS}). "
                             "Default: pasta atual. Se os 3 arquivos não existirem, "
                             "gera os .sql do PASSO 1 em --sql-dir e para.")
    parser.add_argument("--saida-dir", default="diagnostico_clonagem",
                        help="Diretório de saída dos diag_*.csv. "
                             "Default: ./diagnostico_clonagem")
    parser.add_argument("--sql-dir", default="sql_diagnostico",
                        help="Diretório onde os .sql do PASSO 1 são gerados. "
                             "Default: ./sql_diagnostico")
    return parser.parse_args()


def diagnostica(spec: str = "spec_config.json",
                owner: Optional[str] = None,
                pasta_csvs: str = ".",
                saida_dir: str = "diagnostico_clonagem",
                sql_dir: str = "sql_diagnostico") -> Optional[Dict[str, List[dict]]]:
    """Ponto de entrada único — terminal E notebook.

    Comportamento em dois passos, decidido pela presença dos dumps:
      * Se os 3 CSVs do dicionário (exportados do DBeaver) NÃO existirem em
        `pasta_csvs`, gera os arquivos .sql do PASSO 1 em `sql_dir` e retorna
        None (rode-os no DBeaver, exporte os CSVs e chame de novo).
      * Se existirem, roda as análises, grava os diag_*.csv em `saida_dir`,
        loga o resumo e retorna {analise: linhas} (dicts prontos para
        pandas.DataFrame, se quiser inspecionar no notebook).

    Uso em notebook (spec e CSVs na pasta do kernel):
        from diagnostica_clonagem import diagnostica
        resultado = diagnostica(spec="spec_config.json", owner="CETIP")

    `spec` é o CAMINHO do spec_config.json; nada é alterado no banco (este
    processo nem conexão com o Oracle abre).
    """
    owner = (owner or os.environ.get("DIAG_ORACLE_OWNER", "CETIP")).strip().upper()

    spec = carrega_spec(spec)
    estaticas = {t for t, cfg in spec.items() if cfg.get("static")}
    alvo = sorted(set(spec) - estaticas)

    invalidas = sorted(t for t in spec if not _NOME_OBJETO_VALIDO.match(t))
    if invalidas:
        # Nome fora do padrão viraria SQL malformado na IN-list literal;
        # melhor abortar com lista clara. ValueError (e não sys.exit) para
        # não derrubar sessão de notebook.
        raise ValueError(
            f"Nome(s) de tabela fora do padrão Oracle no spec: {invalidas}")

    tabelas = sorted(spec)
    logger.info("=" * 78)
    logger.info("DIAGNÓSTICO DE CLONAGEM — fecho com %d tabela(s): %d alvo (clonáveis), "
                "%d static (referência). Owner: %s", len(tabelas), len(alvo),
                len(estaticas), owner)
    logger.info("=" * 78)

    faltantes = [arq for arq in (_ARQ_CONSTRAINTS, _ARQ_INDICES, _ARQ_COLUNAS)
                 if not os.path.isfile(os.path.join(pasta_csvs, arq))]
    if faltantes:
        logger.info("Dump(s) do dicionário ainda não encontrados em %s: %s",
                    os.path.abspath(pasta_csvs), faltantes)
        logger.info("Gerando as queries do PASSO 1 em %s ...", os.path.abspath(sql_dir))
        gera_arquivos_sql(tabelas, owner, sql_dir)
        return None

    logger.info("Lendo dumps do dicionário em %s ...", os.path.abspath(pasta_csvs))
    linhas_cons, linhas_idx, linhas_cols = carrega_dumps_csv(pasta_csvs)
    logger.info("  %d linha(s) de constraint, %d de índice UNIQUE, %d coluna(s).",
                len(linhas_cons), len(linhas_idx), len(linhas_cols))

    # ---- Parse -----------------------------------------------------------
    pk_dic, uniques_cons, fks_dic, indices_pu, chaves_pu = _agrupa_constraints(linhas_cons)
    indices_unicos = _agrupa_indices_unicos(linhas_idx, indices_pu, chaves_pu)
    chaves_unicas = uniques_cons + indices_unicos

    nullable = {(l["TABLE_NAME"], l["COLUMN_NAME"]): str(l.get("NULLABLE") or "?")
                for l in linhas_cols}
    dtypes = {(l["TABLE_NAME"], l["COLUMN_NAME"]): str(l.get("DATA_TYPE") or "?")
              for l in linhas_cols}
    tabelas_no_dicionario = {l["TABLE_NAME"] for l in linhas_cols}

    # ---- Análises --------------------------------------------------------
    pks_rows, remap_total = analisa_pks(spec, estaticas, pk_dic, dtypes,
                                        tabelas_no_dicionario)
    unicidade_rows = analisa_unicidade(chaves_unicas, spec, estaticas,
                                       remap_total, nullable)
    refs_rows = analisa_referencias_instrumento(fks_dic, spec, estaticas, owner)
    fk_nn_rows = analisa_fk_not_null(fks_dic, spec, estaticas, nullable, owner)
    diverg_rows = analisa_divergencias(spec, estaticas, pk_dic, fks_dic,
                                       nullable, tabelas_no_dicionario, owner)

    # ---- CSVs ------------------------------------------------------------
    os.makedirs(saida_dir, exist_ok=True)
    logger.info("Gravando CSVs em %s ...", os.path.abspath(saida_dir))

    escreve_csv(os.path.join(saida_dir, "diag_unicidade.csv"), unicidade_rows,
                ("TABELA", "ORIGEM", "NOME", "STATUS", "COLUNAS",
                 "COLUNAS_REMAPEADAS", "COLUNAS_NEGOCIO", "VEREDICTO", "DETALHE"))
    escreve_csv(os.path.join(saida_dir, "diag_pks.csv"), pks_rows,
                ("TABELA", "STATIC", "PK", "FONTE_PK", "TIPO_DADO_PK",
                 "CLASSIFICACAO", "COLUNAS_REMAPEADAS", "DETALHE"))
    escreve_csv(os.path.join(saida_dir, "diag_referencias_instrumento.csv"),
                refs_rows,
                ("DIRECAO", "TABELA_FILHA", "CONSTRAINT", "COLUNAS_FILHA",
                 "TABELA_PAI", "COLUNAS_PAI", "STATUS", "OBSERVACAO"))
    escreve_csv(os.path.join(saida_dir, "diag_fk_not_null.csv"), fk_nn_rows,
                ("TABELA", "CONSTRAINT", "COLUNA", "POSICAO", "TABELA_PAI",
                 "COLUNA_PAI", "PAI_STATIC", "STATUS", "REGRA"))
    escreve_csv(os.path.join(saida_dir, "diag_divergencias.csv"), diverg_rows,
                ("TIPO", "SEVERIDADE", "TABELA", "DETALHE"))

    # ---- Resumo legível ----------------------------------------------------
    logger.info("=" * 78)
    logger.info("RESUMO DO DIAGNÓSTICO (nada foi alterado no banco)")
    logger.info("=" * 78)
    _resume("(a) Unicidade fora da PK — RISCO_ORA00001/ATENCAO/VERIFICAR",
            unicidade_rows,
            lambda l: l["VEREDICTO"] in
            ("RISCO_ORA00001", "ATENCAO_REMAP_NULLABLE", "VERIFICAR_EXPRESSAO"))
    _resume("(a2) PKs sem remap definido", pks_rows,
            lambda l: l["CLASSIFICACAO"].startswith("VERIFICAR")
            or l["CLASSIFICACAO"] in ("SEM_PK", "TABELA_AUSENTE_NO_OWNER"))
    _resume("(b) Referências entre instrumentos / self-references", refs_rows,
            lambda l: l["DIRECAO"] in ("SELF_REFERENCE", "FILHA_DE_INSTRUMENTO")
            and "decidir" in l["OBSERVACAO"])
    logger.info("--- (c) Colunas NOT NULL em FK (nunca anular): %d coluna(s); "
                "lista completa em diag_fk_not_null.csv ---", len(fk_nn_rows))
    _resume("(d) Divergências spec × dicionário (CRITICA/ALTA)", diverg_rows,
            lambda l: l["SEVERIDADE"] in ("CRITICA", "ALTA"))
    logger.info("=" * 78)
    logger.info("Diagnóstico concluído. Envie os CSVs de %s para definirmos as "
                "regras da Etapa 2 (clona_instrumentos.py).",
                os.path.abspath(saida_dir))

    # Retorno para uso em notebook (ex.: pandas.DataFrame(resultado["pks"])).
    return {
        "unicidade": unicidade_rows,
        "pks": pks_rows,
        "referencias_instrumento": refs_rows,
        "fk_not_null": fk_nn_rows,
        "divergencias": diverg_rows,
    }


def main() -> None:
    args = parse_arguments()
    diagnostica(spec=args.spec, owner=args.owner, pasta_csvs=args.de_csvs,
                saida_dir=args.saida_dir, sql_dir=args.sql_dir)


if __name__ == "__main__":
    main()



# from diagnostica_clonagem import diagnostica
# resultado = diagnostica(spec="spec_config.json", owner="CETIP")
