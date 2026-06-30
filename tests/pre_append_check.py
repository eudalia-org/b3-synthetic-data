#!/usr/bin/env python3
"""
preappend_check.py — Validação pré-append em SPARK (dados completos, não amostra).

Replica offline as checagens que o Oracle faria no append das tabelas sintéticas,
lendo os parquets do OCI Object Storage via spark.read.parquet. Cobre as três
famílias de erro que travam o append:

    - PK nula            -> ORA-01400 (PK é NOT NULL)
    - PK duplicada       -> ORA-00001 (unique constraint)
    - FK órfã            -> ORA-02291 (parent key not found)
    - coluna NOT NULL    -> ORA-01400

HONESTIDADE SOBRE O FURO:
    Uma FK só pode ser verificada se o PAI estiver disponível em parquet. Se o
    pai não está no conjunto sintético (ex.: tabela de domínio que só existe em
    produção), a FK é marcada NÃO VERIFICÁVEL em vez de receber um OK falso.
    O Oracle vai checá-la contra produção; aqui não temos como. O relatório
    separa "verificada: passou/falhou" de "não verificável: confirmar em prod".

ENTRADA (CSVs exportados do Oracle via all_constraints/all_tab_columns):
    pk_real.csv   -> TABLE_NAME, COLUMN_NAME, POSITION
    fk_real.csv   -> CONSTRAINT_NAME, CHILD_TABLE, CHILD_COLUMN, COL_POSITION,
                     PARENT_TABLE, PARENT_COLUMN
    cols_real.csv -> TABLE_NAME, COLUMN_NAME, NULLABLE  (NULLABLE: 'Y'/'N')

CONFIG:
    Preencha SYNTH_BASE com o prefixo OCI dos parquets sintéticos e, se os pais
    de produção estiverem em parquet noutro prefixo, PROD_BASE. As tabelas a
    validar (as 15 que vão pro append) vão em TABLES_TO_APPEND. Se PROD_BASE for
    definido, FKs cujo pai não está no conjunto sintético são verificadas contra
    o parquet de produção; senão, marcadas não-verificáveis.

USO (no notebook, com `spark` já criado):
    from preappend_check import run_check
    run_check(spark,
              pk_csv="pk_real.csv", fk_csv="fk_real.csv", cols_csv="cols_real.csv",
              synth_base="oci://bucket@ns/synthetic",
              tables_to_append=[...],            # as 15
              prod_base=None)                    # ou "oci://bucket@ns/onprem-export"
"""

from __future__ import annotations

import csv
from collections import defaultdict
from functools import reduce
from typing import Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Leitura das constraints (CSVs locais no notebook)
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return (s or "").strip().upper()


def le_pks(caminho: str) -> Dict[str, List[str]]:
    acc: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            acc[_norm(row["TABLE_NAME"])].append(
                (int(row["POSITION"]), _norm(row["COLUMN_NAME"]))
            )
    return {t: [c for _, c in sorted(v)] for t, v in acc.items()}


# FK: (constraint, child, [child_cols], parent, [parent_cols])
FkRec = Tuple[str, str, List[str], str, List[str]]


def le_fks(caminho: str) -> Dict[str, List[FkRec]]:
    meta: Dict[str, Tuple[str, str]] = {}
    cols: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "CONSTRAINT_NAME" not in (reader.fieldnames or []):
            raise ValueError(
                "fk_real.csv precisa da coluna CONSTRAINT_NAME (use o SQL revisado)."
            )
        for row in reader:
            cn = _norm(row["CONSTRAINT_NAME"])
            meta[cn] = (_norm(row["CHILD_TABLE"]), _norm(row["PARENT_TABLE"]))
            cols[cn].append(
                (int(row["COL_POSITION"]), _norm(row["CHILD_COLUMN"]),
                 _norm(row["PARENT_COLUMN"]))
            )
    by_child: Dict[str, List[FkRec]] = defaultdict(list)
    for cn, (child, parent) in meta.items():
        trips = sorted(cols[cn])
        by_child[child].append(
            (cn, child, [c for _, c, _ in trips], parent, [p for _, _, p in trips])
        )
    return by_child


def le_notnull(caminho: str) -> Dict[str, List[str]]:
    """tabela -> lista de colunas NOT NULL (NULLABLE == 'N')."""
    acc: Dict[str, List[str]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if _norm(row["NULLABLE"]) == "N":
                acc[_norm(row["TABLE_NAME"])].append(_norm(row["COLUMN_NAME"]))
    return acc


# ---------------------------------------------------------------------------
# Leitura dos parquets (Spark, dados COMPLETOS)
# ---------------------------------------------------------------------------

def _read(spark: SparkSession, base: str, table: str) -> Optional[DataFrame]:
    """Lê o parquet de uma tabela; None se não existir/falhar."""
    path = f"{base.rstrip('/')}/{table}"
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def _upper_cols(df: DataFrame) -> DataFrame:
    """Normaliza nomes de coluna para upper, casando com os CSVs do Oracle."""
    return df.toDF(*[c.upper() for c in df.columns])


# ---------------------------------------------------------------------------
# Checagens
# ---------------------------------------------------------------------------

def check_pk(df: DataFrame, pk_cols: List[str]) -> Tuple[int, int]:
    """Retorna (linhas_pk_nula, linhas_pk_duplicada)."""
    present = [c for c in pk_cols if c in df.columns]
    if not present:
        return (-1, -1)  # PK não encontrada no parquet -> sinaliza
    null_cond = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in present])
    n_null = df.where(null_cond).count()
    total = df.count()
    distinct = df.select(*present).dropDuplicates().count()
    return (n_null, total - distinct)


def check_notnull(df: DataFrame, nn_cols: List[str]) -> List[Tuple[str, int]]:
    """Lista (coluna, n_nulos) para colunas NOT NULL que têm nulo no parquet."""
    out: List[Tuple[str, int]] = []
    for c in nn_cols:
        if c in df.columns:
            n = df.where(F.col(c).isNull()).count()
            if n > 0:
                out.append((c, n))
    return out


def check_fk(child_df: DataFrame, parent_df: DataFrame,
             child_cols: List[str], parent_cols: List[str]) -> int:
    """
    Conta linhas-filha órfãs: FK não-nula sem PK correspondente no pai.
    Sob MATCH SIMPLE do Oracle, linha com QUALQUER coluna da FK nula é ignorada
    pela checagem — então só validamos linhas com TODAS as colunas FK não-nulas.
    """
    cp = [c for c in child_cols if c in child_df.columns]
    pp = [p for p in parent_cols if p in parent_df.columns]
    if len(cp) != len(child_cols) or len(pp) != len(parent_cols):
        return -1  # coluna ausente -> não verificável

    not_null = reduce(lambda a, b: a & b,
                      [F.col(c).isNotNull() for c in child_cols])
    child_keys = (child_df.where(not_null)
                  .select(*child_cols).dropDuplicates())

    parent_keys = parent_df.select(
        *[F.col(pc).alias(cc) for cc, pc in zip(child_cols, parent_cols)]
    ).dropDuplicates()

    return child_keys.join(parent_keys, on=child_cols, how="left_anti").count()


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------

def run_check(
    spark: SparkSession,
    *,
    pk_csv: str,
    fk_csv: str,
    cols_csv: str,
    synth_base: str,
    tables_to_append: List[str],
    prod_base: Optional[str] = None,
) -> None:
    pks = le_pks(pk_csv)
    fks = le_fks(fk_csv)
    notnull = le_notnull(cols_csv)
    targets = {_norm(t) for t in tables_to_append}

    # cache dos DataFrames sintéticos lidos (evita reler)
    synth_cache: Dict[str, Optional[DataFrame]] = {}

    def synth(table: str) -> Optional[DataFrame]:
        if table not in synth_cache:
            df = _read(spark, synth_base, table)
            synth_cache[table] = _upper_cols(df) if df is not None else None
        return synth_cache[table]

    prod_cache: Dict[str, Optional[DataFrame]] = {}

    def prod(table: str) -> Optional[DataFrame]:
        if prod_base is None:
            return None
        if table not in prod_cache:
            df = _read(spark, prod_base, table)
            prod_cache[table] = _upper_cols(df) if df is not None else None
        return prod_cache[table]

    problemas = 0
    nao_verificaveis: List[str] = []

    print("=" * 78)
    print("VALIDAÇÃO PRÉ-APPEND (Spark, dados completos)")
    print(f"  sintético: {synth_base}")
    print(f"  produção : {prod_base or '(não fornecido — FKs p/ pais externos não verificáveis)'}")
    print(f"  tabelas  : {len(targets)}")
    print("=" * 78)

    for table in sorted(targets):
        df = synth(table)
        print(f"\n### {table}")
        if df is None:
            print("  [ERRO] parquet sintético não encontrado/ilegível — pulando.")
            problemas += 1
            continue

        total = df.count()
        print(f"  linhas: {total:,}")

        # ---- PK ----
        pk_cols = pks.get(table, [])
        if not pk_cols:
            print("  [PK] sem PK no metadado do banco (?). Pulando checagem de PK.")
        else:
            n_null, n_dup = check_pk(df, pk_cols)
            if n_null == -1:
                print(f"  [PK] colunas {pk_cols} não estão no parquet — NÃO verificável.")
                nao_verificaveis.append(f"{table} PK {pk_cols}")
            else:
                ok = (n_null == 0 and n_dup == 0)
                flag = "OK" if ok else "FALHA"
                print(f"  [PK] {flag}: nulas={n_null:,} duplicadas={n_dup:,} "
                      f"(cols={pk_cols})")
                if not ok:
                    problemas += 1

        # ---- NOT NULL ----
        nn = notnull.get(table, [])
        viol = check_notnull(df, nn) if nn else []
        if viol:
            for c, n in viol:
                print(f"  [NOT NULL] FALHA: coluna `{c}` tem {n:,} nulo(s) "
                      "(NOT NULL no banco -> ORA-01400)")
            problemas += 1
        else:
            print(f"  [NOT NULL] OK ({len(nn)} coluna(s) NOT NULL checada(s))")

        # ---- FK ----
        for cn, _child, ccols, parent, pcols in fks.get(table, []):
            pdf = synth(parent)
            origem = "sintético"
            if pdf is None:
                pdf = prod(parent)
                origem = "produção"
            if pdf is None:
                print(f"  [FK {cn}] -> {parent}.{pcols}: NÃO verificável "
                      f"(pai ausente em sintético e produção)")
                nao_verificaveis.append(f"{table}.{ccols} -> {parent}.{pcols}")
                continue

            orphans = check_fk(df, pdf, ccols, pcols)
            if orphans == -1:
                print(f"  [FK {cn}] -> {parent}.{pcols}: NÃO verificável "
                      "(coluna ausente em filha/pai)")
                nao_verificaveis.append(f"{table}.{ccols} -> {parent}.{pcols}")
            elif orphans == 0:
                print(f"  [FK {cn}] OK (vs {origem}): {ccols} -> {parent}.{pcols}")
            else:
                print(f"  [FK {cn}] FALHA (vs {origem}): {orphans:,} órfã(s) "
                      f"{ccols} -> {parent}.{pcols} -> ORA-02291")
                problemas += 1

    # ---- Resumo ----
    print("\n" + "=" * 78)
    if problemas == 0:
        print("RESULTADO: nenhuma violação detectada nas checagens possíveis.")
        print("O append NÃO deve falhar por PK/FK/NOT NULL nas tabelas verificadas.")
    else:
        print(f"RESULTADO: {problemas} tabela(s)/checagem(ns) com FALHA — corrigir antes do append.")
    if nao_verificaveis:
        print(f"\nNÃO VERIFICÁVEIS OFFLINE ({len(nao_verificaveis)}) — confirmar em produção:")
        for x in nao_verificaveis:
            print(f"  - {x}")
        print("(FKs cujo pai não está em parquet. O Oracle as checa contra produção.)")
    print("=" * 78)




SELECT table_name, column_name, nullable
FROM   all_tab_columns
WHERE  owner = :OWNER
ORDER BY table_name, column_id;



from preappend_check import run_check

AS_15 = [
    "INSTRUMENTO_FINANCEIRO", "CONDICAO_IF", "CARTEIRA_COMITENTE",
    "CARTEIRA_PARTICIPANTE", "CREDITO", "DEPOSITO_AUTOMATICO_IF", "TITULO",
    "JUROS_FLUTUANTE", "RESGATE", "EVENTO", "OPERACAO", "ESPECIFICACAO",
    "LANCAMENTO", "DADO_OPERACAO", "ESPECIFICACAO_COMITENTE",
]

run_check(
    spark,
    pk_csv="pk_real.csv",
    fk_csv="fk_real.csv",
    cols_csv="cols_real.csv",
    synth_base="oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/synthetic",  # ajuste o prefixo
    tables_to_append=AS_15,
    prod_base=None,   # ou o prefixo onprem-export quando o engenheiro terminar de subir
)
