#!/usr/bin/env python3
"""
gera_specs_fecho.py

Gera o spec_config MÍNIMO e COMPLETO para sintetizar as 15 tabelas-alvo.

Em vez de incluir o schema inteiro (1693 tabelas) ou uma lista manual de pais
(que sempre esquece alguém), calcula o FECHO TRANSITIVO de ancestrais: começa
com as 15, adiciona todo parent_table referenciado, depois os pais desses, e
itera até o conjunto parar de crescer. O resultado é exatamente o conjunto de
tabelas que participam das FKs das 15 — nem uma a mais, nem uma a menos.

As 15 saem static=False (engordadas); todo ancestral sai static=True (referência).

RELATÓRIO DE BURACOS (o ponto principal — falha visível, não silenciosa):
  - Buraco A: tabela no fecho SEM PK no pk_real.csv -> não pode virar bloco.
              Reportada; a FK que aponta pra ela será descartada na síntese.
  - Buraco B: alguma das 15 AUSENTE do pk_real.csv -> não seria nem sintetizada.
  - Buraco C: ancestral com bloco mas SEM parquet -> synthesizer pula na leitura;
              a FK fica órfã. Só detectável se você passar o conjunto com parquet
              (parquet_disponivel) ou spark+bases. Sem isso, reporta como
              "verificar parquet".

ENTRADA:
    pk_real.csv  -> TABLE_NAME, COLUMN_NAME, POSITION           (schema inteiro, SEM filtro IN)
    fk_real.csv  -> CONSTRAINT_NAME, CHILD_TABLE, CHILD_COLUMN, COL_POSITION,
                    PARENT_TABLE, PARENT_COLUMN                 (schema inteiro, SEM filtro IN)
    cols_real.csv-> TABLE_NAME, COLUMN_NAME, NULLABLE (Y/N)      (schema inteiro, SEM filtro IN)
                    Alimenta `not_null_cols` no spec, usado pelo engorda para
                    DROPAR (em vez de anular) linhas órfãs em colunas NOT NULL.

SAÍDA:
    spec_config.json com as 15 + ancestrais (fecho), static correto, e
    not_null_cols por tabela (quando cols_real.csv é informado).

USO:
    from gera_specs_fecho import gera
    gera(pk_csv="pk_real.csv", fk_csv="fk_real.csv", cols_csv="cols_real.csv",
         saida="spec_config.json",
         parquet_disponivel={"COMITENTE","GRP_MODALIDADE_LIQUIDACAO", ...})  # opcional
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple


TABELAS_ALVO = {
    "INSTRUMENTO_FINANCEIRO", "CONDICAO_IF", "CARTEIRA_COMITENTE",
    "CARTEIRA_PARTICIPANTE", "CREDITO", "DEPOSITO_AUTOMATICO_IF", "TITULO",
    "JUROS_FLUTUANTE", "RESGATE", "EVENTO", "OPERACAO", "ESPECIFICACAO",
    "LANCAMENTO", "DADO_OPERACAO", "ESPECIFICACAO_COMITENTE",
    # Subtipos joined-subclass de CONDICAO_IF (polimorfismo Hibernate sem
    # discriminador). Um CDB resolve seu tipo concreto pela tabela-subtipo que
    # contém a linha; portanto TODO subtipo que um CDB pode ter precisa ser
    # engordado, senão CONDICAO_IF daquele tipo fica sem subtipo (dangling) e o
    # batch da NoMe estoura no cast. Antes só JUROS_FLUTUANTE/RESGATE estavam
    # aqui -> CDB prefixado (tipo 2 = JUROS_FIXO) nascia sem tabela de taxa.
    # Ver SUBTYPE_BY_TIPO em engorda_tables.py; subtipos derivative-only
    # (TERMO*, OPCAO, PREMIO_*) NÃO se aplicam a CDB puro e ficam de fora.
    "JUROS_FIXO", "ATUALIZACAO_POS", "ATUALIZACAO_PRE", "SPREAD",
    "AMORTIZACAO", "RESET", "PARTICIPACAO_LUCROS", "DESDOBRAMENTO",
}


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


def le_not_null(caminho: str) -> Dict[str, List[str]]:
    """Colunas NOT NULL por tabela, lidas do cols_real.csv.

    Espera as colunas TABLE_NAME, COLUMN_NAME, NULLABLE (Y/N). Retorna, por
    tabela, a lista ordenada das colunas com NULLABLE == 'N'. Essa informação
    alimenta `not_null_cols` no spec, para o engorda decidir entre ANULAR
    (coluna nullable órfã) e DROPAR a linha (coluna NOT NULL órfã) em vez de
    anular às cegas e violar NOT NULL no append (ORA-01400).
    """
    acc: Dict[str, List[str]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        campos = reader.fieldnames or []
        for obrig in ("TABLE_NAME", "COLUMN_NAME", "NULLABLE"):
            if obrig not in campos:
                raise ValueError(f"cols_real.csv precisa da coluna {obrig}.")
        for row in reader:
            if _norm(row["NULLABLE"]) == "N":
                acc[_norm(row["TABLE_NAME"])].append(_norm(row["COLUMN_NAME"]))
    return {t: sorted(set(v)) for t, v in acc.items()}


# FK por constraint: child -> lista de (columns, parent, parent_columns)
def le_fks(caminho: str) -> Dict[str, List[dict]]:
    meta: Dict[str, Tuple[str, str]] = {}
    cols: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "CONSTRAINT_NAME" not in (reader.fieldnames or []):
            raise ValueError("fk_real.csv precisa de CONSTRAINT_NAME.")
        for row in reader:
            cn = _norm(row["CONSTRAINT_NAME"])
            meta[cn] = (_norm(row["CHILD_TABLE"]), _norm(row["PARENT_TABLE"]))
            cols[cn].append((int(row["COL_POSITION"]),
                             _norm(row["CHILD_COLUMN"]), _norm(row["PARENT_COLUMN"])))
    by_child: Dict[str, List[dict]] = defaultdict(list)
    for cn, (child, parent) in meta.items():
        trips = sorted(cols[cn])
        by_child[child].append({
            "columns": [c for _, c, _ in trips],
            "parent_table": parent,
            "parent_columns": [p for _, _, p in trips],
            "_constraint": cn,
        })
    return by_child


def fecho_ancestrais(alvo: Set[str], fks: Dict[str, List[dict]]) -> Set[str]:
    """
    Fecho transitivo 'para cima': alvo + todos os pais alcançáveis por FK.
    Itera até ponto fixo (pega avós, bisavós, etc). Ignora self-ref.
    """
    conjunto = set(alvo)
    fronteira = set(alvo)
    while fronteira:
        nova = set()
        for t in fronteira:
            for fk in fks.get(t, []):
                p = fk["parent_table"]
                if p != t and p not in conjunto:
                    nova.add(p)
        conjunto |= nova
        fronteira = nova
    return conjunto


def _tem_parquet_spark(spark, bases: List[str], table: str) -> bool:
    for base in bases:
        path = f"{base.rstrip('/')}/{table}"
        try:
            spark.read.parquet(path).take(1)
            return True
        except Exception:
            continue
    return False


def gera(
    *,
    pk_csv: str,
    fk_csv: str,
    cols_csv: Optional[str] = None,
    saida: str = "spec_config.json",
    parquet_disponivel: Optional[Set[str]] = None,
    spark=None,
    parquet_bases: Optional[List[str]] = None,
) -> dict:
    pks = le_pks(pk_csv)
    fks = le_fks(fk_csv)
    # not_null_cols alimenta a decisão anula-vs-dropa do engorda. Sem o CSV de
    # colunas o spec sai SEM essa informação e o engorda cai no comportamento
    # antigo (anula sempre) — que reintroduz o risco de ORA-01400. Por isso é
    # fortemente recomendado passar cols_csv; um aviso é emitido se faltar.
    not_null = le_not_null(cols_csv) if cols_csv else {}

    # --- Buraco B: alguma das 15 sem PK? ---
    alvo_sem_pk = sorted(t for t in TABELAS_ALVO if t not in pks)

    # fecho transitivo de ancestrais das 15
    conjunto = fecho_ancestrais(TABELAS_ALVO, fks)

    # --- Buraco A: tabela no fecho sem PK -> não vira bloco ---
    no_fecho_sem_pk = sorted(t for t in conjunto if t not in pks)

    # monta specs só para as tabelas do fecho que TÊM PK
    specs: dict = {}
    for t in sorted(conjunto):
        if t not in pks:
            continue  # reportado em no_fecho_sem_pk
        cfg: dict = {"pk_cols": pks[t]}
        # inclui só FKs cujo pai também está no conjunto (senão seria descartada)
        fk_list = []
        for fk in fks.get(t, []):
            if fk["parent_table"] in conjunto and fk["parent_table"] in pks:
                fk_list.append({
                    "columns": fk["columns"],
                    "parent_table": fk["parent_table"],
                    "parent_columns": fk["parent_columns"],
                })
        if fk_list:
            cfg["foreign_keys"] = sorted(
                fk_list, key=lambda x: (x["parent_table"], tuple(x["columns"]))
            )
        # Colunas NOT NULL da tabela (só as que existem no pk_real como pista de
        # que a tabela é conhecida; a fonte é o cols_real). O engorda usa isto
        # para dropar (em vez de anular) linhas órfãs em colunas NOT NULL.
        nn = not_null.get(t, [])
        if nn:
            cfg["not_null_cols"] = nn
        cfg["static"] = t not in TABELAS_ALVO
        specs[t] = cfg

    with open(saida, "w", encoding="utf-8") as f:
        json.dump(specs, f, ensure_ascii=False, indent=2)

    # --- Buraco C: ancestrais (com bloco) sem parquet ---
    ancestrais = sorted(t for t in specs if t not in TABELAS_ALVO)
    disp = {_norm(x) for x in (parquet_disponivel or set())}
    usar_spark = spark is not None and parquet_bases

    if usar_spark:
        # checa parquet SÓ dos ancestrais do fecho (não das 1600 do schema)
        sem_parquet = [t for t in ancestrais
                       if not _tem_parquet_spark(spark, parquet_bases, t)]
        parquet_desconhecido = []
    elif disp:
        sem_parquet = [t for t in ancestrais if t not in disp]
        parquet_desconhecido = []
    else:
        sem_parquet = []
        parquet_desconhecido = ancestrais

    # ---------------- RELATÓRIO ----------------
    n_static = sum(1 for c in specs.values() if c.get("static"))
    print("=" * 84)
    print("SPEC GERADO POR FECHO TRANSITIVO (15 + ancestrais)")
    print("=" * 84)
    print(f"  tabelas no specs: {len(specs)}  "
          f"(alvo/não-static: {len(specs)-n_static}, ancestrais/static: {n_static})")
    print(f"  arquivo: {saida}")

    print("\n--- Buraco B: alguma das 15 SEM PK no pk_real.csv? ---")
    if alvo_sem_pk:
        print(f"  [CRÍTICO] estas ALVO não têm PK e NÃO serão sintetizadas: {alvo_sem_pk}")
    else:
        print("  OK: todas as 15 têm PK.")

    print("\n--- Buraco A: tabela no fecho SEM PK (FK pra ela será descartada) ---")
    if no_fecho_sem_pk:
        print(f"  [ATENÇÃO] {len(no_fecho_sem_pk)} pai(s) sem PK no CSV: {no_fecho_sem_pk}")
        print("  As FKs que apontam pra estas serão ignoradas na síntese e a coluna")
        print("  anulada. Se alguma for NOT NULL, o append quebra. Verifique se")
        print("  faltou PK no banco ou se o pk_real.csv está incompleto.")
    else:
        print("  OK: todos os pais do fecho têm PK.")

    print("\n--- Buraco C: ancestrais SEM parquet (synthesizer pula -> FK órfã) ---")
    print(f"  (checando só os {len(ancestrais)} ancestrais do fecho, não o schema inteiro)")
    if usar_spark or disp:
        if sem_parquet:
            print(f"  [ATENÇÃO] {len(sem_parquet)} ancestral(is) sem parquet: {sem_parquet}")
            print("  Precisam de parquet no OCI ou a FK fica órfã (NOT NULL -> ORA-01400).")
        else:
            print("  OK: todos os ancestrais do fecho têm parquet.")
    else:
        print(f"  [VERIFICAR] passe spark+parquet_bases ou parquet_disponivel para checar.")
        print(f"  Ancestrais do fecho: {parquet_desconhecido}")

    print("\n--- Buraco D: NOT NULL das colunas (anula-vs-dropa no engorda) ---")
    if not cols_csv:
        print("  [ATENÇÃO] cols_csv NÃO informado: o spec sai SEM not_null_cols.")
        print("  O engorda cairá no modo antigo (anula FK órfã sempre), reabrindo o")
        print("  risco de ORA-01400 em coluna NOT NULL. Passe cols_real.csv.")
    else:
        com_nn = sorted(t for t in specs if specs[t].get("not_null_cols"))
        alvo_sem_nn = sorted(t for t in TABELAS_ALVO
                             if t in specs and not specs[t].get("not_null_cols"))
        n_nn = sum(len(specs[t]["not_null_cols"]) for t in com_nn)
        print(f"  OK: not_null_cols em {len(com_nn)}/{len(specs)} tabela(s), "
              f"{n_nn} coluna(s) NOT NULL no total.")
        if alvo_sem_nn:
            print(f"  [VERIFICAR] alvo(s) sem NENHUMA coluna NOT NULL no cols_real "
                  f"(esperado ao menos a PK): {alvo_sem_nn}")

    # valida JSON
    with open(saida, encoding="utf-8") as f:
        json.load(f)
    print("\n  JSON válido confirmado.")
    print("=" * 84)

    return specs


if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise SystemExit(
            "Uso: python gera_specs_fecho.py pk_real.csv fk_real.csv cols_real.csv "
            "[saida.json] [parquet_disp separados por virgula]\n"
            "  cols_real.csv (TABLE_NAME,COLUMN_NAME,NULLABLE) alimenta not_null_cols; "
            "passe '-' para omitir (NÃO recomendado — reabre risco de ORA-01400)."
        )
    pk_csv, fk_csv, cols_csv = sys.argv[1], sys.argv[2], sys.argv[3]
    cols_csv = None if cols_csv == "-" else cols_csv
    saida = sys.argv[4] if len(sys.argv) > 4 else "spec_config.json"
    disp = set(sys.argv[5].split(",")) if len(sys.argv) > 5 else None
    gera(pk_csv=pk_csv, fk_csv=fk_csv, cols_csv=cols_csv, saida=saida,
         parquet_disponivel=disp)
