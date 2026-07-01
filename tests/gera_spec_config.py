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

SAÍDA:
    spec_config.json com as 15 + ancestrais (fecho), static correto.

USO:
    from gera_specs_fecho import gera
    gera(pk_csv="pk_real.csv", fk_csv="fk_real.csv", saida="spec_config.json",
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


def gera(
    *,
    pk_csv: str,
    fk_csv: str,
    saida: str = "spec_config.json",
    parquet_disponivel: Optional[Set[str]] = None,
) -> dict:
    pks = le_pks(pk_csv)
    fks = le_fks(fk_csv)

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
        cfg["static"] = t not in TABELAS_ALVO
        specs[t] = cfg

    with open(saida, "w", encoding="utf-8") as f:
        json.dump(specs, f, ensure_ascii=False, indent=2)

    # --- Buraco C: ancestrais (com bloco) sem parquet ---
    disp = {_norm(x) for x in (parquet_disponivel or set())}
    ancestrais = sorted(t for t in specs if t not in TABELAS_ALVO)
    if disp:
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
    if disp:
        if sem_parquet:
            print(f"  [ATENÇÃO] {len(sem_parquet)} ancestral(is) sem parquet: {sem_parquet}")
            print("  Precisam de parquet no OCI ou a FK fica órfã (NOT NULL -> ORA-01400).")
        else:
            print("  OK: todos os ancestrais têm parquet disponível.")
    else:
        print(f"  [VERIFICAR] {len(parquet_desconhecido)} ancestral(is) — passe")
        print("  parquet_disponivel (ou rode diagnostica_pais_fk com spark) para saber")
        print(f"  quais têm parquet: {parquet_desconhecido}")

    # valida JSON
    with open(saida, encoding="utf-8") as f:
        json.load(f)
    print("\n  JSON válido confirmado.")
    print("=" * 84)

    return specs


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(
            "Uso: python gera_specs_fecho.py pk_real.csv fk_real.csv [saida.json] "
            "[parquet_disp separados por virgula]"
        )
    pk_csv, fk_csv = sys.argv[1], sys.argv[2]
    saida = sys.argv[3] if len(sys.argv) > 3 else "spec_config.json"
    disp = set(sys.argv[4].split(",")) if len(sys.argv) > 4 else None
    gera(pk_csv=pk_csv, fk_csv=fk_csv, saida=saida, parquet_disponivel=disp)
