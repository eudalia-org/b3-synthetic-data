#!/usr/bin/env python3
"""
valida_specs_vs_oracle.py

Compara o specs_config (reconstruído do CSV de metadado) contra as PK/FK REAIS
do Oracle, extraídas via all_constraints/all_cons_columns.

Entrada:
    specs.json   -> o specs_config que o synthesizer usa.
    pk_real.csv  -> export do SQL de PKs   (cols: TABLE_NAME, COLUMN_NAME, POSITION)
    fk_real.csv  -> export do SQL de FKs   (cols: CHILD_TABLE, CHILD_COLUMN,
                    COL_POSITION, PARENT_TABLE, PARENT_COLUMN)

Saída: relatório de divergências nos DOIS sentidos:
    - PK/FK que estão no specs mas NÃO no banco  (specs errado / a mais)
    - PK/FK que estão no banco mas NÃO no specs   (specs incompleto / a menos)

Só compara as tabelas presentes no specs (as que você vai sintetizar/carregar).
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple


def carrega_specs(caminho: str) -> dict:
    with open(caminho, "r", encoding="utf-8") as f:
        return json.load(f)


def _norm(s: str) -> str:
    """Normaliza nome (Oracle costuma vir maiúsculo; tira espaços)."""
    return (s or "").strip().upper()


# ---------- PK ----------

def pks_do_specs(specs: dict) -> Dict[str, Tuple[str, ...]]:
    """tabela -> tupla ordenada de colunas de PK (como declarado no specs)."""
    out: Dict[str, Tuple[str, ...]] = {}
    for t, cfg in specs.items():
        pk = tuple(_norm(c) for c in (cfg.get("pk_cols") or []))
        out[_norm(t)] = pk
    return out


def pks_do_banco(caminho_csv: str) -> Dict[str, Tuple[str, ...]]:
    """Lê pk_real.csv -> tabela -> tupla de colunas ordenada por POSITION."""
    acc: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    with open(caminho_csv, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            t = _norm(row["TABLE_NAME"])
            col = _norm(row["COLUMN_NAME"])
            pos = int(row["POSITION"])
            acc[t].append((pos, col))
    return {t: tuple(c for _, c in sorted(cols)) for t, cols in acc.items()}


# ---------- FK ----------

# Representa uma FK como uma chave hashável e comparável:
#   (child_table, (child_cols...), parent_table, (parent_cols...))
FkKey = Tuple[str, Tuple[str, ...], str, Tuple[str, ...]]


def fks_do_specs(specs: dict) -> Set[FkKey]:
    out: Set[FkKey] = set()
    for t, cfg in specs.items():
        child = _norm(t)
        for fk in (cfg.get("foreign_keys") or cfg.get("fks") or []):
            if not isinstance(fk, dict):
                continue
            cols = tuple(_norm(c) for c in (fk.get("columns") or []))
            parent = _norm(fk.get("parent_table"))
            pcols = tuple(_norm(c) for c in (fk.get("parent_columns") or []))
            if cols and parent and pcols:
                out.add((child, cols, parent, pcols))
    return out


def fks_do_banco(caminho_csv: str) -> Set[FkKey]:
    """
    Lê fk_real.csv agrupando por CONSTRAINT_NAME — cada constraint vira UMA FK.

    Requer o CSV revisado com a coluna CONSTRAINT_NAME. Isso trata
    corretamente FKs múltiplas para o mesmo pai (ex.: OPERACAO tem NUM_IF e
    NUM_IF_PERTENCE -> INSTRUMENTO_FINANCEIRO) e FKs compostas (ex.:
    OPER_CTX_MSG_FK com P1,P2 -> CONTEXTO_MENSAGEM), que o agrupamento antigo
    por (child, parent) embaralhava, gerando falso-positivos "SÓ NO SPECS".
    """
    por_constraint: Dict[str, Tuple[str, str]] = {}     # cname -> (child, parent)
    colunas: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)

    with open(caminho_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "CONSTRAINT_NAME" not in (reader.fieldnames or []):
            raise ValueError(
                "fk_real.csv não tem a coluna CONSTRAINT_NAME. Re-extraia com o "
                "SQL revisado (o que inclui ac.constraint_name)."
            )
        for row in reader:
            cname = _norm(row["CONSTRAINT_NAME"])
            child = _norm(row["CHILD_TABLE"])
            parent = _norm(row["PARENT_TABLE"])
            pos = int(row["COL_POSITION"])
            ccol = _norm(row["CHILD_COLUMN"])
            pcol = _norm(row["PARENT_COLUMN"])
            por_constraint[cname] = (child, parent)
            colunas[cname].append((pos, ccol, pcol))

    out: Set[FkKey] = set()
    for cname, (child, parent) in por_constraint.items():
        trips = sorted(colunas[cname])
        ccols = tuple(c for _, c, _ in trips)
        pcols = tuple(p for _, _, p in trips)
        out.add((child, ccols, parent, pcols))
    return out


def _fmt_fk(fk: FkKey) -> str:
    child, ccols, parent, pcols = fk
    return f"{child}.{list(ccols)} -> {parent}.{list(pcols)}"


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "Uso: python valida_specs_vs_oracle.py specs.json pk_real.csv fk_real.csv"
        )
    specs_path, pk_csv, fk_csv = sys.argv[1], sys.argv[2], sys.argv[3]

    specs = carrega_specs(specs_path)
    tabelas_specs = {_norm(t) for t in specs}

    # ---- PK ----
    pk_specs = pks_do_specs(specs)
    pk_db = pks_do_banco(pk_csv)

    print("=" * 78)
    print("VALIDAÇÃO DE PRIMARY KEYS (specs vs banco)")
    print("=" * 78)
    pk_div = 0
    for t in sorted(tabelas_specs):
        s = pk_specs.get(t, ())
        d = pk_db.get(t, ())
        if t not in pk_db:
            print(f"  [SEM PK NO BANCO] {t}: specs={list(s)} | banco=(tabela/PK não encontrada)")
            pk_div += 1
        elif s != d:
            print(f"  [DIVERGE] {t}: specs={list(s)} | banco={list(d)}")
            pk_div += 1
    if pk_div == 0:
        print("  OK: todas as PKs do specs batem com o banco.")

    # ---- FK ----
    fk_specs = fks_do_specs(specs)
    fk_db_all = fks_do_banco(fk_csv)
    # só compara FKs cuja FILHA está no specs (escopo do que você sintetiza)
    fk_db = {fk for fk in fk_db_all if fk[0] in tabelas_specs}

    print("\n" + "=" * 78)
    print("VALIDAÇÃO DE FOREIGN KEYS (specs vs banco)")
    print("=" * 78)

    no_specs_nao_banco = sorted(fk_specs - fk_db, key=_fmt_fk)
    no_banco_nao_specs = sorted(fk_db - fk_specs, key=_fmt_fk)

    print("\n-- FKs declaradas no SPECS que NÃO existem no banco "
          "(specs a mais / errado): --")
    if no_specs_nao_banco:
        for fk in no_specs_nao_banco:
            print(f"  [SÓ NO SPECS] {_fmt_fk(fk)}")
    else:
        print("  (nenhuma)")

    print("\n-- FKs que existem no BANCO mas NÃO estão no specs "
          "(specs incompleto): --")
    if no_banco_nao_specs:
        for fk in no_banco_nao_specs:
            print(f"  [SÓ NO BANCO] {_fmt_fk(fk)}")
    else:
        print("  (nenhuma)")

    print("\n" + "=" * 78)
    total = pk_div + len(no_specs_nao_banco) + len(no_banco_nao_specs)
    if total == 0:
        print("RESULTADO: specs 100% alinhado com o banco (PK e FK).")
    else:
        print(f"RESULTADO: {total} divergência(s) — revisar antes do append.")
    print("=" * 78)


if __name__ == "__main__":
    main()
    
-- ===== PKs reais (uma linha por coluna de PK, com posição) =====
SELECT
    ac.table_name,
    acc.column_name,
    acc.position
FROM   all_constraints ac
JOIN   all_cons_columns acc
       ON  ac.owner = acc.owner
       AND ac.constraint_name = acc.constraint_name
WHERE  ac.constraint_type = 'P'
AND    ac.owner = :OWNER          -- << preencha o schema (ex.: 'BLC')
AND    ac.table_name IN ( /* suas 47 tabelas, ou remova o IN p/ todas */ )
ORDER BY ac.table_name, acc.position;



-- ===== FKs reais (filha -> pai, com colunas pareadas por posição) =====
SELECT
    ac.table_name              AS child_table,
    acc.column_name            AS child_column,
    acc.position               AS col_position,
    r.table_name               AS parent_table,
    rcc.column_name            AS parent_column
FROM   all_constraints ac
JOIN   all_cons_columns acc
       ON  ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
JOIN   all_constraints r
       ON  ac.r_owner = r.owner AND ac.r_constraint_name = r.constraint_name
JOIN   all_cons_columns rcc
       ON  r.owner = rcc.owner
       AND r.constraint_name = rcc.constraint_name
       AND acc.position = rcc.position      -- pareia coluna filha com a do pai na mesma posição
WHERE  ac.constraint_type = 'R'
AND    ac.owner = :OWNER
AND    ac.table_name IN ( /* suas 47 */ )
ORDER BY ac.table_name, ac.constraint_name, acc.position;







SELECT
    ac.constraint_name         AS constraint_name,
    ac.table_name              AS child_table,
    acc.column_name            AS child_column,
    acc.position               AS col_position,
    r.table_name               AS parent_table,
    rcc.column_name            AS parent_column
FROM   all_constraints ac
JOIN   all_cons_columns acc
       ON  ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
JOIN   all_constraints r
       ON  ac.r_owner = r.owner AND ac.r_constraint_name = r.constraint_name
JOIN   all_cons_columns rcc
       ON  r.owner = rcc.owner
       AND r.constraint_name = rcc.constraint_name
       AND acc.position = rcc.position
WHERE  ac.constraint_type = 'R'
AND    ac.owner = :OWNER
ORDER BY ac.table_name, ac.constraint_name, acc.position;
