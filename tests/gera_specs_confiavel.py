#!/usr/bin/env python3
"""
gera_specs_do_banco.py

Constrói o spec_config a partir das PK/FK REAIS do Oracle (all_constraints),
em vez do CSV de metadado reconstruído. Resolve o problema das ~149 FKs que
existem no banco mas faltavam no specs (causa do ORA-02291 no append, ex.:
CETIP.OPER_CTX_MSG_FK em OPERACAO -> CONTEXTO_MENSAGEM).

Entrada (exports do DBeaver):
    pk_real.csv  -> cols: TABLE_NAME, COLUMN_NAME, POSITION
    fk_real.csv  -> cols: CONSTRAINT_NAME, CHILD_TABLE, CHILD_COLUMN,
                          COL_POSITION, PARENT_TABLE, PARENT_COLUMN

O CONSTRAINT_NAME é essencial: é o que separa FKs múltiplas/compostas
corretamente. OPERACAO tem 2 FKs para INSTRUMENTO_FINANCEIRO (NUM_IF e
NUM_IF_PERTENCE) e a OPER_CTX_MSG_FK composta (P1,P2 -> NUM_ID_CTX_MSG);
agrupar por constraint_name monta cada uma como uma FK física distinta.

Saída:
    spec_config.json -> formato que o synthesizer consome, com:
      - pk_cols por tabela (ordenado por POSITION)
      - foreign_keys por tabela (cada constraint = uma FK, colunas por posição)
      - static: False nas 15 tabelas a engordar; True em todas as outras

Regra de static (definida por VOCÊ, não pelo banco): as 15 transacionais do
CDB são engordadas (static=False); o resto é referência (static=True).
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from typing import Dict, List, Tuple


# As 15 tabelas a engordar (static = False). Todo o resto vira static = True.
TABELAS_NAO_STATIC = {
    "INSTRUMENTO_FINANCEIRO", "CONDICAO_IF", "CARTEIRA_COMITENTE",
    "CARTEIRA_PARTICIPANTE", "CREDITO", "DEPOSITO_AUTOMATICO_IF", "TITULO",
    "JUROS_FLUTUANTE", "RESGATE", "EVENTO", "OPERACAO", "ESPECIFICACAO",
    "LANCAMENTO", "DADO_OPERACAO", "ESPECIFICACAO_COMITENTE",
}


def _norm(s: str) -> str:
    return (s or "").strip().upper()


def le_pks(caminho: str) -> Dict[str, List[str]]:
    """tabela -> lista de colunas de PK ordenada por POSITION."""
    acc: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            t = _norm(row["TABLE_NAME"])
            acc[t].append((int(row["POSITION"]), _norm(row["COLUMN_NAME"])))
    return {t: [c for _, c in sorted(cols)] for t, cols in acc.items()}


def le_fks(caminho: str) -> Dict[str, List[dict]]:
    """
    tabela_filha -> lista de FKs. Cada FK é agrupada por CONSTRAINT_NAME, com
    columns e parent_columns ordenadas por COL_POSITION (pareadas).
    """
    # constraint_name -> dados acumulados
    por_constraint: Dict[str, dict] = {}
    colunas: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)

    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            cname = _norm(row["CONSTRAINT_NAME"])
            child = _norm(row["CHILD_TABLE"])
            parent = _norm(row["PARENT_TABLE"])
            pos = int(row["COL_POSITION"])
            ccol = _norm(row["CHILD_COLUMN"])
            pcol = _norm(row["PARENT_COLUMN"])
            por_constraint[cname] = {"child": child, "parent": parent}
            colunas[cname].append((pos, ccol, pcol))

    fks_por_tabela: Dict[str, List[dict]] = defaultdict(list)
    for cname, info in por_constraint.items():
        trips = sorted(colunas[cname])
        ccols = [c for _, c, _ in trips]
        pcols = [p for _, _, p in trips]
        fks_por_tabela[info["child"]].append({
            "columns": ccols,
            "parent_table": info["parent"],
            "parent_columns": pcols,
        })
    return fks_por_tabela


def monta_specs(pks: Dict[str, List[str]], fks: Dict[str, List[dict]]) -> dict:
    """
    Monta o spec_config. A base de tabelas são as que têm PK (toda tabela
    sintetizável precisa de PK). FKs sem PK na filha são ignoradas com aviso.
    """
    specs: dict = {}
    sem_pk: List[str] = []

    todas_tabelas = set(pks) | set(fks)

    for t in sorted(todas_tabelas):
        if t not in pks:
            # tabela aparece como filha de FK mas não tem PK no export:
            # provavelmente fora do escopo de PK extraído. Registra e pula.
            sem_pk.append(t)
            continue

        cfg: dict = {"pk_cols": pks[t]}

        if t in fks:
            # ordena FKs por (parent_table, columns) para saída determinística
            cfg["foreign_keys"] = sorted(
                fks[t], key=lambda fk: (fk["parent_table"], tuple(fk["columns"]))
            )

        cfg["static"] = t not in TABELAS_NAO_STATIC
        specs[t] = cfg

    if sem_pk:
        print(f"[AVISO] {len(sem_pk)} tabela(s) com FK mas sem PK no export "
              f"(ignoradas): {sem_pk}", file=sys.stderr)

    return specs


def relata_fks_para_fora(specs: dict) -> List[Tuple[str, str, str]]:
    """
    Encontra FKs cujo PAI não está entre as tabelas do specs (fora do escopo).

    Estas são o ponto de risco no append: o synthesizer não tem o pai para
    remapear, então vai ANULAR a FK (se nullable) ou o load estoura
    ORA-02291/ORA-01400 (se NOT NULL). Você precisa decidir, para cada uma:
      - incluir a tabela-pai no conjunto sintetizado, OU
      - confirmar que o pai já existe em produção E que a FK é nullable.

    Retorna lista de (tabela_filha, colunas_fk, tabela_pai_ausente).
    """
    conhecidas = set(specs)
    fora: List[Tuple[str, str, str]] = []
    for t, cfg in specs.items():
        for fk in cfg.get("foreign_keys", []):
            parent = fk["parent_table"]
            if parent not in conhecidas:
                fora.append((t, ",".join(fk["columns"]), parent))
    return sorted(fora)


def main() -> None:
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "Uso: python gera_specs_do_banco.py pk_real.csv fk_real.csv [saida.json]"
        )
    pk_csv, fk_csv = sys.argv[1], sys.argv[2]
    saida = sys.argv[3] if len(sys.argv) == 4 else "spec_config.json"

    pks = le_pks(pk_csv)
    fks = le_fks(fk_csv)
    specs = monta_specs(pks, fks)

    with open(saida, "w", encoding="utf-8") as f:
        json.dump(specs, f, ensure_ascii=False, indent=2)

    # Resumo para conferência rápida
    n_static = sum(1 for c in specs.values() if c.get("static"))
    n_nao_static = len(specs) - n_static
    n_fks = sum(len(c.get("foreign_keys", [])) for c in specs.values())

    print(f"OK: {saida} gerado.")
    print(f"  tabelas: {len(specs)}  (não-static: {n_nao_static}, static: {n_static})")
    print(f"  total de FKs declaradas: {n_fks}")

    # ---- Relatório: FKs que referenciam tabelas FORA do conjunto gerado ----
    fora = relata_fks_para_fora(specs)
    if fora:
        pais_ausentes = sorted({p for _, _, p in fora})
        print("\n" + "!" * 70)
        print(f"FKs PARA TABELAS FORA DO CONJUNTO: {len(fora)} FK(s) apontam para")
        print(f"{len(pais_ausentes)} tabela(s)-pai que NÃO estão no specs gerado.")
        print("Risco no append: sem o pai, o synthesizer anula a FK (se nullable)")
        print("ou o load estoura ORA-02291/ORA-01400 (se NOT NULL).")
        print("Decida por pai: incluir no conjunto, ou confirmar que já existe em")
        print("produção E que a FK filha é nullable.")
        print("!" * 70)
        print("\nTabelas-pai ausentes:")
        for p in pais_ausentes:
            filhas = [f"{c}.[{cols}]" for c, cols, pp in fora if pp == p]
            print(f"  - {p}  <- referenciada por: {', '.join(filhas)}")
    else:
        print("\n  OK: nenhuma FK referencia tabela fora do conjunto gerado.")

    # Sanidade: as 15 que deviam ser não-static estão presentes e não-static?
    faltando = TABELAS_NAO_STATIC - set(specs)
    if faltando:
        print(f"  [ATENÇÃO] tabelas não-static esperadas AUSENTES no banco: "
              f"{sorted(faltando)}", file=sys.stderr)
    erradas = [t for t in TABELAS_NAO_STATIC if t in specs and specs[t]["static"]]
    if erradas:
        print(f"  [ATENÇÃO] deveriam ser não-static mas vieram static: {erradas}",
              file=sys.stderr)

    # valida que o JSON relê
    with open(saida, "r", encoding="utf-8") as f:
        json.load(f)
    print("  JSON válido confirmado.")


if __name__ == "__main__":
    main()
