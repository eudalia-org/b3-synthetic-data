#!/usr/bin/env python3
"""
valida_spec_final.py

Validador DEFINITIVO do spec_config antes de rodar o engorda. Cruza o specs
contra as fontes de verdade (PK/FK/NOT NULL do banco + parquet real) e checa,
um a um, TODOS os modos de falha que já travaram este projeto. Cada checagem
existe porque o problema correspondente aconteceu de verdade.

CHECAGENS (severidade):
  C1  [CRITICO] specs não é JSON válido / vazio.
  C2  [CRITICO] alguma das 15 ALVO ausente do specs ou marcada static.
  C3  [CRITICO] pai referenciado por FK SEM bloco no specs (FK descartada).
                (o caso COMITENTE)
  C4  [CRITICO] FK que existe no BANCO mas falta no specs (append quebra).
                (as ~149 FKs)
  C5  [CRITICO] FK NOT NULL cujo pai NÃO tem parquet -> coluna anulada ->
                ORA-01400 no append. (o caso NUM_CONTA_PARTICIPANTE)
  C6  [ALERTA]  PK do specs diverge da PK do banco.
  C7  [ALERTA]  pai (static) com bloco mas SEM parquet -> synthesizer pula,
                FK nullable fica órfã (anulada, sem quebrar o append).
  C8  [ALERTA]  FK declarada no specs que NÃO existe no banco (specs a mais).

ENTRADA:
    spec_config.json
    pk_real.csv    -> TABLE_NAME, COLUMN_NAME, POSITION
    fk_real.csv    -> CONSTRAINT_NAME, CHILD_TABLE, CHILD_COLUMN, COL_POSITION,
                      PARENT_TABLE, PARENT_COLUMN
    cols_real.csv  -> TABLE_NAME, COLUMN_NAME, NULLABLE ('Y'/'N')

PARQUET (uma das duas formas):
    - spark + parquet_bases: o validador lê cada tabela pra saber se há dado.
    - parquet_disponivel: conjunto de nomes que você sabe que têm parquet.

USO (notebook):
    from valida_spec_final import valida
    valida(spec_json="spec_config.json",
           pk_csv="pk_real.csv", fk_csv="fk_real.csv", cols_csv="cols_real.csv",
           spark=spark, parquet_bases=["oci://bucket@ns/onprem-export-full"])
"""

from __future__ import annotations

import csv
import json
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


# ---- fontes de verdade (banco) ----

def _pks_banco(caminho: str) -> Dict[str, Tuple[str, ...]]:
    acc: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            acc[_norm(row["TABLE_NAME"])].append(
                (int(row["POSITION"]), _norm(row["COLUMN_NAME"])))
    return {t: tuple(c for _, c in sorted(v)) for t, v in acc.items()}


FkKey = Tuple[str, Tuple[str, ...], str, Tuple[str, ...]]


def _fks_banco(caminho: str) -> Set[FkKey]:
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
    out: Set[FkKey] = set()
    for cn, (child, parent) in meta.items():
        trips = sorted(cols[cn])
        out.add((child, tuple(c for _, c, _ in trips),
                 parent, tuple(p for _, _, p in trips)))
    return out


def _notnull_banco(caminho: str) -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if _norm(row["NULLABLE"]) == "N":
                out.add((_norm(row["TABLE_NAME"]), _norm(row["COLUMN_NAME"])))
    return out


# ---- specs ----

def _fks_specs(specs: dict) -> Set[FkKey]:
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


def _fmt_fk(fk: FkKey) -> str:
    c, cc, p, pc = fk
    return f"{c}.{list(cc)} -> {p}.{list(pc)}"


# ---- parquet ----

def _tem_parquet(spark, bases, table, disp) -> Optional[bool]:
    if disp is not None:
        return table in disp
    if spark is None or not bases:
        return None  # desconhecido
    for base in bases:
        try:
            spark.read.parquet(f"{base.rstrip('/')}/{table}").take(1)
            return True
        except Exception:
            continue
    return False


# ---- orquestração ----

def valida(
    *,
    spec_json: str,
    pk_csv: str,
    fk_csv: str,
    cols_csv: str,
    spark=None,
    parquet_bases: Optional[List[str]] = None,
    parquet_disponivel: Optional[Set[str]] = None,
) -> bool:
    criticos: List[str] = []
    alertas: List[str] = []

    # C1 — JSON válido / não vazio
    try:
        with open(spec_json, encoding="utf-8") as f:
            specs = json.load(f)
        if not isinstance(specs, dict) or not specs:
            criticos.append("C1: specs vazio ou não é objeto JSON.")
            _print_resultado(criticos, alertas)
            return False
    except Exception as e:
        criticos.append(f"C1: specs não é JSON válido: {e}")
        _print_resultado(criticos, alertas)
        return False

    specs = {_norm(t): cfg for t, cfg in specs.items()}
    pk_db = _pks_banco(pk_csv)
    fk_db = _fks_banco(fk_csv)
    nn_db = _notnull_banco(cols_csv)
    disp = {_norm(x) for x in parquet_disponivel} if parquet_disponivel else None

    parq_cache: Dict[str, Optional[bool]] = {}
    def tem_parquet(t: str) -> Optional[bool]:
        if t not in parq_cache:
            parq_cache[t] = _tem_parquet(spark, parquet_bases, t, disp)
        return parq_cache[t]

    # C2 — as 15 presentes e não-static
    for t in sorted(TABELAS_ALVO):
        if t not in specs:
            criticos.append(f"C2: ALVO `{t}` ausente do specs (não seria sintetizada).")
        elif specs[t].get("static"):
            criticos.append(f"C2: ALVO `{t}` marcada static (não seria engordada).")

    # C3 — pai referenciado sem bloco
    for t, cfg in specs.items():
        for fk in (cfg.get("foreign_keys") or []):
            p = _norm(fk.get("parent_table"))
            if p and p not in specs:
                criticos.append(f"C3: FK {t}.{fk.get('columns')} -> `{p}` sem bloco no specs.")

    # C4 — FK do banco ausente no specs (só as que envolvem tabelas do specs)
    fk_specs = _fks_specs(specs)
    tabelas_specs = set(specs)
    fk_db_relevante = {fk for fk in fk_db if fk[0] in tabelas_specs}
    so_no_banco = fk_db_relevante - fk_specs
    for fk in sorted(so_no_banco, key=_fmt_fk):
        criticos.append(f"C4: FK existe no BANCO mas falta no specs: {_fmt_fk(fk)}")

    # C5 — FK NOT NULL cujo pai não tem parquet -> ORA-01400
    # C7 — FK (nullable) cujo pai não tem parquet -> órfã anulada (alerta)
    for t, cfg in specs.items():
        for fk in (cfg.get("foreign_keys") or []):
            p = _norm(fk.get("parent_table"))
            cols = [_norm(c) for c in (fk.get("columns") or [])]
            if not p or p not in specs:
                continue  # já pego em C3
            pq = tem_parquet(p)
            if pq is True:
                continue
            # pai sem parquet (ou desconhecido). É NOT NULL em alguma col da filha?
            nn_cols = [c for c in cols if (t, c) in nn_db]
            estado = "SEM parquet" if pq is False else "parquet DESCONHECIDO"
            if nn_cols:
                criticos.append(
                    f"C5: FK NOT NULL {t}.{nn_cols} -> `{p}` ({estado}). "
                    "Coluna será anulada -> ORA-01400 no append.")
            else:
                alertas.append(
                    f"C7: FK {t}.{cols} -> `{p}` ({estado}). Nullable: "
                    "será anulada se órfã (não quebra append).")

    # C6 — PK specs vs banco
    for t, cfg in specs.items():
        pk_spec = tuple(_norm(c) for c in (cfg.get("pk_cols") or []))
        pk_real = pk_db.get(t)
        if pk_real is None:
            alertas.append(f"C6: `{t}` sem PK no banco (pk_real.csv) para comparar.")
        elif pk_spec != pk_real:
            alertas.append(f"C6: PK diverge em `{t}`: specs={list(pk_spec)} banco={list(pk_real)}")

    # C8 — FK no specs que não existe no banco
    so_no_specs = fk_specs - fk_db
    for fk in sorted(so_no_specs, key=_fmt_fk):
        alertas.append(f"C8: FK no specs que NÃO existe no banco: {_fmt_fk(fk)}")

    return _print_resultado(criticos, alertas)


def _print_resultado(criticos: List[str], alertas: List[str]) -> bool:
    print("=" * 84)
    print("VALIDAÇÃO FINAL DO SPEC_CONFIG")
    print("=" * 84)
    print(f"\nCRÍTICOS (quebram o append): {len(criticos)}")
    for m in criticos:
        print(f"  [X] {m}")
    if not criticos:
        print("  (nenhum)")
    print(f"\nALERTAS (revisar, não bloqueiam): {len(alertas)}")
    for m in alertas:
        print(f"  [!] {m}")
    if not alertas:
        print("  (nenhum)")
    print("\n" + "=" * 84)
    ok = not criticos
    if ok:
        print("RESULTADO: specs APROVADO — sem críticos. Revise os alertas por garantia.")
    else:
        print(f"RESULTADO: specs REPROVADO — {len(criticos)} crítico(s) travariam o append.")
    print("=" * 84)
    return ok


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 5:
        raise SystemExit(
            "Uso: python valida_spec_final.py spec_config.json pk_real.csv "
            "fk_real.csv cols_real.csv [parquet_disp,sep,virgula]")
    disp = set(sys.argv[5].split(",")) if len(sys.argv) > 5 else None
    valida(spec_json=sys.argv[1], pk_csv=sys.argv[2], fk_csv=sys.argv[3],
           cols_csv=sys.argv[4], parquet_disponivel=disp)




from valida_spec_final import valida

valida(
    spec_json="spec_config.json",
    pk_csv="pk_real.csv",
    fk_csv="fk_real.csv",
    cols_csv="cols_real.csv",
    spark=spark,
    parquet_bases=["oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/onprem-export-full"],
)
