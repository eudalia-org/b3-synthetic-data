#!/usr/bin/env python3
"""
diagnostica_pais_fk.py

Cruza três fontes para descobrir, para cada tabela-pai referenciada por FK,
se ela vai conseguir ser remapeada na síntese ou se vai deixar a FK órfã:

  1. fk_real.csv          -> quais tabelas são referenciadas como PAI
  2. spec_config.json     -> quais têm BLOCO (senão a FK é descartada no saneamento)
  3. parquet disponível   -> quais têm DADO (senão o synthesizer pula o pai na leitura)

Uma FK só é remapeada (coluna preenchida) se o pai tem BLOCO **e** PARQUET.
Faltando qualquer um, a coluna da filha é anulada por null_orphan_fks — e se
for NOT NULL no banco, o append quebra (ORA-01400/02291).

MODO DE DESCOBRIR O PARQUET:
  - Se rodar no notebook com `spark`, passe spark + base(s) OCI e o script tenta
    ler cada pai (spark.read.parquet) para ver se existe.
  - Sem spark, passe uma lista PARQUET_DISPONIVEL com os nomes que você sabe que
    têm parquet (ex.: as 47 + as que o engenheiro subiu).

USO (notebook, com spark):
    from diagnostica_pais_fk import diagnostica
    diagnostica(
        fk_csv="fk_real.csv",
        specs_json="spec_config.json",
        spark=spark,
        parquet_bases=["oci://bucket@ns/onprem-export-full"],
    )

USO (sem spark, lista manual):
    from diagnostica_pais_fk import diagnostica
    diagnostica(
        fk_csv="fk_real.csv",
        specs_json="spec_config.json",
        parquet_disponivel={"COMITENTE", "GRP_MODALIDADE_LIQUIDACAO", ...},
    )
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from typing import Dict, List, Optional, Set


def _norm(s: str) -> str:
    return (s or "").strip().upper()


def _pais_referenciados(fk_csv: str) -> Dict[str, List[str]]:
    """pai -> lista de 'filha.[colunas]' que o referenciam."""
    refs: Dict[str, List[str]] = defaultdict(list)
    cols_por_constraint: Dict[str, list] = defaultdict(list)
    meta: Dict[str, tuple] = {}
    with open(fk_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        has_cn = "CONSTRAINT_NAME" in (reader.fieldnames or [])
        for row in reader:
            child = _norm(row["CHILD_TABLE"])
            parent = _norm(row["PARENT_TABLE"])
            ccol = _norm(row["CHILD_COLUMN"])
            key = _norm(row["CONSTRAINT_NAME"]) if has_cn else f"{child}|{parent}"
            meta[key] = (child, parent)
            cols_por_constraint[key].append((int(row["COL_POSITION"]), ccol))
    for key, (child, parent) in meta.items():
        ccols = [c for _, c in sorted(cols_por_constraint[key])]
        refs[parent].append(f"{child}.{ccols}")
    return refs


def _tabelas_com_bloco(specs_json: str) -> Set[str]:
    with open(specs_json, encoding="utf-8") as f:
        specs = json.load(f)
    return {_norm(t) for t in specs}


def _tem_parquet_spark(spark, bases: List[str], table: str) -> bool:
    for base in bases:
        path = f"{base.rstrip('/')}/{table}"
        try:
            spark.read.parquet(path).take(1)
            return True
        except Exception:
            continue
    return False


def diagnostica(
    *,
    fk_csv: str,
    specs_json: str,
    spark=None,
    parquet_bases: Optional[List[str]] = None,
    parquet_disponivel: Optional[Set[str]] = None,
) -> None:
    refs = _pais_referenciados(fk_csv)
    com_bloco = _tabelas_com_bloco(specs_json)

    pais = sorted(refs)

    # resolve disponibilidade de parquet
    disp: Set[str] = set(_norm(t) for t in (parquet_disponivel or set()))
    checar_spark = spark is not None and parquet_bases

    print("=" * 90)
    print("DIAGNÓSTICO DE PAIS DE FK — bloco no specs x parquet disponível")
    print("=" * 90)
    print(f"{'PAI':<32} {'BLOCO':<8} {'PARQUET':<9} AÇÃO")
    print("-" * 90)

    faltam_parquet: List[str] = []
    faltam_bloco_tem_parquet: List[str] = []
    ok: List[str] = []

    for parent in pais:
        tem_bloco = parent in com_bloco
        if checar_spark:
            tem_parq = _tem_parquet_spark(spark, parquet_bases, parent)
        else:
            tem_parq = parent in disp

        if tem_bloco and tem_parq:
            acao = "OK — será remapeada"
            ok.append(parent)
        elif not tem_bloco and tem_parq:
            acao = "ADD BLOCO (incluir no pk_real -> regerar specs)"
            faltam_bloco_tem_parquet.append(parent)
        elif tem_bloco and not tem_parq:
            acao = "SUBIR PARQUET (bloco existe, falta dado)"
            faltam_parquet.append(parent)
        else:
            acao = "SUBIR PARQUET + ADD BLOCO"
            faltam_parquet.append(parent)

        b = "sim" if tem_bloco else "NAO"
        p = "sim" if tem_parq else "NAO"
        print(f"{parent:<32} {b:<8} {p:<9} {acao}")

    print("-" * 90)
    print(f"\nRESUMO:")
    print(f"  OK (bloco + parquet):            {len(ok)}")
    print(f"  Falta só BLOCO (tem parquet):    {len(faltam_bloco_tem_parquet)} "
          f"-> {faltam_bloco_tem_parquet}")
    print(f"  Falta PARQUET:                   {len(faltam_parquet)} "
          f"-> {faltam_parquet}")

    if faltam_bloco_tem_parquet:
        print("\n>> Ação imediata: remover o filtro IN da query de PK e regerar o")
        print("   specs. Estes pais TÊM parquet, só faltou bloco por não estarem")
        print("   no pk_real.csv.")
    if faltam_parquet:
        print("\n>> Depende do engenheiro: estes pais precisam de parquet no OCI.")
        print("   Enquanto não tiverem, as FKs que apontam pra eles ficam órfãs.")
        print("   Se a FK for NOT NULL, o append quebra (ORA-01400). Cruze esta")
        print("   lista com o cols_real.csv (NOT NULL) para priorizar.")
    print("=" * 90)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        raise SystemExit(
            "Uso: python diagnostica_pais_fk.py fk_real.csv spec_config.json "
            "[tab1,tab2,... parquet disponível]"
        )
    fk_csv, specs_json = sys.argv[1], sys.argv[2]
    disp = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else set()
    diagnostica(fk_csv=fk_csv, specs_json=specs_json, parquet_disponivel=disp)
