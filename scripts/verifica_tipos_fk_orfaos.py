#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
verifica_tipos_fk_orfaos.py
===========================

Prova, em ~1 minuto e SEM re-rodar o engorda, a causa dos 4 fk_orphan do
pre-append check de 2026-07-13:

  COMITENTE.NUM_ID_PAIS_NACIONALIDADE -> PAIS.NUM_ID_PAIS
  CONTEXTO_MENSAGEM.NUM_ID_TP_ESTADO  -> TIPO_ESTADO.NUM_ID_TP_ESTADO
  LOTE.NUM_TIPO_IF                    -> TIPO_IF.NUM_TIPO_IF
  PESSOA_JURIDICA.NUM_ID_PAIS         -> PAIS.NUM_ID_PAIS

Lê o output sintético JÁ GRAVADO (o mesmo run que falhou) e, por aresta,
compara os dois métodos de detecção de órfã:

  - STRING  : cast("string") + anti-join — exatamente o que o
              validate_cdb_simplificado.py (check_referential) faz;
  - NUMÉRICO: anti-join com igualdade nativa do Spark (coerção de tipos) —
              exatamente o que os passes do engorda (null_orphan_fks etc.)
              usam.

Interpretação:
  numérico = 0  e  string > 0  e  tipos físicos divergentes
      => falso órfão por representação (decimal(38,10) '206.0000000000' vs
         int '206'); o passe harmoniza_tipos_fk_com_pai do engorda_tables.py
         resolve a aresta com certeza (mesmo tipo + mesmo valor => mesma
         string) — pode rodar o run de 2h.
  numérico > 0
      => órfão REAL; o fix de tipos NÃO cobre — investigar antes de rodar.

Uso (mesmo ambiente Spark do validador; não precisa de Oracle):
  spark-submit scripts/verifica_tipos_fk_orfaos.py

Env:
  DATAGEN_SYNTHETIC_BASE_URI  (obrigatória; mesma do validador)
  DATAGEN_SYNTHETIC_PREFIX    (opcional)
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# (tabela filha, coluna FK, tabela pai, coluna do pai) — as 4 arestas acusadas.
EDGES = [
    ("COMITENTE", "NUM_ID_PAIS_NACIONALIDADE", "PAIS", "NUM_ID_PAIS"),
    ("CONTEXTO_MENSAGEM", "NUM_ID_TP_ESTADO", "TIPO_ESTADO", "NUM_ID_TP_ESTADO"),
    ("LOTE", "NUM_TIPO_IF", "TIPO_IF", "NUM_TIPO_IF"),
    ("PESSOA_JURIDICA", "NUM_ID_PAIS", "PAIS", "NUM_ID_PAIS"),
]


def main() -> None:
    base = os.environ.get("DATAGEN_SYNTHETIC_BASE_URI", "").strip().rstrip("/")
    if not base:
        sys.exit("DATAGEN_SYNTHETIC_BASE_URI é obrigatória.")
    prefix = os.environ.get("DATAGEN_SYNTHETIC_PREFIX", "").strip().strip("/")
    if prefix:
        base = f"{base}/{prefix}"

    spark = SparkSession.builder.appName("verifica_tipos_fk_orfaos").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dfs = {}
    confirmadas = 0
    problemas = 0
    for child, ccol, parent, pcol in EDGES:
        for t in (child, parent):
            if t not in dfs:
                dfs[t] = spark.read.parquet(f"{base}/{t}")
        cdf, pdf = dfs[child], dfs[parent]

        tipo_filha = cdf.schema[ccol].dataType.simpleString()
        tipo_pai = pdf.schema[pcol].dataType.simpleString()

        # Método do validador: chaves como STRING.
        ck_str = (cdf.select(F.col(ccol).cast("string").alias("k"))
                  .where(F.col("k").isNotNull()).distinct())
        pk_str = (pdf.select(F.col(pcol).cast("string").alias("k"))
                  .where(F.col("k").isNotNull()).distinct())
        orfas_string = ck_str.join(pk_str, "k", "left_anti").count()

        # Método do engorda: igualdade nativa (Spark coage os tipos).
        ck_num = cdf.select(F.col(ccol).alias("k")).dropna().distinct()
        pk_num = pdf.select(F.col(pcol).alias("k")).dropna().distinct()
        orfas_numerico = ck_num.join(
            pk_num, ck_num["k"] == pk_num["k"], "left_anti").count()
        distintas = ck_num.count()

        print(f"\n{child}.{ccol} ({tipo_filha})  ->  {parent}.{pcol} ({tipo_pai})")
        print(f"  chaves distintas nao-nulas na filha  : {distintas}")
        print(f"  orfas por STRING (metodo do validador): {orfas_string}")
        print(f"  orfas por IGUALDADE NUMERICA (engorda): {orfas_numerico}")

        if orfas_numerico == 0 and orfas_string > 0 and tipo_filha != tipo_pai:
            confirmadas += 1
            print("  => CONFIRMADO: falso orfao por tipo fisico divergente; "
                  "harmoniza_tipos_fk_com_pai resolve esta aresta.")
        elif orfas_numerico == 0 and orfas_string == 0:
            print("  => aresta ja integra nas duas comparacoes "
                  "(nada a corrigir aqui).")
        else:
            problemas += 1
            print("  => ATENCAO: orfao NUMERICO real (ou string divergente com "
                  "tipos iguais) — o fix de tipos NAO cobre; investigar ANTES "
                  "de rodar o engorda.")

    print("\n" + "=" * 70)
    if problemas == 0:
        print(f"RESULTADO: diagnostico confirmado em {confirmadas} aresta(s) "
              "divergente(s); as demais ja integras. Pode rodar o engorda "
              "com o fix.")
    else:
        print(f"RESULTADO: {problemas} aresta(s) NAO se encaixam no diagnostico "
              "de tipo fisico — NAO gaste o run de 2h ainda; investigar.")
    spark.stop()
    sys.exit(0 if problemas == 0 else 1)


if __name__ == "__main__":
    main()
