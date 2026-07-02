"""
diagnostica_sampling.py — identifica com CERTEZA a causa de (a) tabelas
zeradas e (b) FKs anuladas no pipeline de engorda, sem alterar nada.

Replay instrumentado do sampling do engorda_tables.py, em 5 estágios:

  [1] CICLOS      — lista as arestas de FK que violam a ordem topológica
                    (quebradas em silêncio por topo_order_tables). Cada uma
                    é uma poda PULADA na descida e um risco de resíduo no
                    fecho.
  [2] DESCIDA     — replay da poda descendente com contador POR ARESTA:
                    n_antes, n_depois, n_all_null_mantidas, arestas puladas.
                    Mostra exatamente qual FK zerou qual tabela.
  [3] FECHO       — fecho ascendente iterado a PONTO FIXO com contador por
                    passe. Se o passe >= 2 puxar linhas, está PROVADO que a
                    passada única do código de produção é insuficiente
                    (hipótese N2).
  [4] ÓRFÃOS      — para cada FK, classifica as chaves órfãs pós-fecho:
                      * existe no Parquet COMPLETO do pai -> BUG DE PROCESSO
                        (o fecho deveria ter puxado e não puxou);
                      * não existe -> ÓRFÃ DE PRODUÇÃO (nada a puxar).
                    Distingue N1/N2 de N3.
  [5] DOMÍNIO 49  — (opcional, custa full scan por tabela, só colunas de
                    chave) recomputa o domínio NUM_TIPO_IF==49 por CHAVE a
                    partir dos Parquets COMPLETOS, sem --limit. Para cada
                    tabela zerada na amostra: domínio > 0 => artefato do
                    --limit (Z1); domínio == 0 => zero é correto (Z2).

Uso (mesmo ambiente/env vars do engorda_tables.py):

    python diagnostica_sampling.py --limit 10000
    python diagnostica_sampling.py --limit 10000 --sem-dominio-full
    python diagnostica_sampling.py --limit 10000 \
        --tabelas TITULO,CARTEIRA_COMITENTE,CONDICAO_IF

Observações:
  - NÃO grava nada em lugar nenhum; só lê os Parquets e imprime o laudo.
  - NÃO roda a neutralização: o objetivo é VER os órfãos, não consertá-los.
  - df.limit(N) não é determinístico entre execuções: as contagens podem
    variar levemente vs o run original (ex.: 7.881 vs 7.8xx). A CLASSE do
    resultado (zero/não-zero, existe/não-existe) é estável.
"""
from __future__ import annotations

import argparse
import logging
import sys
from functools import reduce
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, functions as F

import engorda_tables as et

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("diagnostica_sampling")

SEP = "=" * 78
SUBSEP = "-" * 78


# ---------------------------------------------------------------------------
# [1] CICLOS: arestas que violam a ordem topológica
# ---------------------------------------------------------------------------
def relatorio_ciclos(comp_specs: dict) -> List[Tuple[str, str]]:
    """Arestas filha->pai onde o PAI aparece DEPOIS da filha na ordem usada
    pela descida (topo_order_tables). Cada uma é uma poda pulada
    (`parent not in sampled`) — hoje SILENCIOSA no código de produção."""
    order = et.topo_order_tables(comp_specs)
    pos = {t: i for i, t in enumerate(order)}
    violadas: List[Tuple[str, str]] = []
    for table, cfg in comp_specs.items():
        for fk in et._fk_list(cfg):
            parent = fk.get("parent_table")
            if parent in comp_specs and parent != table and pos[parent] > pos[table]:
                violadas.append((table, parent))

    print(SEP)
    print("[1] CICLOS / ARESTAS FORA DE ORDEM NA DESCIDA")
    print(SEP)
    print(f"Ordem da descida: {' -> '.join(order)}")
    if violadas:
        print(f"\n!! {len(violadas)} aresta(s) com pai DEPOIS da filha "
              "(poda pulada em silêncio na descida):")
        for child, parent in violadas:
            print(f"   PULADA: {child} -> {parent}")
    else:
        print("\nOK: nenhuma aresta viola a ordem (componente é DAG na prática).")
    return violadas


# ---------------------------------------------------------------------------
# [2] DESCIDA instrumentada (mesma lógica do referential_sample, com contadores)
# ---------------------------------------------------------------------------
def descida_instrumentada(spark, config, comp_specs: dict,
                          limit: Optional[int]) -> Dict[str, DataFrame]:
    order = et.topo_order_tables(comp_specs)
    sampled: Dict[str, DataFrame] = {}
    broadcast_keys = limit is not None

    print(SEP)
    print(f"[2] DESCIDA (poda referencial) — limit={limit}")
    print(SEP)

    for table in order:
        df = et._aplica_filtro_tipo_if(
            et.read_parquet(spark, et.raw_path(config, table)), table)
        n_raiz = df.count()
        eh_raiz = table in et.TABELAS_RAIZ_FILTRO
        print(f"\n### {table}"
              f"{'  [RAIZ: filtro NUM_TIPO_IF=' + str(et.FILTRO_TIPO_IF_VALUE) + ']' if eh_raiz else ''}")
        print(f"    linhas iniciais{' (pós-filtro raiz)' if eh_raiz else ''}: {n_raiz:,}")

        for fk in et._fk_list(comp_specs[table]):
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            rotulo = f"{','.join(cols)} -> {parent}.{','.join(pcols)}"

            if parent == table:
                print(f"    FK {rotulo}: self — não poda (fecho trata)")
                continue
            if parent not in sampled or not cols or len(cols) != len(pcols):
                motivo = ("PAI AINDA NAO AMOSTRADO (ciclo!)"
                          if parent in comp_specs else "pai fora do componente")
                print(f"    FK {rotulo}: !! PODA PULADA — {motivo}")
                continue

            n_antes = df.count()
            keys = (sampled[parent]
                    .select(*[F.col(pc).alias(f"__k{i}") for i, pc in enumerate(pcols)])
                    .dropna().distinct())
            cond = reduce(lambda a, b: a & b,
                          [df[cols[i]] == keys[f"__k{i}"] for i in range(len(cols))])
            keys_side = F.broadcast(keys) if broadcast_keys else keys
            joined = df.join(keys_side, cond, "left")
            all_fk_null = reduce(lambda a, b: a & b,
                                 [F.col(c).isNull() for c in cols])
            df = (joined
                  .where(F.col("__k0").isNotNull() | all_fk_null)
                  .drop(*[f"__k{i}" for i in range(len(pcols))]))
            df = df.localCheckpoint(eager=True)
            n_depois = df.count()
            n_allnull = df.where(all_fk_null).count()
            queda = n_antes - n_depois
            pct = (100.0 * queda / n_antes) if n_antes else 0.0
            marca = "  <== DERRUBOU TUDO" if (n_antes > 0 and n_depois == 0) else ""
            print(f"    FK {rotulo}: {n_antes:,} -> {n_depois:,} "
                  f"(-{queda:,}, {pct:.1f}%; all-null mantidas: {n_allnull:,}){marca}")

        if limit is not None:
            df = df.limit(limit)
        df = df.localCheckpoint(eager=True)
        n_final = df.count()
        marca = "  <== ZERADA" if n_final == 0 else ""
        print(f"    FINAL pós-limit: {n_final:,}{marca}")
        sampled[table] = df
    return sampled


# ---------------------------------------------------------------------------
# [3] FECHO ascendente iterado a PONTO FIXO, com contadores por passe
# ---------------------------------------------------------------------------
def fecho_instrumentado(spark, config, comp_specs: dict,
                        sampled: Dict[str, DataFrame],
                        broadcast_missing: bool,
                        max_passes: int = 10) -> Dict[str, DataFrame]:
    print(SEP)
    print("[3] FECHO ASCENDENTE — iterado a ponto fixo")
    print("    (produção roda UMA passada; se o passe >= 2 puxar linhas,")
    print("     a insuficiência da passada única está PROVADA — hipótese N2)")
    print(SEP)

    order = et.topo_order_tables(comp_specs)
    total_por_passe: List[int] = []

    for passe in range(1, max_passes + 1):
        puxadas_passe = 0
        print(f"\n--- passe {passe} ---")
        for child in reversed(order):
            child_df = sampled.get(child)
            if child_df is None:
                continue
            for fk in et._fk_list(comp_specs[child]):
                parent = fk.get("parent_table")
                cols = list(fk.get("columns") or [])
                pcols = list(fk.get("parent_columns") or [])
                if (not cols or len(cols) != len(pcols)
                        or parent not in sampled):
                    continue
                eh_self = parent == child
                if eh_self and set(cols) & set(pcols):
                    continue  # degenerada
                alvo_df = sampled[child] if eh_self else sampled[parent]
                ref_keys = (sampled[child]
                            .select(*[F.col(c).alias(p)
                                      for c, p in zip(cols, pcols)])
                            .dropna().distinct())
                faltantes = ref_keys.join(
                    alvo_df.select(*pcols).distinct(),
                    on=pcols, how="left_anti").localCheckpoint(eager=True)
                n_falt = faltantes.count()
                if n_falt == 0:
                    continue
                faltantes_side = (F.broadcast(faltantes)
                                  if broadcast_missing else faltantes)
                alvo = child if eh_self else parent
                extra = (et.read_parquet(spark, et.raw_path(config, alvo))
                         .join(faltantes_side, on=pcols, how="left_semi")
                         .localCheckpoint(eager=True))
                n_puxadas = extra.count()
                print(f"    {child}.{','.join(cols)} -> {alvo}"
                      f"{' (self)' if eh_self else ''}: "
                      f"{n_falt:,} chave(s) faltante(s), "
                      f"{n_puxadas:,} linha(s) puxada(s)"
                      + ("  !! chaves sem linha no Parquet completo: "
                         f"{n_falt - n_puxadas:,} (orfa de producao)"
                         if n_puxadas < n_falt else ""))
                if n_puxadas > 0:
                    sampled[alvo] = (sampled[alvo].unionByName(extra)
                                     .localCheckpoint(eager=True))
                    puxadas_passe += n_puxadas
        total_por_passe.append(puxadas_passe)
        print(f"--- passe {passe}: {puxadas_passe:,} linha(s) puxada(s) no total ---")
        if puxadas_passe == 0:
            break

    print(f"\nResumo dos passes: {total_por_passe}")
    if len(total_por_passe) > 1 and any(n > 0 for n in total_por_passe[1:]):
        print(">>> VEREDITO N2 CONFIRMADO: passes >= 2 puxaram linhas. A")
        print(">>> passada UNICA do codigo de producao deixa orfaos que a")
        print(">>> neutralizacao depois anula/dropa. Fecho precisa de ponto fixo.")
    else:
        print(">>> N2 refutada neste componente: 1 passada bastou.")
    if total_por_passe and total_por_passe[-1] > 0:
        print(">>> ATENCAO: nao convergiu em"
              f" {max_passes} passes — investigar antes de qualquer fix.")
    return sampled


# ---------------------------------------------------------------------------
# [4] Classificação dos órfãos residuais: bug de processo vs produção
# ---------------------------------------------------------------------------
def classifica_orfaos(spark, config, comp_specs: dict,
                      sampled: Dict[str, DataFrame]) -> None:
    print(SEP)
    print("[4] ORFAOS RESIDUAIS POS-FECHO — classificacao")
    print("    chave existe no Parquet COMPLETO do pai -> BUG DE PROCESSO")
    print("    chave NAO existe                        -> ORFA DE PRODUCAO")
    print(SEP)

    algum = False
    for child, cfg in comp_specs.items():
        child_df = sampled.get(child)
        if child_df is None:
            continue
        pk_set = set(cfg.get("pk_cols") or [])
        for fk in et._fk_list(cfg):
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = list(fk.get("parent_columns") or [])
            if (parent not in sampled or not cols or len(cols) != len(pcols)):
                continue
            eh_self = parent == child
            if eh_self and set(cols) & set(pcols):
                continue
            base_pai = sampled[child] if eh_self else sampled[parent]
            parent_keys = (base_pai
                           .select(*[F.col(p).alias(c)
                                     for c, p in zip(cols, pcols)])
                           .distinct())
            orfas = (child_df.select(*cols).dropna().distinct()
                     .join(parent_keys, on=cols, how="left_anti")
                     .localCheckpoint(eager=True))
            n_orf = orfas.count()
            if n_orf == 0:
                continue
            algum = True
            full_keys = (et.read_parquet(spark, et.raw_path(config, parent))
                         .select(*[F.col(p).alias(c)
                                   for c, p in zip(cols, pcols)])
                         .distinct())
            producao = orfas.join(full_keys, on=cols, how="left_anti")
            n_prod = producao.count()
            n_bug = n_orf - n_prod
            nullable_no_spec = [c for c in cols if c not in pk_set]
            destino = ("ANULADA (nao-PK)" if nullable_no_spec
                       else "DROPADA (FK dentro da PK)")
            print(f"\n### {child}.{','.join(cols)} -> {parent}"
                  f"{' (self)' if eh_self else ''}")
            print(f"    chaves orfas distintas: {n_orf:,}")
            print(f"      -> existem no Parquet completo (BUG DE PROCESSO): {n_bug:,}")
            print(f"      -> nao existem (ORFA DE PRODUCAO):               {n_prod:,}")
            print(f"    a neutralizacao trataria como: {destino}")
            if n_bug > 0:
                print("    >>> VEREDITO: o fecho NAO completou chaves que EXISTEM.")
                print("    >>> Anular/dropar aqui destroi dado recuperavel.")
            if n_prod > 0:
                print("    >>> Orfas de producao reais: conferir constraint no Oracle")
                print("    >>> (NOVALIDATE/DISABLED?) — Parquet==Oracle nao basta,")
                print("    >>> a constraint pode nao valer para linhas antigas.")
    if not algum:
        print("\nOK: nenhum orfao residual pos-fecho em nenhuma FK.")


# ---------------------------------------------------------------------------
# [5] Domínio 49 completo por CHAVE (sem limit) — só colunas de chave
# ---------------------------------------------------------------------------
def dominio_full_por_chave(spark, config, comp_specs: dict,
                           zeradas: List[str]) -> None:
    print(SEP)
    print("[5] DOMINIO 49 COMPLETO POR CHAVE (sem --limit; so colunas de chave)")
    print("    tabela zerada com dominio > 0  -> Z1: artefato do --limit")
    print("    tabela zerada com dominio == 0 -> Z2: zero e o valor CORRETO")
    print(SEP)

    # Quais colunas de cada pai são referenciadas (para propagar só chaves).
    parent_refs: Dict[str, set] = {}
    for cfg in comp_specs.values():
        for fk in et._fk_list(cfg):
            p = fk.get("parent_table")
            pcols = tuple(fk.get("parent_columns") or [])
            if p in comp_specs and pcols:
                parent_refs.setdefault(p, set()).add(pcols)

    order = et.topo_order_tables(comp_specs)
    domain_keys: Dict[Tuple[str, Tuple[str, ...]], DataFrame] = {}

    for table in order:
        df = et._aplica_filtro_tipo_if(
            et.read_parquet(spark, et.raw_path(config, table)), table)
        # Poda por chave contra o domínio COMPLETO dos pais (sem limit).
        for fk in et._fk_list(comp_specs[table]):
            parent = fk.get("parent_table")
            cols = list(fk.get("columns") or [])
            pcols = tuple(fk.get("parent_columns") or [])
            key = (parent, pcols)
            if parent == table or key not in domain_keys or not cols \
                    or len(cols) != len(pcols):
                continue
            keys = domain_keys[key]
            cond = reduce(lambda a, b: a & b,
                          [df[cols[i]] == keys[f"__k{i}"]
                           for i in range(len(cols))])
            joined = df.join(keys, cond, "left")
            all_fk_null = reduce(lambda a, b: a & b,
                                 [F.col(c).isNull() for c in cols])
            df = (joined
                  .where(F.col("__k0").isNotNull() | all_fk_null)
                  .drop(*[f"__k{i}" for i in range(len(pcols))]))
        n_dom = df.count()
        marca = ""
        if table in zeradas:
            marca = ("  <== Z1: ARTEFATO DO --limit (dominio completo tem linhas)"
                     if n_dom > 0 else
                     "  <== Z2: dominio realmente vazio (zero e correto)")
        print(f"    {table}: dominio completo = {n_dom:,}{marca}")
        # Publica as chaves referenciadas desta tabela para as filhas.
        for pcols in parent_refs.get(table, set()):
            domain_keys[(table, pcols)] = (
                df.select(*[F.col(pc).alias(f"__k{i}")
                            for i, pc in enumerate(pcols)])
                .dropna().distinct()
                .localCheckpoint(eager=True))


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--specs", default=None)
    ap.add_argument("--tabelas", default=None,
                    help="Foco: roda so o(s) componente(s) que contem estas "
                         "tabelas (separadas por virgula).")
    ap.add_argument("--sem-dominio-full", action="store_true",
                    help="Pula o estagio [5] (full scan das colunas de chave).")
    ap.add_argument("--max-passes-fecho", type=int, default=10)
    args = ap.parse_args()

    config = et.get_engorda_env()
    spark = et.create_spark_session("DiagnosticaSampling")
    try:
        specs = et.load_specs(spark, args.specs or config["DATAGEN_SPECS_URI"])
        foco = (set(t.strip().upper() for t in args.tabelas.split(","))
                if args.tabelas else None)

        for comp in et.connected_components(specs):
            if foco and not (foco & set(comp)):
                continue
            comp_specs = {t: specs[t] for t in comp}
            print(f"\n{SEP}\nCOMPONENTE: {', '.join(sorted(comp))}\n{SEP}")

            relatorio_ciclos(comp_specs)
            sampled = descida_instrumentada(spark, config, comp_specs, args.limit)
            zeradas = [t for t, df in sampled.items() if df.count() == 0]
            sampled = fecho_instrumentado(
                spark, config, comp_specs, sampled,
                broadcast_missing=args.limit is not None,
                max_passes=args.max_passes_fecho)
            classifica_orfaos(spark, config, comp_specs, sampled)
            if not args.sem_dominio_full:
                dominio_full_por_chave(spark, config, comp_specs, zeradas)
            elif zeradas:
                print(f"\n[5] PULADO (--sem-dominio-full). Tabelas zeradas sem "
                      f"veredito Z1/Z2: {', '.join(zeradas)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()




import os
os.environ["DATAGEN_RAW_BASE_URI"] = "oci://oci-st-blc-engordai-qab-n@gr97zovfhcmu/<prefixo_raw>"
os.environ["DATAGEN_SYNTHETIC_BASE_URI"] = "oci://.../<prefixo_synthetic>"  # não será gravado, mas o env exige
os.environ["DATAGEN_SPECS_URI"] = "oci://.../specs.json"



from pyspark.sql import SparkSession

# se o kernel já tem uma sessão com confs antigas: spark.stop() antes,
# porque driver.memory só vale na criação
spark = (SparkSession.builder
    .appName("DiagnosticaSampling")
    .master("local[*]")
    .config("spark.driver.memory", "48g")            # ~75% dos 64GB
    .config("spark.sql.shuffle.partitions", "64")    # NÃO os 8000 de produção
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.aggregatePushdown", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # + as confs do conector OCI HDFS que vocês já usam para ler oci://
    #   (jar do conector + auth via ~/.oci/config, o padrão de vocês)
    .getOrCreate())



import engorda_tables as et
import diagnostica_sampling as diag

config = et.get_engorda_env()
specs = et.load_specs(spark, config["DATAGEN_SPECS_URI"])

foco = {"CONDICAO_IF"}  # qualquer tabela do componente das 15
comp = next(c for c in et.connected_components(specs) if foco & set(c))
comp_specs = {t: specs[t] for t in comp}
print(f"Componente: {len(comp)} tabelas")


# [1] ciclos — instantâneo, só metadado
violadas = diag.relatorio_ciclos(comp_specs)


# [2] descida com contadores por aresta — identifica a FK que zerou cada tabela
sampled = diag.descida_instrumentada(spark, config, comp_specs, limit=10000)
zeradas = [t for t, df in sampled.items() if df.count() == 0]
print("Zeradas:", zeradas)

# [4] classificação dos órfãos — bug de processo vs órfã de produção
diag.classifica_orfaos(spark, config, comp_specs, sampled)

# [5] domínio 49 completo por chave — veredito Z1/Z2 das zeradas
# (o mais caro: um scan por tabela nas colunas de chave; rode por último)
diag.dominio_full_por_chave(spark, config, comp_specs, zeradas)
