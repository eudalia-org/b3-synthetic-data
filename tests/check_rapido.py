# =====================================================================
# DIAGNÓSTICO DE NULOS/ÓRFÃOS — cole no notebook (Spark já ativo como `spark`)
# =====================================================================
from pyspark.sql import functions as F
from functools import reduce

# --- AJUSTE ISTO --------------------------------------------------
RAW_BASE = "oci://SEU_BUCKET@SEU_NAMESPACE/caminho/raw"  # = DATAGEN_RAW_BASE_URI (+ prefix, se houver)

def raw_path(table):
    return f"{RAW_BASE}/{table}"

# Cada entrada: (tabela_filha, [colunas_fk], tabela_pai, [colunas_pk_do_pai])
# Preencha os pais/PKs conforme seu specs.json.
CHECKS = [
    ("OPERACAO",                ["NUM_CONTA_PARTICIPANTE_P1"], "CONTA_PARTICIPANTE", ["NUM_CONTA_PARTICIPANTE_CETIP"]),
    ("OPERACAO",                ["NUM_CONTA_PARTICIPANTE_P2"], "CONTA_PARTICIPANTE", ["NUM_CONTA_PARTICIPANTE_CETIP"]),
    ("LANCAMENTO",              ["NUM_ID_ENTIDADE"],           "USUARIO",            ["NUM_ID_ENTIDADE"]),
    ("ESPECIFICACAO_COMITENTE", ["NUM_ID_ENTIDADE"],           "USUARIO",            ["NUM_ID_ENTIDADE"]),
    ("CONDICAO_IF",             ["NUM_IF"],                    "INSTRUMENTO_FINANCEIRO", ["NUM_IF"]),
]
# ------------------------------------------------------------------

def null_counts(df, cols):
    present = [c for c in cols if c in df.columns]
    if not present:
        return {}
    row = df.agg(*[F.count(F.when(F.col(c).isNull(), F.lit(1))).alias(c) for c in present]).first()
    return {c: int(row[c]) for c in present}

def orphan_count(child_df, parent_df, cols, pcols):
    # MATCH SIMPLE: linha com QUALQUER coluna da FK nula não conta como órfã.
    child_keys  = child_df.select(*cols).dropna().distinct()
    parent_keys = parent_df.select(*[F.col(p).alias(c) for c, p in zip(cols, pcols)]).dropna().distinct()
    return child_keys.join(parent_keys, on=cols, how="left_anti").count()

print("=== NÍVEL A: tabela INTEIRA (sem filtro de domínio) ===")
for child, cols, parent, pcols in CHECKS:
    cdf = spark.read.parquet(raw_path(child))
    pdf = spark.read.parquet(raw_path(parent))
    total = cdf.count()
    nulls = null_counts(cdf, cols)
    orph  = orphan_count(cdf, pdf, cols, pcols)
    pct = {c: f"{100.0*n/total:.3f}%" for c, n in nulls.items()} if total else {}
    print(f"{child}.{','.join(cols)} -> {parent}: linhas={total:,} | nulos={nulls} {pct} | órfãos={orph:,}")
