from pyspark.sql import functions as F

RAW_BASE = "oci://SEU_BUCKET@SEU_NAMESPACE/caminho/raw"   # ajuste
def raw_path(t): return f"{RAW_BASE}/{t}"

# (filha, [cols_fk], pai, [pk_do_pai])
CHECKS = [
    ("OPERACAO",                ["NUM_CONTA_PARTICIPANTE_P1"], "CONTA_PARTICIPANTE",     ["NUM_CONTA_PARTICIPANTE_CETIP"]),  # <-confirmar PK
    ("OPERACAO",                ["NUM_CONTA_PARTICIPANTE_P2"], "CONTA_PARTICIPANTE",     ["NUM_CONTA_PARTICIPANTE_CETIP"]),  # <-confirmar PK
    ("ESPECIFICACAO_COMITENTE", ["NUM_ID_ENTIDADE"],           "COMITENTE",              ["NUM_ID_ENTIDADE"]),               # <-confirmar PK
    ("CARTEIRA_COMITENTE",      ["NUM_ID_ENTIDADE"],           "COMITENTE",              ["NUM_ID_ENTIDADE"]),               # <-confirmar PK
    ("CONDICAO_IF",             ["NUM_IF"],                    "INSTRUMENTO_FINANCEIRO", ["NUM_IF"]),                        # <-confirmar PK
]

def null_counts(df, cols):
    present = [c for c in cols if c in df.columns]
    if not present: return {}
    row = df.agg(*[F.count(F.when(F.col(c).isNull(), F.lit(1))).alias(c) for c in present]).first()
    return {c: int(row[c]) for c in present}

def orphan_count_safe(child_df, parent_df, cols, pcols):
    parent_keys = (parent_df.select(*[F.col(p).alias(c) for c, p in zip(cols, pcols)])
                            .dropna().distinct())
    child_keys  = child_df.select(*cols).dropna().distinct()
    return child_keys.join(F.broadcast(parent_keys), on=cols, how="left_anti").count()

for child, cols, parent, pcols in CHECKS:
    cdf = spark.read.parquet(raw_path(child))
    pdf = spark.read.parquet(raw_path(parent))
    total = cdf.count()
    nulls = null_counts(cdf, cols)
    orph  = orphan_count_safe(cdf, pdf, cols, pcols)
    pct = {c: f"{100.0*n/total:.4f}%" for c, n in nulls.items()} if total else {}
    print(f"{child}.{','.join(cols)} -> {parent}.{','.join(pcols)}: "
          f"linhas={total:,} | nulos={nulls} {pct} | órfãos={orph:,}")
