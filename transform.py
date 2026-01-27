from __future__ import annotations

import json
import logging
import re
import sys
from typing import Dict, List, Optional, Sequence, Tuple

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ======================
# Função principal (Spark)
# ======================
def synthesize_table_spark(
    sdf: DataFrame,
    keys: Sequence[str],
    *,
    seed: int,
    numeric_transform: str = "scale_0_100",  # "scale_0_100" | "multiply"
    multiplier: float = 1.0,
    global_minmax: Optional[_MinMax] = None,
    tokenize_strings: bool = True,
    keep_int: bool = True,
    non_negative_cols: Optional[Sequence[str]] = None,
) -> DataFrame:
    """
    - Keys: hash (string)
    - Numéricas: sem ruído. "scale_0_100" global por nome OU "multiply".
    - Date/Timestamp: NÃO MUDA.
    - Strings: tokenização determinística (se tokenize_strings=True).
    """
    non_negative_cols = set(non_negative_cols or [])
    keys = list(keys or [])
    df = sdf

    # 1) Hash das chaves
    for k in keys:
        if k in df.columns:
            df = df.withColumn(k, _expr_hash_key_value(seed, k, F.col(k)))

    # 2) Numéricos
    for f in df.schema.fields:
        c = f.name
        if c in keys:
            continue
        dt = f.dataType
        if _is_boolean_type(dt) or _is_datetime_type(dt):
            continue
        if _is_numeric_type(dt):
            col = F.col(c).cast("double")

            if numeric_transform == "scale_0_100":
                if global_minmax and c in global_minmax:
                    mn, mx = global_minmax[c]
                else:
                    # min/max locais como fallback
                    mm = df.agg(F.min(F.col(c)).alias("mn"), F.max(F.col(c)).alias("mx")).collect()[
                        0
                    ]
                    mn, mx = float(mm["mn"]), float(mm["mx"])
                denom = mx - mn
                if denom == 0.0:
                    expr = F.when(col.isNotNull(), F.lit(0.0)).otherwise(F.lit(None).cast("double"))
                else:
                    expr = (col - F.lit(mn)) / F.lit(denom) * F.lit(100.0)

            elif numeric_transform == "multiply":
                expr = col * F.lit(float(multiplier))

            else:
                raise ValueError("numeric_transform deve ser 'scale_0_100' ou 'multiply'.")

            if c in non_negative_cols:
                expr = F.greatest(expr, F.lit(0.0))

            # Voltar ao tipo original se era inteiro e keep_int=True
            if keep_int and isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
                # arredonda e faz cast de volta
                if isinstance(dt, T.ByteType):
                    expr = F.round(expr).cast("byte")
                elif isinstance(dt, T.ShortType):
                    expr = F.round(expr).cast("short")
                elif isinstance(dt, T.IntegerType):
                    expr = F.round(expr).cast("int")
                else:
                    expr = F.round(expr).cast("long")
            else:
                expr = expr.cast("double")

            df = df.withColumn(c, expr)

    # 3) Datetimes: não mexe (já passamos reto)

    # 4) Strings → tokens
    if tokenize_strings:
        for f in df.schema.fields:
            c = f.name
            if c in keys:
                continue
            dt = f.dataType
            if _is_stringish_type(dt) and not _is_datetime_type(dt):
                df = df.withColumn(c, _expr_token_from_text(seed, c, F.col(c)))

    return df


# ===========================
# Multi-tabelas com escala global 0..100 (Spark)
# ===========================
def synthesize_many_spark(
    tables: Dict[str, DataFrame],
    keys_per_table: Dict[str, Sequence[str]],
    *,
    seed: int,
    numeric_transform: str = "scale_0_100",  # "scale_0_100" | "multiply"
    multiplier: float = 1.0,
    tokenize_strings: bool = False,
    keep_int: bool = True,
    non_negative_cols: Optional[Sequence[str]] = None,
) -> Dict[str, DataFrame]:
    """
    Se numeric_transform == "scale_0_100": calcula min/max GLOBAIS por nome de coluna,
    garantindo a MESMA escala 0..100 em todas as tabelas homônimas (Spark).
    """
    global_minmax = None
    if numeric_transform == "scale_0_100":
        global_minmax = _collect_global_minmax_spark(tables, skip_cols=())

    out: Dict[str, DataFrame] = {}
    for name, sdf in tables.items():
        keys = keys_per_table.get(name, [])
        out[name] = synthesize_table_spark(
            sdf,
            keys=keys,
            seed=seed,
            numeric_transform=numeric_transform,
            multiplier=multiplier,
            global_minmax=global_minmax,
            tokenize_strings=tokenize_strings,
            keep_int=keep_int,
            non_negative_cols=non_negative_cols,
        )
    return out


# ===========================
# LLM – inferência de chaves (Spark)
# ===========================
def infer_table_keys_llm_langchain_spark(
    sdf: DataFrame, llm_api_key: str, sample_rows: int = 3000, model: str = "gpt-4o-mini"
) -> List[str]:
    """
    Gera um "profile" resumido da tabela Spark (amostra) e pergunta ao LLM as colunas-chave.
    """
    n = sdf.count()
    take_n = min(sample_rows, n) if n else 0
    if take_n == 0:
        return []

    # Amostra para pandas (apenas linhas necessárias)
    pdf = sdf.limit(take_n).toPandas()

    # Perfil por coluna
    def _profile_pandas(s):
        n = len(s)
        nulls = int(s.isna().sum())
        non_null = s.dropna()
        nunique = int(non_null.nunique())
        pct_unique = (nunique / len(non_null) * 100) if len(non_null) else 0.0
        samples = list(map(str, non_null.drop_duplicates().astype(str).head(5)))
        return {
            "dtype": str(s.dtype),
            "null_pct": round(nulls / n * 100, 2),
            "pct_unique": round(pct_unique, 2),
            "samples": samples,
        }

    profile = {"rows": int(take_n), "columns": {c: _profile_pandas(pdf[c]) for c in pdf.columns}}

    system_msg = SystemMessage(
        content=(
            "Você é especialista em modelagem de dados relacional. "
            "Dada UMA tabela e o perfil de cada coluna, decida quais colunas são chaves relevantes: "
            "primary key e possíveis foreign keys. "
            "Critérios: 0% nulos e alta unicidade sugerem PK; *_id/id_* sugerem FK. "
            'Responda SOMENTE um JSON array com os nomes das colunas. Exemplo: ["id_cliente","id_conta"]'
        )
    )
    human_msg = HumanMessage(content=json.dumps(profile, ensure_ascii=False))

    llm = ChatOpenAI(model=model, api_key=llm_api_key, temperature=0.0)
    resp = llm.invoke([system_msg, human_msg])
    text = resp.content.strip()

    def _parse_json(txt):
        try:
            return json.loads(txt)
        except Exception:
            m = re.search(r"\[.*\]", txt, flags=re.DOTALL)
            return json.loads(m.group(0)) if m else []

    result = _parse_json(text)
    if not isinstance(result, list):
        return []
    cols = set(sdf.columns)
    return [str(c) for c in result if str(c) in cols]


# --------------------------
# Tipos e checagens
# --------------------------
def _is_numeric_type(dt: T.DataType) -> bool:
    return isinstance(
        dt,
        (
            T.ByteType,
            T.ShortType,
            T.IntegerType,
            T.LongType,
            T.FloatType,
            T.DoubleType,
            T.DecimalType,
        ),
    )


def _is_boolean_type(dt: T.DataType) -> bool:
    return isinstance(dt, T.BooleanType)


def _is_datetime_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.DateType, T.TimestampType))


def _is_stringish_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.StringType,))


# --------------------------
# Expressões determinísticas
# --------------------------
def _expr_hash_key_value(seed: int, col_name: str, col: F.Column) -> F.Column:
    """
    "K_" + sha256(f"{seed}|{col_name}|{value}")[:32]
    """
    base = F.concat(F.lit(f"{seed}|{col_name}|"), F.coalesce(col.cast("string"), F.lit("")))
    h = F.sha2(base, 256)
    return F.concat(F.lit("K_"), F.substring(h, 1, 32)).cast("string")


def _expr_token_from_text(seed: int, col_name: str, col: F.Column) -> F.Column:
    """
    "S{crc32(col_name)%1000}_" + sha256(f"{seed}|{col_name}|{value}")[:12]
    """
    base = F.concat(F.lit(f"{seed}|{col_name}|"), F.coalesce(col.cast("string"), F.lit("")))
    h = F.sha2(base, 256)
    prefix_num = F.pmod(F.crc32(F.lit(col_name)), F.lit(1000))
    tok = F.concat(F.lit("S"), prefix_num.cast("string"), F.lit("_"), F.substring(h, 1, 12))
    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(tok.cast("string"))


# --------------------------
# Escala global 0..100 para numéricas
# --------------------------
_MinMax = Dict[str, Tuple[float, float]]


def _collect_global_minmax_spark(
    tables: Dict[str, DataFrame],
    skip_cols: Sequence[str] = (),
) -> _MinMax:
    """
    Calcula min/max GLOBAIS por NOME DE COLUNA atravessando todas as tabelas Spark.
    Ignora boolean, date/timestamp e colunas listadas em skip_cols.
    """
    skip = set(skip_cols or [])
    acc: _MinMax = {}
    for _, sdf in tables.items():
        for field in sdf.schema.fields:
            c = field.name
            if c in skip:
                continue
            dt = field.dataType
            if _is_boolean_type(dt) or _is_datetime_type(dt):
                continue
            if _is_numeric_type(dt):
                mm = sdf.agg(F.min(F.col(c)).alias("mn"), F.max(F.col(c)).alias("mx")).collect()[0]
                mn = mm["mn"]
                mx = mm["mx"]
                if mn is None or mx is None:
                    continue
                if c not in acc:
                    acc[c] = (float(mn), float(mx))
                else:
                    gmn, gmx = acc[c]
                    acc[c] = (min(gmn, float(mn)), max(gmx, float(mx)))
    return acc


from secrets import get_secret

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPENAI_API_KEY = get_secret("datagen-openai-key")
INPUT_BUCKET = "oci://datagen-initial-data@grqa3pd7srgw"
OUTPUT_BUCKET = "oci://datagen-synthetic-data@grqa3pd7srgw"
SEED = 42
NUMERIC_TRANSFORM = "scale_0_100"
TOKENIZE_STRINGS = True
KEEP_INT = True


def parse_arguments() -> tuple[str, str, list[str] | None]:
    """
    Parse and validate command-line arguments.

    Returns:
        tuple: (table_name, date, keys_list_or_none)

    Exits:
        With code 1 if arguments are invalid
    """
    if len(sys.argv) < 3:
        print("Usage: transform.py <table> <YYYYMMDD> [key_columns]")
        sys.exit(1)

    table = sys.argv[1]
    date = sys.argv[2]
    keys = sys.argv[3].split(",") if len(sys.argv) == 4 else None

    return table, date, keys


def build_paths(table: str, date: str) -> tuple[str, str]:
    """
    Build input and output paths for Parquet files.

    Args:
        table: Table name
        date: Date in YYYYMMDD format

    Returns:
        tuple: (input_path, output_path)
    """
    input_path = f"{INPUT_BUCKET}/{table}/{date}_{table}.parquet"
    output_path = f"{OUTPUT_BUCKET}/{table}/{date}_{table}_synthetic.parquet"
    return input_path, output_path


def infer_or_use_keys(df: DataFrame, provided_keys: list[str] | None) -> list[str]:
    """
    Determine final key columns to use for synthesis.

    Args:
        df: Spark DataFrame
        provided_keys: User-provided key columns or None

    Returns:
        list: Final list of key column names

    Exits:
        With code 1 if key inference fails or all provided keys are invalid
    """
    if provided_keys is not None:
        df_columns = set(df.columns)
        valid_keys = [k for k in provided_keys if k in df_columns]
        invalid_keys = [k for k in provided_keys if k not in df_columns]

        if invalid_keys:
            logger.warning(f"Keys {invalid_keys} not found in table, using {valid_keys}")

        if not valid_keys:
            logger.error("All provided keys are invalid")
            sys.exit(1)

        logger.info(f"Using provided keys: {valid_keys}")
        return valid_keys
    else:
        try:
            logger.info("Inferring keys using LLM...")
            inferred_keys = infer_table_keys_llm_langchain_spark(df, OPENAI_API_KEY)
            logger.info(f"Keys detected: {inferred_keys}")
            return inferred_keys
        except Exception as e:
            logger.error(f"Failed to infer keys: {e}")
            sys.exit(1)


def main():
    """Main transform workflow."""
    table, date, provided_keys = parse_arguments()

    input_path, output_path = build_paths(table, date)

    spark = SparkSession.builder.appName("DataGenTransform").getOrCreate()

    try:
        logger.info(f"Reading from: {input_path}")
        df = spark.read.parquet(input_path)
        row_count = df.count()
        logger.info(f"Read {row_count} rows")

        if row_count == 0:
            logger.warning("Input table is empty, writing empty synthetic output")
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Writing to: {output_path}")
            logger.info("Transform complete")
            return

        keys = infer_or_use_keys(df, provided_keys)

        logger.info(f"Synthesizing data with seed={SEED}, transform={NUMERIC_TRANSFORM}")
        synthetic_df = synthesize_table_spark(
            df,
            keys=keys,
            seed=SEED,
            numeric_transform=NUMERIC_TRANSFORM,
            tokenize_strings=TOKENIZE_STRINGS,
            keep_int=KEEP_INT,
        )

        logger.info(f"Writing to: {output_path}")
        synthetic_df.write.mode("overwrite").parquet(output_path)
        logger.info("Transform complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
