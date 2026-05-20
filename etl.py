from __future__ import annotations

import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SOURCE_DB_USER = "ADMIN"
DEFAULT_TARGET_DB_USER = "ADMIN"
BATCH_SIZE = 10000
SEED = 42
NUMERIC_TRANSFORM = "scale_0_100"
TOKENIZE_STRINGS = True
KEEP_INT = True

REQUIRED_ENV_VARS = (
    "DATAGEN_SOURCE_JDBC_URL",
    "DATAGEN_SOURCE_DB_PASSWORD",
    "DATAGEN_TARGET_JDBC_URL",
    "DATAGEN_TARGET_DB_PASSWORD",
    "DATAGEN_RAW_BASE_URI",
    "DATAGEN_SYNTHETIC_BASE_URI",
)

_MinMax = Dict[str, Tuple[float, float]]


def load_spark_modules():
    global F, SparkSession, T

    from pyspark.sql import SparkSession as _SparkSession
    from pyspark.sql import functions as _F
    from pyspark.sql import types as _T

    SparkSession = _SparkSession
    F = _F
    T = _T


def parse_arguments() -> tuple[str, str, list[str] | None]:
    if len(sys.argv) < 3:
        print("Usage: etl.py <table> <YYYYMMDD> [key_columns]")
        sys.exit(1)

    table = sys.argv[1]
    date = sys.argv[2]
    keys = (
        [key.strip() for key in sys.argv[3].split(",") if key.strip()]
        if len(sys.argv) >= 4
        else None
    )

    return table, date, keys


def get_required_env() -> dict[str, str]:
    config = {}
    missing = []

    for name in REQUIRED_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            missing.append(name)
        else:
            config[name] = value.rstrip("/")

    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        sys.exit(1)

    config["DATAGEN_SOURCE_DB_USER"] = os.environ.get(
        "DATAGEN_SOURCE_DB_USER", DEFAULT_SOURCE_DB_USER
    )
    config["DATAGEN_TARGET_DB_USER"] = os.environ.get(
        "DATAGEN_TARGET_DB_USER", DEFAULT_TARGET_DB_USER
    )

    return config


def build_paths(config: dict[str, str], table: str, date: str) -> tuple[str, str]:
    raw_path = f"{config['DATAGEN_RAW_BASE_URI']}/{table}/{date}_{table}.parquet"
    synthetic_path = (
        f"{config['DATAGEN_SYNTHETIC_BASE_URI']}/{table}/{date}_{table}_synthetic.parquet"
    )
    return raw_path, synthetic_path


def extract_table(
    spark: SparkSession, config: dict[str, str], table: str, output_path: str
) -> None:
    source_user = config["DATAGEN_SOURCE_DB_USER"]
    properties = {
        "url": config["DATAGEN_SOURCE_JDBC_URL"],
        "user": source_user,
        "password": config["DATAGEN_SOURCE_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
    }

    logger.info("Starting extract for %s.%s", source_user, table)
    df = (
        spark.read.format("jdbc")
        .options(**properties)
        .option("dbtable", f"{source_user}.{table}")
        .load()
    )
    row_count = df.count()
    logger.info("Extract read %s rows from %s.%s", row_count, source_user, table)

    logger.info("Writing raw Parquet to %s", output_path)
    df.write.mode("overwrite").parquet(output_path)


def infer_or_use_keys(df: DataFrame, provided_keys: list[str] | None) -> list[str]:
    if provided_keys is not None:
        df_columns = set(df.columns)
        valid_keys = [key for key in provided_keys if key in df_columns]
        invalid_keys = [key for key in provided_keys if key not in df_columns]

        if invalid_keys:
            logger.warning("Keys %s not found in table, using %s", invalid_keys, valid_keys)

        if not valid_keys:
            logger.error("All provided keys are invalid")
            sys.exit(1)

        logger.info("Using provided keys: %s", valid_keys)
        return valid_keys

    openai_api_key = os.environ.get("DATAGEN_OPENAI_KEY")
    if not openai_api_key:
        logger.error("DATAGEN_OPENAI_KEY is required when key_columns are omitted")
        sys.exit(1)

    try:
        logger.info("Inferring keys using LLM")
        inferred_keys = infer_table_keys_llm_langchain_spark(df, openai_api_key)
        logger.info("Keys detected: %s", inferred_keys)
        return inferred_keys
    except Exception as exc:
        logger.error("Failed to infer keys: %s", exc)
        sys.exit(1)


def transform_table(
    spark: SparkSession,
    raw_path: str,
    synthetic_path: str,
    provided_keys: list[str] | None,
) -> None:
    logger.info("Starting transform from %s", raw_path)
    df = spark.read.parquet(raw_path)
    row_count = df.count()
    logger.info("Transform read %s rows from raw Parquet", row_count)

    if row_count == 0:
        logger.warning("Input table is empty, writing empty synthetic output")
        logger.info("Writing synthetic Parquet to %s", synthetic_path)
        df.write.mode("overwrite").parquet(synthetic_path)
        return

    keys = infer_or_use_keys(df, provided_keys)
    logger.info("Synthesizing data with seed=%s, transform=%s", SEED, NUMERIC_TRANSFORM)
    synthetic_df = synthesize_table_spark(
        df,
        keys=keys,
        seed=SEED,
        numeric_transform=NUMERIC_TRANSFORM,
        tokenize_strings=TOKENIZE_STRINGS,
        keep_int=KEEP_INT,
    )

    logger.info("Writing synthetic Parquet to %s", synthetic_path)
    synthetic_df.write.mode("overwrite").parquet(synthetic_path)


def load_table(spark: SparkSession, config: dict[str, str], table: str, synthetic_path: str) -> None:
    logger.info("Starting load from %s", synthetic_path)
    df = spark.read.parquet(synthetic_path)
    row_count = df.count()
    logger.info("Load read %s rows from synthetic Parquet", row_count)

    target_user = config["DATAGEN_TARGET_DB_USER"]
    properties = {
        "url": config["DATAGEN_TARGET_JDBC_URL"],
        "user": target_user,
        "password": config["DATAGEN_TARGET_DB_PASSWORD"],
        "driver": "oracle.jdbc.OracleDriver",
    }

    logger.info("Starting load into %s.%s", target_user, table)
    df.write.format("jdbc").options(**properties).option(
        "dbtable", f"{target_user}.{table}"
    ).option("batchsize", BATCH_SIZE).option("createTableOptions", "").mode(
        "append"
    ).save()
    logger.info("Successfully loaded %s rows to %s.%s", row_count, target_user, table)


def synthesize_table_spark(
    sdf: DataFrame,
    keys: Sequence[str],
    *,
    seed: int,
    numeric_transform: str = "scale_0_100",
    multiplier: float = 1.0,
    global_minmax: Optional[_MinMax] = None,
    tokenize_strings: bool = True,
    keep_int: bool = True,
    non_negative_cols: Optional[Sequence[str]] = None,
) -> DataFrame:
    non_negative_cols = set(non_negative_cols or [])
    keys = list(keys or [])
    df = sdf

    for key in keys:
        if key in df.columns:
            df = df.withColumn(key, _expr_hash_key_value(seed, key, F.col(key)))

    for field in df.schema.fields:
        column_name = field.name
        if column_name in keys:
            continue

        data_type = field.dataType
        if _is_boolean_type(data_type) or _is_datetime_type(data_type):
            continue

        if _is_numeric_type(data_type):
            column = F.col(column_name).cast("double")

            if numeric_transform == "scale_0_100":
                if global_minmax and column_name in global_minmax:
                    minimum, maximum = global_minmax[column_name]
                else:
                    min_max = df.agg(
                        F.min(F.col(column_name)).alias("mn"),
                        F.max(F.col(column_name)).alias("mx"),
                    ).collect()[0]
                    minimum = float(min_max["mn"])
                    maximum = float(min_max["mx"])

                denominator = maximum - minimum
                if denominator == 0.0:
                    expr = F.when(column.isNotNull(), F.lit(0.0)).otherwise(
                        F.lit(None).cast("double")
                    )
                else:
                    expr = (column - F.lit(minimum)) / F.lit(denominator) * F.lit(100.0)

            elif numeric_transform == "multiply":
                expr = column * F.lit(float(multiplier))
            else:
                raise ValueError("numeric_transform must be 'scale_0_100' or 'multiply'.")

            if column_name in non_negative_cols:
                expr = F.greatest(expr, F.lit(0.0))

            if keep_int and isinstance(
                data_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
            ):
                if isinstance(data_type, T.ByteType):
                    expr = F.round(expr).cast("byte")
                elif isinstance(data_type, T.ShortType):
                    expr = F.round(expr).cast("short")
                elif isinstance(data_type, T.IntegerType):
                    expr = F.round(expr).cast("int")
                else:
                    expr = F.round(expr).cast("long")
            else:
                expr = expr.cast("double")

            df = df.withColumn(column_name, expr)

    if tokenize_strings:
        for field in df.schema.fields:
            column_name = field.name
            if column_name in keys:
                continue

            data_type = field.dataType
            if _is_stringish_type(data_type) and not _is_datetime_type(data_type):
                df = df.withColumn(
                    column_name,
                    _expr_token_from_text(seed, column_name, F.col(column_name)),
                )

    return df


def infer_table_keys_llm_langchain_spark(
    sdf: DataFrame, llm_api_key: str, sample_rows: int = 3000, model: str = "gpt-4o-mini"
) -> List[str]:
    from langchain_core.messages import HumanMessage, SystemMessage
    from langchain_openai import ChatOpenAI

    row_count = sdf.count()
    sample_count = min(sample_rows, row_count) if row_count else 0
    if sample_count == 0:
        return []

    pdf = sdf.limit(sample_count).toPandas()

    def profile_pandas(series):
        length = len(series)
        nulls = int(series.isna().sum())
        non_null = series.dropna()
        unique_count = int(non_null.nunique())
        unique_percent = (unique_count / len(non_null) * 100) if len(non_null) else 0.0
        samples = list(map(str, non_null.drop_duplicates().astype(str).head(5)))
        return {
            "dtype": str(series.dtype),
            "null_pct": round(nulls / length * 100, 2),
            "pct_unique": round(unique_percent, 2),
            "samples": samples,
        }

    profile = {
        "rows": int(sample_count),
        "columns": {column: profile_pandas(pdf[column]) for column in pdf.columns},
    }

    system_msg = SystemMessage(
        content=(
            "Voce e especialista em modelagem de dados relacional. "
            "Dada UMA tabela e o perfil de cada coluna, decida quais colunas sao chaves "
            "relevantes: primary key e possiveis foreign keys. "
            "Criterios: 0% nulos e alta unicidade sugerem PK; *_id/id_* sugerem FK. "
            'Responda SOMENTE um JSON array com os nomes das colunas. Exemplo: ["id_cliente","id_conta"]'
        )
    )
    human_msg = HumanMessage(content=json.dumps(profile, ensure_ascii=False))

    llm = ChatOpenAI(model=model, api_key=llm_api_key, temperature=0.0)
    response = llm.invoke([system_msg, human_msg])
    text = response.content.strip()

    def parse_json(value):
        try:
            return json.loads(value)
        except Exception:
            match = re.search(r"\[.*\]", value, flags=re.DOTALL)
            return json.loads(match.group(0)) if match else []

    result = parse_json(text)
    if not isinstance(result, list):
        return []

    columns = set(sdf.columns)
    return [str(column) for column in result if str(column) in columns]


def _is_numeric_type(data_type: T.DataType) -> bool:
    return isinstance(
        data_type,
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


def _is_boolean_type(data_type: T.DataType) -> bool:
    return isinstance(data_type, T.BooleanType)


def _is_datetime_type(data_type: T.DataType) -> bool:
    return isinstance(data_type, (T.DateType, T.TimestampType))


def _is_stringish_type(data_type: T.DataType) -> bool:
    return isinstance(data_type, T.StringType)


def _expr_hash_key_value(seed: int, column_name: str, column: F.Column) -> F.Column:
    base = F.concat(F.lit(f"{seed}|{column_name}|"), F.coalesce(column.cast("string"), F.lit("")))
    hashed = F.sha2(base, 256)
    return F.concat(F.lit("K_"), F.substring(hashed, 1, 32)).cast("string")


def _expr_token_from_text(seed: int, column_name: str, column: F.Column) -> F.Column:
    base = F.concat(F.lit(f"{seed}|{column_name}|"), F.coalesce(column.cast("string"), F.lit("")))
    hashed = F.sha2(base, 256)
    prefix_num = F.pmod(F.crc32(F.lit(column_name)), F.lit(1000))
    token = F.concat(F.lit("S"), prefix_num.cast("string"), F.lit("_"), F.substring(hashed, 1, 12))
    return F.when(column.isNull(), F.lit(None).cast("string")).otherwise(token.cast("string"))


def main():
    table, date, provided_keys = parse_arguments()
    config = get_required_env()
    raw_path, synthetic_path = build_paths(config, table, date)

    load_spark_modules()
    spark = SparkSession.builder.appName("DataGenETL").getOrCreate()
    try:
        extract_table(spark, config, table, raw_path)
        transform_table(spark, raw_path, synthetic_path, provided_keys)
        load_table(spark, config, table, synthetic_path)
        logger.info("ETL complete")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
