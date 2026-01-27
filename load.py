"""
Load synthetic Parquet data from Object Storage into Oracle QAB database.
"""

import logging
import sys

from pyspark.sql import SparkSession

from secrets import get_secret

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DSN_QAB = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=g0b5f3b4bb778b4_qabdb_medium.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"
JDBC_URL_QAB = f"jdbc:oracle:thin:@{DSN_QAB}"
USER_QAB = "ADMIN"
PASSWORD_QAB = get_secret("datagen-target-db-password")

INPUT_BUCKET = "oci://datagen-synthetic-data@grqa3pd7srgw"

BATCH_SIZE = 10000


def parse_arguments() -> tuple[str, str]:
    """
    Parse and validate command-line arguments.

    Returns:
        tuple: (table_name, date)

    Exits:
        With code 1 if arguments are invalid
    """
    if len(sys.argv) < 3:
        print("Usage: load.py <table> <YYYYMMDD>")
        sys.exit(1)

    table = sys.argv[1]
    date = sys.argv[2]

    return table, date


def build_paths(table: str, date: str) -> str:
    """
    Build input path for synthetic Parquet file.

    Args:
        table: Table name
        date: Date in YYYYMMDD format

    Returns:
        str: Input path to synthetic Parquet
    """
    input_path = f"{INPUT_BUCKET}/{table}/{date}_{table}_synthetic.parquet"
    return input_path


def main():
    """Main load workflow."""
    table, date = parse_arguments()

    input_path = build_paths(table, date)

    spark = SparkSession.builder.appName("DataGenLoad").getOrCreate()

    try:
        logger.info(f"Reading synthetic data from: {input_path}")
        df = spark.read.parquet(input_path)
        row_count = df.count()
        logger.info(f"Read {row_count} rows from synthetic Parquet")

        properties = {
            "url": JDBC_URL_QAB,
            "user": USER_QAB,
            "password": PASSWORD_QAB,
            "driver": "oracle.jdbc.OracleDriver",
        }

        logger.info(f"Writing to QAB database: ADMIN.{table}")
        df.write.format("jdbc").options(**properties).option("dbtable", f"ADMIN.{table}").option(
            "batchsize", BATCH_SIZE
        ).option("createTableOptions", "").mode("append").save()

        logger.info(f"Successfully loaded {row_count} rows to ADMIN.{table}")
        logger.info("Load complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
