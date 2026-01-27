"""
Extract data from Oracle source database to Object Storage as Parquet.
"""

import logging
import sys

from pyspark.sql import SparkSession

from secrets import get_secret

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DSN = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=g0b5f3b4bb778b4_nomedb_medium.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"
JDBC_URL = f"jdbc:oracle:thin:@{DSN}"
USER = "ADMIN"
PASSWORD = get_secret("datagen-source-db-password")

OUTPUT_BUCKET = "oci://datagen-initial-data@grqa3pd7srgw"


def parse_arguments() -> tuple[str, str]:
    """
    Parse and validate command-line arguments.

    Returns:
        tuple: (table_name, date)

    Exits:
        With code 1 if arguments are invalid
    """
    if len(sys.argv) < 3:
        print("Usage: extract.py <table> <YYYYMMDD>")
        sys.exit(1)

    table = sys.argv[1]
    date = sys.argv[2]

    return table, date


def build_paths(table: str, date: str) -> str:
    """
    Build output path for extracted Parquet file.

    Args:
        table: Table name
        date: Date in YYYYMMDD format

    Returns:
        str: Output path for Parquet file
    """
    output_path = f"{OUTPUT_BUCKET}/{table}/{date}_{table}.parquet"
    return output_path


def main():
    """Main extract workflow."""
    table, date = parse_arguments()

    output_path = build_paths(table, date)

    spark = SparkSession.builder.appName("DataGenExtract").getOrCreate()

    try:
        properties = {
            "url": JDBC_URL,
            "user": USER,
            "password": PASSWORD,
            "driver": "oracle.jdbc.OracleDriver",
        }

        logger.info(f"Reading data from source database: ADMIN.{table}")
        df = (
            spark.read.format("jdbc")
            .options(**properties)
            .option("dbtable", f"ADMIN.{table}")
            .load()
        )

        row_count = df.count()
        logger.info(f"Read {row_count} rows from source table")

        logger.info(f"Writing data to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"Successfully extracted {row_count} rows to {output_path}")
        logger.info("Extract complete")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
