import os
import sys


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing required environment variable: {name}", file=sys.stderr)
        sys.exit(1)
    return value


def main():
    if len(sys.argv) != 2:
        print("Usage: oracle_read_smoke.py <table>")
        sys.exit(1)

    from pyspark.sql import SparkSession

    table = sys.argv[1]
    jdbc_url = required_env("ORACLE_JDBC_URL")
    user = required_env("ORACLE_DB_USER")
    password = required_env("ORACLE_DB_PASSWORD")

    spark = SparkSession.builder.appName("OracleReadSmoke").getOrCreate()
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("user", user)
            .option("password", password)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("dbtable", table)
            .load()
        )

        print(f"Successfully read table: {table}")
        print(f"Columns: {df.columns}")
        print(f"Rows: {df.count()}")
        df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
