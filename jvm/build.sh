#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_JARS="$SPARK_HOME/jars"
else
  SPARK_JARS="$(python3 -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / "jars")')"
fi
if [[ ! -f "$SPARK_JARS/spark-sql_2.12-3.5.0.jar" ]]; then
  echo "Spark 3.5.0 (Scala 2.12) is required; jars not found in $SPARK_JARS" >&2
  exit 1
fi

rm -rf "$ROOT/build/classes"
mkdir -p "$ROOT/build/classes"
javac --release 8 -Xlint:-options \
  -cp "$SPARK_JARS/*" \
  -d "$ROOT/build/classes" \
  "$ROOT/src/main/java/com/eudalia/datagen/OracleAuditJdbcWriter.java"
jar cf "$ROOT/oracle-audit-writer.jar" -C "$ROOT/build/classes" .
echo "Built $ROOT/oracle-audit-writer.jar"
