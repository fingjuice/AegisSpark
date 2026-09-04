#!/usr/bin/env bash
# 离线编译 CompanyAbeBenchmark 并更新 spark-abe-eac JAR
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
SPARK_JARS="${SPARK_JARS:-$ROOT/jars}"
ABE_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar"
BUILD_DIR="$ROOT/abe-eac/target/company-bench-classes"
SRC="$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/CompanyAbeBenchmark.java"

mkdir -p "$BUILD_DIR"
CLASSPATH="$ABE_JAR:$(echo $SPARK_JARS/*.jar | tr ' ' ':')"

echo ">>> Compiling CompanyAbeBenchmark ..."
javac -cp "$CLASSPATH" \
  -sourcepath "$ROOT/abe-eac/src/main/java" \
  -d "$BUILD_DIR" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/AbeHdfsXAttr.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/AbeNativeBridge.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/WorkerWritePipeline.java" \
  "$SRC"

echo ">>> Updating $ABE_JAR ..."
TMP_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.new.jar"
cp "$ABE_JAR" "$TMP_JAR"
(
  cd "$BUILD_DIR"
  find org -name '*.class' | xargs jar uf "$TMP_JAR"
)
mv "$TMP_JAR" "$ABE_JAR"
echo ">>> Done: $ABE_JAR"
jar tf "$ABE_JAR" | grep CompanyAbeBenchmark || true
