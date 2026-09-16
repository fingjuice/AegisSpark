#!/usr/bin/env bash
# 编译 NativeSparkCompanyBenchmark 并打入 spark-abe-eac JAR
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
SPARK_JARS="${SPARK_JARS:-$ROOT/jars}"
ABE_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar"
BUILD_DIR="$ROOT/abe-eac/target/native-bench-classes"
SRC="$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/NativeSparkCompanyBenchmark.java"

mkdir -p "$BUILD_DIR"
CLASSPATH="$ABE_JAR:$(echo $SPARK_JARS/*.jar | tr ' ' ':')"

echo ">>> Compiling NativeSparkCompanyBenchmark ..."
javac -cp "$CLASSPATH" -d "$BUILD_DIR" "$SRC"

echo ">>> Updating $ABE_JAR ..."
TMP_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.new.jar"
cp "$ABE_JAR" "$TMP_JAR"
(
  cd "$BUILD_DIR"
  find org -name '*.class' | xargs jar uf "$TMP_JAR"
)
mv "$TMP_JAR" "$ABE_JAR"
echo ">>> Done"
jar tf "$ABE_JAR" | grep NativeSparkCompanyBenchmark || true
