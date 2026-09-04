#!/usr/bin/env bash
# Spark + HDFS 单次写/读请求真实实验（非 native demo）
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
export ABE_SPARK_CONF_DIR="${ABE_SPARK_CONF_DIR:-$ROOT/abe-eac/conf/keys}"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}"
export PATH="$HADOOP_HOME/bin:$ROOT/bin:$JAVA_HOME/bin:$PATH"
export SGX_SDK="${SGX_SDK:-/opt/intel/sgxsdk}"
export LD_LIBRARY_PATH="$ROOT/abe-eac/native/build:${MCL_ROOT}/build-eac/lib:${SGX_SDK}/lib64:/home/shanlicheng/.local/opt/openssl11/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-shanlicheng}"

ABE_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar"
JNI_DIR="$ROOT/abe-eac/native/build"
CONF="$ROOT/abe-eac/conf/abe-spark.conf"
RESULT_DIR="${RESULT_DIR:-$ROOT/result/single-request}"
HDFS_NN="${HDFS_NN:-hdfs://10.26.40.83:9000}"
HDFS_DATA_ROOT="${HDFS_DATA_ROOT:-$HDFS_NN/abe-bench/single-req}"
BENCH_MODE="${BENCH_MODE:-all}"

export ABE_SPARK_CONFIG="$CONF"
export RESULT_DIR HDFS_DATA_ROOT BENCH_MODE

mkdir -p "$RESULT_DIR"

echo "=== Build native JNI + benchmark class ==="
cmake -S "$ROOT/abe-eac/native" -B "$JNI_DIR" -DMCL_ROOT="$MCL_ROOT" -DHADOOP_HOME="$HADOOP_HOME" -DSGX_SDK="$SGX_SDK" -DSGX_STUB_DIR="${MCL_ROOT}/sgx-enclave/build/generated/App"
cmake --build "$JNI_DIR" -j"$(nproc)" --target abe_spark_jni

BUILD_DIR="$ROOT/abe-eac/target/single-req-classes"
mkdir -p "$BUILD_DIR"
SPARK_JARS="${SPARK_JARS:-$ROOT/jars}"
CLASSPATH="$ABE_JAR:$(echo "$SPARK_JARS"/*.jar | tr ' ' ':')"

javac -cp "$CLASSPATH" \
  -sourcepath "$ROOT/abe-eac/src/main/java" \
  -d "$BUILD_DIR" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/AbeNativeBridge.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/EncryptedTablePackage.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/CompanyAbeBenchmark.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/SingleRequestSparkBenchmark.java"

TMP_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.new.jar"
cp "$ABE_JAR" "$TMP_JAR"
( cd "$BUILD_DIR" && find org -name '*.class' | xargs jar uf "$TMP_JAR" )
mv "$TMP_JAR" "$ABE_JAR"
cp "$ABE_JAR" "$ROOT/jars/spark-abe-eac_2.12-3.2.3.jar"
echo "Updated JAR: $ABE_JAR and $ROOT/jars/"

echo "=== Prepare HDFS ==="
hdfs dfs -rm -r -f "$HDFS_DATA_ROOT" 2>/dev/null || true
hdfs dfs -mkdir -p "$HDFS_DATA_ROOT"
hdfs dfs -chmod 777 "$HDFS_DATA_ROOT"

echo "=== Spark submit (local[1]) ==="
"$ROOT/bin/spark-submit" \
  --master "local[1]" \
  --deploy-mode client \
  --class org.apache.spark.abe.benchmark.SingleRequestSparkBenchmark \
  --driver-memory 4g \
  --conf spark.driver.extraLibraryPath="$JNI_DIR" \
  --conf spark.driver.extraJavaOptions="-Djava.library.path=$JNI_DIR" \
  --conf spark.executor.extraLibraryPath="$JNI_DIR" \
  --conf spark.executor.extraJavaOptions="-Djava.library.path=$JNI_DIR" \
  --conf spark.ui.enabled=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.hadoop.fs.defaultFS="$HDFS_NN" \
  --conf spark.abe.enabled=true \
  --conf spark.abe.master.config="$CONF" \
  --conf spark.executorEnv.ABE_SPARK_ROOT="$ROOT" \
  --conf spark.executorEnv.ABE_SPARK_CONFIG="$CONF" \
  --conf spark.executorEnv.ABE_SPARK_CONF_DIR="$ABE_SPARK_CONF_DIR" \
      --conf spark.executorEnv.LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
      --conf spark.executorEnv.SGX_SDK="$SGX_SDK" \
  --conf spark.executorEnv.HADOOP_USER_NAME="$HADOOP_USER_NAME" \
  "$ABE_JAR" 2>&1 | tee "$RESULT_DIR/spark_single_request.log"

echo ""
echo "Results: $RESULT_DIR"
ls -la "$RESULT_DIR"/*.csv 2>/dev/null || true
