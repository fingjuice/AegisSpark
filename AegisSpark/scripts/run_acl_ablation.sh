#!/usr/bin/env bash
# ACL 表空间占用消融：与 xattr 实验相同 100GB company 数据集规模
# 多用户（默认 1000）+ 列级细粒度 ACL，每 5GB 采样
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT="$ROOT/result/acl"
MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
ABE_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar"
BUILD_DIR="$ROOT/abe-eac/target/acl-bench-classes"

mkdir -p "$RESULT" "$BUILD_DIR"
log() { echo "[$(date -Is)] $*" | tee -a "$RESULT/acl_runner.log"; }

log "=== Build AclTableSpaceBenchmark ==="
bash "$ROOT/scripts/build_company_benchmark_jar.sh"
SPARK_JARS="${SPARK_JARS:-$ROOT/jars}"
CLASSPATH="$ABE_JAR:$(echo $SPARK_JARS/*.jar | tr ' ' ':')"

javac -cp "$CLASSPATH" \
  -sourcepath "$ROOT/abe-eac/src/main/java" \
  -d "$BUILD_DIR" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/AclStorageModel.java" \
  "$ROOT/abe-eac/src/main/java/org/apache/spark/abe/benchmark/AclTableSpaceBenchmark.java"

log "=== Run ACL space simulation (100GB, checkpoint 5GB) ==="
export ABE_SPARK_ROOT="$ROOT"
export RESULT_DIR="$RESULT"
export TARGET_GB="${TARGET_GB:-100}"
export CHECKPOINT_GB="${CHECKPOINT_GB:-5}"
export ACL_NUM_USERS="${ACL_NUM_USERS:-1000}"
export ACL_ENTRY_BYTES="${ACL_ENTRY_BYTES:-248}"
export HDFS_DATA_ROOT="${HDFS_DATA_ROOT:-hdfs://10.26.40.83:9000/abe-bench/acl-100gb}"
export ACL_CSV="${ACL_CSV:-acl_size.csv}"

java -cp "$BUILD_DIR:$CLASSPATH" \
  org.apache.spark.abe.benchmark.AclTableSpaceBenchmark \
  2>&1 | tee -a "$RESULT/acl_runner.log"

log "=== Merge xattr vs ACL comparison ==="
python3 "$ROOT/scripts/merge_xattr_acl_results.py"

log "=== DONE ==="
head -1 "$RESULT/acl_size.csv"
tail -3 "$RESULT/acl_size.csv"
