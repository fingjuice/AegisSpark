#!/usr/bin/env bash
# Launch 100GB ABE/Write-Verification benchmark in background
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
LOG="$ROOT/result/company/full_benchmark.log"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT="/home/shanlicheng/mcl"
export HADOOP_HOME="/home/shanlicheng/opt/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export ABE_SPARK_CONF_DIR="$ROOT/abe-eac/conf/keys"
export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
export LD_LIBRARY_PATH="$MCL_ROOT/build-eac/lib:$HADOOP_HOME/lib/native"
export CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"
export TARGET_GB=100
export CHECKPOINT_GB=10
export BENCH_MODE=all
export PARALLEL_WORKERS=1
export HDFS_USER=shanlicheng
export RESULT_DIR="$ROOT/result/company"

nohup "$ROOT/access-bench/build/abe_access_bench" > "$LOG" 2>&1 &
echo $! > "$ROOT/result/company/benchmark.pid"
echo "Started PID $(cat $ROOT/result/company/benchmark.pid), log: $LOG"
