#!/usr/bin/env bash
#
# ABE/Write-Verification TEE Company Benchmark - 100GB write/read performance test
# Results saved to result/company/*.csv (every 10GB checkpoint)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export ABE_SPARK_ROOT="$PROJECT_ROOT"
export MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
export ABE_SPARK_CONF_DIR="${ABE_SPARK_CONF_DIR:-$PROJECT_ROOT/abe-eac/conf/keys}"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}"

TARGET_GB="${TARGET_GB:-100}"
CHECKPOINT_GB="${CHECKPOINT_GB:-10}"
BENCH_MODE="${BENCH_MODE:-all}"
PARALLEL_WORKERS="${PARALLEL_WORKERS:-8}"
HDFS_USER="${HDFS_USER:-shanlicheng}"
RESULT_DIR="${RESULT_DIR:-$PROJECT_ROOT/result/company}"
BENCH_BIN="${BENCH_BIN:-$PROJECT_ROOT/access-bench/build/abe_access_bench}"

export PATH="$HADOOP_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$MCL_ROOT/build-eac/lib:$HADOOP_HOME/lib/native:${LD_LIBRARY_PATH:-}"
export CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath --glob 2>/dev/null || $HADOOP_HOME/bin/hadoop classpath)"

mkdir -p "$RESULT_DIR"

echo "=============================================="
echo " ABE/Write-Verification TEE Company Benchmark"
echo " Target: ${TARGET_GB}GB | Checkpoint: every ${CHECKPOINT_GB}GB"
echo " Mode: $BENCH_MODE | Workers: $PARALLEL_WORKERS"
echo " Results: $RESULT_DIR"
echo " HDFS: hdfs://10.26.40.83:9000/abe-bench/data"
echo "=============================================="

# ---- Build native + benchmark if missing ----
if [ ! -f "$BENCH_BIN" ]; then
  echo "[BUILD] Compiling abe_spark native..."
  cmake -S "$PROJECT_ROOT/abe-eac/native" -B "$PROJECT_ROOT/abe-eac/native/build" \
    -DMCL_ROOT="$MCL_ROOT" -DHADOOP_HOME="$HADOOP_HOME"
  cmake --build "$PROJECT_ROOT/abe-eac/native/build" -j"$(nproc)"
  echo "[BUILD] Benchmark ready: $BENCH_BIN"
fi

export TARGET_GB CHECKPOINT_GB BENCH_MODE RESULT_DIR HDFS_USER PARALLEL_WORKERS

# Patch experiment settings via env override in config defaults
export ABE_SPARK_CONFIG="$PROJECT_ROOT/abe-eac/conf/abe-spark.conf"

LOG_SUFFIX="${BENCH_MODE}"
"$BENCH_BIN" 2>&1 | tee "$RESULT_DIR/${LOG_SUFFIX}_benchmark.log"

echo ""
echo "=============================================="
echo " Benchmark Complete"
echo " Results in: $RESULT_DIR"
echo "  - write_perf.csv   (EAC verify + ABE encrypt + HDFS write, every ${CHECKPOINT_GB}GB)"
echo "  - read_perf.csv    (ABE lookup + decrypt + HDFS read, every ${CHECKPOINT_GB}GB)"
echo "  - *_benchmark.log"
echo "=============================================="
ls -la "$RESULT_DIR/" 2>/dev/null || true
