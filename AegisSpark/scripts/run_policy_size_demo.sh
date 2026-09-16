#!/usr/bin/env bash
# 本地 TEE 模拟：FAME CP-ABE，policy 10~100 字节，256-bit DEK 加/解密耗时
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
OUT_DIR="$ROOT/result/policy-size"
BUILD="$ROOT/abe-eac/native/build"
MCL_BUILD="${MCL_BUILD:-/home/shanlicheng/mcl/build-eac}"
REPS="${1:-40}"

mkdir -p "$OUT_DIR"
export ABE_SPARK_ROOT="$ROOT"
export ABE_SPARK_CONFIG="${ABE_SPARK_CONFIG:-$ROOT/abe-eac/conf/abe-spark.conf}"
export ABE_SPARK_CONF_DIR="${ABE_SPARK_CONF_DIR:-$ROOT/abe-eac/conf/keys}"
export MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
export LD_LIBRARY_PATH="${MCL_ROOT}/build-eac/lib:${LD_LIBRARY_PATH:-}"

echo "[1/3] rebuild MCL abe_framework (incl. FAME)..."
cmake --build "$MCL_BUILD" -j"$(nproc)" --target abe_framework

echo "[2/3] rebuild policy_size_encrypt_demo..."
cmake --build "$BUILD" -j"$(nproc)" --target policy_size_encrypt_demo

echo "[3/3] run FAME policy-size enc/dec benchmark (reps=$REPS)..."
CSV="$OUT_DIR/encrypt_vs_policy.csv"
"$BUILD/policy_size_encrypt_demo" "$CSV" "$REPS"

echo
echo "==== result ===="
column -t -s, "$CSV" 2>/dev/null || cat "$CSV"
echo
echo "CSV: $CSV"
