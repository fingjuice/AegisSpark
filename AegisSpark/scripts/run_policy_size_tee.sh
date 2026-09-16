#!/usr/bin/env bash
# 在 Intel SGX Enclave（默认 SIM）内跑 FAME policy-size 加/解密基准
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
SGX_DIR="${SGX_DIR:-/home/shanlicheng/mcl/sgx-enclave}"
OUT_DIR="$ROOT/result/policy-size"
REPS="${1:-40}"
SGX_MODE="${SGX_MODE:-SIM}"
SGX_DEBUG="${SGX_DEBUG:-1}"

mkdir -p "$OUT_DIR"
export SGX_SDK="${SGX_SDK:-/opt/intel/sgxsdk}"
# shellcheck disable=SC1091
source "$SGX_SDK/environment" 2>/dev/null || true

export PATH="/home/shanlicheng/.local/opt/nasm/usr/bin:${PATH}"
export LD_LIBRARY_PATH="/home/shanlicheng/.local/opt/openssl11/usr/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"

echo "[1/2] build SGX enclave + policy_size_tee_bench (SGX_MODE=$SGX_MODE)..."
make -C "$SGX_DIR" SGX_MODE="$SGX_MODE" SGX_DEBUG="$SGX_DEBUG" -j"$(nproc)"

CSV="$OUT_DIR/encrypt_vs_policy.csv"

echo "[2/2] run TEE FAME policy-size bench (reps=$REPS)..."
(
  cd "$SGX_DIR/build/bin"
  ./policy_size_tee_bench "$CSV" "$REPS"
)

echo
echo "==== result ===="
column -t -s, "$CSV" 2>/dev/null || cat "$CSV"
echo
echo "CSV: $CSV"
