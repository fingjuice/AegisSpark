#!/usr/bin/env bash
# FAME + SGX TEE 集成冒烟：重建 Enclave / native，验证 encryptDek / decryptDek
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
SGX_DIR="${SGX_DIR:-$MCL_ROOT/sgx-enclave}"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT
export SGX_SDK="${SGX_SDK:-/opt/intel/sgxsdk}"
OPENSSL11="/home/shanlicheng/.local/opt/openssl11/usr/lib/x86_64-linux-gnu"

export LD_LIBRARY_PATH="$ROOT/abe-eac/native/build:${MCL_ROOT}/build-eac/lib:${SGX_SDK}/lib64:${OPENSSL11}:${LD_LIBRARY_PATH:-}"

if [[ -f "$SGX_SDK/environment" ]]; then
  # shellcheck disable=SC1091
  source "$SGX_SDK/environment" || true
fi

echo "=== [1/4] rebuild MCL abe_framework (FAME default) ==="
if [[ -f "${MCL_ROOT}/build-eac/Makefile" ]]; then
  cmake --build "${MCL_ROOT}/build-eac" -j"$(nproc)"
else
  cmake -S "$MCL_ROOT" -B "${MCL_ROOT}/build-eac" -DMCL_USE_GMP=ON
  cmake --build "${MCL_ROOT}/build-eac" -j"$(nproc)"
fi

echo "=== [2/4] rebuild SGX enclave (Spark ABE ECALLs) ==="
make -C "$SGX_DIR" SGX_MODE=SIM SGX_DEBUG=1 -j"$(nproc)"

echo "=== [3/4] rebuild abe_spark native (SGX bridge) ==="
cmake -S "$ROOT/abe-eac/native" -B "$ROOT/abe-eac/native/build" \
  -DMCL_ROOT="$MCL_ROOT" \
  -DSGX_SDK="$SGX_SDK" \
  -DSGX_STUB_DIR="$SGX_DIR/build/generated/App"
cmake --build "$ROOT/abe-eac/native/build" -j"$(nproc)"

echo "=== [4/4] run native tests (SGX TEE) ==="
"$ROOT/abe-eac/native/build/abe_spark_types_test"
"$ROOT/abe-eac/native/build/abe_spark_pipeline_test"

echo
echo "=== SGX policy-size TEE bench (quick) ==="
"$SGX_DIR/build/bin/policy_size_tee_bench" /tmp/fame_tee_smoke.csv 3

echo
echo "=== Config ==="
grep -E '^engine|^scheme_id|^mode' "$ROOT/abe-eac/conf/abe-spark.conf" || true

echo
echo "ALL_OK: FAME default + SGX TEE integrated"
