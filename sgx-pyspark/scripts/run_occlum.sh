#!/usr/bin/env bash
# Occlum TEE 内运行 sgx-pyspark（未安装 Occlum 时回退宿主 sim 模式）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
MODE="${1:-pipeline}"
# shellcheck disable=SC1091
source "${BENCHMARK_ROOT}/env.sh" 2>/dev/null || true
export SGX_PYSPARK_ROOT="$ROOT"
export PYTHONPATH="$ROOT"

run_host_fallback() {
  case "${MODE}" in
    pyspark|pyspark-demo)
      bash "${ROOT}/scripts/setup_keys.sh"
      python3 -m sgx_pyspark.cli pyspark-demo
      ;;
    pyspark-compute)
      bash "${ROOT}/scripts/setup_keys.sh"
      python3 -m sgx_pyspark.cli pyspark-compute-demo
      ;;
    *)
      exec bash "${ROOT}/scripts/run_demo.sh"
      ;;
  esac
}

if ! command -v occlum &>/dev/null; then
  echo "[occlum] not installed, falling back to host sim mode"
  run_host_fallback
  exit 0
fi

IMAGE_DIR="${ROOT}/occlum/instance"
mkdir -p "${IMAGE_DIR}/opt/sgx-pyspark" "${IMAGE_DIR}/opt/mcl/lib"

rsync -a --delete \
  --exclude='.venv' --exclude='Experimental-Result' --exclude='.pytest_cache' \
  "${ROOT}/" "${IMAGE_DIR}/opt/sgx-pyspark/"

cp "${MCL_ROOT:-/dev/null}/build-eac/lib/"lib*.so* "${IMAGE_DIR}/opt/mcl/lib/" 2>/dev/null || true
cp "${ROOT}/native/build/libsgx_pyspark_ffi.so" "${IMAGE_DIR}/opt/sgx-pyspark/native/build/" 2>/dev/null || true

occlum build --image-dir "${IMAGE_DIR}"

case "${MODE}" in
  pyspark|pyspark-demo)
    CMD="python3 -m sgx_pyspark.cli pyspark-demo"
    ;;
  pyspark-compute)
    CMD="python3 -m sgx_pyspark.cli pyspark-compute-demo"
    ;;
  *)
    CMD="python3 -m sgx_pyspark.cli pipeline"
    ;;
esac

occlum run --image-dir "${IMAGE_DIR}" /bin/bash -c "
  export SGX_PYSPARK_ROOT=/opt/sgx-pyspark
  export SGX_PYSPARK_TEE_MODE=occlum
  export OCCLUM=1
  export ABE_SPARK_ROOT=${ABE_SPARK_ROOT:-/opt/abe-spark}
  export MCL_ROOT=/opt/mcl
  export LD_LIBRARY_PATH=/opt/sgx-pyspark/native/build:/opt/mcl/lib
  export PYTHONPATH=/opt/sgx-pyspark
  cd /opt/sgx-pyspark && ${CMD}
"
