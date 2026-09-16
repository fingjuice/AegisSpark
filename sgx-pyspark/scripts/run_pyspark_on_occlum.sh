#!/usr/bin/env bash
# 在 Occlum LibOS 内运行 PySpark 作业
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OCCLUM_DIR="${ROOT}/occlum/instance"
JOB="${1:-examples/pyspark_salary_job.py}"
SGX_MEM_SIZE="${SGX_MEM_SIZE:-8G}"

export SGX_PYSPARK_ROOT="${ROOT}"
export SGX_PYSPARK_TEE_MODE=occlum
export OCCLUM=1

if ! command -v occlum &>/dev/null; then
  echo "occlum CLI not found; running in host sim mode instead"
  export SGX_PYSPARK_TEE_MODE=sim
  bash "${ROOT}/scripts/setup_keys.sh"
  python3 "${ROOT}/${JOB}"
  exit 0
fi

bash "${ROOT}/scripts/occlum_build.sh"

cd "${OCCLUM_DIR}"
occlum run /bin/bash -c "
  export SGX_PYSPARK_ROOT=/opt/sgx-pyspark
  export SGX_PYSPARK_TEE_MODE=occlum
  export OCCLUM=1
  export PYTHONPATH=/opt/sgx-pyspark:/opt/python-libs
  export LD_LIBRARY_PATH=/opt/sgx-pyspark/native/build:/opt/mcl/lib:\${LD_LIBRARY_PATH:-}
  python3 /opt/sgx-pyspark/${JOB}
"
