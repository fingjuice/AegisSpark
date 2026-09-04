#!/usr/bin/env bash
# 宿主环境快速演示（无需 Occlum/SGX 硬件）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
# shellcheck disable=SC1091
source "${BENCHMARK_ROOT}/env.sh"
export SGX_PYSPARK_ROOT="$ROOT"
export PYTHONPATH="$ROOT"

python3 "$ROOT/scripts/gen_keys.py"
python3 -m sgx_pyspark.cli pipeline
