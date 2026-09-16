#!/usr/bin/env bash
# Host-side quick demo (no Occlum/SGX hardware required)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
if [[ -f "${BENCHMARK_ROOT}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BENCHMARK_ROOT}/env.sh"
fi
export SGX_PYSPARK_ROOT="$ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

python3 "$ROOT/scripts/gen_keys.py"
python3 -m sgx_pyspark.cli pipeline
