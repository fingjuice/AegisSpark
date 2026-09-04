#!/usr/bin/env bash
# 100GB ABE/Write-Verification 访问控制实验（nohup 后台运行，结果保存至 Experimental-Result/）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
# shellcheck disable=SC1091
source "${BENCHMARK_ROOT}/env.sh"
export SGX_PYSPARK_ROOT="${ROOT}"
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"

RESULT_DIR="${ROOT}/Experimental-Result"
LOG_FILE="${RESULT_DIR}/run.log"
PID_FILE="${RESULT_DIR}/run.pid"

# 可通过环境变量覆盖
export SGX_EXP_TARGET_GB="${SGX_EXP_TARGET_GB:-100}"
export SGX_EXP_CHECKPOINT_GB="${SGX_EXP_CHECKPOINT_GB:-10}"
export SGX_EXP_WORKERS="${SGX_EXP_WORKERS:-32}"

FOREGROUND=0
MODE="all"
for arg in "$@"; do
  case "${arg}" in
    --foreground|-f) FOREGROUND=1 ;;
    write|read|all) MODE="${arg}" ;;
    *)
      echo "未知参数: ${arg}" >&2
      echo "用法: $0 [write|read|all] [--foreground|-f]" >&2
      exit 1
      ;;
  esac
done

PY="${ROOT}/.venv/bin/python3"
if [[ ! -x "${PY}" ]]; then
  python3 -m venv "${ROOT}/.venv"
  "${ROOT}/.venv/bin/pip" install -q -r "${ROOT}/requirements.txt"
  PY="${ROOT}/.venv/bin/python3"
fi

mkdir -p "${RESULT_DIR}"

# 清理旧 CSV 结果；100GB 密文数据在后台删除，避免阻塞实验启动
BENCH_STORE="${ROOT}/data/bench_store"
if [[ "${MODE}" == "all" && -d "${BENCH_STORE}" ]]; then
  echo "后台清理旧数据集 ${BENCH_STORE} ..."
  nohup rm -rf "${BENCH_STORE}" >>"${LOG_FILE}" 2>&1 &
fi

bash "${ROOT}/scripts/setup_keys.sh" >>"${LOG_FILE}" 2>&1 || true

if [[ -f "${PID_FILE}" ]]; then
  OLD_PID="$(cat "${PID_FILE}")"
  if kill -0 "${OLD_PID}" 2>/dev/null; then
    echo "停止旧实验 PID=${OLD_PID}..."
    kill "${OLD_PID}" 2>/dev/null || true
    sleep 2
    kill -9 "${OLD_PID}" 2>/dev/null || true
  fi
  rm -f "${PID_FILE}"
fi

echo "" >>"${LOG_FILE}"
echo "========== $(date -Iseconds) 新实验启动 mode=${MODE} ==========" >>"${LOG_FILE}"

_run() {
  echo "=== sgx-pyspark 实验: mode=${MODE}, target=${SGX_EXP_TARGET_GB}GB, checkpoint=${SGX_EXP_CHECKPOINT_GB}GB ==="
  exec "${PY}" -u -m sgx_pyspark.benchmark.run_experiment "${MODE}"
}

if [[ "${FOREGROUND}" -eq 1 ]]; then
  echo "前台运行 (mode=${MODE})..."
  _run
else
  echo "nohup 后台启动实验 (mode=${MODE})..."
  echo "  日志: ${LOG_FILE}"
  echo "  PID:  ${PID_FILE}"
  nohup bash -c "
    cd '${ROOT}'
    export SGX_PYSPARK_ROOT='${ROOT}'
    export PYTHONPATH='${ROOT}'
    export SGX_EXP_TARGET_GB='${SGX_EXP_TARGET_GB}'
    export SGX_EXP_CHECKPOINT_GB='${SGX_EXP_CHECKPOINT_GB}'
    export SGX_EXP_WORKERS='${SGX_EXP_WORKERS}'
    ${PY} -u -m sgx_pyspark.benchmark.run_experiment '${MODE}'
  " >>"${LOG_FILE}" 2>&1 &
  echo $! >"${PID_FILE}"
  echo "已启动 PID=$(cat "${PID_FILE}")"
  echo "查看进度: python3 scripts/check_experiment_progress.py"
  echo "查看日志:   tail -f ${LOG_FILE}"
fi
