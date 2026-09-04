#!/usr/bin/env bash
# BigDL-PPML 实验：宿主原生运行 ABE-Spark access-bench（无需 Docker）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
# shellcheck disable=SC1091
source "${BENCHMARK_ROOT}/env.sh"
ABE_ROOT="${ABE_SPARK_ROOT}"
BENCH="${ABE_ROOT}/access-bench"
OUT="${ROOT}/Experimental-Result-BigDL-PPML"

export ABE_EXP_TARGET_GB="${ABE_EXP_TARGET_GB:-100}"
export ABE_EXP_CHECKPOINT_GB="${ABE_EXP_CHECKPOINT_GB:-10}"
export ABE_EXP_WORKERS="${ABE_EXP_WORKERS:-32}"
export ABE_HDFS_ROOT="${ABE_HDFS_ROOT:-${HDFS_DEFAULT_FS}/abe-bench/data}"

mkdir -p "${OUT}"

# 将 access-bench 结果软链/同步到 PPML 结果目录
sync_results() {
  local src="${BENCH}/results"
  mkdir -p "${OUT}"
  for f in timing_summary.csv write_events.csv write_checkpoints.csv \
           read_events.csv read_checkpoints.csv write_manifest.csv \
           experiment_metadata.csv; do
    if [[ -f "${src}/${f}" ]]; then
      cp -f "${src}/${f}" "${OUT}/${f}"
    fi
  done
  if [[ -f "${src}/run.log" ]]; then
    tail -500 "${src}/run.log" > "${OUT}/run.log" 2>/dev/null || cp "${src}/run.log" "${OUT}/run.log"
  fi
}

print_summary() {
  if [[ -f "${OUT}/timing_summary.csv" ]]; then
    python3 - <<'PY'
import csv
from pathlib import Path
p = Path(__import__("os").environ["OUT"]) / "timing_summary.csv"
rows = {r["metric"]: float(r["value_ms"]) for r in csv.DictReader(p.open())}
total = rows.get("process_total", 1) or 1
abe = rows.get("abe_total", 0)
eac = rows.get("eac_total", 0)
hdfs = rows.get("hdfs_io_total", 0)
meta = rows.get("meta_total", 0)
gen = rows.get("write_gen_plain", 0)
print("\n========== 时间占比汇总 (BigDL-PPML / access-bench) ==========")
print(f"任务总处理时间: {total/1000:.1f} s")
print(f"ABE 合计:       {abe/1000:.1f} s ({abe/total*100:.2f}%)")
print(f"EAC 合计:       {eac/1000:.1f} s ({eac/total*100:.2f}%)")
print(f"HDFS IO 合计:   {hdfs/1000:.1f} s ({hdfs/total*100:.2f}%)")
print(f"元数据 IO:      {meta/1000:.1f} s ({meta/total*100:.2f}%)")
print(f"明文生成:       {gen/1000:.1f} s ({gen/total*100:.2f}%)")
print(f"结果文件: {p.resolve()}")
PY
  fi
}

export OUT

case "${1:-start}" in
  start)
    echo "[BigDL-PPML] 宿主原生实验 target=${ABE_EXP_TARGET_GB}GB checkpoint=${ABE_EXP_CHECKPOINT_GB}GB"
    echo "[BigDL-PPML] 引擎: ABE-Spark access-bench @ ${BENCH}"
    echo "[BigDL-PPML] 结果: ${OUT}"
    # 修改 access-bench 结果输出后同步
    (
      export ABE_EXP_TARGET_GB ABE_EXP_CHECKPOINT_GB ABE_EXP_WORKERS ABE_HDFS_ROOT
      cd "${BENCH}"
      if [[ -f results/run.pid ]] && kill -0 "$(cat results/run.pid)" 2>/dev/null; then
        echo "access-bench 已在运行 pid=$(cat results/run.pid)" >&2
        exit 1
      fi
      ./run.sh start
      # 等待完成并同步
      while [[ -f results/run.pid ]] && kill -0 "$(cat results/run.pid)" 2>/dev/null; do
        sleep 60
        sync_results
      done
      sync_results
      print_summary
    ) &
    echo $! > "${OUT}/sync.pid"
    echo "已启动后台同步 watcher pid=$(cat ${OUT}/sync.pid)"
    echo "进度: bash bigdl-ppml/run_native_experiment.sh progress"
    ;;
  progress)
    sync_results
    echo "=== ${OUT} ==="
    for f in write_checkpoints.csv read_checkpoints.csv timing_summary.csv; do
      if [[ -f "${OUT}/${f}" ]]; then
        echo "--- ${f} (last 2) ---"
        tail -2 "${OUT}/${f}"
      fi
    done
    if [[ -f "${BENCH}/results/run.pid" ]] && kill -0 "$(cat "${BENCH}/results/run.pid")" 2>/dev/null; then
      echo "access-bench 运行中 pid=$(cat ${BENCH}/results/run.pid)"
    fi
    ;;
  sync)
    sync_results
    print_summary
    ;;
  stop)
    cd "${BENCH}" && ./run.sh stop || true
    kill "$(cat ${OUT}/sync.pid 2>/dev/null)" 2>/dev/null || true
    rm -f "${OUT}/sync.pid"
    ;;
  summary)
    sync_results
    print_summary
    ;;
  *)
    echo "用法: $0 {start|progress|sync|summary|stop}"
    exit 1
    ;;
esac
