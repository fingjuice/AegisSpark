#!/usr/bin/env bash
# 仅提交消融 Job（假定 JAR/native 已构建并同步）
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT_HOST="$ROOT/result/cache"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
HDFS_CACHE="hdfs://10.26.40.83:9000/abe-bench/cache-data"
HDFS_EAC_FAST="hdfs://10.26.40.83:9000/abe-bench/eac-fast"
HDFS_EAC_FULL="hdfs://10.26.40.83:9000/abe-bench/eac-full"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
mkdir -p "$RESULT_HOST"

gen_yaml() {
  python3 - "$BASE_YAML" "$RESULT_HOST" "$@" <<'PY'
import re, sys
from pathlib import Path
base = Path(sys.argv[1]).read_text()
result_dir = sys.argv[2]
env = dict(x.split("=", 1) for x in sys.argv[3:])
text = base.replace(
    "/home/shanlicheng/test-benchmark/ABE-Spark/result/company",
    result_dir,
)
defaults = {
    "TARGET_GB": "10",
    "CHECKPOINT_GB": "1",
    "FINE_CHECKPOINT_GB": "0.1",
    "BENCH_MODE": "write",
    "RESUME_WRITE": "0",
    "WV_FAST_VERIFY": "0",
    "ABE_DEK_CACHE": "0",
    "ABE_SHARED_DEK": "0",
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/cache-data",
    "EAC_CSV": "eac_time.csv",
    "ABE_CSV": "abe_time.csv",
    "WRITE_CSV": "write_perf.csv",
    "READ_CSV": "read_perf.csv",
}
defaults.update(env)
for k, v in defaults.items():
    pat = rf'(- name: {k}\n\s+value: )"[^"]*"'
    if re.search(pat, text):
        text = re.sub(pat, rf'\1"{v}"', text)
    else:
        anchor = '            - name: RESUME_WRITE\n              value: "0"'
        text = text.replace(anchor, anchor + f'\n            - name: {k}\n              value: "{v}"', 1)
Path("/tmp/company-abe-cache-run.yaml").write_text(text)
print("applied", {k: defaults[k] for k in (
    "BENCH_MODE", "ABE_DEK_CACHE", "ABE_SHARED_DEK", "WV_FAST_VERIFY",
    "HDFS_DATA_ROOT", "EAC_CSV", "ABE_CSV")})
PY
}

wait_job() {
  local name="$1"
  sleep 10
  for i in $(seq 1 200); do
    s=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)
    f=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)
    if [ "$s" = "1" ]; then
      echo "[OK] $name"
      rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
      return 0
    fi
    if [ -n "$f" ] && [ "$f" != "0" ]; then
      echo "[FAIL] $name"
      ssh shanlicheng@10.26.40.84 \
        "grep -aE 'Exception in thread|UnsatisfiedLink|\\[WRITE\\]|\\[READ\\]' $RESULT_HOST/distributed_benchmark.log | tail -50" || true
      return 1
    fi
    if (( i % 4 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[WRITE\\]|\\[READ\\]|hdfsRoot|dekCache|eacFast|sharedDek' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -4" || true
    fi
    sleep 15
  done
  echo "[TIMEOUT] $name"
  return 1
}

run_job() {
  local name="$1"; shift
  echo "======== RUN $name ========"
  gen_yaml "$@"
  kubectl delete job -n spark company-abe-spark --wait=true 2>/dev/null || true
  sleep 3
  kubectl apply -f /tmp/company-abe-cache-run.yaml
  wait_job "$name"
}

run_job write_shared_dek \
  BENCH_MODE=write ABE_SHARED_DEK=1 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT="$HDFS_CACHE" \
  EAC_CSV=write_shared_eac.csv WRITE_CSV=write_shared_perf.csv

run_job read_cache_on \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=1 WV_FAST_VERIFY=0 \
  HDFS_DATA_ROOT="$HDFS_CACHE" \
  ABE_CSV=dek_cache_on.csv READ_CSV=read_cache_on_perf.csv

run_job read_cache_off \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 WV_FAST_VERIFY=0 \
  HDFS_DATA_ROOT="$HDFS_CACHE" \
  ABE_CSV=dek_cache_off.csv READ_CSV=read_cache_off_perf.csv

run_job eac_fast_on \
  BENCH_MODE=write ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT="$HDFS_EAC_FAST" \
  EAC_CSV=eac_fast_on.csv WRITE_CSV=write_eac_fast_perf.csv

run_job eac_fast_off \
  BENCH_MODE=write ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 WV_FAST_VERIFY=0 \
  HDFS_DATA_ROOT="$HDFS_EAC_FULL" \
  EAC_CSV=eac_fast_off.csv WRITE_CSV=write_eac_full_perf.csv

echo ALL_DONE
ls -la "$RESULT_HOST"
wc -l "$RESULT_HOST"/dek_cache_*.csv "$RESULT_HOST"/eac_fast_*.csv
