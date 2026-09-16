#!/usr/bin/env bash
# ABE 只读：复用已完成的 mt-eac-64（~20GB）数据
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT_HOST="$ROOT/result/multi-thread"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
HDFS_ROOT="hdfs://10.26.40.83:9000/abe-bench/mt-eac-64"
TARGET_GB="${TARGET_GB:-20}"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
mkdir -p "$RESULT_HOST"
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT_HOST; chmod 777 $RESULT_HOST"

echo "HDFS_DATA_ROOT=$HDFS_ROOT TARGET_GB=$TARGET_GB"
hdfs dfs -du -s -h /abe-bench/mt-eac-64 /abe-bench/mt-eac-128 || true

bash "$ROOT/scripts/build_company_benchmark_jar.sh"
for ip in 10.26.40.83 10.26.40.84 10.26.40.85; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" "shanlicheng@$ip:$ROOT/abe-eac/target/"
done

# 清远程/本地旧 ABE 读结果，保留 eac_write_* 部分结果
ssh shanlicheng@10.26.40.84 \
  "cd $RESULT_HOST && rm -f abe_read_*.csv read_abe_*.csv" || true
rm -f "$RESULT_HOST"/abe_read_*.csv "$RESULT_HOST"/read_abe_*.csv 2>/dev/null || true

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
    "TARGET_GB": "20",
    "CHECKPOINT_GB": "2",
    "FINE_CHECKPOINT_GB": "0.1",
    "BENCH_MODE": "read",
    "RESUME_WRITE": "0",
    "WV_FAST_VERIFY": "1",
    "ABE_DEK_CACHE": "0",
    "ABE_SHARED_DEK": "0",
    "MT_SIMPLE_CSV": "1",
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/mt-eac-64",
    "EAC_CSV": "eac_write_64.csv",
    "ABE_CSV": "abe_read_64.csv",
    "WRITE_CSV": "write_perf.csv",
    "READ_CSV": "read_perf.csv",
    "SPARK_CORES_MAX": "64",
    "SPARK_PARALLELISM": "64",
    "SPARK_EXECUTOR_CORES": "8",
    "SPARK_EXECUTOR_INSTANCES": "8",
}
defaults.update(env)
for k, v in defaults.items():
    pat = rf'(- name: {k}\n\s+value: )"[^"]*"'
    if re.search(pat, text):
        text = re.sub(pat, rf'\1"{v}"', text)
    else:
        anchor = '            - name: RESUME_WRITE\n              value: "0"'
        text = text.replace(anchor, anchor + f'\n            - name: {k}\n              value: "{v}"', 1)
Path("/tmp/company-abe-mt.yaml").write_text(text)
keep = ["BENCH_MODE","ABE_SHARED_DEK","ABE_DEK_CACHE","MT_SIMPLE_CSV",
        "SPARK_CORES_MAX","SPARK_PARALLELISM","HDFS_DATA_ROOT","EAC_CSV","ABE_CSV"]
print("applied", {k: defaults[k] for k in keep})
PY
}

wait_job() {
  local name="$1"
  sleep 12
  # 读更快：约 2h 上限（800 * 15s）
  for i in $(seq 1 800); do
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
        "grep -aE 'Exception|Error|fine |sharedDek|dekCache' $RESULT_HOST/distributed_benchmark.log | tail -50" || true
      return 1
    fi
    if (( i % 4 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[WRITE\\] fine|\\[READ\\] fine|cores.max|sharedDek=|dekCache=|BENCH_MODE' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -3" || true
    fi
    sleep 15
  done
  echo "[TIMEOUT] $name"; return 1
}

run_job() {
  local name="$1"; shift
  echo "======== RUN $name ========"
  gen_yaml "$@"
  kubectl delete job -n spark company-abe-spark --wait=true 2>/dev/null || true
  sleep 3
  kubectl apply -f /tmp/company-abe-mt.yaml
  wait_job "$name"
}

run_job abe_read_64 \
  BENCH_MODE=read TARGET_GB="$TARGET_GB" FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=64 SPARK_PARALLELISM=64 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=8 \
  HDFS_DATA_ROOT="$HDFS_ROOT" \
  ABE_CSV=abe_read_64.csv READ_CSV=read_abe_64_perf.csv

run_job abe_read_128 \
  BENCH_MODE=read TARGET_GB="$TARGET_GB" FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=128 SPARK_PARALLELISM=128 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=16 \
  HDFS_DATA_ROOT="$HDFS_ROOT" \
  ABE_CSV=abe_read_128.csv READ_CSV=read_abe_128_perf.csv

echo ALL_DONE
rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
python3 << 'PY'
from pathlib import Path
base=Path('/home/shanlicheng/test-benchmark/ABE-Spark/result/multi-thread')
for name in ['abe_read_64.csv','abe_read_128.csv']:
  p=base/name
  if not p.exists():
    print(name, 'MISSING'); continue
  lines=p.read_text().strip().splitlines()
  print(f'== {name} header={lines[0]} n={len(lines)-1}')
  print(f'   first={lines[1] if len(lines)>1 else None}')
  print(f'   last ={lines[-1] if len(lines)>1 else None}')
PY
