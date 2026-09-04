#!/usr/bin/env bash
# 10GB Write Verification 验证：64 vs 128 并发，记录每次写验证均值 → result/cache/
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT_HOST="$ROOT/result/cache"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
mkdir -p "$RESULT_HOST"

# 更新 README 说明
cat >> "$RESULT_HOST/README.md" << 'EOF'

## EAC 并发写验证（64 / 128 线程）

- `eac_conc_64.csv`：`spark.cores.max=64`，并行度 64
- `eac_conc_128.csv`：`spark.cores.max=128`，并行度 128
- 10GB company 数据集，EAC 快速写验证（仅 verifyWriteChain）
- 每 0.1GB：`EAC-time`（累计）、`eac_verify_per_call_ms`（单次均值 = 累计/表数）
EOF

bash "$ROOT/scripts/build_company_benchmark_jar.sh"
for ip in 10.26.40.83 10.26.40.84 10.26.40.85; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" \
    "shanlicheng@$ip:$ROOT/abe-eac/target/"
done
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT_HOST; chmod 777 $RESULT_HOST"

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
    "WV_FAST_VERIFY": "1",
    "ABE_DEK_CACHE": "0",
    "ABE_SHARED_DEK": "0",
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/eac-conc-64",
    "EAC_CSV": "eac_conc_64.csv",
    "WRITE_CSV": "write_conc_64_perf.csv",
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
Path("/tmp/company-abe-cache-run.yaml").write_text(text)
print("applied", {k: defaults[k] for k in (
    "SPARK_CORES_MAX", "SPARK_PARALLELISM", "SPARK_EXECUTOR_INSTANCES",
    "HDFS_DATA_ROOT", "EAC_CSV", "WV_FAST_VERIFY")})
PY
}

wait_job() {
  local name="$1"
  sleep 12
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
        "grep -aE 'Exception in thread|per_call|parallelism|cores.max' $RESULT_HOST/distributed_benchmark.log | tail -40" || true
      return 1
    fi
    if (( i % 3 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE 'cores.max|parallelism=|per_call|checkpoint' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -4" || true
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

hdfs dfs -rm -r -f /abe-bench/eac-conc-64 /abe-bench/eac-conc-128 2>/dev/null || true
hdfs dfs -mkdir -p /abe-bench/eac-conc-64 /abe-bench/eac-conc-128
hdfs dfs -chmod 777 /abe-bench/eac-conc-64 /abe-bench/eac-conc-128

run_job eac_conc_64 \
  SPARK_CORES_MAX=64 SPARK_PARALLELISM=64 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=8 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-conc-64 \
  EAC_CSV=eac_conc_64.csv WRITE_CSV=write_conc_64_perf.csv \
  WV_FAST_VERIFY=1 BENCH_MODE=write

run_job eac_conc_128 \
  SPARK_CORES_MAX=128 SPARK_PARALLELISM=128 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=16 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-conc-128 \
  EAC_CSV=eac_conc_128.csv WRITE_CSV=write_conc_128_perf.csv \
  WV_FAST_VERIFY=1 BENCH_MODE=write

echo ALL_DONE
rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
wc -l "$RESULT_HOST"/eac_conc_*.csv
python3 << 'PY'
from pathlib import Path
base=Path('/home/shanlicheng/test-benchmark/ABE-Spark/result/cache')
for name in ['eac_conc_64.csv','eac_conc_128.csv']:
  p=base/name
  lines=p.read_text().strip().splitlines()
  print(name, 'npts', len(lines)-1)
  print(' first', lines[1])
  print(' last ', lines[-1])
  # per_call at end
  last=lines[-1].split(',')
  print('  eac_verify_per_call_ms=', last[3])
PY
