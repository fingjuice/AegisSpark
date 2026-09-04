#!/usr/bin/env bash
# result/cache 修正重跑：
# - DEK：共享密文 DEK 写库 + 真实 tee_calls/cache_hits（on 与 off 应明显不同）
# - EAC：每采样含 tables、单次加密、单次写验证、累计、总时间；启动前删旧 CSV
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT_HOST="$ROOT/result/cache"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
mkdir -p "$RESULT_HOST" "$RESULT_HOST/invalid-v2"

# 归档旧 CSV
shopt -s nullglob
for f in "$RESULT_HOST"/*.csv; do
  mv -f "$f" "$RESULT_HOST/invalid-v2/" 2>/dev/null || true
done
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT_HOST/invalid-v2; mv $RESULT_HOST/*.csv $RESULT_HOST/invalid-v2/ 2>/dev/null || true"

# 构建 native + JAR，同步到三节点，滚动重启 worker 使 .so 生效
bash "$ROOT/scripts/build_company_benchmark_jar.sh"
(
  cd "$ROOT/abe-eac/native/build"
  cmake --build . -j"$(nproc)" --target abe_spark_jni 2>/dev/null \
    || cmake --build . -j"$(nproc)"
)
for ip in 10.26.40.83 10.26.40.84 10.26.40.85; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" "shanlicheng@$ip:$ROOT/abe-eac/target/"
  rsync -az "$ROOT/abe-eac/native/build/libabe_spark_jni.so" \
    "shanlicheng@$ip:$ROOT/abe-eac/native/build/"
done
kubectl delete pod -n spark -l component=worker --wait=false 2>/dev/null || true
sleep 20
kubectl wait --for=condition=ready pod -n spark -l component=worker --timeout=180s || true
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
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/cache-dek",
    "EAC_CSV": "eac_time.csv",
    "ABE_CSV": "abe_time.csv",
    "WRITE_CSV": "write_perf.csv",
    "READ_CSV": "read_perf.csv",
    "SPARK_CORES_MAX": "372",
    "SPARK_PARALLELISM": "372",
    "SPARK_EXECUTOR_CORES": "8",
    "SPARK_EXECUTOR_INSTANCES": "46",
}
defaults.update(env)
for k, v in defaults.items():
    pat = rf'(- name: {k}\n\s+value: )"[^"]*"'
    if re.search(pat, text):
        text = re.sub(pat, rf'\1"{v}"', text)
    else:
        anchor = '            - name: RESUME_WRITE\n              value: "0"'
        text = text.replace(anchor, anchor + f'\n            - name: {k}\n              value: "{v}"', 1)
Path("/tmp/company-abe-cache-fix2.yaml").write_text(text)
keep = ["BENCH_MODE","ABE_SHARED_DEK","ABE_DEK_CACHE","WV_FAST_VERIFY",
        "SPARK_CORES_MAX","HDFS_DATA_ROOT","EAC_CSV","ABE_CSV"]
print("applied", {k: defaults[k] for k in keep})
PY
}

wait_job() {
  local name="$1"
  sleep 12
  for i in $(seq 1 240); do
    s=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)
    f=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)
    if [ "$s" = "1" ]; then
      echo "[OK] $name"
      rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
      return 0
    fi
    if [ -n "$f" ] && [ "$f" != "0" ]; then
      echo "[FAIL] $name"
      ssh shanlicheng@10.26.40.84 "grep -aE 'Exception|fine |tables=' $RESULT_HOST/distributed_benchmark.log | tail -40" || true
      return 1
    fi
    if (( i % 3 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[WRITE\\] fine|\\[READ\\] fine|sharedDek|dekCache' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -3" || true
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
  kubectl apply -f /tmp/company-abe-cache-fix2.yaml
  wait_job "$name"
}

hdfs dfs -rm -r -f /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3 \
  /abe-bench/eac-conc-64 /abe-bench/eac-conc-128 2>/dev/null || true
hdfs dfs -mkdir -p /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3 \
  /abe-bench/eac-conc-64 /abe-bench/eac-conc-128
hdfs dfs -chmod 777 /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3 \
  /abe-bench/eac-conc-64 /abe-bench/eac-conc-128

# 1) DEK cache 前置：同策略共享密文 DEK（否则每个表密文不同，cache 永远 miss，on≈off）
run_job write_shared_for_dek \
  BENCH_MODE=write ABE_SHARED_DEK=1 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 \
  SPARK_CORES_MAX=372 SPARK_PARALLELISM=372 SPARK_EXECUTOR_INSTANCES=46 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  EAC_CSV=write_shared_for_dek.csv WRITE_CSV=write_shared_for_dek_perf.csv

# 2) 读：cache ON / OFF（对比 ABE-time 与 tee_calls；per_call≈单次 miss 成本）
run_job read_cache_on \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=1 \
  SPARK_CORES_MAX=372 SPARK_PARALLELISM=372 SPARK_EXECUTOR_INSTANCES=46 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  ABE_CSV=dek_cache_on.csv READ_CSV=read_cache_on_perf.csv

run_job read_cache_off \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 \
  SPARK_CORES_MAX=372 SPARK_PARALLELISM=372 SPARK_EXECUTOR_INSTANCES=46 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  ABE_CSV=dek_cache_off.csv READ_CSV=read_cache_off_perf.csv

# 3) EAC 快验 / 全验（每表独立 DEK；含单次加密/写验证）
run_job eac_fast_on \
  BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=1 \
  SPARK_CORES_MAX=372 SPARK_PARALLELISM=372 SPARK_EXECUTOR_INSTANCES=46 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-fast-v3 \
  EAC_CSV=eac_fast_on.csv WRITE_CSV=write_eac_fast_perf.csv

run_job eac_fast_off \
  BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=0 \
  SPARK_CORES_MAX=372 SPARK_PARALLELISM=372 SPARK_EXECUTOR_INSTANCES=46 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-full-v3 \
  EAC_CSV=eac_fast_off.csv WRITE_CSV=write_eac_full_perf.csv

# 4) 64 / 128 并发
run_job eac_conc_64 \
  BENCH_MODE=write WV_FAST_VERIFY=1 ABE_SHARED_DEK=0 \
  SPARK_CORES_MAX=64 SPARK_PARALLELISM=64 SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=8 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-conc-64 \
  EAC_CSV=eac_conc_64.csv WRITE_CSV=write_conc_64_perf.csv

run_job eac_conc_128 \
  BENCH_MODE=write WV_FAST_VERIFY=1 ABE_SHARED_DEK=0 \
  SPARK_CORES_MAX=128 SPARK_PARALLELISM=128 SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=16 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-conc-128 \
  EAC_CSV=eac_conc_128.csv WRITE_CSV=write_conc_128_perf.csv

echo ALL_DONE
rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
python3 << 'PY'
from pathlib import Path
base=Path('/home/shanlicheng/test-benchmark/ABE-Spark/result/cache')
for name in ['dek_cache_on.csv','dek_cache_off.csv','eac_fast_on.csv','eac_fast_off.csv',
             'eac_conc_64.csv','eac_conc_128.csv']:
  p=base/name
  if not p.exists():
    print(name, 'MISSING'); continue
  lines=p.read_text().strip().splitlines()
  print(f'== {name}')
  print('  header:', lines[0])
  print('  n=', len(lines)-1, 'last=', lines[-1])
if (base/'dek_cache_on.csv').exists() and (base/'dek_cache_off.csv').exists():
  def grab(p):
    h=p.read_text().strip().splitlines()[0].split(',')
    last=p.read_text().strip().splitlines()[-1].split(',')
    return dict(zip(h,last))
  on, off = grab(base/'dek_cache_on.csv'), grab(base/'dek_cache_off.csv')
  print('DEK compare: on tee=', on.get('tee_calls'), 'hits=', on.get('cache_hits'),
        'ABE=', on.get('ABE-time'), '| off tee=', off.get('tee_calls'),
        'ABE=', off.get('ABE-time'))
PY
