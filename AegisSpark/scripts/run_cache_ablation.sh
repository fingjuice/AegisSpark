#!/usr/bin/env bash
# result/cache 消融实验（K8s + HDFS，TEE 模拟，FAME，10GB，每 1GB 采样）
#
# 每 1GB 记录单次读/写请求平均耗时：
#   读：abe_decrypt / hdfs_read / request_total
#   写：abe_encrypt / eac_verify / hdfs_write / request_total
#
# 环境：10.26.40.83-86，Spark Standalone on K8s，HDFS on 83
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
RESULT_HOST="$ROOT/result/cache"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
SGX_SDK="${SGX_SDK:-/opt/intel/sgxsdk}"
NODES=(10.26.40.83 10.26.40.84 10.26.40.85 10.26.40.86)

export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT

# 3 个 Spark Worker 节点 × 128 核
SPARK_CORES_MAX="${SPARK_CORES_MAX:-384}"
SPARK_PARALLELISM="${SPARK_PARALLELISM:-384}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-8}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-48}"

ARCHIVE="$RESULT_HOST/archive-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULT_HOST" "$ARCHIVE"
shopt -s nullglob
for f in "$RESULT_HOST"/*.csv "$RESULT_HOST"/*.log; do
  [[ "$f" == "$ARCHIVE"* ]] && continue
  mv -f "$f" "$ARCHIVE/" 2>/dev/null || true
done

log() { echo "[$(date -Is)] $*" | tee -a "$RESULT_HOST/ablation_runner.log"; }

log "=== Build JAR + native (FAME, TEE sim for K8s) ==="
bash "$ROOT/scripts/build_company_benchmark_jar.sh"
cmake -S "$ROOT/abe-eac/native" -B "$ROOT/abe-eac/native/build" \
  -DMCL_ROOT="$MCL_ROOT" -DHADOOP_HOME="$HADOOP_HOME" \
  -DABE_SPARK_ENABLE_SGX=OFF
cmake --build "$ROOT/abe-eac/native/build" -j"$(nproc)" --target abe_spark_jni

cp -f "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" "$ROOT/jars/"

log "=== Sync artifacts to nodes ${NODES[*]} ==="
for ip in "${NODES[@]}"; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" \
    "shanlicheng@$ip:$ROOT/abe-eac/target/"
  rsync -az "$ROOT/abe-eac/native/build/libabe_spark_jni.so" \
    "shanlicheng@$ip:$ROOT/abe-eac/native/build/"
  rsync -az "$MCL_ROOT/build-eac/lib/libmcl.so" "$MCL_ROOT/build-eac/lib/libabe_framework.a" \
    "shanlicheng@$ip:$MCL_ROOT/build-eac/lib/" 2>/dev/null || true
  rsync -az "$ROOT/abe-eac/conf/abe-spark.conf" \
    "shanlicheng@$ip:$ROOT/abe-eac/conf/"
  ssh "shanlicheng@$ip" "mkdir -p $RESULT_HOST && chmod 777 $RESULT_HOST" || true
done

# 86 上有独立 Spark Worker（非 K8s），executor 通过 hostPath 绝对路径加载 JNI
log "=== Executor native paths (all nodes including 86) ==="
for ip in "${NODES[@]}"; do
  ssh "shanlicheng@$ip" "test -f $ROOT/abe-eac/native/build/libabe_spark_jni.so && test -f $MCL_ROOT/build-eac/lib/libmcl.so" \
    || { log "[WARN] native libs missing on $ip"; }
done

kubectl delete pod -n spark -l component=worker --wait=false 2>/dev/null || true
sleep 25
kubectl wait --for=condition=ready pod -n spark -l component=worker --timeout=300s || true

# 结果目录挂载在 84 的 hostPath，先改 yaml 中 results-host
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT_HOST && chmod 777 $RESULT_HOST"

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
    "FINE_CHECKPOINT_GB": "1",
    "PER_REQ_CSV": "1",
    "BENCH_MODE": "write",
    "RESUME_WRITE": "0",
    "WV_FAST_VERIFY": "1",
    "ABE_DEK_CACHE": "0",
    "ABE_SHARED_DEK": "0",
    "ABE_TEE_MODE": "sim",
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/cache-dek",
    "EAC_CSV": "eac_time.csv",
    "ABE_CSV": "abe_time.csv",
    "WRITE_CSV": "write_perf.csv",
    "READ_CSV": "read_perf.csv",
    "SPARK_CORES_MAX": "384",
    "SPARK_PARALLELISM": "384",
    "SPARK_EXECUTOR_CORES": "8",
    "SPARK_EXECUTOR_INSTANCES": "48",
}
defaults.update(env)
for k, v in defaults.items():
    pat = rf'(- name: {k}\n\s+value: )"[^"]*"'
    if re.search(pat, text):
        text = re.sub(pat, rf'\1"{v}"', text)
    else:
        anchor = '            - name: RESUME_WRITE\n              value: "0"'
        if anchor in text:
            text = text.replace(anchor, anchor + f'\n            - name: {k}\n              value: "{v}"', 1)
Path("/tmp/company-abe-cache-ablation.yaml").write_text(text)
print("applied", {k: defaults[k] for k in sorted(defaults) if k.startswith(("BENCH","ABE","EAC","TARGET","FINE","SPARK","HDFS"))})
PY
}

wait_job() {
  local name="$1"
  sleep 15
  for i in $(seq 1 360); do
    s=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)
    f=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)
    if [ "$s" = "1" ]; then
      log "[OK] $name"
      rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
      return 0
    fi
    if [ -n "$f" ] && [ "$f" != "0" ]; then
      log "[FAIL] $name"
      kubectl logs -n spark job/company-abe-spark --tail=60 2>/dev/null || true
      ssh shanlicheng@10.26.40.84 "tail -40 $RESULT_HOST/distributed_benchmark.log" 2>/dev/null || true
      return 1
    fi
    if (( i % 4 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[WRITE\\] fine|\\[READ\\] fine' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -2" \
        | tee -a "$RESULT_HOST/ablation_runner.log" || true
    fi
    sleep 20
  done
  log "[TIMEOUT] $name"
  return 1
}

run_job() {
  local name="$1"; shift
  log "======== RUN $name ========"
  gen_yaml "$@"
  kubectl delete job -n spark company-abe-spark --wait=true 2>/dev/null || true
  sleep 3
  kubectl apply -f /tmp/company-abe-cache-ablation.yaml
  wait_job "$name"
}

log "=== Prepare HDFS ==="
hdfs dfs -rm -r -f /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3 2>/dev/null || true
hdfs dfs -mkdir -p /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3
hdfs dfs -chmod 777 /abe-bench/cache-dek /abe-bench/eac-fast-v3 /abe-bench/eac-full-v3

COMMON="TARGET_GB=10 CHECKPOINT_GB=1 FINE_CHECKPOINT_GB=1 PER_REQ_CSV=1 ABE_TEE_MODE=sim \
  SPARK_CORES_MAX=$SPARK_CORES_MAX SPARK_PARALLELISM=$SPARK_PARALLELISM \
  SPARK_EXECUTOR_CORES=$SPARK_EXECUTOR_CORES SPARK_EXECUTOR_INSTANCES=$SPARK_EXECUTOR_INSTANCES"

# --- 实验 1：DEK cache（读路径；写库阶段同策略共享密文 DEK 以便 cache 命中）---
run_job write_shared_for_dek \
  $COMMON BENCH_MODE=write ABE_SHARED_DEK=1 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  EAC_CSV=write_shared_for_dek.csv WRITE_CSV=write_shared_for_dek_perf.csv

run_job read_cache_on \
  $COMMON BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=1 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  ABE_CSV=dek_cache_on.csv READ_CSV=read_cache_on_perf.csv

run_job read_cache_off \
  $COMMON BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/cache-dek \
  ABE_CSV=dek_cache_off.csv READ_CSV=read_cache_off_perf.csv

# --- 实验 2：EAC 快速写验证（每表独立 AES-DEK）---
run_job eac_fast_on \
  $COMMON BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-fast-v3 \
  EAC_CSV=eac_fast_on.csv WRITE_CSV=write_eac_fast_perf.csv

run_job eac_fast_off \
  $COMMON BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=0 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/eac-full-v3 \
  EAC_CSV=eac_fast_off.csv WRITE_CSV=write_eac_full_perf.csv

log "=== ALL DONE ==="
rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true

python3 << 'PY'
from pathlib import Path
base = Path("/home/shanlicheng/test-benchmark/ABE-Spark/result/cache")
for name in ["dek_cache_on.csv", "dek_cache_off.csv", "eac_fast_on.csv", "eac_fast_off.csv"]:
    p = base / name
    if not p.exists():
        print(name, "MISSING")
        continue
    lines = p.read_text().strip().splitlines()
    print(f"== {name} rows={len(lines)-1}")
    if len(lines) > 1:
        print("  last:", lines[-1][:240])
PY
python3 "$ROOT/scripts/merge_cache_results.py"
