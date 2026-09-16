#!/usr/bin/env bash
# xattr 消融：100GB company 数据集，每表独立 AES 密钥，每 5GB 记录 xattr 密文空间占用
#
# 指标：HDFS xattr 中 ABE 加密的 AES 密钥密文（security.abe.header + column_layout）
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
MCL_ROOT="${MCL_ROOT:-/home/shanlicheng/mcl}"
RESULT_HOST="$ROOT/result/xattr"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
NODES=(10.26.40.83 10.26.40.84 10.26.40.85 10.26.40.86)

export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT

SPARK_CORES_MAX="${SPARK_CORES_MAX:-384}"
SPARK_PARALLELISM="${SPARK_PARALLELISM:-384}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-8}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-48}"

mkdir -p "$RESULT_HOST"
log() { echo "[$(date -Is)] $*" | tee -a "$RESULT_HOST/xattr_runner.log"; }

log "=== Build JAR + native (FAME, TEE sim) ==="
bash "$ROOT/scripts/build_company_benchmark_jar.sh"
cmake -S "$ROOT/abe-eac/native" -B "$ROOT/abe-eac/native/build" \
  -DMCL_ROOT="$MCL_ROOT" -DHADOOP_HOME="$HADOOP_HOME" \
  -DABE_SPARK_ENABLE_SGX=OFF
cmake --build "$ROOT/abe-eac/native/build" -j"$(nproc)" --target abe_spark_jni
cp -f "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" "$ROOT/jars/"

log "=== Sync to nodes ${NODES[*]} ==="
for ip in "${NODES[@]}"; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" \
    "shanlicheng@$ip:$ROOT/abe-eac/target/"
  rsync -az "$ROOT/abe-eac/native/build/libabe_spark_jni.so" \
    "shanlicheng@$ip:$ROOT/abe-eac/native/build/"
  rsync -az "$MCL_ROOT/build-eac/lib/libmcl.so" \
    "shanlicheng@$ip:$MCL_ROOT/build-eac/lib/" 2>/dev/null || true
  rsync -az "$ROOT/abe-eac/conf/abe-spark.conf" \
    "shanlicheng@$ip:$ROOT/abe-eac/conf/"
  ssh "shanlicheng@$ip" "mkdir -p $RESULT_HOST && chmod 777 $RESULT_HOST" || true
done

kubectl delete pod -n spark -l component=worker --wait=false 2>/dev/null || true
sleep 25
kubectl wait --for=condition=ready pod -n spark -l component=worker --timeout=300s || true
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
    "TARGET_GB": "100",
    "CHECKPOINT_GB": "5",
    "FINE_CHECKPOINT_GB": "5",
    "BENCH_MODE": "write",
    "RESUME_WRITE": "0",
    "PER_REQ_CSV": "0",
    "XATTR_CSV": "1",
    "WV_FAST_VERIFY": "1",
    "ABE_DEK_CACHE": "0",
    "ABE_SHARED_DEK": "0",
    "ABE_TEE_MODE": "sim",
    "HDFS_DATA_ROOT": "hdfs://10.26.40.83:9000/abe-bench/xattr-100gb",
    "XATTR_CSV": "1",
    "XATTR_SIZE_CSV": "xattr_size.csv",
    "WRITE_CSV": "write_perf.csv",
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
Path("/tmp/company-abe-xattr.yaml").write_text(text)
print("applied", defaults)
PY
}

wait_job() {
  sleep 15
  for i in $(seq 1 1440); do
    s=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)
    f=$(kubectl get job -n spark company-abe-spark -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)
    if [ "$s" = "1" ]; then
      log "[OK] xattr 100GB write"
      rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
      return 0
    fi
    if [ -n "$f" ] && [ "$f" != "0" ]; then
      log "[FAIL] xattr 100GB write"
      kubectl logs -n spark job/company-abe-spark --tail=80 2>/dev/null || true
      ssh shanlicheng@10.26.40.84 "tail -40 $RESULT_HOST/distributed_benchmark.log" 2>/dev/null || true
      return 1
    fi
    if (( i % 6 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[XATTR\\]|\\[WRITE\\] checkpoint' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -2" \
        | tee -a "$RESULT_HOST/xattr_runner.log" || true
    fi
    sleep 20
  done
  log "[TIMEOUT]"
  return 1
}

log "=== Prepare HDFS ==="
hdfs dfs -rm -r -f /abe-bench/xattr-100gb 2>/dev/null || true
hdfs dfs -mkdir -p /abe-bench/xattr-100gb
hdfs dfs -chmod 777 /abe-bench/xattr-100gb

log "======== RUN xattr 100GB (independent AES-DEK per table) ========"
gen_yaml \
  TARGET_GB=100 CHECKPOINT_GB=5 FINE_CHECKPOINT_GB=5 \
  BENCH_MODE=write XATTR_CSV=1 PER_REQ_CSV=0 ABE_SHARED_DEK=0 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/xattr-100gb \
  XATTR_SIZE_CSV=xattr_size.csv

kubectl delete job -n spark company-abe-spark --wait=true 2>/dev/null || true
sleep 3
kubectl apply -f /tmp/company-abe-xattr.yaml
wait_job

log "=== DONE ==="
if [ -f "$RESULT_HOST/xattr_size.csv" ]; then
  echo "== xattr_size.csv =="
  head -1 "$RESULT_HOST/xattr_size.csv"
  tail -3 "$RESULT_HOST/xattr_size.csv"
fi
