#!/usr/bin/env bash
# 多线程对比：64 vs 128
# - Write Verification 验证 20GB（独立 DEK，每 0.1GB：tables / eac_avg / eac_total）
# - ABE 读解密 20GB（独立 DEK，cache 关，每 0.1GB：tables / abe_avg / abe_total）
# 结果 → result/multi-thread/
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT_HOST="$ROOT/result/multi-thread"
BASE_YAML="$ROOT/k8s/company-abe-spark.yaml"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"
mkdir -p "$RESULT_HOST"

cat > "$RESULT_HOST/README.md" << 'EOF'
# multi-thread：64 vs 128 并发

约定（避免历史踩坑）：
- `ABE_SHARED_DEK=0`：每表独立密文 DEK（不做共享 DEK 人造命中）
- `ABE_DEK_CACHE=0`：读侧关闭 DEK cache（测真实 decrypt）
- `WV_FAST_VERIFY=1`：写验证为 verifyWriteChain
- 写前删除旧 CSV，表头固定为简化列

## Write Verification 验证

| 文件 | 并发 |
|------|------|
| `eac_write_64.csv` | cores.max=64 |
| `eac_write_128.csv` | cores.max=128 |

列：`checkpoint_gb,tables,eac_avg_ms,eac_total_ms,timestamp`
- `tables`：累计处理表数
- `eac_avg_ms`：平均单次写验证 = eac_total / tables
- `eac_total_ms`：累计写验证总时间

## ABE 读解密

复用 `eac_write_128` 写入的独立 DEK 数据（`mt-eac-128`），再分别以 64/128 读。

| 文件 | 并发 |
|------|------|
| `abe_read_64.csv` | cores.max=64 |
| `abe_read_128.csv` | cores.max=128 |

列：`checkpoint_gb,tables,abe_avg_ms,abe_total_ms,timestamp`
- `tables`：累计读取表数
- `abe_avg_ms`：平均每表 ABE 时间 = ABE-total / tables（每表含 public+sensitive 两次 decryptDek）
- `abe_total_ms`：累计 ABE decrypt 总时间
EOF

bash "$ROOT/scripts/build_company_benchmark_jar.sh"
for ip in 10.26.40.83 10.26.40.84 10.26.40.85; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" "shanlicheng@$ip:$ROOT/abe-eac/target/"
done
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT_HOST; chmod 777 $RESULT_HOST"

# 清掉可能冲突的旧结果
ssh shanlicheng@10.26.40.84 "rm -f $RESULT_HOST/*.csv $RESULT_HOST/distributed_benchmark.log" || true
rm -f "$RESULT_HOST"/*.csv 2>/dev/null || true

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
    "BENCH_MODE": "write",
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
  for i in $(seq 1 360); do
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
    if (( i % 3 == 0 )); then
      ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
        "grep -aE '\\[WRITE\\] fine|\\[READ\\] fine|cores.max|sharedDek=|dekCache=' $RESULT_HOST/distributed_benchmark.log 2>/dev/null | tail -3" || true
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

# HDFS 目录
hdfs dfs -rm -r -f /abe-bench/mt-eac-64 /abe-bench/mt-eac-128 2>/dev/null || true
hdfs dfs -mkdir -p /abe-bench/mt-eac-64 /abe-bench/mt-eac-128
hdfs dfs -chmod 777 /abe-bench/mt-eac-64 /abe-bench/mt-eac-128

# ---- Write Verification ：64 / 128 ----
run_job eac_write_64 \
  BENCH_MODE=write TARGET_GB=20 FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=64 SPARK_PARALLELISM=64 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=8 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/mt-eac-64 \
  EAC_CSV=eac_write_64.csv WRITE_CSV=write_eac_64_perf.csv

run_job eac_write_128 \
  BENCH_MODE=write TARGET_GB=20 FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=128 SPARK_PARALLELISM=128 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=16 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/mt-eac-128 \
  EAC_CSV=eac_write_128.csv WRITE_CSV=write_eac_128_perf.csv

# ---- ABE 读：复用 eac_write_128 的独立 DEK 数据（避免再写 20GB）----
run_job abe_read_64 \
  BENCH_MODE=read TARGET_GB=20 FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=64 SPARK_PARALLELISM=64 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=8 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/mt-eac-128 \
  ABE_CSV=abe_read_64.csv READ_CSV=read_abe_64_perf.csv

run_job abe_read_128 \
  BENCH_MODE=read TARGET_GB=20 FINE_CHECKPOINT_GB=0.1 \
  ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 MT_SIMPLE_CSV=1 \
  SPARK_CORES_MAX=128 SPARK_PARALLELISM=128 \
  SPARK_EXECUTOR_CORES=8 SPARK_EXECUTOR_INSTANCES=16 \
  HDFS_DATA_ROOT=hdfs://10.26.40.83:9000/abe-bench/mt-eac-128 \
  ABE_CSV=abe_read_128.csv READ_CSV=read_abe_128_perf.csv

echo ALL_DONE
rsync -az "shanlicheng@10.26.40.84:$RESULT_HOST/" "$RESULT_HOST/" || true
python3 << 'PY'
from pathlib import Path
base=Path('/home/shanlicheng/test-benchmark/ABE-Spark/result/multi-thread')
for name in ['eac_write_64.csv','eac_write_128.csv','abe_read_64.csv','abe_read_128.csv']:
  p=base/name
  if not p.exists():
    print(name, 'MISSING'); continue
  lines=p.read_text().strip().splitlines()
  print(f'== {name} header={lines[0]} n={len(lines)-1}')
  print(f'   first={lines[1] if len(lines)>1 else None}')
  print(f'   last ={lines[-1] if len(lines)>1 else None}')
PY
