#!/usr/bin/env bash
# 在 10.26.40.60（SGX 可用）本地重跑 result/cache 消融实验（对齐 run_cache_fix2.sh）
# 用法（在 60 上）：bash scripts/run_cache_on_node60.sh
set -euo pipefail

ROOT="${ABE_SPARK_ROOT:-/home/slc/test-benchmark/ABE-Spark}"
RESULT_HOST="$ROOT/result/cache"
SPARK_HOME="${SPARK_HOME:-/home/slc/test-benchmark/spark-3.2.3-bin-hadoop3.2}"
HADOOP_HOME="${HADOOP_HOME:-/home/slc/hadoop}"
JAVA_HOME="${JAVA_HOME:-/home/slc/environments/jdk1.8.0_202}"
ABE_JAR="$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar"
JNI_DIR="$ROOT/abe-eac/native/build"
CONF_DIR="$ROOT/abe-eac/conf"
KEYS_DIR="$CONF_DIR/keys"
HDFS_NN="hdfs://localhost:8020"

export JAVA_HOME HADOOP_HOME SPARK_HOME ROOT
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-slc}"
export ABE_SPARK_ROOT="$ROOT"
export ABE_SPARK_CONFIG="$CONF_DIR/abe-spark.conf"
export ABE_SPARK_CONF_DIR="$KEYS_DIR"
export MCL_ROOT="${MCL_ROOT:-/home/slc/mcl}"
export LD_LIBRARY_PATH="$JNI_DIR:${MCL_ROOT}/build-eac/lib:${LD_LIBRARY_PATH:-}"

mkdir -p "$RESULT_HOST" "$RESULT_HOST/invalid-node60"
# 归档旧 CSV
shopt -s nullglob
for f in "$RESULT_HOST"/*.csv; do
  mv -f "$f" "$RESULT_HOST/invalid-node60/" 2>/dev/null || true
done
: > "$RESULT_HOST/distributed_benchmark.log"

echo "=== node60 cache experiment ==="
echo "ROOT=$ROOT SPARK=$SPARK_HOME JAVA=$($JAVA_HOME/bin/java -version 2>&1 | head -1)"
echo "SGX: $(ls /dev/sgx_enclave 2>&1); aesmd=$(systemctl is-active aesmd 2>/dev/null || echo n/a)"

# 启动本地 HDFS（若未运行）
if ! hdfs dfs -ls / >/dev/null 2>&1; then
  echo "[HDFS] starting local dfs..."
  if [ ! -d /home/slc/hadoop-data/namenode/current ]; then
    echo "[HDFS] format namenode..."
    hdfs namenode -format -force -nonInteractive || true
  fi
  bash "$HADOOP_HOME/sbin/start-dfs.sh"
  sleep 8
fi
hdfs dfs -ls / >/dev/null
echo "[HDFS] ok"

# 准备 HDFS 目录
for d in cache-dek eac-fast-v3 eac-full-v3 eac-conc-64 eac-conc-128; do
  hdfs dfs -rm -r -f "/abe-bench/$d" 2>/dev/null || true
  hdfs dfs -mkdir -p "/abe-bench/$d"
  hdfs dfs -chmod 777 "/abe-bench/$d"
done

# 并行度：60 节点 128 核
CORES="${SPARK_CORES_MAX:-96}"
EXECS="${SPARK_EXECUTOR_INSTANCES:-12}"
ECORES="${SPARK_EXECUTOR_CORES:-8}"

run_one() {
  local name="$1"; shift
  echo
  echo "======== RUN $name ========"
  # 解析 KEY=VAL
  local TARGET_GB=10 CHECKPOINT_GB=1 FINE_CHECKPOINT_GB=0.1
  local BENCH_MODE=write RESUME_WRITE=0
  local WV_FAST_VERIFY=1 ABE_DEK_CACHE=0 ABE_SHARED_DEK=0
  local HDFS_DATA_ROOT="$HDFS_NN/abe-bench/cache-dek"
  local EAC_CSV=eac_time.csv ABE_CSV=abe_time.csv
  local WRITE_CSV=write_perf.csv READ_CSV=read_perf.csv
  local LOCAL_CORES="$CORES" LOCAL_EXECS="$EXECS" LOCAL_ECORES="$ECORES"
  for kv in "$@"; do
    case "$kv" in
      TARGET_GB=*) TARGET_GB="${kv#*=}" ;;
      CHECKPOINT_GB=*) CHECKPOINT_GB="${kv#*=}" ;;
      FINE_CHECKPOINT_GB=*) FINE_CHECKPOINT_GB="${kv#*=}" ;;
      BENCH_MODE=*) BENCH_MODE="${kv#*=}" ;;
      RESUME_WRITE=*) RESUME_WRITE="${kv#*=}" ;;
      WV_FAST_VERIFY=*) WV_FAST_VERIFY="${kv#*=}" ;;
      ABE_DEK_CACHE=*) ABE_DEK_CACHE="${kv#*=}" ;;
      ABE_SHARED_DEK=*) ABE_SHARED_DEK="${kv#*=}" ;;
      HDFS_DATA_ROOT=*) HDFS_DATA_ROOT="${kv#*=}" ;;
      EAC_CSV=*) EAC_CSV="${kv#*=}" ;;
      ABE_CSV=*) ABE_CSV="${kv#*=}" ;;
      WRITE_CSV=*) WRITE_CSV="${kv#*=}" ;;
      READ_CSV=*) READ_CSV="${kv#*=}" ;;
      SPARK_CORES_MAX=*) LOCAL_CORES="${kv#*=}" ;;
      SPARK_EXECUTOR_INSTANCES=*) LOCAL_EXECS="${kv#*=}" ;;
      SPARK_EXECUTOR_CORES=*) LOCAL_ECORES="${kv#*=}" ;;
    esac
  done

  export TARGET_GB CHECKPOINT_GB FINE_CHECKPOINT_GB BENCH_MODE RESUME_WRITE
  export WV_FAST_VERIFY ABE_DEK_CACHE ABE_SHARED_DEK HDFS_DATA_ROOT
  export EAC_CSV ABE_CSV WRITE_CSV READ_CSV
  export RESULT_DIR="$RESULT_HOST"

  echo "mode=$BENCH_MODE sharedDek=$ABE_SHARED_DEK dekCache=$ABE_DEK_CACHE eacFast=$WV_FAST_VERIFY"
  echo "hdfs=$HDFS_DATA_ROOT cores=$LOCAL_CORES"

  # 删除本 job 将写入的 CSV，避免旧表头
  rm -f "$RESULT_HOST/$EAC_CSV" "$RESULT_HOST/$ABE_CSV" \
        "$RESULT_HOST/$WRITE_CSV" "$RESULT_HOST/$READ_CSV" 2>/dev/null || true

  {
    echo "===== start $(date -Is) name=$name sharedDek=$ABE_SHARED_DEK dekCache=$ABE_DEK_CACHE eacFast=$WV_FAST_VERIFY ====="
    "$SPARK_HOME/bin/spark-submit" \
      --master "local[$LOCAL_CORES]" \
      --deploy-mode client \
      --class org.apache.spark.abe.benchmark.CompanyAbeBenchmark \
      --driver-memory 32g \
      --conf spark.driver.memoryOverhead=8g \
      --conf spark.driver.extraLibraryPath="$JNI_DIR" \
      --conf spark.driver.extraJavaOptions="-Djava.library.path=$JNI_DIR" \
      --conf spark.executor.extraLibraryPath="$JNI_DIR" \
      --conf spark.executor.extraJavaOptions="-Djava.library.path=$JNI_DIR" \
      --conf spark.default.parallelism="$LOCAL_CORES" \
      --conf spark.sql.shuffle.partitions="$LOCAL_CORES" \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --conf spark.network.timeout=800s \
      --conf spark.ui.enabled=false \
      --conf spark.eventLog.enabled=false \
      --conf spark.hadoop.fs.defaultFS="$HDFS_NN" \
      --conf spark.abe.enabled=true \
      --conf spark.abe.master.config="$ABE_SPARK_CONFIG" \
      --conf spark.abe.user.id=analyst_user \
      --conf spark.abe.user.attributes=role:analyst,dept:finance \
      --conf spark.executorEnv.ABE_SPARK_ROOT="$ROOT" \
      --conf spark.executorEnv.ABE_SPARK_CONFIG="$ABE_SPARK_CONFIG" \
      --conf spark.executorEnv.ABE_SPARK_CONF_DIR="$KEYS_DIR" \
      --conf spark.executorEnv.LD_LIBRARY_PATH="$LD_LIBRARY_PATH" \
      --conf spark.executorEnv.HADOOP_USER_NAME="$HADOOP_USER_NAME" \
      "$ABE_JAR"
    echo "===== done $(date -Is) name=$name ====="
  } >> "$RESULT_HOST/distributed_benchmark.log" 2>&1

  echo "[OK] $name"
  ls -la "$RESULT_HOST"/*.csv 2>/dev/null | tail -8 || true
}

# 1) DEK cache 前置：共享密文 DEK
run_one write_shared_for_dek \
  BENCH_MODE=write ABE_SHARED_DEK=1 ABE_DEK_CACHE=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/cache-dek" \
  EAC_CSV=write_shared_for_dek.csv WRITE_CSV=write_shared_for_dek_perf.csv

# 2) 读：cache ON / OFF
run_one read_cache_on \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=1 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/cache-dek" \
  ABE_CSV=dek_cache_on.csv READ_CSV=read_cache_on_perf.csv

run_one read_cache_off \
  BENCH_MODE=read ABE_SHARED_DEK=0 ABE_DEK_CACHE=0 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/cache-dek" \
  ABE_CSV=dek_cache_off.csv READ_CSV=read_cache_off_perf.csv

# 3) EAC 快验 / 全验
run_one eac_fast_on \
  BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=1 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/eac-fast-v3" \
  EAC_CSV=eac_fast_on.csv WRITE_CSV=write_eac_fast_perf.csv

run_one eac_fast_off \
  BENCH_MODE=write ABE_SHARED_DEK=0 WV_FAST_VERIFY=0 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/eac-full-v3" \
  EAC_CSV=eac_fast_off.csv WRITE_CSV=write_eac_full_perf.csv

# 4) 并发 64 / 128（local 模式下用 parallelism 近似）
run_one eac_conc_64 \
  BENCH_MODE=write WV_FAST_VERIFY=1 ABE_SHARED_DEK=0 \
  SPARK_CORES_MAX=64 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/eac-conc-64" \
  EAC_CSV=eac_conc_64.csv WRITE_CSV=write_conc_64_perf.csv

run_one eac_conc_128 \
  BENCH_MODE=write WV_FAST_VERIFY=1 ABE_SHARED_DEK=0 \
  SPARK_CORES_MAX=128 \
  HDFS_DATA_ROOT="$HDFS_NN/abe-bench/eac-conc-128" \
  EAC_CSV=eac_conc_128.csv WRITE_CSV=write_conc_128_perf.csv

echo ALL_DONE
python3 - <<'PY'
from pathlib import Path
base=Path('/home/slc/test-benchmark/ABE-Spark/result/cache')
for name in ['dek_cache_on.csv','dek_cache_off.csv','eac_fast_on.csv','eac_fast_off.csv',
             'eac_conc_64.csv','eac_conc_128.csv']:
  p=base/name
  if not p.exists():
    print(name, 'MISSING'); continue
  lines=p.read_text().strip().splitlines()
  print(f'== {name}')
  print('  header:', lines[0])
  print('  n=', len(lines)-1, 'last=', lines[-1][:160])
if (base/'dek_cache_on.csv').exists() and (base/'dek_cache_off.csv').exists():
  def grab(p):
    rows=p.read_text().strip().splitlines()
    return dict(zip(rows[0].split(','), rows[-1].split(',')))
  on, off = grab(base/'dek_cache_on.csv'), grab(base/'dek_cache_off.csv')
  print('DEK compare: on tee=', on.get('tee_calls'), 'hits=', on.get('cache_hits'),
        'ABE=', on.get('ABE-time'), '| off tee=', off.get('tee_calls'),
        'ABE=', off.get('ABE-time'))
PY
