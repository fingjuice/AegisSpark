#!/usr/bin/env bash
# 四节点 SSH 分布式 benchmark（真实 HDFS，每节点独立 Python 进程）
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/sgx-pyspark"
REMOTE_ROOT="/home/shanlicheng/sgx-pyspark-run"
NODES=(10.26.40.83 10.26.40.84 10.26.40.85 10.26.40.86)
USER="shanlicheng"
CLUSTER_SIZE=${#NODES[@]}

export SGX_PYSPARK_ROOT="$ROOT"
export SGX_HDFS_MODE=real
export SGX_PYSPARK_TEE_MODE=sim
export HADOOP_HOME="/home/shanlicheng/opt/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_USER_NAME=shanlicheng
export PATH="$HADOOP_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:/usr/lib/jvm/java-11-openjdk-amd64/lib/server:${LD_LIBRARY_PATH:-}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"

TARGET_GB="${TARGET_GB:-100}"
CHECKPOINT_GB="${CHECKPOINT_GB:-10}"
FINE_GB="${FINE_GB:-0.5}"
BENCH_MODE="${BENCH_MODE:-all}"
WORKERS="${SGX_EXP_WORKERS:-32}"
RESULT_DIR="$ROOT/result/company"
NODE_TARGET_GB=$(python3 -c "print(${TARGET_GB}/${CLUSTER_SIZE})")

echo "=== 四节点 SSH 分布式 benchmark ==="
echo "节点: ${NODES[*]}"
echo "HDFS: hdfs://10.26.40.83:9000/sgx-pyspark/data"
echo "目标: ${TARGET_GB}GB (每节点 ${NODE_TARGET_GB}GB), 主采样: ${CHECKPOINT_GB}GB, EAC/ABE细粒度: ${FINE_GB}GB, workers/节点: ${WORKERS}"

echo "[0/4] 同步代码到四节点..."
for node in "${NODES[@]}"; do
  if [[ "$node" == "10.26.40.83" ]]; then continue; fi
  echo "  rsync -> ${node}:${REMOTE_ROOT}"
  ssh -o BatchMode=yes "${USER}@${node}" "mkdir -p ${REMOTE_ROOT}"
  rsync -az --delete \
    --exclude='.venv' --exclude='data/' --exclude='result/' \
    --exclude='Experimental-Result' --exclude='dist/' --exclude='.pytest_cache' \
    --exclude='occlum/instance' --exclude='native/build' --exclude='spark-warehouse' \
    --exclude='*.egg-info' \
    "$ROOT/" "${USER}@${node}:${REMOTE_ROOT}/"
done

# 准备 HDFS（仅写阶段清理）
if [[ "$BENCH_MODE" == "write" || "$BENCH_MODE" == "all" ]]; then
  hdfs dfs -rm -r -f hdfs://10.26.40.83:9000/sgx-pyspark/data/bench 2>/dev/null || true
  hdfs dfs -mkdir -p hdfs://10.26.40.83:9000/sgx-pyspark/data
  hdfs dfs -chmod -R 777 hdfs://10.26.40.83:9000/sgx-pyspark/data
fi

# 清理旧结果（读阶段保留写结果）
mkdir -p "$RESULT_DIR"
if [[ "$BENCH_MODE" == "write" || "$BENCH_MODE" == "all" ]]; then
  # 保留主控日志与 pid
  find "$RESULT_DIR" -maxdepth 1 -type f ! -name 'cluster_run.log' ! -name 'cluster_run.pid' -delete 2>/dev/null || true
  rm -rf "$RESULT_DIR"/node_* 2>/dev/null || true
  for node in "${NODES[@]}"; do
    [[ "$node" == "10.26.40.83" ]] && continue
    ssh -o BatchMode=yes "${USER}@${node}" \
      "rm -rf ${REMOTE_ROOT}/result/company/node_* ${REMOTE_ROOT}/result/company/*.csv" 2>/dev/null || true
  done
elif [[ "$BENCH_MODE" == "read" ]]; then
  rm -f "$RESULT_DIR"/read_perf.csv "$RESULT_DIR"/abe_time.csv \
        "$RESULT_DIR"/coordinator_read.log "$RESULT_DIR"/coordinator_read.pid 2>/dev/null || true
  for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    rm -f "$RESULT_DIR/node_${i}"/read_* "$RESULT_DIR/node_${i}/progress.json" 2>/dev/null || true
  done
  for node in "${NODES[@]}"; do
    [[ "$node" == "10.26.40.83" ]] && continue
    ssh -o BatchMode=yes "${USER}@${node}" \
      "rm -f ${REMOTE_ROOT}/result/company/node_*/progress.json ${REMOTE_ROOT}/result/company/node_*/read_*" 2>/dev/null || true
  done
fi
kubectl delete job company-sgx-pyspark -n spark --ignore-not-found=true 2>/dev/null || true

_run_remote() {
  local node="$1"
  local node_id="$2"
  local mode="$3"
  local log="$RESULT_DIR/node_${node_id}/${mode}.log"
  mkdir -p "$RESULT_DIR/node_${node_id}"

  local pyroot="$ROOT"
  local result_arg="$RESULT_DIR"
  if [[ "$node" != "10.26.40.83" ]]; then
    pyroot="$REMOTE_ROOT"
    result_arg="$REMOTE_ROOT/result/company"
    ssh -o BatchMode=yes "${USER}@${node}" "mkdir -p ${result_arg}/node_${node_id}"
  fi

  local runner="$RESULT_DIR/node_${node_id}/run_${mode}.sh"
  cat >"$runner" <<RUNEOF
#!/usr/bin/env bash
set -euo pipefail
for _j in /usr/lib/jvm/java-11-openjdk-amd64 /home/shanlicheng/env/jdk11; do
  if [[ -f "\$_j/lib/server/libjvm.so" ]]; then
    export JAVA_HOME="\$_j"
    export LD_LIBRARY_PATH="\$_j/lib/server:\${LD_LIBRARY_PATH:-}"
    break
  fi
done
export SGX_PYSPARK_ROOT=$pyroot
export PYTHONPATH=$pyroot
export SGX_HDFS_MODE=real
export SGX_PYSPARK_TEE_MODE=sim
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_USER_NAME=shanlicheng
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:\${LD_LIBRARY_PATH:-}
export PATH=$HADOOP_HOME/bin:\$PATH
export SGX_CLUSTER_MODE=1
export SGX_CLUSTER_NODE=$node_id
export SGX_CLUSTER_SIZE=$CLUSTER_SIZE
export SGX_EXP_TARGET_GB=$NODE_TARGET_GB
export SGX_EXP_CHECKPOINT_GB=$CHECKPOINT_GB
export SGX_EXP_WORKERS=$WORKERS
python3 -m sgx_pyspark.benchmark.cluster_bench $mode --result-dir $result_arg
RUNEOF
  chmod +x "$runner"

  if [[ "$node" == "10.26.40.83" ]]; then
    bash "$runner" >"$log" 2>&1 &
  else
    scp -q "$runner" "${USER}@${node}:${result_arg}/node_${node_id}/run_${mode}.sh"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${USER}@${node}" \
      "bash ${result_arg}/node_${node_id}/run_${mode}.sh" >"$log" 2>&1 &
  fi
  echo $! > "$RESULT_DIR/node_${node_id}/${mode}.pid"
}

_sync_node_results() {
  for i in $(seq 1 $((CLUSTER_SIZE - 1))); do
    echo "  拉取 node_${i} 结果 <- ${NODES[$i]}"
    rsync -az "${USER}@${NODES[$i]}:${REMOTE_ROOT}/result/company/node_${i}/" "$RESULT_DIR/node_${i}/"
  done
}

_wait_all() {
  local mode="$1"
  echo "等待四节点 ${mode} 完成..."
  while true; do
    local running=false
    for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
      pidfile="$RESULT_DIR/node_${i}/${mode}.pid"
      if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        running=true
      fi
    done
    if ! $running; then break; fi
    sleep 10
    for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
      if [[ -f "$RESULT_DIR/node_${i}/${mode}.log" ]]; then
        tail -1 "$RESULT_DIR/node_${i}/${mode}.log" 2>/dev/null | sed "s/^/[node_${i}] /" || true
      fi
    done
  done
}

_start_coordinator() {
  local mode="$1"
  local log="$RESULT_DIR/coordinator_${mode}.log"
  PYTHONUNBUFFERED=1 \
  SGX_EXP_TARGET_GB="$TARGET_GB" SGX_EXP_CHECKPOINT_GB="$CHECKPOINT_GB" SGX_EXP_FINE_GB="$FINE_GB" \
    python3 -u "$ROOT/scripts/cluster_coordinator.py" "$mode" --result-dir "$RESULT_DIR" \
    >"$log" 2>&1 &
  echo $! > "$RESULT_DIR/coordinator_${mode}.pid"
  echo "[协调器] 已启动 PID=$(cat "$RESULT_DIR/coordinator_${mode}.pid") -> $RESULT_DIR/${mode}_perf.csv（每表 EAC/ABE 写入 node_*/）"
}

_stop_coordinator() {
  local mode="$1"
  local pidfile="$RESULT_DIR/coordinator_${mode}.pid"
  if [[ -f "$pidfile" ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    wait "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

if [[ "$BENCH_MODE" == "write" || "$BENCH_MODE" == "all" ]]; then
  echo "[写阶段] 启动集群协调器 + 四节点..."
  _start_coordinator write
  for i in "${!NODES[@]}"; do
    _run_remote "${NODES[$i]}" "$i" write
  done
  _wait_all write
  _stop_coordinator write
  _sync_node_results
  python3 "$ROOT/scripts/merge_ops_results.py" "$RESULT_DIR"
fi

if [[ "$BENCH_MODE" == "read" || "$BENCH_MODE" == "all" ]]; then
  echo "[读阶段] 清理写阶段残留 progress，避免协调器误判已完成..."
  rm -f "$RESULT_DIR"/read_perf.csv "$RESULT_DIR"/abe_time.csv \
        "$RESULT_DIR"/coordinator_read.log "$RESULT_DIR"/coordinator_read.pid 2>/dev/null || true
  for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    rm -f "$RESULT_DIR/node_${i}/progress.json" \
          "$RESULT_DIR/node_${i}"/read_*.csv \
          "$RESULT_DIR/node_${i}"/abe_time.csv \
          "$RESULT_DIR/node_${i}/read.log" \
          "$RESULT_DIR/node_${i}/read.pid" 2>/dev/null || true
  done
  for node in "${NODES[@]}"; do
    [[ "$node" == "10.26.40.83" ]] && continue
    ssh -o BatchMode=yes "${USER}@${node}" \
      "rm -f ${REMOTE_ROOT}/result/company/node_*/progress.json \
             ${REMOTE_ROOT}/result/company/node_*/read_*.csv \
             ${REMOTE_ROOT}/result/company/node_*/abe_time.csv \
             ${REMOTE_ROOT}/result/company/node_*/read.log \
             ${REMOTE_ROOT}/result/company/node_*/read.pid" 2>/dev/null || true
  done

  echo "[读阶段] 启动集群协调器 + 四节点..."
  _start_coordinator read
  for i in "${!NODES[@]}"; do
    _run_remote "${NODES[$i]}" "$i" read
  done
  _wait_all read
  _stop_coordinator read
  _sync_node_results
  python3 "$ROOT/scripts/merge_ops_results.py" "$RESULT_DIR"
fi

echo "实验完成。结果: $RESULT_DIR"
ls -la "$RESULT_DIR"/*.csv 2>/dev/null || true
