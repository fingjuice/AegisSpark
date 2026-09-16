#!/usr/bin/env bash
# 提交 sgx-pyspark 100GB Company 分布式基准到四节点 K8s Spark 集群（真实 HDFS）
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/sgx-pyspark"
export SGX_PYSPARK_ROOT="$ROOT"
export HADOOP_HOME="/home/shanlicheng/opt/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"
export HADOOP_USER_NAME=shanlicheng

TARGET_GB="${TARGET_GB:-100}"
CHECKPOINT_GB="${CHECKPOINT_GB:-10}"
BENCH_MODE="${BENCH_MODE:-all}"

echo "[1/5] 检查四节点 K8s 集群..."
kubectl get nodes -o wide

echo "[2/5] 准备 HDFS 目录 (真实 HDFS)..."
hdfs dfs -rm -r -f hdfs://10.26.40.83:9000/sgx-pyspark/data/bench 2>/dev/null || true
hdfs dfs -mkdir -p hdfs://10.26.40.83:9000/sgx-pyspark/data
hdfs dfs -chmod -R 777 hdfs://10.26.40.83:9000/sgx-pyspark/data

echo "[3/5] 清理旧结果与停止冲突 Job..."
mkdir -p "$ROOT/result/company"
rm -f "$ROOT/result/company"/write_*.csv "$ROOT/result/company"/read_*.csv \
      "$ROOT/result/company"/timing_summary.csv "$ROOT/result/company"/cluster_benchmark.log
kubectl delete job company-sgx-pyspark -n spark --ignore-not-found=true
kubectl delete job company-abe-spark -n spark --ignore-not-found=true
sleep 5

echo "[4/5] 验证 HDFS 写入..."
echo "sgx-pyspark-hdfs-test" | hdfs dfs -put -f - hdfs://10.26.40.83:9000/sgx-pyspark/data/.probe
hdfs dfs -cat hdfs://10.26.40.83:9000/sgx-pyspark/data/.probe
hdfs dfs -rm -f hdfs://10.26.40.83:9000/sgx-pyspark/data/.probe

echo "[5/5] 提交四节点分布式 benchmark Job (Driver@84, Executors@83-86)..."
kubectl apply -f "$ROOT/k8s/company-sgx-pyspark.yaml"
echo ""
echo "Job 已提交。监控命令："
echo "  kubectl logs -n spark job/company-sgx-pyspark -f"
echo "  tail -f $ROOT/result/company/cluster_benchmark.log"
echo "  ls -la $ROOT/result/company/"
