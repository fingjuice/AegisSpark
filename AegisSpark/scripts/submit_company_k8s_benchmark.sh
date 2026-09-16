#!/usr/bin/env bash
# 提交 ABE/Write-Verification Company 100GB 分布式基准到 K8s Spark 集群（372 核）
set -euo pipefail

ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
export ABE_SPARK_ROOT="$ROOT"
export MCL_ROOT="/home/shanlicheng/mcl"
export HADOOP_HOME="/home/shanlicheng/opt/hadoop"

echo "[1/4] 更新 Spark Worker DaemonSet（挂载 ABE native 库）..."
kubectl apply -f /home/shanlicheng/spark-k8s/03-worker-daemonset.yaml
kubectl rollout status daemonset/spark-worker -n spark --timeout=300s || true

echo "[2/4] 构建 abe-eac JAR..."
cd "$ROOT"
./build/mvn -pl abe-eac -am package -DskipTests -q

echo "[3/5] 清理 HDFS 数据目录..."
export HADOOP_HOME=/home/shanlicheng/opt/hadoop
export PATH=$HADOOP_HOME/bin:$PATH
hdfs dfs -rm -r -f hdfs://10.26.40.83:9000/abe-bench/data 2>/dev/null || true
hdfs dfs -mkdir -p hdfs://10.26.40.83:9000/abe-bench/data
hdfs dfs -chmod 777 hdfs://10.26.40.83:9000/abe-bench/data

echo "[4/5] 清理旧 Job..."
kubectl delete job company-abe-spark -n spark --ignore-not-found=true
sleep 3

echo "[5/5] 提交分布式 benchmark Job..."
kubectl apply -f "$ROOT/k8s/company-abe-spark.yaml"
echo "Job 已提交。监控："
echo "  kubectl logs -n spark job/company-abe-spark -f"
echo "  ls -la $ROOT/result/company/"
