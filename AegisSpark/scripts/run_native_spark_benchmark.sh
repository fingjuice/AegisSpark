#!/usr/bin/env bash
# 原生 Spark 100GB company 读写；结果 → result/native-spark/
# 会等待 HDFS /abe-bench 清空完成后再提交。
set -euo pipefail
ROOT="/home/shanlicheng/test-benchmark/ABE-Spark"
RESULT="$ROOT/result/native-spark"
YAML="$ROOT/k8s/native-spark-company.yaml"
export HADOOP_HOME="${HADOOP_HOME:-/home/shanlicheng/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

mkdir -p "$RESULT"
cat > "$RESULT/README.md" << 'EOF'
# 原生 Spark company 100GB 读写基准（无 ABE/Write-Verification）

- 数据集：`company_xxxxxx`，50~300 行/表，8 字段（与 ABE 实验相同）
- HDFS：`hdfs://10.26.40.83:9000/abe-bench/native-data/`
- 每 **1GB** 记录：`checkpoint_gb,tables,rows,plain_bytes,task_total_ms,timestamp`
- 文件：`write_perf.csv` / `read_perf.csv`
EOF

chmod +x "$ROOT/scripts/build_native_spark_benchmark_jar.sh"
bash "$ROOT/scripts/build_native_spark_benchmark_jar.sh"
for ip in 10.26.40.83 10.26.40.84 10.26.40.85; do
  rsync -az "$ROOT/abe-eac/target/spark-abe-eac_2.12-3.2.3.jar" \
    "shanlicheng@$ip:$ROOT/abe-eac/target/"
done
ssh shanlicheng@10.26.40.84 "mkdir -p $RESULT; chmod 777 $RESULT"

# 等待 HDFS 清理（若后台清理仍在跑）
echo "Waiting for HDFS /abe-bench cleanup ..."
for i in $(seq 1 720); do
  if grep -q '^DONE' /tmp/hdfs_rm_abe.log 2>/dev/null; then
    echo "HDFS cleanup finished"
    break
  fi
  # 若无后台清理日志，主动再清一次
  if [ ! -f /tmp/hdfs_rm_abe.log ] && (( i == 1 )); then
    hdfs dfs -rm -r -skipTrash -f /abe-bench || true
    hdfs dfs -mkdir -p /abe-bench/native-data
    hdfs dfs -chmod -R 777 /abe-bench || true
    echo DONE > /tmp/hdfs_rm_abe.log
    break
  fi
  if (( i % 12 == 0 )); then
    echo "[$(date +%H:%M:%S)] still cleaning... $(tail -1 /tmp/hdfs_rm_abe.log 2>/dev/null || true)"
  fi
  sleep 10
done

hdfs dfs -mkdir -p /abe-bench/native-data
hdfs dfs -chmod 777 /abe-bench /abe-bench/native-data || true
hdfs dfs -du -s -h /abe-bench || true

kubectl delete job -n spark native-spark-company --wait=true 2>/dev/null || true
kubectl delete job -n spark company-abe-spark --wait=true 2>/dev/null || true
sleep 2
kubectl apply -f "$YAML"

echo "Job submitted. Log: $RESULT/native_benchmark.log"
# 轮询直到完成
for i in $(seq 1 2400); do
  s=$(kubectl get job -n spark native-spark-company -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)
  f=$(kubectl get job -n spark native-spark-company -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)
  if [ "$s" = "1" ]; then
    echo "[OK] native-spark-company"
    rsync -az "shanlicheng@10.26.40.84:$RESULT/" "$RESULT/" || true
    wc -l "$RESULT"/write_perf.csv "$RESULT"/read_perf.csv 2>/dev/null || true
    head -2 "$RESULT"/write_perf.csv 2>/dev/null || true
    tail -2 "$RESULT"/write_perf.csv 2>/dev/null || true
    head -2 "$RESULT"/read_perf.csv 2>/dev/null || true
    tail -2 "$RESULT"/read_perf.csv 2>/dev/null || true
    exit 0
  fi
  if [ -n "$f" ] && [ "$f" != "0" ]; then
    echo "[FAIL]"
    ssh shanlicheng@10.26.40.84 "tail -80 $RESULT/native_benchmark.log" || true
    exit 1
  fi
  if (( i % 4 == 0 )); then
    ssh -o ConnectTimeout=5 shanlicheng@10.26.40.84 \
      "grep -aE '\\[WRITE\\]|\\[READ\\]|Native Spark' $RESULT/native_benchmark.log 2>/dev/null | tail -3" || true
  fi
  sleep 15
done
echo "[TIMEOUT]"; exit 1
