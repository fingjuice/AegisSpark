#!/usr/bin/env bash
# sgx-pyspark 四节点集群 100GB Company 实验（真实 HDFS + SSH 分布式）
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export TARGET_GB="${SGX_EXP_TARGET_GB:-100}"
export CHECKPOINT_GB="${SGX_EXP_CHECKPOINT_GB:-10}"
export BENCH_MODE="${BENCH_MODE:-all}"
export SGX_EXP_WORKERS="${SGX_EXP_WORKERS:-32}"

echo "=== sgx-pyspark 四节点集群实验 ==="
echo "  节点: 10.26.40.83-86"
echo "  HDFS: hdfs://10.26.40.83:9000/sgx-pyspark/data (真实)"
echo "  执行: 四节点 SSH 并行 (每节点 ${SGX_EXP_WORKERS} workers)"
echo "  TEE: Occlum sim 模式"
echo "  目标: ${TARGET_GB}GB, checkpoint: ${CHECKPOINT_GB}GB"
echo "  结果: ${ROOT}/result/company/"
echo ""

bash "$ROOT/scripts/run_cluster_ssh_benchmark.sh"
