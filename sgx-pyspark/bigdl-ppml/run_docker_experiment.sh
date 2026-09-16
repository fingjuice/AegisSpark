#!/usr/bin/env bash
# BigDL-PPML Docker 内运行 ABE/Write-Verification 100GB 实验
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_ROOT="$(cd "${ROOT}/.." && pwd)"
# shellcheck disable=SC1091
source "${BENCHMARK_ROOT}/env.sh"
IMG="${PPML_IMAGE:-intelanalytics/bigdl-ppml-trusted-bigdata-gramine-reference-16g:2.3.0-SNAPSHOT}"
OUT="${ROOT}/Experimental-Result-BigDL-PPML"
ABE_ROOT="${ABE_SPARK_ROOT}"
MCL="${MCL_ROOT}"
HADOOP="${HADOOP_HOME}"

export ABE_EXP_TARGET_GB="${ABE_EXP_TARGET_GB:-100}"
export ABE_EXP_CHECKPOINT_GB="${ABE_EXP_CHECKPOINT_GB:-10}"
export ABE_EXP_WORKERS="${ABE_EXP_WORKERS:-32}"

DOCKER="${DOCKER_CMD:-docker}"
if ! $DOCKER info >/dev/null 2>&1; then
  if sudo -n $DOCKER info >/dev/null 2>&1; then
    DOCKER="sudo docker"
  else
    echo "无法访问 Docker，请使用: bash bigdl-ppml/run_native_experiment.sh start" >&2
    exit 1
  fi
fi

mkdir -p "${OUT}"

run_bench_in_container() {
  $DOCKER run --rm --net=host --privileged \
    --device=/dev/sgx/enclave \
    --device=/dev/sgx/provision \
    -v /var/run/aesmd/aesm.socket:/var/run/aesmd/aesm.socket \
    -v "${ABE_ROOT}:/abe-spark:ro" \
    -v "${MCL}:/mcl:ro" \
    -v "${HADOOP}:/hadoop:ro" \
    -v "${OUT}:/results" \
    -e ABE_EXP_TARGET_GB="${ABE_EXP_TARGET_GB}" \
    -e ABE_EXP_CHECKPOINT_GB="${ABE_EXP_CHECKPOINT_GB}" \
    -e ABE_EXP_WORKERS="${ABE_EXP_WORKERS}" \
    -e ABE_HDFS_ROOT="hdfs://localhost:8020/abe-bench/data" \
    -e ABE_SPARK_ROOT="/abe-spark" \
    -e MCL_ROOT="/mcl" \
    -e HADOOP_HOME="/hadoop" \
    -e JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" \
    --entrypoint /bin/bash \
    "${IMG}" \
    -c '
      set -e
      export LD_LIBRARY_PATH=/mcl/build-eac/lib:/hadoop/lib/native:/abe-spark/access-bench/build:/abe-spark/abe-eac/native/build:${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH:-}
      export CLASSPATH=$(/hadoop/bin/hadoop classpath)
      BIN=/abe-spark/access-bench/build/abe_access_bench
      [[ -x "$BIN" ]] || BIN=/abe-spark/abe-eac/native/build/abe_access_bench
      mkdir -p /results
      ln -sfn /results /abe-spark/access-bench/results
      cd /abe-spark/access-bench
      "$BIN" all 2>&1 | tee /results/run.log
    '
}

case "${1:-start}" in
  start)
    echo "拉取镜像 ${IMG} ..."
    $DOCKER pull "${IMG}" || true
    echo "在 BigDL-PPML 容器内启动 100GB 实验 ..."
    nohup bash "$0" _inner >>"${OUT}/docker.log" 2>&1 &
    echo $! > "${OUT}/run.pid"
    echo "已启动 pid=$(cat ${OUT}/run.pid) 日志: ${OUT}/docker.log"
    ;;
  _inner)
    run_bench_in_container
    ;;
  progress)
    tail -5 "${OUT}/run.log" 2>/dev/null || tail -5 "${OUT}/docker.log" 2>/dev/null || echo "暂无"
    ;;
  *)
    echo "用法: $0 {start|progress}"
    ;;
esac
