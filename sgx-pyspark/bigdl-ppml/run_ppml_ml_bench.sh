#!/usr/bin/env bash
# BigDL-PPML PySpark ML 基准（Gramine/SGX 容器内 Spark）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LEGACY="${LEGACY_PPML_DIR:-/home/slc/bigdl-ppml}"

if [[ -f "${LEGACY}/bash.sh" ]]; then
  exec bash "${LEGACY}/bash.sh"
fi

IMG="${PPML_IMAGE:-intelanalytics/bigdl-ppml-trusted-bigdata-gramine-reference-16g:2.3.0-SNAPSHOT}"
DOCKER="${DOCKER_CMD:-docker}"
$DOCKER info >/dev/null 2>&1 || DOCKER="sudo docker"

$DOCKER run --rm --net=host --privileged \
  --device=/dev/sgx/enclave --device=/dev/sgx/provision \
  -v /var/run/aesmd/aesm.socket:/var/run/aesmd/aesm.socket \
  -v "${SCRIPT_DIR}/..:/ppml/work" \
  -e SGX_MEM_SIZE=16G -e LOCAL_IP=127.0.0.1 \
  --entrypoint /bin/bash "${IMG}" \
  -c '/ppml/trusted-big-data-ml/spark-submit-with-sgx.sh --master local[4] /ppml/work/bigdl-ppml/benchmark_ml_stub.py 2>/dev/null || echo "请挂载 benchmark_ml.py"'
