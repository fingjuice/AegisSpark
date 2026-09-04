#!/usr/bin/env bash
# 构建 Occlum 实例：拷贝项目、安装 Python 依赖、生成 Occlum 镜像
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTANCE="${ROOT}/occlum/instance"
IMAGE="${ROOT}/occlum/image"

rm -rf "${INSTANCE}"
mkdir -p "${INSTANCE}"

if command -v occlum &>/dev/null; then
  cd "${INSTANCE}"
  occlum init
  cp "${ROOT}/occlum/Occlum.json" "${INSTANCE}/Occlum.json"

  rm -rf "${IMAGE}"
  mkdir -p "${IMAGE}/opt/sgx-pyspark"
  cp -r "${ROOT}/sgx_pyspark" "${ROOT}/examples" "${ROOT}/conf" "${ROOT}/scripts" "${IMAGE}/opt/sgx-pyspark/"
  cp -r "${ROOT}/examples" "${ROOT}/conf" "${IMAGE}/opt/"

  # 安装依赖到 Occlum 镜像（需要网络）
  if command -v pip3 &>/dev/null; then
    pip3 install --target="${IMAGE}/opt/python-libs" cryptography pyspark 2>/dev/null || true
  fi

  occlum build
  echo "Occlum instance built at ${INSTANCE}"
else
  echo "skip occlum build: occlum CLI not installed"
fi
