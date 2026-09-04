#!/usr/bin/env bash
# 生成 System Admin 与 Driver TEE 的 ECDSA 密钥对
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEYS_DIR="${ROOT}/conf/keys"
export SGX_PYSPARK_ROOT="${ROOT}"
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"

mkdir -p "${KEYS_DIR}"

python3 - <<'PY'
from pathlib import Path
from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto

keys = Path(__import__("os").environ["SGX_PYSPARK_ROOT"]) / "conf" / "keys"
ecdsa = ECDSACrypto()
for name in ("admin", "driver_tee"):
    priv = keys / f"{name}_ecdsa.pem"
    pub = keys / f"{name}_ecdsa.pub"
    if not priv.exists():
        ecdsa.generate_keypair(priv, pub)
        print(f"generated {priv.name} / {pub.name}")
    else:
        print(f"exists {priv.name}")
PY

echo "Keys ready under ${KEYS_DIR}"
