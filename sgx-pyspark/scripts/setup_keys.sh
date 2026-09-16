#!/usr/bin/env bash
# Generate System Admin / Driver TEE Ed25519 keypairs
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEYS_DIR="${ROOT}/conf/keys"
export SGX_PYSPARK_ROOT="${ROOT}"
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"

mkdir -p "${KEYS_DIR}"

python3 - <<'PY'
from pathlib import Path
from sgx_pyspark.crypto.ed25519_sign import Ed25519Crypto

keys = Path(__import__("os").environ["SGX_PYSPARK_ROOT"]) / "conf" / "keys"
crypto = Ed25519Crypto()
for name in ("admin", "driver_tee", "write_verification"):
    priv = keys / f"{name}_ed25519.pem"
    pub = keys / f"{name}_ed25519.pub"
    if not priv.exists():
        crypto.generate_keypair(priv, pub)
        print(f"generated {priv.name} / {pub.name}")
    else:
        print(f"exists {priv.name}")
PY

echo "Keys ready under ${KEYS_DIR}"
