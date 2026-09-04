#!/usr/bin/env python3
"""生成 System Admin 与 Driver TEE 的 ECDSA-P256 密钥对。"""

from __future__ import annotations

import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


def _write_keypair(out_dir: Path, name: str) -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    priv_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    (out_dir / f"{name}.pem").write_bytes(priv_pem)
    (out_dir / f"{name}.pub").write_bytes(pub_pem)
    print(f"generated {out_dir / name}.pem")


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    keys_dir = root / "conf" / "keys"
    keys_dir.mkdir(parents=True, exist_ok=True)
    _write_keypair(keys_dir, "admin_ecdsa")
    _write_keypair(keys_dir, "driver_tee_ecdsa")
    _write_keypair(keys_dir, "write_verification_ecdsa")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
