#!/usr/bin/env python3
"""Generate System Admin / Driver / Write Verification Ed25519 keypairs."""

from __future__ import annotations

import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def _write_keypair(out_dir: Path, name: str) -> None:
    private_key = Ed25519PrivateKey.generate()
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
    _write_keypair(keys_dir, "admin_ed25519")
    _write_keypair(keys_dir, "driver_tee_ed25519")
    _write_keypair(keys_dir, "write_verification_ed25519")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
