"""KGC: offline attribute issuance and job endorsement."""

from __future__ import annotations

import time
from pathlib import Path

from sgx_pyspark.crypto.abe import ABECrypto, UserSecretKey
from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto, build_admin_endorsement_payload, sha256_hex
from sgx_pyspark.types import AdminEndorsement


class Kgc:
    """Holds MSK; root of trust (System Admin / KGC)."""

    def __init__(
        self,
        abe: ABECrypto | None = None,
        ecdsa: ECDSACrypto | None = None,
        admin_private_key_path: str | Path | None = None,
        admin_public_key_path: str | Path | None = None,
    ) -> None:
        self.abe = abe or ABECrypto()
        self.ecdsa = ecdsa or ECDSACrypto()
        self.admin_private_key_path = Path(admin_private_key_path) if admin_private_key_path else None
        self.admin_public_key_path = Path(admin_public_key_path) if admin_public_key_path else None

    def issue_user_attributes(self, user_id: str, attributes: list[str]) -> UserSecretKey:
        return self.abe.keygen_user(user_id, attributes)

    def endorse_spark_job(
        self,
        spark_app_binary: bytes,
        allowed_dirs: list[str],
        admin_private_key_path: str | Path | None = None,
    ) -> AdminEndorsement:
        sk_path = Path(admin_private_key_path or self.admin_private_key_path or "")
        if not sk_path.is_file():
            raise FileNotFoundError(f"admin private key not found: {sk_path}")
        code_hash = sha256_hex(spark_app_binary)
        timestamp = int(time.time())
        payload = build_admin_endorsement_payload(code_hash, allowed_dirs, timestamp)
        sig = self.ecdsa.sign(payload, sk_path)
        if not sig.ok:
            raise RuntimeError(f"admin endorsement sign failed: {sig.reason}")
        return AdminEndorsement(
            app_code_hash=code_hash,
            allowed_root_directories=allowed_dirs,
            timestamp=timestamp,
            admin_signature=sig.signature_hex,
        )

    def ensure_keypair(self, private_path: str | Path, public_path: str | Path) -> None:
        private_path = Path(private_path)
        if not private_path.is_file():
            self.ecdsa.generate_keypair(private_path, public_path)
