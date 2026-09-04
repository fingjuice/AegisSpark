"""ECDSA-P256-SHA256 签名与验签（Admin / Driver 信任链）。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
from cryptography.exceptions import InvalidSignature


@dataclass
class SignResult:
    ok: bool
    signature_hex: str = ""
    reason: str = ""


@dataclass
class VerifyResult:
    ok: bool
    reason: str = ""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_admin_endorsement_payload(app_code_hash: str, allowed_dirs: list[str], timestamp: int) -> bytes:
    dirs = ",".join(allowed_dirs)
    return f"{app_code_hash}|{dirs}|{timestamp}".encode("utf-8")


def build_task_ticket_payload(
    job_id: str, task_id: str, worker_ip: str, allowed_target_path: str, expires_at: int
) -> bytes:
    return f"{job_id}|{task_id}|{worker_ip}|{allowed_target_path}|{expires_at}".encode("utf-8")


class ECDSACrypto:
    def generate_keypair(self, private_path: str | Path, public_path: str | Path) -> None:
        private_key = ec.generate_private_key(ec.SECP256R1())
        private_path = Path(private_path)
        public_path = Path(public_path)
        private_path.parent.mkdir(parents=True, exist_ok=True)
        private_path.write_bytes(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        public_path.write_bytes(
            private_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )

    def sign(self, payload: bytes, private_key_path: str | Path) -> SignResult:
        try:
            private_key = serialization.load_pem_private_key(Path(private_key_path).read_bytes(), password=None)
            digest = hashlib.sha256(payload).digest()
            signature = private_key.sign(digest, ec.ECDSA(Prehashed(hashes.SHA256())))
            return SignResult(ok=True, signature_hex=signature.hex())
        except Exception as exc:
            return SignResult(ok=False, reason=str(exc))

    def verify(self, payload: bytes, signature_hex: str, public_key_path: str | Path) -> VerifyResult:
        try:
            public_key = serialization.load_pem_public_key(Path(public_key_path).read_bytes())
            digest = hashlib.sha256(payload).digest()
            public_key.verify(bytes.fromhex(signature_hex), digest, ec.ECDSA(Prehashed(hashes.SHA256())))
            return VerifyResult(ok=True)
        except InvalidSignature:
            return VerifyResult(ok=False, reason="invalid signature")
        except Exception as exc:
            return VerifyResult(ok=False, reason=str(exc))
