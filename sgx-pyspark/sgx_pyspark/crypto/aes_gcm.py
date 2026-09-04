"""AES-128-GCM 列数据加解密。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass
class AesGcmResult:
    ok: bool
    output: bytes = b""
    reason: str = ""


class AESGCMCrypto:
    def __init__(self, key_bits: int = 128) -> None:
        self.key_bytes = key_bits // 8

    def generate_dek(self) -> bytes:
        return os.urandom(self.key_bytes)

    def encrypt(self, dek: bytes, nonce: bytes, plaintext: bytes) -> AesGcmResult:
        try:
            aesgcm = AESGCM(dek[: self.key_bytes])
            ct = aesgcm.encrypt(nonce, plaintext, None)
            return AesGcmResult(ok=True, output=ct)
        except Exception as exc:
            return AesGcmResult(ok=False, reason=str(exc))

    def decrypt(self, dek: bytes, nonce: bytes, ciphertext_with_tag: bytes) -> AesGcmResult:
        try:
            aesgcm = AESGCM(dek[: self.key_bytes])
            pt = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
            return AesGcmResult(ok=True, output=pt)
        except Exception as exc:
            return AesGcmResult(ok=False, reason=str(exc))

    def encrypt_column_segment(
        self, dek: bytes, plaintext: bytes, nonce_bytes: int, tag_bytes: int, nonce_seed: int = 0
    ) -> bytes:
        nonce = bytes([(nonce_seed + i) & 0xFF for i in range(nonce_bytes)])
        enc = self.encrypt(dek, nonce, plaintext)
        if not enc.ok:
            raise RuntimeError(f"AES-GCM encrypt failed: {enc.reason}")
        # cryptography AESGCM 输出已含 tag
        return nonce + enc.output

    def decrypt_column_segment(self, dek: bytes, segment: bytes, nonce_bytes: int) -> bytes:
        nonce = segment[:nonce_bytes]
        ciphertext_with_tag = segment[nonce_bytes:]
        dec = self.decrypt(dek, nonce, ciphertext_with_tag)
        if not dec.ok:
            raise RuntimeError(f"AES-GCM decrypt failed: {dec.reason}")
        return dec.output
