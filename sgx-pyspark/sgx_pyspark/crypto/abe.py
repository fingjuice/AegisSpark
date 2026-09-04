"""CP-ABE 策略引擎与 DEK 封装（基于属性匹配 + MSK 派生密钥，兼容 design.md 语义）。"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass

from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto


@dataclass
class UserSecretKey:
    user_id: str
    attributes: list[str]
    raw: bytes = b""


@dataclass
class DekEncryptResult:
    ok: bool
    encrypted_dek_b64: str = ""
    reason: str = ""


@dataclass
class DekDecryptResult:
    ok: bool
    dek: bytes = b""
    reason: str = ""


class PolicyEngine:
    """解析并评估 design.md 风格策略表达式。"""

    _TOKEN = re.compile(r"([()])|(\w+:\w+)|\bOR\b|\bAND\b", re.IGNORECASE)

    @classmethod
    def evaluate(cls, expression: str, attributes: set[str]) -> bool:
        tokens = cls._tokenize(expression)
        pos = [0]

        def parse_expr() -> bool:
            left = parse_term()
            while pos[0] < len(tokens) and tokens[pos[0]].upper() == "OR":
                pos[0] += 1
                left = left or parse_term()
            return left

        def parse_term() -> bool:
            if pos[0] < len(tokens) and tokens[pos[0]] == "(":
                pos[0] += 1
                val = parse_expr()
                if pos[0] < len(tokens) and tokens[pos[0]] == ")":
                    pos[0] += 1
                return val
            if pos[0] < len(tokens) and tokens[pos[0]].upper() == "AND":
                pos[0] += 1
                return parse_term()
            if pos[0] < len(tokens) and ":" in tokens[pos[0]]:
                attr = tokens[pos[0]]
                pos[0] += 1
                return attr in attributes
            return False

        return parse_expr()

    @classmethod
    def _tokenize(cls, expression: str) -> list[str]:
        tokens: list[str] = []
        for match in cls._TOKEN.finditer(expression):
            tokens.append(match.group(0))
        return tokens


class ABECrypto:
    """FAM-ABE 语义模拟：MSK 派生策略密钥封装 DEK。"""

    def __init__(self, msk: bytes | None = None) -> None:
        self.msk = msk or os.urandom(32)
        self.aes = AESGCMCrypto()

    def setup(self) -> bytes:
        return self.msk

    def keygen_user(self, user_id: str, attributes: list[str]) -> UserSecretKey:
        material = hmac.new(self.msk, f"user:{user_id}:{','.join(sorted(attributes))}".encode(), hashlib.sha256).digest()
        return UserSecretKey(user_id=user_id, attributes=list(attributes), raw=material)

    def _policy_key(self, policy_expression: str) -> bytes:
        return hmac.new(self.msk, f"policy:{policy_expression}".encode(), hashlib.sha256).digest()

    def encrypt_dek(self, dek: bytes, policy_expression: str) -> DekEncryptResult:
        policy_key = self._policy_key(policy_expression)
        nonce = os.urandom(12)
        enc = self.aes.encrypt(policy_key, nonce, dek)
        if not enc.ok:
            return DekEncryptResult(ok=False, reason=enc.reason)
        blob = json.dumps({"policy": policy_expression, "nonce": base64.b64encode(nonce).decode(), "ct": base64.b64encode(enc.output).decode()})
        return DekEncryptResult(ok=True, encrypted_dek_b64=base64.b64encode(blob.encode()).decode())

    def decrypt_dek(self, usk: UserSecretKey, encrypted_dek_b64: str, policy_expression: str) -> DekDecryptResult:
        attrs = set(usk.attributes)
        if not PolicyEngine.evaluate(policy_expression, attrs):
            return DekDecryptResult(ok=False, reason="CP-ABE pairing failed: attributes do not satisfy policy")
        try:
            blob = json.loads(base64.b64decode(encrypted_dek_b64).decode())
            if blob.get("policy") != policy_expression:
                return DekDecryptResult(ok=False, reason="policy mismatch")
            policy_key = self._policy_key(policy_expression)
            nonce = base64.b64decode(blob["nonce"])
            ct = base64.b64decode(blob["ct"])
            dec = self.aes.decrypt(policy_key, nonce, ct)
            if not dec.ok:
                return DekDecryptResult(ok=False, reason=dec.reason)
            return DekDecryptResult(ok=True, dek=dec.output)
        except Exception as exc:
            return DekDecryptResult(ok=False, reason=str(exc))
