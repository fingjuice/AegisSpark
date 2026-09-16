"""远程证明服务（Occlum/SGX DCAP 占位 + 模拟模式）。"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

from sgx_pyspark.types import AdminEndorsement


@dataclass
class AttestationResult:
    ok: bool
    session_token: str = ""
    wsk: bytes = b""
    reason: str = ""


class AttestationService:
    """Driver TEE 与 Write Verification 之间的远程证明握手。"""

    def __init__(self, tee_mode: str = "sim") -> None:
        self.tee_mode = tee_mode

    def is_occlum(self) -> bool:
        return os.path.exists("/etc/occlum") or os.environ.get("OCCLUM", "") == "1"

    def driver_attest_to_write_verification(self, write_verification_url: str, endorsement: AdminEndorsement) -> AttestationResult:
        if not write_verification_url:
            return AttestationResult(ok=False, reason="empty Write Verification URL")
        if not endorsement.app_code_hash or not endorsement.admin_signature:
            return AttestationResult(ok=False, reason="invalid admin endorsement")

        if self.tee_mode == "occlum" and not self.is_occlum():
            return AttestationResult(ok=False, reason="Occlum runtime not detected")

        # 模拟远程证明通过后委派全局写权限根密钥 wsk
        session_material = f"{endorsement.app_code_hash}:{endorsement.timestamp}:{write_verification_url}".encode()
        session_token = hashlib.sha256(session_material).hexdigest()
        wsk = hashlib.sha256(b"wsk:" + session_material).digest()
        return AttestationResult(ok=True, session_token=session_token, wsk=wsk)
