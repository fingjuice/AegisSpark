"""Spark Driver TEE Plugin：远程证明、wsk 委派、Task Ticket 分发。"""

from __future__ import annotations

import time
from pathlib import Path

from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto, build_task_ticket_payload
from sgx_pyspark.path_util import is_under_any_root
from sgx_pyspark.tee.attestation import AttestationResult, AttestationService
from sgx_pyspark.types import AdminEndorsement, TaskTicket


class SparkDriverTeePlugin:
    def __init__(
        self,
        driver_private_key_path: str | Path,
        ecdsa: ECDSACrypto | None = None,
        attestation: AttestationService | None = None,
        task_ticket_ttl_sec: int = 3600,
    ) -> None:
        self.driver_sk_path = Path(driver_private_key_path)
        self.ecdsa = ecdsa or ECDSACrypto()
        self.attestation = attestation or AttestationService()
        self.task_ticket_ttl_sec = task_ticket_ttl_sec
        self._session_token = ""
        self._wsk = b""

    @property
    def write_session_key(self) -> bytes:
        return self._wsk

    def initialize_job(self, write_verification_url: str, endorsement: AdminEndorsement) -> AttestationResult:
        result = self.attestation.driver_attest_to_write_verification(write_verification_url, endorsement)
        if result.ok:
            self._session_token = result.session_token
            self._wsk = result.wsk
        return result

    def is_path_authorized(self, target_path: str, allowed_roots: list[str]) -> bool:
        return is_under_any_root(target_path, allowed_roots)

    def dispense_task_ticket(
        self,
        job_id: str,
        task_id: str,
        worker_ip: str,
        allowed_target_path: str,
    ) -> TaskTicket:
        expires_at = int(time.time()) + self.task_ticket_ttl_sec
        payload = build_task_ticket_payload(job_id, task_id, worker_ip, allowed_target_path, expires_at)
        sig = self.ecdsa.sign(payload, self.driver_sk_path)
        if not sig.ok:
            raise RuntimeError(f"deriveTaskTicket sign failed: {sig.reason}")
        return TaskTicket(
            job_id=job_id,
            task_id=task_id,
            worker_ip=worker_ip,
            allowed_target_path=allowed_target_path,
            expires_at=expires_at,
            driver_signature=sig.signature_hex,
        )

    def has_write_session(self) -> bool:
        return bool(self._session_token)
