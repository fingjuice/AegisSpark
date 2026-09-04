"""Write Verification (Server TEE)：写权限最终仲裁者。"""

from __future__ import annotations

import time
from pathlib import Path

from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto, build_admin_endorsement_payload, build_task_ticket_payload
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.path_util import is_under_any_root, ticket_binds_target
from sgx_pyspark.tee.monotonic_counter import InMemoryMonotonicCounter
from sgx_pyspark.types import AdminEndorsement, TaskTicket, WriteProxyResult, WriteVerifyResult


class WriteVerificationTee:
    """运行在 Server TEE 内部，验证 Admin → Driver → Worker 信任链。"""

    def __init__(
        self,
        admin_public_key_path: str | Path,
        driver_public_key_path: str | Path,
        datanode: DataNodeStore,
        ecdsa: ECDSACrypto | None = None,
        counter: InMemoryMonotonicCounter | None = None,
        counter_namespace: str = "sgx-pyspark-wv",
    ) -> None:
        self.admin_vk_path = Path(admin_public_key_path)
        self.driver_vk_path = Path(driver_public_key_path)
        self.datanode = datanode
        self.ecdsa = ecdsa or ECDSACrypto()
        self.counter = counter or InMemoryMonotonicCounter()
        self.counter_ns = counter_namespace

    def verify_write_chain(
        self,
        ticket: TaskTicket,
        endorsement: AdminEndorsement,
        target_path: str,
        now_epoch_sec: int | None = None,
    ) -> WriteVerifyResult:
        now = now_epoch_sec if now_epoch_sec is not None else int(time.time())

        admin_payload = build_admin_endorsement_payload(
            endorsement.app_code_hash,
            endorsement.allowed_root_directories,
            endorsement.timestamp,
        )
        admin_sig = self.ecdsa.verify(admin_payload, endorsement.admin_signature, self.admin_vk_path)
        if not admin_sig.ok:
            return WriteVerifyResult(ok=False, reason=f"Root Trust Verification Failed: {admin_sig.reason}")

        ticket_payload = build_task_ticket_payload(
            ticket.job_id, ticket.task_id, ticket.worker_ip, ticket.allowed_target_path, ticket.expires_at
        )
        driver_sig = self.ecdsa.verify(ticket_payload, ticket.driver_signature, self.driver_vk_path)
        if not driver_sig.ok:
            return WriteVerifyResult(ok=False, reason=f"Capability Delegation Broken: {driver_sig.reason}")

        if now > ticket.expires_at:
            return WriteVerifyResult(ok=False, reason="Task ticket expired")

        if not is_under_any_root(target_path, endorsement.allowed_root_directories):
            return WriteVerifyResult(ok=False, reason="Security Domain Violation: Path not whitelisted by Admin")

        if not ticket_binds_target(ticket.allowed_target_path, target_path):
            return WriteVerifyResult(ok=False, reason="Task Overflow: Worker executing unsanctioned partition path")

        return WriteVerifyResult(ok=True, monotonic_counter=self.counter.current(self.counter_ns))

    def handle_optimized_write(
        self,
        task_ticket: TaskTicket,
        admin_endorsement: AdminEndorsement,
        target_path: str,
        incoming_stream: bytes,
    ) -> WriteProxyResult:
        verify = self.verify_write_chain(task_ticket, admin_endorsement, target_path)
        if not verify.ok:
            return WriteProxyResult(ok=False, reason=verify.reason)

        mc = self.counter.increment(self.counter_ns)
        written = self.datanode.write_block(target_path, incoming_stream)
        return WriteProxyResult(ok=True, bytes_written=written, monotonic_counter=mc, reason="Write Verification Success")
