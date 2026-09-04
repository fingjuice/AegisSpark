"""Spark Worker TEE Plugin：读/写路径编排 + TEE 内计算算子。"""

from __future__ import annotations

from typing import Callable

from sgx_pyspark.compute.operators import TeeOperator
from sgx_pyspark.crypto.abe import ABECrypto, UserSecretKey
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.pipeline.worker_read import WorkerReadPipeline
from sgx_pyspark.pipeline.worker_write import WorkerWritePipeline
from sgx_pyspark.tee.compute_context import TeeComputeContext, TeeComputeResult
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.tee.runtime_guard import require_tee_runtime, tee_runtime_label
from sgx_pyspark.types import AdminEndorsement, TaskTicket, WorkerReadResult, WriteProxyResult

WriteTransformOperator = Callable[[TeeComputeContext], dict[str, bytes]]


class SparkWorkerTeePlugin:
    def __init__(
        self,
        abe: ABECrypto,
        usk: UserSecretKey,
        nne: NameNodeExtension,
        datanode: DataNodeStore,
        wv: WriteVerificationTee,
        aes: AESGCMCrypto | None = None,
    ) -> None:
        self.abe = abe
        self.usk = usk
        self.nne = nne
        self.datanode = datanode
        self.wv = wv
        self.aes = aes or AESGCMCrypto()
        self.read_pipeline = WorkerReadPipeline()
        self.write_pipeline = WorkerWritePipeline(self.abe, self.aes)

    def _read_to_context(self, hdfs_path: str) -> TeeComputeContext:
        header = self.nne.read_security_header(hdfs_path)
        layout = self.nne.read_column_layout(hdfs_path)
        encrypted = self.datanode.read_block(hdfs_path)
        plan = self.read_pipeline.build_access_plan(header, layout, self.abe, self.usk)
        read_result = self.read_pipeline.process_stream(encrypted, plan, self.aes)
        if not read_result.ok:
            raise RuntimeError(read_result.reason or "decrypt failed")
        authorized = {item.range.column_name for item in plan if item.authorized}
        masked = {item.range.column_name for item in plan if not item.authorized}
        return TeeComputeContext(layout, read_result.plaintext, authorized, masked)

    def read_encrypted_file(self, hdfs_path: str) -> WorkerReadResult:
        header = self.nne.read_security_header(hdfs_path)
        layout = self.nne.read_column_layout(hdfs_path)
        encrypted = self.datanode.read_block(hdfs_path)
        plan = self.read_pipeline.build_access_plan(header, layout, self.abe, self.usk)
        return self.read_pipeline.process_stream(encrypted, plan, self.aes)

    def compute_on_encrypted_input(self, hdfs_path: str, operator: TeeOperator, tee_mode: str | None = None) -> TeeComputeResult:
        """
        design.md 读路径步骤 9-10：TEE 内解密 → 算子计算 → 仅返回聚合/摘要指标。
        完整明文不离开 Worker TEE 内存。
        """
        runtime = require_tee_runtime(tee_mode)
        try:
            ctx = self._read_to_context(hdfs_path)
            result = ctx.apply_operator(operator)
            result.tee_runtime = tee_runtime_label()
            return result
        except Exception as exc:  # noqa: BLE001
            return TeeComputeResult(ok=False, reason=str(exc), tee_runtime=runtime)

    def compute_and_write_encrypted_output(
        self,
        hdfs_path: str,
        transform: WriteTransformOperator,
        task_ticket: TaskTicket,
        admin_endorsement: AdminEndorsement,
        target_path: str,
        column_policies: dict[str, str],
        tee_mode: str | None = None,
    ) -> WriteProxyResult:
        """
        design.md 写路径步骤 5：TEE 内解密 → 本地算子变换 → 加密 → Write Verification落盘。
        """
        require_tee_runtime(tee_mode)
        ctx = self._read_to_context(hdfs_path)
        output_columns = transform(ctx)
        if not output_columns:
            return WriteProxyResult(ok=False, reason="write transform produced empty columns")
        return self.write_encrypted_output(
            task_ticket,
            admin_endorsement,
            target_path,
            output_columns,
            column_policies,
        )

    def write_encrypted_output(
        self,
        task_ticket: TaskTicket,
        admin_endorsement: AdminEndorsement,
        target_path: str,
        columns: dict[str, bytes],
        column_policies: dict[str, str],
    ) -> WriteProxyResult:
        header, layout, ciphertext = self.write_pipeline.encrypt_columns(target_path, columns, column_policies)
        result = self.wv.handle_optimized_write(task_ticket, admin_endorsement, target_path, ciphertext)
        if result.ok:
            self.nne.persist_meta(target_path, header, layout)
        return result
