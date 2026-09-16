"""PySpark + TEE 计算算子编排：Driver 分发任务，Executor 在 TEE 内解密并计算。"""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass, field
from typing import Any

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.compute.operators import get_operator, normalize_for_output
from sgx_pyspark.config import SgxPysparkConfig, load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.spark.worker_bootstrap import WorkerBootstrapConfig, build_worker_plugin
from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin
from sgx_pyspark.tee.runtime_guard import require_tee_runtime, tee_runtime_label
from sgx_pyspark.types import AdminEndorsement, TaskTicket


@dataclass
class TeeSparkJobResult:
    ok: bool
    operator: str
    partition_metrics: list[dict[str, Any]] = field(default_factory=list)
    aggregated: dict[str, Any] = field(default_factory=dict)
    write_results: list[dict[str, Any]] = field(default_factory=list)
    tee_runtime: str = ""
    reason: str = ""


def _aggregate_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dept_counts: dict[str, int] = {}
    total_records = 0
    salary_masked = 0
    for row in rows:
        total_records += int(row.get("record_count", 1))
        dept = row.get("dept")
        if dept:
            dept_counts[dept] = dept_counts.get(dept, 0) + 1
        if row.get("salary_masked"):
            salary_masked += 1
    return {
        "total_records": total_records,
        "dept_distribution": dept_counts,
        "salary_masked_partitions": salary_masked,
    }


def _tee_map_compute_partition(partition_iter, bootstrap_dict: dict, operator_name: str):
    """Spark mapPartitions 回调：每个分区在 TEE 内解密并执行算子。"""
    cfg = WorkerBootstrapConfig.from_dict(bootstrap_dict)
    worker = build_worker_plugin(cfg)
    operator = get_operator(operator_name)
    for hdfs_path in partition_iter:
        result = worker.compute_on_encrypted_input(hdfs_path, operator, tee_mode=cfg.tee_mode)
        if not result.ok:
            yield json.dumps({"ok": False, "path": hdfs_path, "reason": result.reason})
            continue
        payload = {"ok": True, "path": hdfs_path, **result.metrics}
        payload["tee_runtime"] = result.tee_runtime
        yield json.dumps(payload)


class TeeSparkJob:
    """
    Spark Driver TEE 编排器。

    - 读路径：parallelize(加密输入路径) → mapPartitions(TEE 解密+算子) → 聚合指标
    - 写路径：mapPartitions(TEE 解密+变换+加密) → Write Verification落盘
    """

    def __init__(
        self,
        cfg: SgxPysparkConfig | None = None,
        user_attributes: list[str] | None = None,
        write_user_attributes: list[str] | None = None,
    ) -> None:
        self.cfg = cfg or load_config()
        self.tee_mode = os.environ.get("SGX_PYSPARK_TEE_MODE", self.cfg.tee.tee_mode)
        self._user_attributes = user_attributes or self.cfg.spark.user_attributes
        self._write_user_attributes = write_user_attributes or ["Department:HR"]
        self._driver: SparkDriverTeePlugin | None = None
        self._endorsement: AdminEndorsement | None = None

    def _worker_bootstrap(self, for_write: bool = False) -> WorkerBootstrapConfig:
        attrs = self._write_user_attributes if for_write else self._user_attributes
        return WorkerBootstrapConfig(
            data_root=self.cfg.paths.data_root,
            keys_dir=self.cfg.paths.keys_dir,
            user_id=self.cfg.spark.user_id if not for_write else f"{self.cfg.spark.user_id}-writer",
            user_attributes=attrs,
            admin_public_key_path=self.cfg.signature.admin_public_key_path,
            driver_public_key_path=self.cfg.signature.driver_public_key_path,
            counter_namespace=self.cfg.tee.monotonic_counter_namespace,
            tee_mode=self.tee_mode,
        )

    def initialize_driver(self, spark_app_bytes: bytes | None = None, allowed_dirs: list[str] | None = None) -> None:
        require_tee_runtime(self.tee_mode)
        admin = Kgc(
            abe=ABECrypto(msk=load_or_create_msk(self.cfg.paths.keys_dir)),
            admin_private_key_path=self.cfg.signature.admin_private_key_path,
        )
        admin.ensure_keypair(self.cfg.signature.admin_private_key_path, self.cfg.signature.admin_public_key_path)
        admin.ensure_keypair(self.cfg.signature.driver_private_key_path, self.cfg.signature.driver_public_key_path)

        app_bytes = spark_app_bytes or b"sgx-pyspark-tee-compute"
        roots = allowed_dirs or ["hdfs://namenode:9000/data/output/"]
        self._endorsement = admin.endorse_spark_job(app_bytes, roots, self.cfg.signature.admin_private_key_path)

        self._driver = SparkDriverTeePlugin(
            self.cfg.signature.driver_private_key_path,
            task_ticket_ttl_sec=self.cfg.spark.task_ticket_ttl_sec,
        )
        attest = self._driver.initialize_job(self.cfg.tee.write_verification_url, self._endorsement)
        if not attest.ok:
            raise RuntimeError(f"driver attestation failed: {attest.reason}")

    def _require_driver(self) -> SparkDriverTeePlugin:
        if self._driver is None or self._endorsement is None:
            raise RuntimeError("call initialize_driver() before submitting Spark tasks")
        return self._driver

    def run_read_compute(self, input_paths: list[str], operator: str = "dept_stats") -> TeeSparkJobResult:
        require_tee_runtime(self.tee_mode)
        get_operator(operator)

        if len(input_paths) == 1:
            worker = build_worker_plugin(self._worker_bootstrap())
            single = worker.compute_on_encrypted_input(input_paths[0], get_operator(operator), self.tee_mode)
            if not single.ok:
                return TeeSparkJobResult(ok=False, operator=operator, reason=single.reason, tee_runtime=tee_runtime_label())
            metrics = [single.metrics]
            return TeeSparkJobResult(
                ok=True,
                operator=operator,
                partition_metrics=metrics,
                aggregated=_aggregate_metrics(metrics),
                tee_runtime=single.tee_runtime or tee_runtime_label(),
            )

        if os.environ.get("SGX_TEE_USE_SPARK", "").lower() in ("1", "true", "yes"):
            metrics = self._run_partitions_spark(input_paths, operator)
            if metrics is not None:
                return TeeSparkJobResult(
                    ok=True,
                    operator=operator,
                    partition_metrics=metrics,
                    aggregated=_aggregate_metrics(metrics),
                    tee_runtime=metrics[0].get("tee_runtime", tee_runtime_label()) if metrics else tee_runtime_label(),
                )

        metrics = self._run_partitions_sequential(input_paths, operator)
        if metrics is not None:
            return TeeSparkJobResult(
                ok=True,
                operator=operator,
                partition_metrics=metrics,
                aggregated=_aggregate_metrics(metrics),
                tee_runtime=tee_runtime_label(),
            )

        metrics = self._run_partitions_spark(input_paths, operator)
        if metrics is None:
            return TeeSparkJobResult(
                ok=False,
                operator=operator,
                reason="spark mapPartitions failed and sequential fallback unavailable",
                tee_runtime=tee_runtime_label(),
            )
        return TeeSparkJobResult(
            ok=True,
            operator=operator,
            partition_metrics=metrics,
            aggregated=_aggregate_metrics(metrics),
            tee_runtime=metrics[0].get("tee_runtime", tee_runtime_label()) if metrics else tee_runtime_label(),
        )

    def _run_partitions_sequential(self, input_paths: list[str], operator: str) -> list[dict[str, Any]] | None:
        """无 Spark/Java 时在 TEE 内顺序执行各分区算子（sim 或 Occlum 单进程）。"""
        worker = build_worker_plugin(self._worker_bootstrap())
        op = get_operator(operator)
        metrics: list[dict[str, Any]] = []
        for hdfs_path in input_paths:
            result = worker.compute_on_encrypted_input(hdfs_path, op, self.tee_mode)
            if not result.ok:
                return None
            metrics.append(result.metrics)
        return metrics

    def _run_partitions_spark(self, input_paths: list[str], operator: str) -> list[dict[str, Any]] | None:
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None

        try:
            spark = (
                SparkSession.builder.appName("sgx-pyspark-tee-compute")
                .master(os.environ.get("SPARK_MASTER", "local[2]"))
                .config("spark.driver.memory", "1g")
                .getOrCreate()
            )
        except Exception:
            return None

        try:
            bootstrap_bc = spark.sparkContext.broadcast(self._worker_bootstrap().to_dict())
            rdd = spark.sparkContext.parallelize(input_paths, max(1, len(input_paths)))
            lines = rdd.mapPartitions(
                lambda part: _tee_map_compute_partition(part, bootstrap_bc.value, operator)
            ).collect()
        except Exception:
            return None
        finally:
            try:
                spark.stop()
            except Exception:
                pass

        metrics: list[dict[str, Any]] = []
        for line in lines:
            row = json.loads(line)
            if not row.get("ok"):
                return None
            metrics.append({k: v for k, v in row.items() if k not in ("ok", "path")})
        return metrics

    def run_compute_write(
        self,
        input_path: str,
        output_path: str,
        column_policies: dict[str, str],
        job_id: str = "tee-compute-write",
    ) -> TeeSparkJobResult:
        require_tee_runtime(self.tee_mode)
        driver = self._require_driver()
        endorsement = self._endorsement
        assert endorsement is not None

        if not driver.is_path_authorized(output_path, endorsement.allowed_root_directories):
            return TeeSparkJobResult(ok=False, operator="normalize_for_output", reason="output path not authorized")

        ticket = driver.dispense_task_ticket(
            job_id,
            "task-partition-000",
            socket.gethostbyname(socket.gethostname()),
            output_path,
        )
        worker = build_worker_plugin(self._worker_bootstrap(for_write=True))
        write_result = worker.compute_and_write_encrypted_output(
            input_path,
            normalize_for_output,
            ticket,
            endorsement,
            output_path,
            column_policies,
            tee_mode=self.tee_mode,
        )
        if not write_result.ok:
            return TeeSparkJobResult(
                ok=False,
                operator="normalize_for_output",
                reason=write_result.reason,
                tee_runtime=tee_runtime_label(),
            )
        return TeeSparkJobResult(
            ok=True,
            operator="normalize_for_output",
            write_results=[
                {
                    "input_path": input_path,
                    "output_path": output_path,
                    "bytes_written": write_result.bytes_written,
                    "monotonic_counter": write_result.monotonic_counter,
                }
            ],
            tee_runtime=tee_runtime_label(),
        )
