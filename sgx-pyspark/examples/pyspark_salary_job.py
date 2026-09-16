#!/usr/bin/env python3
"""PySpark 作业：TEE 内解密 → 算子计算 → 仅输出聚合指标（明文不离开 TEE）。"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.config import load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.pipeline.worker_write import WorkerWritePipeline
from sgx_pyspark.spark.tee_job import TeeSparkJob
from sgx_pyspark.tee.runtime_guard import tee_runtime_label


def ensure_sample_data(cfg, admin: Kgc) -> str:
    data_root = Path(cfg.paths.data_root)
    hdfs_path = "hdfs://namenode:9000/data/salary.csv.enc"
    enc_file = data_root / "data/salary.csv.enc"
    meta_file = data_root / "data/salary.csv.enc.meta.json"
    marker = data_root / ".msk_generation.marker"

    current_msk = admin.abe.msk
    if marker.is_file() and marker.read_bytes() != current_msk:
        if enc_file.exists():
            enc_file.unlink()
        if meta_file.exists():
            meta_file.unlink()
    marker.write_bytes(current_msk)

    if enc_file.exists():
        return hdfs_path

    nne = NameNodeExtension(data_root)
    dn = DataNodeStore(data_root)
    writer = WorkerWritePipeline(admin.abe)
    col_width = 10
    columns = {
        "Name": b"Alice Wang".ljust(col_width, b"\x00"),
        "Dept": b"Developer".ljust(col_width, b"\x00"),
        "Salary": b"85000".ljust(col_width, b"\x00"),
    }
    policies = {
        "Name": "(Department:Developer) OR (Department:HR)",
        "Dept": "(Department:Developer) OR (Department:HR)",
        "Salary": "Department:HR",
    }
    header, layout, ciphertext = writer.encrypt_columns(hdfs_path, columns, policies)
    dn.write_block(hdfs_path, ciphertext)
    nne.persist_meta(hdfs_path, header, layout)
    return hdfs_path


def main() -> None:
    cfg = load_config()
    tee_mode = os.environ.get("SGX_PYSPARK_TEE_MODE", cfg.tee.tee_mode)
    print(f"[sgx-pyspark] TEE mode: {tee_mode}, runtime: {tee_runtime_label()}")

    admin = Kgc(abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)))
    hdfs_path = ensure_sample_data(cfg, admin)

    job = TeeSparkJob(
        cfg,
        user_attributes=["Department:Developer"],
        write_user_attributes=["Department:HR"],
    )
    job.initialize_driver(
        spark_app_bytes=(ROOT / "examples" / "pyspark_salary_job.py").read_bytes(),
        allowed_dirs=["hdfs://namenode:9000/data/output/"],
    )

    operator = os.environ.get("SGX_TEE_OPERATOR", "dept_stats")
    result = job.run_read_compute([hdfs_path], operator=operator)
    assert result.ok, result.reason

    print(f"TEE compute operator: {result.operator}")
    print(f"TEE runtime: {result.tee_runtime}")
    print(f"Partition metrics: {json.dumps(result.partition_metrics, ensure_ascii=False)}")
    print(f"Aggregated (safe to leave TEE): {json.dumps(result.aggregated, ensure_ascii=False)}")

    # 写路径：TEE 内变换后加密落盘
    output_path = "hdfs://namenode:9000/data/output/salary-computed.csv.enc"
    policies = {
        "Name": "(Department:Developer) OR (Department:HR)",
        "Dept": "(Department:Developer) OR (Department:HR)",
        "Salary": "Department:HR",
    }
    write_result = job.run_compute_write(hdfs_path, output_path, policies)
    assert write_result.ok, write_result.reason
    print(f"TEE compute-write: {json.dumps(write_result.write_results, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
