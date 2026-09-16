#!/usr/bin/env python3
"""多分区 PySpark TEE 计算演示：mapPartitions 内在 TEE 解密并聚合。"""

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


def seed_partitioned_data(cfg, admin: Kgc) -> list[str]:
    data_root = Path(cfg.paths.data_root)
    nne = NameNodeExtension(data_root)
    dn = DataNodeStore(data_root)
    writer = WorkerWritePipeline(admin.abe)
    col_width = 10
    rows = [
        ("Alice Wang", "Developer", "85000"),
        ("Bob Chen", "HR", "92000"),
        ("Carol Li", "Developer", "78000"),
    ]
    policies = {
        "Name": "(Department:Developer) OR (Department:HR)",
        "Dept": "(Department:Developer) OR (Department:HR)",
        "Salary": "Department:HR",
    }
    paths: list[str] = []
    for idx, (name, dept, salary) in enumerate(rows):
        hdfs_path = f"hdfs://namenode:9000/data/partition-{idx:03d}.csv.enc"
        columns = {
            "Name": name.ljust(col_width, "\x00").encode("utf-8"),
            "Dept": dept.ljust(col_width, "\x00").encode("utf-8"),
            "Salary": salary.ljust(col_width, "\x00").encode("utf-8"),
        }
        header, layout, ciphertext = writer.encrypt_columns(hdfs_path, columns, policies)
        dn.write_block(hdfs_path, ciphertext)
        nne.persist_meta(hdfs_path, header, layout)
        paths.append(hdfs_path)
    return paths


def main() -> None:
    cfg = load_config()
    print(f"[sgx-pyspark] multi-partition TEE compute, runtime={tee_runtime_label()}")

    admin = Kgc(abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)))
    input_paths = seed_partitioned_data(cfg, admin)

    job = TeeSparkJob(
        cfg,
        user_attributes=["Department:Developer"],
    )
    job.initialize_driver(
        spark_app_bytes=(ROOT / "examples" / "pyspark_tee_compute_job.py").read_bytes(),
    )
    result = job.run_read_compute(input_paths, operator="dept_stats")
    assert result.ok, result.reason

    print(json.dumps(
        {
            "partitions": len(input_paths),
            "tee_runtime": result.tee_runtime,
            "aggregated": result.aggregated,
            "partition_metrics": result.partition_metrics,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
