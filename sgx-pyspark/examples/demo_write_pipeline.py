#!/usr/bin/env python3
"""design.md 写管道演示：Admin 核准 → Driver 证明 → Task Ticket → Write Verification。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.config import load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin


def main() -> None:
    cfg = load_config()
    data_root = Path(cfg.paths.data_root)
    admin = Kgc(abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)))

    # 确保密钥存在
    admin.ensure_keypair(cfg.signature.admin_private_key_path, cfg.signature.admin_public_key_path)
    admin.ensure_keypair(cfg.signature.driver_private_key_path, cfg.signature.driver_public_key_path)

    spark_script = (ROOT / "examples" / "pyspark_salary_job.py").read_bytes()
    allowed_dirs = ["hdfs://namenode:9000/data/output/"]

    # 1. Admin 签署 Code Hash 与初始写凭证
    endorsement = admin.endorse_spark_job(spark_script, allowed_dirs, cfg.signature.admin_private_key_path)
    print(f"Admin endorsed app hash: {endorsement.app_code_hash[:16]}...")

    # 2. Driver TEE 远程证明并获取 wsk
    driver = SparkDriverTeePlugin(cfg.signature.driver_private_key_path, task_ticket_ttl_sec=cfg.spark.task_ticket_ttl_sec)
    attest = driver.initialize_job(cfg.tee.write_verification_url, endorsement)
    assert attest.ok, attest.reason
    print(f"Driver attestation OK, session={attest.session_token[:16]}...")

    # 3. Driver 向 Worker 分发 Task Ticket
    target = "hdfs://namenode:9000/data/output/part-001.csv.enc"
    ticket = driver.dispense_task_ticket("spark-job-20260618", "task-partition-001", "10.0.0.52", target)
    print(f"Task ticket issued for {ticket.allowed_target_path}")

    # 4-6. Worker 计算并提交 Write Verification 请求
    nne = NameNodeExtension(data_root)
    dn = DataNodeStore(data_root)
    wv = WriteVerificationTee(
        admin_public_key_path=cfg.signature.admin_public_key_path,
        driver_public_key_path=cfg.signature.driver_public_key_path,
        datanode=dn,
        counter_namespace=cfg.tee.monotonic_counter_namespace,
    )
    usk = admin.issue_user_attributes("writer", ["Department:HR"])
    worker = SparkWorkerTeePlugin(admin.abe, usk, nne, dn, wv)

    output_columns = {
        "Name": b"Bob Chen\x00\x00",
        "Dept": b"HR\x00\x00\x00\x00\x00",
        "Salary": b"92000\x00\x00\x00",
    }
    policies = {
        "Name": "(Department:Developer) OR (Department:HR)",
        "Dept": "(Department:Developer) OR (Department:HR)",
        "Salary": "Department:HR",
    }

    write_result = worker.write_encrypted_output(ticket, endorsement, target, output_columns, policies)
    assert write_result.ok, write_result.reason
    print(f"EAC write OK: {write_result.bytes_written} bytes, counter={write_result.monotonic_counter}")

    # 验证落盘
    assert dn.read_block(target)
    assert nne.read_security_header(target).file_path == target
    print("Write pipeline trust chain verified end-to-end.")


if __name__ == "__main__":
    main()
