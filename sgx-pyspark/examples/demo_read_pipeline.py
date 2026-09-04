#!/usr/bin/env python3
"""design.md 读管道演示：Developer 用户读取 salary.csv，Salary 列脱敏为 NULL。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.config import load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.pipeline.worker_write import WorkerWritePipeline
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin
from sgx_pyspark.tee.write_verification import WriteVerificationTee


def main() -> None:
    cfg = load_config()
    data_root = Path(cfg.paths.data_root)
    admin = Kgc(abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)))

    # 1. Admin 离线签发 Developer 属性私钥
    usk = admin.issue_user_attributes("dev_user", ["Department:Developer"])

    hdfs_path = "hdfs://namenode:9000/data/salary.csv.enc"
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

    # 预置加密数据（模拟 DataNode 存储）
    nne = NameNodeExtension(data_root)
    dn = DataNodeStore(data_root)
    writer = WorkerWritePipeline(admin.abe)
    header, layout, ciphertext = writer.encrypt_columns(hdfs_path, columns, policies)
    dn.write_block(hdfs_path, ciphertext)
    nne.persist_meta(hdfs_path, header, layout)

    # 2. Worker TEE 读管道
    wv = WriteVerificationTee(
        admin_public_key_path=cfg.signature.admin_public_key_path,
        driver_public_key_path=cfg.signature.driver_public_key_path,
        datanode=dn,
    )
    worker = SparkWorkerTeePlugin(admin.abe, usk, nne, dn, wv)
    result = worker.read_encrypted_file(hdfs_path)

    assert result.ok, result.reason
    assert result.columns_authorized == 2
    assert result.columns_masked == 1

    text = result.plaintext.decode("utf-8", errors="replace")
    print("Decrypted (TEE memory):")
    print(f"  Name   = {text[0:10].strip()}")
    print(f"  Dept   = {text[10:20].strip()}")
    print(f"  Salary = {text[20:30].strip()}  (masked -> NULL)")
    print(f"authorized={result.columns_authorized}, masked={result.columns_masked}")


if __name__ == "__main__":
    main()
