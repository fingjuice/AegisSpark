"""sgx-pyspark 单元测试。"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pytest

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.config import load_config
from sgx_pyspark.crypto.abe import ABECrypto, PolicyEngine
from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto, build_admin_endorsement_payload, build_task_ticket_payload
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.path_util import is_under_any_root, ticket_binds_target
from sgx_pyspark.pipeline.worker_read import WorkerReadPipeline
from sgx_pyspark.pipeline.worker_write import WorkerWritePipeline
from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin
from sgx_pyspark.types import AdminEndorsement, TaskTicket


def test_policy_engine():
    attrs = {"Department:Developer"}
    assert PolicyEngine.evaluate("(Department:Developer) OR (Department:HR)", attrs)
    assert not PolicyEngine.evaluate("Department:HR", attrs)


def test_path_util():
    root = "hdfs://namenode:9000/data/output/"
    assert is_under_any_root("hdfs://namenode:9000/data/output/part-001.csv", [root])
    assert ticket_binds_target(
        "hdfs://namenode:9000/data/output/part-001.csv",
        "hdfs://namenode:9000/data/output/part-001.csv",
    )
    assert ticket_binds_target(
        "hdfs://namenode:9000/data/output/",
        "hdfs://namenode:9000/data/output/part-001.csv",
    )


def test_read_pipeline_masks_salary(tmp_path: Path):
    keys = tmp_path / "keys"
    keys.mkdir()
    ecdsa = ECDSACrypto()
    admin_pub = keys / "admin.pub"
    driver_pub = keys / "driver.pub"
    ecdsa.generate_keypair(keys / "admin.pem", admin_pub)
    ecdsa.generate_keypair(keys / "driver.pem", driver_pub)

    abe = ABECrypto()
    admin = Kgc(abe=abe)
    usk = admin.issue_user_attributes("dev", ["Department:Developer"])

    data = tmp_path / "data"
    nne = NameNodeExtension(data)
    dn = DataNodeStore(data)
    hdfs_path = "hdfs://namenode:9000/data/salary.csv.enc"
    columns = {"Name": b"Alice\x00\x00\x00\x00\x00", "Salary": b"99999\x00\x00\x00\x00"}
    policies = {"Name": "Department:Developer", "Salary": "Department:HR"}
    header, layout, ct = WorkerWritePipeline(abe).encrypt_columns(hdfs_path, columns, policies)
    dn.write_block(hdfs_path, ct)
    nne.persist_meta(hdfs_path, header, layout)

    wv = WriteVerificationTee(admin_pub, driver_pub, dn)
    worker = SparkWorkerTeePlugin(abe, usk, nne, dn, wv)
    result = worker.read_encrypted_file(hdfs_path)
    assert result.ok
    assert result.columns_authorized == 1
    assert result.columns_masked == 1
    assert b"NULL" in result.plaintext[6:]


def test_write_trust_chain(tmp_path: Path):
    keys = tmp_path / "keys"
    keys.mkdir()
    ecdsa = ECDSACrypto()
    admin_priv, admin_pub = keys / "admin.pem", keys / "admin.pub"
    driver_priv, driver_pub = keys / "driver.pem", keys / "driver.pub"
    ecdsa.generate_keypair(admin_priv, admin_pub)
    ecdsa.generate_keypair(driver_priv, driver_pub)

    abe = ABECrypto()
    admin = Kgc(abe=abe, admin_private_key_path=admin_priv)
    endorsement = admin.endorse_spark_job(b"spark-app", ["hdfs://namenode:9000/data/output/"], admin_priv)

    driver = SparkDriverTeePlugin(driver_priv)
    driver.initialize_job("tee://write-verification:9000", endorsement)
    target = "hdfs://namenode:9000/data/output/part-001.csv.enc"
    ticket = driver.dispense_task_ticket("job-1", "task-1", "127.0.0.1", target)

    dn = DataNodeStore(tmp_path)
    wv = WriteVerificationTee(admin_pub, driver_pub, dn)
    result = wv.handle_optimized_write(ticket, endorsement, target, b"cipher")
    assert result.ok

    # 伪造 ticket 应被拒绝
    bad_ticket = TaskTicket(
        job_id="job-1",
        task_id="task-1",
        worker_ip="127.0.0.1",
        allowed_target_path="hdfs://namenode:9000/data/evil/out.csv",
        expires_at=int(time.time()) + 3600,
        driver_signature=ticket.driver_signature,
    )
    bad = wv.verify_write_chain(bad_ticket, endorsement, target)
    assert not bad.ok
