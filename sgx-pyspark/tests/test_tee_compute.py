"""TEE 内计算算子单元测试。"""

from __future__ import annotations

from pathlib import Path

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.compute.operators import dept_stats, get_operator, normalize_for_output
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.ecdsa_sign import ECDSACrypto
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.pipeline.worker_write import WorkerWritePipeline
from sgx_pyspark.spark.tee_job import TeeSparkJob
from sgx_pyspark.tee.compute_context import TeeComputeContext
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.tee.runtime_guard import require_tee_runtime
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin


def _setup_encrypted_row(tmp_path: Path, attrs: list[str]):
    keys = tmp_path / "keys"
    keys.mkdir()
    ecdsa = ECDSACrypto()
    admin_priv, admin_pub = keys / "admin.pem", keys / "admin.pub"
    driver_priv, driver_pub = keys / "driver.pem", keys / "driver.pub"
    ecdsa.generate_keypair(admin_priv, admin_pub)
    ecdsa.generate_keypair(driver_priv, driver_pub)

    abe = ABECrypto()
    admin = Kgc(abe=abe, admin_private_key_path=admin_priv)
    usk = admin.issue_user_attributes("dev", attrs)

    nne = NameNodeExtension(tmp_path)
    dn = DataNodeStore(tmp_path)
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
    header, layout, ct = WorkerWritePipeline(abe).encrypt_columns(hdfs_path, columns, policies)
    dn.write_block(hdfs_path, ct)
    nne.persist_meta(hdfs_path, header, layout)

    wv = WriteVerificationTee(admin_pub, driver_pub, dn)
    worker = SparkWorkerTeePlugin(abe, usk, nne, dn, wv)
    return admin, worker, hdfs_path, policies, admin_priv, driver_priv


def test_compute_context_no_plaintext_export(tmp_path: Path):
    _, worker, hdfs_path, _, _, _ = _setup_encrypted_row(tmp_path, ["Department:Developer"])
    ctx = worker._read_to_context(hdfs_path)
    assert not hasattr(ctx, "export_plaintext")
    assert ctx.get_column("Name") == "Alice Wang"
    assert ctx.is_authorized("Salary") is False


def test_compute_on_encrypted_input_in_sim(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SGX_PYSPARK_TEE_MODE", "sim")
    _, worker, hdfs_path, _, _, _ = _setup_encrypted_row(tmp_path, ["Department:Developer"])
    result = worker.compute_on_encrypted_input(hdfs_path, dept_stats, tee_mode="sim")
    assert result.ok
    assert result.metrics["dept"] == "Developer"
    assert result.metrics["salary_masked"] is True
    assert "Alice" not in str(result.metrics)


def test_compute_and_write_in_tee(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SGX_PYSPARK_TEE_MODE", "sim")
    admin, worker, hdfs_path, policies, admin_priv, driver_priv = _setup_encrypted_row(
        tmp_path, ["Department:HR"]
    )
    from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin

    endorsement = admin.endorse_spark_job(b"app", ["hdfs://namenode:9000/data/output/"], admin_priv)
    driver = SparkDriverTeePlugin(driver_priv)
    driver.initialize_job("tee://write-verification:9000", endorsement)
    target = "hdfs://namenode:9000/data/output/part-computed.csv.enc"
    ticket = driver.dispense_task_ticket("job-tee", "task-0", "127.0.0.1", target)

    write_result = worker.compute_and_write_encrypted_output(
        hdfs_path,
        normalize_for_output,
        ticket,
        endorsement,
        target,
        policies,
        tee_mode="sim",
    )
    assert write_result.ok

    read_back = worker.read_encrypted_file(target)
    assert read_back.ok
    text = read_back.plaintext.decode("utf-8", errors="replace")
    assert "ALICE WANG" in text
    assert "85000+" in text


def test_tee_spark_job_single_partition(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SGX_PYSPARK_TEE_MODE", "sim")
    monkeypatch.setenv("SGX_PYSPARK_ROOT", str(tmp_path))
    monkeypatch.setenv("SGX_PYSPARK_CONFIG", "")

    admin, _, hdfs_path, _, admin_priv, driver_priv = _setup_encrypted_row(tmp_path, ["Department:Developer"])

    from sgx_pyspark.config import SgxPysparkConfig, PathsConfig, SignatureConfig, TeeConfig, SparkConfig, NativeConfig

    cfg = SgxPysparkConfig(
        config_path=tmp_path / "conf.conf",
        paths=PathsConfig(str(tmp_path), str(tmp_path), str(tmp_path / "keys"), "hdfs://x"),
        signature=SignatureConfig(
            str(admin_priv), str(tmp_path / "keys" / "admin.pub"),
            str(driver_priv), str(tmp_path / "keys" / "driver.pub"),
        ),
        tee=TeeConfig("occlum", "sim", "tee://write-verification", "ns", str(tmp_path / "occlum")),
        spark=SparkConfig("dev", ["Department:Developer"], 3600),
        native=NativeConfig(str(tmp_path / "native"), str(tmp_path / "mcl")),
    )

    job = TeeSparkJob(cfg)
    job.initialize_driver(b"test-app")
    result = job.run_read_compute([hdfs_path], operator="dept_stats")
    assert result.ok
    assert result.aggregated["total_records"] == 1


def test_require_tee_runtime_occlum_outside_enclave(monkeypatch):
    monkeypatch.setenv("SGX_PYSPARK_TEE_MODE", "occlum")
    monkeypatch.delenv("OCCLUM", raising=False)
    try:
        require_tee_runtime("occlum")
        raised = False
    except RuntimeError:
        raised = True
    assert raised


def test_operator_registry():
    op = get_operator("aggregate_count")
    assert callable(op)
