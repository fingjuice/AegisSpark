"""基准测试冒烟测试（小数据量）。"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest


@pytest.fixture
def tiny_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SGX_EXP_TARGET_GB", "0.002")
    monkeypatch.setenv("SGX_EXP_CHECKPOINT_GB", "0.001")
    monkeypatch.setenv("SGX_EXP_WORKERS", "2")
    monkeypatch.setenv("SGX_PYSPARK_ROOT", str(tmp_path))
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setenv("SGX_PYSPARK_CONFIG", str(root / "conf" / "sgx-pyspark.conf"))
    # 复制密钥
    import shutil

    keys_src = root / "conf" / "keys"
    keys_dst = tmp_path / "conf" / "keys"
    if keys_src.is_dir():
        shutil.copytree(keys_src, keys_dst)
    else:
        from sgx_pyspark.crypto.ed25519_sign import Ed25519Crypto

        keys_dst.mkdir(parents=True)
        crypto = Ed25519Crypto()
        crypto.generate_keypair(keys_dst / "admin_ed25519.pem", keys_dst / "admin_ed25519.pub")
        crypto.generate_keypair(keys_dst / "driver_tee_ed25519.pem", keys_dst / "driver_tee_ed25519.pub")
        crypto.generate_keypair(
            keys_dst / "write_verification_ed25519.pem",
            keys_dst / "write_verification_ed25519.pub",
        )
    return tmp_path


def test_benchmark_smoke(tiny_env):
    from sgx_pyspark.benchmark.run_experiment import main

    result_dir = tiny_env / "Experimental-Result"
    rc = main(["all", "--result-dir", str(result_dir)])
    assert rc == 0
    assert (result_dir / "write_events.csv").is_file()
    assert (result_dir / "write_checkpoints.csv").is_file()
    assert (result_dir / "read_events.csv").is_file()
    assert (result_dir / "read_checkpoints.csv").is_file()

    with (result_dir / "write_events.csv").open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert rows
        assert "eac_tee_ms" in rows[0]
        assert "hdfs_write_ms" in rows[0]
        assert "ac_to_persist_ms" in rows[0]

    with (result_dir / "read_events.csv").open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert rows
        assert "abe_tee_ms" in rows[0]
        assert "ac_to_decrypt_ms" in rows[0]
