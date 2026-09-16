"""Executor 侧 Worker 插件引导：供 Spark mapPartitions 在 TEE 内重建信任链组件。"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.tee.worker_plugin import SparkWorkerTeePlugin


@dataclass
class WorkerBootstrapConfig:
    data_root: str
    keys_dir: str
    user_id: str
    user_attributes: list[str]
    admin_public_key_path: str
    driver_public_key_path: str
    counter_namespace: str
    tee_mode: str = "sim"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> WorkerBootstrapConfig:
        return cls(**raw)


def build_worker_plugin(cfg: WorkerBootstrapConfig) -> SparkWorkerTeePlugin:
    abe = ABECrypto(msk=load_or_create_msk(cfg.keys_dir))
    admin = Kgc(abe=abe)
    usk = admin.issue_user_attributes(cfg.user_id, cfg.user_attributes)
    nne = NameNodeExtension(cfg.data_root)
    dn = DataNodeStore(cfg.data_root)
    wv = WriteVerificationTee(
        admin_public_key_path=cfg.admin_public_key_path,
        driver_public_key_path=cfg.driver_public_key_path,
        datanode=dn,
        counter_namespace=cfg.counter_namespace,
    )
    return SparkWorkerTeePlugin(abe, usk, nne, dn, wv)
