"""HDFS 存储后端工厂：sim（本地模拟）/ real（真实 HDFS）。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from sgx_pyspark.config import SgxPysparkConfig
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.hdfs.real_store import RealHdfsStore, RealNameNodeExtension


def hdfs_mode() -> str:
    return os.environ.get("SGX_HDFS_MODE", "sim").lower()


def create_benchmark_stores(
    cfg: SgxPysparkConfig,
    bench_store_name: str = "bench_store",
) -> tuple[Any, Any]:
    """返回 (DataNodeStore|RealHdfsStore, NameNodeExtension|RealNameNodeExtension)。"""
    if hdfs_mode() == "real":
        user = os.environ.get("HADOOP_USER_NAME", "shanlicheng")
        root = cfg.paths.hdfs_data_root.rstrip("/")
        return RealHdfsStore(root, user=user), RealNameNodeExtension(root, user=user)

    data_root = Path(cfg.paths.data_root) / bench_store_name
    return DataNodeStore(data_root), NameNodeExtension(data_root)
