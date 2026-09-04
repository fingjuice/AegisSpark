"""配置加载与环境变量展开。"""

from __future__ import annotations

import os
import re
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        return os.environ.get(match.group(1), "")

    prev = None
    cur = value
    while prev != cur:
        prev = cur
        cur = _ENV_PATTERN.sub(repl, cur)
    return cur


@dataclass
class PathsConfig:
    sgx_pyspark_root: str
    data_root: str
    keys_dir: str
    hdfs_data_root: str


@dataclass
class SignatureConfig:
    admin_private_key_path: str
    admin_public_key_path: str
    driver_private_key_path: str
    driver_public_key_path: str


@dataclass
class TeeConfig:
    libos: str
    tee_mode: str
    write_verification_url: str
    monotonic_counter_namespace: str
    occlum_instance_dir: str


@dataclass
class SparkConfig:
    user_id: str
    user_attributes: list[str]
    task_ticket_ttl_sec: int


@dataclass
class NativeConfig:
    ffi_library_dir: str
    mcl_lib_dir: str


@dataclass
class SgxPysparkConfig:
    config_path: Path
    paths: PathsConfig
    signature: SignatureConfig
    tee: TeeConfig
    spark: SparkConfig
    native: NativeConfig


def default_config_path() -> Path:
    env = os.environ.get("SGX_PYSPARK_CONFIG")
    if env:
        return Path(env)
    root = Path(os.environ.get("SGX_PYSPARK_ROOT", Path(__file__).resolve().parents[1]))
    return root / "conf" / "sgx-pyspark.conf"


def load_config(path: Path | None = None) -> SgxPysparkConfig:
    cfg_path = path or default_config_path()
    root = cfg_path.resolve().parents[1]
    benchmark_root = root.parent
    os.environ.setdefault("SGX_PYSPARK_ROOT", str(root))
    os.environ.setdefault("ABE_SPARK_ROOT", str(benchmark_root / "ABE-Spark"))
    os.environ.setdefault("MCL_ROOT", "/home/shanlicheng/mcl")

    parser = ConfigParser()
    parser.read(cfg_path)

    def get(section: str, key: str, default: str = "") -> str:
        return _expand(parser.get(section, key, fallback=default))

    attrs = [a.strip() for a in get("spark", "user_attributes", "").split(",") if a.strip()]
    return SgxPysparkConfig(
        config_path=cfg_path,
        paths=PathsConfig(
            sgx_pyspark_root=get("paths", "sgx_pyspark_root", str(root)),
            data_root=get("paths", "data_dir", str(root / "data")),
            keys_dir=get("paths", "keys_dir", str(root / "conf" / "keys")),
            hdfs_data_root=get("paths", "hdfs_data_root", "hdfs://localhost:8020/sgx-pyspark/data"),
        ),
        signature=SignatureConfig(
            admin_private_key_path=get("signature", "admin_private_key_path"),
            admin_public_key_path=get("signature", "admin_public_key_path"),
            driver_private_key_path=get("signature", "driver_private_key_path"),
            driver_public_key_path=get("signature", "driver_public_key_path"),
        ),
        tee=TeeConfig(
            libos=get("tee", "libos", "occlum"),
            tee_mode=get("tee", "tee_mode", "sim"),
            write_verification_url=get("tee", "write_verification_url", "tee://write-verification:9000"),
            monotonic_counter_namespace=get("tee", "monotonic_counter_namespace", "sgx-pyspark-wv"),
            occlum_instance_dir=get("tee", "occlum_instance_dir", str(root / "occlum" / "instance")),
        ),
        spark=SparkConfig(
            user_id=get("spark", "user_id", "analyst_user"),
            user_attributes=attrs,
            task_ticket_ttl_sec=int(get("spark", "task_ticket_ttl_sec", "3600")),
        ),
        native=NativeConfig(
            ffi_library_dir=get("native", "ffi_library_dir", str(root / "native" / "build")),
            mcl_lib_dir=get("native", "mcl_lib_dir", f"{os.environ.get('MCL_ROOT', '/home/shanlicheng/mcl')}/build-eac/lib"),
        ),
    )
