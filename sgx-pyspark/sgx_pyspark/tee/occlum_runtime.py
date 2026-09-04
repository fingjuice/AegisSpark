"""Occlum LibOS TEE 运行时检测与封装。"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from sgx_pyspark.config import load_config


def is_occlum_runtime() -> bool:
    """检测当前进程是否运行在 Occlum LibOS 内。"""
    return (
        os.environ.get("OCCLUM", "") == "1"
        or os.environ.get("OCCLUM_VERSION", "") != ""
        or Path("/etc/occlum").exists()
        or Path("/opt/occlum").exists()
    )


def detect_tee_mode() -> str:
    """返回当前 TEE 模式：occlum / sim。"""
    cfg = load_config()
    env_mode = os.environ.get("SGX_PYSPARK_TEE_MODE", "")
    if env_mode:
        return env_mode
    if cfg.tee.libos == "occlum" and is_occlum_runtime():
        return "occlum"
    return cfg.tee.tee_mode


def setup_occlum_env() -> dict[str, str]:
    """设置 Occlum 内运行所需的环境变量。"""
    cfg = load_config()
    env = os.environ.copy()
    env["OCCLUM"] = "1"
    env.setdefault("SGX_PYSPARK_ROOT", cfg.paths.sgx_pyspark_root)
    env["LD_LIBRARY_PATH"] = ":".join([
        cfg.native.ffi_library_dir,
        cfg.native.mcl_lib_dir,
        env.get("LD_LIBRARY_PATH", ""),
    ])
    env.setdefault("PYTHONPATH", cfg.paths.sgx_pyspark_root)
    return env


def run_in_occlum(command: str, instance_dir: Path | None = None) -> int:
    """在 Occlum 实例内执行命令。"""
    cfg = load_config()
    inst = instance_dir or Path(cfg.tee.occlum_instance_dir)
    if not (inst / "Occlum.json").exists():
        raise FileNotFoundError(f"Occlum instance not initialized: {inst}")
    result = subprocess.run(
        ["occlum", "run", "/bin/bash", "-c", command],
        cwd=str(inst),
        env=setup_occlum_env(),
        check=False,
    )
    return result.returncode
