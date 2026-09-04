"""TEE 运行时边界检测：计算算子必须在 TEE 内执行。"""

from __future__ import annotations

import os

from sgx_pyspark.tee.occlum_runtime import detect_tee_mode, is_occlum_runtime


class TeeRuntimeError(RuntimeError):
    """计算算子在 TEE 外被调用时抛出。"""


def require_tee_runtime(tee_mode: str | None = None) -> str:
    """
    校验当前进程允许执行 TEE 内计算算子。

    - sim：宿主模拟 TEE，允许执行（开发/无 SGX 硬件）
    - occlum：必须在 Occlum LibOS 内
    """
    mode = tee_mode or detect_tee_mode()
    if mode == "occlum" and not is_occlum_runtime():
        raise TeeRuntimeError(
            "compute operators require Occlum TEE runtime (SGX_PYSPARK_TEE_MODE=occlum); "
            "current process is running outside enclave"
        )
    if mode not in ("sim", "occlum"):
        raise TeeRuntimeError(f"unsupported tee_mode: {mode}")
    return mode


def tee_runtime_label() -> str:
    mode = detect_tee_mode()
    if mode == "occlum" and is_occlum_runtime():
        return "occlum-enclave"
    if mode == "occlum":
        return "occlum-requested-host"
    return "sim"
