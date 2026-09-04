"""ctypes 绑定 ABE-Spark native FFI（mcl CP-ABE 生产级密码学）。"""

from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
from typing import Any

from .config import SgxPysparkConfig, load_config


class NativeBridge:
    """Python 侧 native 桥接层，可选用于替代纯 Python 密码学后端。"""

    def __init__(self, config: SgxPysparkConfig | None = None) -> None:
        self.config = config or load_config()
        self._lib = self._load_library()
        self._bind_symbols()
        self._initialized = False

    def _load_library(self) -> ctypes.CDLL:
        lib_dir = Path(self.config.native.ffi_library_dir)
        mcl_dir = Path(self.config.native.mcl_lib_dir)
        prev = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{lib_dir}:{mcl_dir}:{prev}"
        lib_path = lib_dir / "libsgx_pyspark_ffi.so"
        if not lib_path.exists():
            raise FileNotFoundError(f"native library not found: {lib_path}")
        return ctypes.CDLL(str(lib_path))

    def _bind_symbols(self) -> None:
        lib = self._lib
        lib.abe_spark_init.argtypes = [ctypes.c_char_p]
        lib.abe_spark_init.restype = ctypes.c_int
        lib.abe_spark_set_user_context.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        lib.abe_spark_set_user_context.restype = ctypes.c_int
        lib.abe_spark_free.argtypes = [ctypes.c_void_p]
        lib.abe_spark_last_error.argtypes = []
        lib.abe_spark_last_error.restype = ctypes.c_char_p
        lib.abe_spark_endorse_spark_job.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int64]
        lib.abe_spark_endorse_spark_job.restype = ctypes.c_char_p
        lib.abe_spark_derive_task_ticket.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int64
        ]
        lib.abe_spark_derive_task_ticket.restype = ctypes.c_char_p
        lib.abe_spark_verify_write_chain.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int64
        ]
        lib.abe_spark_verify_write_chain.restype = ctypes.c_char_p
        lib.abe_spark_proxy_write.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t, ctypes.c_int64, ctypes.c_char_p,
        ]
        lib.abe_spark_proxy_write.restype = ctypes.c_char_p
        lib.abe_spark_process_worker_read.argtypes = [
            ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t, ctypes.c_char_p, ctypes.c_char_p
        ]
        lib.abe_spark_process_worker_read.restype = ctypes.c_char_p

    def last_error(self) -> str:
        err = self._lib.abe_spark_last_error()
        return err.decode("utf-8") if err else ""

    def _take(self, ptr: ctypes.c_char_p) -> str:
        if not ptr:
            raise RuntimeError(self.last_error() or "native call failed")
        try:
            return ptr.decode("utf-8")
        finally:
            self._lib.abe_spark_free(ptr)

    def init(self) -> None:
        ok = self._lib.abe_spark_init(str(self.config.config_path).encode("utf-8"))
        if not ok:
            raise RuntimeError(f"abe_spark_init failed: {self.last_error()}")
        self._initialized = True

    def ensure_init(self) -> None:
        if not self._initialized:
            self.init()

    def set_user_context(self, user_id: str, attributes: list[str]) -> None:
        self.ensure_init()
        ok = self._lib.abe_spark_set_user_context(
            user_id.encode("utf-8"), json.dumps(attributes).encode("utf-8")
        )
        if not ok:
            raise RuntimeError(self.last_error())

    def process_worker_read(self, encrypted: bytes, header_json: str, layout_json: str) -> dict[str, Any]:
        self.ensure_init()
        buf = (ctypes.c_ubyte * len(encrypted)).from_buffer_copy(encrypted)
        raw = self._take(
            self._lib.abe_spark_process_worker_read(
                buf, ctypes.c_size_t(len(encrypted)),
                header_json.encode("utf-8"), layout_json.encode("utf-8"),
            )
        )
        return json.loads(raw)


_bridge: NativeBridge | None = None


def get_bridge() -> NativeBridge:
    global _bridge
    if _bridge is None:
        _bridge = NativeBridge()
    return _bridge


def native_available() -> bool:
    try:
        cfg = load_config()
        return (Path(cfg.native.ffi_library_dir) / "libsgx_pyspark_ffi.so").is_file()
    except OSError:
        return False
