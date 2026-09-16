"""libhdfs 原生客户端（ctypes），用于真实 HDFS 高性能读写。"""

from __future__ import annotations

import ctypes
import os
import threading
from pathlib import Path

from sgx_pyspark.hdfs.hdfs_cli import hdfs_path_from_uri

O_RDONLY = 0
O_WRONLY = 1
O_CREAT = 64


def _ensure_hadoop_env() -> Path:
    hadoop_home = Path(os.environ.get("HADOOP_HOME", "/home/shanlicheng/opt/hadoop"))
    native_dir = hadoop_home / "lib" / "native"
    os.environ.setdefault("HADOOP_CONF_DIR", str(hadoop_home / "etc" / "hadoop"))
    ld = os.environ.get("LD_LIBRARY_PATH", "")
    if str(native_dir) not in ld.split(":"):
        os.environ["LD_LIBRARY_PATH"] = f"{native_dir}:{ld}" if ld else str(native_dir)
    if "CLASSPATH" not in os.environ:
        import subprocess

        cp = subprocess.check_output([str(hadoop_home / "bin" / "hadoop"), "classpath"], text=True).strip()
        os.environ["CLASSPATH"] = cp
    return hadoop_home


def _parse_namenode(hdfs_uri: str) -> tuple[str, int]:
    if hdfs_uri.startswith("hdfs://"):
        rest = hdfs_uri[7:]
        slash = rest.find("/")
        hostport = rest[:slash] if slash >= 0 else rest
        if ":" in hostport:
            host, port = hostport.split(":", 1)
            return host, int(port)
        return hostport, 9000
    return "10.26.40.83", 9000


class LibHdfsClient:
    _lib: ctypes.CDLL | None = None
    _lib_lock = threading.Lock()

    def __init__(self, namenode_uri: str, user: str | None = None) -> None:
        hadoop_home = _ensure_hadoop_env()
        self.host, self.port = _parse_namenode(namenode_uri)
        self.user = (user or os.environ.get("HADOOP_USER_NAME", "shanlicheng")).encode()
        self._fs: ctypes.c_void_p | None = None
        self._conn_lock = threading.Lock()
        self._load_lib(hadoop_home)

    @classmethod
    def _load_lib(cls, hadoop_home: Path) -> None:
        with cls._lib_lock:
            if cls._lib is not None:
                return
            lib_path = hadoop_home / "lib" / "native" / "libhdfs.so"
            lib = ctypes.CDLL(str(lib_path))
            lib.hdfsConnectAsUser.restype = ctypes.c_void_p
            lib.hdfsConnectAsUser.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p]
            lib.hdfsDisconnect.argtypes = [ctypes.c_void_p]
            lib.hdfsCreateDirectory.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            lib.hdfsExists.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
            lib.hdfsOpenFile.restype = ctypes.c_void_p
            lib.hdfsOpenFile.argtypes = [
                ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int,
                ctypes.c_int, ctypes.c_int, ctypes.c_int,
            ]
            lib.hdfsWrite.restype = ctypes.c_int
            lib.hdfsWrite.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
            lib.hdfsRead.restype = ctypes.c_int
            lib.hdfsRead.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
            lib.hdfsCloseFile.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
            cls._lib = lib

    def _connect(self) -> ctypes.c_void_p:
        if self._fs:
            return self._fs
        with self._conn_lock:
            if self._fs:
                return self._fs
            fs = self._lib.hdfsConnectAsUser(self.host.encode(), self.port, self.user)
            if not fs:
                raise OSError(f"hdfsConnectAsUser failed: {self.host}:{self.port}")
            self._fs = fs
            return fs

    def mkdirs(self, hdfs_path: str) -> None:
        fs = self._connect()
        hpath = hdfs_path_from_uri(hdfs_path).encode()
        self._lib.hdfsCreateDirectory(fs, hpath)

    def exists(self, hdfs_path: str) -> bool:
        fs = self._connect()
        return self._lib.hdfsExists(fs, hdfs_path_from_uri(hdfs_path).encode()) == 0

    def write_bytes(self, hdfs_path: str, data: bytes) -> int:
        fs = self._connect()
        hpath = hdfs_path_from_uri(hdfs_path)
        parent = str(Path(hpath).parent)
        if parent and parent != "/":
            self.mkdirs(parent)
        fh = self._lib.hdfsOpenFile(fs, hpath.encode(), O_WRONLY | O_CREAT, 0, 0, 0)
        if not fh:
            raise OSError(f"hdfsOpenFile write failed: {hdfs_path}")
        try:
            if data:
                written = self._lib.hdfsWrite(fs, fh, data, len(data))
                if written < 0:
                    raise OSError(f"hdfsWrite failed: {hdfs_path}")
        finally:
            self._lib.hdfsCloseFile(fs, fh)
        return len(data)

    def read_bytes(self, hdfs_path: str, offset: int = 0, length: int | None = None) -> bytes:
        fs = self._connect()
        fh = self._lib.hdfsOpenFile(fs, hdfs_path_from_uri(hdfs_path).encode(), O_RDONLY, 0, 0, 0)
        if not fh:
            raise OSError(f"hdfsOpenFile read failed: {hdfs_path}")
        chunks: list[bytes] = []
        try:
            while True:
                buf = ctypes.create_string_buffer(65536)
                n = self._lib.hdfsRead(fs, fh, buf, 65536)
                if n < 0:
                    raise OSError(f"hdfsRead failed: {hdfs_path}")
                if n == 0:
                    break
                chunks.append(buf.raw[:n])
        finally:
            self._lib.hdfsCloseFile(fs, fh)
        data = b"".join(chunks)
        if length is None:
            return data[offset:]
        return data[offset : offset + length]

    def delete(self, hdfs_path: str, recursive: bool = False) -> None:
        from sgx_pyspark.hdfs.hdfs_cli import HdfsCli

        HdfsCli(user=self.user.decode()).delete(hdfs_path, recursive=recursive)
