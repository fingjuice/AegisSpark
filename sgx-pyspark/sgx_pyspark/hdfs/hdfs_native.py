"""libhdfs 原生客户端（ctypes），避免 hdfs dfs 子进程开销。"""

from __future__ import annotations

import ctypes
import os
from ctypes import c_char_p, c_int, c_void_p
from pathlib import Path

from sgx_pyspark.hdfs.hdfs_cli import hdfs_path_from_uri


class LibHdfsClient:
    _lib = None

    def __init__(self, namenode_uri: str, user: str | None = None) -> None:
        self.namenode_uri = namenode_uri
        self.user = user or os.environ.get("HADOOP_USER_NAME", "shanlicheng")
        self._fs: c_void_p | None = None
        self._load()

    @classmethod
    def _load(cls) -> None:
        if cls._lib is not None:
            return
        hadoop_home = Path(os.environ.get("HADOOP_HOME", "/home/shanlicheng/opt/hadoop"))
        lib_path = hadoop_home / "lib" / "native" / "libhdfs.so"
        lib = ctypes.CDLL(str(lib_path))
        lib.hdfsConnectAsUser.argtypes = [c_char_p, c_int, c_char_p]
        lib.hdfsConnectAsUser.restype = c_void_p
        lib.hdfsDisconnect.argtypes = [c_void_p]
        lib.hdfsCreateDirectory.argtypes = [c_void_p, c_char_p]
        lib.hdfsCreateDirectory.restype = c_int
        lib.hdfsExists.argtypes = [c_void_p, c_char_p]
        lib.hdfsExists.restype = c_int
        lib.hdfsOpenFile.argtypes = [c_void_p, c_char_p, c_int, c_int, c_int, c_int]
        lib.hdfsOpenFile.restype = c_void_p
        lib.hdfsCloseFile.argtypes = [c_void_p, c_void_p]
        lib.hdfsWrite.argtypes = [c_void_p, c_void_p, c_void_p, c_int]
        lib.hdfsWrite.restype = c_int
        lib.hdfsRead.argtypes = [c_void_p, c_void_p, c_void_p, c_int]
        lib.hdfsRead.restype = c_int
        cls._lib = lib

    def _connect(self) -> c_void_p:
        if self._fs is not None:
            return self._fs
        host = self.namenode_uri.replace("hdfs://", "").split(":")[0].split("/")[0]
        port = 9000
        if ":" in self.namenode_uri.replace("hdfs://", ""):
            port = int(self.namenode_uri.replace("hdfs://", "").split(":")[1].split("/")[0])
        fs = self._lib.hdfsConnectAsUser(host.encode(), port, self.user.encode())
        if not fs:
            raise RuntimeError(f"libhdfs connect failed: {host}:{port}")
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
        O_WRONLY = 1
        O_CREAT = 64
        f = self._lib.hdfsOpenFile(fs, hpath.encode(), O_WRONLY | O_CREAT, 0, 0, 0)
        if not f:
            raise RuntimeError(f"hdfsOpenFile failed: {hpath}")
        try:
            if data:
                buf = ctypes.create_string_buffer(data, len(data))
                n = self._lib.hdfsWrite(fs, f, buf, len(data))
                if n < 0:
                    raise RuntimeError(f"hdfsWrite failed: {hpath}")
        finally:
            self._lib.hdfsCloseFile(fs, f)
        return len(data)

    def read_bytes(self, hdfs_path: str, offset: int = 0, length: int | None = None) -> bytes:
        fs = self._connect()
        O_RDONLY = 0
        hpath = hdfs_path_from_uri(hdfs_path)
        f = self._lib.hdfsOpenFile(fs, hpath.encode(), O_RDONLY, 0, 0, 0)
        if not f:
            raise RuntimeError(f"hdfsOpenFile failed: {hpath}")
        chunks: list[bytes] = []
        try:
            if offset:
                # libhdfs 无 seek 时读全量再切片
                pass
            buf = ctypes.create_string_buffer(65536)
            while True:
                n = self._lib.hdfsRead(fs, f, buf, 65536)
                if n <= 0:
                    break
                chunks.append(buf.raw[:n])
        finally:
            self._lib.hdfsCloseFile(fs, f)
        data = b"".join(chunks)
        if length is None:
            return data[offset:]
        return data[offset : offset + length]

    def close(self) -> None:
        if self._fs is not None:
            self._lib.hdfsDisconnect(self._fs)
            self._fs = None


def try_libhdfs_client(namenode_uri: str, user: str | None = None) -> LibHdfsClient | None:
    try:
        java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
        hadoop_home = os.environ.get("HADOOP_HOME", "/home/shanlicheng/opt/hadoop")
        ld = os.environ.get("LD_LIBRARY_PATH", "")
        os.environ["LD_LIBRARY_PATH"] = f"{hadoop_home}/lib/native:{ld}"
        os.environ.setdefault("CLASSPATH", "")
        os.environ["CLASSPATH"] = os.popen(f"{hadoop_home}/bin/hadoop classpath").read().strip()
        os.environ.setdefault("JAVA_HOME", java_home)
        return LibHdfsClient(namenode_uri, user=user)
    except Exception:
        return None
