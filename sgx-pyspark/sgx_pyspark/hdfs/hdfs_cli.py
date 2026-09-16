"""HDFS CLI 封装：通过 hdfs dfs 命令访问真实 HDFS 集群。"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path


def hdfs_path_from_uri(hdfs_uri: str) -> str:
    if hdfs_uri.startswith("hdfs://"):
        idx = hdfs_uri.find("/", 7)
        return hdfs_uri[idx:] if idx >= 0 else "/"
    return hdfs_uri if hdfs_uri.startswith("/") else f"/{hdfs_uri}"


class HdfsCli:
    def __init__(self, user: str | None = None) -> None:
        self.hadoop_home = Path(os.environ.get("HADOOP_HOME", "/home/shanlicheng/opt/hadoop"))
        self.hdfs_bin = self.hadoop_home / "bin" / "hdfs"
        self.user = user or os.environ.get("HADOOP_USER_NAME", "shanlicheng")

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["HADOOP_USER_NAME"] = self.user
        env.setdefault("HADOOP_CONF_DIR", str(self.hadoop_home / "etc" / "hadoop"))
        env["PATH"] = f"{self.hadoop_home / 'bin'}:{env.get('PATH', '')}"
        return env

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = [str(self.hdfs_bin), "dfs", *args]
        return subprocess.run(
            cmd,
            env=self._env(),
            capture_output=True,
            text=True,
            check=check,
        )

    def mkdirs(self, hdfs_path: str) -> None:
        self._run("-mkdir", "-p", hdfs_path_from_uri(hdfs_path))

    def exists(self, hdfs_path: str) -> bool:
        return self._run("-test", "-e", hdfs_path_from_uri(hdfs_path), check=False).returncode == 0

    def write_bytes(self, hdfs_path: str, data: bytes) -> int:
        hpath = hdfs_path_from_uri(hdfs_path)
        parent = str(Path(hpath).parent)
        if parent and parent != "/":
            self.mkdirs(parent)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        try:
            self._run("-put", "-f", tmp_path, hpath)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        return len(data)

    def read_bytes(self, hdfs_path: str, offset: int = 0, length: int | None = None) -> bytes:
        hpath = hdfs_path_from_uri(hdfs_path)
        cmd = [str(self.hdfs_bin), "dfs", "-cat", hpath]
        result = subprocess.run(cmd, env=self._env(), capture_output=True, check=True)
        data = result.stdout
        if length is None:
            return data[offset:]
        return data[offset : offset + length]

    def delete(self, hdfs_path: str, recursive: bool = False) -> None:
        args = ["-rm"]
        if recursive:
            args.append("-r")
        args.extend(["-f", hdfs_path_from_uri(hdfs_path)])
        self._run(*args, check=False)
