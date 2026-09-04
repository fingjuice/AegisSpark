"""DataNode：纯密文块持久化存储。"""

from __future__ import annotations

from pathlib import Path


class DataNodeStore:
    def __init__(self, data_root: str | Path) -> None:
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, hdfs_path: str) -> Path:
        if hdfs_path.startswith("hdfs://"):
            idx = hdfs_path.find("/", 7)
            rel = hdfs_path[idx + 1 :] if idx >= 0 else hdfs_path
        else:
            rel = hdfs_path.lstrip("/")
        return self.data_root / rel

    def write_block(self, hdfs_path: str, ciphertext: bytes) -> int:
        path = self._resolve(hdfs_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(ciphertext)
        return len(ciphertext)

    def read_block(self, hdfs_path: str, offset: int = 0, length: int | None = None) -> bytes:
        path = self._resolve(hdfs_path)
        data = path.read_bytes()
        if length is None:
            return data[offset:]
        return data[offset : offset + length]
