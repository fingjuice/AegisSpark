"""真实 HDFS 存储：密文块与元数据侧车写入 HDFS 集群。"""

from __future__ import annotations

import json
from pathlib import Path

from sgx_pyspark.hdfs.hdfs_cli import hdfs_path_from_uri
from sgx_pyspark.hdfs.libhdfs_client import LibHdfsClient
from sgx_pyspark.types import Hsec, ColumnByteRange

XATTR_HEADER = "security.abe.header"
XATTR_LAYOUT = "security.abe.column_layout"


def _meta_sidecar_uri(enc_uri: str) -> str:
    hpath = hdfs_path_from_uri(enc_uri)
    if hpath.endswith(".enc"):
        return enc_uri.rsplit(".enc", 1)[0] + ".enc.meta.json"
    return enc_uri + ".meta.json"


class RealHdfsStore:
    """DataNode 真实 HDFS 实现。"""

    def __init__(self, hdfs_root: str, user: str | None = None) -> None:
        self.hdfs_root = hdfs_root.rstrip("/")
        self.cli = LibHdfsClient(hdfs_root, user=user)

    def write_block(self, hdfs_path: str, ciphertext: bytes) -> int:
        return self.cli.write_bytes(hdfs_path, ciphertext)

    def read_block(self, hdfs_path: str, offset: int = 0, length: int | None = None) -> bytes:
        return self.cli.read_bytes(hdfs_path, offset=offset, length=length)


class RealNameNodeExtension:
    """NameNode Extension 真实 HDFS 实现（.meta.json 侧车存于 HDFS）。"""

    def __init__(self, hdfs_root: str, user: str | None = None) -> None:
        self.hdfs_root = hdfs_root.rstrip("/")
        self.cli = LibHdfsClient(hdfs_root, user=user)

    def persist_meta(
        self,
        hdfs_path: str,
        header: Hsec,
        column_layout: list[ColumnByteRange],
    ) -> None:
        meta = {
            "header": json.loads(header.to_json()),
            "column_layout": [json.loads(c.to_json()) for c in column_layout],
        }
        payload = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        self.cli.write_bytes(_meta_sidecar_uri(hdfs_path), payload)

    def persist_acl_meta(
        self,
        hdfs_path: str,
        column_layout: list[ColumnByteRange],
        policies: dict,
        aes_keys: dict[str, str] | None = None,
    ) -> None:
        meta: dict = {
            "access_mode": "acl",
            "column_layout": [json.loads(c.to_json()) for c in column_layout],
            "policies": policies,
        }
        if aes_keys:
            meta["aes_keys"] = aes_keys
        payload = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
        self.cli.write_bytes(_meta_sidecar_uri(hdfs_path), payload)

    def read_acl_layout(self, hdfs_path: str) -> list[ColumnByteRange]:
        meta = self._read_meta(hdfs_path)
        return [ColumnByteRange(**item) for item in meta["column_layout"]]

    def read_acl_keys(self, hdfs_path: str) -> dict[str, str]:
        meta = self._read_meta(hdfs_path)
        keys = meta.get("aes_keys")
        return dict(keys) if keys else {}

    def read_hsec(self, hdfs_path: str) -> Hsec:
        meta = self._read_meta(hdfs_path)
        return Hsec.from_json(json.dumps(meta["header"]))

    read_security_header = read_hsec

    def read_column_layout(self, hdfs_path: str) -> list[ColumnByteRange]:
        meta = self._read_meta(hdfs_path)
        return [ColumnByteRange(**item) for item in meta["column_layout"]]

    def _read_meta(self, hdfs_path: str) -> dict:
        sidecar = _meta_sidecar_uri(hdfs_path)
        if not self.cli.exists(sidecar):
            raise FileNotFoundError(f"ABE metadata not found for {hdfs_path}")
        return json.loads(self.cli.read_bytes(sidecar).decode("utf-8"))

    def list_block_locations(self, hdfs_path: str) -> dict:
        return {
            "path": hdfs_path,
            "length": len(self.cli.read_bytes(hdfs_path)) if self.cli.exists(hdfs_path) else 0,
            "hdfs_path": hdfs_path_from_uri(hdfs_path),
        }
