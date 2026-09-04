"""NameNode Extension：维护 ABE 密文元数据头（xattr / 侧车 JSON）。"""

from __future__ import annotations

import json
from pathlib import Path

from sgx_pyspark.types import Hsec, ColumnByteRange

XATTR_HEADER = "security.abe.header"
XATTR_LAYOUT = "security.abe.column_layout"


class NameNodeExtension:
    """本地文件系统模拟 HDFS xattr；不支持 xattr 时写 .meta.json 侧车。"""

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

    def _meta_sidecar(self, enc_path: Path) -> Path:
        return enc_path.with_suffix(enc_path.suffix + ".meta.json")

    def persist_meta(
        self,
        hdfs_path: str,
        header: Hsec,
        column_layout: list[ColumnByteRange],
    ) -> None:
        enc_path = self._resolve(hdfs_path)
        enc_path.parent.mkdir(parents=True, exist_ok=True)
        meta = {
            "header": json.loads(header.to_json()),
            "column_layout": [json.loads(c.to_json()) for c in column_layout],
        }
        self._meta_sidecar(enc_path).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def persist_acl_meta(
        self,
        hdfs_path: str,
        column_layout: list[ColumnByteRange],
        policies: dict,
        aes_keys: dict[str, str] | None = None,
    ) -> None:
        """ACL 模式元数据（列布局 + 策略 + 可选 AES 密钥）。"""
        path = self._resolve(hdfs_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        meta = {
            "access_mode": "acl",
            "column_layout": [json.loads(c.to_json()) for c in column_layout],
            "policies": policies,
        }
        if aes_keys:
            meta["aes_keys"] = aes_keys
        self._meta_sidecar(path).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def read_acl_layout(self, hdfs_path: str) -> list[ColumnByteRange]:
        meta = self._read_meta(hdfs_path)
        return [ColumnByteRange(**item) for item in meta["column_layout"]]

    def read_acl_keys(self, hdfs_path: str) -> dict[str, str]:
        meta = self._read_meta(hdfs_path)
        keys = meta.get("aes_keys")
        return dict(keys) if keys else {}

    def read_hsec(self, hdfs_path: str) -> Hsec:
        """Load Security Header (Hsec) for an encrypted file."""
        meta = self._read_meta(hdfs_path)
        return Hsec.from_json(json.dumps(meta["header"]))

    # Backward-compatible alias
    read_security_header = read_hsec

    def read_column_layout(self, hdfs_path: str) -> list[ColumnByteRange]:
        meta = self._read_meta(hdfs_path)
        return [ColumnByteRange(**item) for item in meta["column_layout"]]

    def _read_meta(self, hdfs_path: str) -> dict:
        sidecar = self._meta_sidecar(self._resolve(hdfs_path))
        if not sidecar.is_file():
            raise FileNotFoundError(f"ABE metadata not found for {hdfs_path}")
        return json.loads(sidecar.read_text(encoding="utf-8"))

    def list_block_locations(self, hdfs_path: str) -> dict:
        enc_path = self._resolve(hdfs_path)
        return {
            "path": hdfs_path,
            "length": enc_path.stat().st_size if enc_path.is_file() else 0,
            "local_path": str(enc_path),
        }
