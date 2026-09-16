"""ACL 列级访问控制（Native Spark 路径）：敏感列 AES 加密，密钥存 ACL 元数据。"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass

from sgx_pyspark.benchmark.dataset import ColumnSpec, SENSITIVE_COLUMNS
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.types import ColumnByteRange


@dataclass
class AclReadResult:
    ok: bool
    plaintext: bytes
    columns_authorized: int
    columns_masked: int
    reason: str = ""


@dataclass
class AclEncryptedTable:
    payload: bytes
    column_layout: list[ColumnByteRange]
    aes_keys: dict[str, str]
    plain_bytes: int
    enc_bytes: int
    acl_ms: float
    aes_ms: float


def _has_attr(user_attributes: list[str], required: str) -> bool:
    if " and " in required:
        return all(p.strip() in user_attributes for p in required.split(" and "))
    return required in user_attributes


def column_visible(spec: ColumnSpec, user_attributes: list[str]) -> bool:
    return _has_attr(user_attributes, spec.policy)


def acl_enforce_write(hdfs_path: str, user_attributes: list[str]) -> float:
    """模拟 ACL 写权限校验，返回耗时 ms。"""
    t0 = time.perf_counter()
    if "role:analyst" not in user_attributes and "role:admin" not in user_attributes:
        raise PermissionError(f"ACL deny write: {hdfs_path}")
    return (time.perf_counter() - t0) * 1000.0


def acl_encrypt_table(
    gen,
    specs: list[ColumnSpec],
    aes: AESGCMCrypto,
    hdfs_path: str,
    user_attributes: list[str],
    nonce_bytes: int = 12,
    tag_bytes: int = 16,
) -> AclEncryptedTable:
    """公开列明文落盘，敏感列 AES-GCM 加密；DEK 按策略分组写入 ACL 元数据。"""
    acl_ms = acl_enforce_write(hdfs_path, user_attributes)

    t_aes0 = time.perf_counter()
    dek_by_policy: dict[str, bytes] = {}
    for spec in specs:
        if spec.sensitive and spec.policy not in dek_by_policy:
            dek_by_policy[spec.policy] = aes.generate_dek()

    range_by_name = {r.column_name: r for r in gen.column_ranges}
    payload_parts: list[bytes] = []
    file_layout: list[ColumnByteRange] = []
    offset = 0

    for idx, spec in enumerate(specs):
        plain_range = range_by_name[spec.name]
        plain_slice = gen.plaintext[plain_range.byte_offset : plain_range.byte_offset + plain_range.byte_length]
        if spec.sensitive:
            dek = dek_by_policy[spec.policy]
            segment = aes.encrypt_column_segment(dek, plain_slice, nonce_bytes, tag_bytes, idx + 1)
            payload_parts.append(segment)
            file_layout.append(ColumnByteRange(column_name=spec.name, byte_offset=offset, byte_length=len(segment)))
            offset += len(segment)
        else:
            payload_parts.append(plain_slice)
            file_layout.append(ColumnByteRange(column_name=spec.name, byte_offset=offset, byte_length=len(plain_slice)))
            offset += len(plain_slice)

    aes_ms = (time.perf_counter() - t_aes0) * 1000.0
    payload = b"".join(payload_parts)
    aes_keys = {policy: base64.b64encode(dek).decode("ascii") for policy, dek in dek_by_policy.items()}

    return AclEncryptedTable(
        payload=payload,
        column_layout=file_layout,
        aes_keys=aes_keys,
        plain_bytes=len(gen.plaintext),
        enc_bytes=len(payload),
        acl_ms=acl_ms,
        aes_ms=aes_ms,
    )


def acl_decrypt_or_mask(
    payload: bytes,
    file_layout: list[ColumnByteRange],
    specs: list[ColumnSpec],
    user_attributes: list[str],
    aes_keys: dict[str, str],
    aes: AESGCMCrypto,
    record_count: int,
    nonce_bytes: int = 12,
) -> AclReadResult:
    """按 ACL 解密授权列；无权限列（含敏感列）填 NULL。"""
    t0 = time.perf_counter()
    layout_by_name = {r.column_name: r for r in file_layout}
    total_plain = sum(spec.width * record_count for spec in specs)
    out = bytearray(total_plain)
    null_pat = b"NULL"
    authorized = 0
    masked = 0
    plain_offset = 0

    for spec in specs:
        col_len = spec.width * record_count
        file_range = layout_by_name.get(spec.name)
        if file_range is None:
            raise ValueError(f"missing ACL layout for column {spec.name}")

        if column_visible(spec, user_attributes):
            authorized += 1
            file_slice = payload[file_range.byte_offset : file_range.byte_offset + file_range.byte_length]
            if spec.sensitive:
                dek_b64 = aes_keys.get(spec.policy)
                if not dek_b64:
                    return AclReadResult(ok=False, plaintext=b"", columns_authorized=0, columns_masked=0, reason=f"missing AES key for {spec.policy}")
                dek = base64.b64decode(dek_b64)
                plain_col = aes.decrypt_column_segment(dek, file_slice, nonce_bytes)
                out[plain_offset : plain_offset + col_len] = plain_col[:col_len]
            else:
                out[plain_offset : plain_offset + col_len] = file_slice[:col_len]
        else:
            masked += 1
            for off in range(0, col_len, 4):
                chunk = min(4, col_len - off)
                out[plain_offset + off : plain_offset + off + chunk] = null_pat[:chunk]

        plain_offset += col_len

    _ = (time.perf_counter() - t0) * 1000.0
    return AclReadResult(
        ok=True,
        plaintext=bytes(out),
        columns_authorized=authorized,
        columns_masked=masked,
    )


def acl_mask_plaintext(
    plaintext: bytes,
    column_ranges: list,
    specs: list[ColumnSpec],
    user_attributes: list[str],
) -> AclReadResult:
    """兼容旧明文 ACL 路径：无权限列填 NULL。"""
    t0 = time.perf_counter()
    out = bytearray(plaintext)
    spec_by_name = {s.name: s for s in specs}
    authorized = 0
    masked = 0
    null_pat = b"NULL"

    for col_range in column_ranges:
        spec = spec_by_name.get(col_range.column_name)
        if spec is None:
            continue
        if column_visible(spec, user_attributes):
            authorized += 1
            continue
        masked += 1
        base = col_range.byte_offset
        width = col_range.byte_length
        for off in range(0, width, 4):
            chunk = min(4, width - off)
            out[base + off : base + off + chunk] = null_pat[:chunk]

    _ = (time.perf_counter() - t0) * 1000.0
    return AclReadResult(
        ok=True,
        plaintext=bytes(out),
        columns_authorized=authorized,
        columns_masked=masked,
    )
