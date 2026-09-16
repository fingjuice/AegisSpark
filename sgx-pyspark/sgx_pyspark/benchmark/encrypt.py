"""表级 ABE 加密（Worker TEE 写路径）。"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass

from sgx_pyspark.benchmark.dataset import ColumnSpec, GeneratedTable, POLICY_PUBLIC, POLICY_SENSITIVE
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.types import HsecColumn, Hsec, ColumnByteRange


@dataclass
class EncryptedTable:
    ciphertext: bytes
    header: Hsec
    ranges: list[ColumnByteRange]
    plain_bytes: int
    enc_bytes: int
    abe_size: int = 0  # 对称密钥 ABE 密文占用字节
    policy_size: int = 0  # 策略表达式 + 列作用域存储占用字节


def encrypt_table(
    gen: GeneratedTable,
    abe: ABECrypto,
    aes: AESGCMCrypto,
    hdfs_path: str,
    specs: list[ColumnSpec],
    nonce_bytes: int = 12,
    tag_bytes: int = 16,
    lock: threading.Lock | None = None,
) -> EncryptedTable:
    header = Hsec(file_path=hdfs_path, abe_headers=[])
    policy_groups: dict[str, list[str]] = {
        POLICY_PUBLIC: [],
        POLICY_SENSITIVE: [],
    }
    for spec in specs:
        policy_groups[spec.policy].append(spec.name)

    dek_by_policy: dict[str, bytes] = {}
    for policy, cols in policy_groups.items():
        if not cols:
            continue
        dek = aes.generate_dek()
        dek_by_policy[policy] = dek
        if lock:
            with lock:
                enc = abe.encrypt_dek(dek, policy)
        else:
            enc = abe.encrypt_dek(dek, policy)
        if not enc.ok:
            raise RuntimeError(f"encrypt_dek failed: {enc.reason}")
        header.abe_headers.append(
            HsecColumn(column_scope=sorted(cols), policy_expression=policy, encrypted_dek=enc.encrypted_dek_b64)
        )

    ciphertext_parts: list[bytes] = []
    for idx, spec in enumerate(specs):
        col_range = gen.column_ranges[idx]
        off = col_range.byte_offset
        length = col_range.byte_length
        plain_slice = gen.plaintext[off : off + length]
        dek = dek_by_policy[spec.policy]
        segment = aes.encrypt_column_segment(dek, plain_slice, nonce_bytes, tag_bytes, idx + 1)
        ciphertext_parts.append(segment)

    ciphertext = b"".join(ciphertext_parts)
    abe_size = 0
    policy_size = 0
    for col_h in header.abe_headers:
        abe_size += len(col_h.encrypted_dek.encode("utf-8"))
        policy_size += len(col_h.policy_expression.encode("utf-8"))
        policy_size += len(json.dumps(col_h.column_scope, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return EncryptedTable(
        ciphertext=ciphertext,
        header=header,
        ranges=gen.column_ranges,
        plain_bytes=len(gen.plaintext),
        enc_bytes=len(ciphertext),
        abe_size=abe_size,
        policy_size=policy_size,
    )
