"""Worker 写管道：列级加密 + ABE 头封装。"""

from __future__ import annotations

from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.types import HsecColumn, Hsec, ColumnByteRange


class WorkerWritePipeline:
    def __init__(self, abe: ABECrypto, aes: AESGCMCrypto | None = None, nonce_bytes: int = 12, tag_bytes: int = 16) -> None:
        self.abe = abe
        self.aes = aes or AESGCMCrypto()
        self.nonce_bytes = nonce_bytes
        self.tag_bytes = tag_bytes

    def encrypt_columns(
        self,
        hdfs_path: str,
        columns: dict[str, bytes],
        column_policies: dict[str, str],
    ) -> tuple[Hsec, list[ColumnByteRange], bytes]:
        """按列加密并生成 ABE 元数据头与连续密文流。"""
        policy_groups: dict[str, list[str]] = {}
        for col, policy in column_policies.items():
            policy_groups.setdefault(policy, []).append(col)

        header = Hsec(file_path=hdfs_path, abe_headers=[])
        dek_by_policy: dict[str, bytes] = {}

        for policy, scoped_cols in policy_groups.items():
            dek = self.aes.generate_dek()
            dek_by_policy[policy] = dek
            enc = self.abe.encrypt_dek(dek, policy)
            if not enc.ok:
                raise RuntimeError(f"ABE encrypt DEK failed: {enc.reason}")
            header.abe_headers.append(
                HsecColumn(column_scope=sorted(scoped_cols), policy_expression=policy, encrypted_dek=enc.encrypted_dek_b64)
            )

        ranges: list[ColumnByteRange] = []
        ciphertext_parts: list[bytes] = []
        offset = 0
        nonce_seed = 0

        for col_name, plaintext in columns.items():
            policy = column_policies[col_name]
            dek = dek_by_policy[policy]
            segment = self.aes.encrypt_column_segment(dek, plaintext, self.nonce_bytes, self.tag_bytes, nonce_seed)
            nonce_seed += 1
            ranges.append(ColumnByteRange(column_name=col_name, byte_offset=offset, byte_length=len(plaintext)))
            ciphertext_parts.append(segment)
            offset += len(plaintext)

        return header, ranges, b"".join(ciphertext_parts)
