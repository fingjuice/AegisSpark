"""Worker 读管道：ABE 解 DEK + AES-GCM 解密 + NULL 脱敏。"""

from __future__ import annotations

from sgx_pyspark.crypto.abe import ABECrypto, UserSecretKey
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.types import (
    Hsec,
    ColumnAccessPlan,
    ColumnByteRange,
    WorkerReadResult,
)

NULL_MASK = b"NULL"


class WorkerReadPipeline:
    @staticmethod
    def fill_null_mask(dest: bytearray, offset: int, length: int) -> None:
        for i in range(length):
            dest[offset + i] = NULL_MASK[i % 4]

    @classmethod
    def build_access_plan(
        cls,
        header: Hsec,
        column_ranges: list[ColumnByteRange],
        abe: ABECrypto,
        usk: UserSecretKey,
    ) -> list[ColumnAccessPlan]:
        plan_by_column: dict[str, ColumnAccessPlan] = {}

        for abe_header in header.abe_headers:
            dek_result = abe.decrypt_dek(usk, abe_header.encrypted_dek, abe_header.policy_expression)
            for col_name in abe_header.column_scope:
                col_range = next((r for r in column_ranges if r.column_name == col_name), None)
                if col_range is None:
                    continue
                if col_name in plan_by_column:
                    continue
                plan = ColumnAccessPlan(
                    range=col_range,
                    policy_expression=abe_header.policy_expression,
                    authorized=dek_result.ok,
                    dek=dek_result.dek if dek_result.ok else b"",
                    deny_reason="" if dek_result.ok else dek_result.reason,
                )
                plan_by_column[col_name] = plan

        for col_range in column_ranges:
            if col_range.column_name not in plan_by_column:
                plan_by_column[col_range.column_name] = ColumnAccessPlan(
                    range=col_range,
                    authorized=False,
                    deny_reason="no abe header for column",
                )

        return sorted(plan_by_column.values(), key=lambda p: p.range.byte_offset)

    @classmethod
    def process_stream(
        cls,
        encrypted_stream: bytes,
        plan: list[ColumnAccessPlan],
        aes: AESGCMCrypto,
        nonce_bytes: int = 12,
        tag_bytes: int = 16,
    ) -> WorkerReadResult:
        if not plan:
            return WorkerReadResult(ok=False, reason="empty access plan")

        max_end = max(p.range.byte_offset + p.range.byte_length for p in plan)
        plaintext = bytearray(max_end)
        cursor = 0
        authorized = 0
        masked = 0

        for col in plan:
            seg_nonce = nonce_bytes
            seg_ct = col.range.byte_length
            seg_tag = tag_bytes
            seg_total = seg_nonce + seg_ct + seg_tag

            if cursor + seg_total > len(encrypted_stream):
                return WorkerReadResult(ok=False, reason=f"encrypted stream truncated at column {col.range.column_name}")

            seg = encrypted_stream[cursor : cursor + seg_total]
            cursor += seg_total
            nonce = seg[:seg_nonce]
            ciphertext_with_tag = seg[seg_nonce:]

            dest_offset = col.range.byte_offset
            dest_len = col.range.byte_length

            if col.authorized and len(col.dek) >= 16:
                dec = aes.decrypt(col.dek, nonce, ciphertext_with_tag)
                if dec.ok and len(dec.output) == dest_len:
                    plaintext[dest_offset : dest_offset + dest_len] = dec.output
                    authorized += 1
                else:
                    cls.fill_null_mask(plaintext, dest_offset, dest_len)
                    masked += 1
            else:
                cls.fill_null_mask(plaintext, dest_offset, dest_len)
                masked += 1

        return WorkerReadResult(
            ok=True,
            plaintext=bytes(plaintext),
            columns_authorized=authorized,
            columns_masked=masked,
        )
