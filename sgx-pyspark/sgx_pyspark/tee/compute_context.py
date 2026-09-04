"""TEE 计算上下文：明文仅存在于 TEE 内存，禁止直接导出完整行。"""

from __future__ import annotations

from dataclasses import dataclass, field

from sgx_pyspark.types import ColumnByteRange, WorkerReadResult


@dataclass
class TeeComputeResult:
    ok: bool
    metrics: dict = field(default_factory=dict)
    output_columns: dict[str, bytes] = field(default_factory=dict)
    columns_authorized: int = 0
    columns_masked: int = 0
    reason: str = ""
    tee_runtime: str = ""


class TeeComputeContext:
    """
    封装 TEE 内解密后的列布局明文。

    不提供 export_plaintext()；算子通过 get_column() 按列访问，
    聚合/摘要结果经 TeeComputeResult 离开 TEE 边界。
    """

    def __init__(
        self,
        layout: list[ColumnByteRange],
        plaintext: bytes | bytearray,
        authorized_columns: set[str],
        masked_columns: set[str],
    ) -> None:
        self._layout = layout
        self._plaintext = bytearray(plaintext)
        self._authorized = authorized_columns
        self._masked = masked_columns
        self._layout_by_name = {item.column_name: item for item in layout}

    @classmethod
    def from_read_result(cls, layout: list[ColumnByteRange], read_result: WorkerReadResult) -> TeeComputeContext:
        if not read_result.ok:
            raise ValueError(read_result.reason or "read failed")
        authorized: set[str] = set()
        masked: set[str] = set()
        for item in layout:
            segment = read_result.plaintext[item.byte_offset : item.byte_offset + item.byte_length]
            if b"NULL" in segment and segment.strip(b"\x00") == b"NULL":
                masked.add(item.column_name)
            else:
                authorized.add(item.column_name)
        return cls(layout, read_result.plaintext, authorized, masked)

    def column_width(self) -> int:
        if not self._layout:
            return 10
        return self._layout[0].byte_length

    def is_authorized(self, column_name: str) -> bool:
        return column_name in self._authorized

    def get_column(self, column_name: str) -> str:
        item = self._layout_by_name.get(column_name)
        if item is None:
            raise KeyError(f"unknown column: {column_name}")
        raw = self._plaintext[item.byte_offset : item.byte_offset + item.byte_length]
        return raw.decode("utf-8", errors="replace")

    def authorized_column_names(self) -> list[str]:
        return sorted(self._authorized)

    def authorized_column_count(self) -> int:
        return len(self._authorized)

    def masked_column_count(self) -> int:
        return len(self._masked)

    def apply_operator(self, operator) -> TeeComputeResult:
        metrics = operator(self)
        return TeeComputeResult(
            ok=True,
            metrics=metrics,
            columns_authorized=self.authorized_column_count(),
            columns_masked=self.masked_column_count(),
        )

    def apply_write_operator(self, operator) -> TeeComputeResult:
        output_columns = operator(self)
        return TeeComputeResult(
            ok=True,
            output_columns=output_columns,
            columns_authorized=self.authorized_column_count(),
            columns_masked=self.masked_column_count(),
        )
