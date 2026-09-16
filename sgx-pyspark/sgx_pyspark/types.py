"""design.md 核心数据结构定义。"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class AdminEndorsement:
    """System Admin 签发的 Spark 作业初始写授权。"""

    app_code_hash: str
    allowed_root_directories: list[str]
    timestamp: int
    admin_signature: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> AdminEndorsement:
        data = json.loads(raw)
        return cls(**data)

    def to_design_dict(self) -> dict[str, Any]:
        return {
            "app_code_hash": self.app_code_hash,
            "allowed_root_directories": self.allowed_root_directories,
            "admin_signature": self.admin_signature,
        }


@dataclass
class HsecColumn:
    column_scope: list[str]
    policy_expression: str
    encrypted_dek: str


@dataclass
class Hsec:
    file_path: str
    abe_headers: list[HsecColumn] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> Hsec:
        data = json.loads(raw)
        headers = [HsecColumn(**h) for h in data.get("abe_headers", [])]
        return cls(file_path=data["file_path"], abe_headers=headers)


# Backward-compatible aliases (pre-architecture rename)
AbeColumnHeader = HsecColumn
AbeSecurityHeader = Hsec


@dataclass
class TaskTicket:
    job_id: str
    task_id: str
    worker_ip: str
    allowed_target_path: str
    expires_at: int
    driver_signature: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> TaskTicket:
        data = json.loads(raw)
        return cls(**data)

    def to_design_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "task_id": self.task_id,
            "worker_ip": self.worker_ip,
            "allowed_target_path": self.allowed_target_path,
            "driver_signature": self.driver_signature,
        }


@dataclass
class ColumnByteRange:
    column_name: str
    byte_offset: int
    byte_length: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, raw: str) -> ColumnByteRange:
        return cls(**json.loads(raw))


@dataclass
class ColumnAccessPlan:
    range: ColumnByteRange
    authorized: bool = False
    dek: bytes = b""
    policy_expression: str = ""
    deny_reason: str = ""


@dataclass
class WriteVerifyResult:
    ok: bool
    reason: str = ""
    monotonic_counter: int = 0


@dataclass
class WriteProxyResult:
    ok: bool
    reason: str = ""
    bytes_written: int = 0
    monotonic_counter: int = 0


@dataclass
class WorkerReadResult:
    ok: bool
    plaintext: bytes = b""
    columns_authorized: int = 0
    columns_masked: int = 0
    reason: str = ""
