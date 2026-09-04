"""四节点集群进度上报：各 worker 写 progress.json，协调器实时聚合。"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class NodeProgress:
    node: int = 0
    events: int = 0
    rows: int = 0
    enc_bytes: int = 0
    plain_bytes: int = 0
    abe_size: int = 0  # 对称密钥 ABE 密文占用
    policy_size: int = 0  # 策略存储占用
    abe_header_bytes: int = 0  # 兼容旧字段：abe_size + policy_size
    sums: dict[str, float] = field(default_factory=dict)
    wall_start: float = 0.0
    updated_at: float = 0.0
    done: bool = False
    error: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict) -> NodeProgress:
        abe_size = int(data.get("abe_size", 0))
        policy_size = int(data.get("policy_size", 0))
        abe_header = int(data.get("abe_header_bytes", 0))
        if abe_header and not abe_size and not policy_size:
            # 兼容旧 progress：整体记入 abe_size
            abe_size = abe_header
        return cls(
            node=int(data.get("node", 0)),
            events=int(data.get("events", 0)),
            rows=int(data.get("rows", 0)),
            enc_bytes=int(data.get("enc_bytes", 0)),
            plain_bytes=int(data.get("plain_bytes", 0)),
            abe_size=abe_size,
            policy_size=policy_size,
            abe_header_bytes=abe_header or (abe_size + policy_size),
            sums={k: float(v) for k, v in (data.get("sums") or {}).items()},
            wall_start=float(data.get("wall_start", 0.0)),
            updated_at=float(data.get("updated_at", 0.0)),
            done=bool(data.get("done", False)),
            error=str(data.get("error", "")),
        )


def is_cluster_mode() -> bool:
    return os.environ.get("SGX_CLUSTER_MODE", "") == "1"


def progress_path(result_dir: Path, node: int) -> Path:
    return result_dir / f"node_{node}" / "progress.json"


def write_progress(result_dir: Path, prog: NodeProgress) -> None:
    path = progress_path(result_dir, prog.node)
    path.parent.mkdir(parents=True, exist_ok=True)
    prog.updated_at = time.time()
    if not prog.abe_header_bytes:
        prog.abe_header_bytes = prog.abe_size + prog.policy_size
    payload = prog.to_json()
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
    except OSError:
        path.write_text(payload, encoding="utf-8")


def read_progress(path: Path) -> NodeProgress | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return NodeProgress.from_dict(data)
    except (json.JSONDecodeError, OSError, ValueError):
        return None
