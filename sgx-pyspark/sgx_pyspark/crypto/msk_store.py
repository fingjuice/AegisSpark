"""MSK 持久化（演示/开发环境）。"""

from __future__ import annotations

import os
from pathlib import Path


def load_or_create_msk(keys_dir: str | Path) -> bytes:
    path = Path(keys_dir) / "abe_msk.bin"
    if path.is_file():
        return path.read_bytes()
    msk = os.urandom(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(msk)
    return msk
