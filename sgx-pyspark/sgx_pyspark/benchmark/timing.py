"""实验计时与 CSV 输出工具。"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass, field
from pathlib import Path


def now_ns() -> int:
    return time.time_ns()


def ms_between(start: float, end: float) -> float:
    return (end - start) * 1000.0


GB = 1024**3


def bytes_to_gb(n: int) -> float:
    return n / GB


@dataclass
class CsvWriter:
    path: Path
    fieldnames: list[str]
    _file: object = field(init=False, repr=False)
    _writer: csv.DictWriter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self.path.exists() or self.path.stat().st_size == 0
        self._file = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)
        if new_file:
            self._writer.writeheader()

    def write_row(self, row: dict) -> None:
        self._writer.writerow(row)
        self._file.flush()

    def close(self) -> None:
        self._file.close()
