#!/usr/bin/env python3
"""查看 100GB 实验进度。"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULT = ROOT / "Experimental-Result"


def last_cumulative(path: Path, col: str = "cumulative_gb") -> str:
    if not path.is_file():
        return "N/A"
    last = "0"
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            last = row.get(col, last)
    return last


def main() -> None:
    we = RESULT / "write_events.csv"
    wc = RESULT / "write_checkpoints.csv"
    re = RESULT / "read_events.csv"
    rc = RESULT / "read_checkpoints.csv"

    print(f"结果目录: {RESULT}")
    if we.is_file():
        n = sum(1 for _ in open(we)) - 1
        print(f"写事件: {n} 条, 累计 {last_cumulative(we)} GB")
    if wc.is_file() and wc.stat().st_size > 0:
        with wc.open(encoding="utf-8") as f:
            cks = list(csv.DictReader(f))
        print(f"写 checkpoint: {len(cks)} 个 (最近 {cks[-1]['checkpoint_gb']} GB)" if cks else "写 checkpoint: 0")
    if re.is_file():
        n = sum(1 for _ in open(re)) - 1
        print(f"读事件: {n} 条, 累计 {last_cumulative(re)} GB")
    if rc.is_file() and rc.stat().st_size > 0:
        with rc.open(encoding="utf-8") as f:
            cks = list(csv.DictReader(f))
        print(f"读 checkpoint: {len(cks)} 个 (最近 {cks[-1]['checkpoint_gb']} GB)" if cks else "读 checkpoint: 0")


if __name__ == "__main__":
    main()
