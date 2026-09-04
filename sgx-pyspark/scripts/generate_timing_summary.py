#!/usr/bin/env python3
"""从已有 Experimental-Result CSV 重新生成 timing_summary 与控制台汇总。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sgx_pyspark.benchmark.summary import write_timing_summary


def main() -> int:
    result_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "Experimental-Result"
    if not result_dir.is_dir():
        print(f"目录不存在: {result_dir}", file=sys.stderr)
        return 1
    write_timing_summary(result_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
