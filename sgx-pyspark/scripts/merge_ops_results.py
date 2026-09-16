#!/usr/bin/env python3
"""合并四节点逐次 EAC/ABE 操作 CSV 到 result/company/。"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def merge_ops(result_dir: Path, fname: str, out_name: str | None = None) -> int:
    out_name = out_name or fname
    sources = [result_dir / f"node_{i}" / fname for i in range(4)]
    dest = result_dir / out_name
    rows: list[dict] = []
    fieldnames: list[str] = []
    for src in sources:
        if not src.is_file() or src.stat().st_size == 0:
            continue
        with src.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                for fn in reader.fieldnames:
                    if fn not in fieldnames:
                        fieldnames.append(fn)
            rows.extend(reader)
    if not rows:
        print(f"无记录: {fname}")
        return 0
    # 按 cumulative_gb + node + op_id 排序
    def sort_key(r: dict):
        try:
            return (float(r.get("cumulative_gb", 0)), int(r.get("node", 0)), int(r.get("op_id", 0)))
        except ValueError:
            return (0.0, 0, 0)

    rows.sort(key=sort_key)
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for i, row in enumerate(rows, 1):
            row = dict(row)
            row["global_op_id"] = str(i)
            w.writerow(row)
    print(f"合并 {fname}: {len(rows)} 条 -> {dest}")
    return len(rows)


def main() -> int:
    result_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "result" / "company"
    merge_ops(result_dir, "eac_time.csv", "eac_time.csv")
    merge_ops(result_dir, "abe_encrypt_time.csv", "abe_encrypt_time.csv")
    merge_ops(result_dir, "abe_time.csv", "abe_time.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
