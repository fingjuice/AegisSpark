#!/usr/bin/env python3
"""合并四节点分片结果到 result/company/ 汇总 CSV。"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def _merge_csvs(sources: list[Path], dest: Path, sort_key: str | None = None) -> int:
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
        return 0
    if sort_key and sort_key in fieldnames:
        rows.sort(key=lambda r: r.get(sort_key, ""))
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i, row in enumerate(rows, 1):
            if "event_id" in fieldnames:
                row["event_id"] = str(i)
            w.writerow(row)
    return len(rows)


def merge_cluster_results(result_dir: Path, cluster_size: int = 4) -> None:
    nodes = [result_dir / f"node_{i}" for i in range(cluster_size)]
    pairs = [
        ("write_events.csv", "table_name"),
        ("write_manifest.csv", "table_name"),
        ("write_checkpoints.csv", "checkpoint_index"),
        ("read_events.csv", "table_name"),
        ("read_checkpoints.csv", "checkpoint_index"),
    ]
    for fname, sort_key in pairs:
        sources = [n / fname for n in nodes]
        _merge_csvs(sources, result_dir / fname, sort_key)
    from sgx_pyspark.benchmark.summary import write_timing_summary
    write_timing_summary(result_dir)
    print(f"合并完成: {result_dir}")


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[2] / "result" / "company"
    merge_cluster_results(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
