"""实验结果汇总（CSV + 控制台，对齐 ABE-Spark access-bench）。"""

from __future__ import annotations

import csv
from pathlib import Path


def _sum_col(rows: list[dict], col: str) -> float:
    total = 0.0
    for row in rows:
        val = row.get(col)
        if val is not None and val != "":
            total += float(val)
    return total


def _read_wall_ms(path: Path, metric: str = "wall_ms") -> float:
    if not path.is_file():
        return 0.0
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("metric") == metric:
                try:
                    return float(row["value"])
                except (KeyError, ValueError):
                    pass
    return 0.0


def write_timing_summary(result_dir: Path) -> Path:
    write_path = result_dir / "write_events.csv"
    read_path = result_dir / "read_events.csv"
    out = result_dir / "timing_summary.csv"

    write_rows: list[dict] = []
    read_rows: list[dict] = []
    if write_path.is_file():
        with write_path.open(encoding="utf-8") as f:
            write_rows = list(csv.DictReader(f))
    if read_path.is_file():
        with read_path.open(encoding="utf-8") as f:
            read_rows = list(csv.DictReader(f))

    w_gen = _sum_col(write_rows, "gen_plain_ms")
    w_abe = _sum_col(write_rows, "abe_encrypt_ms")
    w_eac = _sum_col(write_rows, "eac_tee_ms")
    w_hdfs = _sum_col(write_rows, "hdfs_write_ms")
    w_meta = _sum_col(write_rows, "meta_persist_ms")
    w_total = _sum_col(write_rows, "total_ms")

    r_hdfs = _sum_col(read_rows, "io_read_ms")
    r_meta = _sum_col(read_rows, "meta_read_ms")
    r_abe = _sum_col(read_rows, "abe_tee_ms")
    r_total = _sum_col(read_rows, "ac_to_decrypt_ms")

    abe_ms = w_abe + r_abe
    eac_ms = w_eac
    hdfs_ms = w_hdfs + r_hdfs
    meta_ms = w_meta + r_meta
    gen_ms = w_gen
    proc_ms = w_total + r_total if (w_total + r_total) > 0 else 1.0

    wall_write = _read_wall_ms(result_dir / "write_run_summary.csv")
    wall_read = _read_wall_ms(result_dir / "read_run_summary.csv")
    wall_total = wall_write + wall_read
    total_ms = proc_ms if proc_ms > 0 else wall_total

    def pct(v: float) -> float:
        return v / total_ms * 100 if total_ms else 0.0

    rows = [
        ("write_gen_plain", gen_ms, pct(gen_ms)),
        ("write_abe_encrypt", w_abe, pct(w_abe)),
        ("write_eac_tee", w_eac, pct(w_eac)),
        ("write_hdfs_io", w_hdfs, pct(w_hdfs)),
        ("write_meta", w_meta, pct(w_meta)),
        ("read_hdfs_io", r_hdfs, pct(r_hdfs)),
        ("read_meta", r_meta, pct(r_meta)),
        ("read_abe_tee", r_abe, pct(r_abe)),
        ("abe_total", abe_ms, pct(abe_ms)),
        ("eac_total", eac_ms, pct(eac_ms)),
        ("hdfs_io_total", hdfs_ms, pct(hdfs_ms)),
        ("meta_total", meta_ms, pct(meta_ms)),
        ("process_total", proc_ms, 100.0 if proc_ms else 0.0),
        ("wall_write", wall_write, ""),
        ("wall_read", wall_read, ""),
        ("wall_total", wall_total, ""),
    ]

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["metric", "value_ms", "percent_of_total"])
        w.writeheader()
        for name, val, p in rows:
            w.writerow(
                {
                    "metric": name,
                    "value_ms": f"{val:.6f}",
                    "percent_of_total": f"{p:.4f}" if p != "" else "",
                }
            )

    print("\n========== 时间占比汇总 ==========")
    print(f"任务总处理时间: {total_ms / 1000.0:.1f} s")
    print(f"ABE 合计:       {abe_ms / 1000.0:.1f} s ({pct(abe_ms):.2f}%)")
    print(f"EAC 合计:       {eac_ms / 1000.0:.1f} s ({pct(eac_ms):.2f}%)")
    print(f"HDFS IO 合计:   {hdfs_ms / 1000.0:.1f} s ({pct(hdfs_ms):.2f}%)")
    print(f"元数据 IO:      {meta_ms / 1000.0:.1f} s ({pct(meta_ms):.2f}%)")
    print(f"明文生成:       {gen_ms / 1000.0:.1f} s ({pct(gen_ms):.2f}%)")
    if wall_write:
        print(f"写 wall 时间:   {wall_write / 1000.0:.1f} s")
    if wall_read:
        print(f"读 wall 时间:   {wall_read / 1000.0:.1f} s")
    print(f"结果文件: {out.resolve()}")

    return out
