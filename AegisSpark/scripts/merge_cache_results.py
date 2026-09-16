#!/usr/bin/env python3
"""合并 result/cache 消融实验 CSV 为 cache_ablation_all.csv"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "result" / "cache"
OUT = BASE / "cache_ablation_all.csv"

READ = {
    ("DEK cache", "on"): BASE / "dek_cache_on.csv",
    ("DEK cache", "off"): BASE / "dek_cache_off.csv",
}
WRITE = {
    ("EAC write verify", "on"): BASE / "eac_fast_on.csv",
    ("EAC write verify", "off"): BASE / "eac_fast_off.csv",
}

HEADER = [
    "experiment", "ablation", "variant", "path",
    "checkpoint_gb", "tables", "plain_bytes",
    "ABE-READ", "HDFS-READ", "READ-Total", "AES-read",
    "AES-write", "ABE-Write", "Write-Verify", "HDFS-Write", "Write-Total",
    "timestamp",
]


def row_read(exp, abl, var, line):
    return {
        "experiment": exp, "ablation": abl, "variant": var, "path": "read",
        "checkpoint_gb": line["checkpoint_gb"],
        "tables": line["tables"],
        "plain_bytes": line["plain_bytes"],
        "ABE-READ": line.get("ABE-READ", ""),
        "HDFS-READ": line.get("HDFS-READ", ""),
        "READ-Total": line.get("READ-Total", ""),
        "AES-read": line.get("AES-read", ""),
        "AES-write": "", "ABE-Write": "", "Write-Verify": "",
        "HDFS-Write": "", "Write-Total": "",
        "timestamp": line["timestamp"],
    }


def row_write(exp, abl, var, line):
    return {
        "experiment": exp, "ablation": abl, "variant": var, "path": "write",
        "checkpoint_gb": line["checkpoint_gb"],
        "tables": line["tables"],
        "plain_bytes": line["plain_bytes"],
        "ABE-READ": "", "HDFS-READ": "", "READ-Total": "", "AES-read": "",
        "AES-write": line.get("AES-write", ""),
        "ABE-Write": line.get("ABE-Write", ""),
        "Write-Verify": line.get("Write-Verify", ""),
        "HDFS-Write": line.get("HDFS-Write", ""),
        "Write-Total": line.get("Write-Total", ""),
        "timestamp": line["timestamp"],
    }


def main():
    rows = []
    for (abl, var), fp in READ.items():
        if not fp.exists():
            continue
        with fp.open() as f:
            for line in csv.DictReader(f):
                rows.append(row_read(f"read_dek_cache_{var}", abl, var, line))
    for (abl, var), fp in WRITE.items():
        if not fp.exists():
            continue
        with fp.open() as f:
            for line in csv.DictReader(f):
                v = "on" if var == "on" else "off"
                rows.append(row_write(f"write_eac_{var}", abl, v, line))
    rows.sort(key=lambda x: (x["path"], x["experiment"], float(x["checkpoint_gb"])))
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {OUT}")


if __name__ == "__main__":
    main()
