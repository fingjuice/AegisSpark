#!/usr/bin/env python3
"""合并 xattr 与 ACL 100GB 空间占用对比 CSV。"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
XATTR = ROOT / "result" / "xattr" / "xattr_size.csv"
ACL = ROOT / "result" / "acl" / "acl_size.csv"
OUT = ROOT / "result" / "acl" / "xattr_vs_acl_100gb.csv"

HEADER = [
    "checkpoint_gb", "tables", "plain_bytes",
    "xattr_ciphertext_bytes", "xattr_ciphertext_per_table_bytes", "xattr_total_bytes",
    "acl_entry_count", "acl_table_bytes", "acl_bytes_per_table",
    "acl_vs_xattr_ciphertext_ratio", "acl_vs_xattr_total_ratio",
]


def load(path):
    if not path.exists():
        return {}
    with path.open() as f:
        rows = list(csv.DictReader(f))
    return {int(float(r["checkpoint_gb"])): r for r in rows}


def main():
    xattr = load(XATTR)
    acl = load(ACL)
    keys = sorted(set(xattr) | set(acl))
    rows = []
    for k in keys:
        x = xattr.get(k, {})
        a = acl.get(k, {})
        x_ct = float(x.get("xattr_ciphertext_bytes", 0) or 0)
        x_tot = float(x.get("xattr_total_bytes", 0) or 0)
        a_bytes = float(a.get("acl_table_bytes", 0) or 0)
        ratio_ct = (a_bytes / x_ct) if x_ct > 0 else ""
        ratio_tot = (a_bytes / x_tot) if x_tot > 0 else ""
        rows.append({
            "checkpoint_gb": k,
            "tables": a.get("tables", x.get("tables", "")),
            "plain_bytes": a.get("plain_bytes", x.get("plain_bytes", "")),
            "xattr_ciphertext_bytes": x.get("xattr_ciphertext_bytes", ""),
            "xattr_ciphertext_per_table_bytes": x.get("xattr_ciphertext_per_table_bytes", ""),
            "xattr_total_bytes": x.get("xattr_total_bytes", ""),
            "acl_entry_count": a.get("acl_entry_count", ""),
            "acl_table_bytes": a.get("acl_table_bytes", ""),
            "acl_bytes_per_table": a.get("acl_bytes_per_table", ""),
            "acl_vs_xattr_ciphertext_ratio": f"{ratio_ct:.1f}" if ratio_ct != "" else "",
            "acl_vs_xattr_total_ratio": f"{ratio_tot:.1f}" if ratio_tot != "" else "",
        })
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {OUT}")


if __name__ == "__main__":
    main()
