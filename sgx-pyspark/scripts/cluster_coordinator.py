#!/usr/bin/env python3
"""四节点集群协调器：每 10GB 写 write_perf/read_perf（ABE-size/policy-size/task-total-time）。

单次 EAC/ABE 加解密耗时由各节点实时写入：
  node_X/eac_time.csv
  node_X/abe_encrypt_time.csv
  node_X/abe_time.csv
结束后合并到 result/company/。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sgx_pyspark.benchmark.cluster_progress import NodeProgress, read_progress
from sgx_pyspark.benchmark.timing import CsvWriter, bytes_to_gb

NODES = ["10.26.40.83", "10.26.40.84", "10.26.40.85", "10.26.40.86"]
REMOTE_ROOT = "/home/shanlicheng/sgx-pyspark-run"
USER = "shanlicheng"


def _target_bytes() -> int:
    return int(float(os.environ.get("SGX_EXP_TARGET_GB", "100")) * 1024**3)


def _checkpoint_bytes() -> int:
    return int(float(os.environ.get("SGX_EXP_CHECKPOINT_GB", "10")) * 1024**3)


def _fetch_progress(result_dir: Path, node: int) -> NodeProgress | None:
    local = result_dir / f"node_{node}" / "progress.json"
    if node == 0:
        return read_progress(local)
    try:
        out = subprocess.check_output(
            [
                "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                f"{USER}@{NODES[node]}",
                f"cat {REMOTE_ROOT}/result/company/node_{node}/progress.json",
            ],
            text=True,
            timeout=10,
        )
        return NodeProgress.from_dict(json.loads(out))
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
        return read_progress(local)


def _sum_field(progs: list[NodeProgress], name: str) -> float:
    return sum(p.sums.get(name, 0.0) for p in progs)


def _aggregate(progs: list[NodeProgress]) -> dict:
    return {
        "events": sum(p.events for p in progs),
        "rows": sum(p.rows for p in progs),
        "enc_bytes": sum(p.enc_bytes for p in progs),
        "plain_bytes": sum(p.plain_bytes for p in progs),
        "abe_size": sum(p.abe_size for p in progs),
        "policy_size": sum(p.policy_size for p in progs),
        "abe_header_bytes": sum(p.abe_header_bytes for p in progs),
        "gen_plain_ms": _sum_field(progs, "gen_plain_ms"),
        "abe_encrypt_ms": _sum_field(progs, "abe_encrypt_ms"),
        "eac_tee_ms": _sum_field(progs, "eac_tee_ms"),
        "hdfs_write_ms": _sum_field(progs, "hdfs_write_ms"),
        "meta_persist_ms": _sum_field(progs, "meta_persist_ms"),
        "total_ms": _sum_field(progs, "total_ms"),
        "io_read_ms": _sum_field(progs, "io_read_ms"),
        "abe_tee_ms": _sum_field(progs, "abe_tee_ms"),
        "ac_to_decrypt_ms": _sum_field(progs, "ac_to_decrypt_ms"),
    }


def _proportional_delta(cur: dict, prev: dict, cluster_bytes: int, seg_bytes: int) -> dict:
    remain = cluster_bytes - prev.get("enc_bytes", 0)
    if remain <= 0 or cluster_bytes <= 0:
        return {k: (0 if not k.endswith("_ms") else 0.0) for k in cur}
    share = min(seg_bytes, remain) / remain
    out: dict = {}
    for k, v in cur.items():
        if k.endswith("_ms"):
            out[k] = (v - prev.get(k, 0.0)) * share
        else:
            out[k] = int((v - prev.get(k, 0)) * share)
    return out


def _proportional_cumulative(cur: dict, cluster_bytes: int, at_bytes: int) -> dict:
    if cluster_bytes <= 0:
        return dict(cur)
    frac = min(at_bytes, cluster_bytes) / cluster_bytes
    out: dict = {}
    for k, v in cur.items():
        if k.endswith("_ms"):
            out[k] = v * frac
        else:
            out[k] = int(v * frac)
    return out


def _advance_prev(prev: dict, delta: dict, enc_bytes: int) -> dict:
    nxt = dict(prev)
    for k, v in delta.items():
        if k.endswith("_ms"):
            nxt[k] = nxt.get(k, 0.0) + v
        else:
            nxt[k] = nxt.get(k, 0) + v
    nxt["enc_bytes"] = enc_bytes
    return nxt


def run_coordinator(result_dir: Path, mode: str, cluster_size: int) -> int:
    result_dir.mkdir(parents=True, exist_ok=True)
    target = _target_bytes()
    ck_bytes = _checkpoint_bytes()
    wall_start = time.perf_counter()

    if mode == "write":
        out_path = result_dir / "write_perf.csv"
        fields = [
            "checkpoint_gb", "cumulative_tables", "cumulative_rows", "cumulative_bytes",
            "ABE-size", "policy-size", "task-total-time",
            "encrypt_time_ms", "eac_time_sum_ms", "hdfs_write_time_ms", "timestamp",
        ]
    else:
        out_path = result_dir / "read_perf.csv"
        fields = [
            "checkpoint_gb", "cumulative_tables", "cumulative_rows", "cumulative_bytes_read",
            "task-total-time",
            "abe_lookup_sum_ms", "decrypt_time_ms", "hdfs_read_time_ms", "timestamp",
        ]

    writer = CsvWriter(out_path, fields)
    next_ck_bytes = ck_bytes
    prev_totals: dict = {}
    ready_nodes = 0
    logged_first = False
    wall_offset_ms = float(os.environ.get("SGX_COORD_WALL_OFFSET_MS", "0") or "0")

    print(
        f"[协调器] mode={mode}, 目标={bytes_to_gb(target):.1f}GB, "
        f"主采样={bytes_to_gb(ck_bytes):.1f}GB（每表 EAC/ABE 时间写入 node_*/eac_time.csv 等）"
    )
    print(f"[协调器] 主结果: {out_path}")
    if wall_offset_ms:
        print(f"[协调器] wall offset={wall_offset_ms:.0f}ms（中途重启补偿）")

    while True:
        progs = [_fetch_progress(result_dir, i) for i in range(cluster_size)]
        # 读阶段必须忽略写阶段残留 progress（含 eac_tee_ms / abe_encrypt_ms，且已 done）
        if mode == "read":
            progs_ok = [
                p for p in progs
                if p is not None
                and (
                    "io_read_ms" in p.sums
                    or "abe_tee_ms" in p.sums
                    or (not p.done and p.events == 0)
                )
            ]
        else:
            progs_ok = [p for p in progs if p is not None]
        if not progs_ok:
            time.sleep(1)
            continue
        if all(p.enc_bytes == 0 and p.events == 0 for p in progs_ok):
            time.sleep(1)
            continue

        if mode == "read" and ready_nodes < cluster_size:
            # 至少要有读侧计时字段，才算真正进入读阶段
            reading = [
                p for p in progs_ok
                if ("io_read_ms" in p.sums or "abe_tee_ms" in p.sums) and p.events > 0
            ]
            if reading:
                ready_nodes = cluster_size
                print(f"[协调器] 读阶段已开始（{len(reading)} 节点有进度）")
            else:
                time.sleep(1)
                continue

        cur = _aggregate(progs_ok)
        if not logged_first and cur["events"] > 0:
            logged_first = True
            print(
                f"[协调器] 首次进度: events={cur['events']}, "
                f"enc={bytes_to_gb(cur['enc_bytes']):.3f}GB, "
                f"ABE-lookup={cur.get('abe_tee_ms', 0.0):.1f}ms, "
                f"decrypt={cur.get('ac_to_decrypt_ms', 0.0):.1f}ms, "
                f"hdfs_read={cur.get('io_read_ms', 0.0):.1f}ms"
            )

        cluster_bytes = cur["enc_bytes"]
        # 读阶段：只有带读侧 sums 且 done 的节点才算完成
        if mode == "read":
            reading_done = [
                p for p in progs_ok
                if ("io_read_ms" in p.sums or "abe_tee_ms" in p.sums)
            ]
            all_done = (
                len(reading_done) == cluster_size
                and all(p.done for p in reading_done)
            )
        else:
            all_done = all(p.done for p in progs_ok) and len(progs_ok) == cluster_size
        any_error = next((p.error for p in progs_ok if p.error), "")

        while next_ck_bytes <= cluster_bytes or (all_done and next_ck_bytes <= target):
            if cluster_bytes < next_ck_bytes and not all_done:
                break
            seg = min(ck_bytes, max(cluster_bytes - (next_ck_bytes - ck_bytes), 0)) or ck_bytes
            d = _proportional_delta(cur, prev_totals, cluster_bytes, seg)
            cum = _proportional_cumulative(cur, cluster_bytes, next_ck_bytes)
            wall_ms = (time.perf_counter() - wall_start) * 1000.0 + wall_offset_ms
            gb = bytes_to_gb(min(next_ck_bytes, target))
            if mode == "write":
                writer.write_row(
                    {
                        "checkpoint_gb": f"{gb:.6f}",
                        "cumulative_tables": cum["events"],
                        "cumulative_rows": cum["rows"],
                        "cumulative_bytes": cum["plain_bytes"],
                        "ABE-size": cum["abe_size"],
                        "policy-size": cum["policy_size"],
                        "task-total-time": f"{wall_ms:.6f}",
                        "encrypt_time_ms": f"{d.get('abe_encrypt_ms', 0.0):.6f}",
                        "eac_time_sum_ms": f"{d.get('eac_tee_ms', 0.0):.6f}",
                        "hdfs_write_time_ms": f"{d.get('hdfs_write_ms', 0.0):.6f}",
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                    }
                )
            else:
                writer.write_row(
                    {
                        "checkpoint_gb": f"{gb:.6f}",
                        "cumulative_tables": cum["events"],
                        "cumulative_rows": cum["rows"],
                        "cumulative_bytes_read": cum["plain_bytes"],
                        "task-total-time": f"{wall_ms:.6f}",
                        "abe_lookup_sum_ms": f"{d.get('abe_tee_ms', 0.0):.6f}",
                        "decrypt_time_ms": f"{d.get('ac_to_decrypt_ms', 0.0):.6f}",
                        "hdfs_read_time_ms": f"{d.get('io_read_ms', 0.0):.6f}",
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                    }
                )
            print(
                f"[集群 checkpoint] {gb:.1f} GB, 表={cum['events']}, "
                f"ABE-lookup={d.get('abe_tee_ms', 0.0):.1f}ms, "
                f"decrypt={d.get('ac_to_decrypt_ms', 0.0):.1f}ms"
            )
            prev_totals = _advance_prev(prev_totals, d, min(next_ck_bytes, cluster_bytes))
            if next_ck_bytes >= target:
                break
            next_ck_bytes += ck_bytes
            if all_done and next_ck_bytes > cluster_bytes and next_ck_bytes > target:
                break

        if any_error:
            print(f"[协调器] 节点错误: {any_error}")
            writer.close()
            return 1
        if all_done and cluster_bytes >= target * 0.999 and next_ck_bytes > target:
            break
        if all_done and cluster_bytes >= target * 0.999:
            # 最后一轮补齐
            while next_ck_bytes <= target:
                seg = min(ck_bytes, max(cluster_bytes - (next_ck_bytes - ck_bytes), 0)) or ck_bytes
                d = _proportional_delta(cur, prev_totals, cluster_bytes, seg)
                cum = _proportional_cumulative(cur, cluster_bytes, next_ck_bytes)
                wall_ms = (time.perf_counter() - wall_start) * 1000.0 + wall_offset_ms
                gb = bytes_to_gb(min(next_ck_bytes, target))
                if mode == "write":
                    writer.write_row(
                        {
                            "checkpoint_gb": f"{gb:.6f}",
                            "cumulative_tables": cum["events"],
                            "cumulative_rows": cum["rows"],
                            "cumulative_bytes": cum["plain_bytes"],
                            "ABE-size": cum["abe_size"],
                            "policy-size": cum["policy_size"],
                            "task-total-time": f"{wall_ms:.6f}",
                            "encrypt_time_ms": f"{d.get('abe_encrypt_ms', 0.0):.6f}",
                            "eac_time_sum_ms": f"{d.get('eac_tee_ms', 0.0):.6f}",
                            "hdfs_write_time_ms": f"{d.get('hdfs_write_ms', 0.0):.6f}",
                            "timestamp": datetime.now().isoformat(timespec="seconds"),
                        }
                    )
                else:
                    writer.write_row(
                        {
                            "checkpoint_gb": f"{gb:.6f}",
                            "cumulative_tables": cum["events"],
                            "cumulative_rows": cum["rows"],
                            "cumulative_bytes_read": cum["plain_bytes"],
                            "task-total-time": f"{wall_ms:.6f}",
                            "abe_lookup_sum_ms": f"{d.get('abe_tee_ms', 0.0):.6f}",
                            "decrypt_time_ms": f"{d.get('ac_to_decrypt_ms', 0.0):.6f}",
                            "hdfs_read_time_ms": f"{d.get('io_read_ms', 0.0):.6f}",
                            "timestamp": datetime.now().isoformat(timespec="seconds"),
                        }
                    )
                prev_totals = _advance_prev(prev_totals, d, min(next_ck_bytes, cluster_bytes))
                if next_ck_bytes >= target:
                    break
                next_ck_bytes += ck_bytes
            break
        time.sleep(1)

    writer.close()
    print(f"[协调器] 完成 -> {out_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["write", "read"])
    parser.add_argument("--result-dir", default=str(ROOT / "result" / "company"))
    parser.add_argument("--cluster-size", type=int, default=4)
    args = parser.parse_args()
    os.environ.setdefault("SGX_EXP_TARGET_GB", "100")
    os.environ.setdefault("SGX_EXP_CHECKPOINT_GB", "10")
    return run_coordinator(Path(args.result_dir), args.mode, args.cluster_size)


if __name__ == "__main__":
    raise SystemExit(main())
