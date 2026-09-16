"""四节点集群分片 benchmark：每节点独立进程，共享真实 HDFS，协调器实时聚合 checkpoint。"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from sgx_pyspark.benchmark.cluster_progress import NodeProgress, write_progress
from sgx_pyspark.benchmark.timing import CsvWriter, bytes_to_gb, ms_between
from sgx_pyspark.benchmark.write_bench import (
    WriteBenchContext,
    WriteSharedState,
    _flush_cluster_progress,
    _num_workers,
    _target_bytes,
    _write_one,
    build_write_context,
)
from sgx_pyspark.benchmark.read_bench import (
    ReadSharedState,
    _flush_cluster_read_progress,
    _read_worker,
    build_read_context,
    load_manifest,
)
from sgx_pyspark.config import load_config
from sgx_pyspark.hdfs.factory import hdfs_mode


def _cluster_node() -> int:
    return int(os.environ.get("SGX_CLUSTER_NODE", "0"))


def _cluster_size() -> int:
    return int(os.environ.get("SGX_CLUSTER_SIZE", "4"))


def _node_result_dir(base: Path) -> Path:
    node = _cluster_node()
    d = base / f"node_{node}"
    d.mkdir(parents=True, exist_ok=True)
    return d


class ClusterWriteState(WriteSharedState):
    def __init__(self) -> None:
        super().__init__()
        self.next_id = _cluster_node() + 1


def _cluster_write_worker(ctx: WriteBenchContext, st: ClusterWriteState, worker_id: int) -> None:
    seed = 42 + worker_id * 7919 + _cluster_node() * 100003
    target = _target_bytes()
    while True:
        with st.lock:
            if st.cumulative_bytes >= target or st.error:
                return
            cid = st.next_id
            st.next_id += _cluster_size()
        try:
            _write_one(ctx, st, cid, seed + cid)
        except Exception as exc:
            with st.lock:
                st.error = str(exc)
            return
        with st.lock:
            if st.cumulative_bytes >= target:
                return


def _finalize_progress(result_dir: Path, st: WriteSharedState | ReadSharedState, mode: str) -> None:
    node = _cluster_node()
    prog = NodeProgress(
        node=node,
        events=st.event_id,
        rows=getattr(st, "cluster_rows", 0),
        enc_bytes=st.cumulative_bytes,
        plain_bytes=getattr(st, "cluster_plain_bytes", 0),
        abe_size=getattr(st, "cluster_abe_size", 0),
        policy_size=getattr(st, "cluster_policy_size", 0),
        abe_header_bytes=getattr(st, "cluster_abe_size", 0) + getattr(st, "cluster_policy_size", 0),
        sums=dict(getattr(st, "cluster_sums", {})),
        wall_start=getattr(st, "cluster_wall_start", time.time()),
        done=True,
        error=st.error,
    )
    write_progress(result_dir, prog)


def run_cluster_write(result_dir: Path) -> int:
    os.environ["SGX_CLUSTER_MODE"] = "1"
    cfg = load_config()
    node_dir = _node_result_dir(result_dir)
    ctx = build_write_context(cfg, node_dir)
    st = ClusterWriteState()
    target = _target_bytes()

    events = CsvWriter(
        node_dir / "write_events.csv",
        ["event_id", "table_name", "record_count", "plain_bytes", "enc_bytes",
         "access_control_start_ns", "gen_plain_ms", "abe_encrypt_ms", "eac_tee_ms",
         "hdfs_write_ms", "meta_persist_ms", "total_ms", "ac_to_persist_ms",
         "cumulative_gb", "hdfs_path"],
    )
    manifest = CsvWriter(
        node_dir / "write_manifest.csv",
        ["company_id", "table_name", "record_count", "plain_bytes", "enc_bytes", "hdfs_enc"],
    )
    eac_ops = CsvWriter(
        node_dir / "eac_time.csv",
        ["op_id", "table_name", "node", "record_count", "plain_bytes", "cumulative_gb",
         "EAC-time", "ABE-size", "policy-size", "timestamp"],
    )
    abe_enc_ops = CsvWriter(
        node_dir / "abe_encrypt_time.csv",
        ["op_id", "table_name", "node", "record_count", "plain_bytes", "cumulative_gb",
         "ABE-encrypt-time", "ABE-size", "policy-size", "timestamp"],
    )
    ctx.__dict__["_events"] = events
    ctx.__dict__["_manifest"] = manifest
    ctx.__dict__["_eac_ops"] = eac_ops
    ctx.__dict__["_abe_encrypt_ops"] = abe_enc_ops
    ctx.__dict__["_checkpoints"] = CsvWriter(node_dir / "write_checkpoints.csv", ["checkpoint_index"])

    write_progress(result_dir, NodeProgress(node=_cluster_node(), wall_start=time.time()))

    workers = _num_workers()
    node = _cluster_node()
    print(f"[节点{node}] 写开始: workers={workers}, hdfs={hdfs_mode()}, 目标={bytes_to_gb(target):.2f}GB")

    wall0 = time.perf_counter()
    threads = [threading.Thread(target=_cluster_write_worker, args=(ctx, st, w), daemon=True) for w in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    _flush_cluster_progress(ctx, st)
    _finalize_progress(result_dir, st, "write")

    wall_ms = ms_between(wall0, time.perf_counter())
    events.close()
    manifest.close()
    eac_ops.close()
    abe_enc_ops.close()

    meta = CsvWriter(node_dir / "write_run_summary.csv", ["metric", "value"])
    meta.write_row({"metric": "cluster_node", "value": node})
    meta.write_row({"metric": "target_gb", "value": bytes_to_gb(target)})
    meta.write_row({"metric": "total_events", "value": st.event_id})
    meta.write_row({"metric": "total_bytes", "value": st.cumulative_bytes})
    meta.write_row({"metric": "wall_ms", "value": f"{wall_ms:.6f}"})
    meta.write_row({"metric": "hdfs_mode", "value": hdfs_mode()})
    meta.write_row({"metric": "error", "value": st.error or "none"})
    meta.close()

    if st.error:
        print(f"[节点{node}] 写失败: {st.error}")
        return 1
    print(f"[节点{node}] 写完成: {st.event_id} 张表, {bytes_to_gb(st.cumulative_bytes):.2f}GB, wall={wall_ms/1000:.1f}s")
    return 0


def run_cluster_read(result_dir: Path) -> int:
    os.environ["SGX_CLUSTER_MODE"] = "1"
    cfg = load_config()
    node = _cluster_node()
    node_dir = _node_result_dir(result_dir)

    manifest_path = node_dir / "write_manifest.csv"
    if not manifest_path.is_file():
        print(f"[节点{node}] 缺少 {manifest_path}")
        return 1

    my_rows = load_manifest(manifest_path)
    ctx = build_read_context(cfg, node_dir)
    ctx.manifest = my_rows
    st = ReadSharedState()

    events = CsvWriter(
        node_dir / "read_events.csv",
        ["event_id", "table_name", "record_count", "enc_bytes", "plain_bytes",
         "access_control_start_ns", "io_read_ms", "meta_read_ms", "abe_tee_ms",
         "ac_to_decrypt_ms", "columns_authorized", "columns_masked",
         "cumulative_gb", "hdfs_path"],
    )
    abe_ops = CsvWriter(
        node_dir / "abe_time.csv",
        ["op_id", "table_name", "node", "record_count", "enc_bytes", "plain_bytes",
         "cumulative_gb", "ABE-time", "ABE-decrypt-time", "timestamp"],
    )
    ctx.__dict__["_events"] = events
    ctx.__dict__["_abe_ops"] = abe_ops
    ctx.__dict__["_checkpoints"] = CsvWriter(node_dir / "read_checkpoints.csv", ["checkpoint_index"])

    write_progress(result_dir, NodeProgress(node=node, wall_start=time.time()))

    workers = _num_workers()
    print(f"[节点{node}] 读开始: workers={workers}, 表数={len(my_rows)}")

    wall0 = time.perf_counter()
    threads = [threading.Thread(target=_read_worker, args=(ctx, st), daemon=True) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    _flush_cluster_read_progress(ctx, st)
    _finalize_progress(result_dir, st, "read")

    wall_ms = ms_between(wall0, time.perf_counter())
    events.close()
    abe_ops.close()

    if st.error:
        print(f"[节点{node}] 读失败: {st.error}")
        return 1
    print(f"[节点{node}] 读完成: {st.event_id} 张表, wall={wall_ms/1000:.1f}s")
    return 0


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["write", "read"])
    parser.add_argument("--result-dir", default="")
    args = parser.parse_args()

    cfg = load_config()
    result_dir = Path(args.result_dir) if args.result_dir else Path(cfg.paths.sgx_pyspark_root) / "result" / "company"
    result_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "write":
        return run_cluster_write(result_dir)
    return run_cluster_read(result_dir)


if __name__ == "__main__":
    raise SystemExit(main())
