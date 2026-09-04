"""100GB 读访问控制基准：ABE-TEE 开销与解密时延。"""

from __future__ import annotations

import csv
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.benchmark.acl import acl_decrypt_or_mask
from sgx_pyspark.benchmark.dataset import column_specs
from sgx_pyspark.benchmark.timing import CsvWriter, bytes_to_gb, ms_between, now_ns
from sgx_pyspark.config import SgxPysparkConfig, load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.factory import create_benchmark_stores, hdfs_mode
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.pipeline.worker_read import WorkerReadPipeline
from sgx_pyspark.tee.write_verification import WriteVerificationTee


def _access_mode() -> str:
    return os.environ.get("COMP_EXP_ACCESS_MODE", "abe").lower()


def _is_cluster_mode() -> bool:
    return os.environ.get("SGX_CLUSTER_MODE", "") == "1"


def _bench_store_name() -> str:
    return os.environ.get("COMP_EXP_BENCH_STORE", "bench_store")


def _target_bytes() -> int:
    gb = float(os.environ.get("SGX_EXP_TARGET_GB", "100"))
    return int(gb * 1024**3)


def _checkpoint_bytes() -> int:
    gb = float(os.environ.get("SGX_EXP_CHECKPOINT_GB", "10"))
    return int(gb * 1024**3)


def _num_workers() -> int:
    w = int(os.environ.get("SGX_EXP_WORKERS", str(min(32, os.cpu_count() or 4))))
    return max(1, w)


@dataclass
class ReadBenchContext:
    cfg: SgxPysparkConfig
    result_dir: Path
    data_root: Path
    abe: ABECrypto
    aes: AESGCMCrypto
    usk: object
    nne: NameNodeExtension
    dn: DataNodeStore
    pipeline: WorkerReadPipeline
    manifest: list[dict]
    nonce_bytes: int = 12
    tag_bytes: int = 16


@dataclass
class ReadSharedState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    next_idx: int = 0
    cumulative_bytes: int = 0
    event_id: int = 0
    error: str = ""
    ck_idx: int = 0
    ck_base: int = 0
    ck_events: int = 0
    ck_io: float = 0.0
    ck_meta: float = 0.0
    ck_abe: float = 0.0
    ck_ac_to_decrypt: float = 0.0
    ck_wall_start: float = field(default_factory=time.perf_counter)
    cluster_rows: int = 0
    cluster_plain_bytes: int = 0
    cluster_sums: dict[str, float] = field(default_factory=dict)
    cluster_wall_start: float = field(default_factory=time.perf_counter)


def load_manifest(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(f"write manifest not found: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _flush_read_checkpoint(st: ReadSharedState, ev: CsvWriter, cum: int) -> None:
    st.ck_idx += 1
    wall_ms = ms_between(st.ck_wall_start, time.perf_counter())
    avg = lambda s: s / st.ck_events if st.ck_events else 0.0
    ev.write_row(
        {
            "checkpoint_index": st.ck_idx,
            "checkpoint_gb": f"{bytes_to_gb(cum):.6f}",
            "events": st.ck_events,
            "bytes": cum,
            "sum_io_read_ms": f"{st.ck_io:.6f}",
            "avg_io_read_ms": f"{avg(st.ck_io):.6f}",
            "sum_meta_read_ms": f"{st.ck_meta:.6f}",
            "avg_meta_read_ms": f"{avg(st.ck_meta):.6f}",
            "sum_abe_tee_ms": f"{st.ck_abe:.6f}",
            "avg_abe_tee_ms": f"{avg(st.ck_abe):.6f}",
            "sum_ac_to_decrypt_ms": f"{st.ck_ac_to_decrypt:.6f}",
            "avg_ac_to_decrypt_ms": f"{avg(st.ck_ac_to_decrypt):.6f}",
            "wall_ms": f"{wall_ms:.6f}",
        }
    )
    print(f"[读 checkpoint {st.ck_idx}] {bytes_to_gb(cum):.2f} GB, {st.ck_events} 张表")
    st.ck_base = cum
    st.ck_events = 0
    st.ck_io = st.ck_meta = st.ck_abe = st.ck_ac_to_decrypt = 0.0
    st.ck_wall_start = time.perf_counter()


def _flush_cluster_read_progress(ctx: ReadBenchContext, st: ReadSharedState) -> None:
    from sgx_pyspark.benchmark.cluster_progress import NodeProgress, write_progress

    node = int(os.environ.get("SGX_CLUSTER_NODE", "0"))
    base = ctx.result_dir.parent if ctx.result_dir.name.startswith("node_") else ctx.result_dir
    write_progress(
        base,
        NodeProgress(
            node=node,
            events=st.event_id,
            rows=st.cluster_rows,
            enc_bytes=st.cumulative_bytes,
            plain_bytes=st.cluster_plain_bytes,
            sums={
                "io_read_ms": st.cluster_sums.get("io_read_ms", 0.0),
                "meta_persist_ms": st.cluster_sums.get("meta_persist_ms", 0.0),
                "abe_tee_ms": st.cluster_sums.get("abe_tee_ms", 0.0),
                "ac_to_decrypt_ms": st.cluster_sums.get("ac_to_decrypt_ms", 0.0),
            },
            wall_start=st.cluster_wall_start,
            done=False,
            error=st.error,
        ),
    )


def _read_one(ctx: ReadBenchContext, st: ReadSharedState, row: dict) -> None:
    hdfs_enc = row["hdfs_enc"]
    enc_bytes = int(row["enc_bytes"])
    table_name = row["table_name"]
    record_count = int(row["record_count"])

    ac_start_ns = now_ns()
    t0 = time.perf_counter()

    t_io0 = time.perf_counter()
    encrypted = ctx.dn.read_block(hdfs_enc)
    io_ms = ms_between(t_io0, time.perf_counter())

    t_meta0 = time.perf_counter()
    mode = _access_mode()
    abe_lookup_ms = 0.0
    abe_decrypt_ms = 0.0
    if mode == "acl":
        layout = ctx.nne.read_acl_layout(hdfs_enc)
        aes_keys = ctx.nne.read_acl_keys(hdfs_enc)
        meta_ms = ms_between(t_meta0, time.perf_counter())
        t_abe0 = time.perf_counter()
        result = acl_decrypt_or_mask(
            encrypted, layout, column_specs(), ctx.cfg.spark.user_attributes, aes_keys, ctx.aes, record_count, ctx.nonce_bytes
        )
        abe_decrypt_ms = ms_between(t_abe0, time.perf_counter())
        abe_ms = abe_decrypt_ms
    else:
        header = ctx.nne.read_security_header(hdfs_enc)
        layout = ctx.nne.read_column_layout(hdfs_enc)
        meta_ms = ms_between(t_meta0, time.perf_counter())
        t_lookup0 = time.perf_counter()
        plan = ctx.pipeline.build_access_plan(header, layout, ctx.abe, ctx.usk)
        abe_lookup_ms = ms_between(t_lookup0, time.perf_counter())
        t_dec0 = time.perf_counter()
        result = ctx.pipeline.process_stream(encrypted, plan, ctx.aes, ctx.nonce_bytes, ctx.tag_bytes)
        abe_decrypt_ms = ms_between(t_dec0, time.perf_counter())
        abe_ms = abe_lookup_ms + abe_decrypt_ms
    if not result.ok:
        raise RuntimeError(result.reason)

    ac_to_decrypt_ms = ms_between(t0, time.perf_counter())

    with st.lock:
        if st.cumulative_bytes >= _target_bytes():
            return
        st.event_id += 1
        eid = st.event_id
        new_cum = st.cumulative_bytes + enc_bytes
        st.cumulative_bytes = new_cum

        events: CsvWriter = ctx.__dict__["_events"]
        ck: CsvWriter = ctx.__dict__["_checkpoints"]

        events.write_row(
            {
                "event_id": eid,
                "table_name": table_name,
                "record_count": record_count,
                "enc_bytes": enc_bytes,
                "plain_bytes": len(result.plaintext),
                "access_control_start_ns": ac_start_ns,
                "io_read_ms": f"{io_ms:.6f}",
                "meta_read_ms": f"{meta_ms:.6f}",
                "abe_tee_ms": f"{abe_ms:.6f}",
                "ac_to_decrypt_ms": f"{ac_to_decrypt_ms:.6f}",
                "columns_authorized": result.columns_authorized,
                "columns_masked": result.columns_masked,
                "cumulative_gb": f"{bytes_to_gb(new_cum):.6f}",
                "hdfs_path": hdfs_enc,
            }
        )

        st.ck_events += 1
        st.ck_io += io_ms
        st.ck_meta += meta_ms
        st.ck_abe += abe_ms
        st.ck_ac_to_decrypt += ac_to_decrypt_ms

        if _is_cluster_mode():
            plain_bytes = len(result.plaintext)
            st.cluster_rows += record_count
            st.cluster_plain_bytes += plain_bytes
            for key, val in (
                ("io_read_ms", io_ms), ("meta_persist_ms", meta_ms),
                ("abe_tee_ms", abe_ms), ("ac_to_decrypt_ms", ac_to_decrypt_ms),
            ):
                st.cluster_sums[key] = st.cluster_sums.get(key, 0.0) + val

            # 每一次 ABE 查表/解密单独记录一行
            abe_ops: CsvWriter | None = ctx.__dict__.get("_abe_ops")
            if abe_ops is not None:
                abe_ops.write_row(
                    {
                        "op_id": eid,
                        "table_name": table_name,
                        "node": os.environ.get("SGX_CLUSTER_NODE", "0"),
                        "record_count": record_count,
                        "enc_bytes": enc_bytes,
                        "plain_bytes": plain_bytes,
                        "cumulative_gb": f"{bytes_to_gb(new_cum):.6f}",
                        "ABE-time": f"{abe_lookup_ms:.6f}",
                        "ABE-decrypt-time": f"{abe_decrypt_ms:.6f}",
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                )

            if eid % 20 == 0 or new_cum >= _target_bytes():
                _flush_cluster_read_progress(ctx, st)

        if not _is_cluster_mode() and (
            new_cum - st.ck_base >= _checkpoint_bytes() or new_cum >= _target_bytes()
        ):
            _flush_read_checkpoint(st, ck, new_cum)

        if eid % 500 == 0:
            print(f"[读] {eid} 张表, {bytes_to_gb(new_cum):.2f} GB")


def _read_worker(ctx: ReadBenchContext, st: ReadSharedState) -> None:
    while True:
        with st.lock:
            if st.cumulative_bytes >= _target_bytes() or st.error:
                return
            idx = st.next_idx
            st.next_idx += 1
        if idx >= len(ctx.manifest):
            return
        try:
            _read_one(ctx, st, ctx.manifest[idx])
        except Exception as exc:
            with st.lock:
                st.error = str(exc)
            return


def build_read_context(cfg: SgxPysparkConfig, result_dir: Path) -> ReadBenchContext:
    admin = Kgc(abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)))
    usk = admin.issue_user_attributes(cfg.spark.user_id, cfg.spark.user_attributes)
    data_root = Path(cfg.paths.data_root) / _bench_store_name()
    manifest = load_manifest(result_dir / "write_manifest.csv")
    dn, nne = create_benchmark_stores(cfg, _bench_store_name())

    return ReadBenchContext(
        cfg=cfg,
        result_dir=result_dir,
        data_root=data_root,
        abe=admin.abe,
        aes=AESGCMCrypto(),
        usk=usk,
        nne=nne,
        dn=dn,
        pipeline=WorkerReadPipeline(),
        manifest=manifest,
    )


def run_read_benchmark(result_dir: Path | None = None) -> int:
    cfg = load_config()
    out = result_dir or Path(cfg.paths.sgx_pyspark_root) / "Experimental-Result"
    if not (out / "write_manifest.csv").is_file():
        print("读 benchmark 需要先完成写 benchmark（write_manifest.csv 不存在）")
        return 1

    ctx = build_read_context(cfg, out)
    st = ReadSharedState()

    events = CsvWriter(
        out / "read_events.csv",
        [
            "event_id",
            "table_name",
            "record_count",
            "enc_bytes",
            "plain_bytes",
            "access_control_start_ns",
            "io_read_ms",
            "meta_read_ms",
            "abe_tee_ms",
            "ac_to_decrypt_ms",
            "columns_authorized",
            "columns_masked",
            "cumulative_gb",
            "hdfs_path",
        ],
    )
    checkpoints = CsvWriter(
        out / "read_checkpoints.csv",
        [
            "checkpoint_index",
            "checkpoint_gb",
            "events",
            "bytes",
            "sum_io_read_ms",
            "avg_io_read_ms",
            "sum_meta_read_ms",
            "avg_meta_read_ms",
            "sum_abe_tee_ms",
            "avg_abe_tee_ms",
            "sum_ac_to_decrypt_ms",
            "avg_ac_to_decrypt_ms",
            "wall_ms",
        ],
    )
    ctx.__dict__["_events"] = events
    ctx.__dict__["_checkpoints"] = checkpoints

    workers = _num_workers()
    print(f"读 benchmark 开始: workers={workers}, hdfs={hdfs_mode()}, 表数={len(ctx.manifest)}")

    wall0 = time.perf_counter()
    threads = [threading.Thread(target=_read_worker, args=(ctx, st), daemon=True) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    wall_ms = ms_between(wall0, time.perf_counter())
    events.close()
    checkpoints.close()

    meta = CsvWriter(out / "read_run_summary.csv", ["metric", "value"])
    meta.write_row({"metric": "total_events", "value": st.event_id})
    meta.write_row({"metric": "total_bytes", "value": st.cumulative_bytes})
    meta.write_row({"metric": "wall_ms", "value": f"{wall_ms:.6f}"})
    meta.write_row({"metric": "error", "value": st.error or "none"})
    meta.close()

    if st.error:
        print(f"读 benchmark 失败: {st.error}")
        return 1
    print(f"读 benchmark 完成, wall={wall_ms / 1000:.1f} s")
    print(f"读 benchmark 完成: {st.event_id} 张表, {bytes_to_gb(st.cumulative_bytes):.2f} GB")
    return 0
