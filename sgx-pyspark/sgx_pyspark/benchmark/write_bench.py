"""100GB 写访问控制基准：Write-Verification-TEE 开销与落盘时延。"""

from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from sgx_pyspark.admin.kgc import Kgc
from sgx_pyspark.benchmark.acl import acl_encrypt_table
from sgx_pyspark.benchmark.dataset import column_specs, make_table
from sgx_pyspark.benchmark.encrypt import encrypt_table
from sgx_pyspark.benchmark.clean import clean_result_dir
from sgx_pyspark.benchmark.timing import CsvWriter, bytes_to_gb, ms_between, now_ns
from sgx_pyspark.config import SgxPysparkConfig, load_config
from sgx_pyspark.crypto.abe import ABECrypto
from sgx_pyspark.crypto.aes_gcm import AESGCMCrypto
from sgx_pyspark.crypto.msk_store import load_or_create_msk
from sgx_pyspark.hdfs.datanode import DataNodeStore
from sgx_pyspark.hdfs.factory import create_benchmark_stores, hdfs_mode
from sgx_pyspark.hdfs.namenode_ext import NameNodeExtension
from sgx_pyspark.tee.driver_plugin import SparkDriverTeePlugin
from sgx_pyspark.tee.write_verification import WriteVerificationTee
from sgx_pyspark.types import AdminEndorsement, TaskTicket


def _access_mode() -> str:
    return os.environ.get("COMP_EXP_ACCESS_MODE", "abe").lower()


def _bench_store_name() -> str:
    return os.environ.get("COMP_EXP_BENCH_STORE", "bench_store")


def _is_cluster_mode() -> bool:
    return os.environ.get("SGX_CLUSTER_MODE", "") == "1"


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
class WriteBenchContext:
    cfg: SgxPysparkConfig
    result_dir: Path
    data_root: Path
    abe: ABECrypto
    aes: AESGCMCrypto
    specs: list
    endorsement: AdminEndorsement
    ticket: TaskTicket
    wv: WriteVerificationTee
    nne: NameNodeExtension
    dn: DataNodeStore
    hdfs_root: str
    nonce_bytes: int = 12
    tag_bytes: int = 16


@dataclass
class WriteSharedState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    next_id: int = 1
    cumulative_bytes: int = 0
    event_id: int = 0
    error: str = ""
    ck_idx: int = 0
    ck_base: int = 0
    ck_events: int = 0
    ck_gen: float = 0.0
    ck_abe: float = 0.0
    ck_eac: float = 0.0
    ck_hdfs: float = 0.0
    ck_meta: float = 0.0
    ck_total: float = 0.0
    ck_ac_to_persist: float = 0.0
    ck_wall_start: float = field(default_factory=time.perf_counter)
    cluster_rows: int = 0
    cluster_plain_bytes: int = 0
    cluster_abe_size: int = 0
    cluster_policy_size: int = 0
    cluster_abe_header_bytes: int = 0
    cluster_sums: dict[str, float] = field(default_factory=dict)
    cluster_wall_start: float = field(default_factory=time.perf_counter)


def _flush_write_checkpoint(st: WriteSharedState, ev: CsvWriter, cum: int) -> None:
    st.ck_idx += 1
    wall_ms = ms_between(st.ck_wall_start, time.perf_counter())
    avg = lambda s: s / st.ck_events if st.ck_events else 0.0
    ev.write_row(
        {
            "checkpoint_index": st.ck_idx,
            "checkpoint_gb": f"{bytes_to_gb(cum):.6f}",
            "events": st.ck_events,
            "bytes": cum,
            "sum_gen_plain_ms": f"{st.ck_gen:.6f}",
            "avg_gen_plain_ms": f"{avg(st.ck_gen):.6f}",
            "sum_abe_encrypt_ms": f"{st.ck_abe:.6f}",
            "avg_abe_encrypt_ms": f"{avg(st.ck_abe):.6f}",
            "sum_eac_tee_ms": f"{st.ck_eac:.6f}",
            "avg_eac_tee_ms": f"{avg(st.ck_eac):.6f}",
            "sum_hdfs_write_ms": f"{st.ck_hdfs:.6f}",
            "avg_hdfs_write_ms": f"{avg(st.ck_hdfs):.6f}",
            "sum_meta_persist_ms": f"{st.ck_meta:.6f}",
            "avg_meta_persist_ms": f"{avg(st.ck_meta):.6f}",
            "sum_total_ms": f"{st.ck_total:.6f}",
            "avg_total_ms": f"{avg(st.ck_total):.6f}",
            "sum_ac_to_persist_ms": f"{st.ck_ac_to_persist:.6f}",
            "avg_ac_to_persist_ms": f"{avg(st.ck_ac_to_persist):.6f}",
            "wall_ms": f"{wall_ms:.6f}",
        }
    )
    print(f"[写 checkpoint {st.ck_idx}] {bytes_to_gb(cum):.2f} GB, {st.ck_events} 张表")
    st.ck_base = cum
    st.ck_events = 0
    st.ck_gen = st.ck_abe = st.ck_eac = st.ck_hdfs = st.ck_meta = st.ck_total = st.ck_ac_to_persist = 0.0
    st.ck_wall_start = time.perf_counter()


def _flush_cluster_progress(ctx: WriteBenchContext, st: WriteSharedState) -> None:
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
            abe_size=st.cluster_abe_size,
            policy_size=st.cluster_policy_size,
            abe_header_bytes=st.cluster_abe_size + st.cluster_policy_size,
            sums=dict(st.cluster_sums),
            wall_start=st.cluster_wall_start,
            done=False,
            error=st.error,
        ),
    )


def _write_one(ctx: WriteBenchContext, st: WriteSharedState, company_id: int, seed: int) -> None:
    rng = random.Random(seed)
    cpabe_lock = threading.Lock()

    ac_start_ns = now_ns()
    t0 = time.perf_counter()

    t_gen0 = time.perf_counter()
    gen = make_table(company_id, rng, ctx.specs)
    gen_ms = ms_between(t_gen0, time.perf_counter())

    sub = gen.table_name[8:10]
    mode = _access_mode()
    ext = ".dat" if mode == "acl" else ".enc"
    hdfs_enc = f"{ctx.hdfs_root}/bench/{sub}/{gen.table_name}{ext}"

    t_abe0 = time.perf_counter()
    if mode == "acl":
        enc_table = acl_encrypt_table(
            gen, ctx.specs, ctx.aes, hdfs_enc, ctx.cfg.spark.user_attributes, ctx.nonce_bytes, ctx.tag_bytes
        )
        t_hdfs0 = time.perf_counter()
        enc_bytes = ctx.dn.write_block(hdfs_enc, enc_table.payload)
        hdfs_ms = ms_between(t_hdfs0, time.perf_counter())
        t_meta0 = time.perf_counter()
        policies = {s.name: s.policy for s in ctx.specs}
        ctx.nne.persist_acl_meta(hdfs_enc, enc_table.column_layout, policies, enc_table.aes_keys)
        meta_ms = ms_between(t_meta0, time.perf_counter())
        abe_ms = enc_table.aes_ms
        eac_ms = enc_table.acl_ms
        plain_bytes = enc_table.plain_bytes
        table_abe_size = 0
        table_policy_size = 0
    else:
        enc = encrypt_table(gen, ctx.abe, ctx.aes, hdfs_enc, ctx.specs, ctx.nonce_bytes, ctx.tag_bytes, cpabe_lock)
        abe_ms = ms_between(t_abe0, time.perf_counter())

        t_eac0 = time.perf_counter()
        verify = ctx.wv.verify_write_chain(ctx.ticket, ctx.endorsement, hdfs_enc)
        if not verify.ok:
            raise RuntimeError(f"Write Verification failed: {verify.reason}")
        ctx.wv.counter.increment(ctx.wv.counter_ns)
        eac_ms = ms_between(t_eac0, time.perf_counter())

        t_hdfs0 = time.perf_counter()
        ctx.dn.write_block(hdfs_enc, enc.ciphertext)
        hdfs_ms = ms_between(t_hdfs0, time.perf_counter())

        t_meta0 = time.perf_counter()
        ctx.nne.persist_meta(hdfs_enc, enc.header, enc.ranges)
        meta_ms = ms_between(t_meta0, time.perf_counter())
        plain_bytes = enc.plain_bytes
        enc_bytes = enc.enc_bytes
        table_abe_size = enc.abe_size
        table_policy_size = enc.policy_size

    total_ms = ms_between(t0, time.perf_counter())
    ac_to_persist_ms = ms_between(t_abe0, time.perf_counter())

    with st.lock:
        if st.cumulative_bytes >= _target_bytes():
            return
        st.event_id += 1
        eid = st.event_id
        new_cum = st.cumulative_bytes + enc_bytes
        st.cumulative_bytes = new_cum

        events_writer: CsvWriter = ctx.__dict__["_events"]  # type: ignore
        manifest_writer: CsvWriter = ctx.__dict__["_manifest"]  # type: ignore
        ck_writer: CsvWriter = ctx.__dict__["_checkpoints"]  # type: ignore

        events_writer.write_row(
            {
                "event_id": eid,
                "table_name": gen.table_name,
                "record_count": gen.record_count,
                "plain_bytes": plain_bytes,
                "enc_bytes": enc_bytes,
                "access_control_start_ns": ac_start_ns,
                "gen_plain_ms": f"{gen_ms:.6f}",
                "abe_encrypt_ms": f"{abe_ms:.6f}",
                "eac_tee_ms": f"{eac_ms:.6f}",
                "hdfs_write_ms": f"{hdfs_ms:.6f}",
                "meta_persist_ms": f"{meta_ms:.6f}",
                "total_ms": f"{total_ms:.6f}",
                "ac_to_persist_ms": f"{ac_to_persist_ms:.6f}",
                "cumulative_gb": f"{bytes_to_gb(new_cum):.6f}",
                "hdfs_path": hdfs_enc,
            }
        )
        manifest_writer.write_row(
            {
                "company_id": company_id,
                "table_name": gen.table_name,
                "record_count": gen.record_count,
                "plain_bytes": plain_bytes,
                "enc_bytes": enc_bytes,
                "hdfs_enc": hdfs_enc,
            }
        )

        st.ck_events += 1
        st.ck_gen += gen_ms
        st.ck_abe += abe_ms
        st.ck_eac += eac_ms
        st.ck_hdfs += hdfs_ms
        st.ck_meta += meta_ms
        st.ck_total += total_ms
        st.ck_ac_to_persist += ac_to_persist_ms

        if _is_cluster_mode():
            st.cluster_rows += gen.record_count
            st.cluster_plain_bytes += plain_bytes
            st.cluster_abe_size += table_abe_size
            st.cluster_policy_size += table_policy_size
            st.cluster_abe_header_bytes = st.cluster_abe_size + st.cluster_policy_size
            for key, val in (
                ("gen_plain_ms", gen_ms), ("abe_encrypt_ms", abe_ms), ("eac_tee_ms", eac_ms),
                ("hdfs_write_ms", hdfs_ms), ("meta_persist_ms", meta_ms), ("total_ms", total_ms),
            ):
                st.cluster_sums[key] = st.cluster_sums.get(key, 0.0) + val

            # 每一次 EAC / ABE 加密单独记录一行
            eac_ops: CsvWriter | None = ctx.__dict__.get("_eac_ops")
            abe_enc_ops: CsvWriter | None = ctx.__dict__.get("_abe_encrypt_ops")
            if eac_ops is not None:
                eac_ops.write_row(
                    {
                        "op_id": eid,
                        "table_name": gen.table_name,
                        "node": os.environ.get("SGX_CLUSTER_NODE", "0"),
                        "record_count": gen.record_count,
                        "plain_bytes": plain_bytes,
                        "cumulative_gb": f"{bytes_to_gb(new_cum):.6f}",
                        "EAC-time": f"{eac_ms:.6f}",
                        "ABE-size": table_abe_size,
                        "policy-size": table_policy_size,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                )
            if abe_enc_ops is not None:
                abe_enc_ops.write_row(
                    {
                        "op_id": eid,
                        "table_name": gen.table_name,
                        "node": os.environ.get("SGX_CLUSTER_NODE", "0"),
                        "record_count": gen.record_count,
                        "plain_bytes": plain_bytes,
                        "cumulative_gb": f"{bytes_to_gb(new_cum):.6f}",
                        "ABE-encrypt-time": f"{abe_ms:.6f}",
                        "ABE-size": table_abe_size,
                        "policy-size": table_policy_size,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                )

            if eid % 20 == 0 or new_cum >= _target_bytes():
                _flush_cluster_progress(ctx, st)

        if not _is_cluster_mode() and (
            new_cum - st.ck_base >= _checkpoint_bytes() or new_cum >= _target_bytes()
        ):
            _flush_write_checkpoint(st, ck_writer, new_cum)

        if eid % 500 == 0:
            print(f"[写] {eid} 张表, {bytes_to_gb(new_cum):.2f} GB")


def _write_worker(ctx: WriteBenchContext, st: WriteSharedState, worker_id: int) -> None:
    seed = 42 + worker_id * 7919
    while True:
        with st.lock:
            if st.cumulative_bytes >= _target_bytes() or st.error:
                return
            cid = st.next_id
            st.next_id += 1
        try:
            _write_one(ctx, st, cid, seed + cid)
        except Exception as exc:
            with st.lock:
                st.error = str(exc)
            return


def build_write_context(cfg: SgxPysparkConfig, result_dir: Path) -> WriteBenchContext:
    admin = Kgc(
        abe=ABECrypto(msk=load_or_create_msk(cfg.paths.keys_dir)),
        admin_private_key_path=cfg.signature.admin_private_key_path,
    )
    admin.ensure_keypair(cfg.signature.admin_private_key_path, cfg.signature.admin_public_key_path)
    admin.ensure_keypair(cfg.signature.driver_private_key_path, cfg.signature.driver_public_key_path)

    hdfs_root = cfg.paths.hdfs_data_root.rstrip("/")
    data_root = Path(cfg.paths.data_root) / _bench_store_name()
    endorsement = admin.endorse_spark_job(b"sgx-pyspark-100gb-bench", [hdfs_root + "/"], cfg.signature.admin_private_key_path)

    driver = SparkDriverTeePlugin(cfg.signature.driver_private_key_path, task_ticket_ttl_sec=cfg.spark.task_ticket_ttl_sec)
    driver.initialize_job(cfg.tee.write_verification_url, endorsement)
    ticket = driver.dispense_task_ticket("sgx-bench-100gb", "task-0", "127.0.0.1", hdfs_root + "/")

    dn, nne = create_benchmark_stores(cfg, _bench_store_name())
    wv = WriteVerificationTee(
        cfg.signature.admin_public_key_path,
        cfg.signature.driver_public_key_path,
        dn,
        counter_namespace=cfg.tee.monotonic_counter_namespace,
    )

    return WriteBenchContext(
        cfg=cfg,
        result_dir=result_dir,
        data_root=data_root,
        abe=admin.abe,
        aes=AESGCMCrypto(),
        specs=column_specs(),
        endorsement=endorsement,
        ticket=ticket,
        wv=wv,
        nne=nne,
        dn=dn,
        hdfs_root=hdfs_root,
    )


def run_write_benchmark(result_dir: Path | None = None, clean: bool = True) -> int:
    cfg = load_config()
    out = result_dir or Path(cfg.paths.sgx_pyspark_root) / "Experimental-Result"
    out.mkdir(parents=True, exist_ok=True)
    if clean:
        clean_result_dir(out, keep_log=True)

    ctx = build_write_context(cfg, out)
    st = WriteSharedState()

    events = CsvWriter(
        out / "write_events.csv",
        [
            "event_id",
            "table_name",
            "record_count",
            "plain_bytes",
            "enc_bytes",
            "access_control_start_ns",
            "gen_plain_ms",
            "abe_encrypt_ms",
            "eac_tee_ms",
            "hdfs_write_ms",
            "meta_persist_ms",
            "total_ms",
            "ac_to_persist_ms",
            "cumulative_gb",
            "hdfs_path",
        ],
    )
    checkpoints = CsvWriter(
        out / "write_checkpoints.csv",
        [
            "checkpoint_index",
            "checkpoint_gb",
            "events",
            "bytes",
            "sum_gen_plain_ms",
            "avg_gen_plain_ms",
            "sum_abe_encrypt_ms",
            "avg_abe_encrypt_ms",
            "sum_eac_tee_ms",
            "avg_eac_tee_ms",
            "sum_hdfs_write_ms",
            "avg_hdfs_write_ms",
            "sum_meta_persist_ms",
            "avg_meta_persist_ms",
            "sum_total_ms",
            "avg_total_ms",
            "sum_ac_to_persist_ms",
            "avg_ac_to_persist_ms",
            "wall_ms",
        ],
    )
    manifest = CsvWriter(
        out / "write_manifest.csv",
        ["company_id", "table_name", "record_count", "plain_bytes", "enc_bytes", "hdfs_enc"],
    )
    ctx.__dict__["_events"] = events
    ctx.__dict__["_checkpoints"] = checkpoints
    ctx.__dict__["_manifest"] = manifest

    workers = _num_workers()
    target_gb = bytes_to_gb(_target_bytes())
    ck_gb = bytes_to_gb(_checkpoint_bytes())
    print(f"写 benchmark 开始: workers={workers}, hdfs={hdfs_mode()}, 目标={target_gb:.1f} GB, checkpoint={ck_gb:.1f} GB")

    wall0 = time.perf_counter()
    threads = []
    for w in range(workers):
        t = threading.Thread(target=_write_worker, args=(ctx, st, w), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    wall_ms = ms_between(wall0, time.perf_counter())
    events.close()
    checkpoints.close()
    manifest.close()

    meta = CsvWriter(out / "write_run_summary.csv", ["metric", "value"])
    meta.write_row({"metric": "target_gb", "value": target_gb})
    meta.write_row({"metric": "checkpoint_gb", "value": ck_gb})
    meta.write_row({"metric": "workers", "value": workers})
    meta.write_row({"metric": "total_events", "value": st.event_id})
    meta.write_row({"metric": "total_bytes", "value": st.cumulative_bytes})
    meta.write_row({"metric": "wall_ms", "value": f"{wall_ms:.6f}"})
    meta.write_row({"metric": "error", "value": st.error or "none"})
    meta.close()

    if st.error:
        print(f"写 benchmark 失败: {st.error}")
        return 1
    print(f"写 benchmark 完成, wall={wall_ms / 1000:.1f} s")
    print(f"写 benchmark 完成: {st.event_id} 张表, {bytes_to_gb(st.cumulative_bytes):.2f} GB")
    return 0
