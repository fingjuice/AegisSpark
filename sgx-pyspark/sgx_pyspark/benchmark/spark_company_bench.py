#!/usr/bin/env python3
"""四节点 Spark 分布式 100GB company benchmark（真实 HDFS + Occlum sim）。"""

from __future__ import annotations

import csv
import os
import random
import sys
import threading
import time
from pathlib import Path

# 集群模式强制真实 HDFS
os.environ.setdefault("SGX_HDFS_MODE", "real")


def _target_bytes() -> int:
    return int(float(os.environ.get("SGX_EXP_TARGET_GB", "100")) * 1024**3)


def _checkpoint_bytes() -> int:
    return int(float(os.environ.get("SGX_EXP_CHECKPOINT_GB", "10")) * 1024**3)


def _result_dir() -> Path:
    root = Path(os.environ.get("SGX_PYSPARK_ROOT", Path(__file__).resolve().parents[2]))
    return Path(os.environ.get("SGX_EXP_RESULT_DIR", str(root / "result" / "company")))


def _flush_checkpoints(rows: list[dict], ck_path: Path, ck_bytes: int, phase: str) -> None:
    from sgx_pyspark.benchmark.timing import bytes_to_gb, ms_between

    if not rows:
        return
    fieldnames = list(rows[0].keys())
    existing = []
    if ck_path.is_file() and ck_path.stat().st_size > 0:
        with ck_path.open(encoding="utf-8") as f:
            existing = list(csv.DictReader(f))

    cum = 0
    ck_idx = len(existing)
    ck_base = 0
    ck_events = 0
    sums: dict[str, float] = {}
    wall_start = time.perf_counter()
    new_rows: list[dict] = []

    def flush(cum_bytes: int) -> None:
        nonlocal ck_idx, ck_base, ck_events, sums, wall_start
        if ck_events == 0:
            return
        ck_idx += 1
        wall_ms = ms_between(wall_start, time.perf_counter())
        avg = lambda k: sums.get(k, 0.0) / ck_events if ck_events else 0.0
        row = {
            "checkpoint_index": ck_idx,
            "checkpoint_gb": f"{bytes_to_gb(cum_bytes):.6f}",
            "events": ck_events,
            "bytes": cum_bytes,
            "wall_ms": f"{wall_ms:.6f}",
        }
        for k, v in sums.items():
            row[f"sum_{k}"] = f"{v:.6f}"
            row[f"avg_{k}"] = f"{avg(k):.6f}"
        new_rows.append(row)
        print(f"[{phase} checkpoint {ck_idx}] {bytes_to_gb(cum_bytes):.2f} GB, {ck_events} 张表")
        ck_base = cum_bytes
        ck_events = 0
        sums = {}
        wall_start = time.perf_counter()

    for row in rows:
        cum += int(row.get("enc_bytes", row.get("bytes", 0)))
        ck_events += 1
        for key in ("gen_plain_ms", "abe_encrypt_ms", "eac_tee_ms", "hdfs_write_ms", "meta_persist_ms",
                    "total_ms", "ac_to_persist_ms", "io_read_ms", "meta_read_ms", "abe_tee_ms", "ac_to_decrypt_ms"):
            if key in row and row[key] != "":
                sums[key] = sums.get(key, 0.0) + float(row[key])
        if cum - ck_base >= ck_bytes or cum >= _target_bytes():
            flush(cum)

    if ck_events:
        flush(cum)

    all_rows = existing + new_rows
    with ck_path.open("w", newline="", encoding="utf-8") as f:
        if all_rows:
            w = csv.DictWriter(f, fieldnames=sorted({k for r in all_rows for k in r}))
            w.writeheader()
            w.writerows(all_rows)


def _write_partition(partition_id: int, num_partitions: int) -> list[dict]:
    from sgx_pyspark.benchmark.dataset import column_specs, make_table
    from sgx_pyspark.benchmark.encrypt import encrypt_table
    from sgx_pyspark.benchmark.write_bench import build_write_context
    from sgx_pyspark.benchmark.timing import ms_between, now_ns
    from sgx_pyspark.config import load_config

    cfg = load_config()
    result_dir = _result_dir()
    ctx = build_write_context(cfg, result_dir)
    target_per = _target_bytes() // num_partitions + (partition_id < _target_bytes() % num_partitions)
    cpabe_lock = threading.Lock()
    rng = random.Random(42 + partition_id * 7919)

    rows: list[dict] = []
    cum = 0
    company_id = partition_id * 10_000_000 + 1

    while cum < target_per:
        t0 = time.perf_counter()
        ac_start_ns = now_ns()

        t_gen0 = time.perf_counter()
        gen = make_table(company_id, rng, ctx.specs)
        gen_ms = ms_between(t_gen0, time.perf_counter())

        sub = gen.table_name[8:10]
        hdfs_enc = f"{ctx.hdfs_root}/bench/{sub}/{gen.table_name}.enc"

        t_abe0 = time.perf_counter()
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

        total_ms = ms_between(t0, time.perf_counter())
        ac_to_persist_ms = ms_between(t_abe0, time.perf_counter())

        rows.append({
            "partition_id": partition_id,
            "table_name": gen.table_name,
            "record_count": gen.record_count,
            "plain_bytes": enc.plain_bytes,
            "enc_bytes": enc.enc_bytes,
            "gen_plain_ms": f"{gen_ms:.6f}",
            "abe_encrypt_ms": f"{abe_ms:.6f}",
            "eac_tee_ms": f"{eac_ms:.6f}",
            "hdfs_write_ms": f"{hdfs_ms:.6f}",
            "meta_persist_ms": f"{meta_ms:.6f}",
            "total_ms": f"{total_ms:.6f}",
            "ac_to_persist_ms": f"{ac_to_persist_ms:.6f}",
            "hdfs_path": hdfs_enc,
            "access_control_start_ns": ac_start_ns,
        })
        cum += enc.enc_bytes
        company_id += num_partitions

    return rows


def _read_partition(manifest_rows: list[dict]) -> list[dict]:
    from sgx_pyspark.benchmark.read_bench import build_read_context
    from sgx_pyspark.benchmark.timing import ms_between, now_ns
    from sgx_pyspark.config import load_config

    cfg = load_config()
    result_dir = _result_dir()
    ctx = build_read_context(cfg, result_dir)
    rows: list[dict] = []

    for row in manifest_rows:
        hdfs_enc = row["hdfs_enc"]
        enc_bytes = int(row["enc_bytes"])
        table_name = row["table_name"]
        record_count = int(row["record_count"])
        plain_bytes = int(row.get("plain_bytes", 0))

        ac_start_ns = now_ns()
        t0 = time.perf_counter()

        t_io0 = time.perf_counter()
        encrypted = ctx.dn.read_block(hdfs_enc)
        io_ms = ms_between(t_io0, time.perf_counter())

        t_meta0 = time.perf_counter()
        header = ctx.nne.read_security_header(hdfs_enc)
        layout = ctx.nne.read_column_layout(hdfs_enc)
        meta_ms = ms_between(t_meta0, time.perf_counter())

        t_abe0 = time.perf_counter()
        plan = ctx.pipeline.build_access_plan(header, layout, ctx.abe, ctx.usk)
        result = ctx.pipeline.process_stream(encrypted, plan, ctx.aes, ctx.nonce_bytes, ctx.tag_bytes)
        abe_ms = ms_between(t_abe0, time.perf_counter())
        if not result.ok:
            raise RuntimeError(result.reason)

        ac_to_decrypt_ms = ms_between(t0, time.perf_counter())
        rows.append({
            "table_name": table_name,
            "record_count": record_count,
            "enc_bytes": enc_bytes,
            "plain_bytes": plain_bytes,
            "io_read_ms": f"{io_ms:.6f}",
            "meta_read_ms": f"{meta_ms:.6f}",
            "abe_tee_ms": f"{abe_ms:.6f}",
            "ac_to_decrypt_ms": f"{ac_to_decrypt_ms:.6f}",
            "columns_authorized": ",".join(result.authorized_columns),
            "columns_masked": ",".join(result.masked_columns),
            "hdfs_path": hdfs_enc,
            "access_control_start_ns": ac_start_ns,
        })
    return rows


def run_spark_write(spark, num_partitions: int) -> list[dict]:
    rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions)
    all_rows = rdd.flatMap(lambda pid: _write_partition(pid, num_partitions)).collect()
    return all_rows


def run_spark_read(spark, manifest: list[dict], num_partitions: int) -> list[dict]:
    chunks = [manifest[i::num_partitions] for i in range(num_partitions)]
    rdd = spark.sparkContext.parallelize(chunks, num_partitions)
    return rdd.flatMap(_read_partition).collect()


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for i, row in enumerate(rows, 1):
            row = dict(row)
            row["event_id"] = i
            if "enc_bytes" in row:
                from sgx_pyspark.benchmark.timing import bytes_to_gb
                cum = sum(int(r["enc_bytes"]) for r in rows[:i])
                row["cumulative_gb"] = f"{bytes_to_gb(cum):.6f}"
            w.writerow(row)


def main() -> int:
    from pyspark.sql import SparkSession
    from sgx_pyspark.benchmark.summary import write_timing_summary
    from sgx_pyspark.benchmark.timing import bytes_to_gb, ms_between

    mode = os.environ.get("BENCH_MODE", "all")
    result_dir = _result_dir()
    result_dir.mkdir(parents=True, exist_ok=True)
    num_partitions = int(os.environ.get("SGX_SPARK_PARTITIONS", os.environ.get("SPARK_DEFAULT_PARALLELISM", "496")))

    spark = (
        SparkSession.builder.appName("sgx-pyspark-company-benchmark")
        .config("spark.hadoop.fs.defaultFS", os.environ.get("HDFS_DEFAULT_FS", "hdfs://10.26.40.83:9000"))
        .getOrCreate()
    )

    print(f"=== sgx-pyspark 集群 benchmark ===")
    print(f"模式={mode}, HDFS=real, 分区={num_partitions}, 目标={bytes_to_gb(_target_bytes()):.1f}GB")
    print(f"Spark Master={spark.sparkContext.master}, Executors={spark.sparkContext.defaultParallelism}")
    print(f"结果目录={result_dir}")

    wall0 = time.perf_counter()

    if mode in ("write", "all"):
        print("[写阶段] 开始...")
        write_rows = run_spark_write(spark, num_partitions)
        _write_csv(
            result_dir / "write_events.csv",
            write_rows,
            ["event_id", "table_name", "record_count", "plain_bytes", "enc_bytes",
             "access_control_start_ns", "gen_plain_ms", "abe_encrypt_ms", "eac_tee_ms",
             "hdfs_write_ms", "meta_persist_ms", "total_ms", "ac_to_persist_ms",
             "cumulative_gb", "hdfs_path"],
        )
        manifest_fields = ["company_id", "table_name", "record_count", "plain_bytes", "enc_bytes", "hdfs_enc"]
        manifest_rows = []
        for r in write_rows:
            cid = int(r["table_name"].split("_")[1])
            manifest_rows.append({
                "company_id": cid,
                "table_name": r["table_name"],
                "record_count": r["record_count"],
                "plain_bytes": r["plain_bytes"],
                "enc_bytes": r["enc_bytes"],
                "hdfs_enc": r["hdfs_path"],
            })
        _write_csv(result_dir / "write_manifest.csv", manifest_rows, manifest_fields)
        _flush_checkpoints(write_rows, result_dir / "write_checkpoints.csv", _checkpoint_bytes(), "写")
        wall_ms = ms_between(wall0, time.perf_counter())
        with (result_dir / "write_run_summary.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["metric", "value"])
            w.writeheader()
            w.writerows([
                {"metric": "target_gb", "value": bytes_to_gb(_target_bytes())},
                {"metric": "partitions", "value": num_partitions},
                {"metric": "total_events", "value": len(write_rows)},
                {"metric": "wall_ms", "value": f"{wall_ms:.6f}"},
                {"metric": "hdfs_mode", "value": "real"},
            ])
        print(f"[写阶段] 完成: {len(write_rows)} 张表, wall={wall_ms/1000:.1f}s")

    if mode in ("read", "all"):
        print("[读阶段] 开始...")
        with (result_dir / "write_manifest.csv").open(encoding="utf-8") as f:
            manifest = list(csv.DictReader(f))
        read_rows = run_spark_read(spark, manifest, num_partitions)
        _write_csv(
            result_dir / "read_events.csv",
            read_rows,
            ["event_id", "table_name", "record_count", "enc_bytes", "plain_bytes",
             "access_control_start_ns", "io_read_ms", "meta_read_ms", "abe_tee_ms",
             "ac_to_decrypt_ms", "columns_authorized", "columns_masked",
             "cumulative_gb", "hdfs_path"],
        )
        _flush_checkpoints(read_rows, result_dir / "read_checkpoints.csv", _checkpoint_bytes(), "读")
        print(f"[读阶段] 完成: {len(read_rows)} 张表")

    write_timing_summary(result_dir)
    spark.stop()
    print(f"汇总: {result_dir / 'timing_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
