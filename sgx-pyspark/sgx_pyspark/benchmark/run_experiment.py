"""100GB ABE/Write-Verification 访问控制实验入口。"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from sgx_pyspark.benchmark.clean import clean_result_dir
from sgx_pyspark.benchmark.read_bench import run_read_benchmark
from sgx_pyspark.benchmark.summary import write_timing_summary
from sgx_pyspark.benchmark.timing import CsvWriter, bytes_to_gb
from sgx_pyspark.benchmark.write_bench import run_write_benchmark
from sgx_pyspark.config import load_config


def _write_experiment_metadata(result_dir: Path) -> None:
    cfg = load_config()
    meta = CsvWriter(result_dir / "experiment_metadata.csv", ["key", "value"])
    meta.write_row({"key": "timestamp", "value": datetime.now().isoformat(timespec="seconds")})
    meta.write_row({"key": "target_gb", "value": os.environ.get("SGX_EXP_TARGET_GB", "100")})
    meta.write_row({"key": "checkpoint_gb", "value": os.environ.get("SGX_EXP_CHECKPOINT_GB", "10")})
    meta.write_row({"key": "access_mode", "value": os.environ.get("COMP_EXP_ACCESS_MODE", "abe")})
    meta.write_row({"key": "system", "value": os.environ.get("COMP_EXP_SYSTEM", "sgx-pyspark")})
    meta.write_row({"key": "workers", "value": os.environ.get("SGX_EXP_WORKERS", str(min(32, os.cpu_count() or 4)))})
    meta.write_row({"key": "tee_mode", "value": cfg.tee.tee_mode})
    meta.write_row({"key": "hdfs_data_root", "value": cfg.paths.hdfs_data_root})
    meta.write_row({"key": "user_id", "value": cfg.spark.user_id})
    meta.write_row({"key": "user_attributes", "value": ",".join(cfg.spark.user_attributes)})
    meta.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="sgx-pyspark 100GB 访问控制实验")
    parser.add_argument("mode", choices=["write", "read", "all"], help="write|read|all")
    parser.add_argument(
        "--result-dir",
        default="",
        help="结果目录（默认: $SGX_PYSPARK_ROOT/Experimental-Result）",
    )
    args = parser.parse_args(argv)

    cfg = load_config()
    result_dir = Path(args.result_dir) if args.result_dir else Path(cfg.paths.sgx_pyspark_root) / "Experimental-Result"
    result_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "all":
        clean_result_dir(result_dir, keep_log=True)

    _write_experiment_metadata(result_dir)

    target = bytes_to_gb(int(float(os.environ.get("SGX_EXP_TARGET_GB", "100")) * 1024**3))
    ck = bytes_to_gb(int(float(os.environ.get("SGX_EXP_CHECKPOINT_GB", "10")) * 1024**3))
    print(f"实验结果目录: {result_dir}")
    print(f"目标数据量: {target:.1f} GB, checkpoint 间隔: {ck:.1f} GB")

    rc = 0
    if args.mode in ("write", "all"):
        rc = run_write_benchmark(result_dir, clean=(args.mode == "write"))
        if rc != 0:
            return rc
    if args.mode in ("read", "all"):
        rc = run_read_benchmark(result_dir)
    if rc == 0:
        write_timing_summary(result_dir)
        print(f"汇总已写入: {result_dir / 'timing_summary.csv'}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
