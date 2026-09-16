#!/usr/bin/env python3
"""sgx-pyspark CLI：Admin / 读管道 / 写管道 / Occlum 演示。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def cmd_init_keys(_: argparse.Namespace) -> int:
    from scripts.gen_keys import main as gen_keys
    return gen_keys()


def cmd_read_demo(_: argparse.Namespace) -> int:
    from examples.demo_read_pipeline import main
    main()
    return 0


def cmd_write_demo(_: argparse.Namespace) -> int:
    from examples.demo_write_pipeline import main
    main()
    return 0


def cmd_pyspark_demo(_: argparse.Namespace) -> int:
    from examples.pyspark_salary_job import main
    main()
    return 0


def cmd_pyspark_compute_demo(_: argparse.Namespace) -> int:
    from examples.pyspark_tee_compute_job import main
    main()
    return 0


def cmd_pipeline(_: argparse.Namespace) -> int:
    """端到端：读管道 + 写管道。"""
    cmd_read_demo(_)
    cmd_write_demo(_)
    return 0


def cmd_occlum_info(_: argparse.Namespace) -> int:
    from sgx_pyspark.tee.occlum_runtime import detect_tee_mode, is_occlum_runtime
    from sgx_pyspark.native_bridge import native_available

    info = {
        "tee_mode": detect_tee_mode(),
        "occlum_runtime": is_occlum_runtime(),
        "native_ffi_available": native_available(),
    }
    print(json.dumps(info, indent=2, ensure_ascii=False))
    return 0


def cmd_benchmark(args: argparse.Namespace) -> int:
    from sgx_pyspark.benchmark.run_experiment import main as bench_main
    argv = [args.mode]
    if args.result_dir:
        argv.extend(["--result-dir", args.result_dir])
    return bench_main(argv)


def main() -> int:
    parser = argparse.ArgumentParser(description="sgx-pyspark CLI (Occlum LibOS TEE)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-keys", help="Generate Admin/Driver/WV Ed25519 keys").set_defaults(func=cmd_init_keys)
    sub.add_parser("read-demo", help="读管道演示 (design.md Setup & Read)").set_defaults(func=cmd_read_demo)
    sub.add_parser("write-demo", help="写管道演示 (design.md Write Verification)").set_defaults(func=cmd_write_demo)
    sub.add_parser("pyspark-demo", help="PySpark + TEE 内计算算子（读+写）").set_defaults(func=cmd_pyspark_demo)
    sub.add_parser("pyspark-compute-demo", help="PySpark mapPartitions TEE 多分区计算").set_defaults(func=cmd_pyspark_compute_demo)
    sub.add_parser("pipeline", help="端到端读+写管道").set_defaults(func=cmd_pipeline)
    sub.add_parser("occlum-info", help="TEE/Occlum 运行时信息").set_defaults(func=cmd_occlum_info)
    bench = sub.add_parser("benchmark", help="100GB ABE/Write-Verification 访问控制实验")
    bench.add_argument("mode", choices=["write", "read", "all"], nargs="?", default="all")
    bench.add_argument("--result-dir", default="", help="结果目录 (默认 Experimental-Result)")
    bench.set_defaults(func=cmd_benchmark)

    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
