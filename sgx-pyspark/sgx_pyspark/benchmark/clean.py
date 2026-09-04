"""清理实验结果目录，保留 run.log 可选。"""

from __future__ import annotations

from pathlib import Path

RESULT_FILES = (
    "write_events.csv",
    "write_checkpoints.csv",
    "write_manifest.csv",
    "write_run_summary.csv",
    "read_events.csv",
    "read_checkpoints.csv",
    "read_run_summary.csv",
    "timing_summary.csv",
    "experiment_metadata.csv",
    "run.pid",
)


def clean_result_dir(result_dir: Path, keep_log: bool = False) -> None:
    result_dir.mkdir(parents=True, exist_ok=True)
    for name in RESULT_FILES:
        p = result_dir / name
        if p.exists():
            p.unlink()
    if not keep_log and (result_dir / "run.log").exists():
        (result_dir / "run.log").unlink()
