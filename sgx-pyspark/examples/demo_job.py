#!/usr/bin/env python3
"""PySpark 演示作业（Occlum TEE 内运行）。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.pyspark_salary_job import main

if __name__ == "__main__":
    main()
