"""TEE 内计算算子：仅在 TeeComputeContext 上操作，禁止明文外泄。"""

from __future__ import annotations

from typing import Any, Callable

from sgx_pyspark.tee.compute_context import TeeComputeContext

TeeOperator = Callable[[TeeComputeContext], dict[str, Any]]


def _strip_null(value: str) -> str:
    return value.replace("\x00", "").strip()


def aggregate_count(ctx: TeeComputeContext) -> dict[str, Any]:
    """统计单条记录的可访问列数（聚合结果可离开 TEE）。"""
    return {
        "record_count": 1,
        "columns_authorized": ctx.authorized_column_count(),
        "columns_masked": ctx.masked_column_count(),
    }


def dept_stats(ctx: TeeComputeContext) -> dict[str, Any]:
    """按部门统计：仅输出聚合指标，不返回完整明文行。"""
    dept = _strip_null(ctx.get_column("Dept")) if ctx.is_authorized("Dept") else "MASKED"
    name_len = len(_strip_null(ctx.get_column("Name"))) if ctx.is_authorized("Name") else 0
    salary_masked = not ctx.is_authorized("Salary")
    return {
        "record_count": 1,
        "dept": dept,
        "name_length": name_len,
        "salary_masked": salary_masked,
        "authorized_columns": ctx.authorized_column_names(),
    }


def project_authorized_columns(ctx: TeeComputeContext) -> dict[str, Any]:
    """投影已授权列的非敏感摘要（哈希前缀），用于分布式聚合演示。"""
    summary: dict[str, str] = {}
    for col in ctx.authorized_column_names():
        raw = _strip_null(ctx.get_column(col))
        summary[col] = raw[:3] + "..." if len(raw) > 3 else raw
    return {"column_summary": summary}


def normalize_for_output(ctx: TeeComputeContext) -> dict[str, bytes]:
    """
    写路径算子：在 TEE 内变换列值，输出供 WorkerWritePipeline 加密的列字节。
    design.md 步骤 5：本地 RDD 计算后再封装 ABE 头。
    """
    col_width = ctx.column_width()
    output: dict[str, bytes] = {}

    if ctx.is_authorized("Name"):
        name = _strip_null(ctx.get_column("Name")).upper()
        output["Name"] = name.ljust(col_width, "\x00").encode("utf-8")[:col_width]

    if ctx.is_authorized("Dept"):
        dept = _strip_null(ctx.get_column("Dept"))
        output["Dept"] = dept.ljust(col_width, "\x00").encode("utf-8")[:col_width]

    if ctx.is_authorized("Salary"):
        salary_raw = _strip_null(ctx.get_column("Salary"))
        if salary_raw and salary_raw != "NULL":
            try:
                salary_val = int(salary_raw)
                band = f"{(salary_val // 1000) * 1000}+"
            except ValueError:
                band = salary_raw
        else:
            band = "NULL"
        output["Salary"] = band.ljust(col_width, "\x00").encode("utf-8")[:col_width]

    return output


OPERATOR_REGISTRY: dict[str, TeeOperator] = {
    "aggregate_count": aggregate_count,
    "dept_stats": dept_stats,
    "project_authorized_columns": project_authorized_columns,
}


def get_operator(name: str) -> TeeOperator:
    if name not in OPERATOR_REGISTRY:
        raise KeyError(f"unknown tee operator: {name}; available: {sorted(OPERATOR_REGISTRY)}")
    return OPERATOR_REGISTRY[name]
