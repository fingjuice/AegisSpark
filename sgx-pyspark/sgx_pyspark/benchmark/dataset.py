"""100GB 实验数据集：company_xxxxxx 企业员工表。"""

from __future__ import annotations

import random
from dataclasses import dataclass

COL_ORDER = [
    "employee_id",
    "name",
    "email",
    "phone",
    "salary",
    "department",
    "address",
    "bank_account",
]

COL_WIDTH = {
    "employee_id": 16,
    "name": 48,
    "email": 64,
    "phone": 16,
    "salary": 12,
    "department": 32,
    "address": 96,
    "bank_account": 24,
}

SENSITIVE_COLUMNS = frozenset({"email", "phone", "salary", "bank_account"})

POLICY_PUBLIC = "role:analyst"
POLICY_SENSITIVE = "(role:admin and clearance:5)"

FIRST_NAMES = ("James", "Mary", "Wei", "Fang", "John", "Li", "Chen", "Yuki", "Hao", "Min")
LAST_NAMES = ("Smith", "Wang", "Zhang", "Liu", "Brown", "Zhao", "Yang", "Huang", "Wu", "Zhou")
DEPARTMENTS = ("Engineering", "Finance", "HR", "Sales", "Marketing", "Operations", "Legal", "IT")
DOMAINS = ("acmecorp.com", "globex.io", "initech.com", "umbrella.co", "stark-industries.com")
STREETS = (
    "123 Enterprise Ave, Shanghai, CN",
    "456 Innovation Rd, Beijing, CN",
    "789 Cloud Park, Shenzhen, CN",
    "321 Data Center Blvd, Hangzhou, CN",
)


@dataclass
class ColumnSpec:
    name: str
    width: int
    policy: str
    sensitive: bool


@dataclass
class GeneratedTable:
    table_name: str
    company_id: int
    record_count: int
    plaintext: bytes
    column_ranges: list  # ColumnByteRange, typed lazily to avoid circular import


def column_specs() -> list[ColumnSpec]:
    specs: list[ColumnSpec] = []
    for name in COL_ORDER:
        sensitive = name in SENSITIVE_COLUMNS
        specs.append(
            ColumnSpec(
                name=name,
                width=COL_WIDTH[name],
                sensitive=sensitive,
                policy=POLICY_SENSITIVE if sensitive else POLICY_PUBLIC,
            )
        )
    return specs


def _pad_field(width: int, value: str) -> bytes:
    raw = value.encode("utf-8")[:width]
    return raw.ljust(width, b"\x00")


def make_table(company_id: int, rng: random.Random, specs: list[ColumnSpec] | None = None) -> GeneratedTable:
    from sgx_pyspark.types import ColumnByteRange

    specs = specs or column_specs()
    record_count = 50 + (rng.randint(0, 250))
    table_name = f"company_{company_id:06d}"

    total = sum(spec.width * record_count for spec in specs)
    plaintext = bytearray(total)
    ranges: list[ColumnByteRange] = []
    offset = 0

    for spec in specs:
        ranges.append(
            ColumnByteRange(column_name=spec.name, byte_offset=offset, byte_length=spec.width * record_count)
        )
        for row in range(record_count):
            base = offset + row * spec.width
            if spec.name == "employee_id":
                val = f"EMP{company_id:06d}{row:04d}"
            elif spec.name == "name":
                val = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
            elif spec.name == "email":
                val = f"{rng.choice(FIRST_NAMES).lower()}.{rng.choice(LAST_NAMES).lower()}{row}@{rng.choice(DOMAINS)}"
            elif spec.name == "phone":
                val = f"1{rng.randint(3000000000, 3999999999)}"
            elif spec.name == "salary":
                val = f"{rng.randint(8000, 85000)}"
            elif spec.name == "department":
                val = rng.choice(DEPARTMENTS)
            elif spec.name == "address":
                val = rng.choice(STREETS)
            elif spec.name == "bank_account":
                val = f"6222{rng.randint(10**15, 10**16 - 1)}"
            else:
                val = ""
            plaintext[base : base + spec.width] = _pad_field(spec.width, val)
        offset += spec.width * record_count

    return GeneratedTable(
        table_name=table_name,
        company_id=company_id,
        record_count=record_count,
        plaintext=bytes(plaintext),
        column_ranges=ranges,
    )
