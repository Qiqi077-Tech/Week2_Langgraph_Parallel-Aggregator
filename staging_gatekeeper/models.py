"""Table metadata models shared by the orchestrator, workers and the database layer."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field

DataType = Literal["varchar", "int", "smallint", "date"]
Severity = Literal["CRITICAL", "WARNING"]

# Bookkeeping columns every staging table carries.
ROW_ID_COLUMN = "STAGE_ROW_ID"
BATCH_COLUMN = "BATCH_ID"
TENANT_COLUMN = "TENANT_ID"

# Natural bounds of the integer types (used when a column has no tighter business range).
TYPE_LIMITS: dict[str, tuple[int, int]] = {
    "int": (-(2**31), 2**31 - 1),
    "smallint": (-(2**15), 2**15 - 1),
}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")


def check_ident(name: str) -> str:
    """Reject anything that is not a plain SQL identifier (guards generated T-SQL)."""
    if not _IDENT.match(name):
        raise ValueError(f"Illegal SQL identifier: {name!r}")
    return name


def quote_ident(name: str) -> str:
    """`dbo.STG_X` -> `[dbo].[STG_X]`, `COL` -> `[COL]`."""
    return ".".join(f"[{check_ident(part)}]" for part in name.split("."))


def normalize_table_name(name: str) -> str:
    """Canonical `schema.TABLE` form: brackets stripped, default schema `dbo`."""
    parts = [p.strip().strip("[]") for p in name.strip().split(".")]
    if len(parts) == 1:
        parts.insert(0, "dbo")
    if len(parts) != 2:
        raise ValueError(f"Expected [schema.]table, got {name!r}")
    schema, table = (check_ident(p) for p in parts)
    return f"{schema.lower()}.{table.upper()}"


class ColumnSpec(BaseModel):
    name: str
    data_type: DataType
    nullable: bool = True
    max_length: int | None = None  # varchar only
    min_value: int | None = None  # int types only; falls back to TYPE_LIMITS
    max_value: int | None = None
    format: str | None = None  # key into queries.FORMAT_RULES
    format_severity: Severity = "WARNING"


class ForeignKey(BaseModel):
    column: str
    parent_table: str
    parent_column: str


class HierarchySpec(BaseModel):
    """Self-referencing hierarchy: `parent_column` holds a `key_column` value."""

    key_column: str
    parent_column: str


class TableSchema(BaseModel):
    table: str
    master_table: str | None = None  # the live table this staging table loads into
    columns: list[ColumnSpec]
    business_key: list[str]  # entity key; the unique key is business_key + effective_start
    effective_start: str
    effective_end: str
    foreign_keys: list[ForeignKey] = Field(default_factory=list)
    hierarchy: HierarchySpec | None = None

    def column(self, name: str) -> ColumnSpec:
        return next(c for c in self.columns if c.name == name)
