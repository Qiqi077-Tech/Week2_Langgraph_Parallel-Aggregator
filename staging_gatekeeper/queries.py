"""Structured validation queries.

Every check is a `CheckQuery`: a `kind` (what a mock database dispatches on), parameterised
T-SQL (`@batch_id`, `@tenant_id`; what a real database client would execute) and the
parameters both need. Each query returns the *violating* staging rows, always including
`STAGE_ROW_ID`. Output column aliases per kind are part of the contract with the workers.

Blank strings are treated as NULL everywhere via `_txt`, mirroring how loaders trim
text-typed staging columns.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from .models import (
    BATCH_COLUMN,
    ROW_ID_COLUMN,
    TENANT_COLUMN,
    TYPE_LIMITS,
    ColumnSpec,
    ForeignKey,
    TableSchema,
    quote_ident,
)

OPEN_END = "CAST('9999-12-31' AS DATE)"


@dataclass(frozen=True)
class FormatRule:
    invalid_predicate: str  # T-SQL true when the value is malformed; `{c}` = column expr
    regex: re.Pattern[str]  # equivalent used by the mock database


FORMAT_RULES: dict[str, FormatRule] = {
    "email": FormatRule(
        "({c} NOT LIKE N'%_@_%._%' OR {c} LIKE N'% %')",
        re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+"),
    ),
    "department_code": FormatRule(
        "{c} COLLATE Latin1_General_BIN NOT LIKE N'[A-Z][A-Z][A-Z]-[0-9][0-9][0-9]'",
        re.compile(r"[A-Z]{3}-[0-9]{3}"),
    ),
}


@dataclass(frozen=True)
class CheckQuery:
    kind: str
    table: str  # normalised `schema.TABLE`
    tenant_id: int
    batch_id: str
    sql: str
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BatchContext:
    schema: TableSchema
    tenant_id: int
    batch_id: str

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> "BatchContext":
        schema = TableSchema.model_validate(state["metadata"]["table_schema"])
        return cls(schema, int(state["tenant_id"]), state["batch_id"])

    @property
    def stg(self) -> str:
        return quote_ident(self.schema.table)

    def scope(self, alias: str = "s") -> str:
        return f"{alias}.{BATCH_COLUMN} = @batch_id AND {alias}.{TENANT_COLUMN} = @tenant_id"

    def query(self, kind: str, sql: str, **params: Any) -> CheckQuery:
        return CheckQuery(kind, self.schema.table, self.tenant_id, self.batch_id, _tidy(sql), params)


def _tidy(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())


def _txt(col: str, alias: str = "s") -> str:
    return f"NULLIF(LTRIM(RTRIM(CAST({alias}.{quote_ident(col)} AS NVARCHAR(4000)))), N'')"


def _select(ctx: BatchContext, col: str, where: str) -> str:
    return f"""
        SELECT s.{ROW_ID_COLUMN}, s.{quote_ident(col)} AS BAD_VALUE
        FROM {ctx.stg} AS s
        WHERE {ctx.scope()}
          AND {where}"""


# --- Worker 1: schema & null integrity ---------------------------------------------------

def null_check(ctx: BatchContext, col: ColumnSpec) -> CheckQuery:
    return ctx.query("null_check", _select(ctx, col.name, f"{_txt(col.name)} IS NULL"), column=col.name)


def type_check(ctx: BatchContext, col: ColumnSpec) -> CheckQuery:
    target = "DATE" if col.data_type == "date" else "BIGINT"
    where = f"{_txt(col.name)} IS NOT NULL AND TRY_CAST({_txt(col.name)} AS {target}) IS NULL"
    return ctx.query("type_check", _select(ctx, col.name, where), column=col.name, data_type=col.data_type)


def length_check(ctx: BatchContext, col: ColumnSpec) -> CheckQuery:
    where = f"LEN({_txt(col.name)}) > {int(col.max_length)}"
    return ctx.query("length_check", _select(ctx, col.name, where), column=col.name, max_length=col.max_length)


def range_bounds(col: ColumnSpec) -> tuple[int, int]:
    lo, hi = TYPE_LIMITS[col.data_type]
    return (col.min_value if col.min_value is not None else lo, col.max_value if col.max_value is not None else hi)


def range_check(ctx: BatchContext, col: ColumnSpec) -> CheckQuery:
    lo, hi = range_bounds(col)
    cast = f"TRY_CAST({_txt(col.name)} AS BIGINT)"
    where = f"{cast} IS NOT NULL AND ({cast} < {lo} OR {cast} > {hi})"
    return ctx.query("range_check", _select(ctx, col.name, where), column=col.name, min_value=lo, max_value=hi)


def format_check(ctx: BatchContext, col: ColumnSpec) -> CheckQuery:
    if col.format not in FORMAT_RULES:
        raise ValueError(f"Unknown format rule {col.format!r} on column {col.name}")
    predicate = FORMAT_RULES[col.format].invalid_predicate.format(c=_txt(col.name))
    where = f"{_txt(col.name)} IS NOT NULL AND {predicate}"
    return ctx.query("format_check", _select(ctx, col.name, where), column=col.name, format=col.format)


# --- Worker 2: referential & hierarchy ---------------------------------------------------

def fk_check(ctx: BatchContext, fk: ForeignKey) -> CheckQuery:
    where = f"""{_txt(fk.column)} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {quote_ident(fk.parent_table)} AS p
              WHERE p.{quote_ident(fk.parent_column)} = s.{quote_ident(fk.column)}
                AND p.{TENANT_COLUMN} = @tenant_id)"""
    return ctx.query(
        "fk_check", _select(ctx, fk.column, where),
        column=fk.column, parent_table=fk.parent_table, parent_column=fk.parent_column,
    )


def hierarchy_orphan_check(ctx: BatchContext) -> CheckQuery:
    h, master = ctx.schema.hierarchy, ctx.schema.master_table
    key, parent = quote_ident(h.key_column), quote_ident(h.parent_column)
    where = f"""{_txt(h.parent_column)} IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {quote_ident(master)} AS m
                          WHERE m.{key} = s.{parent} AND m.{TENANT_COLUMN} = @tenant_id)
          AND NOT EXISTS (SELECT 1 FROM {ctx.stg} AS p
                          WHERE p.{key} = s.{parent} AND {ctx.scope('p')})"""
    return ctx.query(
        "hierarchy_orphan", _select(ctx, h.parent_column, where),
        key_column=h.key_column, parent_column=h.parent_column, master_table=master,
    )


MAX_HIERARCHY_DEPTH = 50


def hierarchy_cycle_check(ctx: BatchContext) -> CheckQuery:
    """Walks parent links from every staged node; a node that reaches itself is on a loop.
    The batch's own edges override the live master's edges for the same key."""
    h, master = ctx.schema.hierarchy, ctx.schema.master_table
    key, parent = quote_ident(h.key_column), quote_ident(h.parent_column)
    sql = f"""
        WITH hierarchy AS (
            SELECT m.{key} AS NODE, m.{parent} AS PARENT_NODE
            FROM {quote_ident(master)} AS m
            WHERE m.{TENANT_COLUMN} = @tenant_id
              AND m.{key} NOT IN (SELECT s.{key} FROM {ctx.stg} AS s WHERE {ctx.scope()})
            UNION
            SELECT s.{key}, s.{parent} FROM {ctx.stg} AS s WHERE {ctx.scope()}
        ),
        walk AS (
            SELECT h.NODE AS START_NODE, h.PARENT_NODE AS CURRENT_NODE,
                   CAST(h.NODE + N' > ' + h.PARENT_NODE AS NVARCHAR(4000)) AS CYCLE_PATH, 1 AS DEPTH
            FROM hierarchy AS h WHERE h.PARENT_NODE IS NOT NULL
            UNION ALL
            SELECT w.START_NODE, h.PARENT_NODE,
                   CAST(w.CYCLE_PATH + N' > ' + h.PARENT_NODE AS NVARCHAR(4000)), w.DEPTH + 1
            FROM walk AS w
            JOIN hierarchy AS h ON h.NODE = w.CURRENT_NODE AND h.PARENT_NODE IS NOT NULL
            WHERE w.CURRENT_NODE <> w.START_NODE AND w.DEPTH < {MAX_HIERARCHY_DEPTH}
        )
        SELECT DISTINCT s.{ROW_ID_COLUMN}, w.CYCLE_PATH
        FROM walk AS w
        JOIN {ctx.stg} AS s ON s.{key} = w.START_NODE AND {ctx.scope()}
        WHERE w.CURRENT_NODE = w.START_NODE
        OPTION (MAXRECURSION {MAX_HIERARCHY_DEPTH + 1})"""
    return ctx.query("hierarchy_cycle", sql, key_column=h.key_column, parent_column=h.parent_column,
                     master_table=master)


# --- Worker 3: logical collision & date overlap ------------------------------------------

def _unique_key(ctx: BatchContext, alias: str = "s") -> list[str]:
    return [f"{alias}.{quote_ident(c)}" for c in ctx.schema.business_key]


def _key_value_expr(ctx: BatchContext, alias: str = "s") -> str:
    start = f"CONVERT(VARCHAR(10), TRY_CAST({_txt(ctx.schema.effective_start, alias)} AS DATE), 23)"
    return f"CONCAT_WS(N' | ', {', '.join(_unique_key(ctx, alias))}, {start})"


def invalid_window_check(ctx: BatchContext) -> CheckQuery:
    st, en = ctx.schema.effective_start, ctx.schema.effective_end
    sql = f"""
        SELECT s.{ROW_ID_COLUMN}, TRY_CAST({_txt(st)} AS DATE) AS START_DT, TRY_CAST({_txt(en)} AS DATE) AS END_DT
        FROM {ctx.stg} AS s
        WHERE {ctx.scope()}
          AND TRY_CAST({_txt(st)} AS DATE) IS NOT NULL
          AND TRY_CAST({_txt(en)} AS DATE) < TRY_CAST({_txt(st)} AS DATE)"""
    return ctx.query("invalid_window", sql, start_column=st, end_column=en)


def duplicate_key_check(ctx: BatchContext) -> CheckQuery:
    st = ctx.schema.effective_start
    partition = ", ".join(_unique_key(ctx) + [f"TRY_CAST({_txt(st)} AS DATE)"])
    sql = f"""
        SELECT d.{ROW_ID_COLUMN}, d.KEY_VALUE, d.DUP_RANK
        FROM (
            SELECT s.{ROW_ID_COLUMN}, {_key_value_expr(ctx)} AS KEY_VALUE,
                   COUNT(*) OVER (PARTITION BY {partition}) AS DUP_COUNT,
                   ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY s.{ROW_ID_COLUMN}) AS DUP_RANK
            FROM {ctx.stg} AS s
            WHERE {ctx.scope()} AND TRY_CAST({_txt(st)} AS DATE) IS NOT NULL
        ) AS d
        WHERE d.DUP_COUNT > 1"""
    return ctx.query("duplicate_key", sql, business_key=ctx.schema.business_key, start_column=st)


def master_key_collision_check(ctx: BatchContext) -> CheckQuery:
    st, master = ctx.schema.effective_start, quote_ident(ctx.schema.master_table)
    join = " AND ".join(f"m.{quote_ident(c)} = s.{quote_ident(c)}" for c in ctx.schema.business_key)
    sql = f"""
        SELECT s.{ROW_ID_COLUMN}, {_key_value_expr(ctx)} AS KEY_VALUE
        FROM {ctx.stg} AS s
        JOIN {master} AS m ON {join}
            AND m.{quote_ident(st)} = TRY_CAST({_txt(st)} AS DATE) AND m.{TENANT_COLUMN} = @tenant_id
        WHERE {ctx.scope()}"""
    return ctx.query("master_key_collision", sql, business_key=ctx.schema.business_key,
                     start_column=st, master_table=ctx.schema.master_table)


def _window_cte(ctx: BatchContext) -> str:
    st, en = ctx.schema.effective_start, ctx.schema.effective_end
    keys = ", ".join(f"{k} AS K{i}" for i, k in enumerate(_unique_key(ctx)))
    return f"""
        WITH w AS (
            SELECT s.{ROW_ID_COLUMN}, {keys},
                   TRY_CAST({_txt(st)} AS DATE) AS START_DT,
                   COALESCE(TRY_CAST({_txt(en)} AS DATE), {OPEN_END}) AS END_DT
            FROM {ctx.stg} AS s
            WHERE {ctx.scope()}
              AND TRY_CAST({_txt(st)} AS DATE) IS NOT NULL
              AND ({_txt(en)} IS NULL OR TRY_CAST({_txt(en)} AS DATE) IS NOT NULL)
        )"""


def date_overlap_check(ctx: BatchContext) -> CheckQuery:
    """Overlapping windows for the same entity inside the batch. Identical start dates are the
    duplicate-key check's business and are excluded here. Returns one row per overlapping pair
    with the lower STAGE_ROW_ID as STAGE_ROW_ID."""
    on = " AND ".join(f"a.K{i} = b.K{i}" for i in range(len(ctx.schema.business_key)))
    sql = f"""{_window_cte(ctx)}
        SELECT a.{ROW_ID_COLUMN}, b.{ROW_ID_COLUMN} AS OTHER_ROW_ID,
               a.START_DT, a.END_DT, b.START_DT AS OTHER_START, b.END_DT AS OTHER_END
        FROM w AS a
        JOIN w AS b ON {on} AND a.{ROW_ID_COLUMN} < b.{ROW_ID_COLUMN}
        WHERE a.START_DT <> b.START_DT
          AND a.END_DT >= a.START_DT AND b.END_DT >= b.START_DT
          AND a.START_DT <= b.END_DT AND b.START_DT <= a.END_DT"""
    return ctx.query("date_overlap", sql, business_key=ctx.schema.business_key,
                     start_column=ctx.schema.effective_start, end_column=ctx.schema.effective_end)


def master_date_overlap_check(ctx: BatchContext) -> CheckQuery:
    st, en = ctx.schema.effective_start, ctx.schema.effective_end
    master = quote_ident(ctx.schema.master_table)
    on = " AND ".join(f"m.{quote_ident(c)} = w.K{i}" for i, c in enumerate(ctx.schema.business_key))
    m_end = f"COALESCE(m.{quote_ident(en)}, {OPEN_END})"
    sql = f"""{_window_cte(ctx)}
        SELECT w.{ROW_ID_COLUMN}, w.START_DT, w.END_DT,
               m.{quote_ident(st)} AS MASTER_START, {m_end} AS MASTER_END
        FROM w
        JOIN {master} AS m ON {on} AND m.{TENANT_COLUMN} = @tenant_id
        WHERE w.START_DT <> m.{quote_ident(st)}
          AND w.END_DT >= w.START_DT
          AND w.START_DT <= {m_end} AND m.{quote_ident(st)} <= w.END_DT"""
    return ctx.query("master_date_overlap", sql, business_key=ctx.schema.business_key,
                     start_column=st, end_column=en, master_table=ctx.schema.master_table)
