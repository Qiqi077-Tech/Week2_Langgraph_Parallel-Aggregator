"""Worker 1: schema & null integrity (NULLs, type castability, length/range bounds, formats)."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .. import queries as q
from ..findings import make_finding
from ..mock_db import Row, StagingDatabase
from ..models import TYPE_LIMITS, ColumnSpec
from ..state import GatekeeperState
from .common import PlannedCheck, row_ids, run_planned_checks

WORKER = "schema_null_integrity"

FORMAT_HINTS = {
    "email": "Correct the address to the form name@domain.tld, or leave it empty if unknown.",
    "department_code": "Use the AAA-999 pattern: three capital letters, a hyphen, three digits.",
}


def plan_schema_checks(ctx: q.BatchContext) -> list[PlannedCheck]:
    checks: list[PlannedCheck] = []

    def add(name: str, query: q.CheckQuery, code: str, severity: str, message: str, col: ColumnSpec,
            advice: str) -> None:
        def interpret(rows: list[Row]) -> list[dict[str, Any]]:
            if not rows:
                return []
            ids = row_ids(rows)
            return [make_finding(
                worker=WORKER, check=code, severity=severity, column=col.name, affected_rows=ids,
                message=f"{len(set(ids))} row(s): {message}",
                samples=[r.get("BAD_VALUE") for r in rows], recommendation=advice,
            )]
        checks.append(PlannedCheck(name, query, interpret))

    for col in ctx.schema.columns:
        n = col.name
        if not col.nullable:
            add(f"null:{n}", q.null_check(ctx, col), "NULL_VIOLATION", "CRITICAL",
                f"NULL/blank in non-nullable column {n}", col,
                f"Supply a value for {n} in the source system, or exclude these rows from the batch.")
        if col.data_type != "varchar":
            add(f"type:{n}", q.type_check(ctx, col), "TYPE_MISMATCH", "CRITICAL",
                f"{n} is not a valid {col.data_type.upper()}", col,
                f"Correct the source values so {n} is a valid {col.data_type.upper()} (see samples).")
        if col.data_type == "varchar" and col.max_length:
            add(f"length:{n}", q.length_check(ctx, col), "LENGTH_EXCEEDED", "CRITICAL",
                f"{n} exceeds max length {col.max_length}", col,
                f"Shorten {n} to at most {col.max_length} characters, or widen the target column "
                "if the longer value is legitimate.")
        if col.data_type in TYPE_LIMITS:
            lo, hi = q.range_bounds(col)
            add(f"range:{n}", q.range_check(ctx, col), "OUT_OF_RANGE", "CRITICAL",
                f"{n} outside allowed range [{lo}, {hi}]", col,
                f"Correct {n} to a value between {lo} and {hi}; check for a unit or typo error at the source.")
        if col.format:
            add(f"format:{n}", q.format_check(ctx, col), "FORMAT_VIOLATION", col.format_severity,
                f"{n} does not match the '{col.format}' format", col,
                FORMAT_HINTS.get(col.format, f"Correct {n} to match the '{col.format}' format."))
    return checks


def make_schema_worker(db: StagingDatabase) -> Callable[[GatekeeperState], Awaitable[dict[str, Any]]]:
    async def schema_worker(state: GatekeeperState) -> dict[str, Any]:
        ctx = q.BatchContext.from_state(state)
        return {"findings": await run_planned_checks(db, WORKER, plan_schema_checks(ctx))}

    return schema_worker
