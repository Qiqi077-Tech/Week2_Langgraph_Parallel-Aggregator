"""Worker 3: logical collisions (duplicate business keys, effective-date windows)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Awaitable, Callable

from .. import queries as q
from ..findings import make_finding
from ..mock_db import Row, StagingDatabase
from ..state import GatekeeperState
from .common import PlannedCheck, row_ids, run_planned_checks

WORKER = "collision_date_overlap"


def _fmt(d: date) -> str:
    return "open-ended" if d == date.max else d.isoformat()


def plan_collision_checks(ctx: q.BatchContext) -> list[PlannedCheck]:
    s = ctx.schema

    def invalid_window(rows: list[Row]) -> list[dict[str, Any]]:
        if not rows:
            return []
        ids = row_ids(rows)
        return [make_finding(
            worker=WORKER, check="INVALID_DATE_WINDOW", severity="CRITICAL", column=s.effective_end,
            affected_rows=ids, samples=[f"{r['START_DT']} > {r['END_DT']}" for r in rows],
            message=f"{len(ids)} row(s): {s.effective_end} is earlier than {s.effective_start}",
            recommendation=f"Correct {s.effective_end} so it is on or after {s.effective_start}, or leave "
                           "it empty for an open-ended version.",
        )]

    def duplicate_key(rows: list[Row]) -> list[dict[str, Any]]:
        if not rows:
            return []
        keys = sorted({r["KEY_VALUE"] for r in rows})
        # Suggest keeping the lowest STAGE_ROW_ID of every duplicate group.
        extras = sorted(r["STAGE_ROW_ID"] for r in rows if r["DUP_RANK"] > 1)
        return [make_finding(
            worker=WORKER, check="DUPLICATE_BUSINESS_KEY", severity="CRITICAL",
            affected_rows=row_ids(rows), samples=keys,
            message=f"{len(keys)} business key(s) ({' + '.join(s.business_key + [s.effective_start])}) "
                    f"appear more than once in the batch",
            details={"duplicate_keys": keys, "removable_rows": extras},
            recommendation="Keep one row per business key and effective start date. Suggested: keep the "
                           f"first row of each group and drop or merge row(s) {', '.join(map(str, extras))}.",
        )]

    def master_collision(rows: list[Row]) -> list[dict[str, Any]]:
        if not rows:
            return []
        ids = row_ids(rows)
        return [make_finding(
            worker=WORKER, check="MASTER_KEY_COLLISION", severity="CRITICAL", affected_rows=ids,
            samples=[r["KEY_VALUE"] for r in rows],
            message=f"{len(ids)} row(s) reuse a business key that already exists in {s.master_table}",
            recommendation="This version already exists in the master table: drop the row, or change "
                           "the effective start date if it is a genuinely new version.",
        )]

    def date_overlap(rows: list[Row]) -> list[dict[str, Any]]:
        if not rows:
            return []
        # Each pair overlaps; end-date the earlier-starting version the day before the later one starts.
        new_end: dict[int, date] = {}
        for r in rows:
            (early_id, later_start) = (
                (r["STAGE_ROW_ID"], r["OTHER_START"]) if r["START_DT"] < r["OTHER_START"]
                else (r["OTHER_ROW_ID"], r["START_DT"])
            )
            candidate = later_start - timedelta(days=1)
            new_end[early_id] = min(new_end.get(early_id, candidate), candidate)
        advice = "; ".join(f"row {rid} -> {s.effective_end} {d.isoformat()}" for rid, d in sorted(new_end.items()))
        ids = row_ids(rows) + [r["OTHER_ROW_ID"] for r in rows]
        return [make_finding(
            worker=WORKER, check="DATE_OVERLAP", severity="CRITICAL", affected_rows=ids,
            samples=[f"rows {r['STAGE_ROW_ID']}/{r['OTHER_ROW_ID']}: {_fmt(r['START_DT'])}..{_fmt(r['END_DT'])} "
                     f"vs {_fmt(r['OTHER_START'])}..{_fmt(r['OTHER_END'])}" for r in rows],
            message=f"{len(rows)} overlapping effective-date window pair(s) within the batch",
            details={"pairs": [[r["STAGE_ROW_ID"], r["OTHER_ROW_ID"]] for r in rows]},
            recommendation="End-date the earlier version the day before the next one starts: " + advice + ".",
        )]

    def master_overlap(rows: list[Row]) -> list[dict[str, Any]]:
        if not rows:
            return []
        ids = row_ids(rows)
        return [make_finding(
            worker=WORKER, check="MASTER_DATE_OVERLAP", severity="WARNING", affected_rows=ids,
            samples=[f"row {r['STAGE_ROW_ID']}: {_fmt(r['START_DT'])}..{_fmt(r['END_DT'])} vs master "
                     f"{_fmt(r['MASTER_START'])}..{_fmt(r['MASTER_END'])}" for r in rows],
            message=f"{len(set(ids))} row(s) overlap a version already in {s.master_table}; the load "
                    "must end-date the existing version",
            recommendation="Confirm the new version is meant to supersede the existing one; the load "
                           "must then end-date the master version. Otherwise adjust the effective dates.",
        )]

    return [
        PlannedCheck("window:invalid", q.invalid_window_check(ctx), invalid_window),
        PlannedCheck("key:duplicate", q.duplicate_key_check(ctx), duplicate_key),
        PlannedCheck("key:master_collision", q.master_key_collision_check(ctx), master_collision),
        PlannedCheck("window:overlap", q.date_overlap_check(ctx), date_overlap),
        PlannedCheck("window:master_overlap", q.master_date_overlap_check(ctx), master_overlap),
    ]


def make_collision_worker(db: StagingDatabase) -> Callable[[GatekeeperState], Awaitable[dict[str, Any]]]:
    async def collision_worker(state: GatekeeperState) -> dict[str, Any]:
        ctx = q.BatchContext.from_state(state)
        return {"findings": await run_planned_checks(db, WORKER, plan_collision_checks(ctx))}

    return collision_worker
