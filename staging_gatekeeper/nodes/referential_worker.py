"""Worker 2: referential integrity & hierarchy (foreign keys, orphaned parents, cycles)."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .. import queries as q
from ..findings import make_finding
from ..mock_db import Row, StagingDatabase
from ..state import GatekeeperState
from .common import PlannedCheck, row_ids, run_planned_checks

WORKER = "referential_hierarchy"


def _canonical_cycle(path: str) -> str:
    """`B > C > A > B` and `A > B > C > A` are the same loop: rotate to start at the smallest node."""
    nodes = path.split(" > ")[:-1]
    i = nodes.index(min(nodes))
    return " > ".join([*nodes[i:], *nodes[:i], nodes[i]])


def plan_referential_checks(ctx: q.BatchContext) -> list[PlannedCheck]:
    checks: list[PlannedCheck] = []

    for fk in ctx.schema.foreign_keys:
        def interpret_fk(rows: list[Row], fk=fk) -> list[dict[str, Any]]:
            if not rows:
                return []
            ids = row_ids(rows)
            return [make_finding(
                worker=WORKER, check="FK_VIOLATION", severity="CRITICAL", column=fk.column,
                affected_rows=ids, samples=[r["BAD_VALUE"] for r in rows],
                message=f"{len(ids)} row(s): {fk.column} has no matching {fk.parent_column} in "
                        f"{fk.parent_table} for tenant {ctx.tenant_id}",
                details={"parent_table": fk.parent_table},
                recommendation=f"Load the missing {fk.parent_table} record for this tenant first, or "
                               f"correct {fk.column} in the source. Values owned by another tenant are not valid.",
            )]
        checks.append(PlannedCheck(f"fk:{fk.column}", q.fk_check(ctx, fk), interpret_fk))

    if ctx.schema.hierarchy and ctx.schema.master_table:
        h = ctx.schema.hierarchy

        def interpret_orphan(rows: list[Row]) -> list[dict[str, Any]]:
            if not rows:
                return []
            ids = row_ids(rows)
            return [make_finding(
                worker=WORKER, check="HIERARCHY_ORPHAN", severity="CRITICAL", column=h.parent_column,
                affected_rows=ids, samples=[r["BAD_VALUE"] for r in rows],
                message=f"{len(ids)} row(s): {h.parent_column} references a {h.key_column} that exists "
                        "neither in this batch nor in the master table",
                recommendation="Include the parent row in this batch, load it beforehand, or correct the "
                               "parent code in the source.",
            )]

        def interpret_cycle(rows: list[Row]) -> list[dict[str, Any]]:
            if not rows:
                return []
            ids = row_ids(rows)
            loops = sorted({_canonical_cycle(r["CYCLE_PATH"]) for r in rows})
            return [make_finding(
                worker=WORKER, check="HIERARCHY_CYCLE", severity="CRITICAL", column=h.parent_column,
                affected_rows=ids, samples=loops,
                message=f"{len(loops)} circular hierarchy loop(s) involving {len(ids)} row(s)",
                details={"cycles": loops},
                recommendation="Break each loop by clearing or re-pointing one parent link in the source "
                               "so every chain ends at a root (see the listed cycles).",
            )]

        checks.append(PlannedCheck("hierarchy:orphan", q.hierarchy_orphan_check(ctx), interpret_orphan))
        checks.append(PlannedCheck("hierarchy:cycle", q.hierarchy_cycle_check(ctx), interpret_cycle))
    return checks


def make_referential_worker(db: StagingDatabase) -> Callable[[GatekeeperState], Awaitable[dict[str, Any]]]:
    async def referential_worker(state: GatekeeperState) -> dict[str, Any]:
        ctx = q.BatchContext.from_state(state)
        return {"findings": await run_planned_checks(db, WORKER, plan_referential_checks(ctx))}

    return referential_worker
