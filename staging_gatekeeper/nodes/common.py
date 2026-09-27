"""Shared machinery for the three validation workers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from ..findings import worker_failure
from ..mock_db import Row, StagingDatabase, TransientDatabaseError
from ..queries import CheckQuery

Finding = dict[str, Any]


@dataclass
class PlannedCheck:
    name: str
    query: CheckQuery
    interpret: Callable[[list[Row]], list[Finding]]  # violating rows -> findings (may be empty)


async def run_planned_checks(db: StagingDatabase, worker: str, checks: Sequence[PlannedCheck]) -> list[Finding]:
    """Run all checks concurrently.

    Transient database errors are re-raised so the node's RetryPolicy can retry the worker.
    Any other failure becomes a CRITICAL finding: a check that could not run must never
    let a batch through.
    """
    outcomes = await asyncio.gather(*(db.run_check(c.query) for c in checks), return_exceptions=True)
    findings: list[Finding] = []
    for check, outcome in zip(checks, outcomes):
        if isinstance(outcome, TransientDatabaseError):
            raise outcome
        if isinstance(outcome, Exception):
            findings.append(worker_failure(worker, check.name, outcome))
        elif isinstance(outcome, BaseException):
            raise outcome
        else:
            findings.extend(check.interpret(outcome))
    return findings


def row_ids(rows: Sequence[Row]) -> list[int]:
    return [r["STAGE_ROW_ID"] for r in rows]
