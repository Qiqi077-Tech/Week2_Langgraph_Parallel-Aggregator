"""Finding records emitted by the workers and consumed by the aggregator."""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from .models import Severity

SEVERITY_RANK = {"CRITICAL": 0, "WARNING": 1}
SAMPLE_SIZE = 5


def make_finding(
    *,
    worker: str,
    check: str,
    severity: Severity,
    message: str,
    column: str | None = None,
    affected_rows: Iterable[int] = (),
    samples: Sequence[Any] = (),
    details: dict[str, Any] | None = None,
    recommendation: str = "",
) -> dict[str, Any]:
    """`recommendation` is the worker's plain-language advice on how to correct the problem;
    the aggregator's LLM may reword it but must not contradict it."""
    rows = sorted(set(affected_rows))
    return {
        "worker": worker,
        "check": check,
        "severity": severity,
        "column": column,
        "message": message,
        "affected_rows": rows,
        "affected_count": len(rows),
        "samples": [str(s) for s in samples[:SAMPLE_SIZE]],
        "details": details or {},
        "recommendation": recommendation,
    }


def worker_failure(worker: str, check_name: str, exc: BaseException) -> dict[str, Any]:
    """A check that could not run is a blocker: the gatekeeper fails closed."""
    return make_finding(
        worker=worker,
        check="WORKER_FAILURE",
        severity="CRITICAL",
        message=f"Check {check_name} could not be executed: {type(exc).__name__}: {exc}",
        details={"check": check_name},
        recommendation="Investigate the error (permissions, connectivity, schema drift) and re-run the batch.",
    )


def sort_key(finding: dict[str, Any]) -> tuple[int, int, str, str]:
    """Most critical first: severity, then how many rows are hit, then a stable tiebreak."""
    return (
        SEVERITY_RANK.get(finding.get("severity", "CRITICAL"), 0),
        -finding.get("affected_count", 0),
        finding.get("check", ""),
        finding.get("column") or "",
    )
