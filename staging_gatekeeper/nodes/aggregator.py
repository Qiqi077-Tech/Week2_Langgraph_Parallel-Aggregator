"""Aggregator: merges findings, applies the verdict rules, asks the LLM to write the summary."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from ..findings import sort_key
from ..llm import MockReportLLM, ReportLLM, ReportRequest
from ..state import GatekeeperState, Verdict

log = logging.getLogger(__name__)


def merge_findings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop exact duplicates (same check/column/rows/message) and order by severity."""
    seen: set[tuple] = set()
    unique = []
    for f in findings:
        key = (f.get("check"), f.get("column"), tuple(f.get("affected_rows", ())), f.get("message"))
        if key not in seen:
            seen.add(key)
            unique.append(f)
    return sorted(unique, key=sort_key)


def decide_verdict(findings: list[dict[str, Any]]) -> Verdict:
    """Any critical (or unrecognised) severity rejects; warnings only warn; nothing approves."""
    if not findings:
        return "APPROVED"
    if all(f.get("severity") == "WARNING" for f in findings):
        return "WARNING"
    return "REJECTED"


def make_aggregator(llm: ReportLLM | None = None) -> Callable[[GatekeeperState], Awaitable[dict[str, Any]]]:
    llm = llm or MockReportLLM()
    fallback = MockReportLLM()

    async def aggregator(state: GatekeeperState) -> dict[str, Any]:
        findings = merge_findings(state.get("findings", []))
        verdict = decide_verdict(findings)
        row_count = state.get("metadata", {}).get("row_count", 0)

        if verdict == "APPROVED":
            return {
                "final_verdict": verdict,
                "summary_report": (
                    f"Batch {state['batch_id']} -> {state['target_table']} (tenant {state['tenant_id']}): "
                    f"APPROVED. All {row_count} staged row(s) passed the schema, referential and "
                    "collision validators."
                ),
                "remediation_sql": None,
            }

        request = ReportRequest(
            batch_id=state["batch_id"], target_table=state["target_table"], tenant_id=state["tenant_id"],
            row_count=row_count, verdict=verdict, findings=findings,
        )
        try:
            summary = (await llm.generate(request)).summary_report.strip()
        except Exception as exc:  # an LLM outage must not lose the verdict
            log.warning("Report LLM failed (%s: %s); using deterministic report", type(exc).__name__, exc)
            summary = (await fallback.generate(request)).summary_report.strip()
            summary += f"\n(Note: LLM summary unavailable - {type(exc).__name__}; deterministic report shown.)"

        # No data-changing SQL is generated: corrections are made at the source and the batch re-submitted.
        return {"final_verdict": verdict, "summary_report": summary, "remediation_sql": None}

    return aggregator
