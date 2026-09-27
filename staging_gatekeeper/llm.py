"""Executive-report generation behind a small, mockable interface.

The LLM only *writes prose*: it summarises the merged findings, ranks what matters most and
explains how to correct it. It never sees a database, never writes SQL and never influences
the verdict.

* `MockReportLLM` - deterministic, offline; formats the findings and the workers' recommendations.
* `ChatModelReportLLM` - wraps any LangChain chat model (Gemini, Claude, OpenAI, ...).
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from pydantic import BaseModel

MAX_ROWS_SHOWN = 20  # cap row ids sent to the model; the count is always exact


class ReportRequest(BaseModel):
    batch_id: str
    target_table: str
    tenant_id: int
    row_count: int
    verdict: str
    findings: list[dict[str, Any]]  # already merged and ordered: most critical first


class ReportResponse(BaseModel):
    summary_report: str


class ReportLLM(Protocol):
    async def generate(self, request: ReportRequest) -> ReportResponse: ...


class MockReportLLM:
    """Deterministic stand-in: same structure the real prompt asks for, no model call."""

    async def generate(self, request: ReportRequest) -> ReportResponse:
        crit = [f for f in request.findings if f["severity"] != "WARNING"]
        warn = [f for f in request.findings if f["severity"] == "WARNING"]
        lines = [
            f"Batch {request.batch_id} -> {request.target_table} (tenant {request.tenant_id}): "
            f"{request.verdict}. {request.row_count} staged row(s) inspected; "
            f"{len(crit)} critical and {len(warn)} warning finding(s)."
        ]
        if crit:
            lines.append("\nFix these first (most critical first):")
            for i, f in enumerate(crit, 1):
                lines += _describe(f, f"{i}.")
        if warn:
            lines.append("\nWarnings (the batch can proceed, but review):")
            for f in warn:
                lines += _describe(f, "-")
        return ReportResponse(summary_report="\n".join(lines))


def _describe(f: dict[str, Any], bullet: str) -> list[str]:
    where = f" [{f['column']}]" if f.get("column") else ""
    rows = f["affected_rows"]
    shown = ", ".join(map(str, rows[:MAX_ROWS_SHOWN])) + (" ..." if len(rows) > MAX_ROWS_SHOWN else "")
    return [
        f"{bullet} {f['check']}{where}: {f['message']} (rows {shown})",
        f"     How to fix: {f.get('recommendation') or 'Review the affected rows.'}",
    ]


SYSTEM_PROMPT = """You are a data-quality analyst writing the executive summary of a staging-batch \
validation. You receive the batch details, the verdict (already decided; do not change or question \
it) and a list of findings ordered most critical first. Each finding has a severity, a check code, \
a message, affected row ids, sample bad values and a `recommendation` from the validator.

Write a concise plain-text report with exactly these parts:
1. One sentence: the verdict and the scale of the problem.
2. "Most critical issues": the issues that block the batch, in priority order, at most 5. Group \
findings that share a root cause. Cite row ids and sample values.
3. "How to correct": for each issue above, a concrete corrective action. Base it on the finding's \
`recommendation`; you may make it more specific using the samples, but do not invent data or contradict it.
4. "Warnings": one short line per warning, only if there are any.

Rules: use only the information given; no SQL or code; no markdown tables; do not repeat every \
finding verbatim; under 350 words."""


class ChatModelReportLLM:
    def __init__(self, chat_model: Any) -> None:
        self._model = chat_model

    async def generate(self, request: ReportRequest) -> ReportResponse:
        from langchain_core.messages import HumanMessage, SystemMessage

        payload = request.model_dump()
        for f in payload["findings"]:
            f["affected_rows"] = f["affected_rows"][:MAX_ROWS_SHOWN]
            f.pop("details", None)
        reply = await self._model.ainvoke(
            [SystemMessage(SYSTEM_PROMPT), HumanMessage(json.dumps(payload, default=str, indent=1))]
        )
        text = reply.text.strip()
        if not text:
            raise ValueError("model returned an empty report")
        return ReportResponse(summary_report=text)
