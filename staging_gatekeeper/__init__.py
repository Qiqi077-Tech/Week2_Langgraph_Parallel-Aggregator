"""Automated Staging Gatekeeper & Data Integrity Validator (LangGraph fan-out / fan-in)."""

from typing import Any

__all__ = ["ChatModelReportLLM", "MockReportLLM", "app", "build_workflow", "workflow"]


def __getattr__(name: str) -> Any:  # lazy, so `python -m staging_gatekeeper.graph` runs cleanly
    if name in ("app", "build_workflow", "workflow"):
        from . import graph
        return getattr(graph, name)
    if name in ("ChatModelReportLLM", "MockReportLLM"):
        from . import llm
        return getattr(llm, name)
    raise AttributeError(name)
