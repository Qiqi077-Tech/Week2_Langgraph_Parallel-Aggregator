"""LangGraph state schema."""

from __future__ import annotations

import operator
from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

Verdict = Literal["APPROVED", "WARNING", "REJECTED"]


class BatchInput(TypedDict):
    """What the caller must supply."""

    batch_id: str
    target_table: str
    tenant_id: int


class GatekeeperState(BatchInput, total=False):
    metadata: Dict[str, Any]
    # Reducer: findings emitted by the three concurrent workers are appended, never overwritten.
    findings: Annotated[List[Dict[str, Any]], operator.add]
    final_verdict: Verdict
    summary_report: str
    remediation_sql: Optional[str]  # always None: the gatekeeper recommends fixes, it never changes data
