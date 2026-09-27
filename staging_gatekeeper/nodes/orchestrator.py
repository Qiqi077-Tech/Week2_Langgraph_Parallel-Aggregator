"""Orchestrator: validates the batch payload, loads table metadata, prepares parallel dispatch."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from ..findings import make_finding
from ..mock_db import StagingDatabase
from ..models import normalize_table_name
from ..state import GatekeeperState

WORKERS = ("schema_worker", "referential_worker", "collision_worker")


def make_orchestrator(db: StagingDatabase) -> Callable[[GatekeeperState], Awaitable[dict[str, Any]]]:
    async def orchestrator(state: GatekeeperState) -> dict[str, Any]:
        batch_id = str(state.get("batch_id", "")).strip()
        if not batch_id:
            raise ValueError("batch_id is required")
        try:
            tenant_id = int(state["tenant_id"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"tenant_id must be an integer, got {state.get('tenant_id')!r}") from None
        table = normalize_table_name(state.get("target_table", ""))

        schema = await db.get_table_schema(table)  # raises UnknownTableError for unknown tables
        row_count = await db.count_batch_rows(table, tenant_id, batch_id)

        update: dict[str, Any] = {
            "batch_id": batch_id,
            "tenant_id": tenant_id,
            "target_table": table,
            # Workers read the schema from here; they never write metadata (no reducer on it).
            "metadata": {
                **state.get("metadata", {}),
                "table_schema": schema.model_dump(),
                "row_count": row_count,
                "dispatch": list(WORKERS),
            },
        }
        if row_count == 0:
            # Nothing to validate is not the same as nothing wrong: block it.
            update["findings"] = [make_finding(
                worker="orchestrator", check="EMPTY_BATCH", severity="CRITICAL",
                message=f"No staged rows found for batch {batch_id} / tenant {tenant_id} in {table}",
                recommendation="Confirm the extract ran and that the batch id and tenant id are correct.",
            )]
        return update

    return orchestrator
