import pytest

from staging_gatekeeper.demo_data import (
    BATCH_CLEAN, BATCH_DIRTY, BATCH_WARNING, STAGING_TABLE, TENANT_ID, build_demo_database,
)
from staging_gatekeeper.graph import build_workflow


@pytest.fixture
def db():
    return build_demo_database(latency=0.01)


@pytest.fixture
def run(db):
    app = build_workflow(db).compile()

    async def _run(batch_id=BATCH_DIRTY, table=STAGING_TABLE, tenant_id=TENANT_ID):
        return await app.ainvoke({"batch_id": batch_id, "target_table": table, "tenant_id": tenant_id})

    return _run


def by_check(result, check, column=None):
    return [f for f in result["findings"]
            if f["check"] == check and (column is None or f["column"] == column)]
