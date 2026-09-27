"""The 8-row walkthrough batch: each row's documented fate is what actually happens."""

from staging_gatekeeper.graph import build_workflow
from walkthrough import BATCH, build_sample_database

INPUT = {"batch_id": BATCH, "target_table": "dbo.STG_DEPARTMENT", "tenant_id": 42}


async def run(fixed):
    return await build_workflow(build_sample_database(fixed=fixed, latency=0.01)).compile().ainvoke(INPUT)


async def test_sample_batch_is_rejected_for_exactly_the_documented_reasons():
    r = await run(fixed=False)
    got = {(f["check"], f["column"], f["severity"]): f["affected_rows"] for f in r["findings"]}
    assert got == {
        ("NULL_VIOLATION", "DEPARTMENTNAME", "CRITICAL"): [2],
        ("FK_VIOLATION", "COSTCENTERID", "CRITICAL"): [3],
        ("FORMAT_VIOLATION", "MANAGEREMAIL", "WARNING"): [4],
        ("DATE_OVERLAP", None, "CRITICAL"): [5, 6],
        ("HIERARCHY_CYCLE", "PARENTDEPARTMENTCODE", "CRITICAL"): [7, 8],
    }
    assert r["final_verdict"] == "REJECTED"
    assert r["remediation_sql"] is None


async def test_corrected_batch_is_approved():
    r = await run(fixed=True)
    assert r["final_verdict"] == "APPROVED" and r["findings"] == []
