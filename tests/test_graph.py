import pytest

from staging_gatekeeper.demo_data import BATCH_CLEAN, BATCH_WARNING, build_demo_database
from staging_gatekeeper.graph import app, build_workflow
from staging_gatekeeper.mock_db import UnknownTableError
from tests.conftest import by_check


def test_topology_is_fan_out_fan_in():
    edges = {(e.source, e.target) for e in app.get_graph().edges}
    workers = {"schema_worker", "referential_worker", "collision_worker"}
    assert ("__start__", "orchestrator") in edges
    assert {(("orchestrator", w)) for w in workers} <= edges
    assert {((w, "aggregator")) for w in workers} <= edges
    assert ("aggregator", "__end__") in edges


async def test_dirty_batch_is_rejected_with_every_seeded_defect(run):
    r = await run()
    assert r["final_verdict"] == "REJECTED"
    expect = {
        ("NULL_VIOLATION", "DEPARTMENTNAME"): [3],
        ("FORMAT_VIOLATION", "DEPARTMENTCODE"): [4],
        ("LENGTH_EXCEEDED", "DEPARTMENTNAME"): [5],
        ("FORMAT_VIOLATION", "MANAGEREMAIL"): [5],
        ("OUT_OF_RANGE", "HEADCOUNT"): [6],
        ("TYPE_MISMATCH", "HEADCOUNT"): [7],
        ("TYPE_MISMATCH", "EFFECTIVESTARTDATE"): [7],
        ("FK_VIOLATION", "COSTCENTERID"): [8, 9],  # 9 = cost center owned by another tenant
        ("HIERARCHY_ORPHAN", "PARENTDEPARTMENTCODE"): [8],
        ("HIERARCHY_CYCLE", "PARENTDEPARTMENTCODE"): [10, 11, 12, 13],
        ("DUPLICATE_BUSINESS_KEY", None): [14, 15],
        ("DATE_OVERLAP", None): [16, 17],
        ("MASTER_KEY_COLLISION", None): [18],
        ("MASTER_DATE_OVERLAP", None): [19],
        ("INVALID_DATE_WINDOW", "EFFECTIVEENDDATE"): [20],
    }
    for (check, column), rows in expect.items():
        found = by_check(r, check, column)
        assert len(found) == 1, (check, column, r["findings"])
        assert found[0]["affected_rows"] == rows, (check, column)
    assert len(r["findings"]) == len(expect)  # nothing else flagged (rows 1, 2, 21 are clean)


async def test_severities_and_cycle_dedup(run):
    r = await run()
    assert by_check(r, "MASTER_DATE_OVERLAP")[0]["severity"] == "WARNING"
    assert by_check(r, "FORMAT_VIOLATION", "MANAGEREMAIL")[0]["severity"] == "WARNING"
    assert by_check(r, "FORMAT_VIOLATION", "DEPARTMENTCODE")[0]["severity"] == "CRITICAL"
    assert sorted(by_check(r, "HIERARCHY_CYCLE")[0]["details"]["cycles"]) == [
        "CYA-001 > CYB-001 > CYC-001 > CYA-001", "SLF-001 > SLF-001"]


async def test_other_batches_and_tenants_do_not_leak(run):
    r = await run()
    flagged = {row for f in r["findings"] for row in f["affected_rows"]}
    assert not flagged & {101, 102, 201, 202, 301, 302}


async def test_warning_only_batch(run):
    r = await run(BATCH_WARNING)
    assert r["final_verdict"] == "WARNING"
    assert {f["severity"] for f in r["findings"]} == {"WARNING"}
    assert r["remediation_sql"] is None
    assert "How to fix" in r["summary_report"]


async def test_clean_batch_is_approved_without_sql(run):
    r = await run(BATCH_CLEAN)
    assert r["final_verdict"] == "APPROVED"
    assert r["findings"] == []
    assert r["remediation_sql"] is None
    assert "APPROVED" in r["summary_report"]


async def test_empty_batch_is_rejected(run):
    r = await run("NO-SUCH-BATCH")
    assert r["final_verdict"] == "REJECTED"
    assert by_check(r, "EMPTY_BATCH")


async def test_unknown_table_and_bad_input_raise(run):
    with pytest.raises(UnknownTableError):
        await run(table="dbo.NOPE")
    with pytest.raises(ValueError):
        await run(tenant_id="abc")
    with pytest.raises(ValueError):
        await run(batch_id="  ")


async def test_workers_run_concurrently(db, run):
    await run()
    assert db.peak_in_flight > 1
    assert {q.kind for q in db.executed} >= {"null_check", "fk_check", "hierarchy_cycle", "date_overlap"}


async def test_queries_are_parameterised_and_scoped(db, run):
    await run()
    for q in db.executed:
        assert "@batch_id" in q.sql and "@tenant_id" in q.sql
        assert "BATCH-2026" not in q.sql  # values travel as parameters, never inlined


async def test_transient_db_errors_are_retried():
    db = build_demo_database(latency=0.01, transient_failures=2)
    r = await build_workflow(db).compile().ainvoke(
        {"batch_id": "BATCH-2026-0926-003", "target_table": "dbo.STG_DEPARTMENT", "tenant_id": 42})
    assert r["final_verdict"] == "APPROVED"


async def test_check_failure_fails_closed():
    db = build_demo_database(latency=0.01)

    async def boom(query):
        if query.kind == "fk_check":
            raise RuntimeError("permission denied")
        return await original(query)

    original = db.run_check
    db.run_check = boom
    r = await build_workflow(db).compile().ainvoke(
        {"batch_id": "BATCH-2026-0926-003", "target_table": "dbo.STG_DEPARTMENT", "tenant_id": 42})
    assert r["final_verdict"] == "REJECTED"
    assert by_check(r, "WORKER_FAILURE")


async def test_no_sql_is_generated_and_advice_is_specific(run):
    r = await run()
    assert r["remediation_sql"] is None
    assert not any("suggested_sql" in f for f in r["findings"])
    assert all(f["recommendation"] for f in r["findings"])  # every finding says how to fix it
    overlap = by_check(r, "DATE_OVERLAP")[0]["recommendation"]
    assert "row 16 -> EFFECTIVEENDDATE 2024-06-30" in overlap


async def test_summary_lists_most_critical_first(run):
    r = await run()
    text = r["summary_report"]
    assert text.index("HIERARCHY_CYCLE") < text.index("NULL_VIOLATION")  # 4 rows before 1 row
    assert text.index("Fix these first") < text.index("Warnings")
