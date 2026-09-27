"""A guided, step-by-step run of the gatekeeper on a tiny batch.

    python walkthrough.py              # deterministic mock LLM
    python walkthrough.py --llm gemini # Gemini writes the summary (needs GOOGLE_API_KEY in .env)

An 8-row staging batch is loaded, each row engineered to trigger (or avoid) one check. The graph
runs with `stream_mode="updates"` so you see each node's output as it finishes. Then the source
data is "corrected" and the batch is re-submitted.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from datetime import date
from typing import Any

from staging_gatekeeper.demo_data import COSTCENTERS, DEPARTMENT_SCHEMA, STAGING_TABLE, TENANT_ID
from staging_gatekeeper.graph import build_llm, build_workflow
from staging_gatekeeper.mock_db import MockStagingDatabase

BATCH = "DEMO-001"
COLUMNS = ["DEPARTMENTCODE", "DEPARTMENTNAME", "COSTCENTERID", "PARENTDEPARTMENTCODE",
           "MANAGEREMAIL", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE"]


def _row(row_id: int, code: str, name: str | None, cc: int = 100, parent: str | None = None,
         email: str | None = None, start: date = date(2024, 1, 1), end: date | None = None) -> dict[str, Any]:
    return {"STAGE_ROW_ID": row_id, "BATCH_ID": BATCH, "TENANT_ID": TENANT_ID, "DEPARTMENTCODE": code,
            "DEPARTMENTNAME": name, "COSTCENTERID": cc, "PARENTDEPARTMENTCODE": parent, "MANAGEREMAIL": email,
            "HEADCOUNT": 10, "EFFECTIVESTARTDATE": start, "EFFECTIVEENDDATE": end}


# What each row is for (printed next to the data, and asserted by tests/test_walkthrough.py).
EXPECTATION = {
    1: "clean",
    2: "NULL name                       -> Worker 1 (schema)      CRITICAL",
    3: "cost center 555 doesn't exist   -> Worker 2 (referential) CRITICAL",
    4: "malformed email                 -> Worker 1 (schema)      WARNING",
    5: "LEG-001 window 2024-01-01..open -> Worker 3 (collision)   CRITICAL (overlaps row 6)",
    6: "LEG-001 window 2024-06-01..open -> Worker 3 (collision)   CRITICAL (overlaps row 5)",
    7: "parent is row 8                 -> Worker 2 (referential) CRITICAL (circular with row 8)",
    8: "parent is row 7                 -> Worker 2 (referential) CRITICAL (circular with row 7)",
}


def sample_rows(*, fixed: bool = False) -> list[dict[str, Any]]:
    return [
        _row(1, "HRD-001", "Human Resources", 100, email="hr@corp.com"),
        _row(2, "FIN-001", "Finance" if fixed else None, 200),
        _row(3, "OPS-001", "Operations", 200 if fixed else 555),
        _row(4, "MKT-001", "Marketing", 100, email="marketing@corp.com" if fixed else "marketing-at-corp"),
        _row(5, "LEG-001", "Legal", 100, start=date(2024, 1, 1), end=date(2024, 5, 31) if fixed else None),
        _row(6, "LEG-001", "Legal (v2)", 100, start=date(2024, 6, 1)),
        _row(7, "AAA-001", "Team A", 100, parent="BBB-001"),
        _row(8, "BBB-001", "Team B", 100, parent=None if fixed else "AAA-001"),
    ]


def build_sample_database(*, fixed: bool = False, latency: float = 0.05) -> MockStagingDatabase:
    return MockStagingDatabase(
        schemas=[DEPARTMENT_SCHEMA],
        staging={STAGING_TABLE: sample_rows(fixed=fixed)},
        masters={
            "dbo.COSTCENTER": COSTCENTERS,
            "dbo.DEPARTMENT": [{"TENANT_ID": TENANT_ID, "DEPARTMENTCODE": "ENG-001", "PARENTDEPARTMENTCODE": None,
                                "EFFECTIVESTARTDATE": date(2020, 1, 1), "EFFECTIVEENDDATE": None}],
        },
        latency=latency,
    )


# --- printing ----------------------------------------------------------------------------

def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def show_input(rows: list[dict[str, Any]], with_expectations: bool) -> None:
    short = {"DEPARTMENTCODE": "CODE", "DEPARTMENTNAME": "NAME", "COSTCENTERID": "CC", "PARENTDEPARTMENTCODE": "PARENT",
             "MANAGEREMAIL": "EMAIL", "EFFECTIVESTARTDATE": "START", "EFFECTIVEENDDATE": "END"}
    widths = [max(len(short[c]), *(len(str(r[c]) if r[c] is not None else "NULL") for r in rows)) for c in COLUMNS]
    print("ID  " + "  ".join(short[c].ljust(w) for c, w in zip(COLUMNS, widths)))
    for r in rows:
        cells = "  ".join((str(r[c]) if r[c] is not None else "NULL").ljust(w) for c, w in zip(COLUMNS, widths))
        print(f"{r['STAGE_ROW_ID']:<3} {cells}" + (f"   <- {EXPECTATION[r['STAGE_ROW_ID']]}" if with_expectations else ""))


def show_node(node: str, update: dict[str, Any], t0: float) -> None:
    at = f"[+{time.perf_counter() - t0:0.2f}s]"
    if node == "orchestrator":
        meta = update["metadata"]
        cols = ", ".join(c["name"] for c in meta["table_schema"]["columns"])
        print(f"\n{at} ORCHESTRATOR  parsed the payload, loaded the schema for {update['target_table']}")
        print(f"        rows in batch : {meta['row_count']}")
        print(f"        columns       : {cols}")
        print(f"        foreign keys  : {[fk['column'] + ' -> ' + fk['parent_table'] for fk in meta['table_schema']['foreign_keys']]}")
        print(f"        dispatching   : {meta['dispatch']}  (all three start at once)")
    elif node == "aggregator":
        print(f"\n{at} AGGREGATOR    verdict decided by code: {update['final_verdict']}")
    else:
        found = update["findings"]
        print(f"\n{at} {node.upper():<19} finished; returned {len(found)} finding(s) "
              "(appended to state.findings by the reducer)")
        for f in found:
            col = f"[{f['column']}]" if f["column"] else ""
            print(f"        {f['severity']:<8} {f['check']} {col} rows={f['affected_rows']}")
            print(f"                 why : {f['message']}")
            print(f"                 fix : {f['recommendation']}")


async def run_once(*, fixed: bool, llm_name: str) -> dict[str, Any]:
    db = build_sample_database(fixed=fixed)
    app = build_workflow(db, build_llm(llm_name)).compile()
    payload = {"batch_id": BATCH, "target_table": "dbo.STG_DEPARTMENT", "tenant_id": TENANT_ID}

    banner("STEP 1  Input: the staged batch that just landed" if not fixed else
           "STEP 5  The source data has been corrected; same batch, re-submitted")
    show_input(sample_rows(fixed=fixed), with_expectations=not fixed)
    print(f"\nRequest: {payload}")

    banner("STEP 2-4  Graph run: orchestrator -> 3 parallel workers -> aggregator" if not fixed else
           "Graph run again")
    t0, final = time.perf_counter(), {}
    async for chunk in app.astream(payload, stream_mode="updates"):
        for node, update in chunk.items():
            if node == "aggregator":
                final = update
            show_node(node, update, t0)

    if not fixed:
        example = next(q for q in db.executed if q.kind == "null_check" and q.params["column"] == "DEPARTMENTNAME")
        print(f"\n(The graph issued {len(db.executed)} check queries; peak {db.peak_in_flight} in flight at once.)")
        print("Example - the query behind the NULL check on DEPARTMENTNAME (parameters travel separately):\n")
        print("    " + example.sql.replace("\n", "\n    "))
        print(f"    params: batch_id={example.batch_id!r}, tenant_id={example.tenant_id}")

    banner("FINAL SUMMARY (what the aggregator returns)")
    print(final["summary_report"])
    print(f"\nfinal_verdict = {final['final_verdict']}    remediation_sql = {final['remediation_sql']}")
    return final


async def main(llm_name: str) -> None:
    first = await run_once(fixed=False, llm_name=llm_name)
    assert first["final_verdict"] == "REJECTED"
    second = await run_once(fixed=True, llm_name="mock")  # nothing to explain when approved
    assert second["final_verdict"] == "APPROVED"
    banner("Done: REJECTED -> fix the source -> APPROVED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--llm", choices=["mock", "gemini"], default="mock")
    asyncio.run(main(parser.parse_args().llm))
