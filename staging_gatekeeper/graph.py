"""Graph construction and a runnable demo.

    START -> orchestrator -> {schema_worker, referential_worker, collision_worker} -> aggregator -> END

Run:  python -m staging_gatekeeper.graph [--scenario dirty|warning|clean] [--llm mock|gemini]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from typing import Any

from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy

from .demo_data import BATCH_CLEAN, BATCH_DIRTY, BATCH_WARNING, STAGING_TABLE, TENANT_ID, build_demo_database
from .llm import ChatModelReportLLM, MockReportLLM, ReportLLM
from .mock_db import StagingDatabase, TransientDatabaseError
from .nodes import (
    WORKERS,
    make_aggregator,
    make_collision_worker,
    make_orchestrator,
    make_referential_worker,
    make_schema_worker,
)
from .state import GatekeeperState

# Only transient DB errors are retried; anything else surfaces as a finding or an exception.
DB_RETRY = RetryPolicy(max_attempts=3, initial_interval=0.1, retry_on=TransientDatabaseError)


def build_workflow(db: StagingDatabase | None = None, llm: ReportLLM | None = None) -> StateGraph:
    """Uncompiled fan-out / fan-in workflow. Defaults: mock demo database and mock LLM."""
    db = db or build_demo_database()

    workflow = StateGraph(GatekeeperState)
    workflow.add_node("orchestrator", make_orchestrator(db))
    workflow.add_node("schema_worker", make_schema_worker(db), retry_policy=DB_RETRY)
    workflow.add_node("referential_worker", make_referential_worker(db), retry_policy=DB_RETRY)
    workflow.add_node("collision_worker", make_collision_worker(db), retry_policy=DB_RETRY)
    workflow.add_node("aggregator", make_aggregator(llm))

    workflow.add_edge(START, "orchestrator")
    for worker in WORKERS:  # fan-out: all three run in the same superstep
        workflow.add_edge("orchestrator", worker)
    workflow.add_edge(list(WORKERS), "aggregator")  # fan-in: waits for every worker
    workflow.add_edge("aggregator", END)
    return workflow


workflow = build_workflow()
app = workflow.compile()


# --- demo --------------------------------------------------------------------------------

SCENARIOS = {"dirty": BATCH_DIRTY, "warning": BATCH_WARNING, "clean": BATCH_CLEAN}


def print_result(result: dict[str, Any], elapsed: float) -> None:
    findings = result["findings"]
    print(f"\n=== VERDICT: {result['final_verdict']}  ({elapsed:.2f}s, {len(findings)} raw findings) ===\n")
    for f in sorted(findings, key=lambda f: (f["severity"] != "CRITICAL", f["check"])):
        col = f" [{f['column']}]" if f["column"] else ""
        print(f"{f['severity']:<8} {f['check']}{col}  rows={f['affected_rows']}\n         {f['message']}")
    print("\n--- Summary report ---\n" + result["summary_report"])


def build_llm(name: str) -> ReportLLM:
    if name == "mock":
        return MockReportLLM()
    from dotenv import load_dotenv
    from langchain_google_genai import ChatGoogleGenerativeAI

    load_dotenv()  # reads GOOGLE_API_KEY (and optionally GEMINI_MODEL) from a local .env
    if not os.getenv("GOOGLE_API_KEY"):
        raise SystemExit("GOOGLE_API_KEY is not set. Put it in .env or export it, then re-run.")
    model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")
    print(f"Using Gemini model: {model}")
    return ChatModelReportLLM(ChatGoogleGenerativeAI(model=model, temperature=0, timeout=30, max_retries=2))


async def main(scenario: str, llm_name: str = "mock") -> dict[str, Any]:
    db = build_demo_database()
    demo_app = build_workflow(db, build_llm(llm_name)).compile()
    started = time.perf_counter()
    result = await demo_app.ainvoke({
        "batch_id": SCENARIOS[scenario],
        "target_table": STAGING_TABLE,
        "tenant_id": TENANT_ID,
        "metadata": {"source_system": "HRIS-nightly"},
    })
    print_result(result, time.perf_counter() - started)
    print(f"\n({len(db.executed)} check queries issued, peak concurrency {db.peak_in_flight})")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scenario", choices=SCENARIOS, default="dirty")
    parser.add_argument("--llm", choices=["mock", "gemini"], default="mock",
                        help="gemini needs GOOGLE_API_KEY (env var or .env)")
    args = parser.parse_args()
    asyncio.run(main(args.scenario, args.llm))
