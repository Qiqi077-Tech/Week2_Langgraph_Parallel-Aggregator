import json

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage

from staging_gatekeeper.graph import build_workflow
from staging_gatekeeper.llm import MAX_ROWS_SHOWN, SYSTEM_PROMPT, ChatModelReportLLM, ReportResponse
from staging_gatekeeper.nodes.aggregator import decide_verdict, merge_findings

INPUT = {"batch_id": "BATCH-2026-0926-001", "target_table": "dbo.STG_DEPARTMENT", "tenant_id": 42}


def f(severity, check="X", rows=(1,)):
    return {"severity": severity, "check": check, "column": None, "affected_rows": list(rows),
            "affected_count": len(rows), "message": check}


def test_verdict_rules():
    assert decide_verdict([]) == "APPROVED"
    assert decide_verdict([f("WARNING")]) == "WARNING"
    assert decide_verdict([f("WARNING"), f("CRITICAL")]) == "REJECTED"
    assert decide_verdict([f("MYSTERY")]) == "REJECTED"  # unknown severity fails closed


def test_merge_drops_exact_duplicates_and_orders_by_severity_then_impact():
    merged = merge_findings([f("WARNING", "W", [1, 2, 3]), f("CRITICAL", "SMALL", [1]),
                             f("CRITICAL", "BIG", [1, 2, 3]), f("CRITICAL", "SMALL", [1])])
    assert [x["check"] for x in merged] == ["BIG", "SMALL", "W"]


class RecordingModel(GenericFakeChatModel):
    """Fake chat model that also remembers what it was sent."""

    seen: list = []

    def _generate(self, messages, *args, **kwargs):
        self.seen.append(messages)
        return super()._generate(messages, *args, **kwargs)


def fake_llm(reply: str) -> tuple[ChatModelReportLLM, RecordingModel]:
    model = RecordingModel(messages=iter([AIMessage(content=reply)]), seen=[])
    return ChatModelReportLLM(model), model


async def test_llm_text_becomes_the_summary_but_never_changes_verdict_or_data(db):
    llm, model = fake_llm("Critical: cycles in rows 10-13. Fix: re-point one parent link.")
    r = await build_workflow(db, llm).compile().ainvoke(INPUT)
    assert r["summary_report"] == "Critical: cycles in rows 10-13. Fix: re-point one parent link."
    assert r["final_verdict"] == "REJECTED"
    assert r["remediation_sql"] is None
    # the model saw the ordered findings, with the validators' recommendations
    system, human = model.seen[0]
    assert system.content == SYSTEM_PROMPT and "no SQL" in SYSTEM_PROMPT
    sent = json.loads(human.content)
    assert sent["verdict"] == "REJECTED"
    assert sent["findings"][0]["check"] == "HIERARCHY_CYCLE"
    assert all(x["recommendation"] for x in sent["findings"])


async def test_row_ids_sent_to_the_model_are_capped():
    from staging_gatekeeper.llm import ReportRequest

    llm, model = fake_llm("ok")
    big = f("CRITICAL", "BIG", range(1, 500))
    await llm.generate(ReportRequest(batch_id="b", target_table="t", tenant_id=1, row_count=499,
                                     verdict="REJECTED", findings=[big]))
    sent = json.loads(model.seen[0][1].content)["findings"][0]
    assert len(sent["affected_rows"]) == MAX_ROWS_SHOWN and sent["affected_count"] == 499


async def test_empty_llm_reply_falls_back_to_deterministic_report(db):
    llm, _ = fake_llm("   ")
    r = await build_workflow(db, llm).compile().ainvoke(INPUT)
    assert r["final_verdict"] == "REJECTED"
    assert "deterministic report shown" in r["summary_report"]
    assert "How to fix" in r["summary_report"]


async def test_llm_exception_falls_back_too(db):
    class Down:
        async def generate(self, request):
            raise TimeoutError("gemini unreachable")

    r = await build_workflow(db, Down()).compile().ainvoke(INPUT)
    assert r["final_verdict"] == "REJECTED"
    assert "TimeoutError" in r["summary_report"] and "How to fix" in r["summary_report"]


async def test_llm_interface_is_pluggable(db):
    class Stub:
        async def generate(self, request):
            return ReportResponse(summary_report=f"{request.verdict}/{len(request.findings)}")

    r = await build_workflow(db, Stub()).compile().ainvoke(INPUT)
    assert r["summary_report"] == "REJECTED/15"
