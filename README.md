# Automated Staging Gatekeeper & Data Integrity Validator

LangGraph **fan-out / fan-in** pipeline that validates a staged batch with three concurrent
workers and lets an aggregator issue `APPROVED` / `WARNING` / `REJECTED` plus an LLM-written summary of
what to fix first and how. It never changes data: no fix SQL is generated.

```
START -> orchestrator -> schema_worker ------\
                      -> referential_worker --+-> aggregator -> END
                      -> collision_worker ---/
```

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m staging_gatekeeper.graph                    # dirty batch (default)
.venv/bin/python -m staging_gatekeeper.graph --scenario warning # or: clean
.venv/bin/python -m staging_gatekeeper.graph --llm gemini       # needs GOOGLE_API_KEY in .env
.venv/bin/python -m pytest
```

No database or API key needed by default: a mock database and a deterministic mock LLM are used.
For Gemini, copy `.env.example` to `.env` and set `GOOGLE_API_KEY` (optionally `GEMINI_MODEL`).

## Layout

| File | Role |
|---|---|
| `staging_gatekeeper/graph.py` | `build_workflow()`, `workflow`, `app = workflow.compile()`, demo `__main__` |
| `state.py` | `GatekeeperState` with the `findings` `operator.add` reducer |
| `nodes/orchestrator.py` | validates payload, loads table schema into `metadata`, empty batch => CRITICAL |
| `nodes/schema_worker.py` | NULLs, type castability, length/range bounds, formats |
| `nodes/referential_worker.py` | foreign keys, orphaned parents, circular hierarchy loops |
| `nodes/collision_worker.py` | duplicate business keys, master collisions, effective-date overlaps |
| `nodes/aggregator.py` | merge, verdict rules, LLM summary (with deterministic fallback) |
| `queries.py` | parameterised T-SQL per check (`@batch_id`, `@tenant_id`) as `CheckQuery` objects |
| `mock_db.py` | `StagingDatabase` protocol + in-memory `MockStagingDatabase` |
| `llm.py` | `MockReportLLM`, `ChatModelReportLLM` (wraps any LangChain chat model), the prompt |
| `demo_data.py` | `STG_DEPARTMENT` schema and batches with one seeded defect per check |

## Design decisions

* **Verdict is code, not LLM.** Any CRITICAL (or unrecognised severity) => `REJECTED`; only
  WARNINGs => `WARNING`; none => `APPROVED`.
* **The LLM only polishes.** For non-approved batches it receives the merged findings (most
  critical first: severity, then rows affected) and writes a short report: the top issues and how
  to correct each, grounded in each worker's `recommendation`. It has no tools, sees no database,
  and writes no SQL. `remediation_sql` stays `None`; fixes are made at the source and the batch
  is re-submitted. If the model errors or replies empty, a deterministic report is used.
* **Fail closed.** A check that errors becomes a CRITICAL `WORKER_FAILURE` finding; an empty
  batch is `REJECTED`. Transient DB errors (`TransientDatabaseError`) are retried via
  `RetryPolicy` instead.
* **Workers** derive their checks from the schema the orchestrator puts in `metadata`, so
  supporting another staging table means registering a `TableSchema`, not writing new workers.
* **Going live:** implement `StagingDatabase` (`get_table_schema`, `count_batch_rows`, `run_check`
  executing `query.sql` with `query.params`) and pass it to `build_workflow(db, llm)`. For a real
  model: `ChatModelReportLLM(ChatGoogleGenerativeAI(model=...))` (or any LangChain chat model).

## Caveat

The T-SQL in `queries.py` has not been executed against SQL Server in this build; only the mock
evaluators behind it are tested. Validate it (especially the recursive-CTE cycle check and the
`LIKE` format rules, which the mock mirrors with regexes) before pointing at a live database.
