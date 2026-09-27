You are an expert Python and AI engineer specializing in LangGraph, asynchronous 
programming, and database architecture.

Please build a modular, production-ready LangGraph implementation of 
the **Parallel + Aggregator** (fan-out / fan-in) pattern for an 
**Automated Staging Gatekeeper & Data Integrity Validator**.

### Architecture & Objective
When staging data lands in ingestion/staging tables, multiple orthogonal 
validation workers must inspect the batch concurrently before an aggregator merges 
the results to issue a final verdict (`APPROVED`, `WARNING`, or `REJECTED`) and 
generate remediation SQL.

[ Orchestrator ]
                 /         |         \
                /          |          \
    [ Worker 1 ]      [ Worker 2 ]     [ Worker 3 ]
   Schema & Null     Referential &     Logical Overlap &
   Constraint Linter  Hierarchy Check   Collision Detector
                \          |          /
                 \         |         /
                   [ Aggregator ]
           (Decision Engine & SQL Fixer)
           
           ### Specifications & Requirements

1. **State Schema (`TypedDict` or Pydantic):**
   * Fields: `batch_id` (str), `target_table` (str), `tenant_id` (int), `metadata` (dict).
   * Reducer field: `findings: Annotated[List[Dict[str, Any]], operator.add]` 
   so that findings from concurrent worker nodes append automatically to the shared state.
   * Final outputs: `final_verdict` (Literal["APPROVED", "WARNING", "REJECTED"]), 
   `summary_report` (str), `remediation_sql` (Optional[str]).

2. **Graph Nodes:**
   * **Orchestrator:** Parses incoming batch payload, extracts target table 
   schema metadata, and sets up state for parallel dispatch.
   * **Worker 1 (Schema & Null Integrity):** Asynchronously checks for NULL 
   violations on non-nullable target columns, invalid formats, and data type bounds.
   * **Worker 2 (Referential & Hierarchy Check):** Asynchronously verifies 
   foreign key validity against parent/master tables and detects broken hierarchical 
   references or circular dependency loops.
   * **Worker 3 (Logical Collision & Date Overlap):** Asynchronously inspects 
   duplicate unique business keys and overlapping effective date windows 
   (`EFFECTIVESTARTDATE` to `EFFECTIVEENDDATE`).
   * **Aggregator Node:**
     * Fans in and merges all `findings`.
     * Applies threshold logic: If critical errors exist, verdict is `REJECTED`; 
     if only warnings exist, verdict is `WARNING`; if zero issues, verdict is `APPROVED`.
     * Uses an LLM (or mockable LLM chain) to synthesize an executive error report 
     and generate concrete remediation T-SQL statements (e.g., targeted `UPDATE` 
     or `DELETE` statements on `#STAGE_ERRORS`).

3. **LangGraph Construction:**
   * Use `langgraph.graph.StateGraph`, `START`, and `END`.
   * Set up parallel edges from `orchestrator` to all three workers.
   * Set up convergence edges from all three workers directly to `aggregator`.
   * Compile the graph into an executable runnable (`app = workflow.compile()`).

4. **Code Quality & Mock Testing:**
   * Include type hints, clean async handling, and structured mock database 
   check queries (or a mock data simulator) so the code runs standalone 
   out-of-the-box without requiring an active live database connection.
   * Include a runnable `if __name__ == "__main__":` block demonstrating 
   a test run on a mock staging batch containing deliberate validation errors.