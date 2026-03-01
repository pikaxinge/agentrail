# agentrail System Architecture

## 1. Context
agentrail sits between orchestration layers and execution agents.

```text
Chat App -> Orchestrator -> agentrail MCP/CLI -> Core/Storage/Runner -> Worktrees/Agents
```

## 2. Layers
### Interface layer
- `agentrail-cli`: local and CI entrypoints.
  - Plan control: `status/show/next/claim/complete`.
  - Human-facing outputs: `dashboard --plan --out`, `dag --plan`, `report --plan`.
  - Service mode: `serve --transport stdio|http --bind`.
- `agentrail-mcp`: tool-facing server endpoint.
  - Low-level tools: `plan_*`, `orchestrate_*`.
  - High-level tools: `delivery_submit/status/steer/report`.
  - Transports: stdio and HTTP (`/mcp`, `/healthz`).

### Application layer
- Use cases for status/show/claim/complete/mutate/review/checkpoint.
- Shared orchestration flow independent of interface type.

### Domain layer
- Plan, phase, and step models.
- Status state machine and lock semantics.
- DAG ordering and validation rules.

### Infrastructure layer
- Plan repository: YAML + CAS + atomic write.
- Task store: transactional state for sessions, retries, and events.
- Runner adapters: process and tmux implementations.
- Dashboard/report renderer: static HTML, Mermaid DAG text, markdown report text.
- Orchestration runtime: DAG scheduler + gate evaluator + retry/reassignment.
 - Runtime service loop: periodic runner-state polling and runtime-state reconciliation.

## 3. Storage strategy
- Plan state: file-based YAML with hash compare-and-swap.
- Control/runtime state: transactional store (SQLite WAL target).

## 4. Concurrency model
- Code changes isolated by worktree per task.
- Shared state guarded by transaction/CAS semantics.
- Reader-heavy paths optimized for low overhead parsing and indexing.
- Scheduler selects dependency-ready tasks and dispatches within concurrency budget.

## 5. Runner model
- `ProcessRunner`: short-lived deterministic execution.
- `TmuxRunner`: long-lived steerable sessions with attach/replay.
- Unified control actions at trait level for adapter portability.
- On review failures, main orchestrator resumes the original task agent first, then reassigns when retry budget is exceeded.

## 6. Auto-delivery control loop
1. Ingest requirement and build task DAG.
2. Allocate worktree and branch per runnable node.
3. Dispatch worker subagents.
4. Collect review and gate evidence.
5. If failed, wake agent for targeted repair and re-validate.
6. Merge and cleanup automatically on success.

## 7. Evolution path
1. Contract freeze and differential harness.
2. Read-path compatibility.
3. Write-path compatibility with CAS guarantees.
4. Full MCP/CLI parity and dashboard parity.
5. Performance tuning and migration cutover.
