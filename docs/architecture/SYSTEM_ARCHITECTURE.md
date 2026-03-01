# agentrail System Architecture

## 1. Context
agentrail sits between orchestration layers and execution agents.

```text
Chat App -> Orchestrator -> agentrail MCP/CLI -> Core/Storage/Runner -> Worktrees/Agents
```

## 2. Layers
### Interface layer
- `agentrail-cli`: local and CI entrypoints.
- `agentrail-mcp`: tool-facing server endpoint.

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
- Dashboard renderer: static HTML generation.

## 3. Storage strategy
- Plan state: file-based YAML with hash compare-and-swap.
- Control/runtime state: transactional store (SQLite WAL target).

## 4. Concurrency model
- Code changes isolated by worktree per task.
- Shared state guarded by transaction/CAS semantics.
- Reader-heavy paths optimized for low overhead parsing and indexing.

## 5. Runner model
- `ProcessRunner`: short-lived deterministic execution.
- `TmuxRunner`: long-lived steerable sessions with attach/replay.
- Unified control actions at trait level for adapter portability.

## 6. Evolution path
1. Contract freeze and differential harness.
2. Read-path compatibility.
3. Write-path compatibility with CAS guarantees.
4. Full MCP/CLI parity and dashboard parity.
5. Performance tuning and migration cutover.
