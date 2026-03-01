# Agentrail Auto-Delivery Runbook

## 1. Scope
Operational playbook for chat-driven automated delivery with `agentrail` (M1-M5 baseline).

## 2. Daily startup checks
1. Verify toolchain and workspace health.
2. Verify orchestrator command surface and MCP tool surface.
3. Verify compatibility fixtures and contract tests.
4. Verify runtime service mode endpoints (`/healthz`) when HTTP transport is used.

Recommended commands:
```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace

# HTTP MCP service smoke check
cargo run -p agentrail-cli -- serve --transport http --bind 127.0.0.1:8787
# in another shell
curl -s http://127.0.0.1:8787/healthz
```

## 3. Runtime workflow
1. Receive requirement from chat.
2. Decompose into DAG tasks.
3. Schedule dependency-ready tasks.
4. Launch tasks with per-task runtime state transitions.
5. Run review and validation gates.
6. Apply wake/reassign/escalate policy on failures.
7. Merge and cleanup when all required gates pass.

## 4. Failure severity levels
- `S0` Critical outage:
  - control plane unavailable
  - data loss or unrecoverable state corruption
- `S1` Delivery blocked:
  - repeated task failures exceed retry policy
  - gate evaluator cannot reach terminal decision
- `S2` Degraded but serviceable:
  - partial queue slowdown
  - non-critical tool output mismatch

## 5. Auto-retry policy
- Retry budget is task-level.
- Before budget exhaustion: wake original agent.
- On budget exhaustion first time: reassign fresh worker.
- If budget is exhausted after reassignment: escalate to `needs_attention`.

## 6. Recovery procedures
### 6.1 Process restart
1. Rehydrate runtime state from snapshot.
2. Re-run scheduler to compute ready queue.
3. Resume task execution for non-terminal states.

### 6.2 Partial launch failure
1. Keep already launched task records in running state.
2. Mark failed launch task as retryable.
3. Use retry policy to decide wake/reassign/escalate.

## 7. Rollback procedure
1. Freeze new launches.
2. Stop queue ticks.
3. Revert to previous known-good binary/commit.
4. Re-run workspace checks and contract tests.
5. Resume scheduler only after green verification.

## 8. Observability expectations
- Structured events for task launch, transition, tool calls, and failures.
- Per-task logs include `task_id`, state transition, and outcome.
- Gate decisions record required and optional check failures.
- Runtime poll loop should emit warning-level events on state refresh failures.

## 9. Human escalation trigger
Escalate to operator when either condition is true:
1. Same task hits terminal escalation state.
2. Contract tests fail after automated retry cycle.
