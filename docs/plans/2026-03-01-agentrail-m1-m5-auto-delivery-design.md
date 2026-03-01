# Agentrail M1-M5 Auto-Delivery Design

> Status: approved for execution.

## 1. Goal
Build a fully automated delivery loop for `agentrail` from M1 to M5 with no manual merge gate.

## 2. Product outcome
- User gives requirement in chat app.
- Orchestrator decomposes work into a DAG.
- Subagents execute in isolated worktrees.
- Review and tests run automatically.
- Failed reviews wake the original agent for repair.
- PR merges automatically when all gates pass.

## 3. Non-negotiable constraints
- Compatibility level 2 with legacy CLI/MCP behavior.
- Plan file writes must keep CAS + atomic write semantics.
- Shared runtime state must be transactional (SQLite WAL target).
- Worktree isolation handles code conflicts only, not shared-state consistency.

## 4. Stage map
1. M1: Read-path compatibility (`status/show/next/checkpoint`) via CLI + MCP.
2. M2: Write-path compatibility (`claim/complete/mutate/review`) with CAS safety.
3. M3: Runner control plane (`ProcessRunner` + `TmuxRunner`) with steer/pause/resume/stop/logs.
4. M4: Full auto-delivery loop (DAG scheduler, auto-PR, auto-review, auto-merge, auto-cleanup).
5. M5: Performance and reliability hardening (benchmarks, recovery, observability, migration).

## 5. Main agent and subagent contract
- Main agent owns orchestration, gate decisions, merge decisions, and retries.
- Worker subagent owns implementation task execution.
- Reviewer subagent owns risk-oriented code review.
- Awaiter subagent owns long-running waits (tests/build/CI polling).

Main agent is final owner. Subagents provide evidence, not final verdicts.

## 6. Review failure wake-up loop
1. Gate evaluator detects failure.
2. Main agent sends failure evidence to the original worker subagent.
3. Subagent is resumed/interrupted and asked to patch specific issues.
4. Gates rerun automatically.
5. After `max_retries` (default 3), task is reassigned to a fresh worker.

## 7. DAG scheduling model
- Each task has `id`, `deps`, `priority`, `runner_mode`, `retry_budget`.
- Scheduler runs any task whose dependencies are done.
- Concurrency budget is global and stage-aware.
- Failed nodes block downstream nodes unless marked optional.

## 8. Branch and worktree policy
- Branch name: `feat/<stage>-<task-id>`.
- Worktree path: `.worktrees/<stage>-<task-id>`.
- One task == one branch == one worktree.
- Cleanup on terminal states (`done`, `aborted`, `superseded`).

## 9. Gate matrix (auto-merge)
A PR is merged only when all required gates are green:
- `cargo fmt --all`
- `cargo check --workspace`
- `cargo test --workspace`
- compatibility tests (`agentrail-compat-tests`)
- reviewer verdict >= required threshold

If any gate fails, auto-fix loop starts.

## 10. State model
Task runtime states:
`queued -> preparing -> running -> review_failed -> fixing -> validating -> ready_to_merge -> merged`

Failure states:
`failed_retryable`, `failed_terminal`, `needs_attention`

## 11. Observability model
- Structured events for every task transition.
- Per-task timeline for run, review, fix, and merge phases.
- Latency metrics for hot read paths and gate duration.
- Retry counters and failure reason taxonomy.

## 12. Done criteria
- M1-M5 artifacts implemented and tested.
- End-to-end dry run produces auto-created branch, PR, fixes, and merge.
- Recovery verified across orchestrator restart.
- Rollback path preserved.
