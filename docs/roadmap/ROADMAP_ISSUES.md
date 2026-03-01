# Agentrail Roadmap Issues

Last updated: 2026-03-01
Owner: Core platform
Status: Draft for execution planning

This roadmap converts the current discussion into issue-style work items.
Each issue is intentionally implementation-ready and includes acceptance gates.

## AR-101 Runtime Store Default and Config Hardening

- Problem
  - Runtime state can default to in-memory DSN, which is unsafe for restart recovery.
- Goal
  - Make file-backed SQLite the safe default for runtime task state.
- Scope In
  - Set default runtime DSN to `sqlite://.agentrail/runtime.db`.
  - Keep explicit override via `AGENTRAIL_RUNTIME_DSN`.
  - Ensure parent directory bootstrap.
  - Emit startup log line with effective DSN and mode.
- Scope Out
  - Distributed database support.
- Acceptance Criteria
  - With no env var, process starts and creates `runtime.db` automatically.
  - Restart preserves task rows.
  - WAL mode is verified in startup path.
- Risks
  - Relative path confusion across different working directories.
- Test Plan
  - Unit test for DSN resolver.
  - Integration test for create/restart/read flow.

## AR-102 Session and Attempt Persistence Schema

- Problem
  - In-memory task to session mapping is lost on restart.
- Goal
  - Persist runner sessions and attempts with deterministic recovery metadata.
- Scope In
  - Add tables:
    - `task_attempts(task_id, attempt, runner_mode, command, args_json, workdir, state, exit_code, started_at, ended_at)`
    - `task_sessions(task_id, attempt, session_id, runner_mode, attached, last_seen_at)`
  - Add read/write APIs in `agentrail-store`.
  - Write session records on start and transition updates.
- Scope Out
  - Full-text log storage in DB.
- Acceptance Criteria
  - New attempt row appears on each launch.
  - Session row exists while running and remains queryable after exit.
  - Data survives process restart.
- Risks
  - Backfill behavior for existing runtime DB.
- Test Plan
  - Migration test from empty DB.
  - Store contract tests for insert/update/query.

## AR-103 Startup Recovery and Reconciliation

- Problem
  - Runtime reboot cannot deterministically reconcile `preparing/running` tasks.
- Goal
  - Rebuild runtime view from persisted attempts and live runner sessions.
- Scope In
  - Recovery scan at service bootstrap:
    - `running/preparing` + live session -> `running` and attached.
    - `running/preparing` + missing session -> `failed_retryable`.
  - Emit structured recovery events.
  - Idempotent recovery execution.
- Scope Out
  - External scheduler integration.
- Acceptance Criteria
  - Crash and restart does not leave zombie `running` state without session.
  - Recovery path is safe to run multiple times.
- Risks
  - False negatives in session liveness checks.
- Test Plan
  - Integration tests for both attach and missing-session branches.

## AR-104 Delivery Lifecycle MCP Controls

- Problem
  - High-level tools expose submit/status/steer/report only.
- Goal
  - Provide full lifecycle controls for chat-first operation.
- Scope In
  - Add high-level tools:
    - `delivery_pause`
    - `delivery_resume`
    - `delivery_stop`
    - `delivery_retry`
  - Validate per-state action matrix.
  - Keep payload shape stable and explicit for chat orchestrators.
- Scope Out
  - Vendor-specific policy engines.
- Acceptance Criteria
  - Each tool returns stable JSON envelope with `task_id`, `status`, `runtime_state`.
  - Invalid transitions fail with deterministic error messages.
- Risks
  - Pause/resume reliability varies by runner backend.
- Test Plan
  - MCP tools tests for each action and rejection case.

## AR-105 Retry Policy Contract and Idempotency

- Problem
  - Retry behavior needs explicit, non-ambiguous policy.
- Goal
  - Define and enforce retry semantics across runtime and MCP.
- Scope In
  - Retry eligibility matrix by runtime state.
  - Attempt counter increment and budget enforcement.
  - Idempotency key for repeat retry requests.
  - Clear mapping of retry failure reasons.
- Scope Out
  - Adaptive ML-based retry policies.
- Acceptance Criteria
  - Retry cannot bypass budget.
  - Duplicate retry request with same idempotency key is safe.
  - Audit trail includes old/new attempt numbers.
- Risks
  - Edge cases when retry overlaps late status updates.
- Test Plan
  - Transaction-level race tests.
  - End-to-end retry loops with forced failures.

## AR-106 Task Timeline Event Store

- Problem
  - Raw logs are not sufficient for audit, chat summaries, or deterministic diagnosis.
- Goal
  - Add structured task timeline events as first-class data.
- Scope In
  - Add table:
    - `task_events(id, task_id, attempt, ts_ms, event_type, source, actor, session_id, state_before, state_after, message, payload_json, idem_key)`
  - Write events from MCP operations, runtime transitions, runner exits, and recovery.
  - Add indexes on `(task_id, id)`, `(task_id, attempt, id)`, `(ts_ms)`.
- Scope Out
  - Arbitrary event analytics pipeline.
- Acceptance Criteria
  - Every state transition emits exactly one timeline event.
  - Runner exit emits `exit_code` and mapped terminal state.
  - Event writes are transactionally aligned with state writes.
- Risks
  - Event volume growth without retention policy.
- Test Plan
  - Unit tests for event writer.
  - Integration tests asserting event sequences.

## AR-107 Timeline Query and Chat Summaries

- Problem
  - Chat users need compact, human-readable progress and failure explanation.
- Goal
  - Expose timeline query and summary endpoints via MCP.
- Scope In
  - Add `delivery_timeline(task_id, limit, cursor)` tool.
  - Add optional `delivery_explain_failure(task_id)` summary tool.
  - Return deterministic, cursor-based pagination.
- Scope Out
  - UI dashboard redesign.
- Acceptance Criteria
  - Timeline returns stable ordering and pagination.
  - Failure summary references latest relevant runner exit and transition events.
- Risks
  - Summary quality drift without strict templates.
- Test Plan
  - MCP protocol tests for pagination and empty timelines.

## AR-108 Runtime Consistency and CAS Guardrails

- Problem
  - Concurrent operations can create stale writes and duplicate transitions.
- Goal
  - Introduce explicit CAS/version guardrails for runtime mutations.
- Scope In
  - Add `version` column for mutable runtime entities.
  - Enforce compare-and-swap on state mutations.
  - Add conflict error surface for callers.
- Scope Out
  - Cross-region distributed locks.
- Acceptance Criteria
  - Conflicting writes fail cleanly without corrupting state.
  - Poll loop and manual action paths remain consistent under contention.
- Risks
  - Additional write-path complexity.
- Test Plan
  - Concurrency tests with parallel mutation workloads.

## AR-109 End-to-End Runner Validation with Real Agent CLI

- Problem
  - Unit tests pass but chat-to-runner path still needs real local validation.
- Goal
  - Validate complete control-plane flow against real local agent binary.
- Scope In
  - Add opt-in E2E harness for local environment:
    - `delivery_submit -> delivery_status -> delivery_steer -> delivery_stop/retry`
  - Capture artifacts for failure triage.
  - Mark as ignored in CI unless explicitly enabled.
- Scope Out
  - Hosted multi-tenant E2E lab.
- Acceptance Criteria
  - Local run validates happy path and failure path.
  - Artifacts include timeline snapshot and runner logs.
- Risks
  - Environment-specific flakiness.
- Test Plan
  - Scripted E2E with deterministic smoke commands.

## AR-110 Chat Connector Contract for Feishu/Telegram/WhatsApp

- Problem
  - End users should not need CLI syntax to control runtime.
- Goal
  - Define chat connector contract from user messages to `delivery_*` tool calls.
- Scope In
  - Intent to action mapping schema.
  - User auth and authorization envelope.
  - Message templates for progress, failure, and approval-needed states.
- Scope Out
  - Full connector implementation in this phase.
- Acceptance Criteria
  - Connector spec can drive a minimal adapter implementation without ambiguity.
  - Error and retry semantics are explicitly defined.
- Risks
  - Divergent behavior across messaging platforms.
- Test Plan
  - Contract tests over JSON message fixtures.

## AR-111 Interactive DAG Control Console (Deferred Proposal)

- Problem
  - Chat/CLI workflows are effective but become cognitively expensive for large dependency graphs.
- Goal
  - Provide an interactive HTML DAG control console for visual inspection and node-level control actions.
- Scope In
  - DAG home with pan/zoom and status badges.
  - Node hover summary and click-through detail pane.
  - Command panel for safe runtime actions (`steer/stop/retry` as first slice).
  - UI actions routed through existing MCP/runtime APIs only.
- Scope Out
  - Replacing chat-first workflows.
  - New source-of-truth data model outside runtime store and plan state.
- Acceptance Criteria
  - Read-only DAG render from live snapshot.
  - Node detail pane with dependencies and recent events.
  - At least three control actions wired end-to-end through MCP.
  - No regression in existing chat/CLI orchestration flows.
- Risks
  - UI scope creep delaying core runtime hardening.
  - Large-graph rendering performance.
- Test Plan
  - UI contract tests for snapshot schema.
  - End-to-end action tests for command dispatch and result feedback.
- Proposal Doc
  - `docs/roadmap/proposals/2026-03-01-interactive-dag-control-console-proposal.md`

## Priority and Suggested Sequence

- P0
  - AR-101, AR-102, AR-103
- P1
  - AR-104, AR-105, AR-106
- P2
  - AR-107, AR-108
- P3
  - AR-109, AR-110
- P4
  - AR-111 (deferred proposal)

## Definition of Done for This Roadmap Batch

- All P0 and P1 issues completed.
- Workspace verification green:
  - `cargo fmt --all --check`
  - `cargo check --workspace`
  - `cargo test --workspace`
- Docs updated:
  - requirements and architecture sections aligned with final behavior.
