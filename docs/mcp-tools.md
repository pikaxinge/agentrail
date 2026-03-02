# MCP Tools Catalog

This document describes the currently supported MCP control surface in `agentrail`.

## Scope
- Source of truth for tool names and schemas: `crates/agentrail-mcp/src/lib.rs` (`mcp_tools_descriptor`).
- Contract tests: `crates/agentrail-mcp/tests/protocol.rs` and `crates/agentrail-mcp/tests/tools.rs`.

## Tool Groups

### Plan tools
- `plan_status`
- `plan_show`
- `plan_next`
- `plan_claim`
- `plan_complete`

Use these for DAG-backed planning and step-state transitions on a single `plan.yaml`.

### Runtime tools
- `orchestrate_start`
- `orchestrate_status`
- `orchestrate_steer`

Use these for low-level task runtime control.

### Delivery tools
- `delivery_submit`
- `delivery_status`
- `delivery_steer`
- `delivery_stop`
- `delivery_cleanup`
- `delivery_events_subscribe`
- `delivery_events_next`
- `delivery_events_ack`
- `delivery_report`

Use these for chat-first orchestration, lifecycle control, event streaming, and aggregate reporting.

## Core Contracts

### DAG contract for plan tools

#### DAG model
- Plan DAG is declared in YAML using `depends_on` edges.
- Two dependency layers exist:
  - phase-level: `phases[].depends_on`
  - step-level: `phases[].steps[].depends_on`
- A step is considered executable only when:
  - its containing phase is unlocked/active, and
  - all step dependencies are already `done`.

Example shape:

```yaml
version: 1
project: demo
phases:
  - id: phase-1
    steps:
      - id: step-a
        depends_on: []
      - id: step-b
        depends_on: [step-a]
  - id: phase-2
    depends_on: [phase-1]
    steps:
      - id: step-c
        depends_on: []
```

#### Execution semantics
- `plan_next`
  - returns the next ready step according to dependency readiness, not file order alone.
  - recomputes stale locked-phase state before selecting a candidate.
- `plan_claim`
  - requires dependency readiness checks to pass.
  - sets step ownership (`claimed_by`) and transitions to `claimed`.
  - recomputes stale locked-phase state before dependency checks.
- `plan_complete`
  - requires `agent` and owner match with current `claimed_by`.
  - writes `evidence` and transitions step to `done`.
  - recomputes stale locked-phase state before dependency checks.

#### Rejection and safety semantics
- Missing required inputs are rejected deterministically (for example missing `plan_path` or missing `agent`).
- Path access is root-guarded when allowed root is configured; out-of-root plan paths are rejected.
- Ownership violations are rejected (complete-by-non-owner fails when `claimed_by` does not match).

#### Observability semantics
- `plan_status` provides machine-readable summary fields (`phase_count`, `step_counts`, project metadata).
- `plan_show` returns a single step object with dependency and ownership fields.
- `plan_next`/`plan_claim`/`plan_complete` responses include the selected/updated step payload so chat orchestrators can render DAG progress without parsing logs.

#### Visualization semantics
- MCP plan tools expose structured DAG progress data.
- CLI provides dedicated graph rendering:
  - `dag --plan <path>` emits Mermaid DAG text.
  - `dashboard --plan <path> --out <path>` emits static HTML + JSON summary.
  - `report --plan <path>` emits Markdown status report.

### `delivery_submit`
- Starts a tracked delivery task.
- Required input: `task_id`.
- Important options:
  - `runner_mode`: `process` or `app_server`.
  - `app_server_request_policy` (app_server only): `deny_all` (default), `allow_safe_subset`, `delegate_fail_open` (disabled by default).
  - `steer_required`: when `true`, submission must satisfy steer preflight rules.
  - `interactive_command`: deprecated compatibility field. Runtime ignores behavior, but when provided `delivery_submit` emits deterministic warning: `interactive_command is deprecated and ignored; use runner_mode=app_server + steer_required for steerable sessions`.
  - `idempotency_key`: dedupe repeated submit intents.

Idempotency behavior:
- replay occurs only when both `(task_id, idempotency_key)` and request fingerprint match.
- fingerprint currently binds `runner_mode`, command shape (`command` + `args`), `workdir`, and `app_server_request_policy`.
- if key matches but fingerprint differs, tool returns deterministic error containing `idempotency_key_conflict`.

Preflight behavior:
- `steer_required=true` requires `runner_mode=app_server`.
- `runner_mode=process` rejects `codex exec` and `codex-guarded-exec` command shapes (to avoid nondeterministic skill-intake loops), regardless of `steer_required`.
- `app_server_request_policy` is valid only with `runner_mode=app_server`.
- Use `runner_mode=app_server` for steer-required rounds.

App Server server-request handling:
- approval-style server requests return deterministic decision values:
  - `deny_all` -> `decline`
  - `allow_safe_subset` -> `accept` only for read-only command subset, otherwise `decline`
- `allow_safe_subset` currently allows: `pwd`, `ls`, `cat`, `echo`, `rg`, `find`, `git status`, `git diff`, `git show`; any command containing shell control/metacharacter tokens (for example `&&`, `||`, `;`, `|`, `&`, redirection, command substitution, newlines) is denied.
- unsupported server request methods return deterministic JSON-RPC error `code=-32601`.
- `delegate_fail_open` returns deterministic disabled-policy error `code=-32050`.

### `delivery_status`
- Returns current runtime state with normalized machine-first envelope.
- Required input: `task_id`.
- Optional: `tail` for log tail length.
- Structured stall diagnostics fields are always present (nullable when not applicable):
  - `last_output_at`
  - `stall_duration_ms`
  - `stall_reason`
- `stall_reason` enum values:
  - `no_output_freshness`
  - `session_or_pane_missing` (compatibility label; now means runtime session missing/unreachable)
  - `runner_state_stale_or_inconsistent`
  - `unknown`

### `delivery_steer`
- Sends corrective instruction to a running task.
- Required input: `task_id`, `instruction`.

### `delivery_stop`
- Stops an active task and transitions runtime state accordingly.
- Required input: `task_id`.
- Optional: `reason`.

### `delivery_cleanup`
- Removes lifecycle records and runtime residue.
- Supports per-task cleanup and scoped retention prune.
- Optional controls:
  - `force`
  - `retention_mode`: `purge` or `retain`
  - `scope_id`
  - `states`
  - `updated_before_epoch_ms`

### `delivery_events_*`
- `delivery_events_subscribe`: create or reconnect subscriber cursor.
- `delivery_events_next`: fetch next event page after cursor.
- `delivery_events_ack`: acknowledge cursor for at-least-once delivery.

### `delivery_report`
- Returns aggregated runtime summary and task rows.
- Supports filters: `scope_id`, `states`, `updated_since_epoch_ms`, pagination with `limit` and `cursor`.
- Each task row includes diagnostics summary fields:
  - `last_output_at`
  - `stall_duration_ms`
  - `stall_reason`

## Typical MCP Flows

### Single delivery round (high level)
1. `delivery_submit`
2. `delivery_events_subscribe`
3. Repeated `delivery_events_next` + `delivery_events_ack`
4. Repeated `delivery_status`
5. Optional `delivery_steer` for correction
6. Optional `delivery_stop` if stuck
7. `delivery_report`
8. `delivery_cleanup`

### Plan control flow
1. `plan_next`
2. `plan_claim`
3. Work execution
4. `plan_complete`
5. `plan_status`

### Chat-driven DAG flow (recommended)
1. Orchestrator calls `plan_status` to build current DAG snapshot.
2. Orchestrator calls `plan_next` to get a ready node.
3. Worker claims via `plan_claim`.
4. Worker executes and reports evidence via `plan_complete`.
5. Orchestrator refreshes `plan_status` and repeats until terminal.

## Runner Capability Notes
- `AppServer` runner: long-lived steerable sessions over stdio JSON-RPC.
- `ProcessRunner`: deterministic one-shot process lifecycle; steer semantics are limited.

## Known Gaps and Boundaries
- High-level pause/resume/retry tools are not currently exposed in the public MCP descriptor.
- Retry behavior is currently expressed via submit/stop/state transitions, retry budget logic, and idempotency fields.

## Validation Commands
Run these after MCP tool-surface changes:

```bash
scripts/revive-sccache.sh
cargo fmt --all
cargo check -p agentrail-mcp
cargo test -p agentrail-mcp --test protocol --test tools
```

## Change Control
When adding/removing/modifying tools:
- Update descriptor in `crates/agentrail-mcp/src/lib.rs`.
- Update protocol tests for schema and discovery.
- Update tools tests for success and rejection paths.
- Update this catalog in the same PR.
