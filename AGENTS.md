# AGENTS.md

This document defines how coding agents should operate in `agentrail`.

## Project mission
- Build a Rust-native orchestration control plane for chat-driven multi-agent execution.
- Preserve behavior-compatibility level 2 with legacy CLI/MCP semantics.
- Prioritize correctness and observability over speculative optimization.

## Architecture boundaries
- `crates/agentrail-core`: domain model, state machine, DAG and validation rules.
- `crates/agentrail-plan-io`: plan.yaml parsing, hashing, CAS checks, atomic writes.
- `crates/agentrail-store`: transactional runtime state (target: SQLite WAL).
- `crates/agentrail-runner`: runner abstraction and adapters (`ProcessRunner`, `TmuxRunner`).
- `crates/agentrail-mcp`: MCP transport and tool interface.
- `crates/agentrail-cli`: user-facing CLI and compatibility command surface.
- `crates/agentrail-dashboard`: static HTML and Mermaid-based visualization rendering.
- `crates/agentrail-compat-tests`: compatibility/differential harness.

## Engineering principles
- Do the simplest thing that is correct.
- Keep domain logic pure; IO and transport live outside core.
- Maintain deterministic behavior for status, scheduling, and checkpoints.
- Any write path touching plans must keep CAS semantics.

## Workflow for each task
1. Read relevant docs under `docs/requirements` and `docs/architecture`.
2. Add or update tests first for behavior changes.
3. Implement minimal code to satisfy tests.
4. Run required verification commands.
5. Commit focused changes with clear message.

For full session orchestration (DAG decomposition, parallel workers, review loop, CI gates, and cleanup), see:
- `docs/operations/SESSION_ORCHESTRATION_WORKFLOW.md`
- `docs/operations/BOOTSTRAP_LOOP.md`

## TMUX Round Protocol (Required for Self-Bootstrap)
Use this protocol when an orchestrator controls codex/agents through tmux sessions.

### Control model
- `agentrail` MCP tools are the default control plane.
- `tmux` is runtime transport for long tasks and steering, not a replacement for `agentrail`.
- Any non-MCP action (`shell` hotfix/manual git surgery/direct code patching) is a break-glass event and must be logged as friction.

### One round = one closed loop
Each tmux session should run one full round and then exit.

1. Input + Scope:
- load task goal, acceptance criteria, and constraints.

2. Plan:
- decompose into DAG (parallel nodes + dependencies).

3. Execute:
- dispatch workers, run review loops, converge to passing implementation.

4. Validate:
- run CI-equivalent checks (`fmt`, `check`, `test`, locked mode when relevant).

5. Integrate:
- open PR, satisfy review/CI gates, merge.

6. Reflect:
- write retrospective for this round (what blocked smooth execution).

7. Friction logging:
- open at least one friction issue when there was any non-smooth step.

8. Handoff:
- write next-round handoff state (remaining tasks, new issues, expected MCP version).

9. Exit session:
- end this tmux round after close gate passes.

### Hard gates
Round close gate (must pass before tmux session exit):
- PR state is `MERGED`.
- Retrospective file exists and is non-empty.
- Friction issue(s) created when friction occurred.
- Next-round handoff file exists.

Round open gate (must pass before next tmux round starts):
- target binary build succeeded.
- MCP version/probe matches expected revision.
- required MCP tool surface is available:
  - `plan_*`
  - `orchestrate_*`
  - `delivery_*`

### Autonomous worker guard (required)
- For non-interactive coding rounds launched through MCP, use `scripts/codex-guarded-exec.sh`.
- The guard must enforce a no-edit timeout and fail fast when the worker loops in read/search without mutating the workspace.
- Do not run raw `codex exec` directly for autonomous delivery rounds unless explicitly debugging guard behavior.

### Smoothness targets
- `non_mcp_actions = 0` (target).
- high CI first-pass rate.
- low review rework loops.
- no unresolved P0/P1 friction issues.

The loop is not complete at merge. It is complete only after: merge -> retrospective -> friction issue creation -> handoff.

## Required verification before commit
Run all of the following from repository root:
- `scripts/revive-sccache.sh`
- `cargo fmt --all`
- `cargo check --workspace`
- `cargo test --workspace`

If a change is scoped and running full tests is expensive, run targeted tests plus explain why broader coverage was skipped.

## Compatibility policy
- Avoid changing external command names/argument semantics without explicit migration notes.
- Prefer additive evolution over breaking behavior.
- Keep MCP result payloads stable unless versioned.

## Runner policy
- `ProcessRunner` is the baseline for deterministic one-shot tasks.
- `TmuxRunner` is for long-lived sessions requiring steering/log replay/attach.
- Steering support depends on target agent capabilities; fallback to stop-and-restart when unsupported.

## Documentation policy
- Update requirements/architecture docs for any behavior or boundary changes.
- Keep implementation plans in `docs/plans/` with date-prefixed filenames.

## Commit conventions
- `feat:` new functionality
- `fix:` bug fixes
- `refactor:` internal restructuring with no behavior change
- `chore:` tooling/scaffolding/maintenance
- `docs:` documentation only changes

## Safety constraints
- Do not delete unrelated files.
- Do not rewrite git history unless explicitly requested.
- Do not claim completion without fresh command evidence.
