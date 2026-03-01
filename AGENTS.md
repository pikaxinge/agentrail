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

## Required verification before commit
Run all of the following from repository root:
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
