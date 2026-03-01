# Agentrail M1-M5 Auto-Delivery Implementation Plan

> For execution in `feat/auto-m1-m5` worktree.

## Batch 1: Contract and planning baseline
1. Commit pending governance docs (`AGENTS.md`, M1 plan).
2. Add auto-delivery design document and this implementation plan.
3. Update PRD and architecture docs with auto-delivery requirements.

Verification:
- `cargo fmt --all`
- `cargo check --workspace`
- `cargo test -p agentrail-compat-tests`

## Batch 2: Add orchestration application crate
1. Create `crates/agentrail-orchestrator`.
2. Add domain DTOs for task graph, execution state, gate reports.
3. Add pure DAG scheduler with ready-node selection.
4. Add gate evaluator logic for required checks.
5. Add unit tests for DAG and gate behavior.

Verification:
- `cargo test -p agentrail-orchestrator`

## Batch 3: Integrate store and runner adapters
1. Extend `agentrail-store` for runtime task records and transitions.
2. Add orchestration service wiring store + runner + scheduler.
3. Define wake-up retry policy for review failures.
4. Add tests for retry and reassignment behavior.

Verification:
- `cargo test -p agentrail-store`
- `cargo test -p agentrail-orchestrator`

## Batch 4: CLI/MCP surface for orchestrator
1. Add CLI commands: `orchestrate plan`, `orchestrate tick`, `orchestrate resume`.
2. Add MCP tools: `orchestrate_start`, `orchestrate_status`, `orchestrate_steer`.
3. Ensure output contracts are stable and machine-readable.
4. Add compatibility fixtures for new control-path behavior.

Verification:
- `cargo test -p agentrail-cli`
- `cargo test -p agentrail-mcp`
- `cargo test -p agentrail-compat-tests`

## Batch 5: End-to-end hardening
1. Add structured tracing for task lifecycle and gate decisions.
2. Add benchmark scaffold for hot read path and scheduler throughput.
3. Add restart recovery test for in-flight task states.
4. Document operational runbook.

Verification:
- `cargo fmt --all`
- `cargo check --workspace`
- `cargo test --workspace`
