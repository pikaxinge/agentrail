# agentrail

Rust-native control plane for chat-driven multi-agent software delivery.

`agentrail` sits between an orchestration layer and execution agents:

```text
Chat App -> Orchestrator -> agentrail (CLI/MCP) -> Core/Store/Runner -> Worktrees/Agents
```

## Current status

The M1-M5 foundation is implemented:

- DAG scheduling and gate evaluation
- launch service with runtime state transitions
- retry policy (`wake -> reassign -> escalate`)
- MCP tool handlers for orchestration control calls
- CLI orchestration commands with machine-readable JSON outputs
- runtime snapshot export/import for restart recovery
- tracing instrumentation for launch flow and MCP tool calls
- compatibility fixtures and contract tests
- scheduler benchmark scaffold and operations runbook

## Quick start

```bash
# 1) Verify workspace
cargo fmt --all
cargo check --workspace
cargo test --workspace

# 2) Explore CLI
cargo run -p agentrail-cli -- --help
cargo run -p agentrail-cli -- orchestrate plan --max-parallel 4
cargo run -p agentrail-cli -- orchestrate tick --retry-count 1 --retry-budget 3 --reassigned-once false
cargo run -p agentrail-cli -- orchestrate resume --task-id task-42

# 3) Run MCP server
cargo run -p agentrail-mcp -- --transport stdio
```

## Repository layout

- `crates/agentrail-core`: domain models and state semantics
- `crates/agentrail-plan-io`: YAML plan load/save with CAS semantics
- `crates/agentrail-store`: runtime task state, transition guards, snapshot recovery
- `crates/agentrail-runner`: runner abstraction (`ProcessRunner`, `TmuxRunner`)
- `crates/agentrail-orchestrator`: scheduler, gate evaluator, launch service, retry policy
- `crates/agentrail-mcp`: MCP transport and tool handlers
- `crates/agentrail-cli`: local/CI command interface
- `crates/agentrail-dashboard`: static dashboard rendering helpers
- `crates/agentrail-compat-tests`: compatibility fixtures and contract tests

## Performance and operations

- Scheduler throughput benchmark:
  - `cargo bench -p agentrail-orchestrator --bench scheduler_throughput`
- Operational runbook:
  - `docs/operations/AUTO_DELIVERY_RUNBOOK.md`

## Documentation

- Product requirements: `docs/requirements/PRODUCT_REQUIREMENTS.md`
- Architecture: `docs/architecture/SYSTEM_ARCHITECTURE.md`
- Design and implementation plans: `docs/plans/`
