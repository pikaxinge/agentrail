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

The M6 CLI plan command slice is now implemented:

- `status --plan <path>`
- `show --plan <path> --step-id <id>`
- `next --plan <path>`
- `claim --plan <path> --step-id <id> --agent <name>`
- `complete --plan <path> --step-id <id> --evidence <text>`

The M6 MCP plan tool slice is now implemented:

- `plan_status`
- `plan_show`
- `plan_next`
- `plan_claim`
- `plan_complete`

The M7 ProcessRunner slice is now implemented:

- real child process start/stop/status/logs lifecycle
- process-group and descendant cleanup on stop (unix path)
- bounded in-memory logs and bounded terminal-session retention

The M8 runtime store slice is now implemented:

- SQLite-backed `TaskStore` with file-persistence across reconnect
- WAL journal mode enforcement for file-backed stores
- transactional write paths and atomic snapshot import

The M9 dashboard/report CLI slice is now implemented:

- `dashboard --plan <path> --out <path>` (writes static HTML + JSON summary)
- `dag --plan <path>` (prints Mermaid DAG text)
- `report --plan <path>` (prints markdown report text)

The M11 runtime orchestration slice is now implemented:

- real MCP runtime tools: `orchestrate_start`, `orchestrate_status`, `orchestrate_steer`
- high-level MCP chat tools: `delivery_submit`, `delivery_status`, `delivery_steer`, `delivery_report`
- process-backed task execution via MCP with runtime store integration
- tmux runner supports start/steer/pause/resume/stop/status/logs (best effort)
- HTTP MCP transport (`/mcp`) and health endpoint (`/healthz`)
- `serve` CLI command for long-running MCP service mode

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

# 2.1) Plan operations (M6 slice)
cargo run -p agentrail-cli -- status --plan ./plan.yaml
cargo run -p agentrail-cli -- show --plan ./plan.yaml --step-id step-a
cargo run -p agentrail-cli -- next --plan ./plan.yaml
cargo run -p agentrail-cli -- claim --plan ./plan.yaml --step-id step-a --agent worker-1
cargo run -p agentrail-cli -- complete --plan ./plan.yaml --step-id step-a --evidence "tests:ok"

# 2.2) Dashboard/report outputs (M9 slice)
cargo run -p agentrail-cli -- dashboard --plan ./plan.yaml --out ./dashboard.html
cargo run -p agentrail-cli -- dag --plan ./plan.yaml
cargo run -p agentrail-cli -- report --plan ./plan.yaml

# 3) Run MCP server
cargo run -p agentrail-mcp -- --transport stdio
cargo run -p agentrail-mcp -- --transport http --bind 127.0.0.1:8787

# 4) Serve mode (same MCP runtime via CLI)
cargo run -p agentrail-cli -- serve --transport http --bind 127.0.0.1:8787
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
- Rolling release on every `main` push:
  - GitHub Release tag `main-latest` (pre-release, auto-updated by CD)

## Documentation

- Product requirements: `docs/requirements/PRODUCT_REQUIREMENTS.md`
- Architecture: `docs/architecture/SYSTEM_ARCHITECTURE.md`
- Roadmap issues: `docs/roadmap/ROADMAP_ISSUES.md`
- CI protection: `docs/operations/CI_PROTECTION.md`
- Design and implementation plans: `docs/plans/`
