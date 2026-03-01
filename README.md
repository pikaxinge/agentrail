# agentrail

High-performance Rust control plane for agentic execution.

## Scope
- Plan state machine and DAG scheduling
- MCP server for orchestration layers
- CLI for local and CI operations
- Agent runner abstraction (process/tmux)
- Static dashboard rendering

## Quick start
```bash
cargo check --workspace
cargo run -p agentrail-cli -- --help
```

## Repository layout
- `docs/requirements`: product requirements
- `docs/architecture`: architecture docs
- `docs/plans`: design and implementation plans
- `crates/*`: Rust workspace crates
