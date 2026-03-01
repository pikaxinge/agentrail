# Agentrail Foundation Design

> Status: approved for repository bootstrap.

## Design goals
- New project identity (`agentrail`) with clean Rust-first architecture.
- Compatibility-friendly module boundaries for migration from legacy control plane.
- Immediate developer onboarding via scaffolded docs and workspace layout.

## High-level decisions
- Monorepo Rust workspace with explicit crate responsibilities.
- Separate domain model crate from IO, runner, and transport crates.
- Keep plan storage and task storage decoupled.
- Use interface-level compatibility testing crate from day one.

## Deliverables in this bootstrap
- PRD and architecture docs.
- Workspace and crate skeleton.
- Compilable baseline with minimal placeholder implementations.
