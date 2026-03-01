# Agentrail Bootstrap Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a new `agentrail` Rust repository with requirements docs, architecture docs, and a compilable multi-crate skeleton.

**Architecture:** Workspace-based architecture with clear separation between domain, persistence, runtime control, interfaces, and compatibility tests.

**Tech Stack:** Rust workspace, Clap, Tokio, Serde, Tracing.

---

### Task 1: Initialize repository and top-level workspace files
- Create root directories and workspace Cargo manifest.
- Add toolchain, gitignore, and README.
- Verify workspace metadata loads.

### Task 2: Add product and architecture documentation
- Write PRD in `docs/requirements/PRODUCT_REQUIREMENTS.md`.
- Write architecture in `docs/architecture/SYSTEM_ARCHITECTURE.md`.
- Add foundation design artifact under `docs/plans/`.

### Task 3: Scaffold domain and IO crates
- Add `agentrail-core` model/state/error modules.
- Add `agentrail-plan-io` load/save + CAS interfaces.
- Ensure library boundaries compile.

### Task 4: Scaffold runtime crates
- Add `agentrail-store` placeholders for transactional task state.
- Add `agentrail-runner` trait and process/tmux skeleton implementations.
- Add `agentrail-dashboard` placeholder renderer.

### Task 5: Scaffold interface crates
- Add `agentrail-mcp` runtime entrypoints.
- Add `agentrail-cli` command entrypoint and subcommands.
- Add `agentrail-compat-tests` harness placeholder.

### Task 6: Verify workspace health
- Run formatting.
- Run workspace check.
- Fix compile issues until clean.
