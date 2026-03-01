# Agentrail M1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver a usable M1 slice: read-focused control plane (`status/show/next/checkpoint`) with stable CLI + MCP entrypoints.

**Architecture:** Implement read path end-to-end through shared usecases so CLI and MCP consume one application flow. Keep domain logic in `agentrail-core`, persistence in `agentrail-plan-io`, and interfaces thin.

**Tech Stack:** Rust workspace, Clap, Tokio, Serde/Serde YAML, Tracing.

---

### Task 1: Add application-layer read usecases crate

**Files:**
- Create: `crates/agentrail-app/Cargo.toml`
- Create: `crates/agentrail-app/src/lib.rs`
- Create: `crates/agentrail-app/src/read.rs`
- Modify: `Cargo.toml`

**Step 1: Write failing tests**
- Add tests for `status`, `show`, and `checkpoint` output shape in `crates/agentrail-app/src/read.rs`.

**Step 2: Run tests to verify red state**
Run: `cargo test -p agentrail-app`
Expected: failing due to missing usecase implementations.

**Step 3: Implement minimal read usecases**
- Implement pure functions returning structured DTOs.

**Step 4: Run tests to verify green state**
Run: `cargo test -p agentrail-app`
Expected: PASS.

**Step 5: Commit**
`git commit -m "feat(app): add read path usecases for status/show/checkpoint"`

### Task 2: Extend plan IO with deterministic read helpers

**Files:**
- Modify: `crates/agentrail-plan-io/src/lib.rs`
- Create: `crates/agentrail-plan-io/tests/load_roundtrip.rs`

**Step 1: Write failing tests**
- Add roundtrip test for load/hash consistency.
- Add malformed YAML error-shape test.

**Step 2: Run tests to verify fail**
Run: `cargo test -p agentrail-plan-io`
Expected: at least one FAIL before implementation.

**Step 3: Implement minimal code**
- Add helper APIs needed by app read usecases.

**Step 4: Run tests to verify pass**
Run: `cargo test -p agentrail-plan-io`
Expected: PASS.

**Step 5: Commit**
`git commit -m "feat(plan-io): add deterministic read helpers and roundtrip tests"`

### Task 3: Wire read usecases into CLI

**Files:**
- Modify: `crates/agentrail-cli/Cargo.toml`
- Modify: `crates/agentrail-cli/src/main.rs`
- Create: `tests/integration/cli_read.rs`

**Step 1: Write failing integration tests**
- Add CLI tests for `status`, `show <id>`, `checkpoint`.

**Step 2: Run tests to verify fail**
Run: `cargo test --test cli_read`
Expected: FAIL with unsupported command output.

**Step 3: Implement minimal CLI plumbing**
- Add read subcommands and JSON/markdown output modes.

**Step 4: Run tests to verify pass**
Run: `cargo test --test cli_read`
Expected: PASS.

**Step 5: Commit**
`git commit -m "feat(cli): wire read usecases for status/show/checkpoint"`

### Task 4: Wire read usecases into MCP server

**Files:**
- Modify: `crates/agentrail-mcp/src/lib.rs`
- Modify: `crates/agentrail-mcp/src/main.rs`
- Create: `tests/mcp/read_tools.rs`

**Step 1: Write failing tests**
- Add MCP tests for `status`, `show`, `checkpoint` tool handlers.

**Step 2: Run tests to verify fail**
Run: `cargo test --test read_tools`
Expected: FAIL before wiring.

**Step 3: Implement minimal tool handlers**
- Map tool parameters to shared app usecases.

**Step 4: Run tests to verify pass**
Run: `cargo test --test read_tools`
Expected: PASS.

**Step 5: Commit**
`git commit -m "feat(mcp): add read tool handlers using shared usecases"`

### Task 5: Add compatibility fixtures scaffold

**Files:**
- Create: `crates/agentrail-compat-tests/fixtures/README.md`
- Modify: `crates/agentrail-compat-tests/tests/smoke.rs`

**Step 1: Write failing fixture loader test**
- Add fixture load test that asserts required fixture files exist.

**Step 2: Run tests to verify fail**
Run: `cargo test -p agentrail-compat-tests`
Expected: FAIL without fixtures.

**Step 3: Add minimal fixtures and loader**
- Create initial fixture contract docs.

**Step 4: Run tests to verify pass**
Run: `cargo test -p agentrail-compat-tests`
Expected: PASS.

**Step 5: Commit**
`git commit -m "test(compat): scaffold fixture contracts for differential testing"`

### Task 6: Final verification and checkpoint commit

**Files:**
- Modify: `docs/architecture/SYSTEM_ARCHITECTURE.md`
- Modify: `docs/requirements/PRODUCT_REQUIREMENTS.md`

**Step 1: Sync docs with implemented M1 scope**
- Update command/tool coverage notes.

**Step 2: Run full verification**
Run:
- `cargo fmt --all`
- `cargo check --workspace`
- `cargo test --workspace`

Expected: all PASS.

**Step 3: Commit**
`git commit -m "docs: update architecture and requirements for M1 read path"`
