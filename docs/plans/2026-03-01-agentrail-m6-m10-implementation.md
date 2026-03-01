# Agentrail M6-M10 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver full user-facing control plane capability (CLI + MCP + runner + storage + dashboard) so chat-driven orchestration can run real end-to-end delivery loops locally.

**Architecture:** Expand contract-first from interface layer inward: add failing CLI/MCP contract tests, implement app/domain behavior, then connect runtime runner and persistent store, and finally close with dashboard + perf validation. Keep all write paths CAS-safe and all runtime transitions validated by tests.

**Tech Stack:** Rust workspace (`clap`, `serde/serde_json/serde_yaml`, `tokio`, `tracing`, `sha2`, `tempfile`), git worktree, tmux runner integration, sqlite WAL target for runtime store.

---

## Execution DAG

1. M6.1 CLI read/write command surface (depends on existing core + plan-io).
2. M6.2 MCP tool surface and transport loop (depends on M6.1 app usecases).
3. M7 Runner execution adapters (depends on M6.2 tool contracts).
4. M8 Runtime persistence (depends on M7 transition semantics).
5. M9 Dashboard + reporting (depends on M6-M8 data contracts).
6. M10 Differential compatibility + performance proof (depends on all previous milestones).

## Global verification gate per task

Run from workspace root:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

When the task scope is narrow, run targeted tests first and run full workspace gate at batch checkpoint.

### Task 1: Freeze M6 contract tests for CLI read/write commands

**Files:**
- Modify: `crates/agentrail-cli/tests/orchestrate_cli.rs`
- Create: `crates/agentrail-cli/tests/plan_cli_read_write.rs`
- Modify: `crates/agentrail-compat-tests/tests/fixtures_contract.rs`
- Create: `crates/agentrail-compat-tests/fixtures/cli_status.json`
- Create: `crates/agentrail-compat-tests/fixtures/cli_show.json`
- Create: `crates/agentrail-compat-tests/fixtures/cli_next.json`
- Create: `crates/agentrail-compat-tests/fixtures/cli_claim.json`
- Create: `crates/agentrail-compat-tests/fixtures/cli_complete.json`

**Step 1: Write failing CLI tests for `status/show/next/claim/complete`**

**Step 2: Run targeted tests to confirm red**

```bash
cargo test -p agentrail-cli --test plan_cli_read_write
```

**Step 3: Add compatibility fixtures for expected output shapes**

**Step 4: Validate fixtures contract test**

```bash
cargo test -p agentrail-compat-tests --test fixtures_contract
```

**Step 5: Commit**

```bash
git add crates/agentrail-cli/tests crates/agentrail-compat-tests
git commit -m "test(cli): freeze M6 read-write command contracts"
```

### Task 2: Implement shared read/write plan usecases

**Files:**
- Create: `crates/agentrail-app/src/plan.rs`
- Modify: `crates/agentrail-app/src/lib.rs`
- Modify: `crates/agentrail-core/src/models.rs`
- Modify: `crates/agentrail-core/src/state.rs`
- Modify: `crates/agentrail-plan-io/src/lib.rs`
- Test: `crates/agentrail-app/tests/plan_usecases.rs`

**Step 1: Add failing app tests for read/write behaviors**

**Step 2: Run targeted tests to verify failure**

```bash
cargo test -p agentrail-app --test plan_usecases
```

**Step 3: Implement minimal read/write usecases with CAS-safe save**

**Step 4: Re-run targeted tests**

```bash
cargo test -p agentrail-app --test plan_usecases
```

**Step 5: Commit**

```bash
git add crates/agentrail-app crates/agentrail-core crates/agentrail-plan-io
git commit -m "feat(app): add plan read-write usecases for M6 command surface"
```

### Task 3: Wire M6 command surface in CLI

**Files:**
- Modify: `crates/agentrail-cli/src/main.rs`
- Modify: `crates/agentrail-cli/tests/plan_cli_read_write.rs`
- Modify: `README.md`

**Step 1: Add command parsing and JSON output wiring**

**Step 2: Run targeted CLI tests**

```bash
cargo test -p agentrail-cli --test plan_cli_read_write
```

**Step 3: Add README examples for new commands**

**Step 4: Commit**

```bash
git add crates/agentrail-cli/src/main.rs crates/agentrail-cli/tests/plan_cli_read_write.rs README.md
git commit -m "feat(cli): expose M6 status-show-next-claim-complete commands"
```

### Task 4: Expand MCP tools and stdio loop

**Files:**
- Modify: `crates/agentrail-mcp/src/lib.rs`
- Modify: `crates/agentrail-mcp/src/main.rs`
- Create: `crates/agentrail-mcp/tests/plan_tools.rs`

**Step 1: Write failing tool tests for read/write operations**

**Step 2: Run targeted MCP tests**

```bash
cargo test -p agentrail-mcp --test plan_tools
```

**Step 3: Implement tool handlers and transport dispatch loop**

**Step 4: Re-run MCP tests**

```bash
cargo test -p agentrail-mcp --test plan_tools
```

**Step 5: Commit**

```bash
git add crates/agentrail-mcp
git commit -m "feat(mcp): add M6 plan tools and dispatch loop"
```

### Task 5: Implement real ProcessRunner execution path

**Files:**
- Modify: `crates/agentrail-runner/src/lib.rs`
- Create: `crates/agentrail-runner/tests/process_runner.rs`

**Step 1: Add failing tests for start/status/log lifecycle**

**Step 2: Run targeted tests**

```bash
cargo test -p agentrail-runner --test process_runner
```

**Step 3: Implement process spawn, status query, tail logs**

**Step 4: Re-run targeted tests**

```bash
cargo test -p agentrail-runner --test process_runner
```

**Step 5: Commit**

```bash
git add crates/agentrail-runner
git commit -m "feat(runner): implement process runner lifecycle"
```

### Task 6: Implement TmuxRunner steering actions

**Files:**
- Modify: `crates/agentrail-runner/src/lib.rs`
- Create: `crates/agentrail-runner/tests/tmux_runner.rs`

**Step 1: Add failing tests for steer/pause/resume/stop/logs**

**Step 2: Run targeted tests**

```bash
cargo test -p agentrail-runner --test tmux_runner
```

**Step 3: Implement tmux command adapters**

**Step 4: Re-run targeted tests**

```bash
cargo test -p agentrail-runner --test tmux_runner
```

**Step 5: Commit**

```bash
git add crates/agentrail-runner
git commit -m "feat(runner): add tmux steering control path"
```

### Task 7: Move runtime store to SQLite WAL

**Files:**
- Modify: `crates/agentrail-store/src/lib.rs`
- Create: `crates/agentrail-store/tests/sqlite_runtime.rs`
- Modify: `crates/agentrail-store/Cargo.toml`

**Step 1: Add failing tests for persistence and restart recovery**

**Step 2: Run targeted tests**

```bash
cargo test -p agentrail-store --test sqlite_runtime
```

**Step 3: Implement sqlite-backed transactional store**

**Step 4: Re-run store tests**

```bash
cargo test -p agentrail-store
```

**Step 5: Commit**

```bash
git add crates/agentrail-store
git commit -m "feat(store): add sqlite wal runtime persistence"
```

### Task 8: Build lightweight HTML dashboard + Mermaid DAG

**Files:**
- Modify: `crates/agentrail-dashboard/src/lib.rs`
- Create: `crates/agentrail-dashboard/tests/render.rs`
- Modify: `crates/agentrail-cli/src/main.rs`

**Step 1: Add failing render tests for summary + dag section**

**Step 2: Run targeted tests**

```bash
cargo test -p agentrail-dashboard --test render
```

**Step 3: Implement HTML renderer and CLI export wiring**

**Step 4: Re-run dashboard tests**

```bash
cargo test -p agentrail-dashboard
```

**Step 5: Commit**

```bash
git add crates/agentrail-dashboard crates/agentrail-cli/src/main.rs
git commit -m "feat(dashboard): add lightweight html and mermaid rendering"
```

### Task 9: Differential compatibility harness

**Files:**
- Modify: `crates/agentrail-compat-tests/src/lib.rs`
- Modify: `crates/agentrail-compat-tests/tests/fixtures_contract.rs`
- Modify: `scripts/diff-python.sh`

**Step 1: Add failing differential tests across CLI/MCP outputs**

**Step 2: Run targeted compatibility tests**

```bash
cargo test -p agentrail-compat-tests
```

**Step 3: Implement harness helpers and fixture loaders**

**Step 4: Re-run compatibility tests**

```bash
cargo test -p agentrail-compat-tests
```

**Step 5: Commit**

```bash
git add crates/agentrail-compat-tests scripts/diff-python.sh
git commit -m "test(compat): implement differential harness for control plane contracts"
```

### Task 10: Performance and operations closeout

**Files:**
- Modify: `benches/scheduler_throughput.rs`
- Modify: `docs/operations/AUTO_DELIVERY_RUNBOOK.md`
- Modify: `README.md`

**Step 1: Add benchmark matrix and expected thresholds**

**Step 2: Run benchmark**

```bash
cargo bench -p agentrail-orchestrator --bench scheduler_throughput
```

**Step 3: Update runbook and README with measurable outcomes**

**Step 4: Final verification gate**

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

**Step 5: Commit**

```bash
git add benches docs/operations/AUTO_DELIVERY_RUNBOOK.md README.md
git commit -m "chore: finalize M6-M10 performance and operations documentation"
```
