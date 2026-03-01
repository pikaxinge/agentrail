# Agentrail M9 Dashboard/Report Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver a lightweight visualization/reporting layer so chat-first users can inspect progress, dependencies, blockers, and risks without opening code or raw plan files.

**Architecture:** Keep rendering serverless and deterministic: parse plan/runtime snapshots, compute summary view-models in Rust, render static HTML + Mermaid + Markdown report, and expose through CLI commands with stable machine-readable output.

**Tech Stack:** Rust workspace crates (`agentrail-core`, `agentrail-store`, `agentrail-dashboard`, `agentrail-cli`), serde/serde_json, static HTML templates.

---

### Task 1: Freeze M9 output contracts with failing tests

**Files:**
- Create: `crates/agentrail-dashboard/tests/render.rs`
- Create: `crates/agentrail-cli/tests/dashboard_cli.rs`
- Create: `crates/agentrail-compat-tests/fixtures/dashboard_summary.json`
- Create: `crates/agentrail-compat-tests/fixtures/dag_mermaid.txt`
- Create: `crates/agentrail-compat-tests/fixtures/report_markdown.md`
- Modify: `crates/agentrail-compat-tests/tests/fixtures_contract.rs`

**Step 1: Add failing dashboard renderer contract tests**

**Step 2: Add failing CLI contract tests for `dashboard`, `dag`, `report`**

**Step 3: Add compatibility fixtures and fixture assertions**

**Step 4: Verify RED**

```bash
cargo test -p agentrail-dashboard --test render
cargo test -p agentrail-cli --test dashboard_cli
cargo test -p agentrail-compat-tests --test fixtures_contract
```

### Task 2: Implement dashboard view-model and renderers

**Files:**
- Modify: `crates/agentrail-dashboard/src/lib.rs`
- Modify: `crates/agentrail-dashboard/Cargo.toml`

**Step 1: Implement summary model aggregation**
- totals: total/running/completed/failed/needs_attention
- blockers: steps blocked by unsatisfied deps
- risks: non-terminal failure states

**Step 2: Implement Mermaid DAG renderer**

**Step 3: Implement Markdown report renderer**

**Step 4: Re-run dashboard tests**

```bash
cargo test -p agentrail-dashboard --test render
```

### Task 3: Wire M9 commands in CLI

**Files:**
- Modify: `crates/agentrail-cli/src/main.rs`
- Modify: `crates/agentrail-cli/Cargo.toml`
- Modify: `crates/agentrail-cli/tests/dashboard_cli.rs`

**Step 1: Add commands**
- `dashboard --plan <path> [--out <path>]`
- `dag --plan <path>`
- `report --plan <path>`

**Step 2: Integrate dashboard crate renderers**

**Step 3: Return machine-readable summary JSON for dashboard command**

**Step 4: Re-run CLI tests**

```bash
cargo test -p agentrail-cli --test dashboard_cli
```

### Task 4: End-to-end contract and docs update

**Files:**
- Modify: `README.md`
- Modify: `docs/requirements/PRODUCT_REQUIREMENTS.md`
- Modify: `docs/architecture/SYSTEM_ARCHITECTURE.md`

**Step 1: Update docs for M9 capabilities and command examples**

**Step 2: Run verification gate**

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

**Step 3: Commit**

```bash
git add .
git commit -m "feat(dashboard): deliver M9 dashboard, dag, and markdown reporting"
```
