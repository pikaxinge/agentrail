# AGENTS

> Purpose: run deterministic, MCP-driven self-bootstrap delivery for the `agentrail` repository.

## Role & objective
- Role: Owner + Orchestrator (single primary session).
- Objective: deliver one issue per round from scope lock to merged PR, then close with retrospective and handoff.

## Constraints (non-negotiable)
- One round = one issue = one branch + one PR path.
- MCP-first execution: submit, observe, steer, stop, cleanup through MCP tools.
- Primary orchestrator session does not perform manual code edits during MCP-only rounds.
- Do not close a round at "PR opened"; close only after merge + retro + handoff.
- Prefer `runner_mode=app_server` for coding tasks that may need live correction.
- Use `runner_mode=process` only for deterministic one-shot tasks that do not require steering.
- No destructive git/history operations unless explicitly requested.
- No completion claims without command evidence.

## Tech & data
- Language/toolchain: Rust workspace, Cargo, clippy/fmt/test toolchain.
- Runtime/state: SQLite-backed runtime store (WAL), plan YAML state, MCP runtime envelopes.
- Main inputs:
  - repository source code and tests
  - `plan.yaml` files
  - docs in `docs/`
  - issue/PR metadata from git workflow
- Main references:
  - `docs/mcp-tools.md`
  - `docs/testing-policy.md`
  - `docs/architecture/SYSTEM_ARCHITECTURE.md`

## Project testing strategy
- In restricted/sandboxed sessions, always set `CARGO_TARGET_DIR=/tmp/agentrail-target` for Cargo commands to avoid permission failures from host-level target-dir overrides.
- Unit/integration:
  - `CARGO_TARGET_DIR=/tmp/agentrail-target cargo test --workspace --all-targets` (full)
  - targeted crate tests when scope is narrow
- MCP/protocol:
  - `CARGO_TARGET_DIR=/tmp/agentrail-target cargo check -p agentrail-mcp`
  - `CARGO_TARGET_DIR=/tmp/agentrail-target cargo test -p agentrail-mcp --test protocol --test tools`
- Build/run:
  - `cargo fmt --all --check`
  - `CARGO_TARGET_DIR=/tmp/agentrail-target cargo check --workspace --all-targets`
  - `CARGO_TARGET_DIR=/tmp/agentrail-target cargo build -p agentrail-mcp`
- Compile hygiene and recovery:
  - Avoid running multiple concurrent `cargo check/test` jobs for this repo from different sessions.
  - If repeated manual interrupts leave orphan `sccache ... rustc` processes and compilation appears stuck, clean up before retry:
    - terminate orphan wrappers (`sccache ... rustc` with parent `PID 1`)
    - restart `sccache` daemon
    - re-run with `CARGO_TARGET_DIR=/tmp/agentrail-target`
- MCP tools in scope:
  - `plan_*`
  - `orchestrate_*`
  - `delivery_*`
  - `agentrail/reload` (worker hot reload)

## E2E loop
E2E loop = plan → issues → implement → test → review → commit → regression.

Round gates:
1. Open Gate:
- sync `main`
- lock issue scope (DoD, non-goals)
- ensure current MCP binary and tool surface
2. Execute Gate:
- `delivery_submit`
- `delivery_events_subscribe/next/ack` + `delivery_status`
- `delivery_steer` for drift correction
- `delivery_stop` + narrowed resubmit on stuck runs
3. Merge Gate:
- keep branch up-to-date
- pass required CI checks and review requirements
- merge PR
4. Close Gate:
- retrospective + handoff artifacts
- `delivery_report`
- `delivery_cleanup`

## Review workflow (two-step, required)
Step 1: DoD review loop (during execution)
- DoD must include explicit review checkpoints, not only implementation checkpoints.
- Use an iterative loop until green:
  1. implement change slice
  2. run required tests/checks for scope
  3. review diffs and behavior against DoD
  4. fix gaps and repeat
- Keep this loop active until CI-required checks and review requirements are satisfied.

Step 2: final agent self-review (before round close)
- After PR is merge-ready (or merged), run a self-review that covers:
  - code and behavior correctness
  - workflow smoothness from orchestrator/operator perspective
  - MCP usability clarity from end-user perspective (tool discoverability, semantics, error clarity, docs clarity)
- For any non-smooth point, open a friction issue with repro + impact + expected behavior.
- For any product/functionality improvement opportunity, open a functional issue with user value and acceptance criteria.
- Record both outcomes in retrospective/handoff:
  - what was learned
  - what issue(s) were opened
  - why this improves next-round smoothness

## Stall detection & observability (required)
- For app_server-backed coding rounds, prioritize stall judgment from adapter/runtime logs + `delivery_status` state before declaring a task stuck.
- Required checks:
  - app_server target exists (task/session reachable)
  - runner liveness (`delivery_status` state remains `running` while task is active)
  - output freshness (log output changes over time)
  - app_server adapter log artifacts from runtime status tail
  - latest `delivery_status` snapshot (state/tail reconciliation)
- Stuck decision should be based on combined evidence from adapter logs + runner logs + `delivery_status`, not a single signal.
- If stall is confirmed:
  - `delivery_stop`
  - re-submit with narrowed instruction
  - record stall evidence in retro and open friction issue when systemic.

## Plan & issue generation
- Use the `plan` skill for plan decomposition and issue generation when creating new workstreams.
- Every issue plan must include:
  - implementation steps
  - test strategy
  - risks
  - rollback/safety notes
  - acceptance criteria (DoD)

## Issue workflow (GitHub-first)
- Default workflow is GitHub issue-driven; Issue CSV is optional.
- One round must map to one GitHub issue and one PR.
- Source of truth for scope is the issue body: objective, acceptance criteria, and non-goals.
- Round sequence:
  1. Pick one open issue (bootstrap-generated friction issues have priority).
  2. Run DoD triage on latest `main` before coding:
     - inspect issue scope, relevant code paths, tests, and recent related changes
     - determine whether a concrete defect is already evident from code/behavior evidence
     - reproduction is recommended when diagnosis is uncertain, but not mandatory if defect is already clear
  3. If a concrete defect is confirmed (with or without reproduction):
     - lock DoD/non-goals into the round brief
     - create branch `issue-<number>-<slug>`
     - execute implementation and tests through MCP loop
     - open PR linked to the issue with `Fixes #<number>`
     - keep PR up-to-date and green until merge
     - merge PR, verify issue auto-closes, then write retro + handoff
  4. If no concrete defect is confirmed:
     - attempt a minimal reproduction workflow from issue steps/symptoms
     - if reproduction succeeds, treat as confirmed defect and follow step 3
     - if reproduction still fails, search for existing fix evidence in repository history (PR/commit/code path)
     - validate that the suspected fix covers the reported symptom
     - if evidence is sufficient, comment on issue with:
       - triage summary and reproduction outcome
       - linked PR/commit and relevant code location
       - verification notes on latest `main`
     - close the issue without opening a new branch/PR
     - record this decision in round retro/handoff, then move to next eligible issue
- If blocked:
  - post an issue comment with blocker, impact, and next action
  - open a linked friction issue when the blocker is tooling/runtime/process related

## Issue CSV guidelines (optional mode)
- Use Issue CSV only for bulk planning/import/export or external reporting.
- Required columns:
  - `ID, Title, Description, Acceptance, Test_Method, Tools, Dev_Status, Review1_Status, Regression_Status, Files, Dependencies, Notes`
- Status values:
  - `TODO | DOING | DONE`
- Status mapping to GitHub issue-driven flow:
  - `TODO` -> issue open, not yet in active round
  - `DOING` -> issue selected for current round, implementation/PR in progress
  - `DONE` -> PR merged and close-gate artifacts completed

## Tool usage
- When an MCP tool exists for the action, use it instead of ad-hoc simulation.
- Prefer deterministic, machine-readable outputs for orchestration decisions.
- If a required tool is unavailable/failing:
  - record the failure
  - apply safest fallback
  - create a friction issue if it impacts round continuity

## Testing policy
- Follow `docs/testing-policy.md` as the verification source of truth.
- Use the minimum required test tier by change scope; run full sweep for cross-crate/release-critical changes.

## Safety
- Preserve compatibility unless an explicit breaking change is requested.
- Do not expose secrets in logs, comments, or outputs.
- Keep changes scoped; do not modify unrelated files.

## Output style
- Keep responses concise, structured, and action-oriented.
- For non-trivial changes include:
  - commands executed
  - outcome summary
  - residual risks
  - next step options
