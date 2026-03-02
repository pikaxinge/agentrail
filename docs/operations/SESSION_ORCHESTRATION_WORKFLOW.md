# Session Orchestration Workflow

This runbook defines how a session-level orchestrator executes development tasks in this repository.

## Scope
- Use this flow for feature delivery, bugfix batches, and cross-module refactors.
- Use it with isolated worktrees and branch-protected PR merging.
- For self-improving orchestration, pair this runbook with `BOOTSTRAP_LOOP.md`.

## Workflow
1. Confirm target, scope boundaries, and acceptance criteria.
2. Inspect current baseline: open PRs, failing tests, impacted modules.
3. Decompose into a DAG: independent nodes, dependencies, and critical path.
4. Create one isolated worktree + branch per independent node.
5. Define test gates for each node before implementation starts.
6. Dispatch workers in parallel for independent nodes.
7. Require each worker to run local verification and produce focused commits.
8. Integrate worker branches into one integration branch.
9. Resolve semantic overlaps and conflicts in integration.
10. Run spec-compliance review, then code-quality review.
11. Send review findings back to workers until no blocking findings remain.
12. Run CI-equivalent verification on integration branch:
    - `scripts/revive-sccache.sh`
    - `cargo fmt --all -- --check`
    - `cargo check --workspace --all-targets --locked`
    - `cargo test --workspace --all-targets --locked`
13. Push integration branch and open/update PR.
    - For title/body updates, use `scripts/gh-pr-edit-safe.sh` to avoid `gh pr edit` GraphQL `projectCards` deprecation failures.
    - Wrapper prerequisites: `gh` + `jq`, and a GitHub `remote.origin.url`; fallback only supports `--title`, `--body`, `--body-file`.
14. Keep PR branch strict-up-to-date with `main` and resolve review comments.
15. Merge only after branch protection requirements are satisfied.
16. Post-merge cleanup:
    - remove temporary worktrees
    - prune stale worktree metadata
    - clean large local build artifacts as needed (`cargo clean`)

## MCP-Only Mode
- For bootstrap hardening, execution should use MCP tool surfaces as the default control path.
- Non-MCP actions (`shell`/direct file edits/manual git surgery) are break-glass actions and must be logged as friction events.
- For autonomous Codex workers under `delivery_submit`, use `runner_mode=app_server`.
- `runner_mode=process` intentionally rejects `codex exec` / `codex-guarded-exec` command shapes.
- App Server server-request policy is explicit: `app_server_request_policy` defaults to `deny_all` and is only valid with `runner_mode=app_server`.
- `scripts/codex-guarded-exec.sh` remains a direct shell fallback helper for fail-fast no-mutation guarding.
- Required MCP surfaces for orchestration loop:
  - planning: `plan_next`, `plan_claim`, `plan_complete`, `plan_status`, `plan_show`
  - runtime: `orchestrate_start`, `orchestrate_status`, `orchestrate_steer`
  - delivery: `delivery_submit`, `delivery_status`, `delivery_steer`, `delivery_stop`, `delivery_cleanup`, `delivery_report`, `delivery_events_*`

## Branch Protection Alignment
- Do not force push.
- Do not bypass required checks.
- Do not merge without required approvals.
- Do not merge with unresolved review comments.

## Artifacts To Keep
- Integration branch commit history.
- PR validation evidence (`fmt/check/test` results).
- Reviewer findings and resolution notes.
