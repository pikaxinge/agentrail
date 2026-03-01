# Session Orchestration Workflow

This runbook defines how a session-level orchestrator executes development tasks in this repository.

## Scope
- Use this flow for feature delivery, bugfix batches, and cross-module refactors.
- Use it with isolated worktrees and branch-protected PR merging.

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
    - `cargo fmt --all -- --check`
    - `cargo check --workspace --all-targets --locked`
    - `cargo test --workspace --all-targets --locked`
13. Push integration branch and open/update PR.
14. Keep PR branch strict-up-to-date with `main` and resolve review comments.
15. Merge only after branch protection requirements are satisfied.
16. Post-merge cleanup:
    - remove temporary worktrees
    - prune stale worktree metadata
    - clean large local build artifacts as needed (`cargo clean`)

## Branch Protection Alignment
- Do not force push.
- Do not bypass required checks.
- Do not merge without required approvals.
- Do not merge with unresolved review comments.

## Artifacts To Keep
- Integration branch commit history.
- PR validation evidence (`fmt/check/test` results).
- Reviewer findings and resolution notes.
