# Bootstrap Loop (MCP-Only)

This document defines the self-improving loop for session orchestration.

## Goal
- Reach a stable state where task delivery is smooth with minimal friction.
- Detect orchestration friction automatically and convert it into actionable improvement work.

## Core Rule
- Default mode is `MCP-only`.
- Any non-MCP control action counts as manual intervention, even if executed by the orchestrator agent.

## Definitions
- `MCP action`: operation executed through `plan_*`, `orchestrate_*`, or `delivery_*` tools.
- `manual intervention`: any direct `shell`, direct code edit, direct git operation, or ad-hoc workaround not executed via MCP tools.
- `break-glass`: a manual intervention allowed only to unblock delivery. Must generate a friction issue.

## Loop
1. Receive one issue and acceptance criteria.
2. Run DoD triage on latest `main` before coding:
   - inspect issue scope, relevant code paths, tests, and related recent changes.
   - if defect is already clear from code/behavior evidence, proceed directly.
   - if diagnosis is uncertain, run a minimal reproduction workflow.
3. Execute orchestration run using MCP tools first.
   - if defect is confirmed, run implementation -> test -> PR flow.
   - if defect is not confirmed after triage/repro, search for existing fix evidence (PR/commit/code path), comment issue with evidence, close issue, and record decision.
4. Validate result (`fmt`, `check`, `test`, PR gates).
   - run `scripts/revive-sccache.sh` before compile/test steps to recover paused or orphaned wrapper processes.
5. Run post-run reflection:
   - what blocked smooth execution
   - which steps required break-glass actions
   - where MCP surface was missing or insufficient
6. Open friction issues from reflection findings.
7. Open functional-improvement issues when user-facing workflow or MCP usability can be smoother.
8. Prioritize and implement friction issues through the same orchestration flow.
9. Repeat until bootstrap thresholds are met.

## Two-Step Review Contract (required)
1. DoD review loop during execution:
   - implement a small slice
   - run required checks
   - review against DoD
   - fix and repeat until CI/review requirements are green
2. Final agent self-review before close:
   - verify code and behavior correctness
   - review orchestration smoothness
   - review MCP user clarity (tool discoverability, semantics, error clarity, docs clarity)
   - produce friction/functional issues when needed and record them in retro/handoff

## Metrics
- `non_mcp_actions`: count of non-MCP actions in one run. Target: `0`.
- `mcp_coverage`: `mcp_actions / total_actions`. Target: `>= 0.95` short term, `1.00` long term.
- `ci_first_pass_rate`: fraction of runs that pass CI without rerun. Target: `>= 0.90`.
- `review_rework_rounds`: average review fix loops per PR. Target: `<= 1.0`.
- `human_blocker_events`: count of owner-only blocking interventions. Target: `0`.

## Stop Condition (Smooth State)
All conditions should hold for at least 5 consecutive tasks:
- `non_mcp_actions = 0`
- CI required checks pass on first run
- no blocking review findings after final integration pass
- no unresolved P0/P1 friction issues

## Required Artifacts Per Run
- run summary with metrics
- friction list (if any)
- linked issues for each friction item
- closure note describing what improved in the next run

## Guarded Worker Contract
To avoid autonomous no-action loops, bootstrap rounds must use a guarded codex launcher.

Recommended invocation inside `delivery_submit`:

```bash
scripts/codex-guarded-exec.sh \
  --workdir /path/to/repo \
  --timeout-sec 300 \
  -- \
  --dangerously-bypass-approvals-and-sandbox \
  -C /path/to/repo \
  "your implementation prompt"
```

Behavior:
- If the worker fails to mutate the workspace before timeout, the guard exits with code `124`.
- This creates a deterministic failure signal so orchestrator can stop/retry instead of waiting indefinitely.

## Stall Detection & Evidence Chain
For tmux-backed coding rounds, do not declare stall from a single signal.

Required evidence set:
- tmux target exists (session/window/pane reachable)
- pane liveness (`pane_dead`)
- current command (`pane_current_command`)
- output freshness (pane output changes over time)
- runner log artifacts (for example `.agentrail-tmux-logs` when configured)
- latest `delivery_status` snapshot

If stall is confirmed by combined evidence:
1. `delivery_stop`
2. re-submit with narrowed instruction
3. record stall evidence in run summary and open friction issue when systemic
