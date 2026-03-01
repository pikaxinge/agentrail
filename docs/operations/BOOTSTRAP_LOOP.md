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
1. Receive task and acceptance criteria.
2. Execute orchestration run using MCP tools first.
3. Validate result (`fmt`, `check`, `test`, PR gates).
4. Run post-run reflection:
   - what blocked smooth execution
   - which steps required break-glass actions
   - where MCP surface was missing or insufficient
5. Open friction issues from reflection findings.
6. Prioritize and implement friction issues through the same orchestration flow.
7. Repeat until bootstrap thresholds are met.

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
