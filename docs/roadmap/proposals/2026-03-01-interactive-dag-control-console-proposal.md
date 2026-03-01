# Proposal: Interactive DAG Control Console (Low Priority)

Date: 2026-03-01
Status: Proposed (deferred)
Priority: P4 (lowest)
Owner: Product + Control Plane

## Summary

Add an interactive HTML DAG console for runtime orchestration.
The console is a visual control surface, not a replacement for MCP.
Users can inspect dependency state, progress, and node-level detail, then trigger control commands from the graph UI.

This proposal is intentionally deferred and does not change current delivery priorities.

## Why

Current chat-first and CLI-first flows work, but complex task graphs are harder to inspect and steer quickly.
A graph-native UI improves:

- situational awareness (dependencies, blockers, critical path)
- operator speed (jump from graph node to command action)
- stakeholder communication (shareable visual progress)

## Scope (proposed)

### In

- Interactive DAG home view:
  - pan/zoom
  - dynamic node layout
  - node badge: status + short progress
- Hover details:
  - step name
  - current state
  - brief description
  - claimed_by/worker
- Node click detail view:
  - full description
  - dependencies and dependents
  - latest evidence/review context
- Command panel on both DAG and detail view:
  - start/steer/pause/resume/stop/retry
  - explicit confirmation for destructive actions
- Event feedback:
  - command accepted/rejected
  - latest runtime transition
  - simple timeline snippet

### Out

- New source-of-truth model (state remains MCP/store-backed)
- Replacing chat interface
- Multi-tenant auth redesign

## Architecture Fit

- UI is a thin client over existing MCP `delivery_*` and orchestration APIs.
- State remains in runtime store; UI never mutates state directly.
- DAG data and node summaries are read models derived from existing plan/runtime state.

## UX Requirements

- Fast first render for medium graphs (100-300 nodes target)
- Visual grouping by phase
- Clear status color legend
- Keyboard-accessible command controls
- Mobile-safe read mode (control actions can be disabled on small screens)

## Data Contract (draft)

- `dag_snapshot`:
  - nodes: `{id, name, status, phase, progress, summary}`
  - edges: `{from, to, type}`
  - generated_at
- `node_detail`:
  - metadata + dependency lists + recent events
- `command_result`:
  - `{ok, task_id, action, runtime_state, message, ts}`

## Risks

- UI complexity can distract from control-plane reliability goals
- Graph rendering performance for large plans
- Command safety and accidental clicks

## Mitigations

- Keep it P4 and gated behind feature flag
- Start with read-only DAG mode, then add control commands
- Require confirm dialogs and idempotency keys for write actions

## Acceptance Criteria (when scheduled)

- Read-only DAG render from live runtime snapshot
- Node hover + detail drawer
- At least 3 command actions end-to-end through MCP
- Command audit events visible in UI
- No regression in existing chat/CLI workflows

## Suggested Delivery Sequence (future)

1. Read-only DAG snapshot endpoint + basic UI
2. Node detail + event timeline pane
3. Command bar for safe actions (`steer/stop/retry`)
4. Hardening: permissions, rate-limit, UX polish

## Current Decision

- Keep as backlog item at P4.
- Do not allocate active milestone capacity until runtime control-loop hardening is complete.
