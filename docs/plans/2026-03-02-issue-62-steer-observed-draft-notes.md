# Issue #62 Draft Notes: tmux steer observed but no execution

## Context
- Issue: #62
- Branch: `issue-62-tmux-steer-observed-no-execution`
- Goal of current draft: add observability around steer lifecycle and detect likely "echo-only" stalls.

## Current Draft Changes (WIP)
1. Extend steer lifecycle metadata in `orchestrate_status`/`delivery_status`:
- `last_steer_sent_at`
- `last_steer_observed_at`
- `last_steer_applied_at`
- `last_steer_stalled_at`
- `last_steer_apply_hint`

2. Add steer progression signals in MCP runtime:
- `steer_sent`
- `steer_observed`
- `steer_applied` (logs changed after observed echo)
- `steer_stalled` (observed echo but no log progress over threshold)

3. Add tests covering:
- observed-only idempotency
- applied signal when logs change
- stalled signal when logs remain unchanged
- normalized status schema extension

## Why This Is Draft-Only
Subagent review found strategy risks that should be addressed before merge:
- Potential state inconsistency when stalled tasks move to `needs_attention` and later runner status changes.
- Stall/applied semantics can be client poll-shape sensitive.
- Same steer may appear as both stalled and applied without a strict monotonic sub-state model.

## Proposed Next Iteration (pending your better plan)
- Keep steer diagnosis as explicit sub-state machine scoped to steer epoch.
- Separate observability events from task runtime state mutation.
- Use deterministic time-window based stall criteria (not poll-count only).
- Define explicit recovery transition semantics after stall.

## Validation Run Snapshot (current draft)
- `cargo fmt --all --check`
- `CARGO_TARGET_DIR=/tmp/agentrail-target cargo test -p agentrail-mcp --test protocol --test tools`
- `CARGO_TARGET_DIR=/tmp/agentrail-target cargo check -p agentrail-mcp`
