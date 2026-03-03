# Testing Policy

This document defines required verification levels for `agentrail` changes.

## Goals
- Keep branch-protected CI green.
- Require reproducible local verification before merge claims.
- Scale test scope by change risk while preserving minimum safety floors.

## CI Source of Truth
Required pull-request checks are defined in `.github/workflows/ci.yml`:
- `fmt`
- `check`
- `test`

Equivalent local commands:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
```

## Local Preflight
Before compile/test commands, run:

```bash
scripts/revive-sccache.sh
```

If `sccache` is unavailable, continue with normal cargo execution.
The guard resumes paused `sccache` processes, cleans orphan `sccache ... rustc`
wrappers left by interrupted runs, and restarts the daemon when unresponsive.

## Minimum Verification Matrix

### Docs-only changes
Minimum:
- Markdown sanity check by rendering/reading changed docs.
- No code test run required.

If docs modify executable commands or tool contracts, run at least targeted checks for affected area.

### MCP surface or protocol changes
Minimum:

```bash
scripts/revive-sccache.sh
cargo fmt --all
cargo check -p agentrail-mcp
cargo test -p agentrail-mcp --test protocol --test tools
```

### Runner/store/runtime state changes
Minimum:

```bash
scripts/revive-sccache.sh
cargo fmt --all
cargo check --workspace
cargo test -p agentrail-runner
cargo test -p agentrail-store
cargo test -p agentrail-mcp --test tools
```

### Cross-crate behavior changes or release-critical work
Required full sweep:

```bash
scripts/revive-sccache.sh
cargo fmt --all --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
```

## PR and Merge Rules
- Do not claim completion until required local verification has run.
- Do not merge if required CI checks are pending or failing.
- Keep branch strict-up-to-date with `main` when branch protection requires it.

## Evidence Requirements
Each non-trivial PR should include:
- Commands executed.
- Pass/fail result summary.
- If scope was reduced, reason and residual risk.

## Flaky or Infra Failures
If a failure is non-deterministic or environmental:
- Re-run once to confirm flake vs deterministic failure.
- If still failing, capture logs and open/attach a friction issue.
- Do not silently bypass failing checks.

## MCP and Self-Bootstrap Specific Guidance
For MCP-driven rounds, keep verification aligned with close-gate expectations:
- Runtime snapshot via `delivery_report`.
- Cleanup via `delivery_cleanup`.
- Ensure docs and issue artifacts for any friction encountered.

### Opt-in Real CLI E2E Harness (AR-109)
`agentrail-mcp` includes ignored-by-default E2E tests that validate real local App Server
control flow (`delivery_submit -> delivery_status -> delivery_steer -> delivery_stop/retry`)
against a local CLI binary.

Enable explicitly when you have a local CLI binary available:

```bash
AGENTRAIL_E2E_REAL_CLI=1 \
AGENTRAIL_E2E_APP_SERVER_COMMAND=codex \
AGENTRAIL_E2E_APP_SERVER_ARGS_JSON='["app-server"]' \
CARGO_TARGET_DIR=/tmp/agentrail-target \
cargo test -p agentrail-mcp --test tools -- --ignored delivery_submit_app_server_real_cli_e2e
```

Optional environment variables:
- `AGENTRAIL_E2E_ARTIFACT_DIR`: output directory for captured artifacts.
- `AGENTRAIL_E2E_STATUS_TAIL`: `delivery_status` log tail size (default `200`).
- `AGENTRAIL_E2E_STEER_INSTRUCTION`: steering probe text used in happy-path test.

Artifacts are written as JSON files per task and phase:
- `*-status.json`
- `*-timeline.json`
- `*-explain-failure.json`
- `*-summary.json`

## Policy Changes
Any update to this policy must be done with:
- Clear rationale.
- Updated command examples.
- Alignment with `.github/workflows/ci.yml` and branch protection expectations.
