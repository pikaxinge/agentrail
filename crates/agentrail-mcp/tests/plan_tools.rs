use std::{
    fs,
    path::{Path, PathBuf},
};

use agentrail_mcp::{handle_tool_call, handle_tool_call_with_allowed_root};
use serde_json::json;
use tempfile::tempdir;

fn write_plan_yaml(yaml: &str) -> (tempfile::TempDir, PathBuf) {
    let tmp = tempdir().expect("tempdir");
    let plan_path = tmp.path().join("plan.yaml");
    fs::write(&plan_path, yaml).expect("write plan fixture");
    (tmp, plan_path)
}

fn write_plan_fixture() -> (tempfile::TempDir, PathBuf) {
    write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: pending
    steps:
      - id: step-a
        name: Step A
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
      - id: step-b
        name: Step B
        status: pending
        depends_on: [step-a]
        claimed_by: null
        evidence: null
"#,
    )
}

fn run_tool_in_root(
    tool_name: &str,
    args: serde_json::Value,
    allowed_root: &Path,
) -> anyhow::Result<serde_json::Value> {
    handle_tool_call_with_allowed_root(tool_name, args, Some(allowed_root))
}

#[test]
fn plan_status_tool_returns_machine_readable_summary() {
    let (tmp, plan) = write_plan_fixture();
    let result = run_tool_in_root(
        "plan_status",
        json!({
            "plan_path": plan.display().to_string()
        }),
        tmp.path(),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_status");
    assert_eq!(result["operation"], "status");
    assert_eq!(result["project"], "demo");
    assert_eq!(result["phase_count"], 1);
    assert_eq!(result["step_counts"]["pending"], 2);
}

#[test]
fn plan_show_tool_returns_requested_step() {
    let (tmp, plan) = write_plan_fixture();
    let result = run_tool_in_root(
        "plan_show",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-a"
        }),
        tmp.path(),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_show");
    assert_eq!(result["operation"], "show");
    assert_eq!(result["step"]["id"], "step-a");
}

#[test]
fn plan_next_tool_returns_next_ready_step() {
    let (tmp, plan) = write_plan_fixture();
    let result = run_tool_in_root(
        "plan_next",
        json!({
            "plan_path": plan.display().to_string()
        }),
        tmp.path(),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_next");
    assert_eq!(result["operation"], "next");
    assert_eq!(result["step"]["id"], "step-a");
}

#[test]
fn plan_next_recomputes_stale_locked_phase_before_selecting_step() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: done
    steps:
      - id: step-a
        name: Step A
        status: done
        depends_on: []
        claimed_by: null
        evidence: tests:ok
  - id: phase-2
    name: Phase 2
    status: locked
    depends_on: [phase-1]
    steps:
      - id: step-b
        name: Step B
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
"#,
    );
    let result = handle_tool_call(
        "plan_next",
        json!({
            "plan_path": plan.display().to_string()
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_next");
    assert_eq!(result["operation"], "next");
    assert_eq!(result["step"]["id"], "step-b");
}

#[test]
fn plan_claim_tool_updates_step_owner() {
    let (tmp, plan) = write_plan_fixture();
    let result = run_tool_in_root(
        "plan_claim",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-a",
            "agent": "worker-1"
        }),
        tmp.path(),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_claim");
    assert_eq!(result["operation"], "claim");
    assert_eq!(result["step"]["id"], "step-a");
    assert_eq!(result["step"]["status"], "claimed");
    assert_eq!(result["step"]["claimed_by"], "worker-1");
}

#[test]
fn plan_claim_recomputes_stale_locked_phase_before_dependency_checks() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: done
    steps:
      - id: step-a
        name: Step A
        status: done
        depends_on: []
        claimed_by: null
        evidence: tests:ok
  - id: phase-2
    name: Phase 2
    status: locked
    depends_on: [phase-1]
    steps:
      - id: step-b
        name: Step B
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
"#,
    );
    let result = handle_tool_call(
        "plan_claim",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-b",
            "agent": "worker-1"
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_claim");
    assert_eq!(result["operation"], "claim");
    assert_eq!(result["step"]["id"], "step-b");
    assert_eq!(result["step"]["status"], "claimed");
    assert_eq!(result["step"]["claimed_by"], "worker-1");
}

#[test]
fn plan_complete_tool_updates_step_evidence() {
    let (tmp, plan) = write_plan_fixture();
    let plan_path = plan.display().to_string();

    let _ = run_tool_in_root(
        "plan_claim",
        json!({
            "plan_path": plan_path,
            "step_id": "step-a",
            "agent": "worker-1"
        }),
        tmp.path(),
    )
    .expect("claim tool should succeed");

    let result = run_tool_in_root(
        "plan_complete",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-a",
            "agent": "worker-1",
            "evidence": "tests:ok"
        }),
        tmp.path(),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_complete");
    assert_eq!(result["operation"], "complete");
    assert_eq!(result["step"]["id"], "step-a");
    assert_eq!(result["step"]["status"], "done");
    assert_eq!(result["step"]["evidence"], "tests:ok");
}

#[test]
fn plan_complete_recomputes_stale_locked_phase_before_dependency_checks() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: done
    steps:
      - id: step-a
        name: Step A
        status: done
        depends_on: []
        claimed_by: null
        evidence: tests:ok
  - id: phase-2
    name: Phase 2
    status: locked
    depends_on: [phase-1]
    steps:
      - id: step-b
        name: Step B
        status: claimed
        depends_on: []
        claimed_by: worker-1
        evidence: null
"#,
    );
    let result = handle_tool_call(
        "plan_complete",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-b",
            "agent": "worker-1",
            "evidence": "tests:ok"
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "plan_complete");
    assert_eq!(result["operation"], "complete");
    assert_eq!(result["step"]["id"], "step-b");
    assert_eq!(result["step"]["status"], "done");
    assert_eq!(result["step"]["evidence"], "tests:ok");
}

#[test]
fn plan_complete_requires_agent() {
    let (tmp, plan) = write_plan_fixture();
    let plan_path = plan.display().to_string();

    let _ = run_tool_in_root(
        "plan_claim",
        json!({
            "plan_path": plan_path,
            "step_id": "step-a",
            "agent": "worker-1"
        }),
        tmp.path(),
    )
    .expect("claim tool should succeed");

    let err = run_tool_in_root(
        "plan_complete",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-a",
            "evidence": "tests:ok"
        }),
        tmp.path(),
    )
    .expect_err("missing agent should fail");
    assert!(err.to_string().contains("missing agent"));
}

#[test]
fn plan_complete_rejects_owner_mismatch() {
    let (tmp, plan) = write_plan_fixture();
    let plan_path = plan.display().to_string();

    let _ = run_tool_in_root(
        "plan_claim",
        json!({
            "plan_path": plan_path,
            "step_id": "step-a",
            "agent": "worker-1"
        }),
        tmp.path(),
    )
    .expect("claim tool should succeed");

    let err = run_tool_in_root(
        "plan_complete",
        json!({
            "plan_path": plan.display().to_string(),
            "step_id": "step-a",
            "agent": "worker-2",
            "evidence": "tests:ok"
        }),
        tmp.path(),
    )
    .expect_err("owner mismatch should fail");
    assert!(err.to_string().contains("claimed_by"));
}

#[test]
fn plan_tools_require_plan_path() {
    let err = handle_tool_call("plan_status", json!({})).expect_err("missing plan_path");
    assert!(err.to_string().contains("plan_path"));
}

#[test]
fn plan_tools_reject_path_outside_allowed_root() {
    let allowed_root = tempdir().expect("allowed root");
    let (_tmp, plan) = write_plan_fixture();
    let result = handle_tool_call_with_allowed_root(
        "plan_status",
        json!({
            "plan_path": plan.display().to_string()
        }),
        Some(allowed_root.path()),
    );

    let err = result.expect_err("out of root plan path should fail");
    assert!(err.to_string().contains("outside allowed root"));
}
