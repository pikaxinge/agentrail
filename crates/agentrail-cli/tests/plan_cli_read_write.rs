use std::{fs, path::PathBuf};

use assert_cmd::Command;
use serde_json::Value;
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

fn run_cli_json(args: &[&str]) -> Value {
    let mut cmd = Command::cargo_bin("agentrail").expect("binary exists");
    let output = cmd
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    serde_json::from_slice(&output).expect("stdout must be valid json")
}

fn run_cli_failure(args: &[&str]) -> String {
    let mut cmd = Command::cargo_bin("agentrail").expect("binary exists");
    let output = cmd
        .args(args)
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();
    String::from_utf8(output).expect("stderr must be utf8")
}

#[test]
fn status_outputs_machine_readable_summary() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&["status", "--plan", &plan_arg]);
    assert_eq!(payload["operation"], "status");
    assert_eq!(payload["project"], "demo");
    assert_eq!(payload["phase_count"], 1);
    assert_eq!(payload["step_counts"]["pending"], 2);
}

#[test]
fn show_outputs_step_details_for_requested_id() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&["show", "--plan", &plan_arg, "--step-id", "step-a"]);
    assert_eq!(payload["operation"], "show");
    assert_eq!(payload["step"]["id"], "step-a");
    assert_eq!(payload["step"]["status"], "pending");
    assert!(payload["step"]["depends_on"].is_array());
}

#[test]
fn next_outputs_next_ready_step() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&["next", "--plan", &plan_arg]);
    assert_eq!(payload["operation"], "next");
    assert_eq!(payload["step"]["id"], "step-a");
}

#[test]
fn next_skips_steps_in_locked_phases() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-locked
    name: Locked
    status: locked
    steps:
      - id: step-locked
        name: Locked Step
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
  - id: phase-open
    name: Open
    status: pending
    steps:
      - id: step-open
        name: Open Step
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
"#,
    );
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&["next", "--plan", &plan_arg]);
    assert_eq!(payload["operation"], "next");
    assert_eq!(payload["step"]["id"], "step-open");
}

#[test]
fn next_does_not_unlock_explicit_locked_phase_after_other_phase_completes() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-locked
    name: Locked
    status: locked
    steps:
      - id: step-locked
        name: Locked Step
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
  - id: phase-open
    name: Open
    status: pending
    steps:
      - id: step-open
        name: Open Step
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
"#,
    );
    let plan_arg = plan.display().to_string();

    run_cli_json(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-open",
        "--agent",
        "worker-1",
    ]);
    run_cli_json(&[
        "complete",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-open",
        "--evidence",
        "tests:ok",
    ]);

    let payload = run_cli_json(&["next", "--plan", &plan_arg]);
    assert_eq!(payload["operation"], "next");
    assert!(payload["step"].is_null());
}

#[test]
fn next_skips_steps_in_phase_with_unmet_phase_dependencies() {
    let (_tmp, plan) = write_plan_yaml(
        r#"
version: 1
project: demo
phases:
  - id: phase-2
    name: Phase 2
    status: pending
    depends_on: [phase-1]
    steps:
      - id: step-b
        name: Step B
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
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
"#,
    );
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&["next", "--plan", &plan_arg]);
    assert_eq!(payload["operation"], "next");
    assert_eq!(payload["step"]["id"], "step-a");
}

#[test]
fn claim_updates_step_with_agent() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--agent",
        "worker-1",
    ]);
    assert_eq!(payload["operation"], "claim");
    assert_eq!(payload["step"]["id"], "step-a");
    assert_eq!(payload["step"]["status"], "claimed");
    assert_eq!(payload["step"]["claimed_by"], "worker-1");
}

#[test]
fn claim_clears_existing_evidence() {
    let (_tmp, plan) = write_plan_yaml(
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
        evidence: old-evidence
"#,
    );
    let plan_arg = plan.display().to_string();

    let payload = run_cli_json(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--agent",
        "worker-1",
    ]);
    assert_eq!(payload["operation"], "claim");
    assert_eq!(payload["step"]["status"], "claimed");
    assert!(payload["step"]["evidence"].is_null());
}

#[test]
fn claim_rejects_when_step_not_pending() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    run_cli_json(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--agent",
        "worker-1",
    ]);

    let stderr = run_cli_failure(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--agent",
        "worker-2",
    ]);
    assert!(stderr.contains("invalid state transition"));
}

#[test]
fn claim_rejects_when_dependencies_not_ready() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let stderr = run_cli_failure(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-b",
        "--agent",
        "worker-1",
    ]);
    assert!(stderr.contains("dependencies not ready"));
}

#[test]
fn complete_updates_step_with_evidence() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    run_cli_json(&[
        "claim",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--agent",
        "worker-1",
    ]);

    let payload = run_cli_json(&[
        "complete",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--evidence",
        "tests:ok",
    ]);
    assert_eq!(payload["operation"], "complete");
    assert_eq!(payload["step"]["id"], "step-a");
    assert_eq!(payload["step"]["status"], "done");
    assert_eq!(payload["step"]["evidence"], "tests:ok");
}

#[test]
fn complete_rejects_when_step_not_claimed() {
    let (_tmp, plan) = write_plan_fixture();
    let plan_arg = plan.display().to_string();

    let stderr = run_cli_failure(&[
        "complete",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-a",
        "--evidence",
        "tests:ok",
    ]);
    assert!(stderr.contains("invalid state transition"));
}

#[test]
fn complete_rejects_when_dependencies_not_ready() {
    let (_tmp, plan) = write_plan_yaml(
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
        status: claimed
        depends_on: [step-a]
        claimed_by: worker-1
        evidence: null
"#,
    );
    let plan_arg = plan.display().to_string();

    let stderr = run_cli_failure(&[
        "complete",
        "--plan",
        &plan_arg,
        "--step-id",
        "step-b",
        "--evidence",
        "tests:ok",
    ]);
    assert!(stderr.contains("dependencies not ready"));
}
