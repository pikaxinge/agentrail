use std::fs;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::Value;
use tempfile::tempdir;

fn write_plan_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempdir().expect("tempdir");
    let plan_path = tmp.path().join("plan.yaml");
    let yaml = r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: in_progress
    steps:
      - id: step-a
        name: Step A
        status: claimed
        depends_on: []
        claimed_by: worker-a
        evidence: null
      - id: step-b
        name: Step B
        status: pending
        depends_on: [step-a]
        claimed_by: null
        evidence: null
"#;
    fs::write(&plan_path, yaml).expect("write plan");
    (tmp, plan_path)
}

#[test]
fn dashboard_writes_html_and_returns_summary_json() {
    let (_tmp, plan_path) = write_plan_fixture();
    let out_dir = tempdir().expect("tempdir");
    let out_path = out_dir.path().join("dashboard.html");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_agentrail"));
    let output = cmd
        .args([
            "dashboard",
            "--plan",
            &plan_path.display().to_string(),
            "--out",
            &out_path.display().to_string(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: Value = serde_json::from_slice(&output).expect("stdout should be json");
    assert_eq!(payload["operation"], "dashboard");
    assert_eq!(payload["project"], "demo");
    assert!(out_path.exists());
    let html = fs::read_to_string(out_path).expect("dashboard output should exist");
    assert!(html.contains("Agentrail Dashboard"));
}

#[test]
fn dag_outputs_mermaid_graph() {
    let (_tmp, plan_path) = write_plan_fixture();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_agentrail"));
    let output = cmd
        .args(["dag", "--plan", &plan_path.display().to_string()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let text = String::from_utf8(output).expect("stdout should be utf8");
    assert!(text.contains("graph TD"));
    assert!(text.contains("step-a"));
    assert!(text.contains("step-b"));
}

#[test]
fn report_outputs_markdown_summary() {
    let (_tmp, plan_path) = write_plan_fixture();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_agentrail"));
    let output = cmd
        .args(["report", "--plan", &plan_path.display().to_string()])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let text = String::from_utf8(output).expect("stdout should be utf8");
    assert!(text.contains("# Agentrail Report"));
    assert!(text.contains("Project: demo"));
    assert!(text.contains("Running: 1"));
}
