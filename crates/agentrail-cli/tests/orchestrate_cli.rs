use assert_cmd::Command;
use serde_json::Value;

#[test]
fn orchestrate_plan_outputs_machine_readable_json() {
    let mut cmd = Command::cargo_bin("agentrail").expect("binary exists");
    let output = cmd
        .args(["orchestrate", "plan", "--max-parallel", "4"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: Value = serde_json::from_slice(&output).expect("stdout must be valid json");
    assert_eq!(payload["operation"], "orchestrate_plan");
    assert_eq!(payload["max_parallel"], 4);
}

#[test]
fn orchestrate_tick_outputs_repair_action() {
    let mut cmd = Command::cargo_bin("agentrail").expect("binary exists");
    let output = cmd
        .args([
            "orchestrate",
            "tick",
            "--retry-count",
            "1",
            "--retry-budget",
            "3",
            "--reassigned-once",
            "false",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: Value = serde_json::from_slice(&output).expect("stdout must be valid json");
    assert_eq!(payload["operation"], "orchestrate_tick");
    assert_eq!(payload["repair_action"], "wake_original_agent");
}

#[test]
fn orchestrate_resume_outputs_task_id() {
    let mut cmd = Command::cargo_bin("agentrail").expect("binary exists");
    let output = cmd
        .args(["orchestrate", "resume", "--task-id", "task-42"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let payload: Value = serde_json::from_slice(&output).expect("stdout must be valid json");
    assert_eq!(payload["operation"], "orchestrate_resume");
    assert_eq!(payload["task_id"], "task-42");
}
