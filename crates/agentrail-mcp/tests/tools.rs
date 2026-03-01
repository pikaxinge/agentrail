use std::{
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentrail_mcp::handle_tool_call;
use serde_json::json;
use tempfile::tempdir;

fn unique_task_id(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic for test")
        .as_nanos();
    format!("{prefix}-{now}")
}

fn wait_for_runtime_state(task_id: &str, wanted: &str) -> serde_json::Value {
    let mut last_state = None;
    let mut last_status = None;
    let mut last_status_with_session = None;
    for _ in 0..40 {
        let status = handle_tool_call(
            "orchestrate_status",
            json!({
                "task_id": task_id
            }),
        )
        .expect("status should succeed");
        if status["runtime_state"] == wanted {
            return status;
        }
        if status.get("session_id").is_some() {
            last_status_with_session = Some(status.clone());
        }
        last_state = Some(status["runtime_state"].to_string());
        last_status = Some(status);
        thread::sleep(Duration::from_millis(50));
    }

    panic!(
        "timed out waiting for runtime_state={wanted}, last_state={}, last_status={}, last_status_with_session={}",
        last_state.unwrap_or_else(|| "<none>".to_string()),
        last_status.unwrap_or_else(|| json!({"status":"none"})),
        last_status_with_session.unwrap_or_else(|| json!({"status":"none"}))
    );
}

#[test]
fn orchestrate_start_starts_real_process_and_exposes_session() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-start");

    let result = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "echo mcp-orchestrate-ok"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "orchestrate_start");
    assert_eq!(result["status"], "accepted");
    assert!(result["session_id"].is_string());
}

#[test]
fn orchestrate_status_returns_runtime_backed_state() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-status");

    let _ = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 0.2"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("start should succeed");

    let result = handle_tool_call(
        "orchestrate_status",
        json!({
            "task_id": task_id
        }),
    )
    .expect("status should succeed");

    assert_eq!(result["tool"], "orchestrate_status");
    assert!(result["state"].is_string());
    assert!(result["runtime_state"].is_string());
}

#[test]
fn orchestrate_start_rejects_active_task_without_mutating_worker_assignment() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-active-worker");

    let _ = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 1"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial start should succeed");

    let err = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-b",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("second start while running must fail");
    assert!(err.to_string().contains("already active"));

    let status = handle_tool_call(
        "orchestrate_status",
        json!({
            "task_id": task_id
        }),
    )
    .expect("status should succeed");
    assert_eq!(status["assigned_worker"], "worker-a");
}

#[test]
fn orchestrate_start_rejects_restart_when_runtime_state_is_not_restartable() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-invalid-restart");

    let _ = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("start should succeed");

    let _status = wait_for_runtime_state(&task_id, "ready_to_merge");

    let err = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-b",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("restart from ready_to_merge must fail");

    assert!(err.to_string().contains("invalid runtime state"));
}

#[test]
fn orchestrate_terminal_session_is_unregistered_for_steer() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-terminal-cleanup");

    let _ = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("start should succeed");

    let _status = wait_for_runtime_state(&task_id, "ready_to_merge");

    let err = handle_tool_call(
        "orchestrate_steer",
        json!({
            "task_id": task_id,
            "instruction": "follow up"
        }),
    )
    .expect_err("steer for completed session should fail with no active session");
    assert!(err.to_string().contains("no active session"));
}

#[test]
fn orchestrate_steer_returns_error_for_process_runner() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-steer");

    let _ = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 1"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("start should succeed");

    let err = handle_tool_call(
        "orchestrate_steer",
        json!({
            "task_id": task_id,
            "instruction": "focus api layer"
        }),
    )
    .expect_err("process runner steer should be unsupported");

    assert!(err.to_string().contains("unsupported"));
}

#[test]
fn delivery_submit_status_and_report_are_available() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "echo delivery-submit-ok"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    assert_eq!(submit["tool"], "delivery_submit");
    assert_eq!(submit["task_id"], submit["orchestration"]["task_id"]);
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let status = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": submit["task_id"]
        }),
    )
    .expect("delivery_status should succeed");
    assert_eq!(status["tool"], "delivery_status");
    assert!(status["state"].is_string());
    assert!(status["runtime_state"].is_string());

    let report = handle_tool_call("delivery_report", json!({})).expect("report should succeed");
    assert_eq!(report["tool"], "delivery_report");
    assert!(report["summary"]["total"].is_number());
    assert!(report["tasks"].is_array());
}

#[test]
fn unknown_tool_is_rejected() {
    let err = handle_tool_call("unknown_tool", json!({})).expect_err("tool must fail");
    assert!(err.to_string().contains("unsupported tool"));
}

#[test]
fn orchestrate_start_requires_task_id() {
    let err = handle_tool_call("orchestrate_start", json!({ "worker_id": "worker-a" }))
        .expect_err("missing task_id should fail");
    assert!(err.to_string().contains("task_id"));
}

#[test]
fn orchestrate_status_requires_task_id() {
    let err =
        handle_tool_call("orchestrate_status", json!({})).expect_err("missing task_id should fail");
    assert!(err.to_string().contains("task_id"));
}

#[test]
fn orchestrate_steer_requires_task_id() {
    let err = handle_tool_call("orchestrate_steer", json!({ "instruction": "focus api" }))
        .expect_err("missing task_id should fail");
    assert!(err.to_string().contains("task_id"));
}
