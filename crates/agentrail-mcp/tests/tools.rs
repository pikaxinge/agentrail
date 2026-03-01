use agentrail_mcp::handle_tool_call;
use serde_json::json;

#[test]
fn orchestrate_start_tool_returns_machine_readable_payload() {
    let result = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": "task-1",
            "worker_id": "worker-a"
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "orchestrate_start");
    assert_eq!(result["task_id"], "task-1");
    assert_eq!(result["status"], "accepted");
}

#[test]
fn orchestrate_status_tool_returns_state_payload() {
    let result = handle_tool_call(
        "orchestrate_status",
        json!({
            "task_id": "task-2"
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "orchestrate_status");
    assert_eq!(result["task_id"], "task-2");
    assert_eq!(result["state"], "running");
}

#[test]
fn orchestrate_steer_tool_returns_ack_payload() {
    let result = handle_tool_call(
        "orchestrate_steer",
        json!({
            "task_id": "task-3",
            "instruction": "focus api layer"
        }),
    )
    .expect("tool should succeed");

    assert_eq!(result["tool"], "orchestrate_steer");
    assert_eq!(result["task_id"], "task-3");
    assert_eq!(result["status"], "sent");
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
