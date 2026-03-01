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

fn unique_subscriber_id(prefix: &str) -> String {
    unique_task_id(prefix)
}

fn subscribe_events(task_id: &str) -> (String, u64) {
    let subscriber_id = unique_subscriber_id("delivery-sub");
    let response = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": subscriber_id,
            "task_id": task_id
        }),
    )
    .expect("delivery_events_subscribe should succeed");

    (
        response["subscriber_id"]
            .as_str()
            .expect("subscriber_id should be string")
            .to_string(),
        response["cursor"]
            .as_u64()
            .expect("cursor should be u64 integer"),
    )
}

fn next_events(subscriber_id: &str, task_id: &str, limit: u32) -> serde_json::Value {
    handle_tool_call(
        "delivery_events_next",
        json!({
            "subscriber_id": subscriber_id,
            "task_id": task_id,
            "limit": limit
        }),
    )
    .expect("delivery_events_next should succeed")
}

fn ack_events(subscriber_id: &str, cursor: u64) {
    let ack = handle_tool_call(
        "delivery_events_ack",
        json!({
            "subscriber_id": subscriber_id,
            "cursor": cursor
        }),
    )
    .expect("delivery_events_ack should succeed");
    assert_eq!(ack["acked_cursor"], json!(cursor));
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
            "task_id": submit["task_id"],
            "tail": 5
        }),
    )
    .expect("delivery_status should succeed");
    assert_eq!(status["tool"], "delivery_status");
    assert!(status["state"].is_string());
    assert!(status["runtime_state"].is_string());
    assert!(status["orchestration"].is_object());

    let normalized = status["normalized"]
        .as_object()
        .expect("delivery_status normalized envelope should exist");
    let mut normalized_keys = normalized.keys().map(String::as_str).collect::<Vec<_>>();
    normalized_keys.sort_unstable();
    assert_eq!(
        normalized_keys,
        vec![
            "assigned_worker",
            "logs",
            "retry_budget",
            "retry_count",
            "runner_mode",
            "runtime_state",
            "session_id",
            "state",
            "task_id",
            "timestamps",
            "tool"
        ]
    );
    assert_eq!(status["normalized"]["tool"], "delivery_status");
    assert_eq!(status["normalized"]["task_id"], status["task_id"]);
    assert_eq!(status["normalized"]["state"], status["state"]);
    assert_eq!(
        status["normalized"]["runtime_state"],
        status["runtime_state"]
    );
    assert_eq!(
        status["normalized"]["runner_mode"],
        status["orchestration"]["runner_mode"]
    );
    assert_eq!(
        status["normalized"]["session_id"],
        status["orchestration"]["session_id"]
    );
    assert_eq!(
        status["normalized"]["assigned_worker"],
        status["orchestration"]["assigned_worker"]
    );
    assert_eq!(
        status["normalized"]["retry_count"],
        status["orchestration"]["retry_count"]
    );
    assert_eq!(
        status["normalized"]["retry_budget"],
        status["orchestration"]["retry_budget"]
    );
    assert!(status["normalized"]["timestamps"]["updated_at"].is_number());
    assert_eq!(status["normalized"]["logs"]["tail"], 5);
    assert!(status["normalized"]["logs"]["truncated"].is_boolean());

    let report = handle_tool_call("delivery_report", json!({})).expect("report should succeed");
    assert_eq!(report["tool"], "delivery_report");
    assert!(report["summary"]["total"].is_number());
    assert!(report["tasks"].is_array());
}

#[test]
fn delivery_status_normalized_envelope_uses_deterministic_nulls_when_runtime_data_missing() {
    let task_id = unique_task_id("task-delivery-missing");

    let status = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": task_id,
            "tail": 9
        }),
    )
    .expect("delivery_status should succeed for unknown task_id");

    assert_eq!(status["tool"], "delivery_status");
    assert_eq!(status["state"], "unknown");
    assert_eq!(status["runtime_state"], "unknown");
    assert!(status["orchestration"].is_object());

    assert_eq!(status["normalized"]["tool"], "delivery_status");
    assert_eq!(status["normalized"]["task_id"], status["task_id"]);
    assert_eq!(status["normalized"]["state"], "unknown");
    assert_eq!(status["normalized"]["runtime_state"], "unknown");
    assert!(status["normalized"]["runner_mode"].is_null());
    assert!(status["normalized"]["session_id"].is_null());
    assert!(status["normalized"]["assigned_worker"].is_null());
    assert!(status["normalized"]["retry_count"].is_null());
    assert!(status["normalized"]["retry_budget"].is_null());
    assert!(status["normalized"]["timestamps"]["updated_at"].is_number());
    assert_eq!(status["normalized"]["logs"]["tail"], 9);
    assert_eq!(status["normalized"]["logs"]["truncated"], false);
}

#[test]
fn delivery_events_emit_ordered_lifecycle_updates() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-events-ordered");
    let (subscriber_id, _) = subscribe_events(&task_id);

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 0.15"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    let mut collected = Vec::new();
    for _ in 0..80 {
        let _ = handle_tool_call(
            "delivery_status",
            json!({
                "task_id": task_id
            }),
        )
        .expect("delivery_status should succeed");

        let next = next_events(&subscriber_id, &task_id, 16);
        let events = next["events"].as_array().expect("events should be array");
        if !events.is_empty() {
            collected.extend(events.iter().cloned());
            let last_cursor = next["next_cursor"]
                .as_u64()
                .expect("next_cursor should be u64");
            ack_events(&subscriber_id, last_cursor);
        }

        if collected
            .iter()
            .any(|event| event["event_type"] == "completed")
        {
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    assert!(
        collected
            .iter()
            .any(|event| event["event_type"] == "submitted"),
        "expected submitted event, got: {collected:?}"
    );
    assert!(
        collected
            .iter()
            .any(|event| event["event_type"] == "running"),
        "expected running event, got: {collected:?}"
    );
    assert!(
        collected
            .iter()
            .any(|event| event["event_type"] == "completed"),
        "expected completed event, got: {collected:?}"
    );

    let submitted_idx = collected
        .iter()
        .position(|event| event["event_type"] == "submitted")
        .expect("submitted event should exist");
    let running_idx = collected
        .iter()
        .position(|event| event["event_type"] == "running")
        .expect("running event should exist");
    let completed_idx = collected
        .iter()
        .position(|event| event["event_type"] == "completed")
        .expect("completed event should exist");

    assert!(submitted_idx < running_idx);
    assert!(running_idx < completed_idx);

    let cursors = collected
        .iter()
        .map(|event| {
            event["cursor"]
                .as_u64()
                .expect("event cursor should be u64 integer")
        })
        .collect::<Vec<_>>();
    let mut sorted = cursors.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        cursors, sorted,
        "events should be delivered in cursor order"
    );
}

#[test]
fn delivery_events_reconnect_with_cursor_replays_without_loss() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-events-reconnect");
    let subscriber_id = unique_subscriber_id("delivery-sub-reconnect");

    let first_subscribe = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": subscriber_id,
            "task_id": task_id
        }),
    )
    .expect("initial subscribe should succeed");
    let initial_cursor = first_subscribe["cursor"]
        .as_u64()
        .expect("initial cursor should be u64");

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 0.25"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    let _ = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": task_id
        }),
    )
    .expect("delivery_status should succeed");

    let first_batch = handle_tool_call(
        "delivery_events_next",
        json!({
            "subscriber_id": subscriber_id,
            "task_id": task_id,
            "limit": 2
        }),
    )
    .expect("first delivery_events_next should succeed");
    let first_events = first_batch["events"]
        .as_array()
        .expect("events should be array")
        .clone();
    assert!(!first_events.is_empty(), "expected first batch of events");
    let first_batch_cursor = first_batch["next_cursor"]
        .as_u64()
        .expect("next_cursor should be u64");
    ack_events(&subscriber_id, first_batch_cursor);

    let _reconnected = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": subscriber_id,
            "task_id": task_id,
            "cursor": first_batch_cursor
        }),
    )
    .expect("reconnect subscribe should succeed");

    let mut resumed_events = Vec::new();
    for _ in 0..80 {
        let _ = handle_tool_call(
            "delivery_status",
            json!({
                "task_id": task_id
            }),
        )
        .expect("delivery_status should succeed");

        let next = handle_tool_call(
            "delivery_events_next",
            json!({
                "subscriber_id": subscriber_id,
                "task_id": task_id,
                "limit": 16
            }),
        )
        .expect("delivery_events_next after reconnect should succeed");
        let events = next["events"].as_array().expect("events should be array");
        if !events.is_empty() {
            resumed_events.extend(events.iter().cloned());
            let last_cursor = next["next_cursor"]
                .as_u64()
                .expect("next_cursor should be u64");
            ack_events(&subscriber_id, last_cursor);
        }
        if resumed_events
            .iter()
            .any(|event| event["event_type"] == "completed")
        {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    let replay_subscriber = unique_subscriber_id("delivery-sub-replay");
    let _ = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": replay_subscriber,
            "task_id": task_id,
            "cursor": initial_cursor
        }),
    )
    .expect("replay subscribe should succeed");
    let replay_all = handle_tool_call(
        "delivery_events_next",
        json!({
            "subscriber_id": replay_subscriber,
            "task_id": task_id,
            "limit": 128
        }),
    )
    .expect("replay fetch should succeed");
    let expected_all = replay_all["events"]
        .as_array()
        .expect("events should be array")
        .iter()
        .map(|event| {
            event["cursor"]
                .as_u64()
                .expect("event cursor should be u64 integer")
        })
        .collect::<Vec<_>>();

    let mut resumed_cursors = first_events
        .iter()
        .chain(resumed_events.iter())
        .map(|event| {
            event["cursor"]
                .as_u64()
                .expect("event cursor should be u64 integer")
        })
        .collect::<Vec<_>>();
    resumed_cursors.sort_unstable();
    resumed_cursors.dedup();

    assert_eq!(
        resumed_cursors, expected_all,
        "reconnect replay should reconstruct the same event stream"
    );
}

#[test]
fn delivery_events_status_changed_is_deduplicated_for_same_state() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-events-dedupe");
    let (subscriber_id, _) = subscribe_events(&task_id);

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 0.35"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    for _ in 0..6 {
        let _ = handle_tool_call(
            "delivery_status",
            json!({
                "task_id": task_id
            }),
        )
        .expect("delivery_status should succeed");
        thread::sleep(Duration::from_millis(20));
    }

    let events_page = next_events(&subscriber_id, &task_id, 64);
    let events = events_page["events"]
        .as_array()
        .expect("events should be array");
    if let Some(last_cursor) = events_page["next_cursor"].as_u64()
        && !events.is_empty()
    {
        ack_events(&subscriber_id, last_cursor);
    }

    let running_status_changed = events
        .iter()
        .filter(|event| {
            event["event_type"] == "status_changed"
                && event["state"] == "running"
                && event["runtime_state"] == "running"
        })
        .count();

    assert_eq!(
        running_status_changed, 1,
        "expected exactly one running status_changed event, got events: {events:?}"
    );

    let _ = wait_for_runtime_state(&task_id, "ready_to_merge");
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
