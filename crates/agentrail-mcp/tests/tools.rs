use std::{
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentrail_mcp::handle_tool_call;
use agentrail_store::TaskStore;
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

fn report_task_ids(report: &serde_json::Value) -> Vec<String> {
    report["tasks"]
        .as_array()
        .expect("tasks should be an array")
        .iter()
        .map(|task| {
            task["task_id"]
                .as_str()
                .expect("task_id should be a string")
                .to_string()
        })
        .collect()
}

fn runtime_store_dsn() -> String {
    std::env::var("AGENTRAIL_RUNTIME_DSN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "sqlite://.agentrail/runtime.db".to_string())
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
            "last_steer_apply_hint",
            "last_steer_observed_at",
            "last_steer_sent_at",
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
    assert!(status["normalized"]["last_steer_sent_at"].is_null());
    assert!(status["normalized"]["last_steer_observed_at"].is_null());
    assert!(status["normalized"]["last_steer_apply_hint"].is_null());
    assert!(status["normalized"]["timestamps"]["updated_at"].is_number());
    assert_eq!(status["normalized"]["logs"]["tail"], 5);
    assert!(status["normalized"]["logs"]["truncated"].is_boolean());

    let report = handle_tool_call("delivery_report", json!({})).expect("report should succeed");
    assert_eq!(report["tool"], "delivery_report");
    assert!(report["summary"]["total"].is_number());
    assert!(report["tasks"].is_array());
}

#[test]
fn delivery_submit_rejects_steer_required_with_process_runner() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-steer-required-process");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "steer_required": true,
            "interactive_command": true,
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject steer_required with process runner");

    assert!(
        err.to_string()
            .contains("steer_required=true requires runner_mode=tmux"),
        "expected runner_mode validation error, got: {err}"
    );
}

#[test]
fn delivery_submit_rejects_steer_required_without_interactive_command() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-steer-required-noninteractive");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "tmux",
            "steer_required": true,
            "interactive_command": false,
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject steer_required without interactive_command");

    assert!(
        err.to_string()
            .contains("steer_required=true requires interactive_command=true"),
        "expected interactive_command validation error, got: {err}"
    );
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
    assert!(status["normalized"]["last_steer_sent_at"].is_null());
    assert!(status["normalized"]["last_steer_observed_at"].is_null());
    assert!(status["normalized"]["last_steer_apply_hint"].is_null());
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
fn runtime_attempts_and_sessions_persist_for_launches_and_reconnect() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-attempt-persist");

    let first_start = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first start should succeed");
    assert!(first_start["session_id"].is_string());

    let stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "test-stop"
        }),
    )
    .expect("stop should succeed");
    assert_eq!(stop["status"], "stopped");

    let second_start = handle_tool_call(
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
    .expect("second start should succeed");
    assert!(second_start["session_id"].is_string());
    let _ = wait_for_runtime_state(&task_id, "ready_to_merge");

    let dsn = runtime_store_dsn();
    assert!(
        dsn.starts_with("sqlite://"),
        "this test requires sqlite-backed runtime store, got: {dsn}"
    );
    let store = TaskStore::connect(dsn.clone());
    let attempts = store
        .list_task_attempts(&task_id)
        .expect("attempt query should succeed");
    assert_eq!(attempts.len(), 2, "expected one row per launch");
    assert_eq!(attempts[0].attempt_number, 1);
    assert_eq!(attempts[1].attempt_number, 2);

    let sessions = store
        .list_task_sessions(&task_id)
        .expect("session query should succeed");
    assert_eq!(sessions.len(), 2);
    assert!(
        sessions
            .iter()
            .all(|session| session.ended_epoch_ms.is_some()),
        "terminal sessions should be retained with ended timestamp: {sessions:?}"
    );

    drop(store);
    let reopened = TaskStore::connect(dsn);
    let reopened_attempts = reopened
        .list_task_attempts(&task_id)
        .expect("attempt query after reconnect should succeed");
    assert_eq!(reopened_attempts.len(), 2);
}

#[test]
fn delivery_events_reject_forward_cursor_jumps_for_subscribe_and_ack() {
    let subscriber_id = unique_subscriber_id("delivery-sub-forward-jump");

    let subscribe_err = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": subscriber_id,
            "cursor": u64::MAX
        }),
    )
    .expect_err("subscribe should reject forward cursor jumps");
    assert!(
        subscribe_err.to_string().contains("cursor"),
        "expected cursor validation error, got: {subscribe_err}"
    );

    let subscriber_id = unique_subscriber_id("delivery-sub-forward-ack");
    let _ = handle_tool_call(
        "delivery_events_subscribe",
        json!({
            "subscriber_id": subscriber_id
        }),
    )
    .expect("subscribe should succeed without cursor override");

    let ack_err = handle_tool_call(
        "delivery_events_ack",
        json!({
            "subscriber_id": subscriber_id,
            "cursor": u64::MAX
        }),
    )
    .expect_err("ack should reject forward cursor jumps");
    assert!(
        ack_err.to_string().contains("cursor"),
        "expected cursor validation error, got: {ack_err}"
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
fn delivery_stop_transitions_active_process_to_failed_retryable() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-stop-process");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");
    assert_eq!(submit["tool"], "delivery_submit");

    let stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "manual-stop"
        }),
    )
    .expect("delivery_stop should succeed for active task");
    assert_eq!(stop["tool"], "delivery_stop");
    assert_eq!(stop["status"], "stopped");
    assert_eq!(stop["runtime_state"], "failed_retryable");
    assert_eq!(stop["stop_propagated"], true);

    let status = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": task_id
        }),
    )
    .expect("delivery_status should succeed after stop");
    assert_eq!(status["runtime_state"], "failed_retryable");
}

#[test]
fn delivery_stop_emits_lifecycle_events_for_stopped_transition() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-stop-events");
    let (subscriber_id, _) = subscribe_events(&task_id);

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "manual-stop"
        }),
    )
    .expect("delivery_stop should succeed");

    let next = next_events(&subscriber_id, &task_id, 64);
    let events = next["events"].as_array().expect("events should be array");

    assert!(
        events.iter().any(|event| {
            event["event_type"] == "status_changed"
                && event["state"] == "stopped"
                && event["runtime_state"] == "failed_retryable"
        }),
        "expected stopped status_changed event, got: {events:?}"
    );
    assert!(
        events.iter().any(|event| {
            event["event_type"] == "stopped"
                && event["state"] == "stopped"
                && event["runtime_state"] == "failed_retryable"
        }),
        "expected stopped lifecycle event, got: {events:?}"
    );
}

#[test]
fn delivery_cleanup_lifecycle_mode_supports_force_and_retain() {
    let tmp = tempdir().expect("tempdir");
    let active_task = unique_task_id("task-delivery-cleanup-active");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": active_task,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("active delivery_submit should succeed");

    let err = handle_tool_call(
        "delivery_cleanup",
        json!({
            "task_id": active_task
        }),
    )
    .expect_err("cleanup without force should reject active task");
    assert!(err.to_string().contains("force=true"));

    let cleaned = handle_tool_call(
        "delivery_cleanup",
        json!({
            "task_id": active_task,
            "force": true
        }),
    )
    .expect("cleanup with force should succeed");
    assert_eq!(cleaned["tool"], "delivery_cleanup");
    assert_eq!(cleaned["mode"], "lifecycle");
    assert_eq!(cleaned["cleaned_count"], 1);
    assert_eq!(cleaned["deleted_count"], 1);

    let status_after_clean = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": active_task
        }),
    )
    .expect("delivery_status should succeed for cleaned task");
    assert_eq!(status_after_clean["runtime_state"], "unknown");

    let retained_task = unique_task_id("task-delivery-cleanup-retain");
    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": retained_task,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("retained delivery_submit should succeed");
    let _ = wait_for_runtime_state(&retained_task, "ready_to_merge");

    let retained = handle_tool_call(
        "delivery_cleanup",
        json!({
            "task_id": retained_task,
            "retention_mode": "retain"
        }),
    )
    .expect("cleanup retain mode should succeed");
    assert_eq!(retained["mode"], "lifecycle");
    assert_eq!(retained["retention_mode"], "retain");
    assert_eq!(retained["retained_count"], 1);
    assert_eq!(retained["cleaned_count"], 0);

    let status_after_retain = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": retained_task
        }),
    )
    .expect("retained task should still be queryable");
    assert_eq!(status_after_retain["runtime_state"], "ready_to_merge");
}

#[test]
fn delivery_report_supports_scope_filters_and_cursor_pagination() {
    let tmp = tempdir().expect("tempdir");
    let scope_id = unique_task_id("scope-pagination");
    let other_scope = unique_task_id("scope-other");

    let mut scoped_ids = Vec::new();
    for idx in 0..3 {
        let task_id = unique_task_id(&format!("task-scope-ready-{idx}"));
        scoped_ids.push(task_id.clone());
        let _ = handle_tool_call(
            "delivery_submit",
            json!({
                "task_id": task_id,
                "scope_id": scope_id,
                "worker_id": "worker-a",
                "runner_mode": "process",
                "command": "bash",
                "args": ["-lc", "true"],
                "workdir": tmp.path().display().to_string()
            }),
        )
        .expect("scoped submit should succeed");
        let _ = wait_for_runtime_state(&task_id, "ready_to_merge");
    }

    let other_task = unique_task_id("task-other-ready");
    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": other_task,
            "scope_id": other_scope,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("other scope submit should succeed");
    let _ = wait_for_runtime_state(&other_task, "ready_to_merge");

    let scoped_report = handle_tool_call(
        "delivery_report",
        json!({
            "scope_id": scope_id,
            "states": ["ready_to_merge"]
        }),
    )
    .expect("scoped report should succeed");
    assert_eq!(scoped_report["scope_id"], scope_id);
    assert_eq!(scoped_report["summary"]["total"], 3);
    assert!(
        scoped_report["tasks"]
            .as_array()
            .expect("tasks array")
            .iter()
            .all(|task| task["scope_id"] == scope_id),
        "all tasks should match requested scope"
    );

    let full_ids = report_task_ids(&scoped_report);
    let first_page = handle_tool_call(
        "delivery_report",
        json!({
            "scope_id": scope_id,
            "states": ["ready_to_merge"],
            "limit": 2
        }),
    )
    .expect("first page should succeed");
    assert_eq!(report_task_ids(&first_page).len(), 2);
    assert!(first_page["next_cursor"].is_string());

    let second_page = handle_tool_call(
        "delivery_report",
        json!({
            "scope_id": scope_id,
            "states": ["ready_to_merge"],
            "limit": 2,
            "cursor": first_page["next_cursor"]
        }),
    )
    .expect("second page should succeed");

    let mut paged_ids = report_task_ids(&first_page);
    paged_ids.extend(report_task_ids(&second_page));
    assert_eq!(paged_ids, full_ids);
    assert!(paged_ids.iter().all(|id| scoped_ids.contains(id)));
}

#[test]
fn delivery_cleanup_retention_prune_mode_deletes_by_scope_state_and_cutoff() {
    let tmp = tempdir().expect("tempdir");
    let scope_id = unique_task_id("scope-retention");
    let ready_task = unique_task_id("task-prune-ready");
    let running_task = unique_task_id("task-prune-running");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": ready_task,
            "scope_id": scope_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("ready submit should succeed");
    let _ = wait_for_runtime_state(&ready_task, "ready_to_merge");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": running_task,
            "scope_id": scope_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("running submit should succeed");
    let _ = wait_for_runtime_state(&running_task, "running");

    let cutoff_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_millis() as i64
        + 60_000;

    let cleanup = handle_tool_call(
        "delivery_cleanup",
        json!({
            "scope_id": scope_id,
            "states": ["ready_to_merge"],
            "updated_before_epoch_ms": cutoff_ms
        }),
    )
    .expect("prune cleanup should succeed");
    assert_eq!(cleanup["tool"], "delivery_cleanup");
    assert_eq!(cleanup["mode"], "retention_prune");
    assert_eq!(cleanup["deleted_count"], 1);
    assert_eq!(cleanup["deleted_task_ids"][0], ready_task);

    let scoped_report =
        handle_tool_call("delivery_report", json!({ "scope_id": scope_id })).expect("report");
    let remaining = report_task_ids(&scoped_report);
    assert_eq!(remaining, vec![running_task]);
}

#[test]
fn delivery_cleanup_retention_prune_mode_rejects_active_tasks() {
    let tmp = tempdir().expect("tempdir");
    let scope_id = unique_task_id("scope-retention-active-guard");
    let ready_task = unique_task_id("task-prune-guard-ready");
    let running_task = unique_task_id("task-prune-guard-running");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": ready_task,
            "scope_id": scope_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("ready submit should succeed");
    let _ = wait_for_runtime_state(&ready_task, "ready_to_merge");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": running_task,
            "scope_id": scope_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("running submit should succeed");
    let _ = wait_for_runtime_state(&running_task, "running");

    let cutoff_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_millis() as i64
        + 60_000;

    let err = handle_tool_call(
        "delivery_cleanup",
        json!({
            "scope_id": scope_id,
            "updated_before_epoch_ms": cutoff_ms
        }),
    )
    .expect_err("prune cleanup should refuse active tasks");
    assert!(
        err.to_string().contains("active task"),
        "expected active-task guard error, got: {err}"
    );

    let scoped_report =
        handle_tool_call("delivery_report", json!({ "scope_id": scope_id })).expect("report");
    let remaining = report_task_ids(&scoped_report);
    assert!(remaining.contains(&ready_task));
    assert!(remaining.contains(&running_task));
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
