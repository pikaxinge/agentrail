use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentrail_mcp::handle_tool_call;
use agentrail_store::TaskStore;
use serde_json::{Value, json};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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

fn parse_json_lines(raw: &str) -> Vec<Value> {
    raw.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

fn wait_for_server_response_line(log_path: &Path, expected_id: &Value) -> Value {
    let mut last_log = String::new();
    for _ in 0..80 {
        if let Ok(raw) = fs::read_to_string(log_path) {
            last_log = raw.clone();
            let lines = parse_json_lines(&raw);
            if let Some(response) = lines.into_iter().find(|line| {
                line.get("id") == Some(expected_id)
                    && (line.get("result").is_some() || line.get("error").is_some())
            }) {
                return response;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "timed out waiting for server response id={expected_id}: {last_log}",
        expected_id = expected_id
    );
}

fn write_fake_app_server_script(
    root: &Path,
    script_name: &str,
    completion_status: Option<&str>,
) -> (PathBuf, PathBuf) {
    let script_path = root.join(script_name);
    let log_path = root.join(format!("{script_name}.requests.log"));
    let escaped_log_path = log_path.display().to_string().replace('\'', "'\"'\"'");
    let completion_literal = completion_status.unwrap_or("");
    let emit_completion_payload = if completion_status.is_some() {
        "printf '{\"method\":\"turn/completed\",\"params\":{\"turn\":{\"id\":\"%s\",\"status\":\"%s\"}}}\\n' \"$turn_id\" \"$COMPLETE_STATUS\""
    } else {
        ":"
    };
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

LOG_PATH='{escaped_log_path}'
COMPLETE_STATUS='{completion_literal}'
turn_seq=0

next_turn_id() {{
  turn_seq=$((turn_seq + 1))
  printf 'turn-%s' "$turn_seq"
}}

while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LOG_PATH"
  id="$(printf '%s\n' "$line" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"

  if [[ "$line" == *'"method":"initialize"'* ]]; then
    printf '{{"id":%s,"result":{{"userAgent":"fake-app-server/1.0"}}}}\n' "$id"
  elif [[ "$line" == *'"method":"initialized"'* ]]; then
    :
  elif [[ "$line" == *'"method":"thread/start"'* ]]; then
    printf '{{"id":%s,"result":{{"thread":{{"id":"thread-test"}}}}}}\n' "$id"
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    turn_id="$(next_turn_id)"
    printf '{{"id":%s,"result":{{"turn":{{"id":"%s"}}}}}}\n' "$id" "$turn_id"
    {emit_completion_payload}
  elif [[ "$line" == *'"method":"turn/steer"'* ]]; then
    turn_id="$(next_turn_id)"
    printf '{{"id":%s,"result":{{"turn":{{"id":"%s"}}}}}}\n' "$id" "$turn_id"
    {emit_completion_payload}
  elif [[ "$line" == *'"method":"turn/interrupt"'* ]]; then
    printf '{{"id":%s,"result":{{"accepted":true}}}}\n' "$id"
    printf '{{"method":"turn/completed","params":{{"turn":{{"id":"turn-interrupted","status":"interrupted"}}}}}}\n'
  elif [[ -n "$id" ]]; then
    printf '{{"id":%s,"result":{{}}}}\n' "$id"
  fi
done
"#
    );
    fs::write(&script_path, script).expect("write fake app server script");
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(&script_path)
            .expect("metadata for fake app server script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions)
            .expect("set executable bit for fake app server script");
    }
    (script_path, log_path)
}

fn write_fake_app_server_with_server_request_script(
    root: &Path,
    script_name: &str,
    server_request_json: &str,
) -> (PathBuf, PathBuf) {
    let script_path = root.join(script_name);
    let log_path = root.join(format!("{script_name}.requests.log"));
    let escaped_log_path = log_path.display().to_string().replace('\'', "'\"'\"'");
    let escaped_server_request_json = server_request_json.replace('\'', "'\"'\"'");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

LOG_PATH='{escaped_log_path}'
SERVER_REQUEST_JSON='{escaped_server_request_json}'
sent_server_request=0

while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LOG_PATH"
  id="$(printf '%s\n' "$line" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"

  if [[ "$line" == *'"method":"initialize"'* ]]; then
    printf '{{"id":%s,"result":{{"userAgent":"fake-app-server/1.0"}}}}\n' "$id"
  elif [[ "$line" == *'"method":"initialized"'* ]]; then
    :
  elif [[ "$line" == *'"method":"thread/start"'* ]]; then
    printf '{{"id":%s,"result":{{"thread":{{"id":"thread-test"}}}}}}\n' "$id"
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    printf '{{"id":%s,"result":{{"turn":{{"id":"turn-1"}}}}}}\n' "$id"
    if [[ "$sent_server_request" -eq 0 ]]; then
      sent_server_request=1
      printf '%s\n' "$SERVER_REQUEST_JSON"
    fi
  elif [[ "$line" == *'"method":"turn/interrupt"'* ]]; then
    printf '{{"id":%s,"result":{{"accepted":true}}}}\n' "$id"
  elif [[ -n "$id" ]]; then
    printf '{{"id":%s,"result":{{}}}}\n' "$id"
  fi
done
"#
    );
    fs::write(&script_path, script).expect("write fake app server script with server request");
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(&script_path)
            .expect("metadata for fake app server script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions)
            .expect("set executable bit for fake app server script");
    }
    (script_path, log_path)
}

fn write_fake_app_server_steer_requires_turn_start_script(
    root: &Path,
    script_name: &str,
) -> (PathBuf, PathBuf) {
    let script_path = root.join(script_name);
    let log_path = root.join(format!("{script_name}.requests.log"));
    let escaped_log_path = log_path.display().to_string().replace('\'', "'\"'\"'");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

LOG_PATH='{escaped_log_path}'
turn_start_count=0

while IFS= read -r line; do
  printf '%s\n' "$line" >> "$LOG_PATH"
  id="$(printf '%s\n' "$line" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"

  if [[ "$line" == *'"method":"initialize"'* ]]; then
    printf '{{"id":%s,"result":{{"userAgent":"fake-app-server/1.0"}}}}\n' "$id"
  elif [[ "$line" == *'"method":"initialized"'* ]]; then
    :
  elif [[ "$line" == *'"method":"thread/start"'* ]]; then
    printf '{{"id":%s,"result":{{"thread":{{"id":"thread-test"}}}}}}\n' "$id"
  elif [[ "$line" == *'"method":"turn/start"'* ]]; then
    turn_start_count=$((turn_start_count + 1))
    printf '{{"id":%s,"result":{{"turn":{{"id":"turn-%s"}}}}}}\n' "$id" "$turn_start_count"
  elif [[ "$line" == *'"method":"turn/steer"'* ]]; then
    printf '{{"id":%s,"error":{{"code":-32600,"message":"no active turn to steer"}}}}\n' "$id"
  elif [[ "$line" == *'"method":"turn/interrupt"'* ]]; then
    printf '{{"id":%s,"result":{{"accepted":true}}}}\n' "$id"
  elif [[ -n "$id" ]]; then
    printf '{{"id":%s,"result":{{}}}}\n' "$id"
  fi
done
"#
    );
    fs::write(&script_path, script)
        .expect("write fake app server script requiring turn/start steer fallback");
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(&script_path)
            .expect("metadata for fake app server script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions)
            .expect("set executable bit for fake app server script");
    }
    (script_path, log_path)
}

#[derive(Debug)]
struct RealCliE2eConfig {
    command: String,
    args: Vec<String>,
    artifact_dir: PathBuf,
    tail: u32,
    steer_instruction: String,
}

fn parse_string_array_env(var_name: &str, default: &[&str]) -> Vec<String> {
    match std::env::var(var_name) {
        Ok(raw) if !raw.trim().is_empty() => serde_json::from_str::<Vec<String>>(&raw)
            .unwrap_or_else(|error| panic!("{var_name} must be a JSON string array: {error}")),
        _ => default.iter().map(|value| (*value).to_string()).collect(),
    }
}

fn load_real_cli_e2e_config() -> RealCliE2eConfig {
    let enabled = std::env::var("AGENTRAIL_E2E_REAL_CLI")
        .ok()
        .unwrap_or_default();
    assert_eq!(
        enabled, "1",
        "set AGENTRAIL_E2E_REAL_CLI=1 to acknowledge running real local CLI E2E tests"
    );

    let command = std::env::var("AGENTRAIL_E2E_APP_SERVER_COMMAND")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "codex".to_string());
    let args = parse_string_array_env("AGENTRAIL_E2E_APP_SERVER_ARGS_JSON", &["app-server"]);

    let artifact_dir = std::env::var("AGENTRAIL_E2E_ARTIFACT_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("agentrail-e2e-real-cli-{}", unique_task_id("run")))
        });
    fs::create_dir_all(&artifact_dir).expect("create AGENTRAIL_E2E_ARTIFACT_DIR");

    let tail = std::env::var("AGENTRAIL_E2E_STATUS_TAIL")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(200);
    let steer_instruction = std::env::var("AGENTRAIL_E2E_STEER_INSTRUCTION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "real-cli-e2e-steer-probe".to_string());

    RealCliE2eConfig {
        command,
        args,
        artifact_dir,
        tail,
        steer_instruction,
    }
}

fn write_json_artifact(dir: &Path, file_name: &str, value: &Value) {
    let path = dir.join(file_name);
    let payload = serde_json::to_string_pretty(value).expect("serialize artifact JSON");
    fs::write(&path, format!("{payload}\n")).unwrap_or_else(|error| {
        panic!("write artifact {} failed: {error}", path.display());
    });
}

fn wait_for_runtime_state_any(task_id: &str, wanted: &[&str]) -> serde_json::Value {
    let mut last = None;
    for _ in 0..80 {
        let status = handle_tool_call(
            "delivery_status",
            json!({
                "task_id": task_id,
                "tail": 200
            }),
        )
        .expect("delivery_status should succeed while waiting for runtime state");
        let runtime_state = status["runtime_state"].as_str().unwrap_or("unknown");
        if wanted.contains(&runtime_state) {
            return status;
        }
        last = Some(status);
        thread::sleep(Duration::from_millis(75));
    }

    panic!(
        "timed out waiting for runtime_state in {:?}, last_status={}",
        wanted,
        last.unwrap_or_else(|| json!({"status":"none"}))
    );
}

fn capture_task_artifacts(task_id: &str, config: &RealCliE2eConfig, label: &str) {
    let status = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": task_id,
            "tail": config.tail
        }),
    )
    .expect("delivery_status should succeed for artifact capture");
    let timeline = handle_tool_call(
        "delivery_timeline",
        json!({
            "task_id": task_id,
            "limit": 500
        }),
    )
    .expect("delivery_timeline should succeed for artifact capture");
    let explain = handle_tool_call(
        "delivery_explain_failure",
        json!({
            "task_id": task_id
        }),
    )
    .expect("delivery_explain_failure should succeed for artifact capture");
    let summary = json!({
        "task_id": task_id,
        "label": label,
        "runtime_state": status["runtime_state"],
        "state": status["state"],
        "timeline_cursor": timeline["next_cursor"],
        "failed": explain["failed"],
    });

    write_json_artifact(
        &config.artifact_dir,
        &format!("{label}-{task_id}-status.json"),
        &status,
    );
    write_json_artifact(
        &config.artifact_dir,
        &format!("{label}-{task_id}-timeline.json"),
        &timeline,
    );
    write_json_artifact(
        &config.artifact_dir,
        &format!("{label}-{task_id}-explain-failure.json"),
        &explain,
    );
    write_json_artifact(
        &config.artifact_dir,
        &format!("{label}-{task_id}-summary.json"),
        &summary,
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
fn orchestrate_start_rejects_tmux_runner_mode_after_removal() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-start-tmux-unsupported");

    let err = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "tmux",
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("orchestrate_start should reject removed tmux runner mode");

    assert!(
        err.to_string().contains("unsupported runner_mode: tmux"),
        "expected unsupported tmux runner mode validation error, got: {err}"
    );
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
fn delivery_submit_retry_matrix_rejects_ready_to_merge_with_deterministic_reason() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-matrix-ready-to-merge");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");

    let _ = wait_for_runtime_state(&task_id, "ready_to_merge");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "true"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("retry from ready_to_merge should be rejected");

    assert!(
        err.to_string()
            .contains("retry ineligible: runtime_state=ready_to_merge"),
        "expected deterministic retry ineligibility reason, got: {err}"
    );
}

#[test]
fn delivery_submit_retry_budget_is_enforced_with_deterministic_reason() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-budget");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 1,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "retry-cycle-1"
        }),
    )
    .expect("first stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 1,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "retry-cycle-2"
        }),
    )
    .expect("second stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 1,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("submit beyond retry_budget should fail");

    assert!(
        err.to_string().contains("retry budget exhausted"),
        "expected deterministic retry budget rejection, got: {err}"
    );
}

#[test]
fn delivery_submit_retry_idempotency_key_deduplicates_duplicate_requests() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-idempotency");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 3,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "enter-retryable"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let first_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-1",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");

    let second_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-1",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("duplicate retry submit should be idempotent");

    assert_eq!(
        first_retry["orchestration"]["session_id"], second_retry["orchestration"]["session_id"],
        "idempotent duplicate should return the same session"
    );
}

#[test]
fn delivery_submit_retry_idempotency_key_conflicts_on_command_shape_change() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-idempotency-command-conflict");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 3,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "enter-retryable"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let _first_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-command-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-command-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 6"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("idempotency key reuse with changed command shape should fail deterministically");

    assert!(
        err.to_string().contains("idempotency_key_conflict"),
        "expected idempotency_key_conflict error, got: {err}"
    );
}

#[test]
fn delivery_submit_retry_idempotency_key_conflicts_on_runner_mode_change() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-idempotency-runner-mode-conflict");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 3,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "enter-retryable"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let _first_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-runner-mode-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "idempotency_key": "retry-key-runner-mode-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("idempotency key reuse with changed runner_mode should fail deterministically");

    assert!(
        err.to_string().contains("idempotency_key_conflict"),
        "expected idempotency_key_conflict error, got: {err}"
    );
}

#[test]
fn delivery_submit_retry_idempotency_key_conflicts_on_workdir_change() {
    let tmp = tempdir().expect("tempdir");
    let tmp_alt = tempdir().expect("tempdir alt");
    let task_id = unique_task_id("task-retry-idempotency-workdir-conflict");

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "retry_budget": 3,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "enter-retryable"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let _first_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-workdir-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-key-workdir-conflict",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp_alt.path().display().to_string()
        }),
    )
    .expect_err("idempotency key reuse with changed workdir should fail deterministically");

    assert!(
        err.to_string().contains("idempotency_key_conflict"),
        "expected idempotency_key_conflict error, got: {err}"
    );
}

#[test]
fn delivery_submit_retry_idempotency_key_conflicts_on_app_server_policy_change() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-idempotency-policy-conflict");
    let (script_path, _request_log_path) =
        write_fake_app_server_script(tmp.path(), "fake-app-server-idempotency-policy.sh", None);

    let _ = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "retry_budget": 3,
            "app_server_request_policy": "allow_safe_subset",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "enter-retryable"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let _first_retry = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "idempotency_key": "retry-key-policy-conflict",
            "app_server_request_policy": "allow_safe_subset",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("first retry submit should succeed");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "idempotency_key": "retry-key-policy-conflict",
            "app_server_request_policy": "delegate_fail_open",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("idempotency key reuse with changed app_server_request_policy should fail");

    assert!(
        err.to_string().contains("idempotency_key_conflict"),
        "expected idempotency_key_conflict error, got: {err}"
    );
}

#[test]
fn delivery_retry_events_include_old_and_new_attempt_numbers() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-retry-attempt-audit");
    let (subscriber_id, _) = subscribe_events(&task_id);

    let first_submit = handle_tool_call(
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
    .expect("initial submit should succeed");
    let first_trace_id = first_submit["orchestration"]["trace_id"]
        .as_str()
        .expect("first trace_id should be string")
        .to_string();
    let first_span_id = first_submit["orchestration"]["span_id"]
        .as_str()
        .expect("first span_id should be string")
        .to_string();
    assert!(
        first_submit["orchestration"]["parent_span_id"].is_null(),
        "first attempt should not have parent span"
    );
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "prepare-retry"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let retry_submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "idempotency_key": "retry-audit-key",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("retry submit should succeed");
    assert_eq!(
        retry_submit["orchestration"]["trace_id"],
        json!(first_trace_id)
    );
    assert_eq!(
        retry_submit["orchestration"]["parent_span_id"],
        json!(first_span_id)
    );
    assert_ne!(
        retry_submit["orchestration"]["span_id"],
        json!(first_span_id),
        "retry should allocate a new span id"
    );

    let next = next_events(&subscriber_id, &task_id, 128);
    let events = next["events"].as_array().expect("events should be array");

    let has_attempt_delta = events.iter().any(|event| {
        event
            .get("old_attempt_number")
            .and_then(serde_json::Value::as_u64)
            .is_some()
            && event
                .get("new_attempt_number")
                .and_then(serde_json::Value::as_u64)
                .is_some()
    });
    assert!(
        has_attempt_delta,
        "expected retry audit fields old_attempt_number/new_attempt_number in events: {events:?}"
    );
    let retry_started = events
        .iter()
        .find(|event| event["event_type"] == "retry_started")
        .expect("retry_started event should be present");
    assert_eq!(retry_started["trace_id"], json!(first_trace_id));
    assert_eq!(retry_started["parent_span_id"], json!(first_span_id));
    assert!(retry_started["span_id"].is_string());
}

#[test]
fn orchestrate_start_propagates_explicit_trace_context_to_status() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-trace-explicit");

    let started = handle_tool_call(
        "orchestrate_start",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "trace_id": "trace:root-operation",
            "parent_span_id": "span:root-operation:7",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("orchestrate_start should succeed");
    assert_eq!(started["trace_id"], "trace:root-operation");
    assert_eq!(started["parent_span_id"], "span:root-operation:7");
    assert!(started["span_id"].is_string());

    let status = wait_for_runtime_state(&task_id, "running");
    assert_eq!(status["trace_id"], "trace:root-operation");
    assert_eq!(status["parent_span_id"], "span:root-operation:7");
    assert_eq!(status["span_id"], started["span_id"]);

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "trace-context-test-cleanup"
        }),
    )
    .expect("delivery_stop cleanup should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");
}

#[test]
fn delivery_submit_new_trace_without_parent_does_not_link_previous_span() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-trace-new-root");

    let first_submit = handle_tool_call(
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
    .expect("initial submit should succeed");
    let first_span_id = first_submit["orchestration"]["span_id"]
        .as_str()
        .expect("first span id should be a string")
        .to_string();
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "prepare-new-trace-retry"
        }),
    )
    .expect("stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let retry_submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "trace_id": "trace:new-root",
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("retry submit with explicit trace should succeed");
    assert_eq!(retry_submit["orchestration"]["trace_id"], "trace:new-root");
    assert!(
        retry_submit["orchestration"]["parent_span_id"].is_null(),
        "explicit new trace without parent must not auto-link to prior span"
    );
    assert_ne!(
        retry_submit["orchestration"]["span_id"],
        json!(first_span_id)
    );

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup-new-trace-retry"
        }),
    )
    .expect("cleanup stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");
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
            "last_output_at",
            "last_steer_applied_at",
            "last_steer_apply_hint",
            "last_steer_observed_at",
            "last_steer_sent_at",
            "last_steer_stalled_at",
            "logs",
            "parent_span_id",
            "retry_budget",
            "retry_count",
            "runner_mode",
            "runtime_state",
            "session_id",
            "span_id",
            "stall_duration_ms",
            "stall_reason",
            "state",
            "task_id",
            "timestamps",
            "tool",
            "trace_id"
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
        status["normalized"]["trace_id"],
        status["orchestration"]["trace_id"]
    );
    assert_eq!(
        status["normalized"]["span_id"],
        status["orchestration"]["span_id"]
    );
    assert_eq!(
        status["normalized"]["parent_span_id"],
        status["orchestration"]["parent_span_id"]
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
    assert!(status["normalized"]["last_steer_applied_at"].is_null());
    assert!(status["normalized"]["last_steer_stalled_at"].is_null());
    assert!(status["normalized"]["last_steer_apply_hint"].is_null());
    assert_eq!(
        status["normalized"]["last_output_at"],
        status["orchestration"]["last_output_at"]
    );
    assert_eq!(
        status["normalized"]["stall_duration_ms"],
        status["orchestration"]["stall_duration_ms"]
    );
    assert_eq!(
        status["normalized"]["stall_reason"],
        status["orchestration"]["stall_reason"]
    );
    assert!(status["normalized"]["timestamps"]["updated_at"].is_number());
    assert_eq!(status["normalized"]["logs"]["tail"], 5);
    assert!(status["normalized"]["logs"]["truncated"].is_boolean());

    let report = handle_tool_call("delivery_report", json!({})).expect("report should succeed");
    assert_eq!(report["tool"], "delivery_report");
    assert!(report["summary"]["total"].is_number());
    assert!(report["tasks"].is_array());
    let task_row = report["tasks"]
        .as_array()
        .expect("tasks should be array")
        .iter()
        .find(|row| row["task_id"] == status["task_id"])
        .expect("report should include submitted task");
    assert!(task_row.get("last_output_at").is_some());
    assert!(task_row.get("stall_duration_ms").is_some());
    assert!(task_row.get("stall_reason").is_some());
}

#[test]
fn delivery_status_reports_no_output_freshness_after_stagnant_polls() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-stall-no-output");

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "bash",
            "args": ["-lc", "echo once && sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");
    let _ = wait_for_runtime_state(&task_id, "running");

    let mut latest = json!({});
    for _ in 0..5 {
        latest = handle_tool_call(
            "delivery_status",
            json!({
                "task_id": task_id,
                "tail": 32
            }),
        )
        .expect("delivery_status should succeed");
        thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(latest["runtime_state"], "running");
    assert_eq!(latest["normalized"]["stall_reason"], "no_output_freshness");
    assert!(
        latest["normalized"]["last_output_at"].is_null()
            || latest["normalized"]["last_output_at"].is_number()
    );
    assert!(latest["normalized"]["stall_duration_ms"].is_number());

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "stall-test-cleanup"
        }),
    )
    .expect("cleanup delivery_stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");
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
            .contains("steer_required=true requires runner_mode=app_server"),
        "expected runner_mode validation error, got: {err}"
    );
}

#[test]
fn delivery_submit_warns_when_interactive_command_is_provided() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-interactive-command-deprecated");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "interactive_command": true,
            "command": "bash",
            "args": ["-lc", "sleep 5"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed with legacy interactive_command");

    let warnings = submit["warnings"]
        .as_array()
        .expect("warnings should be emitted when interactive_command is provided");
    assert!(
        warnings
            .iter()
            .any(|warning| warning
                == "interactive_command is deprecated and ignored; use runner_mode=app_server + steer_required for steerable sessions"),
        "expected deterministic deprecation warning, got: {warnings:?}"
    );

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop cleanup should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");
}

#[test]
fn delivery_submit_does_not_warn_when_interactive_command_is_absent() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-interactive-command-absent");

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
    .expect("delivery_submit should succeed when interactive_command is absent");

    assert!(
        submit.get("warnings").is_none(),
        "warnings should be omitted when interactive_command is not provided: {submit:?}"
    );

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop cleanup should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");
}

#[test]
fn delivery_submit_rejects_tmux_runner_mode_after_removal() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-runner-mode-tmux-unsupported");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "tmux",
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject removed tmux runner mode");

    assert!(
        err.to_string().contains("unsupported runner_mode: tmux"),
        "expected unsupported tmux runner mode validation error, got: {err}"
    );
}

#[test]
fn delivery_submit_rejects_codex_exec_shape_in_process_mode() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-codex-exec-shape-process");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "codex",
            "args": ["exec", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject codex exec command shape for process mode");

    assert!(
        err.to_string()
            .contains("runner_mode=process rejects codex exec/guarded-exec command shape"),
        "expected deterministic process-mode codex exec rejection, got: {err}"
    );
}

#[test]
fn delivery_submit_rejects_process_runner_for_guarded_exec_wrapper() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-process-guarded-exec-rejected");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "command": "scripts/codex-guarded-exec.sh",
            "args": ["--workdir", tmp.path().display().to_string(), "--timeout-sec", "300", "--", "-C", tmp.path().display().to_string(), "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject process-mode guarded exec wrapper");

    assert!(
        err.to_string()
            .contains("runner_mode=process rejects codex exec/guarded-exec command shape"),
        "expected deterministic process-mode guarded-exec rejection, got: {err}"
    );
}

#[test]
fn delivery_submit_rejects_app_server_request_policy_without_app_server_runner() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-policy-invalid-runner");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "process",
            "app_server_request_policy": "allow_safe_subset",
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err(
        "delivery_submit should reject app_server_request_policy without app_server runner",
    );

    assert!(
        err.to_string()
            .contains("invalid app_server_request_policy: requires runner_mode=app_server"),
        "expected app_server_request_policy runner-mode validation error, got: {err}"
    );
}

#[test]
fn delivery_submit_rejects_unsupported_app_server_request_policy() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-policy-unsupported");

    let err = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "app_server_request_policy": "unknown-mode",
            "command": "bash",
            "args": ["-lc", "echo should-not-run"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect_err("delivery_submit should reject unsupported app_server_request_policy");

    assert!(
        err.to_string()
            .contains("unsupported app_server_request_policy: unknown-mode"),
        "expected deterministic unsupported-policy validation error, got: {err}"
    );
}

#[test]
fn delivery_submit_defaults_runner_mode_to_process_when_omitted() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-delivery-default-runner");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "command": "bash",
            "args": ["-lc", "echo default-runner"],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed with default runner");

    assert_eq!(submit["orchestration"]["runner_mode"], "process");
}

#[test]
fn delivery_submit_allows_steer_required_for_app_server_without_interactive_command() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, _request_log_path) =
        write_fake_app_server_script(tmp.path(), "fake-app-server-steer-required.sh", None);
    let task_id = unique_task_id("task-delivery-app-server-steer-required");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "steer_required": true,
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should allow app_server steer_required without interactive_command");
    assert_eq!(submit["orchestration"]["runner_mode"], "app_server");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop cleanup should succeed");
}

#[test]
fn delivery_status_maps_app_server_completed_turn_to_ready_to_merge() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, _request_log_path) = write_fake_app_server_script(
        tmp.path(),
        "fake-app-server-completed.sh",
        Some("completed"),
    );
    let task_id = unique_task_id("task-delivery-app-server-completed");

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    let status = wait_for_runtime_state(&task_id, "ready_to_merge");
    assert_eq!(status["runtime_state"], "ready_to_merge");
}

#[test]
fn delivery_status_maps_app_server_failed_turn_to_failed_retryable() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, _request_log_path) =
        write_fake_app_server_script(tmp.path(), "fake-app-server-failed.sh", Some("failed"));
    let task_id = unique_task_id("task-delivery-app-server-failed");

    let _submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed");

    let status = wait_for_runtime_state(&task_id, "failed_retryable");
    assert_eq!(status["runtime_state"], "failed_retryable");
}

#[test]
fn delivery_submit_app_server_runner_maps_submit_steer_stop_over_stdio_jsonrpc() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) =
        write_fake_app_server_script(tmp.path(), "fake-app-server.sh", None);
    let task_id = unique_task_id("task-delivery-app-server-lifecycle");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");

    assert_eq!(submit["tool"], "delivery_submit");
    assert_eq!(submit["orchestration"]["runner_mode"], "app_server");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let steer = handle_tool_call(
        "delivery_steer",
        json!({
            "task_id": task_id,
            "instruction": "diagnostic-echo"
        }),
    )
    .expect("delivery_steer should succeed for app_server runner");
    assert_eq!(steer["tool"], "delivery_steer");
    assert_eq!(steer["status"], "sent");

    let stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "test-stop"
        }),
    )
    .expect("delivery_stop should succeed for app_server runner");
    assert_eq!(stop["tool"], "delivery_stop");
    assert_eq!(stop["status"], "stopped");

    let status = handle_tool_call(
        "delivery_status",
        json!({
            "task_id": task_id,
            "tail": 20
        }),
    )
    .expect("delivery_status should succeed after stop");
    assert_eq!(status["tool"], "delivery_status");
    assert_eq!(status["runtime_state"], "failed_retryable");

    let request_log =
        fs::read_to_string(&request_log_path).expect("fake app server request log should exist");
    let requests = request_log
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).expect("line should be valid json")
        })
        .collect::<Vec<_>>();
    assert!(
        requests
            .iter()
            .any(|request| request["method"] == "initialize"),
        "request log should include initialize call: {request_log}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request["method"] == "initialized"),
        "request log should include initialized notification: {request_log}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request["method"] == "thread/start"),
        "request log should include thread/start call: {request_log}"
    );
    let turn_start = requests
        .iter()
        .find(|request| request["method"] == "turn/start")
        .expect("request log should include turn/start");
    assert_eq!(turn_start["params"]["threadId"], "thread-test");
    assert_eq!(
        turn_start["params"]["input"][0]["text"],
        format!("start task {task_id}")
    );

    let turn_steer = requests
        .iter()
        .find(|request| request["method"] == "turn/steer")
        .expect("request log should include turn/steer");
    assert_eq!(turn_steer["params"]["threadId"], "thread-test");
    assert_eq!(turn_steer["params"]["input"][0]["text"], "diagnostic-echo");
    assert!(turn_steer["params"]["expectedTurnId"].is_string());

    let turn_interrupt = requests
        .iter()
        .find(|request| request["method"] == "turn/interrupt")
        .expect("request log should include turn/interrupt");
    assert_eq!(turn_interrupt["params"]["threadId"], "thread-test");
    assert!(turn_interrupt["params"]["turnId"].is_string());
}

#[test]
fn delivery_steer_app_server_falls_back_to_turn_start_when_no_active_turn_error_is_returned() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_steer_requires_turn_start_script(
        tmp.path(),
        "fake-app-server-steer-fallback.sh",
    );
    let task_id = unique_task_id("task-delivery-app-server-steer-fallback");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let steer = handle_tool_call(
        "delivery_steer",
        json!({
            "task_id": task_id,
            "instruction": "fallback-turn-start-instruction"
        }),
    )
    .expect("delivery_steer should succeed via turn/start fallback");
    assert_eq!(steer["status"], "sent");

    let request_log =
        fs::read_to_string(&request_log_path).expect("fake app server request log should exist");
    let requests = request_log
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).expect("line should be valid json")
        })
        .collect::<Vec<_>>();
    let turn_start_calls = requests
        .iter()
        .filter(|request| request["method"] == "turn/start")
        .collect::<Vec<_>>();
    assert!(
        turn_start_calls.len() >= 2,
        "expected startup turn/start and fallback turn/start calls: {request_log}"
    );
    let fallback_turn_start = turn_start_calls
        .last()
        .expect("fallback turn/start request should exist");
    assert_eq!(
        fallback_turn_start["params"]["input"][0]["text"],
        "fallback-turn-start-instruction"
    );

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_approval_request_defaults_to_deny_all_policy() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request.sh",
        r#"{"id":"approval-1","method":"approval/request","params":{"command":"git status","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!("approval-1"));
    assert_eq!(response["result"], "decline");

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_approval_request_allows_safe_subset_policy() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-allow.sh",
        r#"{"id":778,"method":"approval/request","params":{"command":"git status","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-allow");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "app_server_request_policy": "allow_safe_subset",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(778));
    assert_eq!(response["result"], "accept");

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_approval_request_denies_unsafe_safe_subset_command() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-deny-unsafe.sh",
        r#"{"id":781,"method":"approval/request","params":{"command":"git status & whoami","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-deny-unsafe");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "app_server_request_policy": "allow_safe_subset",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(781));
    assert_eq!(response["result"], "decline");

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_handles_command_execution_request_approval_method() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-command-approval.sh",
        r#"{"id":782,"method":"item/commandExecution/requestApproval","params":{"command":"git status","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-command-approval");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "app_server_request_policy": "allow_safe_subset",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(782));
    assert_eq!(response["result"], "accept");

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_handles_suffix_request_approval_method() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-suffix-approval.sh",
        r#"{"id":783,"method":"item/custom/requestApproval","params":{"command":"git status","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-suffix-approval");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(783));
    assert_eq!(response["result"], "decline");

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_returns_error_for_unsupported_server_request_method() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-unsupported.sh",
        r#"{"id":779,"method":"input/request","params":{"prompt":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-unsupported");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(779));
    assert_eq!(response["error"]["code"], -32601);
    assert_eq!(
        response["error"]["message"],
        "unsupported app_server server request method: input/request"
    );

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
}

#[test]
fn delivery_submit_app_server_delegate_fail_open_policy_is_disabled() {
    let tmp = tempdir().expect("tempdir");
    let (script_path, request_log_path) = write_fake_app_server_with_server_request_script(
        tmp.path(),
        "fake-app-server-server-request-delegate.sh",
        r#"{"id":784,"method":"approval/request","params":{"command":"git status","reason":"test-server-request"}}"#,
    );
    let task_id = unique_task_id("task-delivery-app-server-server-request-delegate");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "app_server_request_policy": "delegate_fail_open",
            "command": script_path.display().to_string(),
            "args": [],
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed for app_server runner");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let response = wait_for_server_response_line(&request_log_path, &json!(784));
    assert_eq!(response["error"]["code"], -32050);
    assert_eq!(
        response["error"]["message"],
        "app_server request policy delegate_fail_open is disabled"
    );

    let _stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "cleanup"
        }),
    )
    .expect("delivery_stop should succeed");
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
    assert!(status["normalized"]["trace_id"].is_null());
    assert!(status["normalized"]["span_id"].is_null());
    assert!(status["normalized"]["parent_span_id"].is_null());
    assert!(status["normalized"]["assigned_worker"].is_null());
    assert!(status["normalized"]["retry_count"].is_null());
    assert!(status["normalized"]["retry_budget"].is_null());
    assert!(status["normalized"]["last_steer_sent_at"].is_null());
    assert!(status["normalized"]["last_steer_observed_at"].is_null());
    assert!(status["normalized"]["last_steer_apply_hint"].is_null());
    assert!(status["normalized"]["last_output_at"].is_null());
    assert!(status["normalized"]["stall_duration_ms"].is_null());
    assert!(status["normalized"]["stall_reason"].is_null());
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
fn delivery_timeline_and_explain_failure_return_stable_payloads_for_unknown_task() {
    let task_id = unique_task_id("task-timeline-empty");

    let timeline = handle_tool_call(
        "delivery_timeline",
        json!({
            "task_id": task_id,
            "limit": 5
        }),
    )
    .expect("delivery_timeline should succeed for unknown task");

    assert_eq!(timeline["tool"], "delivery_timeline");
    assert_eq!(timeline["task_id"], json!(task_id));
    assert_eq!(timeline["cursor"], json!(0));
    assert_eq!(timeline["next_cursor"], json!(0));
    assert_eq!(timeline["has_more"], json!(false));
    assert_eq!(
        timeline["events"]
            .as_array()
            .expect("events should be array")
            .len(),
        0
    );

    let resumed_timeline = handle_tool_call(
        "delivery_timeline",
        json!({
            "task_id": task_id,
            "cursor": 99,
            "limit": 5
        }),
    )
    .expect("delivery_timeline should keep empty timeline responses stable for non-zero cursor");
    assert_eq!(resumed_timeline["cursor"], json!(99));
    assert_eq!(resumed_timeline["next_cursor"], json!(99));
    assert_eq!(resumed_timeline["has_more"], json!(false));
    assert_eq!(
        resumed_timeline["events"]
            .as_array()
            .expect("events should be array")
            .len(),
        0
    );

    let explain = handle_tool_call(
        "delivery_explain_failure",
        json!({
            "task_id": task_id
        }),
    )
    .expect("delivery_explain_failure should succeed for unknown task");

    assert_eq!(explain["tool"], "delivery_explain_failure");
    assert_eq!(explain["task_id"], json!(task_id));
    assert_eq!(explain["runtime_state"], "unknown");
    assert_eq!(explain["failed"], json!(false));
    assert!(explain["summary"].is_string());
    assert!(explain["latest_failure_event"].is_null());
    assert_eq!(
        explain["state_transitions"]
            .as_array()
            .expect("state_transitions should be array")
            .len(),
        0
    );
    assert_eq!(explain["timeline_cursor"], json!(0));
}

#[test]
fn delivery_timeline_paginates_stably_by_event_cursor() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-timeline-pagination");

    let _submit = handle_tool_call(
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
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "timeline-pagination"
        }),
    )
    .expect("delivery_stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let first_page = handle_tool_call(
        "delivery_timeline",
        json!({
            "task_id": task_id,
            "limit": 2
        }),
    )
    .expect("delivery_timeline first page should succeed");
    let first_events = first_page["events"]
        .as_array()
        .expect("events should be array");
    assert_eq!(first_events.len(), 2, "expected exact first page size");

    let first_ids = first_events
        .iter()
        .map(|event| {
            event["id"]
                .as_i64()
                .expect("timeline event id should be integer")
        })
        .collect::<Vec<_>>();
    assert!(
        first_ids.windows(2).all(|window| window[0] < window[1]),
        "first page should be sorted by id ascending: {first_ids:?}"
    );

    let next_cursor = first_page["next_cursor"]
        .as_u64()
        .expect("next_cursor should be u64");
    let second_page = handle_tool_call(
        "delivery_timeline",
        json!({
            "task_id": task_id,
            "cursor": next_cursor,
            "limit": 50
        }),
    )
    .expect("delivery_timeline second page should succeed");
    let second_events = second_page["events"]
        .as_array()
        .expect("events should be array");
    assert!(
        second_events
            .iter()
            .all(|event| event["cursor"].as_u64().expect("cursor should be u64") > next_cursor),
        "second page should only include events after cursor {next_cursor}: {second_events:?}"
    );
}

#[test]
fn delivery_explain_failure_references_latest_failure_event() {
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-explain-failure");

    let _submit = handle_tool_call(
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
    let _ = wait_for_runtime_state(&task_id, "running");

    let _ = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "explain-failure"
        }),
    )
    .expect("delivery_stop should succeed");
    let _ = wait_for_runtime_state(&task_id, "failed_retryable");

    let explain = handle_tool_call(
        "delivery_explain_failure",
        json!({
            "task_id": task_id
        }),
    )
    .expect("delivery_explain_failure should succeed");

    assert_eq!(explain["tool"], "delivery_explain_failure");
    assert_eq!(explain["runtime_state"], "failed_retryable");
    assert_eq!(explain["failed"], json!(true));
    assert!(
        explain["summary"]
            .as_str()
            .expect("summary should be string")
            .contains("runtime_state=failed_retryable"),
        "summary should mention runtime state: {}",
        explain["summary"]
    );
    assert_eq!(explain["latest_failure_event"]["task_id"], json!(task_id));
    assert!(
        explain["latest_failure_event"]["id"]
            .as_i64()
            .expect("latest failure event id should be integer")
            > 0
    );
    assert!(
        !explain["state_transitions"]
            .as_array()
            .expect("state_transitions should be array")
            .is_empty(),
        "state_transitions should include failure-related transitions"
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
#[ignore = "opt-in local real CLI E2E harness; set AGENTRAIL_E2E_REAL_CLI=1"]
fn delivery_submit_app_server_real_cli_e2e_happy_path_with_artifacts() {
    let config = load_real_cli_e2e_config();
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-e2e-real-cli-happy");

    let submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "steer_required": true,
            "command": config.command,
            "args": config.args,
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("delivery_submit should succeed with real local app_server command");
    assert_eq!(submit["tool"], "delivery_submit");
    assert_eq!(submit["orchestration"]["status"], "accepted");

    let running_status = wait_for_runtime_state_any(&task_id, &["running"]);
    assert_eq!(running_status["runtime_state"], "running");

    let steer = handle_tool_call(
        "delivery_steer",
        json!({
            "task_id": task_id,
            "instruction": config.steer_instruction
        }),
    )
    .expect("delivery_steer should succeed for active real CLI session");
    assert_eq!(steer["tool"], "delivery_steer");
    assert_eq!(steer["status"], "sent");

    capture_task_artifacts(&task_id, &config, "happy");

    let stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "e2e-happy-cleanup"
        }),
    )
    .expect("delivery_stop should succeed for happy-path cleanup");
    assert!(
        stop["status"] == "stopped" || stop["status"] == "already_stopped",
        "unexpected stop status: {stop}"
    );
}

#[test]
#[ignore = "opt-in local real CLI E2E harness; set AGENTRAIL_E2E_REAL_CLI=1"]
fn delivery_submit_app_server_real_cli_e2e_failure_retry_with_artifacts() {
    let config = load_real_cli_e2e_config();
    let tmp = tempdir().expect("tempdir");
    let task_id = unique_task_id("task-e2e-real-cli-failure");

    let first_submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": config.command,
            "args": config.args,
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("initial delivery_submit should succeed for failure/retry path");
    assert_eq!(first_submit["orchestration"]["status"], "accepted");

    let stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "e2e-controlled-failure"
        }),
    )
    .expect("delivery_stop should succeed to induce controlled failure");
    assert!(
        stop["status"] == "stopped" || stop["status"] == "already_stopped",
        "unexpected controlled-failure stop status: {stop}"
    );
    assert_eq!(stop["runtime_state"], "failed_retryable");

    let failed_status = wait_for_runtime_state_any(&task_id, &["failed_retryable"]);
    assert_eq!(failed_status["runtime_state"], "failed_retryable");
    capture_task_artifacts(&task_id, &config, "failure-before-retry");

    let retry_submit = handle_tool_call(
        "delivery_submit",
        json!({
            "task_id": task_id,
            "worker_id": "worker-a",
            "runner_mode": "app_server",
            "command": config.command,
            "args": config.args,
            "workdir": tmp.path().display().to_string()
        }),
    )
    .expect("retry delivery_submit should succeed from failed_retryable");
    assert_eq!(retry_submit["orchestration"]["status"], "accepted");
    assert!(
        retry_submit["orchestration"]["attempt_number"]
            .as_u64()
            .expect("attempt_number should be u64")
            >= 2,
        "retry attempt should increment attempt_number: {retry_submit}"
    );

    let _ =
        wait_for_runtime_state_any(&task_id, &["running", "ready_to_merge", "failed_retryable"]);
    capture_task_artifacts(&task_id, &config, "failure-after-retry");

    let cleanup_stop = handle_tool_call(
        "delivery_stop",
        json!({
            "task_id": task_id,
            "reason": "e2e-retry-cleanup"
        }),
    )
    .expect("cleanup delivery_stop should succeed after retry");
    assert!(
        cleanup_stop["status"] == "stopped" || cleanup_stop["status"] == "already_stopped",
        "unexpected cleanup stop status: {cleanup_stop}"
    );
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
