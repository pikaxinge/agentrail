use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tracing::{info, warn};

use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus};
use agentrail_runner::{AgentRunner, ProcessRunner, TaskHandle, TaskSpec, TaskStatus, TmuxRunner};
use agentrail_store::{TaskRecord, TaskRuntimeState, TaskStore, TaskStoreSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeRunnerMode {
    Process,
    Tmux,
}

impl RuntimeRunnerMode {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("process") {
            "process" => Ok(Self::Process),
            "tmux" => Ok(Self::Tmux),
            other => anyhow::bail!("unsupported runner_mode: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Tmux => "tmux",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupRetentionMode {
    Purge,
    Retain,
}

impl CleanupRetentionMode {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("purge") {
            "purge" => Ok(Self::Purge),
            "retain" => Ok(Self::Retain),
            other => anyhow::bail!("unsupported retention_mode: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Purge => "purge",
            Self::Retain => "retain",
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeTaskSession {
    session_id: String,
    runner_mode: RuntimeRunnerMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryEvent {
    cursor: u64,
    task_id: String,
    event_type: String,
    timestamp: u64,
    state: String,
    runtime_state: String,
    session_id: Option<String>,
}

impl DeliveryEvent {
    fn as_json(&self) -> Value {
        json!({
            "cursor": self.cursor,
            "task_id": self.task_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "state": self.state,
            "runtime_state": self.runtime_state,
            "session_id": self.session_id
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryEventSignature {
    event_type: String,
    state: String,
    runtime_state: String,
    session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryStatusObservation {
    state: String,
    runtime_state: String,
    session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliverySteerObservation {
    sent_at: u64,
    observed_at: Option<u64>,
    apply_hint: String,
}

#[derive(Debug, Clone)]
struct DeliverySubscription {
    cursor: u64,
    task_id: Option<String>,
}

#[derive(Debug)]
struct RuntimeState {
    store: TaskStore,
    sessions: HashMap<String, RuntimeTaskSession>,
    delivery_events: Vec<DeliveryEvent>,
    next_delivery_cursor: u64,
    delivery_subscriptions: HashMap<String, DeliverySubscription>,
    last_event_signature_by_task: HashMap<String, DeliveryEventSignature>,
    last_status_observation_by_task: HashMap<String, DeliveryStatusObservation>,
    last_steer_observation_by_task: HashMap<String, DeliverySteerObservation>,
}

#[derive(Debug, Clone)]
struct RuntimeCleanupTarget {
    task_id: String,
    record: Option<TaskRecord>,
    session: Option<RuntimeTaskSession>,
}

static RUNTIME_STATE: OnceLock<Mutex<RuntimeState>> = OnceLock::new();
static DELIVERY_SUBSCRIBER_SEQ: AtomicU64 = AtomicU64::new(1);

const DEFAULT_RUNTIME_STORE_DSN: &str = "sqlite://.agentrail/runtime.db";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeStoreSource {
    Default,
    EnvOverride,
}

impl RuntimeStoreSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::EnvOverride => "env_override",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeStoreMode {
    SqliteFileBacked,
    Memory,
    Unsupported,
}

impl RuntimeStoreMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::SqliteFileBacked => "sqlite_file_backed",
            Self::Memory => "memory",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeStoreConfig {
    dsn: String,
    source: RuntimeStoreSource,
    mode: RuntimeStoreMode,
}

fn runtime_store_mode(dsn: &str) -> RuntimeStoreMode {
    if dsn.starts_with("sqlite://") {
        RuntimeStoreMode::SqliteFileBacked
    } else if dsn.starts_with("memory://") {
        RuntimeStoreMode::Memory
    } else {
        RuntimeStoreMode::Unsupported
    }
}

#[cfg(test)]
fn resolve_runtime_store_config_with_default_path(
    env_dsn: Option<String>,
    default_sqlite_path: &Path,
) -> RuntimeStoreConfig {
    if let Some(override_dsn) = env_dsn
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return RuntimeStoreConfig {
            dsn: override_dsn.to_string(),
            source: RuntimeStoreSource::EnvOverride,
            mode: runtime_store_mode(override_dsn),
        };
    }

    let default_path = default_sqlite_path.display().to_string();
    let dsn = format!("sqlite://{default_path}");
    RuntimeStoreConfig {
        dsn,
        source: RuntimeStoreSource::Default,
        mode: RuntimeStoreMode::SqliteFileBacked,
    }
}

fn resolve_runtime_store_config(env_dsn: Option<String>) -> RuntimeStoreConfig {
    if let Some(override_dsn) = env_dsn
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return RuntimeStoreConfig {
            dsn: override_dsn.to_string(),
            source: RuntimeStoreSource::EnvOverride,
            mode: runtime_store_mode(override_dsn),
        };
    }

    RuntimeStoreConfig {
        dsn: DEFAULT_RUNTIME_STORE_DSN.to_string(),
        source: RuntimeStoreSource::Default,
        mode: RuntimeStoreMode::SqliteFileBacked,
    }
}

fn runtime_state() -> &'static Mutex<RuntimeState> {
    RUNTIME_STATE.get_or_init(|| {
        let config = resolve_runtime_store_config(std::env::var("AGENTRAIL_RUNTIME_DSN").ok());
        info!(
            operation = "runtime_store_bootstrap",
            runtime_store_dsn = %config.dsn,
            runtime_store_source = config.source.as_str(),
            runtime_store_mode = config.mode.as_str(),
            "initialized runtime task store"
        );
        Mutex::new(RuntimeState {
            store: TaskStore::connect(config.dsn),
            sessions: HashMap::new(),
            delivery_events: Vec::new(),
            next_delivery_cursor: 1,
            delivery_subscriptions: HashMap::new(),
            last_event_signature_by_task: HashMap::new(),
            last_status_observation_by_task: HashMap::new(),
            last_steer_observation_by_task: HashMap::new(),
        })
    })
}

fn runtime_state_label(state: TaskRuntimeState) -> &'static str {
    match state {
        TaskRuntimeState::Queued => "queued",
        TaskRuntimeState::Preparing => "preparing",
        TaskRuntimeState::Running => "running",
        TaskRuntimeState::ReviewFailed => "review_failed",
        TaskRuntimeState::Fixing => "fixing",
        TaskRuntimeState::Validating => "validating",
        TaskRuntimeState::ReadyToMerge => "ready_to_merge",
        TaskRuntimeState::Merged => "merged",
        TaskRuntimeState::FailedRetryable => "failed_retryable",
        TaskRuntimeState::FailedTerminal => "failed_terminal",
        TaskRuntimeState::NeedsAttention => "needs_attention",
    }
}

fn delivery_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn next_subscriber_id() -> String {
    let seq = DELIVERY_SUBSCRIBER_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("delivery-sub-{seq}")
}

fn runtime_state_mutex() -> Result<std::sync::MutexGuard<'static, RuntimeState>> {
    runtime_state()
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime state lock poisoned"))
}

fn reset_delivery_tracking(runtime: &mut RuntimeState, task_id: &str) {
    runtime.last_event_signature_by_task.remove(task_id);
    runtime.last_status_observation_by_task.remove(task_id);
    runtime.last_steer_observation_by_task.remove(task_id);
}

fn emit_delivery_event(
    runtime: &mut RuntimeState,
    task_id: &str,
    event_type: &str,
    state: &str,
    runtime_state: &str,
    session_id: Option<String>,
) {
    let signature = DeliveryEventSignature {
        event_type: event_type.to_string(),
        state: state.to_string(),
        runtime_state: runtime_state.to_string(),
        session_id: session_id.clone(),
    };
    if runtime
        .last_event_signature_by_task
        .get(task_id)
        .is_some_and(|existing| *existing == signature)
    {
        return;
    }

    let cursor = runtime.next_delivery_cursor;
    runtime.next_delivery_cursor = runtime.next_delivery_cursor.saturating_add(1);
    runtime.delivery_events.push(DeliveryEvent {
        cursor,
        task_id: task_id.to_string(),
        event_type: event_type.to_string(),
        timestamp: delivery_timestamp_ms(),
        state: state.to_string(),
        runtime_state: runtime_state.to_string(),
        session_id,
    });
    runtime
        .last_event_signature_by_task
        .insert(task_id.to_string(), signature);
}

fn emit_delivery_status_events(
    task_id: &str,
    state: &str,
    runtime_state: &str,
    session_id: Option<String>,
) -> Result<()> {
    let mut runtime = runtime_state_mutex()?;
    let observation = DeliveryStatusObservation {
        state: state.to_string(),
        runtime_state: runtime_state.to_string(),
        session_id: session_id.clone(),
    };
    let changed = runtime
        .last_status_observation_by_task
        .get(task_id)
        .is_none_or(|existing| *existing != observation);

    if changed {
        emit_delivery_event(
            &mut runtime,
            task_id,
            "status_changed",
            state,
            runtime_state,
            session_id.clone(),
        );
        runtime
            .last_status_observation_by_task
            .insert(task_id.to_string(), observation);
    }

    if matches!(state, "completed" | "failed" | "stopped") {
        emit_delivery_event(
            &mut runtime,
            task_id,
            state,
            state,
            runtime_state,
            session_id,
        );
    }

    Ok(())
}

fn record_delivery_steer_sent(task_id: &str, session_id: &str) -> Result<u64> {
    let sent_at = delivery_timestamp_ms();
    let mut runtime = runtime_state_mutex()?;
    let (state, runtime_state) = if let Some(task) = runtime.store.get_task(task_id)? {
        let state = runtime_state_label(task.state).to_string();
        (state.clone(), state)
    } else {
        ("unknown".to_string(), "unknown".to_string())
    };

    emit_delivery_event(
        &mut runtime,
        task_id,
        "steer_sent",
        &state,
        &runtime_state,
        Some(session_id.to_string()),
    );
    runtime.last_steer_observation_by_task.insert(
        task_id.to_string(),
        DeliverySteerObservation {
            sent_at,
            observed_at: None,
            apply_hint: "transport_sent".to_string(),
        },
    );

    Ok(sent_at)
}

fn maybe_record_delivery_steer_observed(
    task_id: &str,
    session_id: &str,
    state: &str,
    runtime_state: &str,
) -> Result<()> {
    let mut runtime = runtime_state_mutex()?;
    let Some(existing) = runtime.last_steer_observation_by_task.get(task_id).cloned() else {
        return Ok(());
    };
    if existing.observed_at.is_some() {
        return Ok(());
    }

    let observed_at = delivery_timestamp_ms();
    runtime.last_steer_observation_by_task.insert(
        task_id.to_string(),
        DeliverySteerObservation {
            sent_at: existing.sent_at,
            observed_at: Some(observed_at),
            apply_hint: "session_alive_after_steer".to_string(),
        },
    );
    emit_delivery_event(
        &mut runtime,
        task_id,
        "steer_observed",
        state,
        runtime_state,
        Some(session_id.to_string()),
    );

    Ok(())
}

fn runtime_state_from_label(label: &str) -> Result<TaskRuntimeState> {
    match label {
        "queued" => Ok(TaskRuntimeState::Queued),
        "preparing" => Ok(TaskRuntimeState::Preparing),
        "running" => Ok(TaskRuntimeState::Running),
        "review_failed" => Ok(TaskRuntimeState::ReviewFailed),
        "fixing" => Ok(TaskRuntimeState::Fixing),
        "validating" => Ok(TaskRuntimeState::Validating),
        "ready_to_merge" => Ok(TaskRuntimeState::ReadyToMerge),
        "merged" => Ok(TaskRuntimeState::Merged),
        "failed_retryable" => Ok(TaskRuntimeState::FailedRetryable),
        "failed_terminal" => Ok(TaskRuntimeState::FailedTerminal),
        "needs_attention" => Ok(TaskRuntimeState::NeedsAttention),
        _ => anyhow::bail!("invalid runtime state label: {label}"),
    }
}

fn optional_string(args: &Value, field: &str) -> Option<String> {
    args.get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn validate_delivery_cursor_bounds(
    tool_name: &str,
    cursor_field: &str,
    requested_cursor: u64,
    latest_cursor: u64,
) -> Result<()> {
    if requested_cursor > latest_cursor {
        anyhow::bail!(
            "invalid {cursor_field} for {tool_name}: {requested_cursor} is ahead of latest cursor {latest_cursor}"
        );
    }
    Ok(())
}

fn parse_optional_u64(args: &Value, field: &str) -> Result<Option<u64>> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };

    if let Some(raw) = value.as_u64() {
        return Ok(Some(raw));
    }
    if let Some(raw) = value.as_str() {
        let parsed = raw
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid {field}: must be unsigned integer"))?;
        return Ok(Some(parsed));
    }

    anyhow::bail!("invalid {field}: must be unsigned integer")
}

fn require_u64_field(tool_name: &str, args: &Value, field: &str) -> Result<u64> {
    parse_optional_u64(args, field)?
        .ok_or_else(|| anyhow::anyhow!("missing {field}"))
        .inspect(|_| {
            info!(
                operation = "mcp_tool_call",
                tool = tool_name,
                field = field,
                outcome = "validated",
                "validated MCP tool call arguments"
            );
        })
}

fn optional_u32(args: &Value, field: &str, default: u32) -> Result<u32> {
    match args.get(field) {
        None => Ok(default),
        Some(value) => {
            let raw = value
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("invalid {field}: must be unsigned integer"))?;
            Ok(u32::try_from(raw)
                .map_err(|_| anyhow::anyhow!("invalid {field}: out of range for u32"))?)
        }
    }
}

fn optional_bool(args: &Value, field: &str, default: bool) -> Result<bool> {
    match args.get(field) {
        None => Ok(default),
        Some(value) => value
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("invalid {field}: must be boolean")),
    }
}

fn optional_u64(args: &Value, field: &str) -> Result<Option<u64>> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    let raw = value
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("invalid {field}: must be unsigned integer"))?;
    Ok(Some(raw))
}

fn optional_i64(args: &Value, field: &str) -> Result<Option<i64>> {
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    if let Some(raw) = value.as_i64() {
        return Ok(Some(raw));
    }
    if let Some(raw) = value.as_u64() {
        return Ok(Some(i64::try_from(raw).map_err(|_| {
            anyhow::anyhow!("invalid {field}: out of range for i64")
        })?));
    }
    anyhow::bail!("invalid {field}: must be integer");
}

fn optional_string_array(args: &Value, field: &str) -> Result<Vec<String>> {
    let Some(value) = args.get(field) else {
        return Ok(Vec::new());
    };
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid {field}: must be string array"))?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("invalid {field}: array must contain strings"))
        })
        .collect()
}

fn now_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn delivery_logs_truncated(orchestration: &Value, tail: u32) -> bool {
    if tail == 0 {
        return false;
    }

    let Some(logs) = orchestration.get("logs").and_then(Value::as_str) else {
        return false;
    };

    let line_count = if logs.is_empty() {
        0
    } else {
        logs.lines().count()
    };
    line_count >= tail as usize
}

fn delivery_status_normalized_v1(orchestration: &Value, tail: u32) -> Value {
    let field = |name: &str| orchestration.get(name).cloned().unwrap_or(Value::Null);

    json!({
        "tool": "delivery_status",
        "task_id": field("task_id"),
        "state": field("state"),
        "runtime_state": field("runtime_state"),
        "runner_mode": field("runner_mode"),
        "session_id": field("session_id"),
        "assigned_worker": field("assigned_worker"),
        "retry_count": field("retry_count"),
        "retry_budget": field("retry_budget"),
        "last_steer_sent_at": field("last_steer_sent_at"),
        "last_steer_observed_at": field("last_steer_observed_at"),
        "last_steer_apply_hint": field("last_steer_apply_hint"),
        "timestamps": {
            "updated_at": now_unix_timestamp_ms()
        },
        "logs": {
            "tail": tail,
            "truncated": delivery_logs_truncated(orchestration, tail)
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReportCursor {
    updated_epoch_ms: i64,
    task_id: String,
}

fn encode_report_cursor(cursor: &ReportCursor) -> String {
    json!({
        "updated_epoch_ms": cursor.updated_epoch_ms,
        "task_id": cursor.task_id
    })
    .to_string()
}

fn decode_report_cursor(raw: &str) -> Result<ReportCursor> {
    let parsed: Value = serde_json::from_str(raw)
        .map_err(|error| anyhow::anyhow!("invalid cursor: expected JSON object ({error})"))?;
    let updated_epoch_ms = parsed
        .get("updated_epoch_ms")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("invalid cursor: missing updated_epoch_ms"))?;
    let task_id = parsed
        .get("task_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("invalid cursor: missing task_id"))?
        .to_string();
    Ok(ReportCursor {
        updated_epoch_ms,
        task_id,
    })
}

fn block_on_result<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return tokio::task::block_in_place(|| handle.block_on(future));
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(future)
}

fn resolve_start_command_and_args(args: &Value) -> Result<(String, Vec<String>)> {
    let command = optional_string(args, "command");
    let command_defaulted = command.is_none();
    let command = command.unwrap_or_else(|| "bash".to_string());
    let parsed_args = optional_string_array(args, "args")?;
    let command_args = if command_defaulted && parsed_args.is_empty() {
        vec!["-lc".to_string(), "true".to_string()]
    } else {
        parsed_args
    };

    Ok((command, command_args))
}

fn validate_delivery_submit_preflight(args: &Value) -> Result<()> {
    let steer_required = optional_bool(args, "steer_required", false)?;
    if !steer_required {
        return Ok(());
    }

    let runner_mode = RuntimeRunnerMode::parse(args.get("runner_mode").and_then(Value::as_str))?;
    if runner_mode != RuntimeRunnerMode::Tmux {
        anyhow::bail!("invalid delivery_submit: steer_required=true requires runner_mode=tmux");
    }

    let interactive_command = optional_bool(args, "interactive_command", false)?;
    if !interactive_command {
        anyhow::bail!(
            "invalid delivery_submit: steer_required=true requires interactive_command=true"
        );
    }

    Ok(())
}

async fn runner_start(mode: RuntimeRunnerMode, spec: TaskSpec) -> Result<TaskHandle> {
    match mode {
        RuntimeRunnerMode::Process => ProcessRunner.start(spec).await,
        RuntimeRunnerMode::Tmux => TmuxRunner.start(spec).await,
    }
}

async fn runner_status(mode: RuntimeRunnerMode, session_id: &str) -> Result<TaskStatus> {
    match mode {
        RuntimeRunnerMode::Process => ProcessRunner.status(session_id).await,
        RuntimeRunnerMode::Tmux => TmuxRunner.status(session_id).await,
    }
}

async fn runner_steer(mode: RuntimeRunnerMode, session_id: &str, instruction: &str) -> Result<()> {
    match mode {
        RuntimeRunnerMode::Process => ProcessRunner.steer(session_id, instruction).await,
        RuntimeRunnerMode::Tmux => TmuxRunner.steer(session_id, instruction).await,
    }
}

async fn runner_logs(mode: RuntimeRunnerMode, session_id: &str, tail: usize) -> Result<String> {
    match mode {
        RuntimeRunnerMode::Process => ProcessRunner.logs(session_id, tail).await,
        RuntimeRunnerMode::Tmux => TmuxRunner.logs(session_id, tail).await,
    }
}

async fn runner_stop(mode: RuntimeRunnerMode, session_id: &str) -> Result<()> {
    match mode {
        RuntimeRunnerMode::Process => ProcessRunner.stop(session_id).await,
        RuntimeRunnerMode::Tmux => TmuxRunner.stop(session_id).await,
    }
}

fn map_runner_to_runtime_state(state: &str) -> Option<TaskRuntimeState> {
    match state {
        "running" => Some(TaskRuntimeState::Running),
        "completed" => Some(TaskRuntimeState::ReadyToMerge),
        "failed" | "stopped" => Some(TaskRuntimeState::FailedRetryable),
        _ => None,
    }
}

fn runner_state_is_terminal(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "stopped")
}

fn runtime_state_is_terminal(state: TaskRuntimeState) -> bool {
    matches!(
        state,
        TaskRuntimeState::Merged | TaskRuntimeState::FailedTerminal
    )
}

fn runtime_state_is_active(state: TaskRuntimeState) -> bool {
    matches!(
        state,
        TaskRuntimeState::Preparing | TaskRuntimeState::Running
    )
}

fn runtime_state_is_finished_or_abandoned(state: TaskRuntimeState) -> bool {
    matches!(
        state,
        TaskRuntimeState::ReadyToMerge
            | TaskRuntimeState::Merged
            | TaskRuntimeState::FailedRetryable
            | TaskRuntimeState::FailedTerminal
            | TaskRuntimeState::NeedsAttention
    )
}

fn can_transition_to_failed_retryable(state: TaskRuntimeState) -> bool {
    matches!(
        state,
        TaskRuntimeState::Preparing
            | TaskRuntimeState::Running
            | TaskRuntimeState::ReviewFailed
            | TaskRuntimeState::Fixing
            | TaskRuntimeState::Validating
            | TaskRuntimeState::ReadyToMerge
    )
}

fn can_transition_to_failed_terminal(state: TaskRuntimeState) -> bool {
    matches!(
        state,
        TaskRuntimeState::Queued
            | TaskRuntimeState::Preparing
            | TaskRuntimeState::Running
            | TaskRuntimeState::ReviewFailed
            | TaskRuntimeState::Fixing
            | TaskRuntimeState::Validating
            | TaskRuntimeState::ReadyToMerge
            | TaskRuntimeState::FailedRetryable
            | TaskRuntimeState::NeedsAttention
    )
}

fn is_session_not_found_error(error: &anyhow::Error) -> bool {
    error.to_string().contains("session not found")
}

fn clear_runtime_session(task_id: &str) -> Result<()> {
    let mut runtime = runtime_state_mutex()?;
    runtime.sessions.remove(task_id);
    Ok(())
}

fn ensure_task_preparing(
    task_id: &str,
    worker_id: &str,
    retry_budget: u32,
    scope_id: Option<&str>,
) -> Result<()> {
    let mut runtime = runtime_state_mutex()?;

    match runtime.store.get_task(task_id)? {
        None => {
            let mut task = TaskRecord::new(task_id, worker_id, retry_budget);
            task.scope_id = scope_id.map(ToString::to_string);
            runtime.store.upsert_task(&task)?;
        }
        Some(existing) => match existing.state {
            TaskRuntimeState::Preparing | TaskRuntimeState::Running => {
                anyhow::bail!("task already active: {task_id}");
            }
            TaskRuntimeState::Queued => {
                runtime
                    .store
                    .reassign_worker(task_id, worker_id.to_string())?;
                if let Some(scope_id) = scope_id {
                    runtime
                        .store
                        .set_scope(task_id, Some(scope_id.to_string()))?;
                }
            }
            TaskRuntimeState::FailedRetryable | TaskRuntimeState::NeedsAttention => {
                runtime
                    .store
                    .reassign_worker(task_id, worker_id.to_string())?;
                if let Some(scope_id) = scope_id {
                    runtime
                        .store
                        .set_scope(task_id, Some(scope_id.to_string()))?;
                }
                runtime
                    .store
                    .transition(task_id, TaskRuntimeState::Queued)?;
            }
            _ => {
                anyhow::bail!(
                    "invalid runtime state for orchestrate_start: {task_id} cannot start from {}",
                    runtime_state_label(existing.state)
                );
            }
        },
    }

    let latest = runtime
        .store
        .get_task(task_id)?
        .ok_or_else(|| anyhow::anyhow!("task not found in runtime store: {task_id}"))?;
    if latest.state != TaskRuntimeState::Queued {
        anyhow::bail!(
            "invalid runtime state contract for orchestrate_start: expected queued before launch, found {}",
            runtime_state_label(latest.state)
        );
    }
    runtime
        .store
        .transition(task_id, TaskRuntimeState::Preparing)?;
    reset_delivery_tracking(&mut runtime, task_id);
    Ok(())
}

fn mark_task_start_failed(task_id: &str) -> Result<()> {
    let runtime = runtime_state()
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime state lock poisoned"))?;
    let existing = runtime.store.get_task(task_id)?;
    if let Some(task) = existing
        && task.state == TaskRuntimeState::Preparing
    {
        runtime
            .store
            .transition(task_id, TaskRuntimeState::FailedRetryable)?;
    }
    Ok(())
}

fn register_running_session(
    task_id: &str,
    runner_mode: RuntimeRunnerMode,
    session_id: String,
) -> Result<TaskRuntimeState> {
    let mut runtime = runtime_state()
        .lock()
        .map_err(|_| anyhow::anyhow!("runtime state lock poisoned"))?;
    let existing = runtime.store.get_task(task_id)?;
    if let Some(task) = existing
        && task.state == TaskRuntimeState::Preparing
    {
        runtime
            .store
            .transition(task_id, TaskRuntimeState::Running)?;
    }
    runtime.sessions.insert(
        task_id.to_string(),
        RuntimeTaskSession {
            session_id: session_id.clone(),
            runner_mode,
        },
    );
    let task = runtime
        .store
        .get_task(task_id)?
        .ok_or_else(|| anyhow::anyhow!("task not found in runtime store: {task_id}"))?;
    Ok(task.state)
}

async fn orchestrate_start_runtime(args: Value) -> Result<Value> {
    let task_id = require_task_id("orchestrate_start", &args)?.to_string();
    let worker_id = optional_string(&args, "worker_id").unwrap_or_else(|| "worker-default".into());
    let scope_id = optional_string(&args, "scope_id");
    let retry_budget = optional_u32(&args, "retry_budget", 3)?;
    let runner_mode = RuntimeRunnerMode::parse(args.get("runner_mode").and_then(Value::as_str))?;
    let (command, command_args) = resolve_start_command_and_args(&args)?;
    let workdir = optional_string(&args, "workdir").unwrap_or_else(|| ".".to_string());

    ensure_task_preparing(&task_id, &worker_id, retry_budget, scope_id.as_deref())?;

    let spec = TaskSpec {
        id: task_id.clone(),
        command,
        args: command_args,
        workdir,
    };

    let handle = match runner_start(runner_mode, spec).await {
        Ok(handle) => handle,
        Err(error) => {
            let _ = mark_task_start_failed(&task_id);
            return Err(error);
        }
    };

    let runtime_state = register_running_session(&task_id, runner_mode, handle.session_id.clone())?;

    info!(
        operation = "mcp_tool_call",
        tool = "orchestrate_start",
        task_id = task_id,
        session_id = handle.session_id,
        runner_mode = runner_mode.as_str(),
        outcome = "accepted",
        "started orchestrated task"
    );

    Ok(json!({
        "tool": "orchestrate_start",
        "task_id": task_id,
        "status": "accepted",
        "session_id": handle.session_id,
        "runner_mode": runner_mode.as_str(),
        "runtime_state": runtime_state_label(runtime_state)
    }))
}

async fn task_runtime_status(task_id: &str, tail: usize) -> Result<Value> {
    let (session, record, steer_observation) = {
        let runtime = runtime_state_mutex()?;
        (
            runtime.sessions.get(task_id).cloned(),
            runtime.store.get_task(task_id)?,
            runtime.last_steer_observation_by_task.get(task_id).cloned(),
        )
    };

    let Some(record) = record else {
        if session.is_some() {
            let _ = clear_runtime_session(task_id);
        }
        return Ok(json!({
            "tool": "orchestrate_status",
            "task_id": task_id,
            "state": "unknown",
            "runtime_state": "unknown",
            "last_steer_sent_at": steer_observation.as_ref().map(|observation| observation.sent_at),
            "last_steer_observed_at": steer_observation.as_ref().and_then(|observation| observation.observed_at),
            "last_steer_apply_hint": steer_observation.as_ref().map(|observation| observation.apply_hint.clone())
        }));
    };

    if runtime_state_is_terminal(record.state) {
        if session.is_some() {
            let _ = clear_runtime_session(task_id);
        }
        return Ok(json!({
            "tool": "orchestrate_status",
            "task_id": task_id,
            "state": runtime_state_label(record.state),
            "runtime_state": runtime_state_label(record.state),
            "assigned_worker": record.assigned_worker,
            "retry_count": record.retry_count,
            "retry_budget": record.retry_budget,
            "last_steer_sent_at": steer_observation.as_ref().map(|observation| observation.sent_at),
            "last_steer_observed_at": steer_observation.as_ref().and_then(|observation| observation.observed_at),
            "last_steer_apply_hint": steer_observation.as_ref().map(|observation| observation.apply_hint.clone())
        }));
    }

    let Some(session) = session else {
        return Ok(json!({
            "tool": "orchestrate_status",
            "task_id": task_id,
            "state": runtime_state_label(record.state),
            "runtime_state": runtime_state_label(record.state),
            "retry_count": record.retry_count,
            "retry_budget": record.retry_budget,
            "last_steer_sent_at": steer_observation.as_ref().map(|observation| observation.sent_at),
            "last_steer_observed_at": steer_observation.as_ref().and_then(|observation| observation.observed_at),
            "last_steer_apply_hint": steer_observation.as_ref().map(|observation| observation.apply_hint.clone())
        }));
    };

    let status = match runner_status(session.runner_mode, &session.session_id).await {
        Ok(status) => status,
        Err(error) => {
            let _ = clear_runtime_session(task_id);
            return Err(error);
        }
    };
    if let Some(target) = map_runner_to_runtime_state(&status.state) {
        let runtime = runtime_state_mutex()?;
        if let Some(current) = runtime.store.get_task(task_id)?
            && current.state != target
            && current.state != TaskRuntimeState::Merged
            && current.state != TaskRuntimeState::FailedTerminal
        {
            let _ = runtime.store.transition(task_id, target);
        }
    }

    let latest = {
        let runtime = runtime_state_mutex()?;
        runtime
            .store
            .get_task(task_id)?
            .ok_or_else(|| anyhow::anyhow!("task not found after status refresh: {task_id}"))?
    };

    let logs = match runner_logs(session.runner_mode, &session.session_id, tail).await {
        Ok(logs) => logs,
        Err(error) => {
            let _ = clear_runtime_session(task_id);
            return Err(error);
        }
    };
    let runtime_state = runtime_state_label(latest.state).to_string();
    maybe_record_delivery_steer_observed(
        task_id,
        &session.session_id,
        &status.state,
        &runtime_state,
    )?;
    emit_delivery_status_events(
        task_id,
        &status.state,
        &runtime_state,
        Some(session.session_id.clone()),
    )?;
    let steer_observation = {
        let runtime = runtime_state_mutex()?;
        runtime.last_steer_observation_by_task.get(task_id).cloned()
    };

    if runner_state_is_terminal(&status.state) || runtime_state_is_terminal(latest.state) {
        let _ = clear_runtime_session(task_id);
    }

    Ok(json!({
        "tool": "orchestrate_status",
        "task_id": task_id,
        "state": status.state,
        "runtime_state": runtime_state,
        "session_id": session.session_id,
        "runner_mode": session.runner_mode.as_str(),
        "assigned_worker": latest.assigned_worker,
        "retry_count": latest.retry_count,
        "retry_budget": latest.retry_budget,
        "logs": logs,
        "last_steer_sent_at": steer_observation.as_ref().map(|observation| observation.sent_at),
        "last_steer_observed_at": steer_observation.as_ref().and_then(|observation| observation.observed_at),
        "last_steer_apply_hint": steer_observation.as_ref().map(|observation| observation.apply_hint.clone())
    }))
}

async fn orchestrate_status_runtime(args: Value) -> Result<Value> {
    let task_id = require_task_id("orchestrate_status", &args)?.to_string();
    let tail = optional_u32(&args, "tail", 120)? as usize;
    task_runtime_status(&task_id, tail).await
}

async fn orchestrate_steer_runtime(args: Value) -> Result<Value> {
    let task_id = require_task_id("orchestrate_steer", &args)?.to_string();
    let instruction = require_string_field("orchestrate_steer", &args, "instruction")?.to_string();

    let session = {
        let runtime = runtime_state()
            .lock()
            .map_err(|_| anyhow::anyhow!("runtime state lock poisoned"))?;
        runtime
            .sessions
            .get(&task_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("task has no active session: {task_id}"))?
    };

    runner_steer(session.runner_mode, &session.session_id, &instruction).await?;
    let steer_sent_at = record_delivery_steer_sent(&task_id, &session.session_id)?;

    info!(
        operation = "mcp_tool_call",
        tool = "orchestrate_steer",
        task_id = task_id,
        session_id = session.session_id,
        outcome = "sent",
        "sent steering instruction to active task session"
    );

    Ok(json!({
        "tool": "orchestrate_steer",
        "task_id": task_id,
        "status": "sent",
        "session_id": session.session_id,
        "last_steer_sent_at": steer_sent_at,
        "last_steer_observed_at": Value::Null,
        "last_steer_apply_hint": "transport_sent"
    }))
}

fn runtime_cleanup_targets(task_id_filter: Option<&str>) -> Result<Vec<RuntimeCleanupTarget>> {
    let runtime = runtime_state_mutex()?;

    if let Some(task_id) = task_id_filter {
        return Ok(vec![RuntimeCleanupTarget {
            task_id: task_id.to_string(),
            record: runtime.store.get_task(task_id)?,
            session: runtime.sessions.get(task_id).cloned(),
        }]);
    }

    let snapshot = runtime.store.export_snapshot()?;
    let mut known_task_ids = HashSet::new();
    let mut targets = Vec::new();
    for record in snapshot.tasks {
        known_task_ids.insert(record.id.clone());
        targets.push(RuntimeCleanupTarget {
            task_id: record.id.clone(),
            record: Some(record.clone()),
            session: runtime.sessions.get(&record.id).cloned(),
        });
    }

    for (task_id, session) in &runtime.sessions {
        if !known_task_ids.contains(task_id) {
            targets.push(RuntimeCleanupTarget {
                task_id: task_id.clone(),
                record: None,
                session: Some(session.clone()),
            });
        }
    }

    Ok(targets)
}

fn task_matches_prune_filters(
    task: &TaskRecord,
    scope_id: Option<&str>,
    state_filter: &HashSet<TaskRuntimeState>,
    updated_before_epoch_ms: i64,
) -> bool {
    if let Some(scope) = scope_id
        && task.scope_id.as_deref() != Some(scope)
    {
        return false;
    }
    if !state_filter.is_empty() && !state_filter.contains(&task.state) {
        return false;
    }
    task.updated_epoch_ms < updated_before_epoch_ms
}

fn purge_runtime_tasks(task_ids: &HashSet<String>) -> Result<usize> {
    if task_ids.is_empty() {
        return Ok(0);
    }

    let mut runtime = runtime_state_mutex()?;
    let snapshot = runtime.store.export_snapshot()?;
    let before = snapshot.tasks.len();
    let retained = snapshot
        .tasks
        .into_iter()
        .filter(|task| !task_ids.contains(&task.id))
        .collect::<Vec<_>>();
    let removed = before.saturating_sub(retained.len());
    runtime
        .store
        .import_snapshot(TaskStoreSnapshot { tasks: retained })?;
    for task_id in task_ids {
        runtime.sessions.remove(task_id);
        reset_delivery_tracking(&mut runtime, task_id);
    }
    Ok(removed)
}

async fn delivery_stop_runtime(args: Value) -> Result<Value> {
    let task_id = require_task_id("delivery_stop", &args)?.to_string();
    let reason = optional_string(&args, "reason");

    let (session, record_before) = {
        let runtime = runtime_state_mutex()?;
        (
            runtime.sessions.get(&task_id).cloned(),
            runtime.store.get_task(&task_id)?,
        )
    };

    let active_or_session = session.is_some()
        || record_before
            .as_ref()
            .map(|task| runtime_state_is_active(task.state))
            .unwrap_or(false);

    let mut stop_propagated = false;
    if let Some(active_session) = session.as_ref() {
        match runner_stop(active_session.runner_mode, &active_session.session_id).await {
            Ok(()) => {
                stop_propagated = true;
            }
            Err(error) if is_session_not_found_error(&error) => {}
            Err(error) => return Err(error),
        }
        let _ = clear_runtime_session(&task_id);
    }

    let runtime_state = {
        let runtime = runtime_state_mutex()?;
        if let Some(task) = runtime.store.get_task(&task_id)? {
            if active_or_session
                && task.state != TaskRuntimeState::FailedRetryable
                && can_transition_to_failed_retryable(task.state)
            {
                let _ = runtime
                    .store
                    .transition(&task_id, TaskRuntimeState::FailedRetryable)?;
            }
            let latest = runtime.store.get_task(&task_id)?.ok_or_else(|| {
                anyhow::anyhow!("task not found after stop transition: {task_id}")
            })?;
            runtime_state_label(latest.state).to_string()
        } else {
            "unknown".to_string()
        }
    };

    if active_or_session {
        let session_id = session.as_ref().map(|active| active.session_id.clone());
        emit_delivery_status_events(&task_id, "stopped", &runtime_state, session_id)?;
    }

    Ok(json!({
        "tool": "delivery_stop",
        "task_id": task_id,
        "status": if active_or_session { "stopped" } else { "already_stopped" },
        "runtime_state": runtime_state,
        "stop_propagated": stop_propagated,
        "reason": reason
    }))
}

fn runtime_summary_template(total: usize) -> Value {
    json!({
        "total": total,
        "queued": 0,
        "preparing": 0,
        "running": 0,
        "review_failed": 0,
        "fixing": 0,
        "validating": 0,
        "ready_to_merge": 0,
        "merged": 0,
        "failed_retryable": 0,
        "failed_terminal": 0,
        "needs_attention": 0
    })
}

fn build_runtime_summary(tasks: &[TaskRecord]) -> Value {
    let mut summary = runtime_summary_template(tasks.len());
    for task in tasks {
        let key = runtime_state_label(task.state);
        if let Some(slot) = summary.get_mut(key)
            && let Some(raw) = slot.as_u64()
        {
            *slot = json!(raw + 1);
        }
    }
    summary
}

fn task_is_after_cursor(task: &TaskRecord, cursor: &ReportCursor) -> bool {
    task.updated_epoch_ms < cursor.updated_epoch_ms
        || (task.updated_epoch_ms == cursor.updated_epoch_ms && task.id > cursor.task_id)
}

fn runtime_report(args: &Value) -> Result<Value> {
    let scope_id = optional_string(args, "scope_id");
    let state_labels = optional_string_array(args, "states")?;
    let mut state_filter = HashSet::new();
    for label in &state_labels {
        runtime_state_from_label(label)?;
        state_filter.insert(label.clone());
    }

    let updated_since_epoch_ms = optional_i64(args, "updated_since_epoch_ms")?;
    if let Some(updated_since_epoch_ms) = updated_since_epoch_ms
        && updated_since_epoch_ms < 0
    {
        anyhow::bail!("invalid updated_since_epoch_ms: must be non-negative");
    }

    let limit = optional_u64(args, "limit")?
        .map(|raw| usize::try_from(raw).map_err(|_| anyhow::anyhow!("invalid limit: out of range")))
        .transpose()?;
    if let Some(limit) = limit
        && limit == 0
    {
        anyhow::bail!("invalid limit: must be >= 1");
    }

    let cursor = optional_string(args, "cursor")
        .map(|raw| decode_report_cursor(raw.as_str()))
        .transpose()?;

    let runtime = runtime_state_mutex()?;
    let snapshot = runtime.store.export_snapshot()?;

    let uses_report_query = scope_id.is_some()
        || !state_filter.is_empty()
        || updated_since_epoch_ms.is_some()
        || limit.is_some()
        || cursor.is_some();

    let mut filtered = snapshot
        .tasks
        .into_iter()
        .filter(|task| {
            if let Some(scope_id) = scope_id.as_deref()
                && task.scope_id.as_deref() != Some(scope_id)
            {
                return false;
            }
            if !state_filter.is_empty() && !state_filter.contains(runtime_state_label(task.state)) {
                return false;
            }
            if let Some(updated_since_epoch_ms) = updated_since_epoch_ms
                && task.updated_epoch_ms < updated_since_epoch_ms
            {
                return false;
            }
            true
        })
        .collect::<Vec<_>>();

    if uses_report_query {
        filtered.sort_by(|left, right| {
            right
                .updated_epoch_ms
                .cmp(&left.updated_epoch_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
    }

    let summary = build_runtime_summary(&filtered);

    if let Some(cursor) = cursor {
        filtered.retain(|task| task_is_after_cursor(task, &cursor));
    }

    let mut next_cursor = None;
    if let Some(limit) = limit
        && filtered.len() > limit
    {
        let last_task = &filtered[limit - 1];
        next_cursor = Some(encode_report_cursor(&ReportCursor {
            updated_epoch_ms: last_task.updated_epoch_ms,
            task_id: last_task.id.clone(),
        }));
        filtered.truncate(limit);
    }

    let tasks = filtered
        .iter()
        .map(|task| {
            json!({
                "task_id": task.id,
                "scope_id": task.scope_id,
                "state": runtime_state_label(task.state),
                "updated_epoch_ms": task.updated_epoch_ms,
                "assigned_worker": task.assigned_worker,
                "retry_count": task.retry_count,
                "retry_budget": task.retry_budget
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "tool": "delivery_report",
        "scope_id": scope_id,
        "summary": summary,
        "tasks": tasks,
        "next_cursor": next_cursor
    }))
}

async fn delivery_cleanup_runtime(args: Value) -> Result<Value> {
    let has_prune_filters = args.get("scope_id").is_some()
        || args.get("states").is_some()
        || args.get("updated_before_epoch_ms").is_some();

    if has_prune_filters {
        let scope_id = optional_string(&args, "scope_id");
        let state_labels = optional_string_array(&args, "states")?;
        let mut states = Vec::new();
        for label in &state_labels {
            states.push(runtime_state_from_label(label)?);
        }
        let state_filter = states.iter().copied().collect::<HashSet<_>>();

        let updated_before_epoch_ms = optional_i64(&args, "updated_before_epoch_ms")?
            .ok_or_else(|| anyhow::anyhow!("missing updated_before_epoch_ms"))?;
        if updated_before_epoch_ms < 0 {
            anyhow::bail!("invalid updated_before_epoch_ms: must be non-negative");
        }

        let candidates = {
            let runtime = runtime_state_mutex()?;
            let snapshot = runtime.store.export_snapshot()?;
            snapshot
                .tasks
                .into_iter()
                .filter_map(|task| {
                    if !task_matches_prune_filters(
                        &task,
                        scope_id.as_deref(),
                        &state_filter,
                        updated_before_epoch_ms,
                    ) {
                        return None;
                    }
                    let session = runtime.sessions.get(&task.id).cloned();
                    Some((task.id, task.state, session))
                })
                .collect::<Vec<_>>()
        };

        let mut stale_session_task_ids = HashSet::new();
        let mut active_task_ids = HashSet::new();
        for (task_id, task_state, session) in &candidates {
            if runtime_state_is_active(*task_state) {
                active_task_ids.insert(task_id.clone());
            }

            if let Some(session) = session {
                match runner_status(session.runner_mode, &session.session_id).await {
                    Ok(status) => {
                        if runner_state_is_terminal(&status.state) {
                            stale_session_task_ids.insert(task_id.clone());
                        } else {
                            active_task_ids.insert(task_id.clone());
                        }
                    }
                    Err(error) if is_session_not_found_error(&error) => {
                        stale_session_task_ids.insert(task_id.clone());
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        let mut runtime = runtime_state_mutex()?;
        for task_id in &stale_session_task_ids {
            runtime.sessions.remove(task_id);
        }

        for (task_id, _, _) in &candidates {
            let Some(task) = runtime.store.get_task(task_id)? else {
                continue;
            };
            if !task_matches_prune_filters(
                &task,
                scope_id.as_deref(),
                &state_filter,
                updated_before_epoch_ms,
            ) {
                continue;
            }
            if runtime_state_is_active(task.state) || runtime.sessions.contains_key(task_id) {
                active_task_ids.insert(task_id.clone());
            }
        }

        if !active_task_ids.is_empty() {
            let mut active = active_task_ids.into_iter().collect::<Vec<_>>();
            active.sort_unstable();
            anyhow::bail!(
                "retention prune refused for active task(s): {}",
                active.join(", ")
            );
        }

        let deleted_task_ids = runtime.store.prune_tasks(
            scope_id.as_deref(),
            &states,
            Some(updated_before_epoch_ms),
        )?;
        for task_id in &deleted_task_ids {
            runtime.sessions.remove(task_id);
            reset_delivery_tracking(&mut runtime, task_id);
        }

        let deleted_count = deleted_task_ids.len();
        return Ok(json!({
            "tool": "delivery_cleanup",
            "status": "ok",
            "mode": "retention_prune",
            "scope_id": scope_id,
            "states": state_labels,
            "updated_before_epoch_ms": updated_before_epoch_ms,
            "deleted_count": deleted_count,
            "deleted_task_ids": deleted_task_ids,
            "cleaned_count": deleted_count,
            "retained_count": 0,
            "skipped_count": 0
        }));
    }

    let task_id_filter = optional_string(&args, "task_id");
    let force = optional_bool(&args, "force", false)?;
    let retention_mode =
        CleanupRetentionMode::parse(args.get("retention_mode").and_then(Value::as_str))?;

    let targets = runtime_cleanup_targets(task_id_filter.as_deref())?;

    let mut purged_task_ids = HashSet::new();
    let mut retained_count = 0usize;
    let mut cleaned_count = 0usize;
    let mut skipped_count = 0usize;
    let mut task_results = Vec::new();

    for target in targets {
        let mut record_state = target.record.as_ref().map(|task| task.state);
        let mut active = record_state.map(runtime_state_is_active).unwrap_or(false);

        if let Some(session) = target.session.as_ref() {
            match runner_status(session.runner_mode, &session.session_id).await {
                Ok(status) => {
                    active = !runner_state_is_terminal(&status.state);
                }
                Err(error) if is_session_not_found_error(&error) => {
                    active = false;
                }
                Err(error) => return Err(error),
            }
        }

        if active && !force {
            anyhow::bail!(
                "cleanup refused for active task {}: retry with force=true",
                target.task_id
            );
        }

        if let Some(session) = target.session.as_ref() {
            if force {
                match runner_stop(session.runner_mode, &session.session_id).await {
                    Ok(()) => {}
                    Err(error) if is_session_not_found_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            let _ = clear_runtime_session(&target.task_id);
        }

        let eligible = force
            || record_state
                .map(runtime_state_is_finished_or_abandoned)
                .unwrap_or(true);

        if !eligible {
            skipped_count += 1;
            task_results.push(json!({
                "task_id": target.task_id,
                "action": "skipped_not_finished",
                "runtime_state": record_state.map(runtime_state_label).unwrap_or("unknown")
            }));
            continue;
        }

        match retention_mode {
            CleanupRetentionMode::Purge => {
                if target.record.is_some() {
                    purged_task_ids.insert(target.task_id.clone());
                    cleaned_count += 1;
                } else {
                    skipped_count += 1;
                }
                task_results.push(json!({
                    "task_id": target.task_id,
                    "action": "purged",
                    "runtime_state": "removed"
                }));
            }
            CleanupRetentionMode::Retain => {
                if force && let Some(current) = record_state {
                    if can_transition_to_failed_terminal(current) {
                        let runtime = runtime_state_mutex()?;
                        let _ = runtime
                            .store
                            .transition(&target.task_id, TaskRuntimeState::FailedTerminal)?;
                        record_state = Some(TaskRuntimeState::FailedTerminal);
                    }
                }
                retained_count += usize::from(target.record.is_some());
                task_results.push(json!({
                    "task_id": target.task_id,
                    "action": "retained",
                    "runtime_state": record_state.map(runtime_state_label).unwrap_or("unknown")
                }));
            }
        }
    }

    if !purged_task_ids.is_empty() {
        cleaned_count = purge_runtime_tasks(&purged_task_ids)?;
    }
    let mut deleted_task_ids = purged_task_ids.into_iter().collect::<Vec<_>>();
    deleted_task_ids.sort_unstable();
    let deleted_count = deleted_task_ids.len();

    Ok(json!({
        "tool": "delivery_cleanup",
        "status": "ok",
        "mode": "lifecycle",
        "force": force,
        "retention_mode": retention_mode.as_str(),
        "cleaned_count": cleaned_count,
        "retained_count": retained_count,
        "skipped_count": skipped_count,
        "tasks": task_results,
        "deleted_count": deleted_count,
        "deleted_task_ids": deleted_task_ids
    }))
}

fn task_matches_filter(event_task_id: &str, filter: Option<&str>) -> bool {
    match filter {
        Some(task_id) => event_task_id == task_id,
        None => true,
    }
}

fn delivery_events_subscribe(args: Value) -> Result<Value> {
    let subscriber_id = optional_string(&args, "subscriber_id").unwrap_or_else(next_subscriber_id);
    let requested_cursor = parse_optional_u64(&args, "cursor")?;
    let requested_task_id = optional_string(&args, "task_id");

    let mut runtime = runtime_state_mutex()?;
    let latest_cursor = runtime.next_delivery_cursor.saturating_sub(1);
    if let Some(cursor) = requested_cursor {
        validate_delivery_cursor_bounds(
            "delivery_events_subscribe",
            "cursor",
            cursor,
            latest_cursor,
        )?;
    }
    let subscription = runtime
        .delivery_subscriptions
        .entry(subscriber_id.clone())
        .or_insert_with(|| DeliverySubscription {
            cursor: latest_cursor,
            task_id: requested_task_id.clone(),
        });

    if let Some(cursor) = requested_cursor {
        subscription.cursor = cursor;
    }
    if requested_task_id.is_some() {
        subscription.task_id = requested_task_id.clone();
    }

    Ok(json!({
        "tool": "delivery_events_subscribe",
        "subscriber_id": subscriber_id,
        "cursor": subscription.cursor,
        "task_id": subscription.task_id,
        "semantics": "at_least_once_with_ack"
    }))
}

fn delivery_events_next(args: Value) -> Result<Value> {
    let subscriber_id = require_string_field("delivery_events_next", &args, "subscriber_id")?;
    let requested_cursor = parse_optional_u64(&args, "cursor")?;
    let requested_task_id = optional_string(&args, "task_id");
    let limit = optional_u32(&args, "limit", 50)? as usize;
    let limit = limit.clamp(1, 500);

    let runtime = runtime_state_mutex()?;
    let subscription = runtime
        .delivery_subscriptions
        .get(subscriber_id)
        .ok_or_else(|| anyhow::anyhow!("unknown subscriber_id: {subscriber_id}"))?;
    let latest_cursor = runtime.next_delivery_cursor.saturating_sub(1);
    let start_cursor = requested_cursor.unwrap_or(subscription.cursor);
    validate_delivery_cursor_bounds(
        "delivery_events_next",
        "cursor",
        start_cursor,
        latest_cursor,
    )?;
    let filter_task = requested_task_id
        .as_deref()
        .or(subscription.task_id.as_deref());

    let mut next_cursor = start_cursor;
    let mut events = Vec::new();
    for event in &runtime.delivery_events {
        if event.cursor <= start_cursor || !task_matches_filter(&event.task_id, filter_task) {
            continue;
        }
        next_cursor = event.cursor;
        events.push(event.as_json());
        if events.len() >= limit {
            break;
        }
    }
    let has_more = runtime.delivery_events.iter().any(|event| {
        event.cursor > next_cursor
            && task_matches_filter(&event.task_id, filter_task)
            && event.cursor > start_cursor
    });

    Ok(json!({
        "tool": "delivery_events_next",
        "subscriber_id": subscriber_id,
        "cursor": start_cursor,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "events": events
    }))
}

fn delivery_events_ack(args: Value) -> Result<Value> {
    let subscriber_id = require_string_field("delivery_events_ack", &args, "subscriber_id")?;
    let requested_cursor = require_u64_field("delivery_events_ack", &args, "cursor")?;
    let mut runtime = runtime_state_mutex()?;
    let latest_cursor = runtime.next_delivery_cursor.saturating_sub(1);
    validate_delivery_cursor_bounds(
        "delivery_events_ack",
        "cursor",
        requested_cursor,
        latest_cursor,
    )?;
    let subscription = runtime
        .delivery_subscriptions
        .get_mut(subscriber_id)
        .ok_or_else(|| anyhow::anyhow!("unknown subscriber_id: {subscriber_id}"))?;
    subscription.cursor = subscription.cursor.max(requested_cursor);

    Ok(json!({
        "tool": "delivery_events_ack",
        "subscriber_id": subscriber_id,
        "acked_cursor": subscription.cursor
    }))
}

async fn poll_runtime_once() -> Result<()> {
    let task_ids = {
        let runtime = runtime_state_mutex()?;
        runtime.sessions.keys().cloned().collect::<Vec<_>>()
    };

    let mut failures = Vec::new();
    for task_id in task_ids {
        if let Err(error) = task_runtime_status(&task_id, 50).await {
            failures.push(format!("{task_id}: {error}"));
        }
    }

    if !failures.is_empty() {
        anyhow::bail!(
            "runtime poll encountered per-task failures: {}",
            failures.join("; ")
        );
    }

    Ok(())
}

pub async fn run_stdio() -> Result<()> {
    info!(
        operation = "mcp_server_bootstrap",
        transport = "stdio",
        outcome = "ok",
        "agentrail MCP stdio server bootstrap"
    );

    tokio::spawn(async {
        loop {
            if let Err(error) = poll_runtime_once().await {
                warn!(
                    operation = "runtime_poll",
                    outcome = "error",
                    error = %error,
                    "runtime poll failed"
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();
    let mut writer = BufWriter::new(stdout);

    while let Some(line) = reader.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let parsed: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(error) => {
                let response = json!({
                    "jsonrpc":"2.0",
                    "id": Value::Null,
                    "error": {
                        "code": -32700,
                        "message": format!("parse error: {error}")
                    }
                });
                writer
                    .write_all(serde_json::to_string(&response)?.as_bytes())
                    .await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                continue;
            }
        };

        if let Some(response) = handle_mcp_request(parsed)? {
            writer
                .write_all(serde_json::to_string(&response)?.as_bytes())
                .await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
    }

    Ok(())
}

pub async fn run_http(bind: &str) -> Result<()> {
    info!(
        operation = "mcp_server_bootstrap",
        transport = "http",
        bind = bind,
        outcome = "ok",
        "agentrail MCP HTTP server bootstrap"
    );
    tokio::spawn(async {
        loop {
            if let Err(error) = poll_runtime_once().await {
                warn!(
                    operation = "runtime_poll",
                    outcome = "error",
                    error = %error,
                    "runtime poll failed"
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    #[derive(Clone)]
    struct HttpState;

    async fn health() -> Json<Value> {
        Json(json!({"status":"ok"}))
    }

    async fn mcp_handler(
        State(_state): State<HttpState>,
        Json(request): Json<Value>,
    ) -> impl IntoResponse {
        let (status, body) = handle_http_mcp_request(request);
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        match body {
            Some(payload) => (status, Json(payload)).into_response(),
            None => status.into_response(),
        }
    }

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/mcp", post(mcp_handler))
        .with_state(HttpState);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

struct SupervisedWorker {
    child: Child,
    stdin: ChildStdin,
    stdout_rx: mpsc::UnboundedReceiver<String>,
}

async fn spawn_supervised_worker(worker_cmd: Option<&str>) -> Result<SupervisedWorker> {
    let mut command = if let Some(raw) = worker_cmd {
        let mut cmd = Command::new("bash");
        cmd.args(["-lc", raw]);
        info!(
            operation = "mcp_supervisor_spawn",
            mode = "shell",
            command = raw,
            outcome = "launching",
            "spawning supervised MCP worker command"
        );
        cmd
    } else {
        let current_exe = std::env::current_exe()?;
        let mut cmd = Command::new(&current_exe);
        cmd.args(["--transport", "stdio"]);
        info!(
            operation = "mcp_supervisor_spawn",
            mode = "self",
            command = %format!("{} --transport stdio", current_exe.display()),
            outcome = "launching",
            "spawning supervised MCP worker from current executable"
        );
        cmd
    };

    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    let mut child = command.spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("worker stdin not available"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("worker stdout not available"))?;

    let (stdout_tx, stdout_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if stdout_tx.send(line).is_err() {
                        break;
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
    });

    Ok(SupervisedWorker {
        child,
        stdin,
        stdout_rx,
    })
}

async fn restart_supervised_worker(
    worker: &mut SupervisedWorker,
    worker_cmd: Option<&str>,
) -> Result<()> {
    let _ = worker.child.start_kill();
    let _ = worker.child.wait().await;
    *worker = spawn_supervised_worker(worker_cmd).await?;
    Ok(())
}

fn extract_reload_request_id(raw_line: &str, reload_method: &str) -> Option<Option<Value>> {
    let parsed: Value = serde_json::from_str(raw_line).ok()?;
    let method = parsed.get("method")?.as_str()?;
    if method != reload_method {
        return None;
    }

    let id = parsed.get("id").filter(|value| !value.is_null()).cloned();
    Some(id)
}

pub async fn run_supervisor_stdio(worker_cmd: Option<&str>, reload_method: &str) -> Result<()> {
    info!(
        operation = "mcp_server_bootstrap",
        transport = "supervisor-stdio",
        reload_method = reload_method,
        outcome = "ok",
        "agentrail MCP stdio supervisor bootstrap"
    );

    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut client_reader = BufReader::new(stdin).lines();
    let mut client_writer = BufWriter::new(stdout);
    let mut worker = spawn_supervised_worker(worker_cmd).await?;

    loop {
        loop {
            match worker.stdout_rx.try_recv() {
                Ok(line) => {
                    client_writer.write_all(line.as_bytes()).await?;
                    client_writer.write_all(b"\n").await?;
                    client_writer.flush().await?;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    warn!(
                        operation = "mcp_supervisor_worker_stdout",
                        outcome = "disconnected",
                        "worker stdout channel disconnected; restarting worker"
                    );
                    restart_supervised_worker(&mut worker, worker_cmd).await?;
                    break;
                }
            }
        }

        let next_client_line =
            tokio::time::timeout(Duration::from_millis(100), client_reader.next_line()).await;
        let maybe_line = match next_client_line {
            Ok(line_result) => line_result?,
            Err(_) => continue,
        };

        let Some(line) = maybe_line else {
            let _ = worker.child.start_kill();
            let _ = worker.child.wait().await;
            return Ok(());
        };

        if line.trim().is_empty() {
            continue;
        }

        if let Some(response_id) = extract_reload_request_id(&line, reload_method) {
            info!(
                operation = "mcp_supervisor_reload",
                outcome = "requested",
                "supervisor received reload request"
            );
            restart_supervised_worker(&mut worker, worker_cmd).await?;
            if let Some(id) = response_id {
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "ok": true,
                        "reloaded": true
                    }
                });
                client_writer
                    .write_all(serde_json::to_string(&response)?.as_bytes())
                    .await?;
                client_writer.write_all(b"\n").await?;
                client_writer.flush().await?;
            }
            continue;
        }

        if let Err(error) = worker.stdin.write_all(line.as_bytes()).await {
            warn!(
                operation = "mcp_supervisor_forward",
                outcome = "worker_stdin_write_failed",
                error = %error,
                "worker stdin write failed; restarting worker and retrying"
            );
            restart_supervised_worker(&mut worker, worker_cmd).await?;
            worker.stdin.write_all(line.as_bytes()).await?;
        }
        worker.stdin.write_all(b"\n").await?;
        worker.stdin.flush().await?;
    }
}

fn require_string_field<'a>(tool_name: &str, args: &'a Value, field: &str) -> Result<&'a str> {
    let Some(value) = args.get(field).and_then(Value::as_str) else {
        warn!(
            operation = "mcp_tool_call",
            tool = tool_name,
            missing_field = field,
            outcome = "missing_required_field",
            "MCP tool call missing required field"
        );
        anyhow::bail!("missing {field}");
    };

    info!(
        operation = "mcp_tool_call",
        tool = tool_name,
        field = field,
        outcome = "validated",
        "validated MCP tool call arguments"
    );

    Ok(value)
}

fn require_task_id<'a>(tool_name: &str, args: &'a Value) -> Result<&'a str> {
    require_string_field(tool_name, args, "task_id")
}

fn require_plan_path(
    tool_name: &str,
    args: &Value,
    allowed_root: Option<&Path>,
) -> Result<PathBuf> {
    let raw_plan_path = require_string_field(tool_name, args, "plan_path")?;
    let canonical_plan_path = fs::canonicalize(raw_plan_path)
        .map_err(|e| anyhow::anyhow!("invalid plan_path: {raw_plan_path} ({e})"))?;

    if let Some(root) = allowed_root {
        let canonical_root = fs::canonicalize(root)
            .map_err(|e| anyhow::anyhow!("invalid allowed root: {} ({e})", root.display()))?;
        if !canonical_plan_path.starts_with(&canonical_root) {
            anyhow::bail!(
                "plan_path outside allowed root: {}",
                canonical_plan_path.display()
            );
        }
    } else if let Ok(root) = std::env::var("AGENTRAIL_ALLOWED_PLAN_ROOT") {
        if !root.trim().is_empty() {
            let canonical_root = fs::canonicalize(&root).map_err(|e| {
                anyhow::anyhow!("invalid AGENTRAIL_ALLOWED_PLAN_ROOT: {root} ({e})")
            })?;
            if !canonical_plan_path.starts_with(&canonical_root) {
                anyhow::bail!(
                    "plan_path outside allowed root: {}",
                    canonical_plan_path.display()
                );
            }
        }
    }

    Ok(canonical_plan_path)
}

fn find_step<'a>(plan: &'a Plan, step_id: &str) -> Option<&'a Step> {
    plan.phases
        .iter()
        .flat_map(|phase| phase.steps.iter())
        .find(|step| step.id == step_id)
}

fn find_step_with_phase<'a>(plan: &'a Plan, step_id: &str) -> Option<(&'a Phase, &'a Step)> {
    plan.phases.iter().find_map(|phase| {
        phase
            .steps
            .iter()
            .find(|step| step.id == step_id)
            .map(|step| (phase, step))
    })
}

fn find_step_mut<'a>(plan: &'a mut Plan, step_id: &str) -> Option<&'a mut Step> {
    for phase in &mut plan.phases {
        if let Some(step) = phase.steps.iter_mut().find(|step| step.id == step_id) {
            return Some(step);
        }
    }
    None
}

fn step_counts(plan: &Plan) -> (usize, usize, usize) {
    let mut pending = 0;
    let mut claimed = 0;
    let mut done = 0;

    for step in plan.phases.iter().flat_map(|phase| phase.steps.iter()) {
        match step.status {
            StepStatus::Pending => pending += 1,
            StepStatus::Claimed => claimed += 1,
            StepStatus::Done => done += 1,
            StepStatus::Skipped | StepStatus::Rejected => {}
        }
    }

    (pending, claimed, done)
}

fn step_status_map(plan: &Plan) -> HashMap<String, StepStatus> {
    plan.phases
        .iter()
        .flat_map(|phase| phase.steps.iter())
        .map(|step| (step.id.clone(), step.status.clone()))
        .collect()
}

fn phase_status_map(plan: &Plan) -> HashMap<String, PhaseStatus> {
    plan.phases
        .iter()
        .map(|phase| (phase.id.clone(), phase.status.clone()))
        .collect()
}

fn step_dependencies_ready(step: &Step, step_status_by_id: &HashMap<String, StepStatus>) -> bool {
    step.depends_on.iter().all(|dep| {
        matches!(
            step_status_by_id.get(dep),
            Some(StepStatus::Done) | Some(StepStatus::Skipped)
        )
    })
}

fn phase_ready_for_work(phase: &Phase, phase_status_by_id: &HashMap<String, PhaseStatus>) -> bool {
    if phase.status == PhaseStatus::Locked {
        return false;
    }

    phase
        .depends_on
        .iter()
        .all(|dep| matches!(phase_status_by_id.get(dep), Some(PhaseStatus::Done)))
}

fn next_ready_step(plan: &Plan) -> Option<&Step> {
    let step_status_by_id = step_status_map(plan);
    let phase_status_by_id = phase_status_map(plan);

    for phase in &plan.phases {
        if !phase_ready_for_work(phase, &phase_status_by_id) {
            continue;
        }
        if let Some(step) = phase.steps.iter().find(|step| {
            step.status == StepStatus::Pending && step_dependencies_ready(step, &step_status_by_id)
        }) {
            return Some(step);
        }
    }

    None
}

fn load_plan(path: &Path) -> Result<(Plan, String)> {
    agentrail_plan_io::load_plan(path)
}

fn save_plan(plan: &Plan, path: &Path, expected_hash: Option<&str>) -> Result<String> {
    agentrail_plan_io::save_plan(plan, path, expected_hash)
}

pub fn handle_tool_call(tool_name: &str, args: Value) -> Result<Value> {
    handle_tool_call_with_allowed_root(tool_name, args, None)
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id": id,
        "result": result
    })
}

fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message.into()
        }
    })
}

fn mcp_tools_descriptor() -> Value {
    json!([
        {
            "name":"plan_status",
            "description":"Return plan status summary",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "plan_path": {"type":"string"}
                },
                "required": ["plan_path"]
            }
        },
        {
            "name":"plan_show",
            "description":"Show one step details",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "plan_path": {"type":"string"},
                    "step_id": {"type":"string"}
                },
                "required": ["plan_path","step_id"]
            }
        },
        {
            "name":"plan_next",
            "description":"Get next ready step",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "plan_path": {"type":"string"}
                },
                "required": ["plan_path"]
            }
        },
        {
            "name":"plan_claim",
            "description":"Claim a step for an agent",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "plan_path": {"type":"string"},
                    "step_id": {"type":"string"},
                    "agent": {"type":"string"}
                },
                "required": ["plan_path","step_id","agent"]
            }
        },
        {
            "name":"plan_complete",
            "description":"Complete a claimed step",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "plan_path": {"type":"string"},
                    "step_id": {"type":"string"},
                    "agent": {"type":"string"},
                    "evidence": {"type":"string"}
                },
                "required": ["plan_path","step_id","agent","evidence"]
            }
        },
        {
            "name":"orchestrate_start",
            "description":"Start a runtime task on process/tmux runner",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "scope_id": {"type":"string"},
                    "worker_id": {"type":"string"},
                    "runner_mode": {"type":"string", "enum": ["process", "tmux"]},
                    "command": {"type":"string"},
                    "args": {
                        "type":"array",
                        "items": {"type":"string"}
                    },
                    "workdir": {"type":"string"},
                    "retry_budget": {"type":"integer"}
                },
                "required": ["task_id"]
            }
        },
        {
            "name":"orchestrate_status",
            "description":"Query runtime status for one task",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "tail": {"type":"integer"}
                },
                "required": ["task_id"]
            }
        },
        {
            "name":"orchestrate_steer",
            "description":"Send steering instruction to running task session",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "instruction": {"type":"string"}
                },
                "required": ["task_id", "instruction"]
            }
        },
        {
            "name":"delivery_submit",
            "description":"High-level submit API for chat orchestrators",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "scope_id": {"type":"string"},
                    "worker_id": {"type":"string"},
                    "runner_mode": {"type":"string", "enum": ["process", "tmux"]},
                    "steer_required": {"type":"boolean"},
                    "interactive_command": {"type":"boolean"},
                    "command": {"type":"string"},
                    "args": {
                        "type":"array",
                        "items": {"type":"string"}
                    },
                    "workdir": {"type":"string"},
                    "retry_budget": {"type":"integer"}
                },
                "required": ["task_id"]
            }
        },
        {
            "name":"delivery_status",
            "description":"High-level runtime status API for chat orchestrators with normalized v1 machine-first envelope",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "tail": {"type":"integer"}
                },
                "required": ["task_id"]
            }
        },
        {
            "name":"delivery_steer",
            "description":"High-level steering API for chat orchestrators",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "instruction": {"type":"string"}
                },
                "required": ["task_id", "instruction"]
            }
        },
        {
            "name":"delivery_stop",
            "description":"High-level stop API for chat orchestrators",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "reason": {"type":"string"}
                },
                "required": ["task_id"]
            }
        },
        {
            "name":"delivery_cleanup",
            "description":"Lifecycle cleanup with stop/retain controls, plus optional scoped retention pruning filters",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "task_id": {"type":"string"},
                    "force": {"type":"boolean"},
                    "retention_mode": {"type":"string", "enum": ["purge", "retain"]},
                    "scope_id": {"type":"string"},
                    "states": {
                        "type":"array",
                        "items": {"type":"string"}
                    },
                    "updated_before_epoch_ms": {"type":"integer"}
                },
                "allOf": [
                    {
                        "if": {
                            "anyOf": [
                                {"required": ["scope_id"]},
                                {"required": ["states"]}
                            ]
                        },
                        "then": {
                            "required": ["updated_before_epoch_ms"]
                        }
                    }
                ]
            }
        },
        {
            "name":"delivery_events_subscribe",
            "description":"Subscribe to lifecycle events for delivery tasks",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "subscriber_id": {"type":"string"},
                    "cursor": {"type":"integer"},
                    "task_id": {"type":"string"}
                }
            }
        },
        {
            "name":"delivery_events_next",
            "description":"Read the next page of lifecycle events after a cursor",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "subscriber_id": {"type":"string"},
                    "cursor": {"type":"integer"},
                    "task_id": {"type":"string"},
                    "limit": {"type":"integer"}
                },
                "required": ["subscriber_id"]
            }
        },
        {
            "name":"delivery_events_ack",
            "description":"Acknowledge events up to a cursor for at-least-once delivery",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "subscriber_id": {"type":"string"},
                    "cursor": {"type":"integer"}
                },
                "required": ["subscriber_id", "cursor"]
            }
        },
        {
            "name":"delivery_report",
            "description":"Return aggregated runtime report for all tracked tasks",
            "inputSchema": {
                "type":"object",
                "properties": {
                    "scope_id": {"type":"string"},
                    "states": {
                        "type":"array",
                        "items": {"type":"string"}
                    },
                    "updated_since_epoch_ms": {"type":"integer"},
                    "limit": {"type":"integer"},
                    "cursor": {"type":"string"}
                }
            }
        },

    ])
}

pub fn handle_mcp_request(request: Value) -> Result<Option<Value>> {
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("invalid request: missing method"))?;

    if method == "notifications/initialized" {
        return Ok(None);
    }

    let id = request.get("id").cloned().unwrap_or(Value::Null);

    let response = match method {
        "initialize" => rpc_result(
            id,
            json!({
                "protocolVersion":"2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name":"agentrail-mcp",
                    "version":"0.1.0"
                }
            }),
        ),
        "tools/list" => rpc_result(
            id,
            json!({
                "tools": mcp_tools_descriptor()
            }),
        ),
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = match params.get("name").and_then(Value::as_str) {
                Some(name) => name,
                None => {
                    return Ok(Some(rpc_error(
                        id,
                        -32602,
                        "invalid params: missing tool name",
                    )));
                }
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            match handle_tool_call(name, arguments) {
                Ok(tool_result) => rpc_result(
                    id,
                    json!({
                        "content":[
                            {
                                "type":"text",
                                "text": serde_json::to_string(&tool_result)?
                            }
                        ]
                    }),
                ),
                Err(error) => rpc_error(id, -32000, error.to_string()),
            }
        }
        _ => rpc_error(id, -32601, format!("method not found: {method}")),
    };

    Ok(Some(response))
}

pub fn handle_http_mcp_request(request: Value) -> (u16, Option<Value>) {
    let request_id = request.get("id").cloned().unwrap_or(Value::Null);

    match handle_mcp_request(request) {
        Ok(Some(response)) => (StatusCode::OK.as_u16(), Some(response)),
        Ok(None) => (StatusCode::NO_CONTENT.as_u16(), None),
        Err(error) => (
            StatusCode::OK.as_u16(),
            Some(rpc_error(request_id, -32000, error.to_string())),
        ),
    }
}

pub fn handle_tool_call_with_allowed_root(
    tool_name: &str,
    args: Value,
    allowed_root: Option<&Path>,
) -> Result<Value> {
    match tool_name {
        "orchestrate_start" => block_on_result(orchestrate_start_runtime(args)),
        "orchestrate_status" => block_on_result(orchestrate_status_runtime(args)),
        "orchestrate_steer" => block_on_result(orchestrate_steer_runtime(args)),
        "delivery_submit" => {
            validate_delivery_submit_preflight(&args)?;
            let orchestration = block_on_result(orchestrate_start_runtime(args))?;
            if let Some(task_id) = orchestration.get("task_id").and_then(Value::as_str) {
                let runtime_state = orchestration
                    .get("runtime_state")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let session_id = orchestration
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                let mut runtime = runtime_state_mutex()?;
                emit_delivery_event(
                    &mut runtime,
                    task_id,
                    "submitted",
                    "submitted",
                    runtime_state,
                    session_id.clone(),
                );
                emit_delivery_event(
                    &mut runtime,
                    task_id,
                    "running",
                    "running",
                    runtime_state,
                    session_id,
                );
            }
            Ok(json!({
                "tool": "delivery_submit",
                "task_id": orchestration["task_id"],
                "orchestration": orchestration
            }))
        }
        "delivery_status" => {
            let tail = optional_u32(&args, "tail", 120)?;
            let orchestration = block_on_result(orchestrate_status_runtime(args))?;
            let normalized = delivery_status_normalized_v1(&orchestration, tail);
            Ok(json!({
                "tool": "delivery_status",
                "task_id": orchestration["task_id"],
                "state": orchestration["state"],
                "runtime_state": orchestration["runtime_state"],
                "orchestration": orchestration,
                "normalized": normalized
            }))
        }
        "delivery_steer" => {
            let orchestration = block_on_result(orchestrate_steer_runtime(args))?;
            Ok(json!({
                "tool": "delivery_steer",
                "task_id": orchestration["task_id"],
                "status": orchestration["status"],
                "last_steer_sent_at": orchestration["last_steer_sent_at"],
                "last_steer_observed_at": orchestration["last_steer_observed_at"],
                "last_steer_apply_hint": orchestration["last_steer_apply_hint"],
                "orchestration": orchestration
            }))
        }
        "delivery_events_subscribe" => delivery_events_subscribe(args),
        "delivery_events_next" => delivery_events_next(args),
        "delivery_events_ack" => delivery_events_ack(args),
        "delivery_stop" => block_on_result(delivery_stop_runtime(args)),
        "delivery_cleanup" => block_on_result(delivery_cleanup_runtime(args)),
        "delivery_report" => runtime_report(&args),
        "plan_status" => {
            let plan_path = require_plan_path(tool_name, &args, allowed_root)?;
            let (plan, _) = load_plan(&plan_path)?;
            let (pending, claimed, done) = step_counts(&plan);
            Ok(json!({
                "tool": tool_name,
                "operation": "status",
                "project": plan.project,
                "phase_count": plan.phases.len(),
                "step_counts": {
                    "pending": pending,
                    "claimed": claimed,
                    "done": done
                }
            }))
        }
        "plan_show" => {
            let plan_path = require_plan_path(tool_name, &args, allowed_root)?;
            let step_id = require_string_field(tool_name, &args, "step_id")?;
            let (plan, _) = load_plan(&plan_path)?;
            let step = find_step(&plan, step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
            Ok(json!({
                "tool": tool_name,
                "operation": "show",
                "step": step
            }))
        }
        "plan_next" => {
            let plan_path = require_plan_path(tool_name, &args, allowed_root)?;
            let (mut plan, _) = load_plan(&plan_path)?;
            agentrail_core::recalc_lock_status(&mut plan);
            let step = next_ready_step(&plan);
            Ok(json!({
                "tool": tool_name,
                "operation": "next",
                "step": step
            }))
        }
        "plan_claim" => {
            let plan_path = require_plan_path(tool_name, &args, allowed_root)?;
            let step_id = require_string_field(tool_name, &args, "step_id")?;
            let agent = require_string_field(tool_name, &args, "agent")?;
            let (mut plan, hash) = load_plan(&plan_path)?;
            agentrail_core::recalc_lock_status(&mut plan);
            {
                let (phase, step) = find_step_with_phase(&plan, step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                if step.status != StepStatus::Pending {
                    anyhow::bail!(
                        "invalid state transition: claim requires pending -> claimed, current={:?}",
                        step.status
                    );
                }
                let step_status_by_id = step_status_map(&plan);
                let phase_status_by_id = phase_status_map(&plan);
                if !phase_ready_for_work(phase, &phase_status_by_id)
                    || !step_dependencies_ready(step, &step_status_by_id)
                {
                    anyhow::bail!("dependencies not ready: {step_id}");
                }
            }
            {
                let step = find_step_mut(&mut plan, step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                step.status = StepStatus::Claimed;
                step.claimed_by = Some(agent.to_string());
                step.evidence = None;
            }
            agentrail_core::recalc_lock_status(&mut plan);
            save_plan(&plan, &plan_path, Some(&hash))?;
            let step = find_step(&plan, step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
            Ok(json!({
                "tool": tool_name,
                "operation": "claim",
                "step": step
            }))
        }
        "plan_complete" => {
            let plan_path = require_plan_path(tool_name, &args, allowed_root)?;
            let step_id = require_string_field(tool_name, &args, "step_id")?;
            let agent = require_string_field(tool_name, &args, "agent")?;
            let evidence = require_string_field(tool_name, &args, "evidence")?;
            let (mut plan, hash) = load_plan(&plan_path)?;
            agentrail_core::recalc_lock_status(&mut plan);
            {
                let (phase, step) = find_step_with_phase(&plan, step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                if step.status != StepStatus::Claimed {
                    anyhow::bail!(
                        "invalid state transition: complete requires claimed -> done, current={:?}",
                        step.status
                    );
                }
                if step.claimed_by.as_deref() != Some(agent) {
                    anyhow::bail!(
                        "claimed_by mismatch: expected={agent} actual={:?}",
                        step.claimed_by
                    );
                }
                let step_status_by_id = step_status_map(&plan);
                let phase_status_by_id = phase_status_map(&plan);
                if !phase_ready_for_work(phase, &phase_status_by_id)
                    || !step_dependencies_ready(step, &step_status_by_id)
                {
                    anyhow::bail!("dependencies not ready: {step_id}");
                }
            }
            {
                let step = find_step_mut(&mut plan, step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                step.status = StepStatus::Done;
                step.evidence = Some(evidence.to_string());
            }
            agentrail_core::recalc_lock_status(&mut plan);
            save_plan(&plan, &plan_path, Some(&hash))?;
            let step = find_step(&plan, step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
            Ok(json!({
                "tool": tool_name,
                "operation": "complete",
                "step": step
            }))
        }
        _ => {
            warn!(
                operation = "mcp_tool_call",
                tool = tool_name,
                outcome = "unsupported",
                "unsupported MCP tool"
            );
            anyhow::bail!("unsupported tool: {tool_name}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use tempfile::tempdir;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedBuffer {
        fn into_string(&self) -> String {
            String::from_utf8(self.0.lock().expect("lock tracing buffer").clone())
                .expect("tracing output should be utf8")
        }
    }

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedWriter(self.0.clone())
        }
    }

    impl Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("lock tracing buffer")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn has_field_value(logs: &str, field: &str, value: &str) -> bool {
        logs.contains(&format!("{field}={value}")) || logs.contains(&format!("{field}=\"{value}\""))
    }

    fn unique_test_task_id(prefix: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("monotonic clock")
            .as_nanos();
        format!("{prefix}-{now}")
    }

    #[test]
    fn resolve_start_command_defaults_bash_with_safe_args_when_omitted() {
        let (command, args) = resolve_start_command_and_args(&json!({})).expect("resolve command");
        assert_eq!(command, "bash");
        assert_eq!(args, vec!["-lc".to_string(), "true".to_string()]);
    }

    #[test]
    fn extract_reload_request_id_returns_id_for_matching_method() {
        let id = extract_reload_request_id(
            r#"{"jsonrpc":"2.0","id":"abc","method":"agentrail/reload","params":{}}"#,
            "agentrail/reload",
        )
        .expect("reload request should match");
        assert_eq!(id, Some(json!("abc")));
    }

    #[test]
    fn extract_reload_request_id_ignores_other_methods() {
        let id = extract_reload_request_id(
            r#"{"jsonrpc":"2.0","id":"abc","method":"tools/list","params":{}}"#,
            "agentrail/reload",
        );
        assert!(id.is_none());
    }

    #[test]
    fn resolve_start_command_keeps_empty_args_for_explicit_command() {
        let (command, args) = resolve_start_command_and_args(&json!({"command":"/usr/bin/whoami"}))
            .expect("resolve command");
        assert_eq!(command, "/usr/bin/whoami");
        assert!(args.is_empty());
    }

    #[test]
    fn delivery_status_normalized_v1_includes_steer_metadata_fields() {
        let normalized = delivery_status_normalized_v1(
            &json!({
                "task_id": "task-1",
                "state": "running",
                "runtime_state": "running",
                "runner_mode": "tmux",
                "session_id": "session-1",
                "assigned_worker": "worker-a",
                "retry_count": 0,
                "retry_budget": 3,
                "last_steer_sent_at": 111_u64,
                "last_steer_observed_at": Value::Null,
                "last_steer_apply_hint": "transport_sent"
            }),
            12,
        );

        assert_eq!(normalized["last_steer_sent_at"], 111_u64);
        assert!(normalized["last_steer_observed_at"].is_null());
        assert_eq!(normalized["last_steer_apply_hint"], "transport_sent");
    }

    #[test]
    fn runtime_store_config_defaults_to_sqlite_file_backed_dsn() {
        let config = resolve_runtime_store_config(None);
        assert_eq!(config.dsn, DEFAULT_RUNTIME_STORE_DSN);
        assert_eq!(config.source, RuntimeStoreSource::Default);
        assert_eq!(config.mode, RuntimeStoreMode::SqliteFileBacked);
    }

    #[test]
    fn runtime_store_config_honors_env_override() {
        let config = resolve_runtime_store_config(Some("memory://custom-runtime".to_string()));
        assert_eq!(config.dsn, "memory://custom-runtime");
        assert_eq!(config.source, RuntimeStoreSource::EnvOverride);
        assert_eq!(config.mode, RuntimeStoreMode::Memory);
    }

    #[test]
    fn default_runtime_store_path_bootstraps_and_persists_rows_across_restarts() {
        let tmp = tempdir().expect("tempdir");
        let default_db_path = tmp.path().join(".agentrail").join("runtime.db");
        let config = resolve_runtime_store_config_with_default_path(None, &default_db_path);

        let store = TaskStore::connect(config.dsn.clone());
        store
            .upsert_task(&TaskRecord::new(
                "task-default-runtime-store",
                "worker-a",
                1,
            ))
            .expect("insert should succeed");
        drop(store);

        assert!(
            default_db_path.exists(),
            "default runtime sqlite file should be created on first start"
        );

        let reopened = TaskStore::connect(config.dsn);
        let task = reopened
            .get_task("task-default-runtime-store")
            .expect("read should succeed")
            .expect("task should persist across restart");
        assert_eq!(task.assigned_worker, "worker-a");
    }

    #[test]
    fn handle_tool_call_emits_structured_trace_fields_for_success() {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .with_target(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let response = handle_tool_call("orchestrate_start", json!({ "task_id": "task-123" }))
            .expect("tool call should succeed");
        assert_eq!(response["status"], "accepted");

        let logs = buffer.into_string();
        assert!(
            has_field_value(&logs, "operation", "mcp_tool_call"),
            "expected operation field in tracing output, got: {logs}"
        );
        assert!(
            has_field_value(&logs, "tool", "orchestrate_start"),
            "expected tool field in tracing output, got: {logs}"
        );
        assert!(
            has_field_value(&logs, "task_id", "task-123"),
            "expected task_id field in tracing output, got: {logs}"
        );
        assert!(
            has_field_value(&logs, "outcome", "accepted"),
            "expected outcome field in tracing output, got: {logs}"
        );
    }

    #[test]
    fn handle_tool_call_emits_structured_trace_fields_for_unsupported_tool() {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .with_target(false)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let err =
            handle_tool_call("orchestrate_unknown", json!({ "task_id": "task-123" })).unwrap_err();
        assert!(
            err.to_string().contains("unsupported tool"),
            "unexpected error: {err}"
        );

        let logs = buffer.into_string();
        assert!(
            has_field_value(&logs, "operation", "mcp_tool_call"),
            "expected operation field in tracing output, got: {logs}"
        );
        assert!(
            has_field_value(&logs, "tool", "orchestrate_unknown"),
            "expected tool field in tracing output, got: {logs}"
        );
        assert!(
            has_field_value(&logs, "outcome", "unsupported"),
            "expected unsupported outcome field in tracing output, got: {logs}"
        );
    }

    #[test]
    fn record_delivery_steer_sent_updates_observation_and_emits_event() {
        let task_id = unique_test_task_id("steer-observation");
        {
            let runtime = runtime_state().lock().expect("runtime lock");
            runtime
                .store
                .upsert_task(&TaskRecord::new(task_id.clone(), "worker-a", 2))
                .expect("seed task");
            runtime
                .store
                .transition(&task_id, TaskRuntimeState::Preparing)
                .expect("queued -> preparing");
            runtime
                .store
                .transition(&task_id, TaskRuntimeState::Running)
                .expect("preparing -> running");
        }

        let sent_at =
            record_delivery_steer_sent(&task_id, "tmux-session-1").expect("record steer sent");

        let runtime = runtime_state().lock().expect("runtime lock");
        let observation = runtime
            .last_steer_observation_by_task
            .get(&task_id)
            .expect("steer observation should be present");
        assert_eq!(observation.sent_at, sent_at);
        assert!(observation.observed_at.is_none());
        assert_eq!(observation.apply_hint, "transport_sent");
        assert!(runtime.delivery_events.iter().any(|event| {
            event.task_id == task_id
                && event.event_type == "steer_sent"
                && event.session_id.as_deref() == Some("tmux-session-1")
        }));
    }

    #[test]
    fn maybe_record_delivery_steer_observed_sets_observed_once() {
        let task_id = unique_test_task_id("steer-observed");
        {
            let runtime = runtime_state().lock().expect("runtime lock");
            runtime
                .store
                .upsert_task(&TaskRecord::new(task_id.clone(), "worker-a", 2))
                .expect("seed task");
            runtime
                .store
                .transition(&task_id, TaskRuntimeState::Preparing)
                .expect("queued -> preparing");
            runtime
                .store
                .transition(&task_id, TaskRuntimeState::Running)
                .expect("preparing -> running");
        }

        let _ = record_delivery_steer_sent(&task_id, "tmux-session-2").expect("record steer sent");
        maybe_record_delivery_steer_observed(&task_id, "tmux-session-2", "running", "running")
            .expect("record steer observed");
        maybe_record_delivery_steer_observed(&task_id, "tmux-session-2", "running", "running")
            .expect("idempotent observed update");

        let runtime = runtime_state().lock().expect("runtime lock");
        let observation = runtime
            .last_steer_observation_by_task
            .get(&task_id)
            .expect("steer observation should be present");
        assert!(observation.observed_at.is_some());
        assert_eq!(observation.apply_hint, "session_alive_after_steer");
        assert_eq!(
            runtime
                .delivery_events
                .iter()
                .filter(|event| event.task_id == task_id && event.event_type == "steer_observed")
                .count(),
            1,
            "steer_observed event should be emitted once"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn poll_runtime_once_reports_task_failures_and_cleans_missing_sessions() {
        let task_id = unique_test_task_id("poll-missing-session");
        {
            let mut runtime = runtime_state().lock().expect("runtime lock");
            runtime
                .store
                .upsert_task(&TaskRecord::new(task_id.clone(), "worker-a", 1))
                .expect("seed task");
            runtime.sessions.insert(
                task_id.clone(),
                RuntimeTaskSession {
                    session_id: "process-missing-session".to_string(),
                    runner_mode: RuntimeRunnerMode::Process,
                },
            );
        }

        let err = poll_runtime_once()
            .await
            .expect_err("poll should report per-task failure");
        assert!(
            err.to_string().contains(&task_id),
            "expected task id in poll error: {err}"
        );

        let runtime = runtime_state().lock().expect("runtime lock");
        assert!(
            !runtime.sessions.contains_key(&task_id),
            "stale missing session should be removed after poll failure"
        );
    }
}
