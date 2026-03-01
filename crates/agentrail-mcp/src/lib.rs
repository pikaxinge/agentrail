use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tracing::{info, warn};

use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus};

pub async fn run_stdio() -> Result<()> {
    info!(
        operation = "mcp_server_bootstrap",
        transport = "stdio",
        outcome = "ok",
        "agentrail MCP stdio server bootstrap"
    );

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
                writer.write_all(serde_json::to_string(&response)?.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                continue;
            }
        };

        if let Some(response) = handle_mcp_request(parsed)? {
            writer.write_all(serde_json::to_string(&response)?.as_bytes()).await?;
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
    Ok(())
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
        }
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
            let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
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

pub fn handle_tool_call_with_allowed_root(
    tool_name: &str,
    args: Value,
    allowed_root: Option<&Path>,
) -> Result<Value> {
    match tool_name {
        "orchestrate_start" => {
            let task_id = require_task_id(tool_name, &args)?;
            info!(
                operation = "mcp_tool_call",
                tool = tool_name,
                task_id = task_id,
                outcome = "accepted",
                "handled MCP tool call"
            );
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "status": "accepted"
            }))
        }
        "orchestrate_status" => {
            let task_id = require_task_id(tool_name, &args)?;
            info!(
                operation = "mcp_tool_call",
                tool = tool_name,
                task_id = task_id,
                outcome = "running",
                "handled MCP tool call"
            );
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "state": "running"
            }))
        }
        "orchestrate_steer" => {
            let task_id = require_task_id(tool_name, &args)?;
            info!(
                operation = "mcp_tool_call",
                tool = tool_name,
                task_id = task_id,
                outcome = "sent",
                "handled MCP tool call"
            );
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "status": "sent"
            }))
        }
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
    };

    use super::*;
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
}
