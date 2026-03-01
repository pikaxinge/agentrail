use anyhow::Result;
use serde_json::{Value, json};
use tracing::info;

pub async fn run_stdio() -> Result<()> {
    info!("agentrail MCP stdio server bootstrap");
    Ok(())
}

pub async fn run_http(bind: &str) -> Result<()> {
    info!(bind = bind, "agentrail MCP HTTP server bootstrap");
    Ok(())
}

pub fn handle_tool_call(tool_name: &str, args: Value) -> Result<Value> {
    match tool_name {
        "orchestrate_start" => {
            let task_id = args
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing task_id"))?;
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "status": "accepted"
            }))
        }
        "orchestrate_status" => {
            let task_id = args
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing task_id"))?;
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "state": "running"
            }))
        }
        "orchestrate_steer" => {
            let task_id = args
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing task_id"))?;
            Ok(json!({
                "tool": tool_name,
                "task_id": task_id,
                "status": "sent"
            }))
        }
        _ => anyhow::bail!("unsupported tool: {tool_name}"),
    }
}
