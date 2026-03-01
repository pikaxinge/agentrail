use anyhow::Result;
use serde_json::{Value, json};
use tracing::{info, warn};

pub async fn run_stdio() -> Result<()> {
    info!(
        operation = "mcp_server_bootstrap",
        transport = "stdio",
        outcome = "ok",
        "agentrail MCP stdio server bootstrap"
    );
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

fn require_task_id<'a>(tool_name: &str, args: &'a Value) -> Result<&'a str> {
    let Some(task_id) = args.get("task_id").and_then(Value::as_str) else {
        warn!(
            operation = "mcp_tool_call",
            tool = tool_name,
            outcome = "missing_task_id",
            "MCP tool call missing task_id"
        );
        anyhow::bail!("missing task_id");
    };

    info!(
        operation = "mcp_tool_call",
        tool = tool_name,
        task_id = task_id,
        outcome = "validated",
        "validated MCP tool call arguments"
    );

    Ok(task_id)
}

pub fn handle_tool_call(tool_name: &str, args: Value) -> Result<Value> {
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
