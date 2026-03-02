use agentrail_mcp::{handle_http_mcp_request, handle_mcp_request};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

fn write_plan_fixture() -> String {
    let mut plan_path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    plan_path.push(format!("agentrail-mcp-protocol-{nonce}.yaml"));
    let yaml = r#"
version: 1
project: demo
phases:
  - id: phase-1
    name: Phase 1
    status: pending
    steps:
      - id: step-a
        name: Step A
        status: pending
        depends_on: []
        claimed_by: null
        evidence: null
"#;
    std::fs::write(&plan_path, yaml).expect("write plan");
    plan_path.display().to_string()
}

#[test]
fn initialize_request_returns_server_capabilities() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":1,
        "method":"initialize",
        "params":{
            "protocolVersion":"2024-11-05",
            "capabilities":{},
            "clientInfo":{"name":"tester","version":"1.0"}
        }
    });

    let res = handle_mcp_request(req)
        .expect("initialize should succeed")
        .expect("initialize should return response");
    assert_eq!(res["jsonrpc"], "2.0");
    assert_eq!(res["id"], 1);
    assert!(res["result"]["capabilities"].is_object());
    assert_eq!(res["result"]["serverInfo"]["name"], "agentrail-mcp");
}

#[test]
fn tools_list_contains_plan_tools() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":2,
        "method":"tools/list",
        "params":{}
    });

    let res = handle_mcp_request(req)
        .expect("tools/list should succeed")
        .expect("tools/list should return response");
    let tools = res["result"]["tools"]
        .as_array()
        .expect("tools should be an array");
    assert!(tools.iter().any(|t| t["name"] == "plan_status"));
    assert!(tools.iter().any(|t| t["name"] == "plan_show"));
    assert!(tools.iter().any(|t| t["name"] == "plan_next"));
    assert!(tools.iter().any(|t| t["name"] == "plan_claim"));
    assert!(tools.iter().any(|t| t["name"] == "plan_complete"));
    assert!(tools.iter().any(|t| t["name"] == "orchestrate_start"));
    assert!(tools.iter().any(|t| t["name"] == "orchestrate_status"));
    assert!(tools.iter().any(|t| t["name"] == "orchestrate_steer"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_submit"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_status"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_steer"));
    assert!(
        tools
            .iter()
            .any(|t| t["name"] == "delivery_events_subscribe")
    );
    assert!(tools.iter().any(|t| t["name"] == "delivery_events_next"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_events_ack"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_stop"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_cleanup"));
    assert!(tools.iter().any(|t| t["name"] == "delivery_report"));
}

#[test]
fn delivery_status_tool_description_mentions_normalized_v1_contract() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":22,
        "method":"tools/list",
        "params":{}
    });

    let res = handle_mcp_request(req)
        .expect("tools/list should succeed")
        .expect("tools/list should return response");
    let tools = res["result"]["tools"]
        .as_array()
        .expect("tools should be an array");
    let delivery_status = tools
        .iter()
        .find(|tool| tool["name"] == "delivery_status")
        .expect("delivery_status tool must be present");
    let description = delivery_status["description"]
        .as_str()
        .expect("delivery_status description should be a string");

    assert!(
        description.contains("normalized v1"),
        "description should announce normalized v1 contract, got: {description}"
    );
}

#[test]
fn tools_list_includes_delivery_stop_and_cleanup_schemas() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":23,
        "method":"tools/list",
        "params":{}
    });

    let res = handle_mcp_request(req)
        .expect("tools/list should succeed")
        .expect("tools/list should return response");
    let tools = res["result"]["tools"]
        .as_array()
        .expect("tools should be an array");

    let stop = tools
        .iter()
        .find(|t| t["name"] == "delivery_stop")
        .expect("delivery_stop descriptor should exist");
    assert_eq!(stop["inputSchema"]["required"], json!(["task_id"]));
    assert_eq!(
        stop["inputSchema"]["properties"]["reason"]["type"],
        json!("string")
    );

    let cleanup = tools
        .iter()
        .find(|t| t["name"] == "delivery_cleanup")
        .expect("delivery_cleanup descriptor should exist");
    assert_eq!(
        cleanup["inputSchema"]["properties"]["force"]["type"],
        json!("boolean")
    );
    assert_eq!(
        cleanup["inputSchema"]["properties"]["retention_mode"]["enum"],
        json!(["purge", "retain"])
    );
    assert_eq!(
        cleanup["inputSchema"]["properties"]["scope_id"]["type"],
        json!("string")
    );
    assert_eq!(
        cleanup["inputSchema"]["properties"]["states"]["type"],
        json!("array")
    );
    assert_eq!(
        cleanup["inputSchema"]["properties"]["updated_before_epoch_ms"]["type"],
        json!("integer")
    );
    assert_eq!(
        cleanup["inputSchema"]["allOf"][0]["if"]["anyOf"][0]["required"],
        json!(["scope_id"])
    );
    assert_eq!(
        cleanup["inputSchema"]["allOf"][0]["if"]["anyOf"][1]["required"],
        json!(["states"])
    );
    assert_eq!(
        cleanup["inputSchema"]["allOf"][0]["then"]["required"],
        json!(["updated_before_epoch_ms"])
    );

    let submit = tools
        .iter()
        .find(|t| t["name"] == "delivery_submit")
        .expect("delivery_submit descriptor should exist");
    assert_eq!(
        submit["inputSchema"]["properties"]["runner_mode"]["enum"],
        json!(["process", "app_server"])
    );
    assert_eq!(
        submit["inputSchema"]["properties"]["steer_required"]["type"],
        json!("boolean")
    );
    assert_eq!(
        submit["inputSchema"]["properties"]["interactive_command"]["type"],
        json!("boolean")
    );
    assert_eq!(
        submit["inputSchema"]["properties"]["trace_id"]["type"],
        json!("string")
    );
    assert_eq!(
        submit["inputSchema"]["properties"]["parent_span_id"]["type"],
        json!("string")
    );

    let report = tools
        .iter()
        .find(|t| t["name"] == "delivery_report")
        .expect("delivery_report descriptor should exist");
    assert_eq!(
        report["inputSchema"]["properties"]["scope_id"]["type"],
        json!("string")
    );
    assert_eq!(
        report["inputSchema"]["properties"]["states"]["type"],
        json!("array")
    );
    assert_eq!(
        report["inputSchema"]["properties"]["updated_since_epoch_ms"]["type"],
        json!("integer")
    );
    assert_eq!(
        report["inputSchema"]["properties"]["limit"]["type"],
        json!("integer")
    );
    assert_eq!(
        report["inputSchema"]["properties"]["cursor"]["type"],
        json!("string")
    );

    let orchestrate_start = tools
        .iter()
        .find(|t| t["name"] == "orchestrate_start")
        .expect("orchestrate_start descriptor should exist");
    assert_eq!(
        orchestrate_start["inputSchema"]["properties"]["runner_mode"]["enum"],
        json!(["process", "app_server"])
    );
    assert_eq!(
        orchestrate_start["inputSchema"]["properties"]["trace_id"]["type"],
        json!("string")
    );
    assert_eq!(
        orchestrate_start["inputSchema"]["properties"]["parent_span_id"]["type"],
        json!("string")
    );
}

#[test]
fn tools_call_plan_status_returns_payload_in_text_content() {
    let plan_path = write_plan_fixture();
    let req = json!({
        "jsonrpc":"2.0",
        "id":3,
        "method":"tools/call",
        "params":{
            "name":"plan_status",
            "arguments":{"plan_path": plan_path}
        }
    });

    let res = handle_mcp_request(req)
        .expect("tools/call should succeed")
        .expect("tools/call should return response");
    assert_eq!(res["jsonrpc"], "2.0");
    assert_eq!(res["id"], 3);
    let content = res["result"]["content"]
        .as_array()
        .expect("content should be array");
    assert!(!content.is_empty());
    let text = content[0]["text"].as_str().expect("text content expected");
    let tool_result: serde_json::Value =
        serde_json::from_str(text).expect("text content should be valid json");
    assert_eq!(tool_result["operation"], "status");
    assert_eq!(tool_result["project"], "demo");
}

#[test]
fn initialized_notification_returns_no_response() {
    let req = json!({
        "jsonrpc":"2.0",
        "method":"notifications/initialized",
        "params":{}
    });

    let res = handle_mcp_request(req).expect("notification should succeed");
    assert!(res.is_none());
}

#[test]
fn http_notification_maps_to_204_without_body() {
    let req = json!({
        "jsonrpc":"2.0",
        "method":"notifications/initialized",
        "params":{}
    });

    let (status, body) = handle_http_mcp_request(req);
    assert_eq!(status, 204);
    assert!(body.is_none());
}

#[test]
fn http_error_response_preserves_request_id_when_available() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":"req-7"
    });

    let (status, body) = handle_http_mcp_request(req);
    assert_eq!(status, 200);

    let body = body.expect("error response body expected");
    assert_eq!(body["id"], "req-7");
    assert_eq!(body["error"]["code"], -32000);
}

#[test]
fn unknown_method_returns_jsonrpc_error() {
    let req = json!({
        "jsonrpc":"2.0",
        "id":9,
        "method":"unknown/method",
        "params":{}
    });

    let res = handle_mcp_request(req)
        .expect("unknown method should return error response")
        .expect("response should exist");
    assert_eq!(res["error"]["code"], -32601);
}
