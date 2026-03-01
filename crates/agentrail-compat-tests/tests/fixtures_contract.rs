use std::{fs, path::PathBuf};

use serde_json::Value;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

#[test]
fn orchestrator_contract_fixtures_exist_and_parse() {
    let plan = load_fixture("orchestrate_plan.json");
    assert_eq!(plan["operation"], "orchestrate_plan");
    assert!(plan["max_parallel"].is_number());

    let tick = load_fixture("orchestrate_tick.json");
    assert_eq!(tick["operation"], "orchestrate_tick");
    assert!(tick["repair_action"].is_string());

    let resume = load_fixture("orchestrate_resume.json");
    assert_eq!(resume["operation"], "orchestrate_resume");
    assert!(resume["task_id"].is_string());

    let start = load_fixture("mcp_orchestrate_start.json");
    assert_eq!(start["tool"], "orchestrate_start");
    assert!(start["task_id"].is_string());
    assert_eq!(start["status"], "accepted");

    let status = load_fixture("mcp_orchestrate_status.json");
    assert_eq!(status["tool"], "orchestrate_status");
    assert!(status["task_id"].is_string());
    assert_eq!(status["state"], "running");

    let steer = load_fixture("mcp_orchestrate_steer.json");
    assert_eq!(steer["tool"], "orchestrate_steer");
    assert!(steer["task_id"].is_string());
    assert_eq!(steer["status"], "sent");
}

fn load_fixture(name: &str) -> Value {
    let path = fixture_path(name);
    assert!(path.exists(), "missing fixture: {}", path.display());

    let raw = fs::read_to_string(&path).expect("fixture should be readable");
    serde_json::from_str(&raw).expect("fixture should be valid json")
}
