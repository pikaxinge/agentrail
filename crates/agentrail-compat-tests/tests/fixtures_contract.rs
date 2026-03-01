use std::{fs, path::PathBuf};

use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus};
use serde_json::Value;
use serde_json::json;

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

    let cli_status = load_fixture("cli_status.json");
    assert_eq!(cli_status["operation"], "status");
    assert!(cli_status["phase_count"].is_number());
    assert!(cli_status["step_counts"]["pending"].is_number());

    let cli_show = load_fixture("cli_show.json");
    assert_eq!(cli_show["operation"], "show");
    assert!(cli_show["step"]["id"].is_string());
    assert!(cli_show["step"]["status"].is_string());

    let cli_next = load_fixture("cli_next.json");
    assert_eq!(cli_next["operation"], "next");
    assert!(cli_next["step"]["id"].is_string());

    let cli_claim = load_fixture("cli_claim.json");
    assert_eq!(cli_claim["operation"], "claim");
    assert_eq!(cli_claim["step"]["status"], "claimed");
    assert!(cli_claim["step"]["claimed_by"].is_string());

    let cli_complete = load_fixture("cli_complete.json");
    assert_eq!(cli_complete["operation"], "complete");
    assert_eq!(cli_complete["step"]["status"], "done");
    assert!(cli_complete["step"]["evidence"].is_string());

    let mcp_plan_status = load_fixture("mcp_plan_status.json");
    assert_eq!(mcp_plan_status["tool"], "plan_status");
    assert_eq!(mcp_plan_status["operation"], "status");
    assert!(mcp_plan_status["phase_count"].is_number());

    let mcp_plan_show = load_fixture("mcp_plan_show.json");
    assert_eq!(mcp_plan_show["tool"], "plan_show");
    assert_eq!(mcp_plan_show["operation"], "show");
    assert!(mcp_plan_show["step"]["id"].is_string());

    let mcp_plan_next = load_fixture("mcp_plan_next.json");
    assert_eq!(mcp_plan_next["tool"], "plan_next");
    assert_eq!(mcp_plan_next["operation"], "next");
    assert!(mcp_plan_next["step"]["id"].is_string());

    let mcp_plan_claim = load_fixture("mcp_plan_claim.json");
    assert_eq!(mcp_plan_claim["tool"], "plan_claim");
    assert_eq!(mcp_plan_claim["operation"], "claim");
    assert_eq!(mcp_plan_claim["step"]["status"], "claimed");

    let mcp_plan_complete = load_fixture("mcp_plan_complete.json");
    assert_eq!(mcp_plan_complete["tool"], "plan_complete");
    assert_eq!(mcp_plan_complete["operation"], "complete");
    assert_eq!(mcp_plan_complete["step"]["status"], "done");

    let dashboard = load_fixture("dashboard_summary.json");
    assert_eq!(dashboard["operation"], "dashboard");
    let plan = sample_plan();
    let summary = agentrail_dashboard::summarize(&plan);
    let generated_dashboard = json!({
        "operation": "dashboard",
        "project": agentrail_dashboard::ascii_safe(&plan.project),
        "summary": {
            "total": summary.total,
            "running": summary.running,
            "completed": summary.completed,
            "failed": summary.failed,
            "needs_attention": summary.needs_attention
        }
    });
    assert_eq!(dashboard, generated_dashboard);

    let dag = load_text_fixture("dag_mermaid.txt");
    let generated_dag = agentrail_dashboard::render_mermaid_dag(&plan);
    assert_eq!(dag, generated_dag);

    let report = load_text_fixture("report_markdown.md");
    let generated_report = agentrail_dashboard::render_markdown_report(&plan);
    assert_eq!(report, generated_report);
}

fn load_fixture(name: &str) -> Value {
    let path = fixture_path(name);
    assert!(path.exists(), "missing fixture: {}", path.display());

    let raw = fs::read_to_string(&path).expect("fixture should be readable");
    serde_json::from_str(&raw).expect("fixture should be valid json")
}

fn load_text_fixture(name: &str) -> String {
    let path = fixture_path(name);
    assert!(path.exists(), "missing fixture: {}", path.display());
    fs::read_to_string(path).expect("text fixture should be readable")
}

fn sample_plan() -> Plan {
    Plan {
        version: 1,
        project: "demo".to_string(),
        phases: vec![Phase {
            id: "phase-1".to_string(),
            name: "Phase 1".to_string(),
            status: PhaseStatus::InProgress,
            depends_on: vec![],
            steps: vec![
                Step {
                    id: "step-a".to_string(),
                    name: "Step A".to_string(),
                    status: StepStatus::Claimed,
                    depends_on: vec![],
                    claimed_by: Some("worker-a".to_string()),
                    evidence: None,
                },
                Step {
                    id: "step-b".to_string(),
                    name: "Step B".to_string(),
                    status: StepStatus::Pending,
                    depends_on: vec!["step-a".to_string()],
                    claimed_by: None,
                    evidence: None,
                },
            ],
        }],
    }
}
