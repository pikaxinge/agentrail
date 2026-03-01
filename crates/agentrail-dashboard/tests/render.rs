use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus};

#[test]
fn generate_dashboard_renders_structured_summary_sections() {
    let plan = sample_plan();
    let html = agentrail_dashboard::generate_dashboard(&plan);

    assert!(html.contains("Agentrail Dashboard"));
    assert!(html.contains("data-testid=\"summary-total\""));
    assert!(html.contains("data-testid=\"summary-running\""));
    assert!(html.contains("data-testid=\"summary-completed\""));
    assert!(html.contains("data-testid=\"summary-failed\""));
}

#[test]
fn generate_dashboard_embeds_mermaid_dag() {
    let plan = sample_plan();
    let html = agentrail_dashboard::generate_dashboard(&plan);

    assert!(html.contains("class=\"mermaid\""));
    assert!(html.contains("graph TD"));
    assert!(html.contains("step-a"));
    assert!(html.contains("step-b"));
}

#[test]
fn render_mermaid_uses_collision_free_node_ids() {
    let plan = Plan {
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
                    status: StepStatus::Pending,
                    depends_on: vec![],
                    claimed_by: None,
                    evidence: None,
                },
                Step {
                    id: "step_a".to_string(),
                    name: "Step A Underscore".to_string(),
                    status: StepStatus::Pending,
                    depends_on: vec!["step-a".to_string()],
                    claimed_by: None,
                    evidence: None,
                },
            ],
        }],
    };

    let dag = agentrail_dashboard::render_mermaid_dag(&plan);
    assert!(dag.contains("\"step-a\""));
    assert!(dag.contains("\"step_a\""));
    assert!(dag.contains("step_2d"));
    assert!(dag.contains("step_5f"));
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
