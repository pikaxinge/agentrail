use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus, recalc_lock_status};

#[test]
fn recalc_changes_phase_to_done_when_all_steps_complete() {
    let mut plan = Plan {
        version: 1,
        project: "demo".to_string(),
        phases: vec![Phase {
            id: "core".to_string(),
            name: "Core".to_string(),
            status: PhaseStatus::Pending,
            depends_on: vec![],
            steps: vec![Step {
                id: "core.one".to_string(),
                name: "One".to_string(),
                status: StepStatus::Done,
                depends_on: vec![],
                claimed_by: None,
                evidence: Some("ok".to_string()),
            }],
        }],
    };

    let changed = recalc_lock_status(&mut plan);
    assert_eq!(changed, vec!["core".to_string()]);
    assert!(matches!(plan.phases[0].status, PhaseStatus::Done));
}
