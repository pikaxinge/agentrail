use crate::models::{PhaseStatus, Plan, StepStatus};
use std::collections::HashMap;

pub fn recalc_lock_status(plan: &mut Plan) -> Vec<String> {
    let mut changed = Vec::new();

    let base_status_by_id: HashMap<String, PhaseStatus> = plan
        .phases
        .iter()
        .map(|phase| {
            let complete = phase
                .steps
                .iter()
                .all(|s| matches!(s.status, StepStatus::Done | StepStatus::Skipped));
            let any_claimed = phase.steps.iter().any(|s| s.status == StepStatus::Claimed);
            let base = if complete {
                PhaseStatus::Done
            } else if any_claimed {
                PhaseStatus::InProgress
            } else {
                PhaseStatus::Pending
            };
            (phase.id.clone(), base)
        })
        .collect();

    for phase in &mut plan.phases {
        let base = base_status_by_id
            .get(&phase.id)
            .cloned()
            .unwrap_or(PhaseStatus::Pending);
        let deps_ready = phase
            .depends_on
            .iter()
            .all(|dep| matches!(base_status_by_id.get(dep), Some(PhaseStatus::Done)));

        let next = if base == PhaseStatus::Done {
            PhaseStatus::Done
        } else if phase.status == PhaseStatus::Locked && phase.depends_on.is_empty() {
            // Keep explicitly locked phases locked unless they become done.
            PhaseStatus::Locked
        } else if !phase.depends_on.is_empty() && !deps_ready {
            PhaseStatus::Locked
        } else {
            base
        };

        if phase.status != next {
            phase.status = next;
            changed.push(phase.id.clone());
        }
    }

    changed
}

#[cfg(test)]
mod tests {
    use super::recalc_lock_status;
    use crate::models::{Phase, PhaseStatus, Plan, Step, StepStatus};

    fn step(id: &str, status: StepStatus) -> Step {
        Step {
            id: id.to_string(),
            name: id.to_string(),
            status,
            depends_on: Vec::new(),
            claimed_by: None,
            evidence: None,
        }
    }

    #[test]
    fn keeps_explicit_locked_phase_locked() {
        let mut plan = Plan {
            version: 1,
            project: "demo".to_string(),
            phases: vec![
                Phase {
                    id: "locked".to_string(),
                    name: "Locked".to_string(),
                    status: PhaseStatus::Locked,
                    depends_on: Vec::new(),
                    steps: vec![step("s1", StepStatus::Pending)],
                },
                Phase {
                    id: "open".to_string(),
                    name: "Open".to_string(),
                    status: PhaseStatus::Pending,
                    depends_on: Vec::new(),
                    steps: vec![step("s2", StepStatus::Done)],
                },
            ],
        };

        recalc_lock_status(&mut plan);

        assert_eq!(plan.phases[0].status, PhaseStatus::Locked);
        assert_eq!(plan.phases[1].status, PhaseStatus::Done);
    }

    #[test]
    fn locks_and_unlocks_dependent_phase_from_depends_on() {
        let mut plan = Plan {
            version: 1,
            project: "demo".to_string(),
            phases: vec![
                Phase {
                    id: "phase-a".to_string(),
                    name: "A".to_string(),
                    status: PhaseStatus::Pending,
                    depends_on: Vec::new(),
                    steps: vec![step("a1", StepStatus::Pending)],
                },
                Phase {
                    id: "phase-b".to_string(),
                    name: "B".to_string(),
                    status: PhaseStatus::Pending,
                    depends_on: vec!["phase-a".to_string()],
                    steps: vec![step("b1", StepStatus::Pending)],
                },
            ],
        };

        recalc_lock_status(&mut plan);
        assert_eq!(plan.phases[1].status, PhaseStatus::Locked);

        plan.phases[0].steps[0].status = StepStatus::Done;
        recalc_lock_status(&mut plan);
        assert_eq!(plan.phases[0].status, PhaseStatus::Done);
        assert_eq!(plan.phases[1].status, PhaseStatus::Pending);
    }
}
