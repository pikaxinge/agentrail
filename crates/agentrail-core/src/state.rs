use crate::models::{PhaseStatus, Plan, StepStatus};

pub fn recalc_lock_status(plan: &mut Plan) -> Vec<String> {
    let mut changed = Vec::new();

    for phase in &mut plan.phases {
        let complete = phase
            .steps
            .iter()
            .all(|s| matches!(s.status, StepStatus::Done | StepStatus::Skipped));
        let any_claimed = phase.steps.iter().any(|s| s.status == StepStatus::Claimed);

        let next = if complete {
            PhaseStatus::Done
        } else if any_claimed {
            PhaseStatus::InProgress
        } else {
            PhaseStatus::Pending
        };

        if phase.status != next {
            phase.status = next;
            changed.push(phase.id.clone());
        }
    }

    changed
}
