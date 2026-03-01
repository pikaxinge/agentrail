use agentrail_orchestrator::{RepairAction, decide_review_failure_action};

#[test]
fn review_failure_wakes_original_agent_before_budget_exhausted() {
    let action = decide_review_failure_action(1, 3, false);
    assert_eq!(action, RepairAction::WakeOriginalAgent);
}

#[test]
fn first_failure_wakes_original_agent() {
    let action = decide_review_failure_action(0, 3, false);
    assert_eq!(action, RepairAction::WakeOriginalAgent);
}

#[test]
fn review_failure_reassigns_when_budget_exhausted() {
    let action = decide_review_failure_action(3, 3, false);
    assert_eq!(action, RepairAction::ReassignFreshWorker);
}

#[test]
fn review_failure_escalates_after_reassignment_budget_exhausted() {
    let action = decide_review_failure_action(3, 3, true);
    assert_eq!(action, RepairAction::EscalateNeedsAttention);
}

#[test]
fn reassigned_worker_still_gets_wake_before_budget_exhausted() {
    let action = decide_review_failure_action(1, 3, true);
    assert_eq!(action, RepairAction::WakeOriginalAgent);
}

#[test]
fn zero_retry_budget_escalates_immediately() {
    let action = decide_review_failure_action(0, 0, false);
    assert_eq!(action, RepairAction::EscalateNeedsAttention);
}
