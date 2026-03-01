use agentrail_store::{TaskRecord, TaskRuntimeState, TaskStore};

#[test]
fn upsert_then_get_roundtrip() {
    let store = TaskStore::connect("memory://runtime-roundtrip");
    let task = TaskRecord::new("task-1", "worker-a", 3);

    store.upsert_task(&task).expect("upsert should succeed");

    let loaded = store
        .get_task("task-1")
        .expect("get should succeed")
        .expect("task should exist");

    assert_eq!(loaded.id, "task-1");
    assert_eq!(loaded.assigned_worker, "worker-a");
    assert_eq!(loaded.state, TaskRuntimeState::Queued);
    assert_eq!(loaded.retry_count, 0);
    assert_eq!(loaded.retry_budget, 3);
}

#[test]
fn transition_enforces_state_machine() {
    let store = TaskStore::connect("memory://runtime-transition");
    let task = TaskRecord::new("task-2", "worker-a", 3);
    store.upsert_task(&task).expect("upsert should succeed");

    store
        .transition("task-2", TaskRuntimeState::Preparing)
        .expect("queued -> preparing should succeed");
    store
        .transition("task-2", TaskRuntimeState::Running)
        .expect("preparing -> running should succeed");
    store
        .transition("task-2", TaskRuntimeState::ReviewFailed)
        .expect("running -> review_failed should succeed");

    let err = store
        .transition("task-2", TaskRuntimeState::Merged)
        .expect_err("review_failed -> merged should be rejected");
    assert!(
        err.to_string().contains("invalid transition"),
        "expected invalid transition error, got: {err}"
    );
}

#[test]
fn increment_retry_and_reassign_worker() {
    let store = TaskStore::connect("memory://runtime-retry");
    let task = TaskRecord::new("task-3", "worker-a", 2);
    store.upsert_task(&task).expect("upsert should succeed");

    let retried = store
        .increment_retry("task-3")
        .expect("increment should succeed");
    assert_eq!(retried.retry_count, 1);

    let reassigned = store
        .reassign_worker("task-3", "worker-b")
        .expect("reassign should succeed");
    assert_eq!(reassigned.assigned_worker, "worker-b");

    let loaded = store
        .get_task("task-3")
        .expect("get should succeed")
        .expect("task should exist");
    assert_eq!(loaded.assigned_worker, "worker-b");
    assert_eq!(loaded.retry_count, 1);
}

#[test]
fn upsert_rejects_overwrite_of_existing_task() {
    let store = TaskStore::connect("memory://runtime-upsert-guard");
    let task = TaskRecord::new("task-4", "worker-a", 2);
    store
        .upsert_task(&task)
        .expect("initial insert should succeed");

    let mut overwritten = TaskRecord::new("task-4", "worker-z", 99);
    overwritten.state = TaskRuntimeState::Running;
    overwritten.retry_count = 98;

    let err = store
        .upsert_task(&overwritten)
        .expect_err("overwrite should be rejected");
    assert!(
        err.to_string().contains("already exists"),
        "expected existing-task rejection, got: {err}"
    );
}

#[test]
fn increment_retry_rejects_terminal_state() {
    let store = TaskStore::connect("memory://runtime-terminal-retry");
    let task = TaskRecord::new("task-5", "worker-a", 2);
    store.upsert_task(&task).expect("insert should succeed");

    store
        .transition("task-5", TaskRuntimeState::Preparing)
        .expect("queued -> preparing");
    store
        .transition("task-5", TaskRuntimeState::Running)
        .expect("preparing -> running");
    store
        .transition("task-5", TaskRuntimeState::ReviewFailed)
        .expect("running -> review_failed");
    store
        .transition("task-5", TaskRuntimeState::Fixing)
        .expect("review_failed -> fixing");
    store
        .transition("task-5", TaskRuntimeState::Validating)
        .expect("fixing -> validating");
    store
        .transition("task-5", TaskRuntimeState::ReadyToMerge)
        .expect("validating -> ready_to_merge");
    store
        .transition("task-5", TaskRuntimeState::Merged)
        .expect("ready_to_merge -> merged");

    let err = store
        .increment_retry("task-5")
        .expect_err("terminal state should block retry increment");
    assert!(
        err.to_string().contains("terminal state"),
        "expected terminal-state error, got: {err}"
    );
}

#[test]
fn reassign_rejects_terminal_state() {
    let store = TaskStore::connect("memory://runtime-terminal-reassign");
    let task = TaskRecord::new("task-6", "worker-a", 2);
    store.upsert_task(&task).expect("insert should succeed");

    store
        .transition("task-6", TaskRuntimeState::Preparing)
        .expect("queued -> preparing");
    store
        .transition("task-6", TaskRuntimeState::Running)
        .expect("preparing -> running");
    store
        .transition("task-6", TaskRuntimeState::FailedRetryable)
        .expect("running -> failed_retryable");
    store
        .transition("task-6", TaskRuntimeState::FailedTerminal)
        .expect("failed_retryable -> failed_terminal");

    let err = store
        .reassign_worker("task-6", "worker-b")
        .expect_err("terminal state should block reassign");
    assert!(
        err.to_string().contains("terminal state"),
        "expected terminal-state error, got: {err}"
    );
}

#[test]
fn prune_tasks_respects_scope_state_and_cutoff() {
    let store = TaskStore::connect("memory://runtime-prune");

    let mut old_ready = TaskRecord::new("task-prune-old-ready", "worker-a", 2);
    old_ready.scope_id = Some("scope-a".to_string());
    old_ready.state = TaskRuntimeState::ReadyToMerge;
    old_ready.updated_epoch_ms = 100;

    let mut new_ready = TaskRecord::new("task-prune-new-ready", "worker-a", 2);
    new_ready.scope_id = Some("scope-a".to_string());
    new_ready.state = TaskRuntimeState::ReadyToMerge;
    new_ready.updated_epoch_ms = 300;

    let mut old_running = TaskRecord::new("task-prune-old-running", "worker-a", 2);
    old_running.scope_id = Some("scope-a".to_string());
    old_running.state = TaskRuntimeState::Running;
    old_running.updated_epoch_ms = 50;

    let mut old_other_scope = TaskRecord::new("task-prune-old-other-scope", "worker-a", 2);
    old_other_scope.scope_id = Some("scope-b".to_string());
    old_other_scope.state = TaskRuntimeState::ReadyToMerge;
    old_other_scope.updated_epoch_ms = 25;

    store.upsert_task(&old_ready).expect("seed old_ready");
    store.upsert_task(&new_ready).expect("seed new_ready");
    store.upsert_task(&old_running).expect("seed old_running");
    store
        .upsert_task(&old_other_scope)
        .expect("seed old_other_scope");

    let deleted = store
        .prune_tasks(
            Some("scope-a"),
            &[TaskRuntimeState::ReadyToMerge],
            Some(250),
        )
        .expect("prune should succeed");

    assert_eq!(deleted, vec!["task-prune-old-ready".to_string()]);
    assert!(
        store
            .get_task("task-prune-old-ready")
            .expect("get old_ready")
            .is_none(),
        "old ready task should be pruned"
    );
    assert!(
        store
            .get_task("task-prune-new-ready")
            .expect("get new_ready")
            .is_some(),
        "new ready task should be retained"
    );
    assert!(
        store
            .get_task("task-prune-old-running")
            .expect("get old_running")
            .is_some(),
        "running task should be retained by state filter"
    );
    assert!(
        store
            .get_task("task-prune-old-other-scope")
            .expect("get old_other_scope")
            .is_some(),
        "other scope task should be retained by scope filter"
    );
}
