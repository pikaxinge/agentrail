use agentrail_store::{TaskRecord, TaskRuntimeState, TaskStore};

#[test]
fn snapshot_roundtrip_restores_inflight_runtime_state() {
    let source = TaskStore::connect("memory://recovery-source");
    source
        .upsert_task(&TaskRecord::new("task-recover", "worker-a", 3))
        .expect("seed should succeed");
    source
        .transition("task-recover", TaskRuntimeState::Preparing)
        .expect("queued -> preparing");
    source
        .transition("task-recover", TaskRuntimeState::Running)
        .expect("preparing -> running");
    source
        .increment_retry("task-recover")
        .expect("retry increment should succeed");

    let snapshot = source
        .export_snapshot()
        .expect("snapshot export should succeed");

    let restored = TaskStore::connect("memory://recovery-restored");
    restored
        .import_snapshot(snapshot)
        .expect("snapshot import should succeed");

    let task = restored
        .get_task("task-recover")
        .expect("get should succeed")
        .expect("task should exist after restore");

    assert_eq!(task.assigned_worker, "worker-a");
    assert_eq!(task.state, TaskRuntimeState::Running);
    assert_eq!(task.retry_count, 1);
    assert_eq!(task.retry_budget, 3);
}

#[test]
fn import_snapshot_is_atomic_when_snapshot_is_invalid() {
    let store = TaskStore::connect("memory://recovery-atomic");
    store
        .upsert_task(&TaskRecord::new("existing-task", "worker-a", 3))
        .expect("seed should succeed");

    let invalid_snapshot = agentrail_store::TaskStoreSnapshot {
        tasks: vec![
            TaskRecord::new("dup-task", "worker-a", 3),
            TaskRecord::new("dup-task", "worker-b", 3),
        ],
    };

    let err = store
        .import_snapshot(invalid_snapshot)
        .expect_err("duplicate ids must fail");
    assert!(
        err.to_string().contains("duplicate task id"),
        "unexpected error: {err}"
    );

    let existing = store
        .get_task("existing-task")
        .expect("get should succeed")
        .expect("existing task should remain after failed import");
    assert_eq!(existing.assigned_worker, "worker-a");
}
