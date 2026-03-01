use std::path::Path;

use agentrail_store::{TaskRecord, TaskRuntimeState, TaskStore, TaskStoreSnapshot};
use tempfile::tempdir;

fn sqlite_dsn(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

#[test]
fn sqlite_store_persists_task_state_across_reconnect() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn.clone());
    store
        .upsert_task(&TaskRecord::new("task-sqlite-1", "worker-a", 3))
        .expect("insert should succeed");
    store
        .transition("task-sqlite-1", TaskRuntimeState::Preparing)
        .expect("queued -> preparing");
    store
        .transition("task-sqlite-1", TaskRuntimeState::Running)
        .expect("preparing -> running");
    store
        .increment_retry("task-sqlite-1")
        .expect("retry increment");
    drop(store);

    let reopened = TaskStore::connect(dsn);
    let task = reopened
        .get_task("task-sqlite-1")
        .expect("read should succeed")
        .expect("task should persist");
    assert_eq!(task.assigned_worker, "worker-a");
    assert_eq!(task.state, TaskRuntimeState::Running);
    assert_eq!(task.retry_count, 1);
    assert_eq!(task.retry_budget, 3);
}

#[test]
fn sqlite_import_snapshot_is_atomic_when_invalid() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("existing-task", "worker-a", 3))
        .expect("seed should succeed");

    let invalid_snapshot = TaskStoreSnapshot {
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
        .expect("existing task should remain");
    assert_eq!(existing.assigned_worker, "worker-a");
}

#[test]
fn sqlite_file_backed_store_sets_wal_journal_mode() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-sqlite-2", "worker-a", 1))
        .expect("insert should succeed");
    drop(store);

    let connection =
        rusqlite::Connection::open(db_path).expect("open sqlite connection for journal check");
    let mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
        .expect("read journal_mode");
    assert_eq!(mode.to_lowercase(), "wal");
}

#[test]
fn sqlite_store_creates_missing_parent_directories() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("nested").join("runtime").join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn.clone());
    store
        .upsert_task(&TaskRecord::new("task-sqlite-parent-create", "worker-a", 1))
        .expect("insert should succeed");
    drop(store);

    assert!(
        db_path.exists(),
        "sqlite file should exist after connecting with nested DSN"
    );

    let reopened = TaskStore::connect(dsn);
    let task = reopened
        .get_task("task-sqlite-parent-create")
        .expect("read should succeed")
        .expect("task should persist");
    assert_eq!(task.assigned_worker, "worker-a");
}

#[test]
fn unsupported_dsn_surfaces_runtime_error_instead_of_silent_memory_fallback() {
    let store = TaskStore::connect("bad://runtime-db");
    let err = store
        .upsert_task(&TaskRecord::new("task-unsupported-dsn", "worker-a", 1))
        .expect_err("unsupported dsn should not silently fallback");
    assert!(err.to_string().contains("unsupported task store dsn"));
}
