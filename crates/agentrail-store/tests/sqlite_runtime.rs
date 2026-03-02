use std::{collections::HashSet, path::Path};

use agentrail_store::{TaskEventDraft, TaskRecord, TaskRuntimeState, TaskStore, TaskStoreSnapshot};
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

#[test]
fn sqlite_store_persists_attempts_and_sessions_across_reconnect() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn.clone());
    let attempt = store
        .register_task_attempt("task-with-attempts", "session-1", "process")
        .expect("attempt insert should succeed");
    assert_eq!(attempt.attempt_number, 1);
    store
        .finalize_task_session("session-1", "completed")
        .expect("session finalize should succeed");
    drop(store);

    let reopened = TaskStore::connect(dsn);
    let attempts = reopened
        .list_task_attempts("task-with-attempts")
        .expect("attempt query should succeed");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].session_id, "session-1");
    assert_eq!(attempts[0].trace_id, "trace:task-with-attempts");
    assert_eq!(attempts[0].span_id, "span:task-with-attempts:1");
    assert!(attempts[0].parent_span_id.is_none());
    assert_eq!(attempts[0].terminal_state.as_deref(), Some("completed"));
    assert!(attempts[0].ended_epoch_ms.is_some());

    let sessions = reopened
        .list_task_sessions("task-with-attempts")
        .expect("session query should succeed");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, "session-1");
    assert_eq!(sessions[0].trace_id, "trace:task-with-attempts");
    assert_eq!(sessions[0].span_id, "span:task-with-attempts:1");
    assert!(sessions[0].parent_span_id.is_none());
    assert_eq!(sessions[0].terminal_state.as_deref(), Some("completed"));
    assert!(sessions[0].ended_epoch_ms.is_some());
}

#[test]
fn sqlite_store_attempt_trace_lineage_supports_inheritance_and_overrides() {
    let store = TaskStore::connect("memory://trace-lineage");

    let first = store
        .register_task_attempt("task-trace-lineage", "session-1", "process")
        .expect("register first attempt");
    assert_eq!(first.trace_id, "trace:task-trace-lineage");
    assert_eq!(first.span_id, "span:task-trace-lineage:1");
    assert_eq!(first.parent_span_id, None);

    let second = store
        .register_task_attempt("task-trace-lineage", "session-2", "process")
        .expect("register second attempt");
    assert_eq!(second.trace_id, first.trace_id);
    assert_eq!(second.span_id, "span:task-trace-lineage:2");
    assert_eq!(
        second.parent_span_id.as_deref(),
        Some(first.span_id.as_str())
    );

    let third = store
        .register_task_attempt_with_trace(
            "task-trace-lineage",
            "session-3",
            "process",
            Some("trace:external"),
            Some("span:external-parent"),
        )
        .expect("register third attempt with explicit trace context");
    assert_eq!(third.trace_id, "trace:external");
    assert_eq!(third.span_id, "span:task-trace-lineage:3");
    assert_eq!(
        third.parent_span_id.as_deref(),
        Some("span:external-parent")
    );

    let fourth = store
        .register_task_attempt_with_trace(
            "task-trace-lineage",
            "session-4",
            "process",
            Some("trace:new-root"),
            None,
        )
        .expect("register fourth attempt with new trace root");
    assert_eq!(fourth.trace_id, "trace:new-root");
    assert_eq!(fourth.span_id, "span:task-trace-lineage:4");
    assert!(
        fourth.parent_span_id.is_none(),
        "new trace without explicit parent must not inherit prior span"
    );

    let latest = store
        .latest_trace_context_for_task("task-trace-lineage")
        .expect("latest trace context should load")
        .expect("latest trace context should exist");
    assert_eq!(latest.attempt_number, 4);
    assert_eq!(latest.trace_id, fourth.trace_id);
    assert_eq!(latest.span_id, fourth.span_id);
    assert_eq!(latest.parent_span_id, fourth.parent_span_id);
}

#[test]
fn sqlite_store_creates_task_events_schema_and_indexes() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-schema-check", "worker-a", 1))
        .expect("insert should succeed");
    drop(store);

    let connection = rusqlite::Connection::open(db_path).expect("open sqlite connection");

    let mut columns_stmt = connection
        .prepare("PRAGMA table_info(task_events)")
        .expect("prepare table_info");
    let columns = columns_stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query table_info")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect columns");

    let expected_columns = [
        "id",
        "task_id",
        "attempt",
        "ts_ms",
        "event_type",
        "source",
        "actor",
        "session_id",
        "state_before",
        "state_after",
        "message",
        "payload_json",
        "idem_key",
    ];
    for column in expected_columns {
        assert!(
            columns.iter().any(|value| value == column),
            "expected task_events column `{column}` to exist; columns={columns:?}"
        );
    }

    let mut index_stmt = connection
        .prepare("PRAGMA index_list(task_events)")
        .expect("prepare index_list");
    let index_names = index_stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query index_list")
        .collect::<Result<HashSet<_>, _>>()
        .expect("collect index names");

    for index_name in [
        "idx_task_events_task_id",
        "idx_task_events_task_attempt_id",
        "idx_task_events_ts_ms",
        "idx_task_events_task_idem_key",
    ] {
        assert!(
            index_names.contains(index_name),
            "expected index `{index_name}` to exist; indexes={index_names:?}"
        );
    }
}

#[test]
fn sqlite_store_appends_and_lists_task_events_in_order() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn.clone());
    store
        .upsert_task(&TaskRecord::new("task-events-order", "worker-a", 2))
        .expect("insert should succeed");

    store
        .append_task_event(&TaskEventDraft {
            task_id: "task-events-order".to_string(),
            attempt: 1,
            ts_ms: 1000,
            event_type: "submitted".to_string(),
            source: "mcp".to_string(),
            actor: Some("owner-orchestrator".to_string()),
            session_id: Some("session-1".to_string()),
            state_before: Some("queued".to_string()),
            state_after: Some("preparing".to_string()),
            message: Some("task submitted".to_string()),
            payload_json: Some("{\"k\":\"v\"}".to_string()),
            idem_key: Some("event-1".to_string()),
        })
        .expect("append first event");
    store
        .append_task_event(&TaskEventDraft {
            task_id: "task-events-order".to_string(),
            attempt: 2,
            ts_ms: 500,
            event_type: "running".to_string(),
            source: "runtime".to_string(),
            actor: None,
            session_id: Some("session-2".to_string()),
            state_before: Some("preparing".to_string()),
            state_after: Some("running".to_string()),
            message: Some("task running".to_string()),
            payload_json: None,
            idem_key: Some("event-2".to_string()),
        })
        .expect("append second event");
    drop(store);

    let reopened = TaskStore::connect(dsn);
    let all = reopened
        .list_task_events("task-events-order", None)
        .expect("list all events");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].event_type, "submitted");
    assert_eq!(all[1].event_type, "running");

    let attempt_two = reopened
        .list_task_events("task-events-order", Some(2))
        .expect("list attempt-specific events");
    assert_eq!(attempt_two.len(), 1);
    assert_eq!(attempt_two[0].attempt, 2);
    assert_eq!(attempt_two[0].session_id.as_deref(), Some("session-2"));
}

#[test]
fn sqlite_store_task_event_idem_key_deduplicates_repeated_writes() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-events-idem", "worker-a", 2))
        .expect("insert should succeed");

    let first = store
        .append_task_event(&TaskEventDraft {
            task_id: "task-events-idem".to_string(),
            attempt: 1,
            ts_ms: 1111,
            event_type: "status_changed".to_string(),
            source: "mcp".to_string(),
            actor: None,
            session_id: None,
            state_before: Some("queued".to_string()),
            state_after: Some("preparing".to_string()),
            message: None,
            payload_json: None,
            idem_key: Some("stable-key".to_string()),
        })
        .expect("first append");
    let second = store
        .append_task_event(&TaskEventDraft {
            task_id: "task-events-idem".to_string(),
            attempt: 9,
            ts_ms: 9999,
            event_type: "status_changed".to_string(),
            source: "mcp".to_string(),
            actor: None,
            session_id: None,
            state_before: Some("queued".to_string()),
            state_after: Some("preparing".to_string()),
            message: None,
            payload_json: None,
            idem_key: Some("stable-key".to_string()),
        })
        .expect("second append with same idem key");

    assert_eq!(first.id, second.id);
    assert_eq!(first.ts_ms, second.ts_ms);
    assert_eq!(first.attempt, second.attempt);

    let all = store
        .list_task_events("task-events-idem", None)
        .expect("list events");
    assert_eq!(all.len(), 1);
}

#[test]
fn sqlite_store_task_event_idem_key_is_scoped_per_task() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-idem-a", "worker-a", 2))
        .expect("insert task-idem-a");
    store
        .upsert_task(&TaskRecord::new("task-idem-b", "worker-a", 2))
        .expect("insert task-idem-b");

    let first = store
        .append_task_event(&TaskEventDraft {
            task_id: "task-idem-a".to_string(),
            attempt: 1,
            ts_ms: 10,
            event_type: "status_changed".to_string(),
            source: "mcp".to_string(),
            actor: None,
            session_id: None,
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("same-key".to_string()),
        })
        .expect("append task-idem-a");
    let second = store
        .append_task_event(&TaskEventDraft {
            task_id: "task-idem-b".to_string(),
            attempt: 1,
            ts_ms: 20,
            event_type: "status_changed".to_string(),
            source: "mcp".to_string(),
            actor: None,
            session_id: None,
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("same-key".to_string()),
        })
        .expect("append task-idem-b");

    assert_ne!(first.id, second.id);
    assert_eq!(
        store
            .list_task_events("task-idem-a", None)
            .expect("list task-idem-a events")
            .len(),
        1
    );
    assert_eq!(
        store
            .list_task_events("task-idem-b", None)
            .expect("list task-idem-b events")
            .len(),
        1
    );
}

#[test]
fn sqlite_import_snapshot_prunes_removed_task_metadata_without_wiping_retained() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-retained", "worker-a", 2))
        .expect("seed retained task");
    store
        .upsert_task(&TaskRecord::new("task-removed", "worker-b", 2))
        .expect("seed removed task");

    store
        .register_task_attempt("task-retained", "session-retained", "process")
        .expect("register retained attempt");
    store
        .register_task_attempt("task-removed", "session-removed", "process")
        .expect("register removed attempt");
    store
        .append_task_event(&TaskEventDraft {
            task_id: "task-retained".to_string(),
            attempt: 1,
            ts_ms: 100,
            event_type: "custom".to_string(),
            source: "test".to_string(),
            actor: None,
            session_id: Some("session-retained".to_string()),
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("retained-event".to_string()),
        })
        .expect("append retained event");
    store
        .append_task_event(&TaskEventDraft {
            task_id: "task-removed".to_string(),
            attempt: 1,
            ts_ms: 200,
            event_type: "custom".to_string(),
            source: "test".to_string(),
            actor: None,
            session_id: Some("session-removed".to_string()),
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("removed-event".to_string()),
        })
        .expect("append removed event");

    store
        .import_snapshot(TaskStoreSnapshot {
            tasks: vec![TaskRecord::new("task-retained", "worker-a2", 3)],
        })
        .expect("snapshot import should succeed");

    let retained = store
        .get_task("task-retained")
        .expect("get retained")
        .expect("retained task should still exist");
    assert_eq!(retained.assigned_worker, "worker-a2");
    assert_eq!(retained.retry_budget, 3);
    assert_eq!(
        store
            .list_task_attempts("task-retained")
            .expect("list retained attempts")
            .len(),
        1,
        "retained attempts should survive snapshot import"
    );
    let retained_events = store
        .list_task_events("task-retained", None)
        .expect("list retained events");
    assert!(
        retained_events
            .iter()
            .any(|event| event.event_type == "attempt_registered"),
        "retained attempt_registered event should survive snapshot import"
    );
    assert!(
        retained_events
            .iter()
            .any(|event| event.idem_key.as_deref() == Some("retained-event")),
        "retained custom event should survive snapshot import"
    );

    assert!(
        store
            .get_task("task-removed")
            .expect("get removed")
            .is_none(),
        "removed task should be deleted"
    );
    assert_eq!(
        store
            .list_task_attempts("task-removed")
            .expect("list removed attempts")
            .len(),
        0,
        "removed attempts should be deleted"
    );
    assert_eq!(
        store
            .list_task_sessions("task-removed")
            .expect("list removed sessions")
            .len(),
        0,
        "removed sessions should be deleted"
    );
    assert_eq!(
        store
            .list_task_events("task-removed", None)
            .expect("list removed events")
            .len(),
        0,
        "removed events should be deleted"
    );
}

#[test]
fn sqlite_finalize_task_session_emits_single_event_with_effective_terminal_state() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-finalize-idem", "worker-a", 2))
        .expect("seed task");
    store
        .register_task_attempt("task-finalize-idem", "session-finalize", "process")
        .expect("register attempt");

    store
        .finalize_task_session("session-finalize", "failed")
        .expect("first finalize should succeed");
    store
        .finalize_task_session("session-finalize", "completed")
        .expect("second finalize should be idempotent");

    let sessions = store
        .list_task_sessions("task-finalize-idem")
        .expect("list sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].terminal_state.as_deref(), Some("failed"));

    let finalized_events = store
        .list_task_events("task-finalize-idem", None)
        .expect("list events")
        .into_iter()
        .filter(|event| event.event_type == "session_finalized")
        .collect::<Vec<_>>();
    assert_eq!(finalized_events.len(), 1);
    assert_eq!(finalized_events[0].state_after.as_deref(), Some("failed"));
}

#[test]
fn sqlite_store_migrates_legacy_task_events_schema_missing_idem_key() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("legacy-runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let conn = rusqlite::Connection::open(&db_path).expect("open legacy sqlite");
    conn.execute_batch(
        "CREATE TABLE task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            ts_ms INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL
        );",
    )
    .expect("create legacy task_events");
    drop(conn);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("legacy-task", "worker-a", 1))
        .expect("seed task");
    store
        .append_task_event(&TaskEventDraft {
            task_id: "legacy-task".to_string(),
            attempt: 1,
            ts_ms: 1234,
            event_type: "status_changed".to_string(),
            source: "mcp".to_string(),
            actor: None,
            session_id: None,
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("legacy-key".to_string()),
        })
        .expect("append event after migration");

    let events = store
        .list_task_events("legacy-task", None)
        .expect("list migrated events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].idem_key.as_deref(), Some("legacy-key"));
}

#[test]
fn sqlite_transition_to_preparing_uses_next_attempt_number() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn);
    store
        .upsert_task(&TaskRecord::new("task-preparing-attempt", "worker-a", 2))
        .expect("seed task");
    store
        .transition("task-preparing-attempt", TaskRuntimeState::Preparing)
        .expect("queued -> preparing");

    let state_events = store
        .list_task_events("task-preparing-attempt", None)
        .expect("list task events")
        .into_iter()
        .filter(|event| event.event_type == "state_transition")
        .collect::<Vec<_>>();
    assert_eq!(state_events.len(), 1);
    assert_eq!(state_events[0].state_before.as_deref(), Some("queued"));
    assert_eq!(state_events[0].state_after.as_deref(), Some("preparing"));
    assert_eq!(state_events[0].attempt, 1);
}

#[test]
fn sqlite_import_snapshot_cleans_legacy_orphan_metadata_before_task_reintroduction() {
    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("runtime.db");
    let dsn = sqlite_dsn(&db_path);

    let store = TaskStore::connect(dsn.clone());
    store
        .upsert_task(&TaskRecord::new("legacy-orphan-task", "worker-a", 2))
        .expect("seed legacy-orphan-task");
    store
        .register_task_attempt("legacy-orphan-task", "legacy-orphan-session", "process")
        .expect("register legacy orphan attempt");
    store
        .append_task_event(&TaskEventDraft {
            task_id: "legacy-orphan-task".to_string(),
            attempt: 1,
            ts_ms: 300,
            event_type: "custom".to_string(),
            source: "test".to_string(),
            actor: None,
            session_id: Some("legacy-orphan-session".to_string()),
            state_before: None,
            state_after: None,
            message: None,
            payload_json: None,
            idem_key: Some("legacy-orphan-event".to_string()),
        })
        .expect("append legacy orphan event");
    drop(store);

    let connection = rusqlite::Connection::open(&db_path).expect("open sqlite");
    connection
        .execute("DELETE FROM tasks WHERE id = ?1", ["legacy-orphan-task"])
        .expect("simulate legacy orphan metadata");
    drop(connection);

    let store = TaskStore::connect(dsn);
    store
        .import_snapshot(TaskStoreSnapshot {
            tasks: vec![TaskRecord::new("legacy-orphan-task", "worker-b", 1)],
        })
        .expect("snapshot import should succeed");

    assert_eq!(
        store
            .list_task_attempts("legacy-orphan-task")
            .expect("list attempts")
            .len(),
        0,
        "legacy orphan attempts should be cleaned before reintroducing task id"
    );
    assert_eq!(
        store
            .list_task_events("legacy-orphan-task", None)
            .expect("list events")
            .len(),
        0,
        "legacy orphan events should be cleaned before reintroducing task id"
    );
}
