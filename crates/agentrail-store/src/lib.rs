use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskRuntimeState {
    Queued,
    Preparing,
    Running,
    ReviewFailed,
    Fixing,
    Validating,
    ReadyToMerge,
    Merged,
    FailedRetryable,
    FailedTerminal,
    NeedsAttention,
}

impl TaskRuntimeState {
    fn as_db_str(self) -> &'static str {
        match self {
            TaskRuntimeState::Queued => "queued",
            TaskRuntimeState::Preparing => "preparing",
            TaskRuntimeState::Running => "running",
            TaskRuntimeState::ReviewFailed => "review_failed",
            TaskRuntimeState::Fixing => "fixing",
            TaskRuntimeState::Validating => "validating",
            TaskRuntimeState::ReadyToMerge => "ready_to_merge",
            TaskRuntimeState::Merged => "merged",
            TaskRuntimeState::FailedRetryable => "failed_retryable",
            TaskRuntimeState::FailedTerminal => "failed_terminal",
            TaskRuntimeState::NeedsAttention => "needs_attention",
        }
    }

    fn from_db_str(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(TaskRuntimeState::Queued),
            "preparing" => Ok(TaskRuntimeState::Preparing),
            "running" => Ok(TaskRuntimeState::Running),
            "review_failed" => Ok(TaskRuntimeState::ReviewFailed),
            "fixing" => Ok(TaskRuntimeState::Fixing),
            "validating" => Ok(TaskRuntimeState::Validating),
            "ready_to_merge" => Ok(TaskRuntimeState::ReadyToMerge),
            "merged" => Ok(TaskRuntimeState::Merged),
            "failed_retryable" => Ok(TaskRuntimeState::FailedRetryable),
            "failed_terminal" => Ok(TaskRuntimeState::FailedTerminal),
            "needs_attention" => Ok(TaskRuntimeState::NeedsAttention),
            _ => bail!("invalid task runtime state in store: {value}"),
        }
    }

    fn can_transition_to(self, next: TaskRuntimeState) -> bool {
        use TaskRuntimeState::*;

        match self {
            Queued => matches!(next, Preparing | FailedTerminal | NeedsAttention),
            Preparing => matches!(
                next,
                Running | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            Running => matches!(
                next,
                ReviewFailed | ReadyToMerge | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            ReviewFailed => matches!(
                next,
                Fixing | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            Fixing => matches!(
                next,
                Validating | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            Validating => matches!(
                next,
                ReviewFailed | ReadyToMerge | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            ReadyToMerge => matches!(
                next,
                Merged | FailedRetryable | FailedTerminal | NeedsAttention
            ),
            FailedRetryable => matches!(next, Queued | FailedTerminal | NeedsAttention),
            NeedsAttention => matches!(next, Queued | FailedTerminal),
            Merged | FailedTerminal => false,
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskRuntimeState::Merged | TaskRuntimeState::FailedTerminal
        )
    }
}

fn current_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as i64
}

fn default_updated_epoch_ms() -> i64 {
    current_epoch_ms()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub assigned_worker: String,
    #[serde(default)]
    pub scope_id: Option<String>,
    pub state: TaskRuntimeState,
    pub retry_count: u32,
    pub retry_budget: u32,
    #[serde(default = "default_updated_epoch_ms")]
    pub updated_epoch_ms: i64,
}

impl TaskRecord {
    pub fn new(
        id: impl Into<String>,
        assigned_worker: impl Into<String>,
        retry_budget: u32,
    ) -> Self {
        Self {
            id: id.into(),
            assigned_worker: assigned_worker.into(),
            scope_id: None,
            state: TaskRuntimeState::Queued,
            retry_count: 0,
            retry_budget,
            updated_epoch_ms: current_epoch_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskStoreSnapshot {
    pub tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskAttemptRecord {
    pub id: i64,
    pub task_id: String,
    pub attempt_number: u32,
    pub session_id: String,
    pub runner_mode: String,
    pub started_epoch_ms: i64,
    pub terminal_state: Option<String>,
    pub ended_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSessionRecord {
    pub session_id: String,
    pub task_id: String,
    pub attempt_number: u32,
    pub runner_mode: String,
    pub started_epoch_ms: i64,
    pub terminal_state: Option<String>,
    pub ended_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskEventRecord {
    pub id: i64,
    pub task_id: String,
    pub attempt: u32,
    pub ts_ms: i64,
    pub event_type: String,
    pub source: String,
    pub actor: Option<String>,
    pub session_id: Option<String>,
    pub state_before: Option<String>,
    pub state_after: Option<String>,
    pub message: Option<String>,
    pub payload_json: Option<String>,
    pub idem_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskEventDraft {
    pub task_id: String,
    pub attempt: u32,
    pub ts_ms: i64,
    pub event_type: String,
    pub source: String,
    pub actor: Option<String>,
    pub session_id: Option<String>,
    pub state_before: Option<String>,
    pub state_after: Option<String>,
    pub message: Option<String>,
    pub payload_json: Option<String>,
    pub idem_key: Option<String>,
}

#[derive(Debug)]
pub struct TaskStore {
    pub dsn: String,
    runtime: RuntimeBackend,
}

#[derive(Debug)]
enum RuntimeBackend {
    Ready(Mutex<Connection>),
    InitError(String),
}

impl TaskStore {
    pub fn connect(dsn: impl Into<String>) -> Self {
        let dsn = dsn.into();
        let runtime = match Self::open_connection(&dsn) {
            Ok(connection) => RuntimeBackend::Ready(Mutex::new(connection)),
            Err(error) => RuntimeBackend::InitError(error.to_string()),
        };

        Self { dsn, runtime }
    }

    fn open_connection(dsn: &str) -> Result<Connection> {
        let (connection, file_backed) = match dsn.strip_prefix("sqlite://") {
            Some(path) => {
                Self::ensure_sqlite_parent_dir(path)?;
                (Connection::open(path)?, true)
            }
            None if dsn.starts_with("memory://") => (Connection::open_in_memory()?, false),
            None => bail!("unsupported task store dsn: {dsn}"),
        };

        if file_backed {
            connection.pragma_update(None, "journal_mode", "WAL")?;
            let journal_mode: String =
                connection
                    .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?;
            if journal_mode.to_lowercase() != "wal" {
                bail!("failed to enable WAL journal_mode, got: {journal_mode}");
            }
        }

        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                assigned_worker TEXT NOT NULL,
                scope_id TEXT,
                state TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                retry_budget INTEGER NOT NULL,
                updated_epoch_ms INTEGER NOT NULL DEFAULT 0
            );",
        )?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS task_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                runner_mode TEXT NOT NULL,
                started_epoch_ms INTEGER NOT NULL DEFAULT 0,
                terminal_state TEXT,
                ended_epoch_ms INTEGER
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_task_attempts_task_attempt
                ON task_attempts (task_id, attempt_number);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_task_attempts_session
                ON task_attempts (session_id);",
        )?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS task_sessions (
                session_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                runner_mode TEXT NOT NULL,
                started_epoch_ms INTEGER NOT NULL DEFAULT 0,
                terminal_state TEXT,
                ended_epoch_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_task_sessions_task
                ON task_sessions (task_id, started_epoch_ms);",
        )?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                ts_ms INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                actor TEXT,
                session_id TEXT,
                state_before TEXT,
                state_after TEXT,
                message TEXT,
                payload_json TEXT,
                idem_key TEXT
            );",
        )?;

        if !Self::table_has_column(&connection, "tasks", "scope_id")? {
            connection.execute("ALTER TABLE tasks ADD COLUMN scope_id TEXT", [])?;
        }
        if !Self::table_has_column(&connection, "tasks", "updated_epoch_ms")? {
            connection.execute(
                "ALTER TABLE tasks ADD COLUMN updated_epoch_ms INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        connection.execute(
            "UPDATE tasks
             SET updated_epoch_ms = ?1
             WHERE updated_epoch_ms <= 0",
            params![current_epoch_ms()],
        )?;
        Self::ensure_task_attempts_schema(&connection)?;
        Self::ensure_task_sessions_schema(&connection)?;
        Self::ensure_task_events_schema(&connection)?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_task_events_task_id
                ON task_events (task_id, id);
            CREATE INDEX IF NOT EXISTS idx_task_events_task_attempt_id
                ON task_events (task_id, attempt, id);
            CREATE INDEX IF NOT EXISTS idx_task_events_ts_ms
                ON task_events (ts_ms);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_task_events_task_idem_key
                ON task_events (task_id, idem_key)
                WHERE idem_key IS NOT NULL;",
        )?;

        Ok(connection)
    }

    fn ensure_sqlite_parent_dir(path: &str) -> Result<()> {
        let db_path = Path::new(path);
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create sqlite parent dir: {}", parent.display()))?;
            }
        }
        Ok(())
    }

    fn table_has_column(
        connection: &Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<bool> {
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table_name})"))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let existing: String = row.get(1)?;
            if existing == column_name {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ensure_task_attempts_schema(connection: &Connection) -> Result<()> {
        if !Self::table_has_column(connection, "task_attempts", "started_epoch_ms")? {
            connection.execute(
                "ALTER TABLE task_attempts ADD COLUMN started_epoch_ms INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_attempts", "terminal_state")? {
            connection.execute(
                "ALTER TABLE task_attempts ADD COLUMN terminal_state TEXT",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_attempts", "ended_epoch_ms")? {
            connection.execute(
                "ALTER TABLE task_attempts ADD COLUMN ended_epoch_ms INTEGER",
                [],
            )?;
        }
        connection.execute(
            "UPDATE task_attempts
             SET started_epoch_ms = ?1
             WHERE started_epoch_ms <= 0",
            params![current_epoch_ms()],
        )?;
        Ok(())
    }

    fn ensure_task_sessions_schema(connection: &Connection) -> Result<()> {
        if !Self::table_has_column(connection, "task_sessions", "started_epoch_ms")? {
            connection.execute(
                "ALTER TABLE task_sessions ADD COLUMN started_epoch_ms INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_sessions", "terminal_state")? {
            connection.execute(
                "ALTER TABLE task_sessions ADD COLUMN terminal_state TEXT",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_sessions", "ended_epoch_ms")? {
            connection.execute(
                "ALTER TABLE task_sessions ADD COLUMN ended_epoch_ms INTEGER",
                [],
            )?;
        }
        connection.execute(
            "UPDATE task_sessions
             SET started_epoch_ms = ?1
             WHERE started_epoch_ms <= 0",
            params![current_epoch_ms()],
        )?;
        Ok(())
    }

    fn ensure_task_events_schema(connection: &Connection) -> Result<()> {
        if !Self::table_has_column(connection, "task_events", "attempt")? {
            connection.execute(
                "ALTER TABLE task_events ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_events", "ts_ms")? {
            connection.execute(
                "ALTER TABLE task_events ADD COLUMN ts_ms INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_events", "event_type")? {
            connection.execute(
                "ALTER TABLE task_events ADD COLUMN event_type TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_events", "source")? {
            connection.execute(
                "ALTER TABLE task_events ADD COLUMN source TEXT NOT NULL DEFAULT ''",
                [],
            )?;
        }
        if !Self::table_has_column(connection, "task_events", "actor")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN actor TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "session_id")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN session_id TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "state_before")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN state_before TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "state_after")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN state_after TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "message")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN message TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "payload_json")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN payload_json TEXT", [])?;
        }
        if !Self::table_has_column(connection, "task_events", "idem_key")? {
            connection.execute("ALTER TABLE task_events ADD COLUMN idem_key TEXT", [])?;
        }
        Ok(())
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        match &self.runtime {
            RuntimeBackend::Ready(runtime) => {
                let runtime = runtime
                    .lock()
                    .map_err(|_| anyhow!("task store lock poisoned"))?;
                operation(&runtime)
            }
            RuntimeBackend::InitError(error) => bail!("task store init error: {error}"),
        }
    }

    fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        match &self.runtime {
            RuntimeBackend::Ready(runtime) => {
                let mut runtime = runtime
                    .lock()
                    .map_err(|_| anyhow!("task store lock poisoned"))?;

                let tx = runtime.transaction()?;
                let result = operation(&tx)?;
                tx.commit()?;
                Ok(result)
            }
            RuntimeBackend::InitError(error) => bail!("task store init error: {error}"),
        }
    }

    fn read_task_row(row: &rusqlite::Row<'_>) -> Result<TaskRecord> {
        let id: String = row.get(0)?;
        let assigned_worker: String = row.get(1)?;
        let scope_id: Option<String> = row.get(2)?;
        let state_text: String = row.get(3)?;
        let retry_count: i64 = row.get(4)?;
        let retry_budget: i64 = row.get(5)?;
        let updated_epoch_ms: i64 = row.get(6)?;

        let state = TaskRuntimeState::from_db_str(state_text.as_str())?;
        let retry_count = u32::try_from(retry_count)
            .map_err(|_| anyhow!("invalid retry_count in store for task {id}: {retry_count}"))?;
        let retry_budget = u32::try_from(retry_budget)
            .map_err(|_| anyhow!("invalid retry_budget in store for task {id}: {retry_budget}"))?;

        Ok(TaskRecord {
            id,
            assigned_worker,
            scope_id,
            state,
            retry_count,
            retry_budget,
            updated_epoch_ms,
        })
    }

    fn read_task_attempt_row(row: &rusqlite::Row<'_>) -> Result<TaskAttemptRecord> {
        let id: i64 = row.get(0)?;
        let task_id: String = row.get(1)?;
        let attempt_number_raw: i64 = row.get(2)?;
        let session_id: String = row.get(3)?;
        let runner_mode: String = row.get(4)?;
        let started_epoch_ms: i64 = row.get(5)?;
        let terminal_state: Option<String> = row.get(6)?;
        let ended_epoch_ms: Option<i64> = row.get(7)?;
        let attempt_number = u32::try_from(attempt_number_raw).map_err(|_| {
            anyhow!("invalid attempt_number in store for task {task_id}: {attempt_number_raw}")
        })?;

        Ok(TaskAttemptRecord {
            id,
            task_id,
            attempt_number,
            session_id,
            runner_mode,
            started_epoch_ms,
            terminal_state,
            ended_epoch_ms,
        })
    }

    fn read_task_session_row(row: &rusqlite::Row<'_>) -> Result<TaskSessionRecord> {
        let session_id: String = row.get(0)?;
        let task_id: String = row.get(1)?;
        let attempt_number_raw: i64 = row.get(2)?;
        let runner_mode: String = row.get(3)?;
        let started_epoch_ms: i64 = row.get(4)?;
        let terminal_state: Option<String> = row.get(5)?;
        let ended_epoch_ms: Option<i64> = row.get(6)?;
        let attempt_number = u32::try_from(attempt_number_raw).map_err(|_| {
            anyhow!(
                "invalid attempt_number in session store for task {task_id}: {attempt_number_raw}"
            )
        })?;

        Ok(TaskSessionRecord {
            session_id,
            task_id,
            attempt_number,
            runner_mode,
            started_epoch_ms,
            terminal_state,
            ended_epoch_ms,
        })
    }

    fn read_task_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskEventRecord> {
        let id: i64 = row.get(0)?;
        let task_id: String = row.get(1)?;
        let attempt_raw: i64 = row.get(2)?;
        let ts_ms: i64 = row.get(3)?;
        let event_type: String = row.get(4)?;
        let source: String = row.get(5)?;
        let actor: Option<String> = row.get(6)?;
        let session_id: Option<String> = row.get(7)?;
        let state_before: Option<String> = row.get(8)?;
        let state_after: Option<String> = row.get(9)?;
        let message: Option<String> = row.get(10)?;
        let payload_json: Option<String> = row.get(11)?;
        let idem_key: Option<String> = row.get(12)?;

        let attempt = u32::try_from(attempt_raw)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, attempt_raw))?;

        Ok(TaskEventRecord {
            id,
            task_id,
            attempt,
            ts_ms,
            event_type,
            source,
            actor,
            session_id,
            state_before,
            state_after,
            message,
            payload_json,
            idem_key,
        })
    }

    fn current_attempt_for_task_tx(tx: &rusqlite::Transaction<'_>, task_id: &str) -> Result<u32> {
        let max_attempt_raw: i64 = tx.query_row(
            "SELECT COALESCE(MAX(attempt_number), 0)
             FROM task_attempts
             WHERE task_id = ?1",
            params![task_id],
            |row| row.get(0),
        )?;
        let max_attempt = u32::try_from(max_attempt_raw).map_err(|_| {
            anyhow!("attempt_number overflow for task {task_id}: {max_attempt_raw}")
        })?;
        Ok(max_attempt)
    }

    fn insert_task_event_tx(
        tx: &rusqlite::Transaction<'_>,
        draft: &TaskEventDraft,
    ) -> Result<TaskEventRecord> {
        let insert = tx.execute(
            "INSERT INTO task_events (
                task_id, attempt, ts_ms, event_type, source, actor, session_id,
                state_before, state_after, message, payload_json, idem_key
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
             )",
            params![
                draft.task_id,
                i64::from(draft.attempt),
                draft.ts_ms,
                draft.event_type,
                draft.source,
                draft.actor,
                draft.session_id,
                draft.state_before,
                draft.state_after,
                draft.message,
                draft.payload_json,
                draft.idem_key
            ],
        );
        match insert {
            Ok(_) => {
                let id = tx.last_insert_rowid();
                Ok(tx.query_row(
                    "SELECT id, task_id, attempt, ts_ms, event_type, source, actor, session_id, state_before, state_after, message, payload_json, idem_key
                     FROM task_events
                     WHERE id = ?1",
                    params![id],
                    |row| Self::read_task_event_row(row),
                )?)
            }
            Err(error) => {
                if let Some(idem_key) = draft.idem_key.as_deref()
                    && matches!(
                        &error,
                        rusqlite::Error::SqliteFailure(info, _)
                            if info.code == rusqlite::ErrorCode::ConstraintViolation
                    )
                    && let Some(existing) = tx
                        .query_row(
                            "SELECT id, task_id, attempt, ts_ms, event_type, source, actor, session_id, state_before, state_after, message, payload_json, idem_key
                             FROM task_events
                             WHERE task_id = ?1 AND idem_key = ?2
                             LIMIT 1",
                            params![draft.task_id, idem_key],
                            |row| Self::read_task_event_row(row),
                        )
                        .optional()?
                {
                    if existing.event_type != draft.event_type || existing.source != draft.source {
                        bail!(
                            "task event idempotency key collision for task {} key {}: existing {}:{} vs incoming {}:{}",
                            draft.task_id,
                            idem_key,
                            existing.source,
                            existing.event_type,
                            draft.source,
                            draft.event_type
                        );
                    }
                    return Ok(existing);
                }
                Err(error.into())
            }
        }
    }

    pub fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
        self.with_transaction(|tx| {
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM tasks WHERE id = ?1",
                    params![task.id],
                    |row| row.get(0),
                )
                .optional()?;

            if exists.is_some() {
                bail!("task already exists: {}", task.id);
            }

            tx.execute(
                "INSERT INTO tasks (id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    task.id,
                    task.assigned_worker,
                    task.scope_id,
                    task.state.as_db_str(),
                    task.retry_count,
                    task.retry_budget,
                    task.updated_epoch_ms
                ],
            )?;
            Ok(())
        })
    }

    pub fn get_task(&self, id: &str) -> Result<Option<TaskRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 WHERE id = ?1",
            )?;
            let mut rows = statement.query(params![id])?;

            match rows.next()? {
                Some(row) => Ok(Some(Self::read_task_row(row)?)),
                None => Ok(None),
            }
        })
    }

    pub fn export_snapshot(&self) -> Result<TaskStoreSnapshot> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 ORDER BY id ASC",
            )?;
            let mut rows = statement.query([])?;
            let mut tasks = Vec::new();
            while let Some(row) = rows.next()? {
                tasks.push(Self::read_task_row(row)?);
            }
            Ok(TaskStoreSnapshot { tasks })
        })
    }

    pub fn import_snapshot(&self, snapshot: TaskStoreSnapshot) -> Result<()> {
        self.with_transaction(move |tx| {
            let mut seen_ids = HashSet::with_capacity(snapshot.tasks.len());
            for task in &snapshot.tasks {
                if !seen_ids.insert(task.id.clone()) {
                    bail!("duplicate task id in snapshot: {}", task.id);
                }
            }

            tx.execute(
                "DELETE FROM task_events
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;
            tx.execute(
                "DELETE FROM task_attempts
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;
            tx.execute(
                "DELETE FROM task_sessions
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;

            let mut existing_ids = HashSet::new();
            {
                let mut statement = tx.prepare("SELECT id FROM tasks")?;
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? {
                    existing_ids.insert(row.get::<_, String>(0)?);
                }
            }

            for task_id in existing_ids.difference(&seen_ids) {
                tx.execute("DELETE FROM task_events WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM task_attempts WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM task_sessions WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM tasks WHERE id = ?1", params![task_id])?;
            }

            for task in snapshot.tasks {
                tx.execute(
                    "INSERT INTO tasks (id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(id) DO UPDATE SET
                        assigned_worker = excluded.assigned_worker,
                        scope_id = excluded.scope_id,
                        state = excluded.state,
                        retry_count = excluded.retry_count,
                        retry_budget = excluded.retry_budget,
                        updated_epoch_ms = excluded.updated_epoch_ms",
                    params![
                        task.id,
                        task.assigned_worker,
                        task.scope_id,
                        task.state.as_db_str(),
                        task.retry_count,
                        task.retry_budget,
                        task.updated_epoch_ms
                    ],
                )?;
            }

            tx.execute(
                "DELETE FROM task_events
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;
            tx.execute(
                "DELETE FROM task_attempts
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;
            tx.execute(
                "DELETE FROM task_sessions
                 WHERE task_id NOT IN (SELECT id FROM tasks)",
                [],
            )?;
            Ok(())
        })
    }

    pub fn transition(&self, id: &str, next_state: TaskRuntimeState) -> Result<TaskRecord> {
        self.with_transaction(|tx| {
            let mut statement = tx.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 WHERE id = ?1",
            )?;
            let mut rows = statement.query(params![id])?;
            let Some(row) = rows.next()? else {
                return Err(anyhow!("task not found: {id}"));
            };
            let mut task = Self::read_task_row(row)?;
            if !task.state.can_transition_to(next_state) {
                bail!(
                    "invalid transition for task {id}: {:?} -> {:?}",
                    task.state,
                    next_state
                );
            }

            let previous_state = task.state;
            let state_before = previous_state.as_db_str().to_string();
            task.state = next_state;
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET state = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.state.as_db_str(), task.updated_epoch_ms, id],
            )?;
            let current_attempt = Self::current_attempt_for_task_tx(tx, id)?;
            let attempt = match (previous_state, next_state) {
                (TaskRuntimeState::Queued, TaskRuntimeState::Preparing) => {
                    current_attempt.saturating_add(1)
                }
                _ => current_attempt,
            };
            let _ = Self::insert_task_event_tx(
                tx,
                &TaskEventDraft {
                    task_id: id.to_string(),
                    attempt,
                    ts_ms: task.updated_epoch_ms,
                    event_type: "state_transition".to_string(),
                    source: "task_store".to_string(),
                    actor: None,
                    session_id: None,
                    state_before: Some(state_before),
                    state_after: Some(next_state.as_db_str().to_string()),
                    message: None,
                    payload_json: None,
                    idem_key: None,
                },
            )?;
            Ok(task.clone())
        })
    }

    pub fn increment_retry(&self, id: &str) -> Result<TaskRecord> {
        self.with_transaction(|tx| {
            let mut statement = tx.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 WHERE id = ?1",
            )?;
            let mut rows = statement.query(params![id])?;
            let Some(row) = rows.next()? else {
                return Err(anyhow!("task not found: {id}"));
            };
            let mut task = Self::read_task_row(row)?;
            if task.state.is_terminal() {
                bail!("cannot increment retry in terminal state: {:?}", task.state);
            }

            if task.retry_count >= task.retry_budget {
                bail!(
                    "retry budget exceeded for task {id}: {} / {}",
                    task.retry_count,
                    task.retry_budget
                );
            }

            let old_retry_count = task.retry_count;
            task.retry_count += 1;
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET retry_count = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.retry_count, task.updated_epoch_ms, id],
            )?;
            let attempt = Self::current_attempt_for_task_tx(tx, id)?;
            let _ = Self::insert_task_event_tx(
                tx,
                &TaskEventDraft {
                    task_id: id.to_string(),
                    attempt,
                    ts_ms: task.updated_epoch_ms,
                    event_type: "retry_incremented".to_string(),
                    source: "task_store".to_string(),
                    actor: None,
                    session_id: None,
                    state_before: None,
                    state_after: None,
                    message: Some(format!(
                        "retry_count {} -> {}",
                        old_retry_count, task.retry_count
                    )),
                    payload_json: None,
                    idem_key: None,
                },
            )?;
            Ok(task.clone())
        })
    }

    pub fn reassign_worker(
        &self,
        id: &str,
        assigned_worker: impl Into<String>,
    ) -> Result<TaskRecord> {
        self.with_transaction(|tx| {
            let mut statement = tx.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 WHERE id = ?1",
            )?;
            let mut rows = statement.query(params![id])?;
            let Some(row) = rows.next()? else {
                return Err(anyhow!("task not found: {id}"));
            };
            let mut task = Self::read_task_row(row)?;
            if task.state.is_terminal() {
                bail!("cannot reassign worker in terminal state: {:?}", task.state);
            }

            task.assigned_worker = assigned_worker.into();
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET assigned_worker = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.assigned_worker, task.updated_epoch_ms, id],
            )?;
            Ok(task.clone())
        })
    }

    pub fn set_scope(&self, id: &str, scope_id: Option<String>) -> Result<TaskRecord> {
        self.with_transaction(|tx| {
            let mut statement = tx.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 WHERE id = ?1",
            )?;
            let mut rows = statement.query(params![id])?;
            let Some(row) = rows.next()? else {
                return Err(anyhow!("task not found: {id}"));
            };
            let mut task = Self::read_task_row(row)?;
            if task.state.is_terminal() {
                bail!("cannot update scope in terminal state: {:?}", task.state);
            }

            task.scope_id = scope_id;
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET scope_id = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.scope_id, task.updated_epoch_ms, id],
            )?;
            Ok(task.clone())
        })
    }

    pub fn prune_tasks(
        &self,
        scope_id: Option<&str>,
        states: &[TaskRuntimeState],
        updated_before_epoch_ms: Option<i64>,
    ) -> Result<Vec<String>> {
        let state_filter = states.iter().copied().collect::<HashSet<_>>();
        self.with_transaction(|tx| {
            let mut statement = tx.prepare(
                "SELECT id, assigned_worker, scope_id, state, retry_count, retry_budget, updated_epoch_ms
                 FROM tasks
                 ORDER BY updated_epoch_ms ASC, id ASC",
            )?;
            let mut rows = statement.query([])?;
            let mut to_delete = Vec::new();
            while let Some(row) = rows.next()? {
                let task = Self::read_task_row(row)?;

                if let Some(scope) = scope_id
                    && task.scope_id.as_deref() != Some(scope)
                {
                    continue;
                }
                if !state_filter.is_empty() && !state_filter.contains(&task.state) {
                    continue;
                }
                if let Some(cutoff) = updated_before_epoch_ms
                    && task.updated_epoch_ms >= cutoff
                {
                    continue;
                }

                to_delete.push(task.id);
            }

            for task_id in &to_delete {
                tx.execute("DELETE FROM task_events WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM task_attempts WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM task_sessions WHERE task_id = ?1", params![task_id])?;
                tx.execute("DELETE FROM tasks WHERE id = ?1", params![task_id])?;
            }

            Ok(to_delete)
        })
    }

    pub fn register_task_attempt(
        &self,
        task_id: &str,
        session_id: &str,
        runner_mode: &str,
    ) -> Result<TaskAttemptRecord> {
        self.with_transaction(|tx| {
            let next_attempt_raw: i64 = tx.query_row(
                "SELECT COALESCE(MAX(attempt_number), 0) + 1
                 FROM task_attempts
                 WHERE task_id = ?1",
                params![task_id],
                |row| row.get(0),
            )?;
            let next_attempt = u32::try_from(next_attempt_raw).map_err(|_| {
                anyhow!("attempt_number overflow for task {task_id}: {next_attempt_raw}")
            })?;
            let started_epoch_ms = current_epoch_ms();
            tx.execute(
                "INSERT INTO task_attempts (task_id, attempt_number, session_id, runner_mode, started_epoch_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![task_id, next_attempt, session_id, runner_mode, started_epoch_ms],
            )?;
            tx.execute(
                "INSERT INTO task_sessions (session_id, task_id, attempt_number, runner_mode, started_epoch_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(session_id) DO UPDATE SET
                    task_id = excluded.task_id,
                    attempt_number = excluded.attempt_number,
                    runner_mode = excluded.runner_mode,
                    started_epoch_ms = excluded.started_epoch_ms",
                params![session_id, task_id, next_attempt, runner_mode, started_epoch_ms],
            )?;
            let id = tx.last_insert_rowid();
            let _ = Self::insert_task_event_tx(
                tx,
                &TaskEventDraft {
                    task_id: task_id.to_string(),
                    attempt: next_attempt,
                    ts_ms: started_epoch_ms,
                    event_type: "attempt_registered".to_string(),
                    source: "task_store".to_string(),
                    actor: None,
                    session_id: Some(session_id.to_string()),
                    state_before: None,
                    state_after: None,
                    message: Some(format!("runner_mode={runner_mode}")),
                    payload_json: None,
                    idem_key: Some(format!("attempt_registered:{session_id}")),
                },
            )?;
            Ok(TaskAttemptRecord {
                id,
                task_id: task_id.to_string(),
                attempt_number: next_attempt,
                session_id: session_id.to_string(),
                runner_mode: runner_mode.to_string(),
                started_epoch_ms,
                terminal_state: None,
                ended_epoch_ms: None,
            })
        })
    }

    pub fn finalize_task_session(&self, session_id: &str, terminal_state: &str) -> Result<()> {
        self.with_transaction(|tx| {
            let ended_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE task_sessions
                 SET terminal_state = COALESCE(terminal_state, ?1),
                     ended_epoch_ms = COALESCE(ended_epoch_ms, ?2)
                 WHERE session_id = ?3",
                params![terminal_state, ended_epoch_ms, session_id],
            )?;
            tx.execute(
                "UPDATE task_attempts
                 SET terminal_state = COALESCE(terminal_state, ?1),
                     ended_epoch_ms = COALESCE(ended_epoch_ms, ?2)
                 WHERE session_id = ?3",
                params![terminal_state, ended_epoch_ms, session_id],
            )?;
            let session_meta: Option<(String, i64, Option<String>, Option<i64>)> = tx
                .query_row(
                    "SELECT task_id, attempt_number, terminal_state, ended_epoch_ms
                     FROM task_sessions
                     WHERE session_id = ?1
                     LIMIT 1",
                    params![session_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            if let Some((
                task_id,
                attempt_raw,
                Some(effective_terminal_state),
                effective_ended_epoch_ms,
            )) = session_meta
            {
                let attempt = u32::try_from(attempt_raw).map_err(|_| {
                    anyhow!("invalid attempt_number in task_sessions: {attempt_raw}")
                })?;
                let _ = Self::insert_task_event_tx(
                    tx,
                    &TaskEventDraft {
                        task_id,
                        attempt,
                        ts_ms: effective_ended_epoch_ms.unwrap_or(ended_epoch_ms),
                        event_type: "session_finalized".to_string(),
                        source: "task_store".to_string(),
                        actor: None,
                        session_id: Some(session_id.to_string()),
                        state_before: None,
                        state_after: Some(effective_terminal_state),
                        message: None,
                        payload_json: None,
                        idem_key: Some(format!("session_finalized:{session_id}")),
                    },
                )?;
            }
            Ok(())
        })
    }

    pub fn list_task_attempts(&self, task_id: &str) -> Result<Vec<TaskAttemptRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT id, task_id, attempt_number, session_id, runner_mode, started_epoch_ms, terminal_state, ended_epoch_ms
                 FROM task_attempts
                 WHERE task_id = ?1
                 ORDER BY attempt_number ASC",
            )?;
            let mut rows = statement.query(params![task_id])?;
            let mut attempts = Vec::new();
            while let Some(row) = rows.next()? {
                attempts.push(Self::read_task_attempt_row(row)?);
            }
            Ok(attempts)
        })
    }

    pub fn list_task_sessions(&self, task_id: &str) -> Result<Vec<TaskSessionRecord>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "SELECT session_id, task_id, attempt_number, runner_mode, started_epoch_ms, terminal_state, ended_epoch_ms
                 FROM task_sessions
                 WHERE task_id = ?1
                 ORDER BY started_epoch_ms ASC, session_id ASC",
            )?;
            let mut rows = statement.query(params![task_id])?;
            let mut sessions = Vec::new();
            while let Some(row) = rows.next()? {
                sessions.push(Self::read_task_session_row(row)?);
            }
            Ok(sessions)
        })
    }

    pub fn append_task_event(&self, draft: &TaskEventDraft) -> Result<TaskEventRecord> {
        self.with_transaction(|tx| Self::insert_task_event_tx(tx, draft))
    }

    pub fn list_task_events(
        &self,
        task_id: &str,
        attempt: Option<u32>,
    ) -> Result<Vec<TaskEventRecord>> {
        self.with_connection(|connection| {
            let mut events = Vec::new();
            if let Some(attempt) = attempt {
                let mut statement = connection.prepare(
                    "SELECT id, task_id, attempt, ts_ms, event_type, source, actor, session_id, state_before, state_after, message, payload_json, idem_key
                     FROM task_events
                     WHERE task_id = ?1 AND attempt = ?2
                     ORDER BY id ASC",
                )?;
                let mut rows = statement.query(params![task_id, i64::from(attempt)])?;
                while let Some(row) = rows.next()? {
                    events.push(Self::read_task_event_row(row)?);
                }
                return Ok(events);
            }

            let mut statement = connection.prepare(
                "SELECT id, task_id, attempt, ts_ms, event_type, source, actor, session_id, state_before, state_after, message, payload_json, idem_key
                 FROM task_events
                 WHERE task_id = ?1
                 ORDER BY id ASC",
            )?;
            let mut rows = statement.query(params![task_id])?;
            while let Some(row) = rows.next()? {
                events.push(Self::read_task_event_row(row)?);
            }
            Ok(events)
        })
    }
}
