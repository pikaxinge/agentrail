use std::{
    collections::HashSet,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow, bail};
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
            Some(path) => (Connection::open(path)?, true),
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

        if !Self::table_has_column(&connection, "scope_id")? {
            connection.execute("ALTER TABLE tasks ADD COLUMN scope_id TEXT", [])?;
        }
        if !Self::table_has_column(&connection, "updated_epoch_ms")? {
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

        Ok(connection)
    }

    fn table_has_column(connection: &Connection, column_name: &str) -> Result<bool> {
        let mut statement = connection.prepare("PRAGMA table_info(tasks)")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let existing: String = row.get(1)?;
            if existing == column_name {
                return Ok(true);
            }
        }
        Ok(false)
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
                if !seen_ids.insert(task.id.as_str()) {
                    bail!("duplicate task id in snapshot: {}", task.id);
                }
            }

            tx.execute("DELETE FROM tasks", [])?;

            for task in snapshot.tasks {
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
            }
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

            task.state = next_state;
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET state = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.state.as_db_str(), task.updated_epoch_ms, id],
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

            task.retry_count += 1;
            task.updated_epoch_ms = current_epoch_ms();
            tx.execute(
                "UPDATE tasks
                 SET retry_count = ?1,
                     updated_epoch_ms = ?2
                 WHERE id = ?3",
                params![task.retry_count, task.updated_epoch_ms, id],
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
                tx.execute("DELETE FROM tasks WHERE id = ?1", params![task_id])?;
            }

            Ok(to_delete)
        })
    }
}
