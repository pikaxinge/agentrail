use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub assigned_worker: String,
    pub state: TaskRuntimeState,
    pub retry_count: u32,
    pub retry_budget: u32,
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
            state: TaskRuntimeState::Queued,
            retry_count: 0,
            retry_budget,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskStoreSnapshot {
    pub tasks: Vec<TaskRecord>,
}

#[derive(Debug, Default)]
struct InMemoryRuntimeStore {
    tasks: HashMap<String, TaskRecord>,
}

#[derive(Debug)]
pub struct TaskStore {
    pub dsn: String,
    runtime: Mutex<InMemoryRuntimeStore>,
}

impl TaskStore {
    pub fn connect(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            runtime: Mutex::new(InMemoryRuntimeStore::default()),
        }
    }

    fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut InMemoryRuntimeStore) -> Result<T>,
    ) -> Result<T> {
        // Keep this boundary while the store is in-memory so it can map to a future SQL transaction.
        let mut runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow!("task store lock poisoned"))?;
        operation(&mut runtime)
    }

    pub fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
        self.with_transaction(|runtime| {
            if runtime.tasks.contains_key(task.id.as_str()) {
                bail!("task already exists: {}", task.id);
            }

            runtime.tasks.insert(task.id.clone(), task.clone());
            Ok(())
        })
    }

    pub fn get_task(&self, id: &str) -> Result<Option<TaskRecord>> {
        self.with_transaction(|runtime| Ok(runtime.tasks.get(id).cloned()))
    }

    pub fn export_snapshot(&self) -> Result<TaskStoreSnapshot> {
        self.with_transaction(|runtime| {
            let mut tasks: Vec<TaskRecord> = runtime.tasks.values().cloned().collect();
            tasks.sort_by(|a, b| a.id.cmp(&b.id));
            Ok(TaskStoreSnapshot { tasks })
        })
    }

    pub fn import_snapshot(&self, snapshot: TaskStoreSnapshot) -> Result<()> {
        self.with_transaction(move |runtime| {
            let mut seen_ids = HashSet::with_capacity(snapshot.tasks.len());
            for task in &snapshot.tasks {
                if !seen_ids.insert(task.id.as_str()) {
                    bail!("duplicate task id in snapshot: {}", task.id);
                }
            }

            let mut imported_tasks = HashMap::with_capacity(snapshot.tasks.len());
            for task in snapshot.tasks {
                imported_tasks.insert(task.id.clone(), task);
            }

            runtime.tasks = imported_tasks;
            Ok(())
        })
    }

    pub fn transition(&self, id: &str, next_state: TaskRuntimeState) -> Result<TaskRecord> {
        self.with_transaction(|runtime| {
            let task = runtime
                .tasks
                .get_mut(id)
                .ok_or_else(|| anyhow!("task not found: {id}"))?;

            if !task.state.can_transition_to(next_state) {
                bail!(
                    "invalid transition for task {id}: {:?} -> {:?}",
                    task.state,
                    next_state
                );
            }

            task.state = next_state;
            Ok(task.clone())
        })
    }

    pub fn increment_retry(&self, id: &str) -> Result<TaskRecord> {
        self.with_transaction(|runtime| {
            let task = runtime
                .tasks
                .get_mut(id)
                .ok_or_else(|| anyhow!("task not found: {id}"))?;

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
            Ok(task.clone())
        })
    }

    pub fn reassign_worker(
        &self,
        id: &str,
        assigned_worker: impl Into<String>,
    ) -> Result<TaskRecord> {
        self.with_transaction(|runtime| {
            let task = runtime
                .tasks
                .get_mut(id)
                .ok_or_else(|| anyhow!("task not found: {id}"))?;

            if task.state.is_terminal() {
                bail!("cannot reassign worker in terminal state: {:?}", task.state);
            }

            task.assigned_worker = assigned_worker.into();
            Ok(task.clone())
        })
    }
}
