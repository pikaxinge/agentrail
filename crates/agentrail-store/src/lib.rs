use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub session: String,
    pub status: String,
}

#[derive(Debug)]
pub struct TaskStore {
    pub dsn: String,
}

impl TaskStore {
    pub fn connect(dsn: impl Into<String>) -> Self {
        Self { dsn: dsn.into() }
    }

    pub fn upsert_task(&self, _task: &TaskRecord) -> Result<()> {
        // Placeholder for SQLite WAL-backed implementation.
        Ok(())
    }

    pub fn get_task(&self, _id: &str) -> Result<Option<TaskRecord>> {
        // Placeholder for transactional read.
        Ok(None)
    }
}
