use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: String,
    pub command: String,
    pub args: Vec<String>,
    pub workdir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHandle {
    pub id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: String,
    pub state: String,
}

#[async_trait]
pub trait AgentRunner: Send + Sync {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle>;
    async fn steer(&self, session_id: &str, instruction: &str) -> Result<()>;
    async fn pause(&self, session_id: &str) -> Result<()>;
    async fn resume(&self, session_id: &str) -> Result<()>;
    async fn stop(&self, session_id: &str) -> Result<()>;
    async fn status(&self, session_id: &str) -> Result<TaskStatus>;
    async fn logs(&self, session_id: &str, tail: usize) -> Result<String>;
}

pub struct ProcessRunner;
pub struct TmuxRunner;

#[async_trait]
impl AgentRunner for ProcessRunner {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle> {
        Ok(TaskHandle {
            id: spec.id,
            session_id: "process".to_string(),
        })
    }

    async fn steer(&self, _session_id: &str, _instruction: &str) -> Result<()> {
        Ok(())
    }

    async fn pause(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn resume(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn stop(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn status(&self, session_id: &str) -> Result<TaskStatus> {
        Ok(TaskStatus {
            id: session_id.to_string(),
            state: "running".to_string(),
        })
    }

    async fn logs(&self, _session_id: &str, _tail: usize) -> Result<String> {
        Ok(String::new())
    }
}

#[async_trait]
impl AgentRunner for TmuxRunner {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle> {
        Ok(TaskHandle {
            id: spec.id,
            session_id: "tmux".to_string(),
        })
    }

    async fn steer(&self, _session_id: &str, _instruction: &str) -> Result<()> {
        Ok(())
    }

    async fn pause(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn resume(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn stop(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn status(&self, session_id: &str) -> Result<TaskStatus> {
        Ok(TaskStatus {
            id: session_id.to_string(),
            state: "running".to_string(),
        })
    }

    async fn logs(&self, _session_id: &str, _tail: usize) -> Result<String> {
        Ok(String::new())
    }
}
