use std::{
    collections::{HashMap, VecDeque},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command},
    sync::Mutex,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessState {
    Running,
    Completed,
    Failed,
    Stopped,
}

impl ProcessState {
    fn as_str(self) -> &'static str {
        match self {
            ProcessState::Running => "running",
            ProcessState::Completed => "completed",
            ProcessState::Failed => "failed",
            ProcessState::Stopped => "stopped",
        }
    }

    fn is_terminal(self) -> bool {
        !matches!(self, ProcessState::Running)
    }
}

#[derive(Debug)]
struct SessionRecord {
    task_id: String,
    logs: Arc<Mutex<VecDeque<String>>>,
    inner: Mutex<SessionInner>,
    sequence: u64,
    terminal: AtomicBool,
}

#[derive(Debug)]
struct SessionInner {
    state: ProcessState,
    child: Option<Child>,
}

static PROCESS_SESSIONS: OnceLock<StdMutex<HashMap<String, Arc<SessionRecord>>>> = OnceLock::new();
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
const MAX_LOG_LINES: usize = 2000;
const MAX_TERMINAL_SESSIONS: usize = 256;

fn process_sessions() -> &'static StdMutex<HashMap<String, Arc<SessionRecord>>> {
    PROCESS_SESSIONS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn get_session(session_id: &str) -> Result<Arc<SessionRecord>> {
    let sessions = process_sessions()
        .lock()
        .map_err(|_| anyhow!("session registry lock poisoned"))?;

    sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| anyhow!("session not found: {session_id}"))
}

async fn append_log_line(logs: &Arc<Mutex<VecDeque<String>>>, line: String) {
    let mut guard = logs.lock().await;
    guard.push_back(line);
    while guard.len() > MAX_LOG_LINES {
        guard.pop_front();
    }
}

fn cleanup_terminal_sessions() -> Result<()> {
    let mut sessions = process_sessions()
        .lock()
        .map_err(|_| anyhow!("session registry lock poisoned"))?;

    let terminal_count = sessions
        .values()
        .filter(|record| record.terminal.load(Ordering::Relaxed))
        .count();
    if terminal_count <= MAX_TERMINAL_SESSIONS {
        return Ok(());
    }

    let mut terminal_ids: Vec<(u64, String)> = sessions
        .iter()
        .filter(|(_, record)| record.terminal.load(Ordering::Relaxed))
        .map(|(id, record)| (record.sequence, id.clone()))
        .collect();
    terminal_ids.sort_by_key(|(sequence, _)| *sequence);

    for (_, session_id) in terminal_ids
        .into_iter()
        .take(terminal_count.saturating_sub(MAX_TERMINAL_SESSIONS))
    {
        sessions.remove(&session_id);
    }

    Ok(())
}

#[cfg(unix)]
fn child_pids(parent_pid: u32) -> Vec<u32> {
    let output = match std::process::Command::new("pgrep")
        .args(["-P", &parent_pid.to_string()])
        .output()
    {
        Ok(output) => output,
        Err(_) => return Vec::new(),
    };

    if !output.status.success() {
        return Vec::new();
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect()
}

#[cfg(unix)]
fn collect_descendant_pids(root_pid: u32) -> Vec<u32> {
    let mut stack = vec![root_pid];
    let mut descendants = Vec::new();
    while let Some(pid) = stack.pop() {
        let children = child_pids(pid);
        for child in children {
            descendants.push(child);
            stack.push(child);
        }
    }
    descendants
}

#[cfg(unix)]
fn signal_pid(pid: u32, signal: &str) -> bool {
    std::process::Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(unix)]
async fn terminate_pid_force(pid: u32) {
    let _ = signal_pid(pid, "-TERM");
    tokio::time::sleep(Duration::from_millis(100)).await;
    if pid_alive(pid) {
        let _ = signal_pid(pid, "-KILL");
    }
}

#[cfg(unix)]
async fn terminate_process_group(child: &mut Child) -> Result<()> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    let descendants = collect_descendant_pids(pid);

    let pgid = format!("-{pid}");
    let term_sent = std::process::Command::new("kill")
        .args(["-TERM", &pgid])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    if term_sent
        && tokio::time::timeout(Duration::from_millis(500), child.wait())
            .await
            .is_ok()
    {
        for descendant in descendants {
            if pid_alive(descendant) {
                terminate_pid_force(descendant).await;
            }
        }
        return Ok(());
    }

    let kill_sent = std::process::Command::new("kill")
        .args(["-KILL", &pgid])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    if kill_sent
        && tokio::time::timeout(Duration::from_millis(500), child.wait())
            .await
            .is_ok()
    {
        for descendant in descendants {
            if pid_alive(descendant) {
                terminate_pid_force(descendant).await;
            }
        }
        return Ok(());
    }

    if let Err(error) = child.kill().await {
        if child.try_wait()?.is_none() {
            return Err(error.into());
        }
    }
    tokio::time::timeout(Duration::from_millis(500), child.wait())
        .await
        .map_err(|_| anyhow!("process did not exit after kill timeout"))??;

    for descendant in descendants {
        if pid_alive(descendant) {
            terminate_pid_force(descendant).await;
        }
    }

    Ok(())
}

fn spawn_log_reader<R>(reader: R, logs: Arc<Mutex<VecDeque<String>>>)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => append_log_line(&logs, line).await,
                Ok(None) => break,
                Err(error) => {
                    append_log_line(&logs, format!("[agentrail-runner] log read error: {error}"))
                        .await;
                    break;
                }
            }
        }
    });
}

async fn refresh_state(session: &SessionRecord) -> Result<ProcessState> {
    let mut inner = session.inner.lock().await;
    let mut became_terminal = false;

    if inner.state == ProcessState::Running {
        if let Some(child) = inner.child.as_mut() {
            if let Some(status) = child.try_wait()? {
                inner.state = if status.success() {
                    ProcessState::Completed
                } else {
                    ProcessState::Failed
                };
                inner.child = None;
                became_terminal = true;
            }
        } else {
            inner.state = ProcessState::Failed;
            became_terminal = true;
        }
    }

    let state = inner.state;
    drop(inner);

    if became_terminal || state.is_terminal() {
        session.terminal.store(true, Ordering::Relaxed);
        cleanup_terminal_sessions()?;
    }

    Ok(state)
}

fn spawn_state_reaper(session: Arc<SessionRecord>) {
    tokio::spawn(async move {
        loop {
            let state = match refresh_state(&session).await {
                Ok(state) => state,
                Err(_) => break,
            };
            if state.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}

#[async_trait]
impl AgentRunner for ProcessRunner {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle> {
        std::fs::create_dir_all(&spec.workdir)?;

        let mut command = Command::new(&spec.command);
        command
            .args(&spec.args)
            .current_dir(&spec.workdir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command.spawn()?;
        let logs = Arc::new(Mutex::new(VecDeque::new()));

        if let Some(stdout) = child.stdout.take() {
            spawn_log_reader(stdout, Arc::clone(&logs));
        }
        if let Some(stderr) = child.stderr.take() {
            spawn_log_reader(stderr, Arc::clone(&logs));
        }

        let sequence = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_id = format!("process-{sequence}");
        let record = Arc::new(SessionRecord {
            task_id: spec.id.clone(),
            logs,
            inner: Mutex::new(SessionInner {
                state: ProcessState::Running,
                child: Some(child),
            }),
            sequence,
            terminal: AtomicBool::new(false),
        });

        let mut sessions = process_sessions()
            .lock()
            .map_err(|_| anyhow!("session registry lock poisoned"))?;
        sessions.insert(session_id.clone(), Arc::clone(&record));
        drop(sessions);
        spawn_state_reaper(record);
        cleanup_terminal_sessions()?;

        Ok(TaskHandle {
            id: spec.id,
            session_id,
        })
    }

    async fn steer(&self, session_id: &str, _instruction: &str) -> Result<()> {
        let _ = get_session(session_id)?;
        Err(anyhow!(
            "unsupported: steer is not supported by ProcessRunner"
        ))
    }

    async fn pause(&self, session_id: &str) -> Result<()> {
        let _ = get_session(session_id)?;
        Err(anyhow!(
            "unsupported: pause is not supported by ProcessRunner"
        ))
    }

    async fn resume(&self, session_id: &str) -> Result<()> {
        let _ = get_session(session_id)?;
        Err(anyhow!(
            "unsupported: resume is not supported by ProcessRunner"
        ))
    }

    async fn stop(&self, session_id: &str) -> Result<()> {
        let session = get_session(session_id)?;
        let mut inner = session.inner.lock().await;

        if inner.state != ProcessState::Running {
            return Ok(());
        }

        if let Some(child) = inner.child.as_mut() {
            if let Some(status) = child.try_wait()? {
                inner.state = if status.success() {
                    ProcessState::Completed
                } else {
                    ProcessState::Failed
                };
                inner.child = None;
                drop(inner);
                session.terminal.store(true, Ordering::Relaxed);
                cleanup_terminal_sessions()?;
                return Ok(());
            }

            #[cfg(unix)]
            terminate_process_group(child).await?;

            #[cfg(not(unix))]
            {
                if let Err(error) = child.kill().await {
                    if child.try_wait()?.is_none() {
                        return Err(error.into());
                    }
                } else {
                    let _ = child.wait().await;
                }
            }
        }

        inner.child = None;
        inner.state = ProcessState::Stopped;
        drop(inner);

        session.terminal.store(true, Ordering::Relaxed);
        cleanup_terminal_sessions()?;

        Ok(())
    }

    async fn status(&self, session_id: &str) -> Result<TaskStatus> {
        let session = get_session(session_id)?;
        let state = refresh_state(&session).await?;

        Ok(TaskStatus {
            id: session.task_id.clone(),
            state: state.as_str().to_string(),
        })
    }

    async fn logs(&self, session_id: &str, tail: usize) -> Result<String> {
        let session = get_session(session_id)?;
        let _ = refresh_state(&session).await?;

        let logs = session.logs.lock().await;
        if tail == 0 || tail >= logs.len() {
            return Ok(logs
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join("\n"));
        }

        Ok(logs
            .iter()
            .skip(logs.len() - tail)
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join("\n"))
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
