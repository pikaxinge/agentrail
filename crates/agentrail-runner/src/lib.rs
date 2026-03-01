use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TmuxState {
    Running,
    Completed,
    Failed,
    Stopped,
}

impl TmuxState {
    fn as_str(self) -> &'static str {
        match self {
            TmuxState::Running => "running",
            TmuxState::Completed => "completed",
            TmuxState::Failed => "failed",
            TmuxState::Stopped => "stopped",
        }
    }
}

#[derive(Debug)]
struct TmuxSessionRecord {
    task_id: String,
    tmux_session: String,
    log_path: PathBuf,
    exit_code_path: PathBuf,
    inner: Mutex<TmuxSessionInner>,
    sequence: u64,
    terminal: AtomicBool,
}

#[derive(Debug)]
struct TmuxSessionInner {
    state: TmuxState,
}

static PROCESS_SESSIONS: OnceLock<StdMutex<HashMap<String, Arc<SessionRecord>>>> = OnceLock::new();
static TMUX_SESSIONS: OnceLock<StdMutex<HashMap<String, Arc<TmuxSessionRecord>>>> = OnceLock::new();
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
const MAX_LOG_LINES: usize = 2000;
const MAX_TERMINAL_SESSIONS: usize = 256;
const TMUX_LOG_DIR: &str = ".agentrail-tmux-logs";

fn unique_tmux_session_id(sequence: u64) -> String {
    let ts_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    format!("tmux-{pid}-{ts_nanos}-{sequence}")
}

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

fn tmux_sessions() -> &'static StdMutex<HashMap<String, Arc<TmuxSessionRecord>>> {
    TMUX_SESSIONS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn get_tmux_session(session_id: &str) -> Result<Arc<TmuxSessionRecord>> {
    let sessions = tmux_sessions()
        .lock()
        .map_err(|_| anyhow!("tmux session registry lock poisoned"))?;

    sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| anyhow!("session not found: {session_id}"))
}

fn tmux_available() -> bool {
    StdCommand::new("tmux")
        .arg("-V")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_tmux_available() -> Result<()> {
    if tmux_available() {
        return Ok(());
    }
    Err(anyhow!("tmux is not available in PATH"))
}

fn run_tmux(args: &[&str]) -> Result<std::process::Output> {
    StdCommand::new("tmux")
        .args(args)
        .output()
        .map_err(|error| anyhow!("failed to execute tmux {:?}: {error}", args))
}

fn tmux_target_missing(stderr: &str) -> bool {
    let lowered = stderr.to_ascii_lowercase();
    lowered.contains("can't find session")
        || lowered.contains("can't find pane")
        || lowered.contains("no server running")
}

fn shell_escape(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn shell_escape_double(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`");
    format!("\"{escaped}\"")
}

fn command_as_shell_line(command: &str, args: &[String]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 1);
    parts.push(shell_escape(command));
    parts.extend(args.iter().map(|arg| shell_escape(arg)));
    parts.join(" ")
}

fn build_tmux_shell_command(spec: &TaskSpec, log_path: &Path, exit_code_path: &Path) -> String {
    let command_line = command_as_shell_line(&spec.command, &spec.args);
    let script = format!(
        "cd {}\nscript -q -e -f -c {} {}\nexit_code=$?\nprintf '%s\\n' \"$exit_code\" > {}\nexit \"$exit_code\"\n",
        shell_escape(&spec.workdir),
        shell_escape_double(&command_line),
        shell_escape(&log_path.to_string_lossy()),
        shell_escape(&exit_code_path.to_string_lossy())
    );
    format!("bash -lc {}", shell_escape(&script))
}

fn tail_lines(text: &str, tail: usize) -> String {
    if tail == 0 {
        return text.to_string();
    }

    let lines: Vec<&str> = text.lines().collect();
    if tail >= lines.len() {
        return lines.join("\n");
    }

    lines[lines.len() - tail..].join("\n")
}

fn read_tmux_log_file(log_path: &Path, tail: usize) -> String {
    match std::fs::read_to_string(log_path) {
        Ok(contents) => tail_lines(&contents, tail),
        Err(_) => String::new(),
    }
}

fn read_tmux_exit_code(exit_code_path: &Path) -> Option<i32> {
    std::fs::read_to_string(exit_code_path)
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
}

fn tmux_has_session(session_name: &str) -> Result<bool> {
    let output = run_tmux(&["has-session", "-t", session_name])?;
    if output.status.success() {
        return Ok(true);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if tmux_target_missing(&stderr) {
        return Ok(false);
    }

    Err(anyhow!(
        "tmux has-session failed for {session_name}: {}",
        stderr.trim()
    ))
}

fn tmux_pane_dead(session_name: &str) -> Result<Option<bool>> {
    if !tmux_has_session(session_name)? {
        return Ok(None);
    }

    let target = format!("{session_name}:0.0");
    let output = run_tmux(&["display-message", "-p", "-t", &target, "#{pane_dead}"])?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if tmux_target_missing(&stderr) {
            return Ok(None);
        }
        return Err(anyhow!(
            "tmux display-message failed for {target}: {}",
            stderr.trim()
        ));
    }

    let dead = String::from_utf8_lossy(&output.stdout).trim() == "1";
    Ok(Some(dead))
}

fn tmux_capture_logs(session_name: &str, tail: usize) -> Result<Option<String>> {
    if !tmux_has_session(session_name)? {
        return Ok(None);
    }

    let target = format!("{session_name}:0.0");
    let start = if tail == 0 {
        "-".to_string()
    } else {
        format!("-{tail}")
    };
    let output = run_tmux(&["capture-pane", "-p", "-t", &target, "-S", &start])?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if tmux_target_missing(&stderr) {
            return Ok(None);
        }
        return Err(anyhow!(
            "tmux capture-pane failed for {target}: {}",
            stderr.trim()
        ));
    }

    Ok(Some(String::from_utf8_lossy(&output.stdout).to_string()))
}

fn tmux_state_from_exit_code(exit_code: Option<i32>) -> TmuxState {
    match exit_code {
        Some(0) => TmuxState::Completed,
        Some(_) => TmuxState::Failed,
        None => TmuxState::Completed,
    }
}

#[cfg(unix)]
fn tmux_pane_pid(session_name: &str) -> Result<Option<u32>> {
    if !tmux_has_session(session_name)? {
        return Ok(None);
    }

    let target = format!("{session_name}:0.0");
    let output = run_tmux(&["display-message", "-p", "-t", &target, "#{pane_pid}"])?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if tmux_target_missing(&stderr) {
            return Ok(None);
        }
        return Err(anyhow!(
            "tmux pane pid lookup failed for {target}: {}",
            stderr.trim()
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .ok())
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

fn cleanup_terminal_tmux_sessions() -> Result<()> {
    let mut sessions = tmux_sessions()
        .lock()
        .map_err(|_| anyhow!("tmux session registry lock poisoned"))?;

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
fn process_group_id(pid: u32) -> Option<u32> {
    let output = std::process::Command::new("ps")
        .args(["-o", "pgid=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .ok()
}

#[cfg(unix)]
fn signal_process_group(pgid: u32, signal: &str) -> bool {
    if pgid == 0 {
        return false;
    }

    std::process::Command::new("kill")
        .args([signal, &format!("-{pgid}")])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[cfg(unix)]
fn should_signal_process_group(child_pgid: u32, controller_pgid: Option<u32>) -> bool {
    if child_pgid <= 1 {
        return false;
    }
    if let Some(controller_pgid) = controller_pgid {
        if child_pgid == controller_pgid {
            return false;
        }
    }
    true
}

#[cfg(unix)]
fn safe_target_process_group(child_pid: u32, controller_pid: u32) -> Option<u32> {
    let child_pgid = process_group_id(child_pid)?;
    let controller_pgid = process_group_id(controller_pid);
    if should_signal_process_group(child_pgid, controller_pgid) {
        Some(child_pgid)
    } else {
        None
    }
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

    let _ = signal_pid(pid, "-TERM");
    for descendant in &descendants {
        let _ = signal_pid(*descendant, "-TERM");
    }

    if tokio::time::timeout(Duration::from_millis(500), child.wait())
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

    let _ = signal_pid(pid, "-KILL");
    for descendant in &descendants {
        if pid_alive(*descendant) {
            let _ = signal_pid(*descendant, "-KILL");
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

#[cfg(all(test, unix))]
mod unix_guard_tests {
    use std::path::Path;

    use super::{
        TaskSpec, build_tmux_shell_command, command_as_shell_line, shell_escape_double,
        should_signal_process_group,
    };

    #[test]
    fn should_signal_process_group_rejects_reserved_groups() {
        assert!(!should_signal_process_group(0, Some(42)));
        assert!(!should_signal_process_group(1, Some(42)));
    }

    #[test]
    fn should_signal_process_group_rejects_controller_group() {
        assert!(!should_signal_process_group(1234, Some(1234)));
    }

    #[test]
    fn should_signal_process_group_allows_distinct_group() {
        assert!(should_signal_process_group(1234, Some(5678)));
        assert!(should_signal_process_group(1234, None));
    }

    #[test]
    fn build_tmux_shell_command_uses_script_pty_logging() {
        let spec = TaskSpec {
            id: "task-1".to_string(),
            command: "bash".to_string(),
            args: vec!["-lc".to_string(), "echo hello".to_string()],
            workdir: "/tmp".to_string(),
        };

        let shell = build_tmux_shell_command(
            &spec,
            Path::new("/tmp/runner.log"),
            Path::new("/tmp/runner.exit"),
        );

        assert!(
            shell.contains("script -q -e -f -c"),
            "expected script-based pty logging command, got: {shell}"
        );
        let expected_c_arg = shell_escape_double(&command_as_shell_line(&spec.command, &spec.args));
        let expected_snippet = format!("script -q -e -f -c {expected_c_arg}");
        assert!(
            shell.contains(&expected_snippet),
            "script command should pass a single command string via -c, got: {shell}"
        );
        assert!(
            !shell.contains("tee -a"),
            "shell command must not use tee pipeline that breaks tty semantics: {shell}"
        );
    }
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

async fn refresh_tmux_state(session: &TmuxSessionRecord) -> Result<TmuxState> {
    let mut inner = session.inner.lock().await;
    let mut became_terminal = false;

    if inner.state == TmuxState::Stopped {
        became_terminal = true;
    } else if !tmux_has_session(&session.tmux_session)? {
        inner.state = tmux_state_from_exit_code(read_tmux_exit_code(&session.exit_code_path));
        became_terminal = true;
    } else {
        inner.state = match tmux_pane_dead(&session.tmux_session)? {
            Some(true) => {
                became_terminal = true;
                tmux_state_from_exit_code(read_tmux_exit_code(&session.exit_code_path))
            }
            Some(false) => TmuxState::Running,
            None => {
                became_terminal = true;
                tmux_state_from_exit_code(read_tmux_exit_code(&session.exit_code_path))
            }
        };
    }

    let state = inner.state;
    drop(inner);

    if became_terminal || state != TmuxState::Running {
        session.terminal.store(true, Ordering::Relaxed);
        cleanup_terminal_tmux_sessions()?;
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

fn spawn_tmux_state_reaper(session: Arc<TmuxSessionRecord>) {
    tokio::spawn(async move {
        loop {
            let state = match refresh_tmux_state(&session).await {
                Ok(state) => state,
                Err(_) => break,
            };
            if state != TmuxState::Running {
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
        ensure_tmux_available()?;
        std::fs::create_dir_all(&spec.workdir)?;

        let sequence = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_id = unique_tmux_session_id(sequence);
        let tmux_session = session_id.clone();

        let log_dir = Path::new(&spec.workdir).join(TMUX_LOG_DIR);
        std::fs::create_dir_all(&log_dir)?;
        let log_path = log_dir.join(format!("{session_id}.log"));
        let exit_code_path = log_dir.join(format!("{session_id}.exit"));
        let tmux_command = build_tmux_shell_command(&spec, &log_path, &exit_code_path);

        let output = run_tmux(&["new-session", "-d", "-s", &tmux_session, &tmux_command])?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("tmux new-session failed: {}", stderr.trim()));
        }

        let record = Arc::new(TmuxSessionRecord {
            task_id: spec.id.clone(),
            tmux_session,
            log_path,
            exit_code_path,
            inner: Mutex::new(TmuxSessionInner {
                state: TmuxState::Running,
            }),
            sequence,
            terminal: AtomicBool::new(false),
        });

        let mut sessions = tmux_sessions()
            .lock()
            .map_err(|_| anyhow!("tmux session registry lock poisoned"))?;
        sessions.insert(session_id.clone(), Arc::clone(&record));
        drop(sessions);
        spawn_tmux_state_reaper(record);
        cleanup_terminal_tmux_sessions()?;

        Ok(TaskHandle {
            id: spec.id,
            session_id,
        })
    }

    async fn steer(&self, session_id: &str, instruction: &str) -> Result<()> {
        let session = get_tmux_session(session_id)?;
        let state = refresh_tmux_state(&session).await?;
        if state != TmuxState::Running {
            return Err(anyhow!("session is not running: {session_id}"));
        }

        let target = format!("{}:0.0", session.tmux_session);

        let output = run_tmux(&["send-keys", "-t", &target, "-l", instruction])?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("tmux send-keys failed: {}", stderr.trim()));
        }

        let output = run_tmux(&["send-keys", "-t", &target, "C-m"])?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("tmux send-keys enter failed: {}", stderr.trim()));
        }

        Ok(())
    }

    async fn pause(&self, session_id: &str) -> Result<()> {
        let session = get_tmux_session(session_id)?;
        let state = refresh_tmux_state(&session).await?;
        if state != TmuxState::Running {
            return Err(anyhow!("session is not running: {session_id}"));
        }

        #[cfg(unix)]
        {
            let pid = tmux_pane_pid(&session.tmux_session)?
                .ok_or_else(|| anyhow!("unable to determine pane pid for {session_id}"))?;
            if let Some(pgid) = safe_target_process_group(pid, std::process::id()) {
                if signal_process_group(pgid, "-STOP") {
                    return Ok(());
                }
            }
            if signal_pid(pid, "-STOP") {
                return Ok(());
            }
            return Err(anyhow!("failed to pause pane pid {pid} for {session_id}"));
        }

        #[cfg(not(unix))]
        {
            Err(anyhow!("pause is only supported on unix platforms"))
        }
    }

    async fn resume(&self, session_id: &str) -> Result<()> {
        let session = get_tmux_session(session_id)?;
        let state = refresh_tmux_state(&session).await?;
        if state != TmuxState::Running {
            return Err(anyhow!("session is not running: {session_id}"));
        }

        #[cfg(unix)]
        {
            let pid = tmux_pane_pid(&session.tmux_session)?
                .ok_or_else(|| anyhow!("unable to determine pane pid for {session_id}"))?;
            if let Some(pgid) = safe_target_process_group(pid, std::process::id()) {
                if signal_process_group(pgid, "-CONT") {
                    return Ok(());
                }
            }
            if signal_pid(pid, "-CONT") {
                return Ok(());
            }
            return Err(anyhow!("failed to resume pane pid {pid} for {session_id}"));
        }

        #[cfg(not(unix))]
        {
            Err(anyhow!("resume is only supported on unix platforms"))
        }
    }

    async fn stop(&self, session_id: &str) -> Result<()> {
        let session = get_tmux_session(session_id)?;
        let output = run_tmux(&["kill-session", "-t", &session.tmux_session])?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !tmux_target_missing(&stderr) {
                return Err(anyhow!("tmux kill-session failed: {}", stderr.trim()));
            }
        }

        let mut inner = session.inner.lock().await;
        inner.state = TmuxState::Stopped;
        drop(inner);

        session.terminal.store(true, Ordering::Relaxed);
        cleanup_terminal_tmux_sessions()?;

        Ok(())
    }

    async fn status(&self, session_id: &str) -> Result<TaskStatus> {
        let session = get_tmux_session(session_id)?;
        let state = refresh_tmux_state(&session).await?;
        Ok(TaskStatus {
            id: session.task_id.clone(),
            state: state.as_str().to_string(),
        })
    }

    async fn logs(&self, session_id: &str, tail: usize) -> Result<String> {
        let session = get_tmux_session(session_id)?;
        let state = refresh_tmux_state(&session).await?;

        if state == TmuxState::Running {
            if let Some(captured) = tmux_capture_logs(&session.tmux_session, tail)? {
                if !captured.trim().is_empty() {
                    return Ok(captured);
                }
            }
        }

        Ok(read_tmux_log_file(&session.log_path, tail))
    }
}
