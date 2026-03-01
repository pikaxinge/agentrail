use std::{
    collections::VecDeque, path::Path, process::Command as StdCommand, sync::Mutex, time::Duration,
};

use agentrail_runner::{AgentRunner, TaskHandle, TaskSpec, TaskStatus, TmuxRunner};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tempfile::tempdir;
use tokio::time::sleep;

fn runner() -> TmuxRunner {
    TmuxRunner
}

fn workdir_string(path: &Path) -> String {
    path.display().to_string()
}

fn tmux_available() -> bool {
    StdCommand::new("tmux")
        .arg("-V")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn tmux_tests_enabled() -> bool {
    std::env::var("AGENTRAIL_RUN_TMUX_TESTS")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn nested_tmux_tests_enabled() -> bool {
    std::env::var("AGENTRAIL_RUN_TMUX_TESTS_NESTED")
        .map(|value| value == "1")
        .unwrap_or(false)
}

fn skip_if_no_tmux() -> bool {
    if !tmux_tests_enabled() {
        eprintln!("skipping tmux test; set AGENTRAIL_RUN_TMUX_TESTS=1 to enable");
        return true;
    }

    if std::env::var_os("TMUX").is_some() && !nested_tmux_tests_enabled() {
        eprintln!(
            "skipping tmux test inside existing tmux session; set AGENTRAIL_RUN_TMUX_TESTS_NESTED=1 to force"
        );
        return true;
    }

    if tmux_available() {
        return false;
    }

    eprintln!("skipping tmux test because tmux is not available");
    true
}

fn is_transient_tmux_status_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("server exited unexpectedly")
}

async fn wait_for_terminal_state<R: AgentRunner + ?Sized>(
    runner: &R,
    session_id: &str,
    max_attempts: usize,
) -> String {
    let mut state = "running".to_string();
    let mut last_transient_error = None;

    for _ in 0..max_attempts {
        match runner.status(session_id).await {
            Ok(status) => {
                state = status.state;
                last_transient_error = None;
                if state != "running" {
                    break;
                }
            }
            Err(error) => {
                let message = error.to_string();
                if !is_transient_tmux_status_error(&message) {
                    panic!("status should succeed: {error}");
                }
                last_transient_error = Some(message);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }

    if let Some(message) = last_transient_error {
        panic!("status should recover after transient tmux error: {message}");
    }

    state
}

struct StatusSequenceRunner {
    responses: Mutex<VecDeque<Result<TaskStatus>>>,
}

impl StatusSequenceRunner {
    fn new(responses: Vec<Result<TaskStatus>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
        }
    }
}

#[async_trait]
impl AgentRunner for StatusSequenceRunner {
    async fn start(&self, _spec: TaskSpec) -> Result<TaskHandle> {
        Err(anyhow!("unused in test"))
    }

    async fn steer(&self, _session_id: &str, _instruction: &str) -> Result<()> {
        Err(anyhow!("unused in test"))
    }

    async fn pause(&self, _session_id: &str) -> Result<()> {
        Err(anyhow!("unused in test"))
    }

    async fn resume(&self, _session_id: &str) -> Result<()> {
        Err(anyhow!("unused in test"))
    }

    async fn stop(&self, _session_id: &str) -> Result<()> {
        Err(anyhow!("unused in test"))
    }

    async fn status(&self, _session_id: &str) -> Result<TaskStatus> {
        self.responses
            .lock()
            .expect("response queue lock")
            .pop_front()
            .expect("response should exist")
    }

    async fn logs(&self, _session_id: &str, _tail: usize) -> Result<String> {
        Err(anyhow!("unused in test"))
    }
}

#[tokio::test]
async fn wait_for_terminal_state_retries_transient_tmux_server_errors() {
    let runner = StatusSequenceRunner::new(vec![
        Err(anyhow!(
            "tmux has-session failed for tmux-test: server exited unexpectedly"
        )),
        Ok(TaskStatus {
            id: "task-1".to_string(),
            state: "running".to_string(),
        }),
        Ok(TaskStatus {
            id: "task-1".to_string(),
            state: "completed".to_string(),
        }),
    ]);

    let final_state = wait_for_terminal_state(&runner, "tmux-test", 5).await;
    assert_eq!(final_state, "completed");
}

#[tokio::test]
async fn start_creates_unique_session_and_logs_output() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");

    let first = tmux_runner
        .start(TaskSpec {
            id: "task-1".to_string(),
            command: "bash".to_string(),
            args: vec![
                "-lc".to_string(),
                "echo tmux-runner-ok; sleep 0.1".to_string(),
            ],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("start should succeed");

    let second = tmux_runner
        .start(TaskSpec {
            id: "task-2".to_string(),
            command: "bash".to_string(),
            args: vec![
                "-lc".to_string(),
                "echo tmux-runner-ok-2; sleep 0.1".to_string(),
            ],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("second start should succeed");

    assert_ne!(first.session_id, second.session_id);

    let final_state = wait_for_terminal_state(&tmux_runner, &first.session_id, 40).await;
    assert_eq!(final_state, "completed");

    let logs = tmux_runner
        .logs(&first.session_id, 20)
        .await
        .expect("logs should succeed");
    assert!(logs.contains("tmux-runner-ok"));

    tmux_runner
        .stop(&second.session_id)
        .await
        .expect("stop should succeed");
}

#[tokio::test]
async fn steer_pause_resume_stop_and_status_work_best_effort() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let handle = tmux_runner
        .start(TaskSpec {
            id: "task-control".to_string(),
            command: "bash".to_string(),
            args: vec![
                "-lc".to_string(),
                "while read line; do echo got:$line; done".to_string(),
            ],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("start should succeed");

    tmux_runner
        .steer(&handle.session_id, "hello-from-steer")
        .await
        .expect("steer should succeed");

    let mut saw_steer_line = false;
    for _ in 0..20 {
        let logs = tmux_runner
            .logs(&handle.session_id, 50)
            .await
            .expect("logs should succeed");
        if logs.contains("got:hello-from-steer") {
            saw_steer_line = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_steer_line, "steer input should appear in logs");

    tmux_runner
        .pause(&handle.session_id)
        .await
        .expect("pause should succeed");
    tmux_runner
        .resume(&handle.session_id)
        .await
        .expect("resume should succeed");

    tmux_runner
        .stop(&handle.session_id)
        .await
        .expect("stop should succeed");
    let status = tmux_runner
        .status(&handle.session_id)
        .await
        .expect("status should succeed");
    assert_eq!(status.state, "stopped");
}

#[tokio::test]
async fn interactive_stdout_tty_command_starts_successfully() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let handle = tmux_runner
        .start(TaskSpec {
            id: "task-interactive-tty".to_string(),
            command: "bash".to_string(),
            args: vec![
                "-lc".to_string(),
                "if [ ! -t 1 ]; then echo stdout is not tty; exit 1; fi; echo tty-ok".to_string(),
            ],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("start should succeed");

    let final_state = wait_for_terminal_state(&tmux_runner, &handle.session_id, 40).await;
    assert_eq!(final_state, "completed");

    let logs = tmux_runner
        .logs(&handle.session_id, 40)
        .await
        .expect("logs should succeed");
    assert!(
        logs.contains("tty-ok"),
        "expected tty confirmation in logs: {logs}"
    );
    assert!(
        !logs.contains("stdout is not tty"),
        "interactive command unexpectedly saw non-tty stdout: {logs}"
    );
}

#[tokio::test]
async fn unknown_tmux_session_returns_error() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let err = tmux_runner
        .status("missing-session")
        .await
        .expect_err("unknown session should fail");
    assert!(err.to_string().contains("session"));
}

#[tokio::test]
async fn terminal_tmux_sessions_are_bounded() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");

    let oldest = tmux_runner
        .start(TaskSpec {
            id: "task-oldest".to_string(),
            command: "bash".to_string(),
            args: vec!["-lc".to_string(), "echo oldest-done".to_string()],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("oldest start should succeed");
    let oldest_state = wait_for_terminal_state(&tmux_runner, &oldest.session_id, 40).await;
    assert_eq!(oldest_state, "completed");

    for index in 0..270 {
        let handle = tmux_runner
            .start(TaskSpec {
                id: format!("task-retain-{index}"),
                command: "bash".to_string(),
                args: vec!["-lc".to_string(), "sleep 30".to_string()],
                workdir: workdir_string(tmp.path()),
            })
            .await
            .expect("retention start should succeed");

        tmux_runner
            .stop(&handle.session_id)
            .await
            .expect("retention stop should succeed");
    }

    let err = tmux_runner
        .status(&oldest.session_id)
        .await
        .expect_err("oldest terminal session should be evicted");
    assert!(err.to_string().contains("session not found"));
}

#[tokio::test]
async fn non_zero_exit_transitions_to_failed() {
    if skip_if_no_tmux() {
        return;
    }

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");

    let handle = tmux_runner
        .start(TaskSpec {
            id: "task-fail".to_string(),
            command: "bash".to_string(),
            args: vec!["-lc".to_string(), "echo tmux-nonzero; exit 17".to_string()],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("start should succeed");

    let final_state = wait_for_terminal_state(&tmux_runner, &handle.session_id, 40).await;
    assert_eq!(final_state, "failed");
}
