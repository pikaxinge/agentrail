use std::{path::Path, process::Command as StdCommand, time::Duration};

use agentrail_runner::{AgentRunner, TaskSpec, TmuxRunner};
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

fn skip_if_no_tmux() -> bool {
    if tmux_available() {
        return false;
    }

    eprintln!("skipping tmux test because tmux is not available");
    true
}

async fn wait_for_terminal_state(
    runner: &TmuxRunner,
    session_id: &str,
    max_attempts: usize,
) -> String {
    let mut state = "running".to_string();
    for _ in 0..max_attempts {
        let status = runner
            .status(session_id)
            .await
            .expect("status should succeed");
        state = status.state;
        if state != "running" {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    state
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
