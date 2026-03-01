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

fn tmux_has_session(session_name: &str) -> bool {
    StdCommand::new("tmux")
        .args(["has-session", "-t", session_name])
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

struct LegacySessionGuard {
    session_name: &'static str,
}

impl Drop for LegacySessionGuard {
    fn drop(&mut self) {
        let _ = StdCommand::new("tmux")
            .args(["kill-session", "-t", self.session_name])
            .status();
    }
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
async fn start_avoids_legacy_tmux_session_name_collision() {
    if skip_if_no_tmux() {
        return;
    }

    if tmux_has_session("tmux-1") {
        eprintln!("skipping collision test because existing session tmux-1 is in use");
        return;
    }

    let created = StdCommand::new("tmux")
        .args(["new-session", "-d", "-s", "tmux-1", "sleep 30"])
        .status()
        .expect("should run tmux new-session");
    assert!(created.success(), "legacy tmux-1 session should be created");
    let _legacy_guard = LegacySessionGuard {
        session_name: "tmux-1",
    };

    let tmux_runner = runner();
    let tmp = tempdir().expect("tempdir");

    let handle = tmux_runner
        .start(TaskSpec {
            id: "task-legacy-collision".to_string(),
            command: "bash".to_string(),
            args: vec!["-lc".to_string(), "echo collision-safe".to_string()],
            workdir: workdir_string(tmp.path()),
        })
        .await
        .expect("start should avoid legacy collision");

    assert_ne!(handle.session_id, "tmux-1");
    assert!(
        handle.session_id.split('-').count() >= 4,
        "expected unique session id with pid/timestamp/sequence, got {}",
        handle.session_id
    );

    let final_state = wait_for_terminal_state(&tmux_runner, &handle.session_id, 40).await;
    assert_eq!(final_state, "completed");
}
