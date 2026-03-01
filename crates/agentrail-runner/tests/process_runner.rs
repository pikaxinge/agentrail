use std::{path::Path, time::Duration};

use agentrail_runner::{AgentRunner, ProcessRunner, TaskSpec};
use tempfile::tempdir;
use tokio::process::Command;
use tokio::time::sleep;

fn runner() -> ProcessRunner {
    ProcessRunner
}

fn workdir_string(path: &Path) -> String {
    path.display().to_string()
}

#[tokio::test]
async fn start_then_complete_exposes_logs() {
    let process_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let spec = TaskSpec {
        id: "task-echo".to_string(),
        command: "bash".to_string(),
        args: vec!["-lc".to_string(), "echo process-runner-ok".to_string()],
        workdir: workdir_string(tmp.path()),
    };

    let handle = process_runner
        .start(spec)
        .await
        .expect("start should succeed");
    assert!(!handle.session_id.is_empty());

    let mut final_state = "running".to_string();
    for _ in 0..10 {
        let status = process_runner
            .status(&handle.session_id)
            .await
            .expect("status should succeed");
        final_state = status.state;
        if final_state != "running" {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(final_state, "completed");
    let logs = process_runner
        .logs(&handle.session_id, 20)
        .await
        .expect("logs should succeed");
    assert!(logs.contains("process-runner-ok"));
}

#[tokio::test]
async fn stop_transitions_to_stopped() {
    let process_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let spec = TaskSpec {
        id: "task-sleep".to_string(),
        command: "bash".to_string(),
        args: vec!["-lc".to_string(), "sleep 5".to_string()],
        workdir: workdir_string(tmp.path()),
    };

    let handle = process_runner
        .start(spec)
        .await
        .expect("start should succeed");
    process_runner
        .stop(&handle.session_id)
        .await
        .expect("stop should succeed");

    let status = process_runner
        .status(&handle.session_id)
        .await
        .expect("status should succeed");
    assert_eq!(status.state, "stopped");
}

#[tokio::test]
async fn unknown_session_returns_error() {
    let process_runner = runner();
    let err = process_runner
        .status("missing-session")
        .await
        .expect_err("unknown session should fail");
    assert!(err.to_string().contains("session"));
}

#[tokio::test]
async fn steer_pause_resume_return_unsupported() {
    let process_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let spec = TaskSpec {
        id: "task-control".to_string(),
        command: "bash".to_string(),
        args: vec!["-lc".to_string(), "sleep 2".to_string()],
        workdir: workdir_string(tmp.path()),
    };

    let handle = process_runner
        .start(spec)
        .await
        .expect("start should succeed");

    let err = process_runner
        .steer(&handle.session_id, "focus api")
        .await
        .expect_err("steer should be unsupported");
    assert!(err.to_string().contains("unsupported"));

    let err = process_runner
        .pause(&handle.session_id)
        .await
        .expect_err("pause should be unsupported");
    assert!(err.to_string().contains("unsupported"));

    let err = process_runner
        .resume(&handle.session_id)
        .await
        .expect_err("resume should be unsupported");
    assert!(err.to_string().contains("unsupported"));

    process_runner
        .stop(&handle.session_id)
        .await
        .expect("stop should succeed");
}

#[tokio::test]
async fn stop_kills_process_group_children() {
    let process_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let child_pid_file = tmp.path().join("child.pid");
    let script = format!(
        "sleep 30 & child=$!; echo \"$child\" > {}; wait",
        child_pid_file.display()
    );

    let spec = TaskSpec {
        id: "task-tree".to_string(),
        command: "bash".to_string(),
        args: vec!["-lc".to_string(), script],
        workdir: workdir_string(tmp.path()),
    };

    let handle = process_runner
        .start(spec)
        .await
        .expect("start should succeed");

    for _ in 0..20 {
        if child_pid_file.exists() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(child_pid_file.exists(), "child pid file should exist");

    let child_pid = std::fs::read_to_string(&child_pid_file)
        .expect("read child pid")
        .trim()
        .to_string();

    process_runner
        .stop(&handle.session_id)
        .await
        .expect("stop should succeed");

    let mut terminated = false;
    for _ in 0..20 {
        let alive = Command::new("bash")
            .args(["-lc", &format!("kill -0 {} 2>/dev/null", child_pid)])
            .status()
            .await
            .expect("kill probe should run")
            .success();

        if !alive {
            terminated = true;
            break;
        }

        let stat_output = Command::new("bash")
            .args(["-lc", &format!("ps -o stat= -p {}", child_pid)])
            .output()
            .await
            .expect("ps probe should run");
        let stat = String::from_utf8_lossy(&stat_output.stdout);
        if stat.trim_start().starts_with('Z') {
            terminated = true;
            break;
        }

        sleep(Duration::from_millis(100)).await;
    }

    assert!(terminated, "child process should be terminated");
}

#[tokio::test]
async fn logs_are_bounded_in_memory() {
    let process_runner = runner();
    let tmp = tempdir().expect("tempdir");
    let spec = TaskSpec {
        id: "task-logs".to_string(),
        command: "bash".to_string(),
        args: vec![
            "-lc".to_string(),
            "for i in $(seq 1 3000); do echo line-$i; done".to_string(),
        ],
        workdir: workdir_string(tmp.path()),
    };

    let handle = process_runner
        .start(spec)
        .await
        .expect("start should succeed");
    for _ in 0..30 {
        let status = process_runner
            .status(&handle.session_id)
            .await
            .expect("status should succeed");
        if status.state != "running" {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let logs = process_runner
        .logs(&handle.session_id, 0)
        .await
        .expect("logs should succeed");
    let line_count = logs.lines().count();
    assert!(
        line_count <= 2000,
        "log line count should be bounded, got {line_count}"
    );
}
