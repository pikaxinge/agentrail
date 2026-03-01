use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use agentrail_orchestrator::{
    ExecutionState, ExecutionStateMap, LaunchRequest, RunnerMode, TaskGraph, TaskNode,
    launch_ready_tasks,
};
use agentrail_runner::{AgentRunner, TaskHandle, TaskSpec, TaskStatus};
use agentrail_store::{TaskRecord, TaskRuntimeState, TaskStore};
use anyhow::Result;
use async_trait::async_trait;

struct MockRunner {
    started: Arc<Mutex<Vec<TaskSpec>>>,
}

impl MockRunner {
    fn new() -> Self {
        Self {
            started: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn started_specs(&self) -> Vec<TaskSpec> {
        self.started.lock().expect("lock started specs").clone()
    }
}

struct FailingRunner {
    started: Arc<Mutex<Vec<TaskSpec>>>,
    fail_task_id: String,
}

impl FailingRunner {
    fn new(fail_task_id: impl Into<String>) -> Self {
        Self {
            started: Arc::new(Mutex::new(Vec::new())),
            fail_task_id: fail_task_id.into(),
        }
    }

    fn started_specs(&self) -> Vec<TaskSpec> {
        self.started.lock().expect("lock started specs").clone()
    }
}

#[async_trait]
impl AgentRunner for MockRunner {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle> {
        self.started
            .lock()
            .expect("lock started specs")
            .push(spec.clone());

        Ok(TaskHandle {
            id: spec.id.clone(),
            session_id: format!("session-{}", spec.id),
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
impl AgentRunner for FailingRunner {
    async fn start(&self, spec: TaskSpec) -> Result<TaskHandle> {
        self.started
            .lock()
            .expect("lock started specs")
            .push(spec.clone());

        if spec.id == self.fail_task_id {
            anyhow::bail!("runner start failed for {}", spec.id);
        }

        Ok(TaskHandle {
            id: spec.id.clone(),
            session_id: format!("session-{}", spec.id),
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

fn task(id: &str, deps: Vec<&str>, priority: u8) -> TaskNode {
    TaskNode {
        id: id.to_string(),
        deps: deps.into_iter().map(str::to_string).collect(),
        priority,
        runner_mode: RunnerMode::Process,
        retry_budget: 3,
    }
}

#[tokio::test]
async fn launch_ready_tasks_starts_only_ready_nodes_and_marks_running() {
    let graph = TaskGraph {
        nodes: vec![task("a", vec![], 5), task("b", vec!["a"], 10)],
    };
    let execution = ExecutionStateMap {
        states: HashMap::from([
            ("a".to_string(), ExecutionState::Queued),
            ("b".to_string(), ExecutionState::Queued),
        ]),
    };

    let launches = HashMap::from([
        (
            "a".to_string(),
            LaunchRequest {
                task_id: "a".to_string(),
                worker_id: "worker-a".to_string(),
                command: "echo".to_string(),
                args: vec!["a".to_string()],
                workdir: "/tmp/a".to_string(),
                retry_budget: 3,
            },
        ),
        (
            "b".to_string(),
            LaunchRequest {
                task_id: "b".to_string(),
                worker_id: "worker-b".to_string(),
                command: "echo".to_string(),
                args: vec!["b".to_string()],
                workdir: "/tmp/b".to_string(),
                retry_budget: 3,
            },
        ),
    ]);

    let runner = MockRunner::new();
    let store = TaskStore::connect("memory://launch-ready");

    let launched = launch_ready_tasks(&runner, &store, &graph, &execution, &launches, 2)
        .await
        .expect("launch should succeed");

    assert_eq!(launched.len(), 1);
    assert_eq!(launched[0].task_id, "a");
    assert_eq!(launched[0].session_id, "session-a");

    let started_specs = runner.started_specs();
    assert_eq!(started_specs.len(), 1);
    assert_eq!(started_specs[0].id, "a");

    let running = store
        .get_task("a")
        .expect("store read should succeed")
        .expect("task a should be recorded");
    assert_eq!(running.state, TaskRuntimeState::Running);

    let blocked = store.get_task("b").expect("store read should succeed");
    assert!(
        blocked.is_none(),
        "blocked dependency task should not launch"
    );
}

#[tokio::test]
async fn launch_ready_tasks_errors_when_request_missing() {
    let graph = TaskGraph {
        nodes: vec![task("a", vec![], 5)],
    };
    let execution = ExecutionStateMap {
        states: HashMap::from([("a".to_string(), ExecutionState::Queued)]),
    };

    let launches: HashMap<String, LaunchRequest> = HashMap::new();

    let runner = MockRunner::new();
    let store = TaskStore::connect("memory://launch-missing");

    let err = launch_ready_tasks(&runner, &store, &graph, &execution, &launches, 1)
        .await
        .expect_err("missing request should fail");

    assert!(
        err.to_string().contains("missing launch request"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn launch_ready_tasks_reuses_existing_runtime_record_for_relaunch() {
    let graph = TaskGraph {
        nodes: vec![task("a", vec![], 5)],
    };
    let execution = ExecutionStateMap {
        states: HashMap::from([("a".to_string(), ExecutionState::Queued)]),
    };
    let launches = HashMap::from([(
        "a".to_string(),
        LaunchRequest {
            task_id: "a".to_string(),
            worker_id: "worker-a".to_string(),
            command: "echo".to_string(),
            args: vec!["a".to_string()],
            workdir: "/tmp/a".to_string(),
            retry_budget: 3,
        },
    )]);

    let runner = MockRunner::new();
    let store = TaskStore::connect("memory://launch-relaunch");
    store
        .upsert_task(&TaskRecord::new("a", "worker-a", 3))
        .expect("seed task should succeed");

    let launched = launch_ready_tasks(&runner, &store, &graph, &execution, &launches, 1)
        .await
        .expect("relaunch should succeed");
    assert_eq!(launched.len(), 1);

    let running = store
        .get_task("a")
        .expect("store read should succeed")
        .expect("task should exist");
    assert_eq!(running.state, TaskRuntimeState::Running);
}

#[tokio::test]
async fn launch_ready_tasks_prevalidates_requests_to_avoid_partial_launch() {
    let graph = TaskGraph {
        nodes: vec![task("a", vec![], 10), task("b", vec![], 1)],
    };
    let execution = ExecutionStateMap {
        states: HashMap::from([
            ("a".to_string(), ExecutionState::Queued),
            ("b".to_string(), ExecutionState::Queued),
        ]),
    };
    let launches = HashMap::from([(
        "a".to_string(),
        LaunchRequest {
            task_id: "a".to_string(),
            worker_id: "worker-a".to_string(),
            command: "echo".to_string(),
            args: vec!["a".to_string()],
            workdir: "/tmp/a".to_string(),
            retry_budget: 3,
        },
    )]);

    let runner = MockRunner::new();
    let store = TaskStore::connect("memory://launch-preflight");

    let err = launch_ready_tasks(&runner, &store, &graph, &execution, &launches, 2)
        .await
        .expect_err("prevalidation should fail");
    assert!(
        err.to_string().contains("missing launch request"),
        "unexpected error: {err}"
    );
    assert!(
        runner.started_specs().is_empty(),
        "no task should start before request prevalidation passes"
    );
}

#[tokio::test]
async fn launch_ready_tasks_preserves_prior_launches_in_error_context_on_runner_failure() {
    let graph = TaskGraph {
        nodes: vec![task("a", vec![], 10), task("b", vec![], 1)],
    };
    let execution = ExecutionStateMap {
        states: HashMap::from([
            ("a".to_string(), ExecutionState::Queued),
            ("b".to_string(), ExecutionState::Queued),
        ]),
    };
    let launches = HashMap::from([
        (
            "a".to_string(),
            LaunchRequest {
                task_id: "a".to_string(),
                worker_id: "worker-a".to_string(),
                command: "echo".to_string(),
                args: vec!["a".to_string()],
                workdir: "/tmp/a".to_string(),
                retry_budget: 3,
            },
        ),
        (
            "b".to_string(),
            LaunchRequest {
                task_id: "b".to_string(),
                worker_id: "worker-b".to_string(),
                command: "echo".to_string(),
                args: vec!["b".to_string()],
                workdir: "/tmp/b".to_string(),
                retry_budget: 3,
            },
        ),
    ]);

    let runner = FailingRunner::new("b");
    let store = TaskStore::connect("memory://launch-runner-failure");

    let err = launch_ready_tasks(&runner, &store, &graph, &execution, &launches, 2)
        .await
        .expect_err("runner failure should propagate");
    assert!(
        err.to_string().contains("launched before failure: a"),
        "error should include already-launched task ids, got: {err}"
    );

    let running_a = store
        .get_task("a")
        .expect("store read should succeed")
        .expect("task a should exist");
    assert_eq!(running_a.state, TaskRuntimeState::Running);

    let failed_b = store
        .get_task("b")
        .expect("store read should succeed")
        .expect("task b should exist");
    assert_eq!(failed_b.state, TaskRuntimeState::FailedRetryable);

    let started = runner.started_specs();
    assert_eq!(
        started.len(),
        2,
        "runner should attempt both tasks in order"
    );
}
