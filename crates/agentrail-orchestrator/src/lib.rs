use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMode {
    Process,
    Tmux,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskNode {
    pub id: String,
    #[serde(default)]
    pub deps: Vec<String>,
    pub priority: u8,
    pub runner_mode: RunnerMode,
    pub retry_budget: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct TaskGraph {
    #[serde(default)]
    pub nodes: Vec<TaskNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ExecutionStateMap {
    #[serde(default)]
    pub states: HashMap<String, ExecutionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateCheck {
    pub name: String,
    pub passed: bool,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateReport {
    pub passed: bool,
    pub failed_required: Vec<String>,
    pub failed_optional: Vec<String>,
}

pub fn select_ready_nodes(
    nodes: &[TaskNode],
    states: &HashMap<String, ExecutionState>,
    max_parallel: usize,
) -> Vec<String> {
    if max_parallel == 0 {
        return Vec::new();
    }

    let mut ready: Vec<&TaskNode> = nodes
        .iter()
        .filter(|node| states.get(&node.id) == Some(&ExecutionState::Queued))
        .filter(|node| node.deps.iter().all(|dep| dependency_is_complete(dep, states)))
        .collect();

    ready.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));
    ready
        .into_iter()
        .take(max_parallel)
        .map(|node| node.id.clone())
        .collect()
}

pub fn select_ready_nodes_from_graph(
    graph: &TaskGraph,
    execution: &ExecutionStateMap,
    max_parallel: usize,
) -> Vec<String> {
    select_ready_nodes(&graph.nodes, &execution.states, max_parallel)
}

fn dependency_is_complete(dep: &str, states: &HashMap<String, ExecutionState>) -> bool {
    matches!(
        states.get(dep),
        Some(ExecutionState::ReadyToMerge | ExecutionState::Merged)
    )
}

pub fn evaluate_gate(checks: &[GateCheck]) -> GateReport {
    let mut failed_required = Vec::new();
    let mut failed_optional = Vec::new();

    for check in checks {
        if check.passed {
            continue;
        }

        if check.required {
            failed_required.push(check.name.clone());
        } else {
            failed_optional.push(check.name.clone());
        }
    }

    GateReport {
        passed: failed_required.is_empty(),
        failed_required,
        failed_optional,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{
        evaluate_gate, select_ready_nodes, select_ready_nodes_from_graph, ExecutionState,
        ExecutionStateMap, GateCheck, RunnerMode, TaskGraph, TaskNode,
    };

    fn task(id: &str, deps: Vec<&str>, priority: u8) -> TaskNode {
        TaskNode {
            id: id.to_string(),
            deps: deps.into_iter().map(str::to_string).collect(),
            priority,
            runner_mode: RunnerMode::Process,
            retry_budget: 3,
        }
    }

    #[test]
    fn select_ready_nodes_returns_priority_sorted_ready_nodes() {
        let nodes = vec![
            TaskNode {
                id: "a".to_string(),
                deps: vec![],
                priority: 1,
                runner_mode: RunnerMode::Process,
                retry_budget: 3,
            },
            TaskNode {
                id: "b".to_string(),
                deps: vec!["a".to_string()],
                priority: 10,
                runner_mode: RunnerMode::Tmux,
                retry_budget: 3,
            },
            TaskNode {
                id: "c".to_string(),
                deps: vec![],
                priority: 5,
                runner_mode: RunnerMode::Process,
                retry_budget: 3,
            },
        ];

        let states = HashMap::from([
            ("a".to_string(), ExecutionState::Queued),
            ("b".to_string(), ExecutionState::Queued),
            ("c".to_string(), ExecutionState::Queued),
        ]);

        let ready = select_ready_nodes(&nodes, &states, 2);
        assert_eq!(ready, vec!["c", "a"]);
    }

    #[test]
    fn select_ready_nodes_allows_dependency_after_merge() {
        let nodes = vec![
            TaskNode {
                id: "a".to_string(),
                deps: vec![],
                priority: 1,
                runner_mode: RunnerMode::Process,
                retry_budget: 3,
            },
            TaskNode {
                id: "b".to_string(),
                deps: vec!["a".to_string()],
                priority: 10,
                runner_mode: RunnerMode::Tmux,
                retry_budget: 3,
            },
        ];

        let states = HashMap::from([
            ("a".to_string(), ExecutionState::Merged),
            ("b".to_string(), ExecutionState::Queued),
        ]);

        let ready = select_ready_nodes(&nodes, &states, 2);
        assert_eq!(ready, vec!["b"]);
    }

    #[test]
    fn select_ready_nodes_allows_dependency_after_ready_to_merge() {
        let nodes = vec![
            TaskNode {
                id: "a".to_string(),
                deps: vec![],
                priority: 1,
                runner_mode: RunnerMode::Process,
                retry_budget: 3,
            },
            TaskNode {
                id: "b".to_string(),
                deps: vec!["a".to_string()],
                priority: 10,
                runner_mode: RunnerMode::Tmux,
                retry_budget: 3,
            },
        ];

        let states = HashMap::from([
            ("a".to_string(), ExecutionState::ReadyToMerge),
            ("b".to_string(), ExecutionState::Queued),
        ]);

        let ready = select_ready_nodes(&nodes, &states, 2);
        assert_eq!(ready, vec!["b"]);
    }

    #[test]
    fn evaluate_gate_passes_when_all_required_checks_pass() {
        let checks = vec![
            GateCheck {
                name: "fmt".to_string(),
                passed: true,
                required: true,
            },
            GateCheck {
                name: "compat".to_string(),
                passed: true,
                required: true,
            },
            GateCheck {
                name: "optional-ai-review".to_string(),
                passed: false,
                required: false,
            },
        ];

        let report = evaluate_gate(&checks);
        assert!(report.passed);
        assert!(report.failed_required.is_empty());
        assert_eq!(report.failed_optional, vec!["optional-ai-review"]);
    }

    #[test]
    fn evaluate_gate_blocks_when_required_check_fails() {
        let checks = vec![
            GateCheck {
                name: "test".to_string(),
                passed: false,
                required: true,
            },
            GateCheck {
                name: "review".to_string(),
                passed: true,
                required: false,
            },
        ];

        let report = evaluate_gate(&checks);
        assert!(!report.passed);
        assert_eq!(report.failed_required, vec!["test"]);
    }

    #[test]
    fn select_ready_nodes_returns_empty_when_max_parallel_is_zero() {
        let nodes = vec![task("a", vec![], 1)];
        let states = HashMap::from([("a".to_string(), ExecutionState::Queued)]);

        let ready = select_ready_nodes(&nodes, &states, 0);
        assert!(ready.is_empty());
    }

    #[test]
    fn select_ready_nodes_blocks_when_dependency_state_is_unknown() {
        let nodes = vec![task("a", vec![], 1), task("b", vec!["a"], 10)];
        let states = HashMap::from([("b".to_string(), ExecutionState::Queued)]);

        let ready = select_ready_nodes(&nodes, &states, 1);
        assert!(ready.is_empty());
    }

    #[test]
    fn select_ready_nodes_tie_breaks_by_id_for_same_priority() {
        let nodes = vec![
            task("gamma", vec![], 7),
            task("alpha", vec![], 7),
            task("beta", vec![], 7),
        ];
        let states = HashMap::from([
            ("gamma".to_string(), ExecutionState::Queued),
            ("alpha".to_string(), ExecutionState::Queued),
            ("beta".to_string(), ExecutionState::Queued),
        ]);

        let ready = select_ready_nodes(&nodes, &states, 3);
        assert_eq!(ready, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn evaluate_gate_reports_required_and_optional_failures() {
        let checks = vec![
            GateCheck {
                name: "fmt".to_string(),
                passed: false,
                required: true,
            },
            GateCheck {
                name: "compat".to_string(),
                passed: false,
                required: false,
            },
            GateCheck {
                name: "test".to_string(),
                passed: true,
                required: true,
            },
        ];

        let report = evaluate_gate(&checks);
        assert!(!report.passed);
        assert_eq!(report.failed_required, vec!["fmt"]);
        assert_eq!(report.failed_optional, vec!["compat"]);
    }

    #[test]
    fn select_ready_nodes_from_graph_uses_graph_and_execution_dtos() {
        let graph = TaskGraph {
            nodes: vec![task("a", vec![], 5), task("b", vec!["a"], 10)],
        };
        let execution = ExecutionStateMap {
            states: HashMap::from([
                ("a".to_string(), ExecutionState::Merged),
                ("b".to_string(), ExecutionState::Queued),
            ]),
        };

        let ready = select_ready_nodes_from_graph(&graph, &execution, 2);
        assert_eq!(ready, vec!["b"]);
    }

    #[test]
    fn serde_enum_wire_format_uses_snake_case() {
        assert_eq!(serde_json::to_value(RunnerMode::Process).unwrap(), json!("process"));
        assert_eq!(
            serde_json::to_value(ExecutionState::ReadyToMerge).unwrap(),
            json!("ready_to_merge")
        );

        assert_eq!(
            serde_json::from_value::<RunnerMode>(json!("tmux")).unwrap(),
            RunnerMode::Tmux
        );
        assert_eq!(
            serde_json::from_value::<ExecutionState>(json!("failed_terminal")).unwrap(),
            ExecutionState::FailedTerminal
        );
    }

    #[test]
    fn serde_task_node_and_task_graph_roundtrip() {
        let node = TaskNode {
            id: "task-a".to_string(),
            deps: vec!["dep-1".to_string(), "dep-2".to_string()],
            priority: 9,
            runner_mode: RunnerMode::Tmux,
            retry_budget: 2,
        };

        let node_json = serde_json::to_value(&node).unwrap();
        assert_eq!(
            node_json,
            json!({
                "id": "task-a",
                "deps": ["dep-1", "dep-2"],
                "priority": 9,
                "runner_mode": "tmux",
                "retry_budget": 2
            })
        );
        let decoded_node: TaskNode = serde_json::from_value(node_json).unwrap();
        assert_eq!(decoded_node, node);

        let graph = TaskGraph {
            nodes: vec![decoded_node],
        };
        let graph_json = serde_json::to_value(&graph).unwrap();
        assert_eq!(
            graph_json,
            json!({
                "nodes": [{
                    "id": "task-a",
                    "deps": ["dep-1", "dep-2"],
                    "priority": 9,
                    "runner_mode": "tmux",
                    "retry_budget": 2
                }]
            })
        );
        let decoded_graph: TaskGraph = serde_json::from_value(graph_json).unwrap();
        assert_eq!(decoded_graph, graph);
    }
}
