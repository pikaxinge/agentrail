use std::collections::HashMap;

use agentrail_orchestrator::{ExecutionState, RunnerMode, TaskNode, select_ready_nodes};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn build_scheduler_input(
    task_count: usize,
) -> (Vec<TaskNode>, HashMap<String, ExecutionState>, usize) {
    let nodes: Vec<TaskNode> = (0..task_count)
        .map(|idx| TaskNode {
            id: format!("task-{idx:05}"),
            deps: Vec::new(),
            priority: (idx % 32) as u8,
            runner_mode: RunnerMode::Process,
            retry_budget: 3,
        })
        .collect();

    let states: HashMap<String, ExecutionState> = nodes
        .iter()
        .map(|node| (node.id.clone(), ExecutionState::Queued))
        .collect();

    let max_parallel = task_count.min(128).max(1);
    (nodes, states, max_parallel)
}

fn scheduler_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler_throughput");

    for task_count in [128_usize, 512, 2_048, 8_192] {
        let (nodes, states, max_parallel) = build_scheduler_input(task_count);

        group.throughput(Throughput::Elements(task_count as u64));
        group.bench_with_input(
            BenchmarkId::new("select_ready_nodes", task_count),
            &task_count,
            |b, _| {
                b.iter(|| {
                    black_box(select_ready_nodes(
                        black_box(&nodes),
                        black_box(&states),
                        black_box(max_parallel),
                    ))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, scheduler_throughput);
criterion_main!(benches);
