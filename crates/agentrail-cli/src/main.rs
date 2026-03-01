use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use agentrail_core::{Phase, PhaseStatus, Plan, Step, StepStatus};

#[derive(Debug, Parser)]
#[command(name = "agentrail")]
#[command(about = "Agentrail control plane CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Version,
    Status {
        #[arg(long)]
        plan: PathBuf,
    },
    Show {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        step_id: String,
    },
    Next {
        #[arg(long)]
        plan: PathBuf,
    },
    Claim {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        step_id: String,
        #[arg(long)]
        agent: String,
    },
    Complete {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        step_id: String,
        #[arg(long)]
        evidence: String,
    },
    Dashboard {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    Dag {
        #[arg(long)]
        plan: PathBuf,
    },
    Report {
        #[arg(long)]
        plan: PathBuf,
    },
    Orchestrate {
        #[command(subcommand)]
        command: OrchestrateCommands,
    },
    Mcp {
        #[arg(long, default_value = "stdio")]
        transport: String,
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: String,
    },
    Serve {
        #[arg(long, default_value = "http")]
        transport: String,
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: String,
    },
}

#[derive(Debug, Subcommand)]
enum OrchestrateCommands {
    Plan {
        #[arg(long)]
        max_parallel: usize,
    },
    Tick {
        #[arg(long)]
        retry_count: u8,
        #[arg(long)]
        retry_budget: u8,
        #[arg(long, action = clap::ArgAction::Set)]
        reassigned_once: bool,
    },
    Resume {
        #[arg(long)]
        task_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("agentrail 0.1.0");
        }
        Commands::Status { plan } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            let (pending, claimed, done) = step_counts(&loaded_plan);
            println!(
                "{}",
                json!({
                    "operation": "status",
                    "project": loaded_plan.project,
                    "phase_count": loaded_plan.phases.len(),
                    "step_counts": {
                        "pending": pending,
                        "claimed": claimed,
                        "done": done
                    }
                })
            );
        }
        Commands::Show { plan, step_id } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            let step = find_step(&loaded_plan, &step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
            println!(
                "{}",
                json!({
                    "operation": "show",
                    "step": step
                })
            );
        }
        Commands::Next { plan } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            let step = next_ready_step(&loaded_plan);
            println!(
                "{}",
                json!({
                    "operation": "next",
                    "step": step
                })
            );
        }
        Commands::Claim {
            plan,
            step_id,
            agent,
        } => {
            let plan_path = plan;
            let (mut loaded_plan, hash) = agentrail_plan_io::load_plan(&plan_path)?;
            {
                let (phase, step) = find_step_with_phase(&loaded_plan, &step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                if step.status != StepStatus::Pending {
                    anyhow::bail!(
                        "invalid state transition: claim requires pending -> claimed, current={:?}",
                        step.status
                    );
                }
                let step_status_by_id = step_status_map(&loaded_plan);
                let phase_status_by_id = phase_status_map(&loaded_plan);
                if !phase_ready_for_work(phase, &phase_status_by_id)
                    || !step_dependencies_ready(step, &step_status_by_id)
                {
                    anyhow::bail!("dependencies not ready: {step_id}");
                }
            }
            {
                let step = find_step_mut(&mut loaded_plan, &step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                step.status = StepStatus::Claimed;
                step.claimed_by = Some(agent);
                step.evidence = None;
            }
            agentrail_core::recalc_lock_status(&mut loaded_plan);
            agentrail_plan_io::save_plan(&loaded_plan, &plan_path, Some(&hash))?;
            let step = find_step(&loaded_plan, &step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found after claim: {step_id}"))?;
            println!(
                "{}",
                json!({
                    "operation": "claim",
                    "step": step
                })
            );
        }
        Commands::Complete {
            plan,
            step_id,
            evidence,
        } => {
            let plan_path = plan;
            let (mut loaded_plan, hash) = agentrail_plan_io::load_plan(&plan_path)?;
            {
                let (phase, step) = find_step_with_phase(&loaded_plan, &step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                if step.status != StepStatus::Claimed {
                    anyhow::bail!(
                        "invalid state transition: complete requires claimed -> done, current={:?}",
                        step.status
                    );
                }
                let step_status_by_id = step_status_map(&loaded_plan);
                let phase_status_by_id = phase_status_map(&loaded_plan);
                if !phase_ready_for_work(phase, &phase_status_by_id)
                    || !step_dependencies_ready(step, &step_status_by_id)
                {
                    anyhow::bail!("dependencies not ready: {step_id}");
                }
            }
            {
                let step = find_step_mut(&mut loaded_plan, &step_id)
                    .ok_or_else(|| anyhow::anyhow!("step not found: {step_id}"))?;
                step.status = StepStatus::Done;
                step.evidence = Some(evidence);
            }
            agentrail_core::recalc_lock_status(&mut loaded_plan);
            agentrail_plan_io::save_plan(&loaded_plan, &plan_path, Some(&hash))?;
            let step = find_step(&loaded_plan, &step_id)
                .ok_or_else(|| anyhow::anyhow!("step not found after complete: {step_id}"))?;
            println!(
                "{}",
                json!({
                    "operation": "complete",
                    "step": step
                })
            );
        }
        Commands::Dashboard { plan, out } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            let html = agentrail_dashboard::generate_dashboard(&loaded_plan);
            fs::write(&out, html)?;
            let summary = agentrail_dashboard::summarize(&loaded_plan);
            println!(
                "{}",
                json!({
                    "operation": "dashboard",
                    "project": agentrail_dashboard::ascii_safe(&loaded_plan.project),
                    "summary": {
                        "total": summary.total,
                        "running": summary.running,
                        "completed": summary.completed,
                        "failed": summary.failed,
                        "needs_attention": summary.needs_attention
                    }
                })
            );
        }
        Commands::Dag { plan } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            println!("{}", agentrail_dashboard::render_mermaid_dag(&loaded_plan));
        }
        Commands::Report { plan } => {
            let (loaded_plan, _) = agentrail_plan_io::load_plan(&plan)?;
            println!(
                "{}",
                agentrail_dashboard::render_markdown_report(&loaded_plan)
            );
        }
        Commands::Orchestrate { command } => match command {
            OrchestrateCommands::Plan { max_parallel } => {
                println!(
                    "{}",
                    json!({
                        "operation": "orchestrate_plan",
                        "max_parallel": max_parallel
                    })
                );
            }
            OrchestrateCommands::Tick {
                retry_count,
                retry_budget,
                reassigned_once,
            } => {
                let action = agentrail_orchestrator::decide_review_failure_action(
                    retry_count,
                    retry_budget,
                    reassigned_once,
                );
                println!(
                    "{}",
                    json!({
                        "operation": "orchestrate_tick",
                        "repair_action": action
                    })
                );
            }
            OrchestrateCommands::Resume { task_id } => {
                println!(
                    "{}",
                    json!({
                        "operation": "orchestrate_resume",
                        "task_id": task_id
                    })
                );
            }
        },
        Commands::Mcp { transport, bind } => {
            if transport == "stdio" {
                agentrail_mcp::run_stdio().await?;
            } else if transport == "http" {
                agentrail_mcp::run_http(&bind).await?;
            } else {
                anyhow::bail!("unsupported transport: {transport}");
            }
        }
        Commands::Serve { transport, bind } => {
            if transport == "stdio" {
                agentrail_mcp::run_stdio().await?;
            } else if transport == "http" {
                agentrail_mcp::run_http(&bind).await?;
            } else {
                anyhow::bail!("unsupported transport: {transport}");
            }
        }
    }

    Ok(())
}

fn find_step<'a>(plan: &'a Plan, step_id: &str) -> Option<&'a Step> {
    plan.phases
        .iter()
        .flat_map(|phase| phase.steps.iter())
        .find(|step| step.id == step_id)
}

fn find_step_with_phase<'a>(plan: &'a Plan, step_id: &str) -> Option<(&'a Phase, &'a Step)> {
    plan.phases.iter().find_map(|phase| {
        phase
            .steps
            .iter()
            .find(|step| step.id == step_id)
            .map(|step| (phase, step))
    })
}

fn find_step_mut<'a>(plan: &'a mut Plan, step_id: &str) -> Option<&'a mut Step> {
    for phase in &mut plan.phases {
        if let Some(step) = phase.steps.iter_mut().find(|step| step.id == step_id) {
            return Some(step);
        }
    }
    None
}

fn step_counts(plan: &Plan) -> (usize, usize, usize) {
    let mut pending = 0;
    let mut claimed = 0;
    let mut done = 0;

    for step in plan.phases.iter().flat_map(|phase| phase.steps.iter()) {
        match step.status {
            StepStatus::Pending => pending += 1,
            StepStatus::Claimed => claimed += 1,
            StepStatus::Done => done += 1,
            StepStatus::Skipped | StepStatus::Rejected => {}
        }
    }

    (pending, claimed, done)
}

fn next_ready_step(plan: &Plan) -> Option<&Step> {
    let step_status_by_id = step_status_map(plan);
    let phase_status_by_id = phase_status_map(plan);

    for phase in &plan.phases {
        if !phase_ready_for_work(phase, &phase_status_by_id) {
            continue;
        }
        if let Some(step) = phase.steps.iter().find(|step| {
            step.status == StepStatus::Pending && step_dependencies_ready(step, &step_status_by_id)
        }) {
            return Some(step);
        }
    }

    None
}

fn step_status_map(plan: &Plan) -> HashMap<String, StepStatus> {
    plan.phases
        .iter()
        .flat_map(|phase| phase.steps.iter())
        .map(|step| (step.id.clone(), step.status.clone()))
        .collect()
}

fn phase_status_map(plan: &Plan) -> HashMap<String, PhaseStatus> {
    plan.phases
        .iter()
        .map(|phase| (phase.id.clone(), phase.status.clone()))
        .collect()
}

fn step_dependencies_ready(step: &Step, step_status_by_id: &HashMap<String, StepStatus>) -> bool {
    step.depends_on.iter().all(|dep| {
        matches!(
            step_status_by_id.get(dep),
            Some(StepStatus::Done) | Some(StepStatus::Skipped)
        )
    })
}

fn phase_ready_for_work(phase: &Phase, phase_status_by_id: &HashMap<String, PhaseStatus>) -> bool {
    if phase.status == PhaseStatus::Locked {
        return false;
    }

    phase
        .depends_on
        .iter()
        .all(|dep| matches!(phase_status_by_id.get(dep), Some(PhaseStatus::Done)))
}
