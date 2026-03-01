use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json::json;

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
    Status,
    Dashboard,
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
        Commands::Status => {
            println!("status: bootstrap skeleton ready");
        }
        Commands::Dashboard => {
            println!("dashboard: TODO (static HTML generation)");
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
    }

    Ok(())
}
