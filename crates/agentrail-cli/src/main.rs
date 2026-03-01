use anyhow::Result;
use clap::{Parser, Subcommand};

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
    Mcp {
        #[arg(long, default_value = "stdio")]
        transport: String,
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: String,
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
