use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "agentrail-mcp")]
struct Args {
    #[arg(long, default_value = "stdio")]
    transport: String,
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: String,
    #[arg(long)]
    worker_cmd: Option<String>,
    #[arg(long, default_value = "agentrail/reload")]
    reload_method: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    match args.transport.as_str() {
        "stdio" => agentrail_mcp::run_stdio().await,
        "http" => agentrail_mcp::run_http(&args.bind).await,
        "supervisor-stdio" => {
            agentrail_mcp::run_supervisor_stdio(args.worker_cmd.as_deref(), &args.reload_method)
                .await
        }
        other => anyhow::bail!("unsupported transport: {other}"),
    }
}
