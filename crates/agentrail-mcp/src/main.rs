use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "agentrail-mcp")]
struct Args {
    #[arg(long, default_value = "stdio")]
    transport: String,
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    match args.transport.as_str() {
        "stdio" => agentrail_mcp::run_stdio().await,
        "http" => agentrail_mcp::run_http(&args.bind).await,
        other => anyhow::bail!("unsupported transport: {other}"),
    }
}
