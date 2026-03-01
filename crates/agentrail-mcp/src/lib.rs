use anyhow::Result;
use tracing::info;

pub async fn run_stdio() -> Result<()> {
    info!("agentrail MCP stdio server bootstrap");
    Ok(())
}

pub async fn run_http(bind: &str) -> Result<()> {
    info!(bind = bind, "agentrail MCP HTTP server bootstrap");
    Ok(())
}
