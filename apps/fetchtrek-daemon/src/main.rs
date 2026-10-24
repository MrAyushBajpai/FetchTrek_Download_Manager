use std::error::Error;

use fetchtrek_daemon::{DaemonConfig, DaemonEngine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = DaemonConfig::default();
    let engine = DaemonEngine::start(config).await?;

    // Wait for OS termination signal (Ctrl+C)
    let _ = tokio::signal::ctrl_c().await;

    // Graceful shutdown
    engine.shutdown().await?;

    Ok(())
}
