use nextmini::node::config::LocalConfig;
use nextmini::node::conductor::Conductor;

/// Dispatch helper used by `main` – currently supports only the TCP echo server.
pub async fn execute(config: LocalConfig) -> Result<(), Box<dyn std::error::Error>> {

    let conductor = Conductor::new_for_namespace(config).await;
    conductor.run().await;

    Ok(())
} 