/// The main entry point for the Nextmini dataplane node, with support for:
///
/// Single-node deployment: runs a single dataplane node, typically within a Docker container.
/// Multiple-node deployment: deploys multiple dataplane nodes in isolated network namespaces.
mod node;

use std::error::Error;

use tokio::runtime;
use tokio::signal;
use tokio_util::task::task_tracker::TaskTracker;
use tracing::info;

use node::conductor::Conductor;
use node::config::LocalConfig;
#[cfg(target_os = "linux")]
use node::namespace::manager::NamespaceManager;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let config = LocalConfig::new();

    // checks if we should run in virtual network namespaces on the same machine
    if config.n_nodes > 1 {
        #[cfg(target_os = "linux")]
        {
            info!(
                "Started deploying {} dataplane nodes in isolated network namespaces.",
                config.n_nodes
            );
            deploy_multiple(config);
            return Ok(());
        }

        #[cfg(not(target_os = "linux"))]
        {
            return Err(
                "Running multiple dataplane nodes with network namespaces requires Linux.".into(),
            );
        }
    }

    info!("Started deploying a single dataplane node.");
    let rt = runtime::Runtime::new().expect("Failed to create the Tokio runtime.");

    rt.block_on(deploy(config));

    Ok(())
}

// Deploys multiple dataplane nodes, each in its isolated network namespace.
#[cfg(target_os = "linux")]
fn deploy_multiple(config: LocalConfig) {
    let mut manager = NamespaceManager::new(config);

    manager.spawn_all_nodes();
}

// Deploys a single dataplane node.
//
// We use TaskTracker in Tokio (https://tokio.rs/tokio/topics/shutdown) to manage graceful
// shutdowns, similar to fork/join data parallelism or a structured concurrency model.
//
// Reference:
// https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
async fn deploy(config: LocalConfig) {
    let tracker = TaskTracker::new();

    // spawns the Conductor task with the receiver
    tracker.spawn(async move {
        let conductor = Conductor::new(config).await;
        conductor.run().await;
    });

    tracker.close();

    tokio::select! {
        _ = tracker.wait() => {
            info!("Nextmini finished normally.");
        },
        _ = signal::ctrl_c() => {
            info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            tracker.wait().await;
        },
    }
}
