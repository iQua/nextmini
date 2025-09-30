/// The main entry point for the Nextmini dataplane node.
///
/// Supports two deployment modes:
/// Single-node deployment (n_nodes = 1): Run a traditional single dataplane node with docker containerization.
/// Namespace deployment (n_nodes > 1): Spawn multiple isolated network namespaces with linux namespaces.
///
/// We use TaskTracker in Tokio (https://tokio.rs/tokio/topics/shutdown) to manage graceful
/// shutdowns, similar to fork/join data parallelism or a structured concurrency model.
///
/// Reference:
/// https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
mod node;
mod tests;

use std::error::Error;

use tokio::signal;
use tokio_util::task::task_tracker::TaskTracker;

use tracing::info;

use node::conductor::Conductor;
use node::config::LocalConfig;
use node::namespace::manager::NamespaceManager;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    // reads only n_nodes to determine deployment modes
    let n_nodes = LocalConfig::read_n_nodes();
    info!("Set n_nodes = {}.", n_nodes);

    // checks if we should run in namespace deployment
    if n_nodes > 1 {
        info!("Starting in namespace deployment with {} nodes.", n_nodes);
        // loads full config for namespace mode
        let config = LocalConfig::new();

        deploy_namespace(config);
    } else {
        info!("Starting in single node deployment.");
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime.");

        rt.block_on(deploy_single_node());
    }

    Ok(())
}

// Spawn multiple isolated network namespaces.
fn deploy_namespace(config: LocalConfig) {
    let mut manager = NamespaceManager::new(config);

    manager.spawn_all_nodes();
}

// Run a single dataplane node.
async fn deploy_single_node() {
    let tracker = TaskTracker::new();

    // spawns the Conductor task with the receiver
    tracker.spawn(async move {
        let config = LocalConfig::new();
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
