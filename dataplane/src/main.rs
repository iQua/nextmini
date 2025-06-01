/// The main entry point for the Nextmini dataplane node.

/// We use TaskTracker in Tokio (https://tokio.rs/tokio/topics/shutdown) to manage graceful
/// shutdowns, similar to fork/join data parallelism or a structured concurrency model.

/// Reference:
/// https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
mod node;
mod tests;

use std::error::Error;

use tokio::signal;
use tokio::sync::mpsc;
use tokio_util::task::task_tracker::TaskTracker;

use tracing::info;

use node::conductor::Conductor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    // A channel for the main tokio task to signal a shutdown signal to the Conductor actor.
    let (shutdown_sender, shutdown_receiver) = mpsc::unbounded_channel();

    // creates a TaskTracker to manage graceful shutdowns
    let tracker = TaskTracker::new();

    // Spawn the Conductor task with the receiver
    tracker.spawn(async move {
        let mut conductor = Conductor::new(shutdown_receiver).await;
        conductor.run().await;
    });

    tracker.close();

    tokio::select! {
        _ = tracker.wait() => {
            info!("Nextmini finished normally.");
        },
        _ = signal::ctrl_c() => {
            info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            shutdown_sender.send(()).expect("Failed to send shutdown signal to the conductor.");
            tracker.wait().await;
        },
    }

    Ok(())
}
