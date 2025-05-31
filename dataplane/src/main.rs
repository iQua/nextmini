/// The main entry point for the Nextmini dataplane node.
/// We use the TaskTracker in Tokio (https://tokio.rs/tokio/topics/shutdown) to manage graceful
/// shutdowns, similar to fork/join data parallelism, or a structured concurrency model.

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

    let conductor = Conductor::new(shutdown_receiver);

    // Spawn the Conductor task with the receiver
    tracker.spawn(conductor.run());
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

// let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

// // starts a controller interface and connnect to the controller
// let mut controller = Controller::new(configs.clone(), shutdown_tx).await;

// // starts the protocol servers to accept inter-node connections
// start_protocols_server(
//     controller.get_protocol(),
//     configs.clone(),
//     controller.get_context(),
//     controller.get_processor_manager(),
// )
// .await;

// // spawns a metrics collector
// let mut metrics_collector = controller.take_metrics_collector();
// tokio::spawn(async move {
//     metrics_collector.run().await;
// });

// // splits the controller into sender and receiver ends and spawn them in independent tasks
// let (mut controller_sender, mut controller_receiver) = controller.split().await;

// tokio::spawn(async move {
//     controller_receiver.run().await;
// });

// tokio::spawn(async move { controller_sender.run().await });

// shutdown_rx.changed().await.unwrap();
