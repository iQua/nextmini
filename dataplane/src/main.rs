mod dataplane;
mod tests;

use tracing::info;
use tracing_subscriber;

use dataplane::configs;
use dataplane::controller_interface::Controller;
use dataplane::protocols_server::start_protocols_server;

fn main() {
    tracing_subscriber::fmt::init();
    let configs = configs::new();

    // builds a multi-threaded Tokio runtime
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    if configs.rt_event_interval > 0 {
        rt_builder.event_interval(configs.rt_event_interval);
    }

    if configs.rt_n_worker_threads > 0 {
        rt_builder.worker_threads(configs.rt_n_worker_threads);
    }

    loop {
        let rt = rt_builder.build().unwrap();

        // spawns the main Tokio task
        rt.block_on(async {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

            if configs.enable_tokio_console {
                console_subscriber::init();
            }

            // starts a controller interface and connnect to the controller
            let mut controller = Controller::connect(configs.clone(), shutdown_tx).await;

            // starts the protocol servers to accept inter-node connections
            start_protocols_server(
                controller.get_session_id(),
                controller.get_protocol(),
                configs.clone(),
                controller.get_context(),
                controller.get_processor_manager(),
            )
            .await;

            // spawns a metrics collector
            let mut metrics_collector = controller.take_metrics_collector();
            tokio::spawn(async move {
                metrics_collector.run().await;
            });

            // splits the controller into sender and receiver ends and spawn them in independent tasks
            let (mut controller_sender, mut controller_receiver) = controller.split().await;

            tokio::spawn(async move {
                controller_receiver.run().await;
            });

            tokio::spawn(async move { controller_sender.run().await });

            info!("Nextmini is now running.");

            shutdown_rx.changed().await.unwrap();
        });

        if configs.restart_on_disconnect {
            info!("Restarting Nextmini...");
            continue;
        }

        info!("Shutting down Nextmini...");
        break;
    }
}
