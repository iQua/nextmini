use crate::node::config::LocalConfig;
use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::processor::ProcessorHandle;
/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use tokio::sync::mpsc;
use tracing::info;

pub struct Conductor {
    config: LocalConfig,

    /// the local interface readers and writers
    local_interface: LocalInterfaceHandle,

    /// the processors
    processors: ProcessorHandle,

    /// the controller interface actor, which communicates with the controller
    controller_interface: ControllerInterfaceHandle,

    /// used by the main tokio task to shutdown the conductor
    main_shutdown_recv: Option<mpsc::UnboundedReceiver<()>>,
}

impl Conductor {
    pub async fn new(main_shutdown_recv: mpsc::UnboundedReceiver<()>) -> Self {
        let config = LocalConfig::new();

        // starts the processor actor with the shutdown channel
        let processors = ProcessorHandle::new(config.clone());

        // starts the local interface, providing it with the shutdown channel so that it can
        // signal the conductor to shut down when needed
        let local_interface = LocalInterfaceHandle::new(config.clone(), processors.clone());

        // connects processor with its downstream local interface writers to send packets out
        processors.connect_local_interface(local_interface.clone());

        // starts the controller interface actor, providing it with the shutdown channel
        let controller_interface = ControllerInterfaceHandle::new(config.clone(), processors.clone()).await;

        Conductor {
            config,
            local_interface,
            processors,
            controller_interface,
            main_shutdown_recv: Some(main_shutdown_recv),
        }
    }

    pub async fn run(&mut self) {
        let mut main_shutdown_recv = self.main_shutdown_recv.take().unwrap();

        tokio::select! {
            _ = async { self.start().await; } => {
                // At this point, the conductor actor has finished normally
            },
            _ = main_shutdown_recv.recv() => {
                // handles the shutdown signal from the main tokio task
                self.shutdown().await;
            },
        }
    }

    pub async fn start(&self) {
        info!("Nextmini is starting...");
        // Should start protocol server here.
        // Protocol server needs processor handle to add new remote node connection
    }

    pub async fn shutdown(&self) {
        // Here we would clean up all the actors and resources
        // For example, we could send a shutdown signal to the controller interface,
        // processor, and local interface actors.
        // This is a placeholder for the actual shutdown logic.
        info!("Nextmini is shutting down...");

        self.local_interface.shutdown().await;
    }
}
