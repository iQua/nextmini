/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use tokio::sync::mpsc;

use crate::node::config::LocalConfig;
use crate::node::controller_interface::ControllerInterfaceHandle;
use crate::node::local_interface::LocalInterfaceHandle;
use crate::node::processor::ProcessorHandle;

pub struct Conductor {
    config: LocalConfig,

    // used by actors local to the conductor to shutdown the conductor
    shutdown_send: mpsc::UnboundedSender<()>,
    shutdown_recv: Option<mpsc::UnboundedReceiver<()>>,

    // used by the main tokio task to shutdown the conductor
    main_shutdown_recv: Option<mpsc::UnboundedReceiver<()>>,
}

impl Conductor {
    pub fn new(main_shutdown_recv: mpsc::UnboundedReceiver<()>) -> Self {
        let config = LocalConfig::new();
        let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

        Conductor {
            config,
            shutdown_send,
            shutdown_recv: Some(shutdown_recv),
            main_shutdown_recv: Some(main_shutdown_recv),
        }
    }

    pub async fn run(&mut self) {
        let mut shutdown_recv = self.shutdown_recv.take().unwrap();
        let mut main_shutdown_recv = self.main_shutdown_recv.take().unwrap();

        tokio::select! {
            _ = async { self.start().await; } => {
                // At this point, the conductor actor has finished normally
            },
            _ = shutdown_recv.recv() => {
                // handles the local shutdown signal by cleaning up all the actors
                self.shutdown().await;
            },
            _ = main_shutdown_recv.recv() => {
                // handles the shutdown signal from the main tokio task
                self.shutdown().await;
            },
        }
    }

    pub async fn start(&self) {
        // starts the local interface, providing it with the shutdown channel so that it can
        // signal the conductor to shut down when needed
        let local_interface = LocalInterfaceHandle::new(self.config.clone());

        // starts the processor actor, providing them with the local interface and the shutdown
        // channel
        let processor = ProcessorHandle::new(
            self.config.clone(),
            local_interface,
            self.shutdown_send.clone(),
        );

        // starts the controller interface actor, providing it with the shutdown channel
        let controller_interface = ControllerInterfaceHandle::new(
            self.config.clone(),
            processor,
            self.shutdown_send.clone(),
        );

        // Should start protocol server here.
        // Protocol server needs processor handle to add new remote node connection
    }

    pub async fn shutdown(&self) {
        // Here we would clean up all the actors and resources
        // For example, we could send a shutdown signal to the controller interface,
        // processor, and local interface actors.
        // This is a placeholder for the actual shutdown logic.
        println!("Conductor is shutting down...");
    }
}
