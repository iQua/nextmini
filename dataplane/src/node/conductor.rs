/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use tokio::sync::mpsc;

use crate::node::config::LocalConfig;
use crate::node::controller::ControllerHandle;

pub struct Conductor {
    configs: LocalConfig,
    controller_handle: ControllerHandle,

    // used by actors local to the conductor to shutdown the conductor
    shutdown_recv: mpsc::UnboundedReceiver<()>,

    // used by the main tokio task to shutdown the conductor
    main_shutdown_recv: mpsc::UnboundedReceiver<()>,
}

impl Conductor {
    pub fn new(main_shutdown_recv: mpsc::UnboundedReceiver) -> Self {
        let configs = LocalConfig::new();
        let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();

        let controller_handle = ControllerHandle::new(configs.clone(), shutdown_send.clone());

        Conductor {
            configs,
            controller_handle,
            main_shutdown_recv,
            shutdown_recv,
        }
    }

    pub async fn run(&self) {
        tokio::select! {
            _ = async { self.start().await;} => {
                // At this point, the conductor actor has finished normally
            },
            _ = self.shutdown_recv.recv() => {
                // handles the local shutdown signal by cleaning up all the actors
                conductor.shutdown().await;
            },
            _ = self.main_shutdown_recv.recv() => {
                // handles the shutdown signal from the main tokio task
                self.shutdown().await;
            },
        }
    }
}
