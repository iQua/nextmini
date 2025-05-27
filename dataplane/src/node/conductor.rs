/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use crate::node::config::LocalConfig;
use crate::node::controller::Controller;

pub struct Conductor {
    configs: LocalConfig,
    controller: Controller,
    shutdown_recv: mpsc::UnboundedReceiver,
}

impl Conductor {
    pub fn new(shutdown_recv: mpsc::UnboundedReceiver) -> Self {
        let configs = LocalConfig::new();
        let controller = Controller::new(configs.clone());

        Conductor {
            configs,
            controller,
            shutdown_recv,
        }
    }

    pub async fn run(&self) {
        tokio::select! {
            _ = async { self.start().await;} => {
                // At this point, the conductor actor has finished normally
            },
            _ = shutdown_recv.recv() => {
                // handles the shutdown signal by cleaning up all the actors
                conductor.shutdown().await;
            },
        }
    }
}
