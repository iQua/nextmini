/// The conductor actor is a 'mastermind' who is reponsible for overseeing the entire operation of
/// the dataplane node, including the connection with the controller actor, all processor actors,
/// the local reader and writer actors, and the metrics collector actor.
use crate::node::config::LocalConfig;
use crate::node::controller::Controller;

pub struct Conductor {
    configs: LocalConfig,
    controller: Controller,
}
