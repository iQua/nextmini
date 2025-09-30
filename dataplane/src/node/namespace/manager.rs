use std::net::Ipv4Addr;

use nix::sched::*;
use nix::sys::signal::Signal;
use nix::unistd;
use rand::{rng, Rng};
use tokio::time::Duration;
use tracing::{error, info};

use crate::node::conductor::Conductor;
use crate::node::config::LocalConfig;

use crate::node::namespace::network::{delete_namespace, join_veth_to_ns, prepare_net, setup_veth_peer};

const STACK_SIZE: usize = 1024 * 1024;

/// Generate a random suffix for hostname uniqueness
fn random_suffix(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                             abcdefghijklmnopqrstuvwxyz\
                             0123456789";
    let mut rng = rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}