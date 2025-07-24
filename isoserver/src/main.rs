mod config;
mod handler;
mod net;
mod string_helpers;

use crate::config::Config;
use crate::handler::execute;
use crate::net::{join_veth_to_ns, prepare_net, setup_veth_peer};
use log::{error, info, warn};
use nextmini::node::config::LocalConfig;
use nix::sched::*;
use nix::sys::signal::Signal;
use nix::sys::wait::{waitpid, WaitStatus};
use std::{thread, time};

const STACK_SIZE: usize = 1024 * 1024;

// BUGS/OPTIMIZATIONS:
// The bug is revolving how to shutdown the child processes gracefully and clean up the resources
// 1. Now the bridge name and ip are not dropped, and they exist even after the program exits
// 2. The child processes monitor logic can be optimized
// (tokio block_on and tokio main conflict, main should not be async)

fn main() {
    env_logger::init();

    // load the config for the namespace nodes
    let cfg = Config::new();

    let node_config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/config.toml");
    let mut node_cfg = LocalConfig::new_for_namespace(node_config_path);

    // Set the controller address to the bridge IP if it's running on the host
    if cfg.controller_addr == "127.0.0.1:3000" {
        node_cfg.controller_addr = format!("ws://{}:3000", cfg.bridge_ip);
    } else {
        node_cfg.controller_addr = format!("ws://{}", cfg.controller_addr);
    }
    info!("Controller address set to {}", node_cfg.controller_addr);

    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // Pre-compute namespace IPs
    let ns_ips =
        generate_ns_ips(&cfg.bridge_ip, cfg.n_nodes).expect("Failed to generate namespace IPs");

    // Keep child pids and stacks alive while children run
    let mut child_pids = Vec::new();
    let mut stacks: Vec<Box<[u8; STACK_SIZE]>> = Vec::new();

    for (i, ns_ip) in ns_ips.iter().enumerate() {
        // Prepare bridge + a fresh veth pair (bridge creation is idempotent)
        let (_, _, veth2_idx) = rt
            .block_on(prepare_net(
                cfg.bridge_name.clone(),
                node_cfg.private_network_interface.clone(),
                &cfg.bridge_ip,
                cfg.subnet,
            ))
            .expect("Failed to prepare network");

        // set the node id
        node_cfg.node_id = i;

        // prepare child process
        let cb = Box::new(|| c_process(node_cfg.clone(), ns_ip.clone(), cfg.subnet, veth2_idx));

        let mut tmp_stack: Box<[u8; STACK_SIZE]> = Box::new([0; STACK_SIZE]);
        let child_pid = unsafe {
            clone(
                cb,
                tmp_stack.as_mut(),
                CloneFlags::CLONE_NEWNET | CloneFlags::CLONE_NEWUTS,
                Some(Signal::SIGCHLD as i32),
            )
        }
        .expect("Clone failed");

        // Keep stack memory alive
        stacks.push(tmp_stack);

        info!("Spawned child pid: {}", child_pid);

        // Move veth peer into child's netns
        rt.block_on(async {
            join_veth_to_ns(veth2_idx, child_pid.as_raw() as u32)
                .await
                .expect("Failed to join veth to namespace");
        });

        thread::sleep(time::Duration::from_millis(200));

        child_pids.push(child_pid);
    }

    // Wait for every child
    for pid in child_pids {
        match waitpid(pid, None) {
            Ok(WaitStatus::Exited(cpid, status)) => {
                warn!(
                    "Child process (PID: {}) exited with status: {}",
                    cpid, status
                );
            }
            Ok(WaitStatus::Signaled(cpid, signal, _)) => {
                warn!(
                    "Child process (PID: {}) was killed by signal: {:?}",
                    cpid, signal
                );
            }
            Err(e) => error!("waitpid failed: {}", e),
            _ => error!("Error: Unexpected waitpid result"),
        }
    }
}

fn c_process(node_cfg: LocalConfig, ns_ip: String, subnet: u8, veth_peer_idx: u32) -> isize {
    info!("Child process (PID: {}) started", nix::unistd::getpid());
    // Set the hostname of the new process
    let ns_hostname = format!("isoserver-{}", string_helpers::random_suffix(5));
    nix::unistd::sethostname(ns_hostname).expect("Failed to set hostname");

    // Spawn a new blocking task on the current runtime
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let process = rt.block_on(async {
        setup_veth_peer(veth_peer_idx, &ns_ip, subnet).await?;
        execute(node_cfg).await
    });

    if let Err(e) = process {
        error!("Error: {}", e);
        return -1;
    }

    info!("Child process finished??");
    0
}

fn generate_ns_ips(base_ip: &str, n: u32) -> Result<Vec<String>, std::net::AddrParseError> {
    use std::net::Ipv4Addr;
    let base: Ipv4Addr = base_ip.parse()?;
    let base_u32: u32 = base.into();
    Ok((1..=n)
        .map(|offset| Ipv4Addr::from(base_u32 + offset + 2).to_string())
        .collect())
}
