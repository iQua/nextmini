mod config;
mod net;
mod string_helpers;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;

use crate::config::Config;
use crate::net::{delete_namespace, join_veth_to_ns, prepare_net, setup_veth_peer};
use tracing::{error, info};
use nix::sched::*;
use nix::sys::signal::Signal;
use std::{net::Ipv4Addr, thread, time};

const STACK_SIZE: usize = 1024 * 1024;

fn main() {
    tracing_subscriber::fmt::fmt() 
        .init();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // load node and net configs
    let cfg = Config::new();

    // Set the controller address to the bridge IP if it's running on the host
    let controller_addr: String = match cfg.controller_addr.as_str() {
        "127.0.0.1:3000" => format!("ws://{}:3000", cfg.bridge_ip),
        _ => format!("ws://{}", cfg.controller_addr),
    };
    info!("Controller address set to {}", controller_addr);

    // Pre-compute namespace IPs (inlined)
    let ns_ips: Vec<String> = {
        let base: u32 = cfg.bridge_ip.parse::<Ipv4Addr>().unwrap().into();
        (3..=cfg.n_nodes + 2)
            .map(|offset| Ipv4Addr::from(base + offset).to_string())
            .collect()
    };

    // Keep child pids and stacks alive while children run
    let mut stacks: Vec<Box<[u8; STACK_SIZE]>> = Vec::new();
    let mut bid = 37;

    for ns_ip in ns_ips {
        // Prepare bridge + a fresh veth pair (bridge creation is idempotent)
        let (bridge_idx, _veth_idx, veth2_idx) = rt
            .block_on(prepare_net(
                cfg.bridge_name.clone(),
                &cfg.bridge_ip,
                cfg.subnet,
            ))
            .expect("Failed to prepare network");

        bid = bridge_idx;

        // prepare child process
        let cb = Box::new(|| {
            c_process(
                ns_ip.clone(),
                cfg.subnet,
                veth2_idx,
                controller_addr.clone(),
            )
        });

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
    }

    // keeps the main thread alive.
    rt.block_on(async {
        match tokio::signal::ctrl_c().await {
            Ok(_) => {
                info!("Ctrl+C received, shutting down...");
            }
            Err(e) => {
                error!("Failed to listen for Ctrl+C: {}", e);
            }
        }
    });

    // cleans up the namespaces
    rt.block_on(async {
        if let Err(e) = delete_namespace(bid).await {
            error!("{}", e);
        }
    });
}

// the child process to be executed within main
fn c_process(
    ns_ip: String,
    subnet: u8,
    veth_peer_idx: u32,
    controller_addr: String,
) -> isize {
    info!("Child process (PID: {}) started", nix::unistd::getpid());
    // Set the hostname of the new process
    let ns_hostname = format!("isoserver-{}", string_helpers::random_suffix(5));
    nix::unistd::sethostname(ns_hostname).expect("Failed to set hostname");

    // Spawn a new blocking task on the current runtime
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let process = rt.block_on(async {
        setup_veth_peer(veth_peer_idx, &ns_ip, subnet).await?;
        execute(&controller_addr, ns_ip).await
    });

    if let Err(e) = process {
        error!("Error: {}", e);
        return -1;
    }

    info!("Child process finished??");
    0
}

pub async fn execute(controller_addr: &str, ns_ip: String) -> Result<(), Box<dyn std::error::Error>> {
    // read config file
    let config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/config.toml");
    let config = LocalConfig::new_for_namespace(config_path, &controller_addr, &ns_ip);

    // start the conductor
    let conductor = Conductor::new_for_namespace(config).await;
    conductor.run().await;

    Ok(())
}