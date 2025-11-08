use std::net::Ipv4Addr;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::thread;
use std::time;

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::sched::*;
use nix::sys::signal::Signal;
use nix::unistd;
use rand::{Rng, rng};
use tokio::runtime;
use tracing::{error, info};

use crate::node::conductor::Conductor;
use crate::node::config::LocalConfig;
use crate::node::namespace::network::{
    add_default_route, bring_up_master_veth, delete_namespace, ensure_forward_rules,
    ensure_ip_forward_enabled, ensure_nat_masquerade, join_veth_to_ns, prepare_net,
    setup_veth_peer, wait_for_veth_carrier,
};

const STACK_SIZE: usize = 1024 * 1024;

/// Generates a random suffix for hostname uniqueness.
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

pub struct NamespaceManager {
    config: LocalConfig,
}

impl NamespaceManager {
    pub fn new(config: LocalConfig) -> Self {
        Self { config }
    }

    // Spawns all namespaces and waits for shutdown.
    pub fn spawn_all_nodes(&mut self) {
        let rt = runtime::Runtime::new().expect("Failed to create the Tokio runtime.");

        // The parent process no longer transforms the controller address.
        // It passes the original address from the config directly to the child,
        // which will then be responsible for resolving it to the gateway if needed.
        let controller_addr = format!("ws://{}", self.config.controller_addr);
        info!(
            "The controller address has been set to {}.",
            controller_addr
        );

        // sets up host forwarding/NAT if configured
        if self.config.auto_enable_ip_forward
            && let Err(e) = ensure_ip_forward_enabled()
        {
            error!("Failed to enable ip_forward: {}", e);
        }
        if self.config.auto_add_forward_rules || self.config.auto_add_nat {
            // detects outbound interface
            let out_if = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg("ip route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if($i==\"dev\") {print $(i+1); exit}}'")
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .unwrap_or_else(|_| "ens3".to_string());

            if self.config.auto_add_forward_rules
                && let Err(e) = ensure_forward_rules(&self.config.bridge_name, &out_if)
            {
                error!("Failed to add FORWARD rules: {}.", e);
            }
            if self.config.auto_add_nat
                && let Err(e) = ensure_nat_masquerade("172.16.0.0/16", &out_if)
            {
                error!("Failed to add MASQUERADE: {}.", e);
            }
        }

        // computes the namespace IP addresses
        let ns_ips = self.compute_namespace_ips();

        // keeps child stacks alive while children run
        let mut stacks: Vec<Box<[u8; STACK_SIZE]>> = Vec::new();
        let mut bridge_idx: Option<u32> = None;

        // spawns each namespace
        for (idx, ns_ip) in ns_ips.iter().enumerate() {
            let veth_idx;
            let veth2_idx;

            // prepares bridge + a fresh veth pair (bridge creation is idempotent)
            match rt.block_on(prepare_net(
                self.config.bridge_name.clone(),
                &self.config.bridge_ip,
                self.config.subnet,
                idx,
            )) {
                Ok((bridge_idx_val, veth_index, veth2_index)) => {
                    bridge_idx = Some(bridge_idx_val);
                    veth_idx = veth_index;
                    veth2_idx = veth2_index;
                }
                Err(e) => {
                    error!("Failed to prepare network: {}. Retrying...", e);
                    continue;
                }
            }

            // prepares child process
            let subnet = self.config.subnet;
            let bridge_ip = self.config.bridge_ip.clone();
            let controller_addr_clone = controller_addr.clone();
            let config_path = self.config.config_path.clone();
            let node_id_offset = self.config.node_id_offset;

            // create a pipe for handshake (child signals network ready)
            let (read_fd, write_fd) = nix::unistd::pipe().expect("pipe failed");
            // set read end non-blocking so we can implement timeout polling
            let _ = fcntl(&read_fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK));

            let raw_write_fd = write_fd.into_raw_fd();
            let cb = Box::new(move || {
                child_process(ChildProcessArgs {
                    ns_ip: ns_ip.clone(),
                    veth_peer_idx: veth2_idx,
                    controller_addr: controller_addr_clone.clone(),
                    node_index: idx + node_id_offset,
                    subnet,
                    config_path: config_path.clone(),
                    handshake_fd: Some(unsafe { OwnedFd::from_raw_fd(raw_write_fd) }),
                    bridge_ip: bridge_ip.clone(),
                })
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

            // parent already transferred ownership of write end to child via raw fd

            // keeps stack memory alive
            stacks.push(tmp_stack);

            // moves veth peer into child's netns
            if let Err(e) =
                rt.block_on(async { join_veth_to_ns(veth2_idx, child_pid.as_raw() as u32).await })
            {
                error!("Failed to join veth to namespace: {}. Retrying...", e);
                continue;
            }

            // gives the child process time to start and configure its peer interface
            // adds an initial, configurable small sleep to let child process start
            thread::sleep(time::Duration::from_millis(
                self.config.child_start_delay_ms,
            ));

            // waits for child handshake that peer interface is configured before bringing up master
            let handshake_deadline = time::Instant::now()
                + time::Duration::from_millis(self.config.handshake_timeout_ms);
            let mut handshake_ok = false;

            while time::Instant::now() < handshake_deadline {
                let mut buf = [0u8; 1];

                match nix::unistd::read(&read_fd, &mut buf) {
                    Ok(1) => {
                        handshake_ok = true;
                        break;
                    }
                    Ok(0) => {
                        // pipe closed unexpectedly
                        break;
                    }
                    Ok(2..) => break,
                    Err(nix::errno::Errno::EAGAIN) => {
                        thread::sleep(time::Duration::from_millis(10));
                        continue;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            if !handshake_ok {
                error!(
                    "Handshake timeout waiting for child {} network setup. \
                    Consider increasing handshake_timeout_ms in the local configuration.",
                    idx
                );
                drop(read_fd); // timeout cleanup
                continue;
            }

            drop(read_fd); // success cleanup

            // now brings up the master veth
            if let Err(e) = rt.block_on(async { bring_up_master_veth(veth_idx).await }) {
                error!("Failed to bring up master veth: {}. Retrying...", e);
                continue;
            }

            // waits and verifies that the veth pair link is established (has carrier)
            // uses configurable retry parameters
            let max_retries =
                (self.config.carrier_max_wait_ms / self.config.carrier_poll_interval_ms) as u32;
            let wait_result = rt.block_on(async {
                wait_for_veth_carrier(veth_idx, max_retries, self.config.carrier_poll_interval_ms)
                    .await
            });

            if let Err(e) = wait_result {
                error!(
                    "Veth pair {} (ifindex {}) failed to establish carrier: {}. Giving up after {} ms.",
                    idx, veth_idx, e, self.config.carrier_max_wait_ms
                );
                continue;
            }

            // sleeps between node creation to prevent overwhelming the system
            thread::sleep(time::Duration::from_millis(
                self.config.interval_between_spawn,
            ));
        }

        // waits for shutdown signal
        rt.block_on(self.wait_for_shutdown(bridge_idx));
    }

    // Computes the namespace IP addresses.
    fn compute_namespace_ips(&self) -> Vec<String> {
        let base: u32 = self
            .config
            .bridge_ip
            .parse::<Ipv4Addr>()
            .expect("Invalid bridge IP")
            .into();

        // IP addresses: bridge_ip + 3 to bridge_ip + n_nodes + 2.
        // offsets: 1 for controller, 1 for database, the rest for nodes.
        (3..=self.config.n_nodes + 2)
            .map(|offset| Ipv4Addr::from(base + offset as u32).to_string())
            .collect()
    }

    // Waits for the Ctrl + C shutdown signal.
    async fn wait_for_shutdown(&self, bridge_idx: Option<u32>) {
        match tokio::signal::ctrl_c().await {
            Ok(_) => {
                info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            }
            Err(e) => {
                error!("Failed to listen for Ctrl+C: {}", e);
            }
        }

        // cleans up the bridge
        if let Some(bridge_idx) = bridge_idx
            && let Err(e) = delete_namespace(bridge_idx).await
        {
            error!("Failed to delete namespace: {}", e);
        }
    }
}

/// Parameters required to spawn the child process inside a namespace.
struct ChildProcessArgs {
    ns_ip: String,
    veth_peer_idx: u32,
    controller_addr: String,
    node_index: usize,
    subnet: u8,
    config_path: String,
    handshake_fd: Option<OwnedFd>,
    bridge_ip: String,
}

/// The child process that runs in its own isolated network namespace.
fn child_process(args: ChildProcessArgs) -> isize {
    let ChildProcessArgs {
        ns_ip,
        veth_peer_idx,
        controller_addr,
        node_index,
        subnet,
        config_path,
        mut handshake_fd,
        bridge_ip,
    } = args;
    info!("Child process started with index {}.", node_index);

    // sets hostname for this namespace
    let ns_hostname = format!("nextmini-{}", random_suffix(5));
    unistd::sethostname(&ns_hostname).expect("Failed to set hostname.");

    // creates and runs the Tokio runtime
    let rt = runtime::Runtime::new().expect("Failed to create the Tokio runtime.");

    let process = rt.block_on(async {
        // sets up veth interface (this brings up the peer side)
        setup_veth_peer(veth_peer_idx, &ns_ip, subnet).await?;

        // signal parent that peer interface is configured
        if let Some(fd) = handshake_fd.take() {
            let _ = nix::unistd::write(&fd, &[1u8]);
            drop(fd);
        }

        // The child process is now responsible for resolving the controller address.
        // If the address is localhost, it's replaced by the bridge IP (gateway)
        // to ensure connectivity from within the isolated namespace.
        let mut resolved_controller_addr = controller_addr;
        if resolved_controller_addr.contains("127.0.0.1") {
            info!(
                "Controller address '{}' is localhost, replacing with gateway IP '{}'.",
                resolved_controller_addr, bridge_ip
            );
            resolved_controller_addr = resolved_controller_addr.replace("127.0.0.1", &bridge_ip);
            info!("New controller address: {}.", resolved_controller_addr);
        }

        // loads config using new_for_namespace (handles all namespace-specific settings)
        let mut config = LocalConfig::new_for_namespace(&config_path, &ns_ip, node_index);
        config.controller_addr = resolved_controller_addr; // Set the resolved address

        // adds a default route via the host bridge inside this namespace to enable outbound traffic
        if let Err(e) = add_default_route(veth_peer_idx, &bridge_ip).await {
            error!(
                "Failed to add default route in namespace idx {} via {}: {}.",
                node_index, bridge_ip, e
            );
        }

        // starts the conductor
        let conductor = Conductor::new(config, None).await;
        conductor.run().await;

        Ok::<(), Box<dyn std::error::Error>>(())
    });

    if let Err(e) = process {
        error!("Child process error: {}.", e);
        return -1;
    }

    info!("Child process finished.");
    0
}
