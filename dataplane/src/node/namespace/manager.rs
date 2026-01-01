use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr::NonNull;
use std::time;

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::sched::*;
use nix::sys::mman::{MapFlags, ProtFlags, mmap_anonymous, munmap};
use nix::sys::signal::{Signal, kill};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd;
use nix::unistd::Pid;
use rand::{Rng, rng};
use tokio::runtime;
use tracing::{error, info, warn};
use url::Url;

use crate::node::conductor::Conductor;
use crate::node::config::LocalConfig;
use crate::node::namespace::network::{
    add_default_route, bring_up_master_veth, delete_link_by_name, delete_namespace,
    ensure_forward_rules, ensure_ip_forward_enabled, ensure_nat_masquerade, join_veth_to_ns,
    prepare_net, setup_veth_peer, wait_for_veth_carrier,
};

const STACK_SIZE: usize = 1024 * 1024;

// Linux bridge ports are capped (often 1024). Shard namespace nodes across multiple bridges to allow
// large experiments (e.g., 10,000 nodes).
const BRIDGE_SHARD_SUBNET: u8 = 22; // 1024 addresses per bridge subnet
const BRIDGE_NODE_OFFSET: u32 = 2; // reserve: network (.0), gateway (.1)

fn bridge_name_for_shard(base: &str, shard: usize) -> String {
    if shard == 0 {
        return base.to_string();
    }

    let prefix = base.trim_end_matches(|c: char| c.is_ascii_digit());
    if prefix.is_empty() || prefix == base {
        format!("{base}{shard}")
    } else {
        format!("{prefix}{shard}")
    }
}

fn is_bridge_full_error(err: &crate::node::namespace::network::NetworkError) -> bool {
    match err {
        crate::node::namespace::network::NetworkError::OperationError(msg) => {
            msg.contains("Exchange full") || msg.contains("os error 54")
        }
        crate::node::namespace::network::NetworkError::ConnectionError(e) => {
            e.to_string().contains("Exchange full") || e.to_string().contains("os error 54")
        }
        _ => false,
    }
}

struct MmapStack {
    ptr: NonNull<std::ffi::c_void>,
    len: usize,
}

impl MmapStack {
    fn new(len: usize) -> nix::Result<Self> {
        let length = NonZeroUsize::new(len).ok_or_else(|| nix::errno::Errno::EINVAL)?;
        let ptr = unsafe {
            mmap_anonymous(
                None,
                length,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE,
            )?
        };

        Ok(Self { ptr, len })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr() as *mut u8, self.len) }
    }
}

impl Drop for MmapStack {
    fn drop(&mut self) {
        let _ = unsafe { munmap(self.ptr, self.len) };
    }
}

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

    fn namespace_subnet_cidr(&self) -> Option<String> {
        let Ok(bridge_ip) = self.config.bridge_ip.parse::<Ipv4Addr>() else {
            error!(
                "Invalid bridge_ip '{}' for namespace configuration.",
                self.config.bridge_ip
            );
            return None;
        };

        if self.config.subnet > 32 {
            error!(
                "Invalid subnet '{}' for namespace configuration (must be <= 32).",
                self.config.subnet
            );
            return None;
        }

        let ip_u32 = u32::from(bridge_ip);
        let mask = if self.config.subnet == 0 {
            0
        } else {
            (!0u32) << (32 - self.config.subnet)
        };
        let network = Ipv4Addr::from(ip_u32 & mask);

        Some(format!("{}/{}", network, self.config.subnet))
    }

    // Spawns all namespaces and waits for shutdown.
    pub fn spawn_all_nodes(&mut self) {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create the Tokio runtime.");

        // The parent process passes the controller address directly to the child, which will then
        // be responsible for resolving loopback hosts (127.0.0.1 / localhost / ::1) to the gateway
        // when running inside an isolated namespace.
        let controller_addr = self.config.controller_addr.clone();
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
                && let Some(cidr) = self.namespace_subnet_cidr()
                && let Err(e) = ensure_nat_masquerade(&cidr, &out_if)
            {
                error!("Failed to add MASQUERADE for {}: {}.", cidr, e);
            }
        }

        let Ok(base_gateway_ip) = self.config.bridge_ip.parse::<Ipv4Addr>() else {
            error!(
                "Invalid bridge_ip '{}' for namespace configuration.",
                self.config.bridge_ip
            );
            return;
        };

        let subnet_mask = if BRIDGE_SHARD_SUBNET == 0 {
            0
        } else {
            (!0u32) << (32 - BRIDGE_SHARD_SUBNET)
        };
        let gateway_u32 = u32::from(base_gateway_ip);
        let shard_network_base = gateway_u32 & subnet_mask;
        let shard_size: u32 = if BRIDGE_SHARD_SUBNET == 32 {
            1
        } else {
            1u32 << (32 - BRIDGE_SHARD_SUBNET)
        };
        let gateway_offset = gateway_u32.saturating_sub(shard_network_base);

        if gateway_offset == 0 || gateway_offset >= shard_size.saturating_sub(1) {
            error!(
                "bridge_ip '{}' is invalid for /{} sharded namespace mode.",
                self.config.bridge_ip, BRIDGE_SHARD_SUBNET
            );
            return;
        }

        // Allow for the worst case where we skip over the gateway offset when assigning node IPs.
        let max_nodes_per_shard = shard_size
            .saturating_sub(BRIDGE_NODE_OFFSET)
            .saturating_sub(2) as usize;
        if max_nodes_per_shard == 0 {
            error!(
                "bridge subnet /{} is too small for namespace nodes.",
                BRIDGE_SHARD_SUBNET
            );
            return;
        }

        let mut child_pids = Vec::with_capacity(self.config.n_nodes);
        let mut bridge_indices: Vec<u32> = Vec::new();

        let mut shard: usize = 0;
        let mut shard_node_idx: usize = 0;
        let mut spawn_failed = false;

        // spawns each namespace
        'spawn: for idx in 0..self.config.n_nodes {
            let veth_name = format!("veth{}a", idx);

            // prepares bridge shard + a fresh veth pair (bridge creation is idempotent)
            let mut attempts: u32 = 0;
            let mut prepared: Option<(String, String, u32, u32, u32)> = None;
            loop {
                if shard_node_idx >= max_nodes_per_shard {
                    shard += 1;
                    shard_node_idx = 0;
                }

                let shard_base = shard_network_base as u64 + (shard as u64) * (shard_size as u64);
                if shard_base.saturating_add(shard_size as u64) > u32::MAX as u64 {
                    error!("Out of address space: increase bridge_ip range or reduce n_nodes.");
                    spawn_failed = true;
                    break 'spawn;
                }

                let bridge_name = bridge_name_for_shard(&self.config.bridge_name, shard);
                let bridge_ip = Ipv4Addr::from((shard_base + gateway_offset as u64) as u32);

                let mut host_offset = BRIDGE_NODE_OFFSET + shard_node_idx as u32;
                if gateway_offset >= BRIDGE_NODE_OFFSET && host_offset >= gateway_offset {
                    host_offset = host_offset.saturating_add(1);
                }
                let ns_ip = Ipv4Addr::from((shard_base + host_offset as u64) as u32);

                match rt.block_on(prepare_net(
                    bridge_name.clone(),
                    &bridge_ip.to_string(),
                    BRIDGE_SHARD_SUBNET,
                    idx,
                )) {
                    Ok((bridge_idx_val, veth_index, veth2_index)) => {
                        prepared = Some((
                            bridge_ip.to_string(),
                            ns_ip.to_string(),
                            bridge_idx_val,
                            veth_index,
                            veth2_index,
                        ));
                        break;
                    }
                    Err(e) => {
                        attempts += 1;
                        error!(
                            "Failed to prepare network for node {} (attempt {}): {}.",
                            idx, attempts, e
                        );

                        if let Err(e) = rt.block_on(async { delete_link_by_name(&veth_name).await })
                        {
                            error!("Failed to delete veth '{}': {}.", veth_name, e);
                        }

                        if is_bridge_full_error(&e) {
                            shard += 1;
                            shard_node_idx = 0;
                            attempts = 0;
                            continue;
                        }

                        if attempts >= 10 {
                            error!(
                                "Giving up preparing network for node {} after {} attempts.",
                                idx, attempts
                            );
                            spawn_failed = true;
                            break 'spawn;
                        }

                        rt.block_on(async {
                            tokio::time::sleep(time::Duration::from_millis(20 * attempts as u64))
                                .await;
                        });
                    }
                }
            }

            let Some((bridge_ip, ns_ip, bridge_idx_val, veth_idx, veth2_idx)) = prepared else {
                spawn_failed = true;
                break 'spawn;
            };

            if !bridge_indices.contains(&bridge_idx_val) {
                bridge_indices.push(bridge_idx_val);
            }

            // prepares child process
            let node_id_offset = self.config.node_id_offset;
            let mut child_config = self.config.clone();
            child_config.n_nodes = 1;
            child_config.node_id = idx + node_id_offset + 1;
            child_config.subnet = BRIDGE_SHARD_SUBNET;
            let mut child_config = Some(child_config);

            // create a pipe for handshake (child signals network ready)
            let (read_fd, write_fd) = nix::unistd::pipe().expect("pipe failed");
            // set read end non-blocking so we can implement timeout polling
            let _ = fcntl(&read_fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK));
            let write_fd_raw: RawFd = write_fd.as_raw_fd();

            let cb = Box::new(move || {
                let config = child_config
                    .take()
                    .expect("Child config consumed more than once");
                child_process(ChildProcessArgs {
                    config,
                    ns_ip: ns_ip.clone(),
                    veth_peer_idx: veth2_idx,
                    handshake_fd: Some(write_fd_raw),
                    bridge_ip: bridge_ip.clone(),
                })
            });

            let mut tmp_stack = match MmapStack::new(STACK_SIZE) {
                Ok(stack) => stack,
                Err(e) => {
                    error!("Failed to allocate child stack: {}. Retrying...", e);
                    continue;
                }
            };
            let child_pid = unsafe {
                clone(
                    cb,
                    tmp_stack.as_mut_slice(),
                    CloneFlags::CLONE_NEWNET | CloneFlags::CLONE_NEWUTS,
                    Some(Signal::SIGCHLD as i32),
                )
            }
            .expect("Clone failed");

            // Close the write end in the parent so the read side can observe EOF when the child exits.
            drop(write_fd);

            // Keep track of children so we can terminate and reap them on shutdown.
            child_pids.push(child_pid);

            // moves veth peer into child's netns
            if let Err(e) =
                rt.block_on(async { join_veth_to_ns(veth2_idx, child_pid.as_raw() as u32).await })
            {
                error!("Failed to join veth to namespace: {}. Retrying...", e);
                let _ = kill(child_pid, Signal::SIGKILL);
                let _ = waitpid(child_pid, None);
                child_pids.pop();
                if let Err(e) = rt.block_on(async { delete_link_by_name(&veth_name).await }) {
                    error!("Failed to delete veth '{}': {}.", veth_name, e);
                }
                continue;
            }

            // gives the child process time to start and configure its peer interface
            // adds an initial, configurable small sleep to let child process start
            rt.block_on(async {
                tokio::time::sleep(time::Duration::from_millis(
                    self.config.child_start_delay_ms,
                ))
                .await;
            });

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
                        rt.block_on(async {
                            tokio::time::sleep(time::Duration::from_millis(10)).await
                        });
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
                let _ = kill(child_pid, Signal::SIGKILL);
                let _ = waitpid(child_pid, None);
                child_pids.pop();
                if let Err(e) = rt.block_on(async { delete_link_by_name(&veth_name).await }) {
                    error!("Failed to delete veth '{}': {}.", veth_name, e);
                }
                continue;
            }

            drop(read_fd); // success cleanup

            // now brings up the master veth
            if let Err(e) = rt.block_on(async { bring_up_master_veth(veth_idx).await }) {
                error!("Failed to bring up master veth: {}. Retrying...", e);
                let _ = kill(child_pid, Signal::SIGKILL);
                let _ = waitpid(child_pid, None);
                child_pids.pop();
                if let Err(e) = rt.block_on(async { delete_link_by_name(&veth_name).await }) {
                    error!("Failed to delete veth '{}': {}.", veth_name, e);
                }
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
                let _ = kill(child_pid, Signal::SIGKILL);
                let _ = waitpid(child_pid, None);
                child_pids.pop();
                if let Err(e) = rt.block_on(async { delete_link_by_name(&veth_name).await }) {
                    error!("Failed to delete veth '{}': {}.", veth_name, e);
                }
                continue;
            }

            // sleeps between node creation to prevent overwhelming the system
            rt.block_on(async {
                tokio::time::sleep(time::Duration::from_millis(
                    self.config.interval_between_spawn,
                ))
                .await;
            });

            shard_node_idx += 1;
        }

        if spawn_failed {
            rt.block_on(self.cleanup(bridge_indices, child_pids));
            return;
        }

        // waits for shutdown signal
        rt.block_on(self.wait_for_shutdown(bridge_indices, child_pids));
    }

    // Waits for the Ctrl + C shutdown signal.
    async fn wait_for_shutdown(&self, bridge_indices: Vec<u32>, child_pids: Vec<Pid>) {
        match tokio::signal::ctrl_c().await {
            Ok(_) => {
                info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            }
            Err(e) => {
                error!("Failed to listen for Ctrl+C: {}", e);
            }
        }

        self.cleanup(bridge_indices, child_pids).await;
    }

    async fn cleanup(&self, bridge_indices: Vec<u32>, child_pids: Vec<Pid>) {
        // terminates child processes (best effort)
        if !child_pids.is_empty() {
            info!(
                "Sending SIGTERM to {} namespace children...",
                child_pids.len()
            );
            for pid in &child_pids {
                let _ = kill(*pid, Signal::SIGTERM);
            }

            // Wait briefly and then SIGKILL any remaining children.
            let deadline = time::Instant::now() + time::Duration::from_secs(2);
            let mut remaining = child_pids.clone();
            while !remaining.is_empty() && time::Instant::now() < deadline {
                remaining.retain(|pid| match waitpid(*pid, Some(WaitPidFlag::WNOHANG)) {
                    Ok(WaitStatus::Exited(..) | WaitStatus::Signaled(..)) => false,
                    Ok(WaitStatus::StillAlive) => true,
                    Ok(_) => true,
                    Err(_) => false,
                });

                if remaining.is_empty() {
                    break;
                }
                tokio::time::sleep(time::Duration::from_millis(50)).await;
            }

            if !remaining.is_empty() {
                info!(
                    "Sending SIGKILL to {} namespace children that did not exit in time...",
                    remaining.len()
                );
                for pid in &remaining {
                    let _ = kill(*pid, Signal::SIGKILL);
                }
            }

            // Reap all children to avoid zombies.
            for pid in child_pids {
                let _ = waitpid(pid, None);
            }
        }

        // best-effort cleanup of host veth devices created for namespace nodes
        for idx in 0..self.config.n_nodes {
            let veth_name = format!("veth{}a", idx);
            if let Err(e) = delete_link_by_name(&veth_name).await {
                error!("Failed to delete veth '{}': {}.", veth_name, e);
            }
        }

        // cleans up bridges
        for bridge_idx in bridge_indices {
            if let Err(e) = delete_namespace(bridge_idx).await {
                error!("Failed to delete namespace bridge {}: {}", bridge_idx, e);
            }
        }
    }
}

/// Parameters required to spawn the child process inside a namespace.
struct ChildProcessArgs {
    config: LocalConfig,
    ns_ip: String,
    veth_peer_idx: u32,
    handshake_fd: Option<RawFd>,
    bridge_ip: String,
}

/// The child process that runs in its own isolated network namespace.
fn child_process(args: ChildProcessArgs) -> isize {
    let ChildProcessArgs {
        mut config,
        ns_ip,
        veth_peer_idx,
        handshake_fd,
        bridge_ip,
    } = args;

    info!("Child process started with node_id {}.", config.node_id);

    // sets hostname for this namespace
    let ns_hostname = format!("nextmini-{}", random_suffix(5));
    unistd::sethostname(&ns_hostname).expect("Failed to set hostname.");

    // creates and runs the Tokio runtime
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create the Tokio runtime.");

    let process = rt.block_on(async {
        // sets up veth interface (this brings up the peer side)
        setup_veth_peer(veth_peer_idx, &ns_ip, config.subnet).await?;

        // signal parent that peer interface is configured
        if let Some(raw_fd) = handshake_fd {
            let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
            let _ = nix::unistd::write(&fd, &[1u8]);
        }

        // namespace nodes should advertise their namespace IPs to the controller
        config.private_network_addr = ns_ip.clone();
        config.public_network_addr = ns_ip.clone();

        // The child process is responsible for resolving loopback controller addresses. Within an
        // isolated network namespace, `127.0.0.1` refers to the namespace itself, not the host, so
        // we replace loopback hosts with the bridge gateway IP.
        let controller_addr = config.controller_addr.clone();
        let resolved_controller_addr = match Url::parse(&controller_addr) {
            Ok(mut url) => {
                let host = url.host_str().unwrap_or_default();
                let is_loopback =
                    host == "127.0.0.1" || host == "localhost" || host == "::1";

                if is_loopback {
                    info!(
                        "Controller address '{}' is loopback, replacing host with gateway IP '{}'.",
                        controller_addr, bridge_ip
                    );
                    if let Err(e) = url.set_host(Some(&bridge_ip)) {
                        warn!(
                            "Failed to rewrite controller host in '{}': {}. Using original address.",
                            controller_addr, e
                        );
                        controller_addr
                    } else {
                        url.to_string()
                    }
                } else {
                    controller_addr
                }
            }
            Err(e) => {
                warn!(
                    "Failed to parse controller address '{}': {}. Using original address.",
                    controller_addr, e
                );
                controller_addr
            }
        };

        config.controller_addr = resolved_controller_addr;

        // adds a default route via the host bridge inside this namespace to enable outbound traffic
        if let Err(e) = add_default_route(veth_peer_idx, &bridge_ip).await {
            error!(
                "Failed to add default route in namespace idx {} via {}: {}.",
                config.node_id, bridge_ip, e
            );
        }

        // starts the conductor
        let conductor = Conductor::new(config).await;
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
