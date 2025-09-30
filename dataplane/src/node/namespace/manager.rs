use std::net::Ipv4Addr;
use std::thread;
use std::time;

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

pub struct NamespaceManager {
    config: LocalConfig,
}

impl NamespaceManager {
    pub fn new(config: LocalConfig) -> Self {
        Self { config }
    }

    // spawns all namespaces and waits for shutdown
    pub async fn spawn_all_nodes(&mut self) {
        // Set the controller address to the bridge IP if it's running on the host
        let controller_addr: String = match self.config.controller_addr.as_str() {
            "127.0.0.1:3000" => format!("ws://{}:3000", self.config.bridge_ip),
            _ => format!("ws://{}", self.config.controller_addr),
        };
        info!("Controller address set to {}.", controller_addr);

        // computes the namespace IP addresses
        let ns_ips = self.compute_namespace_ips();

        // keeps child stacks alive while children run
        let mut stacks: Vec<Box<[u8; STACK_SIZE]>> = Vec::new();
        let mut bridge_idx: Option<u32> = None;

        // spawns each namespace
        let mut idx = 0;
        while idx < ns_ips.len() {
            let ns_ip = &ns_ips[idx];
            
            match self
                .spawn_one(idx, ns_ip, &controller_addr, &mut stacks, &mut bridge_idx)
                .await
            {
                Ok(_) => {
                    info!("Successfully spawned namespace {}.", idx);
                    idx += 1;  // only increments on success
                    
                    // sleeps between node creation to prevent overwhelming the system
                    thread::sleep(time::Duration::from_millis(self.config.main_loop_sleep_ms));
                }
                Err(e) => {
                    error!("Failed to spawn namespace {}: {}. Retrying...", idx, e);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }

        // waits for shutdown signal
        self.wait_for_shutdown(bridge_idx).await;
    }

    // computes the namespace IP addresses
    fn compute_namespace_ips(&self) -> Vec<String> {
        let base: u32 = self
            .config
            .bridge_ip
            .parse::<Ipv4Addr>()
            .expect("Invalid bridge IP")
            .into();

        // IP addresses: bridge_ip + 3 to bridge_ip + n_nodes + 2
        // offsets: 1 for controller, 1 for database, the rest for nodes
        (3..=self.config.n_nodes + 2)
            .map(|offset| Ipv4Addr::from(base + offset as u32).to_string())
            .collect()
    }

    // spawns one namespace
    async fn spawn_one(
        &mut self,
        idx: usize,
        ns_ip: &str,
        controller_addr: &str,
        stacks: &mut Vec<Box<[u8; STACK_SIZE]>>,
        bridge_idx: &mut Option<u32>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ns_ip = ns_ip.to_string();

        // prepares bridge + a fresh veth pair
        let (bid, _veth_idx, veth2_idx) = prepare_net(
            self.config.bridge_name.clone(),
            &self.config.bridge_ip,
            self.config.subnet,
            idx,
        )
        .await?;

        *bridge_idx = Some(bid);

        // prepares child process closure
        let child_sleep_multiplier_ms = self.config.child_sleep_multiplier_ms;
        let subnet = self.config.subnet;
        let controller_addr = controller_addr.to_string();
        let cb = Box::new(move || {
            child_process(
                ns_ip,
                veth2_idx,
                controller_addr,
                idx,
                subnet,
                child_sleep_multiplier_ms,
            )
        });

        // clones the child process into a new namespace
        let mut stack: Box<[u8; STACK_SIZE]> = Box::new([0; STACK_SIZE]);
        let child_pid = unsafe {
            clone(
                cb,
                stack.as_mut(),
                CloneFlags::CLONE_NEWNET | CloneFlags::CLONE_NEWUTS,
                Some(Signal::SIGCHLD as i32),
            )
        }
        .expect("Clone failed.");

        // keeps stack memory alive
        stacks.push(stack);
        info!("Spawned child pid: {}.", child_pid);

        // moves veth peer into child's namespace
        join_veth_to_ns(veth2_idx, child_pid.as_raw() as u32).await?;

        Ok(())
    }

    // waits for Ctrl+C shutdown signal
    async fn wait_for_shutdown(&self, bridge_idx: Option<u32>) {
        match tokio::signal::ctrl_c().await {
            Ok(_) => {
                info!("Received Ctrl + C. Shutting down Nextmini gracefully...");
            }
            Err(e) => {
                error!("Failed to listen for Ctrl+C: {}", e);
            }
        }

        // Clean up the bridge
        if let Some(bridge_idx) = bridge_idx {
            if let Err(e) = delete_namespace(bridge_idx).await {
                error!("Failed to delete namespace: {}", e);
            }
        }
    }
}

/// Child process function executed within the namespace
fn child_process(
    ns_ip: String,
    veth_peer_idx: u32,
    controller_addr: String,
    idx: usize,
    subnet: u8,
    child_sleep_multiplier_ms: u64,
) -> isize {
    info!(
        "Child process (PID: {}) started with idx {}",
        unistd::getpid(),
        idx
    );

    // Set hostname for this namespace
    let ns_hostname = format!("nextmini-{}", random_suffix(5));
    unistd::sethostname(&ns_hostname).expect("Failed to set hostname");

    // Create runtime and execute
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let process = rt.block_on(async {
        // Setup veth interface
        setup_veth_peer(veth_peer_idx, &ns_ip, subnet).await?;

        // Staggered connection: each node waits longer to prevent controller overload
        let sleep_ms = (idx as u64) * child_sleep_multiplier_ms;
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        // Load config using new_for_namespace (handles all namespace-specific settings)
        let config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/config.toml");
        let config = LocalConfig::new_for_namespace(config_path, &controller_addr, &ns_ip);

        // Start the conductor
        let conductor = Conductor::new_for_namespace(config).await;
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