use serde::Deserialize;
use tracing::warn;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Socket address on which the echo server (inside each net-ns) will listen.
    #[serde(default = "default_controller_addr")]
    pub controller_addr: String,

    /// Linux bridge to create (or reuse).
    #[serde(default = "default_bridge_name")]
    pub bridge_name: String,

    /// IPv4 address to assign to the bridge.
    #[serde(default = "default_bridge_ip")]
    pub bridge_ip: String,

    /// Subnet mask length (CIDR) associated with `bridge_ip`.
    #[serde(default = "default_subnet")]
    pub subnet: u8,

    /// Number of isolated network namespaces to spawn.
    #[serde(default = "default_n_nodes")]
    pub n_nodes: u32,

    /// Sleep time in milliseconds between spawning each node in the main loop.
    /// This controls the interval between creating network namespaces and child processes.
    /// Helps prevent overwhelming the system when creating many nodes at once.
    #[serde(default = "default_main_loop_sleep_ms")]
    pub main_loop_sleep_ms: u64,

    /// Sleep multiplier in milliseconds for staggered child process connections.
    /// Each child process sleeps for (node_index * multiplier) before connecting to controller.
    /// This prevents all nodes from connecting simultaneously and overwhelming the controller.
    #[serde(default = "default_child_sleep_multiplier_ms")]
    pub child_sleep_multiplier_ms: u64,
}

/// the default controller address running on the host
fn default_controller_addr() -> String {
    "127.0.0.1:3000".to_string()
}

/// the default bridge name
fn default_bridge_name() -> String {
    "isobr0".to_string()
}

/// the default bridge ip
fn default_bridge_ip() -> String {
    "172.18.0.1".to_string()
}

/// the default subnet
fn default_subnet() -> u8 {
    16
}

/// the default number of nodes(namespaces)
fn default_n_nodes() -> u32 {
    2
}

/// the default sleep time in milliseconds between spawning each node
fn default_main_loop_sleep_ms() -> u64 {
    200
}

/// the default sleep multiplier for child processes
fn default_child_sleep_multiplier_ms() -> u64 {
    200
}

/// the default configuration for namespaces
impl Default for Config {
    fn default() -> Self {
        Self {
            controller_addr: default_controller_addr(),
            bridge_name: default_bridge_name(),
            bridge_ip: default_bridge_ip(),
            subnet: default_subnet(),
            n_nodes: default_n_nodes(),
            main_loop_sleep_ms: default_main_loop_sleep_ms(),
            child_sleep_multiplier_ms: default_child_sleep_multiplier_ms(),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        // load config from net-config.toml
        let cfg_path = concat!(env!("CARGO_MANIFEST_DIR"), "/net-config.toml");

        match std::fs::read_to_string(&cfg_path) {
            Ok(contents) => match toml::from_str::<Config>(&contents) {
                Ok(cfg) => cfg,
                Err(e) => {
                    warn!(
                        "Failed to parse config file {} (with error: {}), using default values",
                        cfg_path, e
                    );
                    Config::default()
                }
            },
            Err(e) => {
                warn!(
                    "Failed to read config file {} (with error: {}), using default values",
                    cfg_path, e
                );
                Config::default()
            }
        }
    }
}
