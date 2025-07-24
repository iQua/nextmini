use serde::Deserialize;
use log::warn;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Socket address on which the echo server (inside each net-ns) will listen.
    #[serde(default = "default_server_addr")]
    pub server_addr: String,

    /// Which handler to spawn inside each namespace.  Supported values:
    /// "tcp-echo", "udp-echo", "http".
    #[serde(default = "default_handler")]
    pub handler: String,

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
}

/// the default configua for the server
fn default_server_addr() -> String {
    "0.0.0.0:8080".to_string()
}

/// the default handler for the server
fn default_handler() -> String {
    "tcp-echo".to_string()
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

/// the default configuration for namespaces
impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: default_server_addr(),
            handler: default_handler(),
            bridge_name: default_bridge_name(),
            bridge_ip: default_bridge_ip(),
            subnet: default_subnet(),
            n_nodes: default_n_nodes(),
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