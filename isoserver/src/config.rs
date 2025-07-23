use serde::Deserialize;
use log::warn;

/// Configuration options for the iso-server.
///
/// Values are loaded from the `net-config.toml` file that must sit next to the
/// `Cargo.toml` (crate root) of `isoserver`.  Every field has a reasonable
/// default so the binary can still start even if the file is missing or only
/// partially specified.
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

// ---------------------------------------------------------------------------
// Default helpers (must be `fn() -> T` so they can be referenced from serde).
// ---------------------------------------------------------------------------
fn default_server_addr() -> String {
    "0.0.0.0:8080".to_string()
}
fn default_handler() -> String {
    "tcp-echo".to_string()
}
fn default_bridge_name() -> String {
    "isobr0".to_string()
}
fn default_bridge_ip() -> String {
    "172.18.0.1".to_string()
}
fn default_subnet() -> u8 {
    16
}
fn default_n_nodes() -> u32 {
    2
}

// ---------------------------------------------------------------------------
// Implement `Default` so we can recover gracefully if the TOML file is absent.
// ---------------------------------------------------------------------------
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
    /// Load configuration from `net-config.toml` placed at the crate root.  Any
    /// missing or malformed fields fall back to their default values.
    pub fn new() -> Self {
        // Path resolved at **compile time**; independent of current working dir.
        let cfg_path = String::from("net-config.toml");
        match std::fs::read_to_string(&cfg_path) {
            Ok(contents) => match toml::from_str::<Config>(&contents) {
                Ok(cfg) => cfg,
                Err(e) => {
                    warn!(
                        "Failed to parse config file {} ({}) – falling back to defaults",
                        cfg_path, e
                    );
                    Config::default()
                }
            },
            Err(e) => {
                warn!(
                    "Failed to read config file {} ({}) – falling back to defaults",
                    cfg_path, e
                );
                Config::default()
            }
        }
    }
} 