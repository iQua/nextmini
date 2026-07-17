//! Deterministic WAN pipeline simulation for nextmini.
//!
//! W0a models a single persistent source -> relay -> receiver chain. Protocol feedback, fan-out,
//! and DoF behavior belong to later stages.

pub mod days_bridge;
pub mod determinism;
pub mod metrics;
pub mod overlay;
pub mod scenario;
pub mod transport;

pub const SCENARIO_SCHEMA_VERSION: u32 = 1;
pub const SIMULATOR_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DAYS_UPSTREAM_REV: &str = "d6a473b555d4f129c1eb62c36cb4525cdd5240ad";
pub const NEXOSIM_VENDORED_TREE: &str = "9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29";
