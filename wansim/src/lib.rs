//! Deterministic WAN pipeline simulation for nextmini.
//!
//! W0a models a single persistent source -> relay -> receiver chain. W0b adds the fixed fan-out
//! tree, per-child transport state, hybrid receiver admission, and ideal DoF accounting. W1 adds
//! independent section-P protocol endpoints and runs them over the same transport substrate.

pub mod days_bridge;
pub mod determinism;
pub mod metrics;
pub mod overlay;
pub mod protocol;
pub mod scenario;
pub mod transport;

pub const SCENARIO_SCHEMA_VERSION: u32 = 1;
pub const SIMULATOR_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DAYS_UPSTREAM_REV: &str = "d6a473b555d4f129c1eb62c36cb4525cdd5240ad";
pub const NEXOSIM_VENDORED_TREE: &str = "9cad1c1ee25dac8b31c1bb215ce3801eacfc7e29";
