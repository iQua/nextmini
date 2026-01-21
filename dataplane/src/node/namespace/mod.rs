//! Linux network-namespace orchestration for multi-node-on-one-host runs.
//!
//! `examples/namespace` relies on this module to create veth pairs, bridges, and per-node network
//! namespaces so many dataplane nodes can run on a single machine.

pub mod manager;
pub mod network;
