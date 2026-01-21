//! User-space TCP flow engine (SmolTCP).
//!
//! In addition to forwarding kernel-originated packets via TUN, Nextmini can run TCP flows entirely
//! in user space using SmolTCP. This module provides:
//!
//! - a [`device`] that exposes the dataplane packet path as a SmolTCP `Device`
//! - client/server helpers for establishing and driving flows
//! - flow state tracking for scheduling and metrics

pub mod client;
pub mod device;
pub mod server;
pub mod state;

use crate::node::packet::Packet;
use tokio::sync::mpsc;

const SOCKET_BUFFER_SIZE: usize = 1048576;
pub const INVALID_FLOW_ID: u128 = u128::MAX;

/// A type for sending packets to destinations in user-space TCP flows.
pub type UserSpaceSender = mpsc::Sender<Packet>;
