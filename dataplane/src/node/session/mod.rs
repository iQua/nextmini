//! Lossless session support for the dataplane.
//!
//! This subsystem owns the sender and receiver tasks used for bulk transfers,
//! shared codec helpers, and the background runtime that coordinates active
//! sessions.

pub mod api;
pub mod control;
pub mod fec;
mod fec_policy;
pub mod mettle;
pub mod plan;
pub mod receiver;
pub mod runtime;
pub mod sender;
pub(crate) mod timing;
