//! Block-oriented lossless session support for the dataplane.
//!
//! This subsystem owns the sender and receiver tasks used for bulk transfers,
//! the shared block geometry helpers, the optional fountain code adapter, and
//! the background runtime that coordinates active sessions.

pub mod api;
pub mod control;
pub mod fec;
mod fec_policy;
pub mod plan;
pub mod receiver;
pub mod runtime;
pub mod sender;
pub(crate) mod timing;
