//! Block-oriented lossless session support for the dataplane.
//!
//! This subsystem owns the sender and receiver tasks used for bulk transfers,
//! the shared block geometry and acknowledgement helpers, the optional fountain
//! code adapter, and the controller-facing unicast flow integration.

pub mod api;
pub mod control;
pub mod fec;
mod fec_policy;
pub mod ledger;
pub mod plan;
pub mod receiver;
pub mod runtime;
pub mod sender;
pub mod unicast;
