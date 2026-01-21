//! Packet scheduling, pacing, and queueing disciplines.
//!
//! The scheduler sits between packet processors and network egress. Implementations include FIFO,
//! weighted round-robin (WRR), and token-bucket pacing.

pub mod drop;
pub mod fifo;
pub mod queue;
pub mod reader;
pub mod sched;
pub mod token_bucket;
pub mod writer;
pub mod wrr;
