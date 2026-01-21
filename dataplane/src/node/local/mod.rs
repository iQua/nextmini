//! Local (host) interface integration.
//!
//! The local interface connects the dataplane to the host via a TUN device so unmodified
//! applications can communicate using Nextmini-assigned virtual IPs.
//!
//! Linux has an optimized TSO/GSO fast path (see `reader_tso` / `writer_tso`); other platforms use
//! a simpler reader/writer implementation.

pub mod interface;

#[cfg(target_os = "linux")]
pub mod reader_tso;
#[cfg(target_os = "linux")]
pub mod writer_tso;

#[cfg(not(target_os = "linux"))]
pub mod reader;
#[cfg(not(target_os = "linux"))]
pub mod writer;
