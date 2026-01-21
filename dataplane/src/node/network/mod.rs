//! Inter-node transport implementations.
//!
//! Nextmini can move packets/frames between nodes over:
//!
//! - TCP (default)
//! - QUIC (s2n-quic; optional)
//! - UDP (specialized paths)
//! - MAX transport (`tcp_max`) for connection-on-demand splicing and SOCKS5 proxy ingress

pub mod interface;
pub mod quic;
pub mod tcp;
pub mod tcp_max;
pub mod udp;
