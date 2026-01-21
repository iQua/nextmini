//! Nextmini dataplane (data plane).
//!
//! This crate implements the dataplane node that forwards traffic according to controller-installed
//! routes. It supports multiple ingress paths:
//!
//! - Kernel-backed overlay traffic via a TUN interface (unmodified applications).
//! - User-space engines (SmolTCP flows and lossless sessions) for controller-managed traffic.
//! - MAX transport and SOCKS5 proxy ingress for external endpoints.
//! - Optional in-process Python delivery when built via the `python-api` crate.
//!
//! The core runtime lives under [`node`]. For a conceptual overview, see the documentation site:
//! `docs/` (MkDocs).

pub mod node;
