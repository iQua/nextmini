//! Dataplane ↔ controller interface.
//!
//! This module owns the control-plane connection to the controller and applies:
//!
//! - startup parameters (`StartUp`) such as base addresses and protocol selection
//! - route installs (`InstallRoutes`)
//! - flow installs (`AddFlows`) including lossless-session provisioning
//! - multicast group directory and route updates

pub mod flowstats;
pub mod interface;
pub mod reporter;
