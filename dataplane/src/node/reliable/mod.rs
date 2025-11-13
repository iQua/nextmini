//! Reliable multicast (RLM) node plumbing shared by the dataplane sender and
//! receiver tasks. The modules exposed here are wired together by the node
//! processor runtime; each submodule owns one aspect (session management,
//! control helpers, tracing utilities, etc).

pub mod api;
pub mod control;
mod pgmcc;
pub mod receiver;
pub mod sender;
pub mod session;
pub mod trace;
