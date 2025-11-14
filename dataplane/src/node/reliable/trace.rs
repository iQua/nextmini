//! Lightweight helpers for extracting trace metadata from reliable control
//! frames. Used by both the dataplane and tooling to enrich logging.

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;

/// Minimal metadata extracted from a MANIFEST control frame for logging/tracing.
#[derive(Clone, Copy, Debug)]
pub struct ManifestMeta {
    pub session_id: u64,
}

impl ManifestMeta {
    pub fn new(session_id: u64) -> Self {
        Self { session_id }
    }
}

/// Attempts to parse MANIFEST metadata from an on-wire reliable control frame.
pub fn manifest_from_bytes(bytes: &[u8]) -> Option<ManifestMeta> {
    let (header, control) = rlm::decode_control(bytes)?;
    match control {
        RlmControl::Manifest { .. } => Some(ManifestMeta::new(header.session_id)),
        _ => None,
    }
}

/// Attempts to parse MANIFEST metadata from a [`Packet`]'s TCP payload.
pub fn manifest_from_packet(packet: &Packet) -> Option<ManifestMeta> {
    let payload = packet.tcp_payload()?;
    manifest_from_bytes(payload)
}
