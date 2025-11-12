use crate::node::packet::Packet;
use nextmini_messages::rlm::{self, RlmControl};

/// Minimal metadata extracted from a MANIFEST control frame for logging/tracing.
#[derive(Clone, Copy, Debug)]
pub struct ManifestMeta {
    pub session_id: u64,
    pub total_bytes: u64,
    pub chunk_size: u32,
}

impl ManifestMeta {
    pub fn new(session_id: u64, total_bytes: u64, chunk_size: u32) -> Self {
        Self {
            session_id,
            total_bytes,
            chunk_size,
        }
    }
}

/// Attempts to parse MANIFEST metadata from an on-wire reliable control frame.
pub fn manifest_from_bytes(bytes: &[u8]) -> Option<ManifestMeta> {
    let (header, control) = rlm::decode_control(bytes)?;
    match control {
        RlmControl::Manifest {
            chunk_size,
            total_bytes,
            ..
        } => Some(ManifestMeta::new(
            header.session_id,
            total_bytes,
            chunk_size,
        )),
        _ => None,
    }
}

/// Attempts to parse MANIFEST metadata from a [`Packet`]'s TCP payload.
pub fn manifest_from_packet(packet: &Packet) -> Option<ManifestMeta> {
    let payload = packet.tcp_payload()?;
    manifest_from_bytes(payload)
}
