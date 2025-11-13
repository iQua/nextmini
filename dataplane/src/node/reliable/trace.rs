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

/// Metadata describing PGMCC control frames for logging/tracing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgmccKind {
    Feedback,
    Acker,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub struct PgmccMeta {
    pub session_id: u64,
    pub kind: PgmccKind,
}

/// Attempts to parse PGMCC metadata from an on-wire reliable control frame.
pub fn pgmcc_meta_from_bytes(bytes: &[u8]) -> Option<PgmccMeta> {
    let (header, control) = rlm::decode_control(bytes)?;
    match control {
        RlmControl::PgmccFeedback { .. } => Some(PgmccMeta {
            session_id: header.session_id,
            kind: PgmccKind::Feedback,
        }),
        RlmControl::PgmccAcker { .. } => Some(PgmccMeta {
            session_id: header.session_id,
            kind: PgmccKind::Acker,
        }),
        _ => None,
    }
}

/// Attempts to parse PGMCC metadata from a [`Packet`]'s TCP payload.
#[allow(dead_code)]
pub fn pgmcc_meta_from_packet(packet: &Packet) -> Option<PgmccMeta> {
    let payload = packet.tcp_payload()?;
    pgmcc_meta_from_bytes(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pgmcc_meta_roundtrip() {
        let sid = 42;
        let feedback = RlmControl::PgmccFeedback {
            node_id: 7,
            acked_upto: 9,
            rtt_ms_x8: 80,
            loss_event_rate_x1e6: 100,
        };
        let buf = rlm::encode_control(sid, &feedback);
        let meta = pgmcc_meta_from_bytes(&buf).expect("feedback meta");
        assert_eq!(meta.session_id, sid);
        assert_eq!(meta.kind, PgmccKind::Feedback);

        let acker = RlmControl::PgmccAcker { node_id: 5 };
        let buf = rlm::encode_control(sid, &acker);
        let meta = pgmcc_meta_from_bytes(&buf).expect("acker meta");
        assert_eq!(meta.kind, PgmccKind::Acker);
    }
}
