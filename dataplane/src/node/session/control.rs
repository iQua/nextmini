//! Helpers for emitting block-first lossless session frames through the processor stack.

use std::net::Ipv4Addr;

use nextmini_messages::lossless_session::{self, LosslessSessionControl};

use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressSubmission, ProcessorHandle};

/// Addressing information for one outbound session frame.
#[derive(Clone, Copy, Debug)]
pub struct FrameRoute {
    /// Session identifier encoded into the transport metadata.
    pub session_id: u64,
    /// Optional tree identifier used for FEC symbol striping.
    pub tree_id: Option<u16>,
    /// Source IP address used for the synthetic TCP packet wrapper.
    pub src_ip: Ipv4Addr,
    /// Source TCP port used for the synthetic TCP packet wrapper.
    pub src_port: u16,
    /// Destination IP address used for the synthetic TCP packet wrapper.
    pub dst_ip: Ipv4Addr,
    /// Destination TCP port used for the synthetic TCP packet wrapper.
    pub dst_port: u16,
}

/// Build a synthetic TCP packet carrying one encoded lossless session frame.
pub fn build_packet(route: FrameRoute, payload: &[u8]) -> Packet {
    Packet::build_ipv4_tcp_packet_with_lossless_meta(
        route.src_ip,
        route.src_port,
        route.dst_ip,
        route.dst_port,
        Some(LosslessTransportMeta {
            session_id: route.session_id,
            tree_id: route.tree_id,
        }),
        payload,
    )
}

/// Build a synthetic TCP packet carrying one `BlockData` frame encoded directly into the packet
/// allocation, so the payload is copied exactly once.
pub fn build_block_packet(route: FrameRoute, block_id: u64, payload: &[u8]) -> Packet {
    Packet::build_ipv4_tcp_packet_with_payload_writer(
        route.src_ip,
        route.src_port,
        route.dst_ip,
        route.dst_port,
        Some(LosslessTransportMeta {
            session_id: route.session_id,
            tree_id: route.tree_id,
        }),
        lossless_session::block_data_frame_len(payload.len()),
        |buf| lossless_session::encode_block_data_into(buf, route.session_id, block_id, payload),
    )
}

/// Submit one `BlockData` frame through the blocking processor ingress path.
pub async fn send_block_frame(
    processors: &ProcessorHandle,
    route: FrameRoute,
    block_id: u64,
    payload: &[u8],
) {
    let packet = build_block_packet(route, block_id, payload);
    processors.process_packet(packet).await;
}

/// Submit one session frame through the non-blocking processor ingress path.
pub fn try_send_frame(
    processors: &ProcessorHandle,
    route: FrameRoute,
    payload: &[u8],
) -> LosslessIngressSubmission {
    let packet = build_packet(route, payload);
    processors.try_submit_lossless_packet(packet)
}

/// Build one control packet on the control-routing path.
pub fn build_control_packet(route: FrameRoute, control: &LosslessSessionControl) -> Packet {
    let frame = lossless_session::encode_control(route.session_id, control);
    build_packet(
        FrameRoute {
            tree_id: None,
            ..route
        },
        &frame,
    )
}

/// Encode and emit one lossless session control frame.
pub async fn send_control(
    processors: &ProcessorHandle,
    route: FrameRoute,
    control: &LosslessSessionControl,
) {
    let packet = build_control_packet(route, control);
    processors.process_packet(packet).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn block_packets_match_separately_encoded_frame_packets() {
        for tree_id in [None, Some(3)] {
            let route = FrameRoute {
                session_id: 17,
                tree_id,
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                src_port: 4100,
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                dst_port: 5100,
            };
            // Empty tail, typical, and the largest payload that fits one framed packet.
            for payload_len in [0usize, 1, 8500, 65451] {
                let payload: Vec<u8> = (0..payload_len).map(|i| (i % 251) as u8).collect();
                let frame = lossless_session::encode_block_data(17, 9, &payload);
                let expected = build_packet(route, &frame);
                let actual = build_block_packet(route, 9, &payload);
                assert_eq!(
                    actual.bytes(),
                    expected.bytes(),
                    "payload_len {payload_len}"
                );
                assert_eq!(
                    lossless_session::block_data_frame_len(payload_len),
                    frame.len()
                );
            }
        }
    }

    #[test]
    fn control_packets_clear_payload_tree_ids() {
        let packet = build_control_packet(
            FrameRoute {
                session_id: 17,
                tree_id: Some(9),
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                src_port: 4100,
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                dst_port: 5100,
            },
            &LosslessSessionControl::SourceDone { round_id: 3 },
        );

        assert_eq!(packet.lossless_session_id(), Some(17));
        assert_eq!(packet.lossless_fec_tree_id(), None);
        let payload = packet
            .tcp_payload()
            .expect("control packet should include payload");
        assert_eq!(
            lossless_session::decode_control(payload),
            Some((
                lossless_session::LosslessSessionHeader::decode_from(payload)
                    .expect("control header should decode")
                    .0,
                LosslessSessionControl::SourceDone { round_id: 3 },
            ))
        );
    }
}
