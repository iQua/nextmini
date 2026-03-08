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

/// Submit one session frame through the blocking processor ingress path.
pub async fn send_frame(processors: &ProcessorHandle, route: FrameRoute, payload: &[u8]) {
    let packet = build_packet(route, payload);
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

/// Encode and emit one lossless session control frame.
pub async fn send_control(
    processors: &ProcessorHandle,
    route: FrameRoute,
    control: &LosslessSessionControl,
) {
    let frame = lossless_session::encode_control(route.session_id, control);
    send_frame(
        processors,
        FrameRoute {
            tree_id: None,
            ..route
        },
        &frame,
    )
    .await;
}
