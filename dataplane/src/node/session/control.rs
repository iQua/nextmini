//! Helpers for emitting block-first lossless session frames through the processor stack.

use std::net::Ipv4Addr;

use nextmini_messages::lossless_session::{self, LosslessSessionControl};

use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressSubmission, ProcessorHandle};

#[derive(Clone, Copy, Debug)]
pub struct FrameRoute {
    pub session_id: u64,
    pub tree_id: Option<u16>,
    pub src_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_ip: Ipv4Addr,
    pub dst_port: u16,
}

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

pub async fn send_frame(processors: &ProcessorHandle, route: FrameRoute, payload: &[u8]) {
    let packet = build_packet(route, payload);
    processors.process_packet(packet).await;
}

pub fn try_send_frame(
    processors: &ProcessorHandle,
    route: FrameRoute,
    payload: &[u8],
) -> LosslessIngressSubmission {
    let packet = build_packet(route, payload);
    processors.try_submit_lossless_packet(packet)
}

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
