//! Helpers for emitting block-first lossless session frames through the processor stack.

use std::net::Ipv4Addr;

use nextmini_messages::lossless_session::{self, LosslessSessionControl};

use crate::node::packet::{LosslessTransportMeta, Packet};
use crate::node::processor::{LosslessIngressSubmission, ProcessorHandle};

pub fn build_packet(
    session_id: u64,
    tree_id: Option<u16>,
    src_ip: Ipv4Addr,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> Packet {
    Packet::build_ipv4_tcp_packet_with_lossless_meta(
        src_ip,
        src_port,
        dst_ip,
        dst_port,
        Some(LosslessTransportMeta {
            session_id,
            tree_id,
        }),
        payload,
    )
}

pub async fn send_frame(
    processors: &ProcessorHandle,
    session_id: u64,
    tree_id: Option<u16>,
    src_ip: Ipv4Addr,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) {
    let packet = build_packet(session_id, tree_id, src_ip, src_port, dst_ip, dst_port, payload);
    processors.process_packet(packet).await;
}

pub fn try_send_frame(
    processors: &ProcessorHandle,
    session_id: u64,
    tree_id: Option<u16>,
    src_ip: Ipv4Addr,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> LosslessIngressSubmission {
    let packet = build_packet(session_id, tree_id, src_ip, src_port, dst_ip, dst_port, payload);
    processors.try_submit_lossless_packet(packet)
}

pub async fn send_control(
    processors: &ProcessorHandle,
    session_id: u64,
    src_ip: Ipv4Addr,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    control: &LosslessSessionControl,
) {
    let frame = lossless_session::encode_control(session_id, control);
    send_frame(
        processors,
        session_id,
        None,
        src_ip,
        src_port,
        dst_ip,
        dst_port,
        &frame,
    )
    .await;
}
