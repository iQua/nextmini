use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::runtime::CommonConfig;
use nextmini_messages::lossless_session::{self, LosslessSessionControl};
use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

pub struct PacketCaptureHarness {
    pub cfg: LocalConfig,
    pub processors: ProcessorHandle,
    pub packet_rx: mpsc::Receiver<Packet>,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
}

impl PacketCaptureHarness {
    pub fn common_config(&self, session_id: u64, block_size: usize) -> CommonConfig {
        CommonConfig {
            session_id,
            dest_ip: self.dst_ip,
            block_size,
            src_port: self.src_port,
            dst_port: self.dst_port,
            data_bucket: None,
            local_node_id: self.cfg.node_id,
            user_space_base_addr: self.cfg.user_space_base_addr,
            local_netmask: self.cfg.local_netmask,
        }
    }
}

pub async fn packet_capture(
    local_node_id: usize,
    peer_node_id: usize,
    src_port: u16,
    dst_port: u16,
    num_packet_processors: usize,
    channel_capacity: usize,
) -> PacketCaptureHarness {
    let cfg = LocalConfig {
        node_id: local_node_id,
        n_nodes: local_node_id.max(peer_node_id) + 1,
        num_packet_processors,
        channel_capacity,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let processors = ProcessorHandle::new(cfg.clone());

    let src_ip = cfg
        .node_id
        .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let dst_ip = peer_node_id.ip_addr(cfg.user_space_base_addr, cfg.local_netmask);

    processors
        .update_routing_table(vec![RoutingTableEntry {
            route_id: 1,
            next_hops: vec![cfg.node_id],
            src_node_id: cfg.node_id,
            dst_node_id: peer_node_id,
            forward_mode: RouteForwardingMode::Unicast,
        }])
        .await;

    let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
    let (packet_tx, packet_rx) = mpsc::channel(2048);
    processors.connect_user_space_sender(flow_id, packet_tx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    PacketCaptureHarness {
        cfg,
        processors,
        packet_rx,
        dst_ip,
        src_port,
        dst_port,
    }
}

pub async fn recv_packet(packet_rx: &mut mpsc::Receiver<Packet>) -> Packet {
    timeout(Duration::from_secs(5), packet_rx.recv())
        .await
        .expect("timed out waiting for lossless packet")
        .expect("lossless packet channel closed")
}

pub fn ready_frame(session_id: u64, peer_id: usize) -> InboundFrame {
    control_frame(
        session_id,
        peer_id,
        LosslessSessionControl::Ready {
            node_id: peer_id as u64,
        },
    )
}

pub fn block_ack_frame(session_id: u64, peer_id: usize, block_id: u64) -> InboundFrame {
    control_frame(
        session_id,
        peer_id,
        LosslessSessionControl::BlockAck { block_id },
    )
}

fn control_frame(session_id: u64, peer_id: usize, control: LosslessSessionControl) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_control(session_id, &control),
        peer_id: Some(peer_id),
    }
}
