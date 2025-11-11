use bytes::{Bytes, BytesMut, BufMut};

use nextmini_messages::rlm::{RlmControl};

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::packet::Packet;
use crate::node::{NodeId, NodeIdExt};

use super::session::SenderConfig;

/// Minimal baseline: build and log MANIFEST/DATA/EOT frames without writing to the network
/// when the network writer is not yet wired. Keeps pacing and control logic development
/// moving forward under the `reliable` feature gate.
pub async fn run(cfg: SenderConfig, processors: ProcessorHandle) {
    let sid = cfg.common.session_id;
    let chunk_size = cfg.common.chunk_size as u64;
    let total = cfg.total_bytes;
    let total_chunks = if chunk_size == 0 { 0 } else { (total + chunk_size - 1) / chunk_size };

    // Emit MANIFEST (log-only for now).
    let manifest = RlmControl::Manifest {
        chunk_size: cfg.common.chunk_size as u32,
        total_bytes: cfg.total_bytes,
        checksum_algo: 0,
        options: 0,
    };
    let man_buf = nextmini_messages::rlm::encode_control(sid, &manifest);
    if let Some((_hdr, ctrl)) = nextmini_messages::rlm::decode_control(&man_buf) {
        if std::env::var("RELIABLE_HEX_LOG").ok().as_deref() == Some("1") {
            tracing::info!(session_id=sid, kind="MANIFEST", hex=%hex::encode(&man_buf), ?ctrl, "RLM control frame");
        } else {
            tracing::info!(session_id=sid, ?ctrl, "RLM sender: MANIFEST built");
        }
    } else {
        tracing::warn!(session_id=sid, "RLM sender: failed to round-trip MANIFEST");
    }

    // DATA frames: read from source_path if provided; otherwise simulate empty payloads.
    let mut bytes_sent: u64 = 0;
    if let Some(path) = &cfg.source_path {
        match std::fs::File::open(path) {
            Ok(mut f) => {
                let mut idx: u64 = 1;
                let mut buf = vec![0u8; cfg.common.chunk_size];
                loop {
                    let read = match std::io::Read::read(&mut f, &mut buf) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!(session_id=sid, error=%e, "RLM sender: read error");
                            break;
                        }
                    };
                    let frame = nextmini_messages::rlm::encode_data(sid, idx, &buf[..read]);
                    if let Some((_hdr, data, body)) = nextmini_messages::rlm::decode_data(&frame) {
                        if std::env::var("RELIABLE_HEX_LOG").ok().as_deref() == Some("1") && read <= 64 {
                            tracing::debug!(session_id=sid, index=idx, hex=%hex::encode(&frame), payload_len=read, ?data, "RLM data frame");
                        }
                        // Inject into the processor pipeline for each receiver.
                        for rid in &cfg.receiver_ids {
                            let src_ip = (cfg.common.local_node_id as NodeId)
                                .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                            let dst_ip = (*rid as NodeId)
                                .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                            let packet = Packet::build_ipv4_tcp_packet(
                                src_ip,
                                cfg.common.src_port,
                                dst_ip,
                                cfg.common.dst_port,
                                &frame,
                            );
                            processors.process_packet(packet);
                        }
                    } else {
                        tracing::warn!(session_id=sid, index=idx, "RLM sender: failed to round-trip DATA");
                    }
                    bytes_sent += read as u64;
                    idx += 1;
                    // Simple pacing stub.
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
            Err(e) => {
                tracing::error!(session_id=sid, path=%path, error=%e, "RLM sender: unable to open source file; sending empty payload frames");
                for idx in 1..=total_chunks {
                    let frame = nextmini_messages::rlm::encode_data(sid, idx, &[]);
                    let _ = nextmini_messages::rlm::decode_data(&frame);
                    for rid in &cfg.receiver_ids {
                        let src_ip = (cfg.common.local_node_id as NodeId)
                            .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                        let dst_ip = (*rid as NodeId)
                            .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                        let packet = Packet::build_ipv4_tcp_packet(
                            src_ip,
                            cfg.common.src_port,
                            dst_ip,
                            cfg.common.dst_port,
                            &frame,
                        );
                        processors.process_packet(packet);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
    } else {
        for idx in 1..=total_chunks {
            let frame = nextmini_messages::rlm::encode_data(sid, idx, &[]);
            let _ = nextmini_messages::rlm::decode_data(&frame);
            for rid in &cfg.receiver_ids {
                let src_ip = (cfg.common.local_node_id as NodeId)
                    .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                let dst_ip = (*rid as NodeId)
                    .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
                let packet = Packet::build_ipv4_tcp_packet(
                    src_ip,
                    cfg.common.src_port,
                    dst_ip,
                    cfg.common.dst_port,
                    &frame,
                );
                processors.process_packet(packet);
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    // Emit EOT (log-only).
    let eot = RlmControl::Eot { last_index: total_chunks, checksum: None };
    let eot_buf = nextmini_messages::rlm::encode_control(sid, &eot);
    if let Some((_hdr, ctrl)) = nextmini_messages::rlm::decode_control(&eot_buf) {
        if std::env::var("RELIABLE_HEX_LOG").ok().as_deref() == Some("1") {
            tracing::info!(session_id=sid, kind="EOT", hex=%hex::encode(&eot_buf), ?ctrl, bytes_sent, total_bytes=total, "RLM control frame");
        } else {
            tracing::info!(session_id=sid, ?ctrl, bytes_sent, total_bytes=total, "RLM sender: EOT built");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_roundtrip_header_and_meta() {
        let sid = 99;
        let idx = 7;
        let plen = 4096usize;
        let payload = vec![0xAAu8; plen];
        let buf = nextmini_messages::rlm::encode_data(sid, idx, &payload);
        let (hdr, data, body) = nextmini_messages::rlm::decode_data(&buf).expect("decode data");
        assert_eq!(hdr.session_id, sid);
        assert_eq!(data.index, idx);
        assert_eq!(data.payload_len as usize, plen);
        assert_eq!(body.len(), plen);
    }
}
