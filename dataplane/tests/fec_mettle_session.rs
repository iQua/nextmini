mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

use nextmini::node::packet::Packet;
use nextmini::node::session::api::{InboundFrame, SessionOutcome};
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{ReceiverConfig, SenderConfig};
use nextmini::node::session::sender;
use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{
    self, LosslessSessionBlockSymbol, LosslessSessionControl, LosslessSessionFecMode,
    LosslessSessionManifest, LosslessSessionMode, NeedReport,
};

const SENDER_NODE_ID: usize = 21;
const RECEIVER_NODE_ID: usize = 22;
const SRC_PORT: u16 = 4700;
const DST_PORT: u16 = 4800;
const PEER_REPORT_TIMEOUT_MS: u64 = 30_000;
const RECEIVER_CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const PAPER_SCALE_METTLE_K: usize = 2400;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mettle_lossless_session_repairs_dropped_source_through_sender_receiver_flow() {
    let mut sender_harness = common::packet_capture(
        SENDER_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        4096,
    )
    .await;
    let mut receiver_harness = common::packet_capture(
        RECEIVER_NODE_ID,
        SENDER_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        4096,
    )
    .await;

    let session_id = 0x4D45_5454_1E01;
    let k = PAPER_SCALE_METTLE_K;
    let source_bytes = patterned_source_bytes(k);
    // Fixed one-erasure case for this session seed under the METTLE paper graph.
    // The real Need, repair, Complete, and sink-byte assertions below catch drift.
    let missing_source = 45usize;
    assert!(missing_source < k);
    let symbols_per_block = u16::try_from(k).expect("paper-scale METTLE K fits u16");
    let manifest = LosslessSessionManifest {
        block_size: k as u32,
        total_bytes: k as u64,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(
            symbols_per_block,
            vec![1],
        )),
    };

    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id,
        route: receiver_harness.route(),
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: Some(sink.clone()),
        progress: None,
        peer_report_timeout_ms: PEER_REPORT_TIMEOUT_MS,
        fec_enabled: true,
    };
    let (receiver_tx, receiver_rx) = mpsc::channel::<InboundFrame>(4096);
    let receiver_task = tokio::spawn(receiver::run(
        receiver_cfg,
        receiver_rx,
        receiver_harness.processors.clone(),
    ));

    let sender_cfg = SenderConfig {
        session: sender_harness.session_config(session_id, k),
        route: sender_harness.route(),
        pacing: Some(TokenBucketSpec {
            rate: 50_000,
            bucket_size: 1,
        }),
        receiver_ids: vec![RECEIVER_NODE_ID],
        source_buffer: Bytes::from(source_bytes.clone()),
        manifest: manifest.clone(),
        ready_grace_ms: 500,
        peer_report_timeout_ms: PEER_REPORT_TIMEOUT_MS,
        topology_ready: None,
    };
    let (sender_ctrl_tx, sender_ctrl_rx) = mpsc::channel::<InboundFrame>(4096);
    let sender_task = tokio::spawn(sender::run(
        sender_cfg,
        sender_ctrl_rx,
        sender_harness.processors.clone(),
    ));

    let sender_manifest_packet = common::recv_packet(&mut sender_harness.packet_rx).await;
    assert_eq!(
        sender_manifest_packet.lossless_session_id(),
        Some(session_id)
    );
    let sender_manifest = decode_control(&sender_manifest_packet);
    assert_eq!(
        sender_manifest,
        LosslessSessionControl::Manifest {
            manifest: manifest.clone()
        },
        "sender should advertise the METTLE manifest through its real control path"
    );
    receiver_tx
        .send(inbound_from_packet(sender_manifest_packet, SENDER_NODE_ID))
        .await
        .expect("manifest should enqueue at receiver");

    let ready_packet = recv_receiver_control_packet(&mut receiver_harness.packet_rx).await;
    let ready = decode_control(&ready_packet);
    assert_eq!(ready, LosslessSessionControl::Ready);
    sender_ctrl_tx
        .send(inbound_from_packet(ready_packet, RECEIVER_NODE_ID))
        .await
        .expect("READY should enqueue at sender");

    let mut delivered_sources = 0usize;
    let mut dropped_missing_source = false;
    loop {
        let packet = common::recv_packet(&mut sender_harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured sender packet should include TCP payload");
        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    assert!(dropped_missing_source, "test must drop one source symbol");
                    assert_eq!(
                        delivered_sources,
                        k - 1,
                        "test must deliver every source except the deterministic erasure"
                    );
                    receiver_tx
                        .send(inbound_from_packet(packet, SENDER_NODE_ID))
                        .await
                        .expect("SourceDone(0) should enqueue at receiver");
                    break;
                }
                LosslessSessionControl::Manifest { .. } => {
                    receiver_tx
                        .send(inbound_from_packet(packet, SENDER_NODE_ID))
                        .await
                        .expect("duplicate manifest should enqueue at receiver");
                }
                other => panic!("unexpected sender control before first SourceDone: {other:?}"),
            }
            continue;
        }

        let symbol = decode_symbol(payload);
        assert_eq!(symbol.block_id, 0);
        assert!(
            symbol.symbol_id < k as u32,
            "repair symbol appeared before receiver Need: {symbol:?}"
        );
        if symbol.symbol_id as usize == missing_source {
            dropped_missing_source = true;
        } else {
            delivered_sources += 1;
            receiver_tx
                .send(inbound_from_packet(packet, SENDER_NODE_ID))
                .await
                .expect("source symbol should enqueue at receiver");
        }
    }

    let need_packet = recv_receiver_control_packet(&mut receiver_harness.packet_rx).await;
    let need = decode_control(&need_packet);
    let LosslessSessionControl::Need {
        round_id: 0,
        report: NeedReport::Fec { blocks },
    } = need
    else {
        panic!("receiver should request METTLE repair after SourceDone(0): {need:?}");
    };
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].block_id, 0);
    assert!(
        blocks[0].deficit_symbols > 0,
        "METTLE deficit should be finite and positive"
    );
    sender_ctrl_tx
        .send(inbound_from_packet(need_packet, RECEIVER_NODE_ID))
        .await
        .expect("FEC Need should enqueue at sender");

    let mut saw_repair_symbol = false;
    loop {
        let packet = common::recv_packet(&mut sender_harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured sender packet should include TCP payload");
        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::SourceDone { round_id } => {
                    if round_id == 0 {
                        receiver_tx
                            .send(inbound_from_packet(packet, SENDER_NODE_ID))
                            .await
                            .expect("duplicate SourceDone(0) should enqueue at receiver");
                        let duplicate_need =
                            recv_receiver_control_packet(&mut receiver_harness.packet_rx).await;
                        assert_eq!(
                            decode_control(&duplicate_need),
                            LosslessSessionControl::Need {
                                round_id: 0,
                                report: NeedReport::Fec {
                                    blocks: blocks.clone(),
                                },
                            },
                            "receiver should replay the same round-0 FEC Need"
                        );
                        sender_ctrl_tx
                            .send(inbound_from_packet(duplicate_need, RECEIVER_NODE_ID))
                            .await
                            .expect("duplicate FEC Need should enqueue at sender");
                        continue;
                    }
                    assert_eq!(round_id, 1);
                    assert!(
                        saw_repair_symbol,
                        "test must observe at least one METTLE repair symbol before completion"
                    );
                    receiver_tx
                        .send(inbound_from_packet(packet, SENDER_NODE_ID))
                        .await
                        .expect("SourceDone(1) should enqueue at receiver");
                    break;
                }
                LosslessSessionControl::Manifest { .. } => {
                    receiver_tx
                        .send(inbound_from_packet(packet, SENDER_NODE_ID))
                        .await
                        .expect("duplicate manifest should enqueue at receiver");
                }
                other => panic!("unexpected sender control during repair round: {other:?}"),
            }
            continue;
        }

        let symbol = decode_symbol(payload);
        assert_eq!(symbol.block_id, 0);
        assert!(
            symbol.symbol_id >= k as u32,
            "sender retransmitted a source symbol instead of METTLE repair: {symbol:?}"
        );
        saw_repair_symbol = true;
        receiver_tx
            .send(inbound_from_packet(packet, SENDER_NODE_ID))
            .await
            .expect("repair symbol should enqueue at receiver");
    }

    let complete_packet = recv_receiver_control_packet(&mut receiver_harness.packet_rx).await;
    let complete = decode_control(&complete_packet);
    assert_eq!(
        complete,
        LosslessSessionControl::Need {
            round_id: 1,
            report: NeedReport::Complete,
        },
        "receiver should complete only after the METTLE repair round"
    );
    sender_ctrl_tx
        .send(inbound_from_packet(complete_packet, RECEIVER_NODE_ID))
        .await
        .expect("Complete should enqueue at sender");

    let sender_outcome = timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
    assert_eq!(sender_outcome, SessionOutcome::Completed);

    drop(receiver_tx);
    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task timed out")
        .expect("receiver task failed");

    let sink_bytes = sink.lock().await.clone();
    assert_eq!(sink_bytes, source_bytes);
}

async fn recv_receiver_control_packet(packet_rx: &mut mpsc::Receiver<Packet>) -> Packet {
    loop {
        let packet = timeout(RECEIVER_CONTROL_TIMEOUT, packet_rx.recv())
            .await
            .expect("timed out waiting for receiver control packet")
            .expect("receiver control packet channel closed");
        if packet
            .tcp_payload()
            .and_then(lossless_session::decode_control)
            .is_some()
        {
            return packet;
        }
    }
}

fn inbound_from_packet(packet: Packet, peer_id: usize) -> InboundFrame {
    InboundFrame {
        bytes: packet
            .tcp_payload()
            .expect("captured packet should include TCP payload")
            .to_vec(),
        peer_id: Some(peer_id),
    }
}

fn decode_control(packet: &Packet) -> LosslessSessionControl {
    let payload = packet
        .tcp_payload()
        .expect("captured packet should include TCP payload");
    lossless_session::decode_control(payload)
        .map(|(_, control)| control)
        .expect("packet should carry a lossless control frame")
}

fn decode_symbol(payload: &[u8]) -> LosslessSessionBlockSymbol {
    let (_, symbol, body) =
        lossless_session::decode_block_symbol(payload).expect("expected FEC block symbol");
    assert_eq!(body.len(), 1, "METTLE test uses one-byte source symbols");
    symbol
}

fn patterned_source_bytes(len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| ((idx * 37 + 0x41) % 251) as u8)
        .collect()
}
