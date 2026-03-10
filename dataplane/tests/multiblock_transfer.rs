mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{ReceiverConfig, SenderConfig};
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode,
};

const SOURCE_NODE_ID: usize = 21;
const RECEIVER_NODE_ID: usize = 22;
const SRC_PORT: u16 = 4710;
const DST_PORT: u16 = 5710;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_sender_emits_every_block_id_before_completion() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        2048,
    )
    .await;
    let sender_cfg = SenderConfig {
        session: capture.session_config(0xA11C_E101, 8),
        route: capture.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_NODE_ID],
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwx"),
        manifest: LosslessSessionManifest {
            block_size: 8,
            total_bytes: 24,
            total_blocks: 3,
            mode: LosslessSessionMode::Plain,
        },
        ready_grace_ms: 500,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, capture.processors.clone()));

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    ctrl_tx
        .send(common::ready_frame(0xA11C_E101, RECEIVER_NODE_ID))
        .await
        .expect("ready frame should enqueue");

    let mut block_ids = BTreeSet::new();
    let mut saw_eot = false;
    while block_ids.len() < 3 || !saw_eot {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if let Some((_, data, _)) = lossless_session::decode_block_data(payload) {
            block_ids.insert(data.block_id);
            continue;
        }
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            saw_eot = true;
        }
    }

    assert_eq!(block_ids, BTreeSet::from([0, 1, 2]));
    for block_id in 0..3 {
        ctrl_tx
            .send(common::block_ack_frame(
                0xA11C_E101,
                RECEIVER_NODE_ID,
                block_id,
            ))
            .await
            .expect("block ack should enqueue");
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_writes_and_acks_every_block() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: 0xA11C_E102,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        fec_enabled: false,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            0xA11C_E102,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 24,
                    total_blocks: 3,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");
    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready { .. }))
    ));

    for (block_id, block) in [b"abcdefgh", b"ijklmnop", b"qrstuvwx"]
        .into_iter()
        .enumerate()
    {
        tx.send(InboundFrame {
            bytes: lossless_session::encode_block_data(0xA11C_E102, block_id as u64, block),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("block data should reach receiver");
    }

    let mut acked = BTreeSet::new();
    while acked.len() < 3 {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("ack packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("control packet should decode");
        let LosslessSessionControl::BlockAck { block_id } = control else {
            panic!("unexpected receiver control frame: {control:?}");
        };
        acked.insert(block_id);
    }
    assert_eq!(acked, BTreeSet::from([0, 1, 2]));

    drop(tx);

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");

    assert_eq!(&*sink.lock().await, b"abcdefghijklmnopqrstuvwx");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fec_sender_emits_symbols_for_every_block_before_completion() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT + 2,
        DST_PORT + 2,
        1,
        2048,
    )
    .await;
    let sender_cfg = SenderConfig {
        session: capture.session_config(0xA11C_E103, 8),
        route: capture.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_NODE_ID],
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqr"),
        manifest: LosslessSessionManifest {
            block_size: 8,
            total_bytes: 18,
            total_blocks: 3,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![1, 3])),
        },
        ready_grace_ms: 500,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, capture.processors.clone()));

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    ctrl_tx
        .send(common::ready_frame(0xA11C_E103, RECEIVER_NODE_ID))
        .await
        .expect("ready frame should enqueue");

    let mut block_ids = BTreeSet::new();
    let mut saw_eot = false;
    while block_ids.len() < 3 || !saw_eot {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if let Some((_, symbol, _)) = lossless_session::decode_block_symbol(payload) {
            block_ids.insert(symbol.block_id);
            continue;
        }
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            saw_eot = true;
        }
    }

    assert_eq!(block_ids, BTreeSet::from([0, 1, 2]));
    for block_id in 0..3 {
        ctrl_tx
            .send(common::block_ack_frame(
                0xA11C_E103,
                RECEIVER_NODE_ID,
                block_id,
            ))
            .await
            .expect("block ack should enqueue");
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fec_receiver_decodes_and_acks_every_block() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 3,
        DST_PORT + 3,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: 0xA11C_E104,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        fec_enabled: true,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            0xA11C_E104,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 18,
                    total_blocks: 3,
                    mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![1, 3],
                    )),
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");
    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready { .. }))
    ));

    let blocks = [
        b"abcdefgh".as_slice(),
        b"ijklmnop".as_slice(),
        b"qr".as_slice(),
    ];
    for (block_id, block) in blocks.into_iter().enumerate() {
        let padded = {
            let mut bytes = block.to_vec();
            bytes.resize(8, 0);
            bytes
        };
        for (symbol_id, symbol) in padded.chunks(2).enumerate() {
            tx.send(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    0xA11C_E104,
                    block_id as u64,
                    symbol_id as u32,
                    if symbol_id % 2 == 0 { 1 } else { 3 },
                    symbol,
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("symbol should reach receiver");
        }
    }

    let mut acked = BTreeSet::new();
    while acked.len() < 3 {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("ack packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("control packet should decode");
        let LosslessSessionControl::BlockAck { block_id } = control else {
            panic!("unexpected receiver control frame: {control:?}");
        };
        acked.insert(block_id);
    }
    assert_eq!(acked, BTreeSet::from([0, 1, 2]));

    drop(tx);

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");

    assert_eq!(&sink.lock().await[..18], &b"abcdefghijklmnopqr"[..]);
}
