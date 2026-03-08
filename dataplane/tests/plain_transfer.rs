mod common;

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
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

const SOURCE_NODE_ID: usize = 11;
const RECEIVER_NODE_ID: usize = 12;
const SESSION_ID: u64 = 0xA11C_E001;
const SRC_PORT: u16 = 4700;
const DST_PORT: u16 = 5700;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_acks_completed_block_and_writes_sink() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        common: capture.common_config(SESSION_ID, 16),
        source_node_id: SOURCE_NODE_ID,
        expected_bytes: 16,
        sink_buffer: Some(sink.clone()),
        fec_enabled: false,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
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
    assert_eq!(
        lossless_session::decode_control(ready_payload),
        Some((
            lossless_session::LosslessSessionHeader::decode_from(ready_payload)
                .expect("ready control should decode")
                .0,
            LosslessSessionControl::Ready {
                node_id: RECEIVER_NODE_ID as u64,
            },
        ))
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");

    let ack_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ack_payload = ack_packet
        .tcp_payload()
        .expect("ack packet should include payload");
    let (_, ack_control) =
        lossless_session::decode_control(ack_payload).expect("ack control should decode");
    assert_eq!(
        ack_control,
        LosslessSessionControl::BlockAck { block_id: 0 }
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(SESSION_ID, &LosslessSessionControl::Eot),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("eot should reach receiver");
    drop(tx);

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");

    assert_eq!(&*sink.lock().await, b"abcdefghijklmnop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_sender_completes_after_block_ack() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;
    let sender_cfg = SenderConfig {
        common: capture.common_config(SESSION_ID + 1, 16),
        receiver_ids: vec![RECEIVER_NODE_ID],
        total_bytes: 16,
        source_buffer: Bytes::from_static(b"qrstuvwxyzabcdef"),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
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
        .send(common::ready_frame(SESSION_ID + 1, RECEIVER_NODE_ID))
        .await
        .expect("ready frame should enqueue");

    let mut saw_block_data = false;
    let mut saw_eot = false;
    while !saw_block_data || !saw_eot {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if let Some((_, data, body)) = lossless_session::decode_block_data(payload) {
            assert_eq!(data.block_id, 0);
            assert_eq!(body, b"qrstuvwxyzabcdef");
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            saw_eot = true;
        }
    }

    ctrl_tx
        .send(common::block_ack_frame(SESSION_ID + 1, RECEIVER_NODE_ID, 0))
        .await
        .expect("block ack should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}
