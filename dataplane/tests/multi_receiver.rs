mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

const SOURCE_NODE_ID: usize = 31;
const RECEIVER_A: usize = 32;
const RECEIVER_B: usize = 33;
const SESSION_ID: u64 = 0xA11C_E201;
const SRC_PORT: u16 = 4720;
const DST_PORT: u16 = 5720;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_completes_only_after_every_receiver_acks_every_block() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_A, SRC_PORT, DST_PORT, 1, 2048).await;

    let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456");
    let sender_cfg = SenderConfig {
        session: harness.session_config(SESSION_ID, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B],
        total_bytes: payload.len() as u64,
        source_buffer: payload,
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 32,
            total_blocks: 2,
            mode: LosslessSessionMode::Plain,
        },
        ready_grace_ms: 500,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let mut saw_manifest = false;
    let mut saw_eot = false;
    let mut block_ids = BTreeSet::new();

    while block_ids.len() < 2 || !saw_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    saw_manifest = true;
                    ctrl_tx
                        .send(common::ready_frame(SESSION_ID, RECEIVER_A))
                        .await
                        .expect("receiver A ready should enqueue");
                    ctrl_tx
                        .send(common::ready_frame(SESSION_ID, RECEIVER_B))
                        .await
                        .expect("receiver B ready should enqueue");
                }
                LosslessSessionControl::Eot => saw_eot = true,
                other => panic!("unexpected control frame: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected plain block data");
        block_ids.insert(data.block_id);
    }

    assert!(saw_manifest, "sender should advertise a manifest");
    assert_eq!(block_ids, BTreeSet::from([0, 1]));

    ctrl_tx
        .send(common::block_ack_frame(SESSION_ID, RECEIVER_A, 0))
        .await
        .expect("receiver A block 0 ack should enqueue");
    ctrl_tx
        .send(common::block_ack_frame(SESSION_ID, RECEIVER_A, 1))
        .await
        .expect("receiver A block 1 ack should enqueue");
    ctrl_tx
        .send(common::block_ack_frame(SESSION_ID, RECEIVER_B, 0))
        .await
        .expect("receiver B block 0 ack should enqueue");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !sender_task.is_finished(),
        "sender must remain active until every receiver has acknowledged every block"
    );

    ctrl_tx
        .send(common::block_ack_frame(SESSION_ID, RECEIVER_B, 1))
        .await
        .expect("receiver B block 1 ack should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}
