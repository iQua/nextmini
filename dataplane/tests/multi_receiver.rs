mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode, MissingBlockRange,
    PlainStatus,
};

const SOURCE_NODE_ID: usize = 31;
const RECEIVER_A: usize = 32;
const RECEIVER_B: usize = 33;
const SESSION_ID: u64 = 0xA11C_E201;
const SRC_PORT: u16 = 4720;
const DST_PORT: u16 = 5720;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_completes_only_after_every_receiver_reports_complete() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_A, SRC_PORT, DST_PORT, 1, 2048).await;

    let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456");
    let sender_cfg = SenderConfig {
        session: harness.session_config(SESSION_ID, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B],
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
        .send(common::plain_status_frame(
            SESSION_ID,
            RECEIVER_A,
            PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("receiver A missing report should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            SESSION_ID,
            RECEIVER_A,
            PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("duplicate receiver A missing report should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            SESSION_ID,
            RECEIVER_B,
            PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 0,
                    end_block_id: 1,
                }],
            },
        ))
        .await
        .expect("receiver B missing report should enqueue");

    let mut retransmit_block_ids = BTreeSet::new();
    let mut saw_second_eot = false;
    while retransmit_block_ids.len() < 2 || !saw_second_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Eot => saw_second_eot = true,
                other => panic!("unexpected control frame during retransmit round: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected retransmitted block");
        retransmit_block_ids.insert(data.block_id);
    }

    assert_eq!(retransmit_block_ids, BTreeSet::from([0, 1]));

    ctrl_tx
        .send(common::plain_status_frame(
            SESSION_ID,
            RECEIVER_A,
            PlainStatus::Complete,
        ))
        .await
        .expect("receiver A complete report should enqueue");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !sender_task.is_finished(),
        "sender must remain active until every receiver reports complete"
    );

    ctrl_tx
        .send(common::plain_status_frame(
            SESSION_ID,
            RECEIVER_B,
            PlainStatus::Complete,
        ))
        .await
        .expect("receiver B complete report should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_converges_after_staggered_multi_receiver_rounds() {
    let mut harness = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_A,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;

    let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwx");
    let sender_cfg = SenderConfig {
        session: harness.session_config(SESSION_ID + 1, 8),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B, RECEIVER_B + 1],
        source_buffer: payload,
        manifest: LosslessSessionManifest {
            block_size: 8,
            total_bytes: 24,
            total_blocks: 3,
            mode: LosslessSessionMode::Plain,
        },
        ready_grace_ms: 500,
        topology_ready: None,
    };

    let receiver_c = RECEIVER_B + 1;
    let session_id = SESSION_ID + 1;
    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let mut initial_block_ids = BTreeSet::new();
    let mut saw_manifest = false;
    let mut saw_first_eot = false;

    while initial_block_ids.len() < 3 || !saw_first_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    saw_manifest = true;
                    for peer_id in [RECEIVER_A, RECEIVER_B, receiver_c] {
                        ctrl_tx
                            .send(common::ready_frame(session_id, peer_id))
                            .await
                            .expect("ready frame should enqueue");
                    }
                }
                LosslessSessionControl::Eot => saw_first_eot = true,
                other => panic!("unexpected control frame in first round: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected plain block data");
        initial_block_ids.insert(data.block_id);
    }

    assert!(saw_manifest, "sender should advertise a manifest");
    assert_eq!(initial_block_ids, BTreeSet::from([0, 1, 2]));

    for (peer_id, block_id) in [(RECEIVER_A, 0), (RECEIVER_B, 1), (receiver_c, 2)] {
        ctrl_tx
            .send(common::plain_status_frame(
                session_id,
                peer_id,
                PlainStatus::MissingBlocks {
                    ranges: vec![MissingBlockRange {
                        start_block_id: block_id,
                        end_block_id: block_id + 1,
                    }],
                },
            ))
            .await
            .expect("missing status should enqueue");
    }

    let mut first_retransmit_block_ids = BTreeSet::new();
    let mut saw_second_eot = false;
    while first_retransmit_block_ids.len() < 3 || !saw_second_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Eot => saw_second_eot = true,
                other => panic!("unexpected control frame in retransmit round: {other:?}"),
            }
            continue;
        }

        let (_, data, _) = lossless_session::decode_block_data(payload)
            .expect("expected retransmitted plain block data");
        first_retransmit_block_ids.insert(data.block_id);
    }

    assert_eq!(first_retransmit_block_ids, BTreeSet::from([0, 1, 2]));

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_A,
            PlainStatus::Complete,
        ))
        .await
        .expect("receiver A complete should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            receiver_c,
            PlainStatus::Complete,
        ))
        .await
        .expect("receiver C complete should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            PlainStatus::MissingBlocks {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("receiver B second-round missing status should enqueue");

    let mut second_retransmit_block_ids = BTreeSet::new();
    let mut saw_third_eot = false;
    while second_retransmit_block_ids.len() < 1 || !saw_third_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Eot => saw_third_eot = true,
                other => panic!("unexpected control frame in second retransmit round: {other:?}"),
            }
            continue;
        }

        let (_, data, _) = lossless_session::decode_block_data(payload)
            .expect("expected second-round retransmitted block");
        second_retransmit_block_ids.insert(data.block_id);
    }

    assert_eq!(second_retransmit_block_ids, BTreeSet::from([1]));
    for peer_id in [RECEIVER_A, receiver_c] {
        ctrl_tx
            .send(common::plain_status_frame(
                session_id,
                peer_id,
                PlainStatus::Complete,
            ))
            .await
            .expect("complete status should enqueue");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !sender_task.is_finished(),
        "sender must remain active until every receiver reports for the final round"
    );

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            PlainStatus::Complete,
        ))
        .await
        .expect("receiver B complete should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}
