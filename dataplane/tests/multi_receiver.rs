mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::api::SessionOutcome;
use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode, MissingBlockRange,
    NeedReport,
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
    let mut saw_source_done = false;
    let mut block_ids = BTreeSet::new();

    while block_ids.len() < 2 || !saw_source_done {
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
                LosslessSessionControl::SourceDone { .. } => saw_source_done = true,
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
            0,
            NeedReport::Plain {
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
            0,
            NeedReport::Plain {
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
            0,
            NeedReport::Plain {
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
                LosslessSessionControl::SourceDone { .. } => saw_second_eot = true,
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
            0,
            NeedReport::Complete,
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
            0,
            NeedReport::Complete,
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
                LosslessSessionControl::SourceDone { .. } => saw_first_eot = true,
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
                0,
                NeedReport::Plain {
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
                LosslessSessionControl::SourceDone { .. } => saw_second_eot = true,
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
            1,
            NeedReport::Complete,
        ))
        .await
        .expect("receiver A complete should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            receiver_c,
            1,
            NeedReport::Complete,
        ))
        .await
        .expect("receiver C complete should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            1,
            NeedReport::Plain {
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
    while second_retransmit_block_ids.is_empty() || !saw_third_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::SourceDone { .. } => saw_third_eot = true,
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
                1,
                NeedReport::Complete,
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
            1,
            NeedReport::Complete,
        ))
        .await
        .expect("receiver B complete should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_starts_plain_retransmit_after_first_useful_need() {
    let mut harness = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_A,
        SRC_PORT + 10,
        DST_PORT + 10,
        1,
        2048,
    )
    .await;

    let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456");
    let session_id = SESSION_ID + 10;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
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
    let mut saw_source_done = false;
    let mut block_ids = BTreeSet::new();
    while block_ids.len() < 2 || !saw_source_done {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    saw_manifest = true;
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_A))
                        .await
                        .expect("receiver A ready should enqueue");
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_B))
                        .await
                        .expect("receiver B ready should enqueue");
                }
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    saw_source_done = true;
                }
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
            session_id,
            RECEIVER_A,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("receiver A missing report should enqueue");

    let retransmit_packet = common::recv_packet(&mut harness.packet_rx).await;
    let retransmit_payload = retransmit_packet
        .tcp_payload()
        .expect("captured packet should include a TCP payload");
    let (_, data, _) =
        lossless_session::decode_block_data(retransmit_payload).expect("expected retransmit");
    assert_eq!(
        data.block_id, 1,
        "plain sender should retransmit as soon as the first useful Need arrives"
    );

    assert!(
        timeout(Duration::from_millis(100), harness.packet_rx.recv())
            .await
            .is_err(),
        "sender must not open the next feedback round before the remaining quorum peer reports"
    );

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            0,
            NeedReport::Complete,
        ))
        .await
        .expect("receiver B complete report should enqueue");

    let next_round_packet = common::recv_packet(&mut harness.packet_rx).await;
    let next_round_payload = next_round_packet
        .tcp_payload()
        .expect("captured packet should include a TCP payload");
    let (_, next_round_control) =
        lossless_session::decode_control(next_round_payload).expect("expected next-round marker");
    assert_eq!(
        next_round_control,
        LosslessSessionControl::SourceDone { round_id: 1 }
    );

    for peer_id in [RECEIVER_A, RECEIVER_B] {
        ctrl_tx
            .send(common::plain_status_frame(
                session_id,
                peer_id,
                1,
                NeedReport::Complete,
            ))
            .await
            .expect("complete report should enqueue");
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_resumes_same_round_plain_retransmit_when_late_need_arrives() {
    let receiver_c = RECEIVER_B + 1;
    let mut harness = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_A,
        SRC_PORT + 11,
        DST_PORT + 11,
        1,
        2048,
    )
    .await;

    let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456");
    let session_id = SESSION_ID + 11;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B, receiver_c],
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

    let mut saw_source_done = false;
    let mut block_ids = BTreeSet::new();
    while block_ids.len() < 2 || !saw_source_done {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    for peer_id in [RECEIVER_A, RECEIVER_B, receiver_c] {
                        ctrl_tx
                            .send(common::ready_frame(session_id, peer_id))
                            .await
                            .expect("ready frame should enqueue");
                    }
                }
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    saw_source_done = true;
                }
                other => panic!("unexpected control frame: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected plain block data");
        block_ids.insert(data.block_id);
    }

    assert_eq!(block_ids, BTreeSet::from([0, 1]));

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_A,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 0,
                    end_block_id: 1,
                }],
            },
        ))
        .await
        .expect("receiver A missing report should enqueue");

    let first_retransmit = common::recv_packet(&mut harness.packet_rx).await;
    let first_retransmit_payload = first_retransmit
        .tcp_payload()
        .expect("captured packet should include a TCP payload");
    let (_, first_data, _) =
        lossless_session::decode_block_data(first_retransmit_payload).expect("expected retransmit");
    assert_eq!(first_data.block_id, 0);

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            0,
            NeedReport::Complete,
        ))
        .await
        .expect("receiver B complete report should enqueue");

    assert!(
        timeout(Duration::from_millis(100), harness.packet_rx.recv())
            .await
            .is_err(),
        "sender must keep round 0 open instead of opening round 1 before the final peer reports"
    );

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            receiver_c,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("late receiver C missing report should enqueue");

    let mut saw_second_retransmit = false;
    let mut saw_round_one_source_done = false;
    while !saw_second_retransmit || !saw_round_one_source_done {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 1);
                    saw_round_one_source_done = true;
                }
                other => panic!("unexpected control frame after late need: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected retransmitted block");
        assert_eq!(data.block_id, 1);
        saw_second_retransmit = true;
    }

    for peer_id in [RECEIVER_A, RECEIVER_B, receiver_c] {
        ctrl_tx
            .send(common::plain_status_frame(
                session_id,
                peer_id,
                1,
                NeedReport::Complete,
            ))
            .await
            .expect("complete report should enqueue");
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_ignores_future_and_stale_plain_need_rounds() {
    let mut harness = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_A,
        SRC_PORT + 12,
        DST_PORT + 12,
        1,
        2048,
    )
    .await;

    let session_id = SESSION_ID + 12;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B],
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456"),
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

    let mut saw_source_done = false;
    let mut block_ids = BTreeSet::new();
    while block_ids.len() < 2 || !saw_source_done {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_A))
                        .await
                        .expect("receiver A ready should enqueue");
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_B))
                        .await
                        .expect("receiver B ready should enqueue");
                }
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    saw_source_done = true;
                }
                other => panic!("unexpected control frame: {other:?}"),
            }
            continue;
        }

        let (_, data, _) =
            lossless_session::decode_block_data(payload).expect("expected plain block data");
        block_ids.insert(data.block_id);
    }

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_A,
            1,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("future-round Need should enqueue");

    assert!(
        timeout(Duration::from_millis(100), harness.packet_rx.recv())
            .await
            .is_err(),
        "future-round Need must not trigger retransmit while round 0 is still open"
    );

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_A,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("current-round Need should enqueue");

    let retransmit = common::recv_packet(&mut harness.packet_rx).await;
    let retransmit_payload = retransmit
        .tcp_payload()
        .expect("captured packet should include a TCP payload");
    let (_, retransmit_data, _) =
        lossless_session::decode_block_data(retransmit_payload).expect("expected retransmit");
    assert_eq!(retransmit_data.block_id, 1);

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            0,
            NeedReport::Complete,
        ))
        .await
        .expect("current-round completion should enqueue");

    let next_round = common::recv_packet(&mut harness.packet_rx).await;
    let next_round_payload = next_round
        .tcp_payload()
        .expect("captured packet should include a TCP payload");
    let (_, next_round_control) =
        lossless_session::decode_control(next_round_payload).expect("expected next round marker");
    assert_eq!(
        next_round_control,
        LosslessSessionControl::SourceDone { round_id: 1 }
    );

    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_B,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 0,
                    end_block_id: 1,
                }],
            },
        ))
        .await
        .expect("stale closed-round Need should enqueue");

    assert!(
        timeout(Duration::from_millis(100), harness.packet_rx.recv())
            .await
            .is_err(),
        "stale closed-round Need must be ignored after advancing"
    );

    for peer_id in [RECEIVER_A, RECEIVER_B] {
        ctrl_tx
            .send(common::plain_status_frame(
                session_id,
                peer_id,
                1,
                NeedReport::Complete,
            ))
            .await
            .expect("round 1 completion should enqueue");
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_aborts_on_changed_same_round_plain_need_from_one_peer() {
    let mut harness = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_A,
        SRC_PORT + 2,
        DST_PORT + 2,
        1,
        2048,
    )
    .await;

    let sender_cfg = SenderConfig {
        session: harness.session_config(SESSION_ID + 2, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B],
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456"),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 32,
            total_blocks: 2,
            mode: LosslessSessionMode::Plain,
        },
        ready_grace_ms: 500,
        topology_ready: None,
    };

    let session_id = SESSION_ID + 2;
    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let mut saw_manifest = false;
    let mut saw_source_done = false;
    let mut block_ids = BTreeSet::new();
    while block_ids.len() < 2 || !saw_source_done {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include a TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {
                    saw_manifest = true;
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_A))
                        .await
                        .expect("receiver A ready should enqueue");
                    ctrl_tx
                        .send(common::ready_frame(session_id, RECEIVER_B))
                        .await
                        .expect("receiver B ready should enqueue");
                }
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    saw_source_done = true;
                }
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
            session_id,
            RECEIVER_A,
            0,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            },
        ))
        .await
        .expect("initial receiver A need should enqueue");
    ctrl_tx
        .send(common::plain_status_frame(
            session_id,
            RECEIVER_A,
            0,
            NeedReport::Complete,
        ))
        .await
        .expect("changed duplicate receiver A need should enqueue");

    assert_eq!(
        timeout(Duration::from_secs(5), sender_task)
            .await
            .expect("sender task timed out")
            .expect("sender task failed"),
        SessionOutcome::Aborted,
        "sender must abort when one peer changes its same-round Need snapshot"
    );
}
