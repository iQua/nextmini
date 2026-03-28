mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::api::SessionOutcome;
use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode, NeedBlock, NeedReport,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_prioritizes_source_symbols_before_extra_symbols() {
    let mut harness = common::packet_capture(1, 2, 4100, 5200, 1, 2048).await;

    let session_id = 0xFEC5_0001;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest: manifest.clone(),
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .expect("ready frame should enqueue");

    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let mut saw_manifest = false;
    let mut saw_source_done = false;
    let mut all_symbol_ids = Vec::new();
    let mut extra_symbol_ids = Vec::new();

    while extra_symbol_ids.len() < 2 {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        assert_eq!(packet.lossless_session_id(), Some(session_id));

        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest {
                    manifest: observed_manifest,
                } => {
                    assert_eq!(observed_manifest, manifest);
                    saw_manifest = true;
                }
                LosslessSessionControl::SourceDone { round_id } => {
                    assert_eq!(round_id, 0);
                    saw_source_done = true;
                    ctrl_tx
                        .send(fec_status_frame(
                            session_id,
                            2,
                            0,
                            NeedReport::Fec {
                                blocks: vec![NeedBlock {
                                    block_id: 0,
                                    deficit_symbols: 2,
                                }],
                            },
                        ))
                        .await
                        .expect("round status should enqueue");
                }
                other => panic!("unexpected control frame: {other:?}"),
            }
            continue;
        }

        let (_, symbol, body) =
            lossless_session::decode_block_symbol(payload).expect("expected block symbol");
        assert_eq!(packet.lossless_fec_tree_id(), Some(symbol.tree_id));
        assert_eq!(
            body.len(),
            4,
            "one 16-byte block with K=4 yields 4-byte symbols"
        );

        all_symbol_ids.push(symbol.symbol_id);
        if symbol.symbol_id >= 4 {
            assert!(
                saw_source_done,
                "extra symbols must not appear before SourceDone(0)"
            );
            extra_symbol_ids.push(symbol.symbol_id);
        }
    }

    assert!(saw_manifest, "sender should advertise its manifest");
    assert_eq!(
        &all_symbol_ids[..4],
        &[0, 1, 2, 3],
        "source symbols must be sent before any extra fountain symbols"
    );
    assert_eq!(
        extra_symbol_ids,
        vec![4, 5],
        "extra symbols should continue from the first fountain symbol id"
    );

    ctrl_tx
        .send(fec_status_frame(session_id, 2, 0, NeedReport::Complete))
        .await
        .expect("completion status should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_starts_repair_after_first_receiver_need_without_waiting_for_every_peer() {
    let mut harness = common::packet_capture(1, 2, 4102, 5202, 1, 2048).await;
    let session_id = 0xFEC5_0002;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2, 3],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .unwrap();
    ctrl_tx
        .send(common::ready_frame(session_id, 3))
        .await
        .unwrap();

    let mut sender_task =
        tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            2,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        ))
        .await
        .unwrap();

    let first_extra = recv_symbol(&mut harness.packet_rx).await;
    assert_eq!(first_extra.symbol_id, 4);

    ctrl_tx
        .send(fec_status_frame(session_id, 3, 0, NeedReport::Complete))
        .await
        .unwrap();

    wait_for_source_done(&mut harness.packet_rx, 1).await;

    ctrl_tx
        .send(fec_status_frame(session_id, 2, 1, NeedReport::Complete))
        .await
        .unwrap();
    ctrl_tx
        .send(fec_status_frame(session_id, 3, 1, NeedReport::Complete))
        .await
        .unwrap();

    timeout(Duration::from_secs(5), &mut sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_extends_repair_burst_when_late_receiver_need_arrives_after_local_exhaustion() {
    let mut harness = common::packet_capture(1, 2, 4103, 5203, 1, 2048).await;
    let session_id = 0xFEC5_0003;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2, 3],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .unwrap();
    ctrl_tx
        .send(common::ready_frame(session_id, 3))
        .await
        .unwrap();

    let mut sender_task =
        tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            2,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        ))
        .await
        .unwrap();

    let first_extra = recv_symbol(&mut harness.packet_rx).await;
    assert_eq!(first_extra.symbol_id, 4);

    assert!(
        timeout(Duration::from_millis(150), harness.packet_rx.recv())
            .await
            .is_err(),
        "sender should keep the round open after locally exhausting the first repair burst"
    );

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            3,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 3,
                }],
            },
        ))
        .await
        .unwrap();

    let mut extra_symbol_ids = Vec::new();
    while extra_symbol_ids.len() < 2 {
        extra_symbol_ids.push(recv_symbol(&mut harness.packet_rx).await.symbol_id);
    }
    assert_eq!(extra_symbol_ids, vec![5, 6]);
    wait_for_source_done(&mut harness.packet_rx, 1).await;

    ctrl_tx
        .send(fec_status_frame(session_id, 2, 1, NeedReport::Complete))
        .await
        .unwrap();
    ctrl_tx
        .send(fec_status_frame(session_id, 3, 1, NeedReport::Complete))
        .await
        .unwrap();

    timeout(Duration::from_secs(5), &mut sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_merges_same_round_need_while_repair_is_still_in_flight() {
    let mut harness = common::packet_capture(1, 2, 4106, 5206, 1, 2048).await;
    let session_id = 0xFEC5_0006;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2, 3],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .unwrap();
    ctrl_tx
        .send(common::ready_frame(session_id, 3))
        .await
        .unwrap();

    let mut sender_task =
        tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            2,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 3,
                }],
            },
        ))
        .await
        .unwrap();

    let first_extra = recv_symbol(&mut harness.packet_rx).await;
    assert_eq!(first_extra.symbol_id, 4);

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            3,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 5,
                }],
            },
        ))
        .await
        .unwrap();

    let mut extra_symbol_ids = vec![first_extra.symbol_id];
    while extra_symbol_ids.len() < 5 {
        extra_symbol_ids.push(recv_symbol(&mut harness.packet_rx).await.symbol_id);
    }
    assert_eq!(
        extra_symbol_ids,
        vec![4, 5, 6, 7, 8],
        "same-round in-flight Need should extend the current repair burst instead of being dropped"
    );

    wait_for_source_done(&mut harness.packet_rx, 1).await;

    ctrl_tx
        .send(fec_status_frame(session_id, 2, 1, NeedReport::Complete))
        .await
        .unwrap();
    ctrl_tx
        .send(fec_status_frame(session_id, 3, 1, NeedReport::Complete))
        .await
        .unwrap();

    timeout(Duration::from_secs(5), &mut sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_retransmits_source_done_while_waiting_for_silent_peer_and_times_out() {
    let mut harness = common::packet_capture(1, 2, 4105, 5205, 1, 2048).await;
    let session_id = 0xFEC5_0005;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2, 3],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .unwrap();
    ctrl_tx
        .send(common::ready_frame(session_id, 3))
        .await
        .unwrap();

    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    ctrl_tx
        .send(fec_status_frame(session_id, 2, 0, NeedReport::Complete))
        .await
        .unwrap();

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    assert_eq!(
        timeout(Duration::from_secs(5), sender_task)
            .await
            .expect("sender task timed out")
            .expect("sender task failed"),
        SessionOutcome::Aborted,
        "sender should abort after repeatedly soliciting a silent frozen peer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_aborts_on_changed_same_round_fec_need_from_one_peer() {
    let mut harness = common::packet_capture(1, 2, 4104, 5204, 1, 2048).await;
    let session_id = 0xFEC5_0004;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2, 3],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .unwrap();
    ctrl_tx
        .send(common::ready_frame(session_id, 3))
        .await
        .unwrap();

    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    wait_for_source_done(&mut harness.packet_rx, 0).await;

    ctrl_tx
        .send(fec_status_frame(
            session_id,
            2,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        ))
        .await
        .unwrap();
    ctrl_tx
        .send(fec_status_frame(session_id, 2, 0, NeedReport::Complete))
        .await
        .unwrap();

    assert_eq!(
        timeout(Duration::from_secs(5), sender_task)
            .await
            .expect("sender task timed out")
            .expect("sender task failed"),
        SessionOutcome::Aborted,
        "sender must abort when one peer changes its same-round Need snapshot"
    );
}

fn fec_status_frame(
    session_id: u64,
    peer_id: usize,
    round_id: u32,
    report: NeedReport,
) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_control(
            session_id,
            &LosslessSessionControl::Need { round_id, report },
        ),
        peer_id: Some(peer_id),
    }
}

async fn wait_for_source_done(
    packet_rx: &mut mpsc::Receiver<nextmini::node::packet::Packet>,
    expected_round_id: u32,
) {
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");
        if let Some((_, LosslessSessionControl::SourceDone { round_id })) =
            lossless_session::decode_control(payload)
        {
            assert_eq!(round_id, expected_round_id);
            return;
        }
    }
}

async fn recv_symbol(
    packet_rx: &mut mpsc::Receiver<nextmini::node::packet::Packet>,
) -> nextmini_messages::lossless_session::LosslessSessionBlockSymbol {
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");
        if let Some((_, symbol, _)) = lossless_session::decode_block_symbol(payload) {
            return symbol;
        }
    }
}
