mod common;

#[path = "../src/node/session/sender/state.rs"]
#[allow(dead_code)]
mod sender_state;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{self, timeout};

use nextmini::node::packet::Packet;
use nextmini::node::session::api::{
    InboundFrame, LosslessRuntimeHandle, LosslessSessionHandle, SessionOutcome,
};
use nextmini::node::session::metrics::{SenderWaitState, SessionMetrics, SessionMetricsSnapshot};
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{
    CarouselRuntimeConfig, ReceiverConfig, ReceiverRequest, SenderConfig, SenderRequest,
};
use nextmini::node::session::sender;
use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{
    self, BlockAck, CompletedBlockRange, FecFeedbackMode, LosslessSessionControl,
    LosslessSessionFecMode, LosslessSessionManifest, LosslessSessionMode, MAX_BLOCK_ACK_RANGES,
};

use sender_state::PeerBlockCompletion;

const SOURCE_NODE_ID: usize = 61;
const RECEIVER_NODE_ID: usize = 62;
const RECEIVER_B_NODE_ID: usize = 63;

fn short_timing() -> CarouselRuntimeConfig {
    CarouselRuntimeConfig {
        ack_debounce: Duration::from_millis(5),
        ack_heartbeat: Duration::from_millis(20),
        ack_probe_interval: Duration::from_millis(15),
        peer_silence_timeout: Duration::from_millis(250),
        peer_stall_timeout: Duration::from_millis(500),
        passive_margin: Duration::from_millis(100),
        receiver_passive_window: Duration::from_millis(750),
        session_complete_repeats: 1,
        session_complete_interval: Duration::from_millis(1),
        mettle_repair_reorder_budget: Duration::from_millis(25),
        mettle_repair_no_progress_epochs: 3,
    }
}

fn carousel_manifest(
    total_bytes: u64,
    block_size: u32,
    symbols_per_block: u32,
    tree_ids: Vec<u16>,
) -> LosslessSessionManifest {
    LosslessSessionManifest {
        block_size,
        total_bytes,
        total_blocks: total_bytes.div_ceil(u64::from(block_size)),
        mode: LosslessSessionMode::Fec(
            LosslessSessionFecMode::new_raptorq(symbols_per_block, tree_ids)
                .with_feedback_mode(FecFeedbackMode::Carousel),
        ),
    }
}

fn control_frame(session_id: u64, peer_id: usize, control: LosslessSessionControl) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_control(session_id, &control),
        peer_id: Some(peer_id),
    }
}

fn block_ack_frame(session_id: u64, peer_id: usize, completed_watermark: u64) -> InboundFrame {
    control_frame(
        session_id,
        peer_id,
        LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark,
                extra_completed: Vec::new(),
            },
        },
    )
}

fn symbol_frame(
    session_id: u64,
    peer_id: usize,
    block_id: u64,
    symbol_id: u32,
    tree_id: u16,
    payload: &[u8],
) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_block_symbol(
            session_id, block_id, symbol_id, tree_id, payload,
        ),
        peer_id: Some(peer_id),
    }
}

async fn recv_control_where(
    packet_rx: &mut mpsc::Receiver<Packet>,
    predicate: impl Fn(&LosslessSessionControl) -> bool,
) -> LosslessSessionControl {
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let Some(payload) = packet.tcp_payload() else {
            continue;
        };
        let Some((_, control)) = lossless_session::decode_control(payload) else {
            continue;
        };
        if predicate(&control) {
            return control;
        }
    }
}

async fn recv_session_control_where(
    packet_rx: &mut mpsc::Receiver<Packet>,
    session_id: u64,
    predicate: impl Fn(&LosslessSessionControl) -> bool,
) -> LosslessSessionControl {
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let Some(payload) = packet.tcp_payload() else {
            continue;
        };
        let Some((header, control)) = lossless_session::decode_control(payload) else {
            continue;
        };
        if header.session_id == session_id && predicate(&control) {
            return control;
        }
    }
}

fn sender_config(
    harness: &common::PacketCaptureHarness,
    session_id: u64,
    source: Bytes,
    receiver_ids: Vec<usize>,
    manifest: LosslessSessionManifest,
    pacing: Option<TokenBucketSpec>,
) -> SenderConfig {
    SenderConfig {
        session: harness.session_config(
            session_id,
            usize::try_from(manifest.block_size).expect("test block size fits usize"),
        ),
        route: harness.route(),
        pacing,
        receiver_ids,
        source_buffer: source,
        manifest,
        ready_grace_ms: 50,
        peer_report_timeout_ms: 500,
        topology_ready: None,
        cloudcast: None,
    }
}

async fn complete_runtime_receiver(
    runtime: &LosslessRuntimeHandle,
    packet_rx: &mut mpsc::Receiver<Packet>,
    session: &mut LosslessSessionHandle,
    session_id: u64,
) {
    runtime
        .deliver(
            session_id,
            control_frame(
                session_id,
                SOURCE_NODE_ID,
                LosslessSessionControl::Manifest {
                    manifest: carousel_manifest(16, 16, 4, vec![7]),
                },
            ),
        )
        .await;
    recv_session_control_where(packet_rx, session_id, |control| {
        matches!(control, LosslessSessionControl::Ready)
    })
    .await;

    for symbol_id in 0..4u32 {
        let start = usize::try_from(symbol_id).expect("small symbol id") * 4;
        runtime
            .deliver(
                session_id,
                symbol_frame(
                    session_id,
                    SOURCE_NODE_ID,
                    0,
                    symbol_id,
                    7,
                    &b"abcdefghijklmnop"[start..start + 4],
                ),
            )
            .await;
    }
    recv_session_control_where(packet_rx, session_id, |control| {
        matches!(
            control,
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    ..
                }
            }
        )
    })
    .await;
    runtime
        .deliver(
            session_id,
            control_frame(
                session_id,
                SOURCE_NODE_ID,
                LosslessSessionControl::SessionComplete,
            ),
        )
        .await;
    assert_eq!(
        timeout(Duration::from_secs(1), session.wait())
            .await
            .expect("runtime receiver should finish after SessionComplete"),
        SessionOutcome::Completed
    );
}

#[test]
fn ack_join_is_permutation_duplicate_and_reorder_invariant() {
    let snapshots = vec![
        BlockAck::Blocks {
            completed_watermark: 1,
            extra_completed: vec![CompletedBlockRange {
                start_block_id: 3,
                end_block_id: 4,
            }],
        },
        BlockAck::Blocks {
            completed_watermark: 2,
            extra_completed: vec![CompletedBlockRange {
                start_block_id: 3,
                end_block_id: 6,
            }],
        },
        BlockAck::Blocks {
            completed_watermark: 6,
            extra_completed: Vec::new(),
        },
    ];

    for permutation in permutations(&snapshots) {
        let mut state = PeerBlockCompletion::default();
        for ack in &permutation {
            state.join(ack);
            state.join(ack);
        }
        for ack in snapshots.iter().rev() {
            state.join(ack);
        }
        assert_eq!(
            state.snapshot(),
            BlockAck::Blocks {
                completed_watermark: 6,
                extra_completed: Vec::new(),
            }
        );
        assert!(state.object_complete(6));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_loss_liveness_accepts_first_ack_heartbeats_and_final_ack() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_NODE_ID, 4310, 5310, 1, 128).await;
    let session_id = 0xC011_0001;
    let manifest = carousel_manifest(32, 16, 4, vec![7, 9]);
    let cfg = sender_config(
        &harness,
        session_id,
        Bytes::from_static(b"abcdefghijklmnopqrstuvwxyzABCDEF"),
        vec![RECEIVER_NODE_ID],
        manifest,
        None,
    );
    let (control_tx, control_rx) = mpsc::channel(64);
    control_tx
        .send(common::ready_frame(session_id, RECEIVER_NODE_ID))
        .await
        .expect("Ready should enqueue");
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(sender::run_observed_with_timing(
        cfg,
        control_rx,
        harness.processors.clone(),
        short_timing(),
        metrics.clone(),
    ));

    let mut first_block_sources = BTreeSet::new();
    while first_block_sources.len() < 4 {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if let Some((_, symbol, _)) = lossless_session::decode_block_symbol(payload)
            && symbol.block_id == 0
            && symbol.symbol_id < 4
        {
            first_block_sources.insert(symbol.symbol_id);
        }
    }
    control_tx
        .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 1))
        .await
        .expect("first cumulative acknowledgement should enqueue");

    let mut heartbeats = 0;
    while heartbeats < 3 {
        let control = recv_control_where(&mut harness.packet_rx, |control| {
            matches!(control, LosslessSessionControl::AckProbe { .. })
        })
        .await;
        assert_eq!(
            control,
            LosslessSessionControl::AckProbe {
                target_peer_id: u64::try_from(RECEIVER_NODE_ID).expect("peer id fits u64"),
            }
        );
        control_tx
            .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 1))
            .await
            .expect("heartbeat acknowledgement should enqueue");
        heartbeats += 1;
    }

    control_tx
        .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 2))
        .await
        .expect("final cumulative acknowledgement should enqueue");
    assert_eq!(
        timeout(Duration::from_secs(2), task)
            .await
            .expect("sender should finish after final acknowledgement")
            .expect("sender task should not panic"),
        SessionOutcome::Completed
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.queued_after_final_ack_processed, 0);
    assert!(
        snapshot
            .sender_block_esis
            .values()
            .all(|esi| esi.sequence_violations == 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_completion_recovers_through_probe_during_passive_handoff() {
    let mut harness =
        common::packet_capture(RECEIVER_NODE_ID, SOURCE_NODE_ID, 4311, 5311, 1, 128).await;
    let session_id = 0xC011_0002;
    let manifest = carousel_manifest(16, 16, 4, vec![7, 9]);
    let sink = Arc::new(Mutex::new(Vec::new()));
    let cfg = ReceiverConfig {
        session_id,
        route: harness.route(),
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: true,
        cloudcast: None,
        carousel: short_timing(),
        mettle_decoder_budget: None,
    };
    let (control_tx, control_rx) = mpsc::channel(64);
    let (data_tx, data_rx) = mpsc::channel(64);
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(receiver::run_observed(
        cfg,
        control_rx,
        data_rx,
        harness.processors.clone(),
        metrics,
    ));

    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::Manifest {
                manifest: manifest.clone(),
            },
        ))
        .await
        .expect("manifest should enqueue");
    recv_control_where(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::Ready)
    })
    .await;

    for symbol_id in 0..4u32 {
        let start = usize::try_from(symbol_id).expect("small symbol id") * 4;
        data_tx
            .send(symbol_frame(
                session_id,
                SOURCE_NODE_ID,
                0,
                symbol_id,
                if symbol_id % 2 == 0 { 7 } else { 9 },
                &b"abcdefghijklmnop"[start..start + 4],
            ))
            .await
            .expect("symbol should enqueue");
    }
    let final_ack = recv_control_where(&mut harness.packet_rx, |control| {
        matches!(
            control,
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    ..
                }
            }
        )
    })
    .await;

    // The sender's first SessionComplete is deliberately dropped. A later
    // targeted probe must recover the same final cumulative acknowledgement.
    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::AckProbe {
                target_peer_id: u64::try_from(RECEIVER_NODE_ID).expect("peer id fits u64"),
            },
        ))
        .await
        .expect("probe should enqueue");
    let replayed_ack = recv_control_where(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::BlockAck { .. })
    })
    .await;
    assert_eq!(replayed_ack, final_ack);

    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::SessionComplete,
        ))
        .await
        .expect("replacement completion should enqueue");
    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("passive receiver should finish")
            .expect("receiver task should not panic"),
        SessionOutcome::Completed
    );
    assert_eq!(sink.lock().await.as_slice(), b"abcdefghijklmnop");
}

#[tokio::test]
async fn passive_receiver_finishes_when_session_complete_is_dropped_forever() {
    let mut harness =
        common::packet_capture(RECEIVER_NODE_ID, SOURCE_NODE_ID, 4320, 5320, 1, 128).await;
    time::pause();
    let session_id = 0xC011_000B;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let timing = short_timing();
    let cfg = ReceiverConfig {
        session_id,
        route: harness.route(),
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: true,
        cloudcast: None,
        carousel: timing,
        mettle_decoder_budget: None,
    };
    let (control_tx, control_rx) = mpsc::channel(16);
    let (data_tx, data_rx) = mpsc::channel(16);
    let task = tokio::spawn(receiver::run_observed(
        cfg,
        control_rx,
        data_rx,
        harness.processors.clone(),
        Arc::new(SessionMetrics::default()),
    ));

    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::Manifest {
                manifest: carousel_manifest(16, 16, 4, vec![7]),
            },
        ))
        .await
        .expect("manifest should enqueue");
    spin_recv_control(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::Ready)
    })
    .await;
    for symbol_id in 0..4u32 {
        let start = usize::try_from(symbol_id).expect("small symbol id") * 4;
        data_tx
            .send(symbol_frame(
                session_id,
                SOURCE_NODE_ID,
                0,
                symbol_id,
                7,
                &b"abcdefghijklmnop"[start..start + 4],
            ))
            .await
            .expect("source symbol should enqueue");
    }
    for _ in 0..10_000 {
        if sink.lock().await.as_slice() == b"abcdefghijklmnop" {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sink.lock().await.as_slice(), b"abcdefghijklmnop");
    time::advance(timing.ack_debounce + Duration::from_millis(1)).await;
    spin_recv_control(&mut harness.packet_rx, |control| {
        matches!(
            control,
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    ..
                }
            }
        )
    })
    .await;

    // Never deliver SessionComplete. P5 requires the passive window itself to
    // terminate the receiver successfully.
    time::advance(timing.receiver_passive_window + Duration::from_millis(1)).await;
    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("passive receiver should terminate at its retention deadline")
            .expect("receiver task should not panic"),
        SessionOutcome::Completed
    );
    assert_eq!(sink.lock().await.as_slice(), b"abcdefghijklmnop");
    time::resume();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successor_transfers_reject_stale_payload_and_ack_incarnations() {
    let mut harness =
        common::packet_capture(RECEIVER_NODE_ID, SOURCE_NODE_ID, 4312, 5312, 1, 128).await;
    let mut runtime_cfg = harness.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_feedback_mode = FecFeedbackMode::Carousel;
    runtime_cfg.fec_default_symbols_per_block = 4;
    runtime_cfg.fec_default_tree_ids = vec![7];
    runtime_cfg.carousel_ack_debounce_ms = 5;
    runtime_cfg.carousel_ack_heartbeat_ms = 20;
    runtime_cfg.carousel_ack_probe_interval_ms = 15;
    runtime_cfg.carousel_peer_silence_timeout_ms = 250;
    runtime_cfg.carousel_peer_stall_timeout_ms = 500;
    runtime_cfg.carousel_passive_margin_ms = 100;
    runtime_cfg.carousel_receiver_passive_window_ms = 750;
    runtime_cfg.carousel_session_complete_repeats = 1;
    runtime_cfg.carousel_session_complete_interval_ms = 1;
    let runtime = LosslessRuntimeHandle::new(harness.processors.clone(), runtime_cfg);
    let first_session_id = 0xC011_F003;
    let successor_session_id = 0xC011_0003;

    // Complete one receiver incarnation on this transport slot.
    let mut first = runtime
        .start_receiver(ReceiverRequest {
            session_id: first_session_id,
            route: harness.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            sink_file: None,
            progress: None,
        })
        .await
        .expect("first receiver incarnation should start");
    complete_runtime_receiver(
        &runtime,
        &mut harness.packet_rx,
        &mut first,
        first_session_id,
    )
    .await;

    // Reuse the same runtime and transport route for a successor incarnation.
    // Frames carrying the old wire identity are routed at the live successor
    // to model delayed packets already queued below the runtime boundary.
    let successor_sink = Arc::new(Mutex::new(Vec::new()));
    let mut successor = runtime
        .start_receiver(ReceiverRequest {
            session_id: successor_session_id,
            route: harness.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: Some(successor_sink.clone()),
            sink_file: None,
            progress: None,
        })
        .await
        .expect("successor receiver incarnation should start");
    runtime
        .deliver(
            successor_session_id,
            symbol_frame(first_session_id, SOURCE_NODE_ID, 0, 0, 7, b"stale"),
        )
        .await;
    runtime
        .deliver(
            successor_session_id,
            block_ack_frame(first_session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    complete_runtime_receiver(
        &runtime,
        &mut harness.packet_rx,
        &mut successor,
        successor_session_id,
    )
    .await;
    assert_eq!(successor_sink.lock().await.as_slice(), b"abcdefghijklmnop");

    // Stale completion feedback is meaningful on a sender successor: it must
    // not finish the new transfer, while the matching acknowledgement must.
    runtime.set_topology_ready(true).await;
    let sender_session_id = 0xC011_1003;
    let mut sender_session = runtime
        .start_sender(SenderRequest {
            session: harness.session_config(sender_session_id, 16),
            route: harness.route(),
            pacing: None,
            receiver_ids: vec![SOURCE_NODE_ID],
            total_bytes: 16,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms: 50,
            peer_report_timeout_ms: 500,
        })
        .await
        .expect("sender successor should start");
    runtime
        .deliver(
            sender_session_id,
            common::ready_frame(sender_session_id, SOURCE_NODE_ID),
        )
        .await;
    loop {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if let Some((header, _, _)) = lossless_session::decode_block_symbol(payload)
            && header.session_id == sender_session_id
        {
            break;
        }
    }
    runtime
        .deliver(
            sender_session_id,
            block_ack_frame(first_session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(30), sender_session.wait())
            .await
            .is_err(),
        "stale acknowledgement must not complete the sender successor"
    );
    runtime
        .deliver(
            sender_session_id,
            block_ack_frame(sender_session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    assert_eq!(
        timeout(Duration::from_secs(1), sender_session.wait())
            .await
            .expect("matching successor ack should complete the sender"),
        SessionOutcome::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_ack_terminates_sender_while_every_tree_remains_backpressured() {
    let harness = common::packet_capture_with_output_capacity(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        4313,
        5313,
        1,
        1,
        1,
    )
    .await;
    let session_id = 0xC011_0004;
    let manifest = carousel_manifest(16, 16, 4, vec![7, 9]);
    let cfg = sender_config(
        &harness,
        session_id,
        Bytes::from_static(b"abcdefghijklmnop"),
        vec![RECEIVER_NODE_ID],
        manifest,
        None,
    );
    let (control_tx, control_rx) = mpsc::channel(32);
    control_tx
        .send(common::ready_frame(session_id, RECEIVER_NODE_ID))
        .await
        .expect("Ready should enqueue");
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(sender::run_observed_with_timing(
        cfg,
        control_rx,
        harness.processors,
        short_timing(),
        metrics.clone(),
    ));

    timeout(Duration::from_secs(2), async {
        while metrics.snapshot().carousel_backpressure_sweeps == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("undrained one-slot output should backpressure every tree");
    control_tx
        .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 1))
        .await
        .expect("final acknowledgement should enqueue while trees are blocked");

    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("sender must service control while backpressured")
            .expect("sender task should not panic"),
        SessionOutcome::Completed
    );
    let snapshot = metrics.snapshot();
    assert!(snapshot.carousel_backpressure_sweeps > 0);
    assert!(snapshot.sender_wait_states[&SenderWaitState::Backpressure].count > 0);
    assert_eq!(snapshot.queued_after_final_ack_processed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_wins_the_pacing_race_before_the_next_frame_submission() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_NODE_ID, 4314, 5314, 1, 128).await;
    let session_id = 0xC011_0005;
    let manifest = carousel_manifest(16, 16, 4, vec![7]);
    let cfg = sender_config(
        &harness,
        session_id,
        Bytes::from_static(b"abcdefghijklmnop"),
        vec![RECEIVER_NODE_ID],
        manifest,
        Some(TokenBucketSpec {
            rate: 1,
            bucket_size: 4,
        }),
    );
    let (control_tx, control_rx) = mpsc::channel(32);
    control_tx
        .send(common::ready_frame(session_id, RECEIVER_NODE_ID))
        .await
        .expect("Ready should enqueue");
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(sender::run_observed_with_timing(
        cfg,
        control_rx,
        harness.processors.clone(),
        short_timing(),
        metrics.clone(),
    ));

    loop {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if lossless_session::decode_block_symbol(payload).is_some() {
            break;
        }
    }
    control_tx
        .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 1))
        .await
        .expect("ack should interrupt pacing");

    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("sender should not wait for the pacing deadline")
            .expect("sender task should not panic"),
        SessionOutcome::Completed
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.sender_block_esis[&0].count, 1);
    assert_eq!(snapshot.sender_block_esis[&0].sequence_violations, 0);
    assert!(snapshot.sender_wait_states[&SenderWaitState::Pacing].count >= 2);
    assert_eq!(snapshot.queued_after_final_ack_processed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frozen_quorum_requires_every_peer_and_targets_only_the_missing_peer() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_NODE_ID, 4315, 5315, 1, 128).await;
    let session_id = 0xC011_0006;
    let manifest = carousel_manifest(16, 16, 4, vec![7]);
    let cfg = sender_config(
        &harness,
        session_id,
        Bytes::from_static(b"abcdefghijklmnop"),
        vec![RECEIVER_NODE_ID, RECEIVER_B_NODE_ID],
        manifest,
        None,
    );
    let (control_tx, control_rx) = mpsc::channel(64);
    for peer_id in [RECEIVER_NODE_ID, RECEIVER_B_NODE_ID] {
        control_tx
            .send(common::ready_frame(session_id, peer_id))
            .await
            .expect("Ready should enqueue");
    }
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(sender::run_observed_with_timing(
        cfg,
        control_rx,
        harness.processors.clone(),
        short_timing(),
        metrics,
    ));

    control_tx
        .send(block_ack_frame(session_id, RECEIVER_NODE_ID, 1))
        .await
        .expect("first peer completion should enqueue");
    let probe = recv_control_where(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::AckProbe { .. })
    })
    .await;
    assert_eq!(
        probe,
        LosslessSessionControl::AckProbe {
            target_peer_id: u64::try_from(RECEIVER_B_NODE_ID).expect("peer id fits u64"),
        }
    );
    assert!(
        !task.is_finished(),
        "one peer cannot complete a two-peer quorum"
    );

    control_tx
        .send(block_ack_frame(session_id, RECEIVER_B_NODE_ID, 1))
        .await
        .expect("second peer completion should enqueue");
    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("full quorum should finish")
            .expect("sender task should not panic"),
        SessionOutcome::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_frozen_quorum_is_trivial_success_without_payload_emission() {
    let harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_NODE_ID, 4316, 5316, 1, 128).await;
    let session_id = 0xC011_0007;
    let manifest = carousel_manifest(16, 16, 4, vec![7]);
    let cfg = sender_config(
        &harness,
        session_id,
        Bytes::from_static(b"abcdefghijklmnop"),
        Vec::new(),
        manifest,
        None,
    );
    let (_control_tx, control_rx) = mpsc::channel(1);
    let metrics = Arc::new(SessionMetrics::default());

    assert_eq!(
        timeout(
            Duration::from_secs(1),
            sender::run_observed_with_timing(
                cfg,
                control_rx,
                harness.processors,
                short_timing(),
                metrics.clone(),
            ),
        )
        .await
        .expect("empty quorum should not wait"),
        SessionOutcome::Completed
    );
    assert!(metrics.snapshot().sender_block_esis.is_empty());
}

#[tokio::test]
async fn receiver_ack_timer_is_fair_under_a_continuously_ready_data_inbox() {
    let mut harness =
        common::packet_capture(RECEIVER_NODE_ID, SOURCE_NODE_ID, 4317, 5317, 1, 128).await;
    time::pause();
    let session_id = 0xC011_0008;
    let manifest = carousel_manifest(16, 16, 4, vec![7]);
    let cfg = ReceiverConfig {
        session_id,
        route: harness.route(),
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: None,
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: true,
        cloudcast: None,
        carousel: short_timing(),
        mettle_decoder_budget: None,
    };
    let (control_tx, control_rx) = mpsc::channel(128);
    let (data_tx, data_rx) = mpsc::channel(4);
    let task = tokio::spawn(receiver::run_observed(
        cfg,
        control_rx,
        data_rx,
        harness.processors.clone(),
        Arc::new(SessionMetrics::default()),
    ));
    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::Manifest { manifest },
        ))
        .await
        .expect("manifest should enqueue");
    spin_recv_control(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::Ready)
    })
    .await;

    let duplicate = symbol_frame(session_id, SOURCE_NODE_ID, 0, 0, 7, b"abcd");
    let refill_count = Arc::new(AtomicUsize::new(0));
    let producer_count = refill_count.clone();
    let producer = tokio::spawn(async move {
        while data_tx.send(duplicate.clone()).await.is_ok() {
            producer_count.fetch_add(1, Ordering::Relaxed);
        }
    });
    tokio::task::yield_now().await;
    time::advance(Duration::from_millis(6)).await;
    let debounce_ack = spin_recv_control(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::BlockAck { .. })
    })
    .await;
    assert!(matches!(
        debounce_ack,
        LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark: 0,
                ..
            }
        }
    ));
    let after_debounce = refill_count.load(Ordering::Relaxed);

    time::advance(Duration::from_millis(21)).await;
    let heartbeat_ack = spin_recv_control(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::BlockAck { .. })
    })
    .await;
    assert!(matches!(
        heartbeat_ack,
        LosslessSessionControl::BlockAck {
            ack: BlockAck::Blocks {
                completed_watermark: 0,
                ..
            }
        }
    ));
    assert!(
        refill_count.load(Ordering::Relaxed) > after_debounce,
        "the data inbox must remain continuously refilled through the heartbeat window"
    );

    producer.abort();
    let _ = producer.await;
    task.abort();
    let _ = task.await;
    time::resume();
}

#[test]
fn range_scaling_is_wire_bounded_and_eventually_converges() {
    let total_blocks = 1_200u64;
    let all_islands = (0..600u64)
        .map(|index| CompletedBlockRange {
            start_block_id: index * 2,
            end_block_id: index * 2 + 1,
        })
        .collect::<Vec<_>>();
    let bounded = BlockAck::Blocks {
        completed_watermark: 0,
        extra_completed: all_islands,
    }
    .for_wire(total_blocks)
    .expect("canonical range snapshot should fit after truncation");
    let BlockAck::Blocks {
        completed_watermark,
        extra_completed,
    } = &bounded
    else {
        panic!("Stage 1 benchmark expects the block acknowledgement variant");
    };
    assert_eq!(*completed_watermark, 1);
    assert_eq!(extra_completed.len(), MAX_BLOCK_ACK_RANGES);
    assert!(
        extra_completed
            .windows(2)
            .all(|pair| { pair[0].end_block_id < pair[1].start_block_id })
    );
    let encoded = lossless_session::encode_control(
        77,
        &LosslessSessionControl::BlockAck {
            ack: bounded.clone(),
        },
    );
    assert!(encoded.len() < u16::MAX as usize);

    let mut peer = PeerBlockCompletion::default();
    peer.join(&bounded);
    assert!(!peer.object_complete(total_blocks));
    peer.join(&BlockAck::Blocks {
        completed_watermark: total_blocks,
        extra_completed: Vec::new(),
    });
    assert!(peer.object_complete(total_blocks));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_decode_is_invariant_to_delivery_order_and_tree_labels() {
    let first = run_receiver_permutation(0xC011_0009, 4318, [(0, 7), (1, 9), (2, 7), (3, 9)]).await;
    let second =
        run_receiver_permutation(0xC011_000A, 4319, [(2, 9), (0, 9), (3, 7), (1, 7)]).await;

    assert_eq!(first.0, b"abcdefghijklmnop");
    assert_eq!(second.0, first.0);
    for metrics in [first.1, second.1] {
        assert_eq!(metrics.receiver_duplicate_symbols, 1);
        assert_eq!(metrics.symbols_received_after_local_block_complete, 1);
        assert_eq!(metrics.symbols_at_decode_minus_k, BTreeMap::from([(0, 1)]));
    }
}

#[test]
fn rounds_vs_carousel_matched_seed_trace_benchmark_evidence() {
    let mut evidence = Vec::new();
    for loss_percent in [5u8, 15, 30] {
        for seed in [0x5EED_0001u64, 0x5EED_0002, 0x5EED_0003] {
            let rounds = simulate_rounds(seed, loss_percent);
            let carousel = simulate_carousel(seed, loss_percent);
            assert_eq!(rounds, simulate_rounds(seed, loss_percent));
            assert_eq!(carousel, simulate_carousel(seed, loss_percent));
            evidence.push((loss_percent, seed, rounds, carousel));
        }
    }

    for (loss, seed, rounds, carousel) in evidence {
        eprintln!(
            "BENCHMARK_EVIDENCE loss={loss}% seed={seed:#x} rounds_emitted={} rounds_ticks={} carousel_emitted={} carousel_ticks={}",
            rounds.emitted, rounds.completion_tick, carousel.emitted, carousel.completion_tick,
        );
    }
}

async fn run_receiver_permutation(
    session_id: u64,
    src_port: u16,
    order: [(u32, u16); 4],
) -> (Vec<u8>, SessionMetricsSnapshot) {
    let mut harness = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        src_port,
        src_port + 1_000,
        1,
        128,
    )
    .await;
    let manifest = carousel_manifest(16, 16, 4, vec![7, 9]);
    let sink = Arc::new(Mutex::new(Vec::new()));
    let cfg = ReceiverConfig {
        session_id,
        route: harness.route(),
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: true,
        cloudcast: None,
        carousel: short_timing(),
        mettle_decoder_budget: None,
    };
    let (control_tx, control_rx) = mpsc::channel(64);
    let (data_tx, data_rx) = mpsc::channel(64);
    let metrics = Arc::new(SessionMetrics::default());
    let task = tokio::spawn(receiver::run_observed(
        cfg,
        control_rx,
        data_rx,
        harness.processors.clone(),
        metrics.clone(),
    ));
    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::Manifest { manifest },
        ))
        .await
        .expect("manifest should enqueue");
    recv_control_where(&mut harness.packet_rx, |control| {
        matches!(control, LosslessSessionControl::Ready)
    })
    .await;

    for (index, (symbol_id, tree_id)) in order.into_iter().enumerate() {
        let start = usize::try_from(symbol_id).expect("small symbol id") * 4;
        let frame = symbol_frame(
            session_id,
            SOURCE_NODE_ID,
            0,
            symbol_id,
            tree_id,
            &b"abcdefghijklmnop"[start..start + 4],
        );
        data_tx
            .send(frame.clone())
            .await
            .expect("symbol should enqueue");
        if index == 0 {
            data_tx
                .send(frame)
                .await
                .expect("duplicate symbol should enqueue");
        }
    }
    data_tx
        .send(symbol_frame(session_id, SOURCE_NODE_ID, 0, 4, 7, b"tail"))
        .await
        .expect("post-completion tail should enqueue");

    recv_control_where(&mut harness.packet_rx, |control| {
        matches!(
            control,
            LosslessSessionControl::BlockAck {
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    ..
                }
            }
        )
    })
    .await;
    timeout(Duration::from_secs(1), async {
        while metrics
            .snapshot()
            .symbols_received_after_local_block_complete
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("tail metric should be recorded before finishing the receiver");
    control_tx
        .send(control_frame(
            session_id,
            SOURCE_NODE_ID,
            LosslessSessionControl::SessionComplete,
        ))
        .await
        .expect("completion should enqueue");
    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("receiver should finish")
            .expect("receiver task should not panic"),
        SessionOutcome::Completed
    );
    let output = sink.lock().await.clone();
    (output, metrics.snapshot())
}

async fn spin_recv_control(
    packet_rx: &mut mpsc::Receiver<Packet>,
    predicate: impl Fn(&LosslessSessionControl) -> bool,
) -> LosslessSessionControl {
    for _ in 0..10_000 {
        if let Ok(packet) = packet_rx.try_recv()
            && let Some(payload) = packet.tcp_payload()
            && let Some((_, control)) = lossless_session::decode_control(payload)
            && predicate(&control)
        {
            return control;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out spinning for carousel control frame");
}

fn permutations<T: Clone>(values: &[T]) -> Vec<Vec<T>> {
    fn visit<T: Clone>(prefix: &mut Vec<T>, remaining: &mut Vec<T>, output: &mut Vec<Vec<T>>) {
        if remaining.is_empty() {
            output.push(prefix.clone());
            return;
        }
        for index in 0..remaining.len() {
            let value = remaining.remove(index);
            prefix.push(value.clone());
            visit(prefix, remaining, output);
            prefix.pop();
            remaining.insert(index, value);
        }
    }

    let mut output = Vec::new();
    visit(&mut Vec::new(), &mut values.to_vec(), &mut output);
    output
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TraceResult {
    emitted: u64,
    completion_tick: u64,
}

const TRACE_BLOCKS: usize = 24;
const TRACE_PEERS: usize = 3;
const TRACE_K: u32 = 8;

fn simulate_rounds(seed: u64, loss_percent: u8) -> TraceResult {
    let mut delivered = [[0u32; TRACE_BLOCKS]; TRACE_PEERS];
    let mut next_esi = [0u32; TRACE_BLOCKS];
    let mut emitted = 0u64;
    let mut tick = 0u64;
    for block in 0..TRACE_BLOCKS {
        emit_trace_symbols(
            seed,
            loss_percent,
            block,
            TRACE_K,
            &mut next_esi,
            &mut delivered,
            &mut emitted,
            &mut tick,
        );
    }
    tick += 8;

    for _ in 0..10_000 {
        if trace_complete(seed, &delivered) {
            return TraceResult {
                emitted,
                completion_tick: tick,
            };
        }
        for block in 0..TRACE_BLOCKS {
            let deficit = (0..TRACE_PEERS)
                .map(|peer| {
                    trace_required(seed, block, peer).saturating_sub(delivered[peer][block])
                })
                .max()
                .unwrap_or(0);
            if deficit > 0 {
                emit_trace_symbols(
                    seed,
                    loss_percent,
                    block,
                    deficit,
                    &mut next_esi,
                    &mut delivered,
                    &mut emitted,
                    &mut tick,
                );
            }
        }
        tick += 8;
    }
    panic!("round trace failed to converge");
}

fn simulate_carousel(seed: u64, loss_percent: u8) -> TraceResult {
    let mut delivered = [[0u32; TRACE_BLOCKS]; TRACE_PEERS];
    let mut acknowledged = [[false; TRACE_BLOCKS]; TRACE_PEERS];
    let mut next_esi = [0u32; TRACE_BLOCKS];
    let mut emitted = 0u64;
    let mut tick = 0u64;
    for block in 0..TRACE_BLOCKS {
        emit_trace_symbols(
            seed,
            loss_percent,
            block,
            TRACE_K,
            &mut next_esi,
            &mut delivered,
            &mut emitted,
            &mut tick,
        );
        join_trace_acks(seed, tick, &delivered, &mut acknowledged);
    }

    for _ in 0..100_000 {
        join_trace_acks(seed, tick, &delivered, &mut acknowledged);
        if acknowledged.iter().flatten().all(|complete| *complete) {
            return TraceResult {
                emitted,
                completion_tick: tick,
            };
        }
        for block in 0..TRACE_BLOCKS {
            if (0..TRACE_PEERS).all(|peer| acknowledged[peer][block]) {
                continue;
            }
            emit_trace_symbols(
                seed,
                loss_percent,
                block,
                1,
                &mut next_esi,
                &mut delivered,
                &mut emitted,
                &mut tick,
            );
            join_trace_acks(seed, tick, &delivered, &mut acknowledged);
        }
    }
    panic!("carousel trace failed to converge");
}

#[allow(clippy::too_many_arguments)]
fn emit_trace_symbols(
    seed: u64,
    loss_percent: u8,
    block: usize,
    count: u32,
    next_esi: &mut [u32; TRACE_BLOCKS],
    delivered: &mut [[u32; TRACE_BLOCKS]; TRACE_PEERS],
    emitted: &mut u64,
    tick: &mut u64,
) {
    for _ in 0..count {
        let esi = next_esi[block];
        next_esi[block] = esi.checked_add(1).expect("bounded trace ESI");
        *emitted += 1;
        *tick += 1;
        for (peer, peer_delivered) in delivered.iter_mut().enumerate() {
            if trace_delivery(seed, loss_percent, block, esi, peer) {
                peer_delivered[block] += 1;
            }
        }
    }
}

fn join_trace_acks(
    seed: u64,
    tick: u64,
    delivered: &[[u32; TRACE_BLOCKS]; TRACE_PEERS],
    acknowledged: &mut [[bool; TRACE_BLOCKS]; TRACE_PEERS],
) {
    if !tick.is_multiple_of(7) {
        return;
    }
    for peer in 0..TRACE_PEERS {
        if mix64(seed ^ tick ^ u64::try_from(peer).expect("peer fits")).is_multiple_of(5) {
            continue;
        }
        for block in 0..TRACE_BLOCKS {
            if delivered[peer][block] >= trace_required(seed, block, peer) {
                acknowledged[peer][block] = true;
            }
        }
    }
}

fn trace_complete(seed: u64, delivered: &[[u32; TRACE_BLOCKS]; TRACE_PEERS]) -> bool {
    (0..TRACE_PEERS).all(|peer| {
        (0..TRACE_BLOCKS).all(|block| delivered[peer][block] >= trace_required(seed, block, peer))
    })
}

fn trace_required(seed: u64, block: usize, peer: usize) -> u32 {
    TRACE_K
        + u32::try_from(
            mix64(
                seed ^ (u64::try_from(block).expect("block fits") << 16)
                    ^ u64::try_from(peer).expect("peer fits"),
            ) % 3,
        )
        .expect("overhead fits")
}

fn trace_delivery(seed: u64, loss_percent: u8, block: usize, esi: u32, peer: usize) -> bool {
    let key = seed
        ^ (u64::try_from(block).expect("block fits") << 32)
        ^ (u64::from(esi) << 4)
        ^ u64::try_from(peer).expect("peer fits");
    mix64(key) % 100 >= u64::from(loss_percent)
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
