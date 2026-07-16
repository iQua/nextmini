mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::api::{InboundFrame, SessionOutcome};
use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, BlockAck, FecFeedbackMode, LosslessSessionControl, LosslessSessionFecMode,
    LosslessSessionManifest, LosslessSessionMode,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn carousel_sender_finishes_only_after_cumulative_ack_and_repeats_completion() {
    let mut harness = common::packet_capture(1, 2, 4120, 5230, 1, 128).await;
    let session_id = 0xCA40_0001;
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(
            LosslessSessionFecMode::new_raptorq(4, vec![7, 9])
                .with_feedback_mode(FecFeedbackMode::Carousel),
        ),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2],
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest,
        ready_grace_ms: 200,
        peer_report_timeout_ms: 200,
        topology_ready: None,
        cloudcast: None,
    };
    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .expect("Ready should enqueue");
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let mut saw_symbol = false;
    while !saw_symbol {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if lossless_session::decode_block_symbol(payload).is_some() {
            saw_symbol = true;
        }
    }

    ctrl_tx
        .send(block_ack_frame(
            session_id,
            2,
            BlockAck::Blocks {
                completed_watermark: 1,
                extra_completed: Vec::new(),
            },
        ))
        .await
        .expect("final BlockAck should enqueue");

    let mut completion_count = 0;
    while completion_count < 3 {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if matches!(
            lossless_session::decode_control(payload),
            Some((_, LosslessSessionControl::SessionComplete))
        ) {
            completion_count += 1;
        }
    }

    assert_eq!(
        timeout(Duration::from_secs(2), sender_task)
            .await
            .expect("carousel sender should finish")
            .expect("carousel sender task should not panic"),
        SessionOutcome::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_carousel_probes_the_missing_peer_before_success() {
    let mut harness = common::packet_capture(1, 2, 4121, 5231, 1, 128).await;
    let session_id = 0xCA40_0002;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2],
        source_buffer: Bytes::new(),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 0,
            total_blocks: 0,
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_raptorq(4, vec![7])
                    .with_feedback_mode(FecFeedbackMode::Carousel),
            ),
        },
        ready_grace_ms: 200,
        peer_report_timeout_ms: 200,
        topology_ready: None,
        cloudcast: None,
    };
    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .expect("Ready should enqueue");
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    loop {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if let Some((_, LosslessSessionControl::AckProbe { target_peer_id })) =
            lossless_session::decode_control(payload)
        {
            assert_eq!(target_peer_id, 2);
            break;
        }
    }

    ctrl_tx
        .send(block_ack_frame(
            session_id,
            2,
            BlockAck::Blocks {
                completed_watermark: 0,
                extra_completed: Vec::new(),
            },
        ))
        .await
        .expect("empty-object BlockAck should enqueue");
    assert_eq!(
        timeout(Duration::from_secs(2), sender_task)
            .await
            .expect("empty carousel sender should finish")
            .expect("empty carousel sender task should not panic"),
        SessionOutcome::Completed
    );
}

fn block_ack_frame(session_id: u64, peer_id: usize, ack: BlockAck) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_control(
            session_id,
            &LosslessSessionControl::BlockAck { ack },
        ),
        peer_id: Some(peer_id),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn carousel_empty_frozen_quorum_is_trivial_success() {
    let harness = common::packet_capture(1, 2, 4122, 5232, 1, 128).await;
    let session_id = 0xCA40_0003;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: Vec::new(),
        source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(
                LosslessSessionFecMode::new_raptorq(4, vec![7])
                    .with_feedback_mode(FecFeedbackMode::Carousel),
            ),
        },
        ready_grace_ms: 200,
        peer_report_timeout_ms: 200,
        topology_ready: None,
        cloudcast: None,
    };
    let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);

    assert_eq!(
        timeout(
            Duration::from_secs(1),
            sender::run(sender_cfg, ctrl_rx, harness.processors),
        )
        .await
        .expect("empty-quorum sender should not wait"),
        SessionOutcome::Completed
    );
}
