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
    LosslessSessionManifest, LosslessSessionMode, MettleObjectStreamGeometry,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_first_checkpoint_recovers_on_cadence_retransmission() {
    let mut harness = common::packet_capture(1, 2, 4130, 5240, 1, 256).await;
    let session_id = 0x4D45_5454_1E02;
    let geometry = MettleObjectStreamGeometry::new(2, 4, 1, 4);
    let manifest = LosslessSessionManifest {
        block_size: 8,
        total_bytes: 8,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(
            LosslessSessionFecMode::new_mettle(4, vec![7])
                .with_feedback_mode(FecFeedbackMode::Carousel)
                .with_mettle_object_stream(geometry),
        ),
    };
    manifest.validate().expect("valid METTLE carousel manifest");
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 8),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2],
        source_buffer: Bytes::from_static(b"abcdefgh"),
        manifest,
        ready_grace_ms: 20,
        peer_report_timeout_ms: 3_000,
        topology_ready: None,
        cloudcast: None,
    };
    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .expect("Ready should enqueue");
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let first_checkpoint = loop {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        let payload = packet.tcp_payload().expect("captured packet has payload");
        if let Some((
            _,
            LosslessSessionControl::DepartureCheckpoint {
                stream_id,
                repair_epoch,
                departure_bin_exclusive,
            },
        )) = lossless_session::decode_control(payload)
        {
            break (stream_id, repair_epoch, departure_bin_exclusive);
        }
    };
    assert_eq!(first_checkpoint.0, 0);
    assert!(first_checkpoint.2 > 0);
    // Intentionally drop the first checkpoint: no feedback is delivered.

    let retransmitted_checkpoint = timeout(Duration::from_secs(2), async {
        loop {
            let packet = common::recv_packet(&mut harness.packet_rx).await;
            let payload = packet.tcp_payload().expect("captured packet has payload");
            if let Some((
                _,
                LosslessSessionControl::DepartureCheckpoint {
                    stream_id,
                    repair_epoch,
                    departure_bin_exclusive,
                },
            )) = lossless_session::decode_control(payload)
            {
                break (stream_id, repair_epoch, departure_bin_exclusive);
            }
        }
    })
    .await
    .expect("checkpoint cadence must recover the dropped first copy");
    assert_eq!(retransmitted_checkpoint, first_checkpoint);

    ctrl_tx
        .send(block_ack_frame(
            session_id,
            2,
            BlockAck::MettleStream {
                stream_id: 0,
                decoded_source_watermark: geometry.final_stream_source_symbols(),
                stalled: None,
            },
        ))
        .await
        .expect("final METTLE BlockAck should enqueue");

    assert_eq!(
        timeout(Duration::from_secs(2), sender_task)
            .await
            .expect("sender should finish after retransmitted-checkpoint feedback")
            .expect("sender task should not panic"),
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
