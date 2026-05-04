mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::time::timeout;

use nextmini::node::session::api::{LosslessRuntimeHandle, SessionOutcome, StartError};
use nextmini::node::session::runtime::ReceiverRequest;
use nextmini::node::session::runtime::SenderRequest;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, MissingBlockRange, NeedReport,
};

const SOURCE_NODE_ID: usize = 41;
const RECEIVER_NODE_ID: usize = 42;
const RECEIVER_B_NODE_ID: usize = 43;
const SRC_PORT: u16 = 4730;
const DST_PORT: u16 = 5730;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_waits_for_topology_ready_before_starting_handshake() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = false;
    runtime_cfg.ready_grace_ms = 300;
    let peer_report_timeout_ms = runtime_cfg.peer_report_timeout_ms;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);

    let session_id = 0xA11C_E301;
    let mut session = runtime
        .start_sender(SenderRequest {
            session: capture.session_config(session_id, 16),
            route: capture.route(),
            pacing: None,
            receiver_ids: vec![RECEIVER_NODE_ID],
            total_bytes: 16,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms: 300,
            peer_report_timeout_ms,
        })
        .await
        .expect("sender should start");

    assert!(
        timeout(Duration::from_millis(100), capture.packet_rx.recv())
            .await
            .is_err(),
        "sender should stay completely quiet while topology is not ready"
    );

    runtime.set_topology_ready(true).await;

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    runtime
        .deliver(
            session_id,
            common::ready_frame(session_id, RECEIVER_NODE_ID),
        )
        .await;

    let mut saw_block_data = false;
    let mut saw_source_done = false;
    while !saw_block_data || !saw_source_done {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { .. })) =
            lossless_session::decode_control(payload)
        {
            saw_source_done = true;
        }
    }

    runtime
        .deliver(
            session_id,
            common::plain_status_frame(session_id, RECEIVER_NODE_ID, 0, NeedReport::Complete),
        )
        .await;
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("sender wait should not time out"),
        SessionOutcome::Completed,
        "sender should complete once topology is ready and the receiver reports complete"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_opens_data_gate_after_ready_grace_without_ready() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = false;
    runtime_cfg.ready_grace_ms = 120;
    let peer_report_timeout_ms = runtime_cfg.peer_report_timeout_ms;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true).await;

    let session_id = 0xA11C_E302;
    let mut session = runtime
        .start_sender(SenderRequest {
            session: capture.session_config(session_id, 16),
            route: capture.route(),
            pacing: None,
            receiver_ids: vec![RECEIVER_NODE_ID],
            total_bytes: 16,
            source_buffer: Bytes::from_static(b"qrstuvwxyzabcdef"),
            ready_grace_ms: 120,
            peer_report_timeout_ms,
        })
        .await
        .expect("sender should start");

    let first_packet = common::recv_packet(&mut capture.packet_rx).await;
    let first_payload = first_packet
        .tcp_payload()
        .expect("first packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(first_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    let quiet_deadline = Instant::now() + Duration::from_millis(60);
    while Instant::now() < quiet_deadline {
        if let Ok(Some(packet)) = timeout(Duration::from_millis(20), capture.packet_rx.recv()).await
        {
            let payload = packet
                .tcp_payload()
                .expect("captured packet should include payload");
            assert!(
                lossless_session::decode_block_data(payload).is_none(),
                "sender must not open the data gate before ready_grace expires"
            );
        }
    }

    let mut saw_block_data = false;
    let mut saw_source_done = false;
    while !saw_block_data || !saw_source_done {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { .. })) =
            lossless_session::decode_control(payload)
        {
            saw_source_done = true;
        }
    }

    runtime
        .deliver(
            session_id,
            common::plain_status_frame(session_id, RECEIVER_NODE_ID, 0, NeedReport::Complete),
        )
        .await;
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("sender wait should not time out"),
        SessionOutcome::Completed,
        "sender should still complete once the receiver reports complete after grace expiry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_receiver_rejects_duplicate_active_session_ids() {
    let capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 2,
        DST_PORT + 2,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.peer_report_timeout_ms = 200;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xA11C_E303;

    let mut first = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await
        .expect("first receiver should start");

    let second = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await;

    assert!(
        matches!(
            second,
            Err(StartError::SessionAlreadyActive {
                session_id: duplicate_sid
            }) if duplicate_sid == session_id
        ),
        "runtime should reject duplicate active session IDs instead of overwriting them"
    );

    first.abort();
    assert_eq!(first.wait().await, SessionOutcome::Aborted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_receiver_replays_complete_on_late_eot() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 3,
        DST_PORT + 3,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.peer_report_timeout_ms = 200;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xA11C_E304;

    let mut session = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await
        .expect("receiver should start");

    runtime
        .deliver(
            session_id,
            common::manifest_frame(session_id, SOURCE_NODE_ID, 16, 16, 1),
        )
        .await;
    assert_ready(&mut capture).await;

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert_plain_complete(&mut capture).await;
    assert_eq!(session.wait().await, SessionOutcome::Completed);

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert_plain_complete(&mut capture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passive_complete_receiver_answers_later_round_before_handoff_then_runtime_replays_only_the_cached_round_after_finish()
 {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 6,
        DST_PORT + 6,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.peer_report_timeout_ms = 200;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xA11C_E307;

    let mut session = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await
        .expect("receiver should start");

    runtime
        .deliver(
            session_id,
            common::manifest_frame(session_id, SOURCE_NODE_ID, 16, 16, 1),
        )
        .await;
    assert_ready(&mut capture).await;

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert_plain_complete_round(&mut capture, 0).await;

    assert!(
        timeout(Duration::from_millis(100), session.wait())
            .await
            .is_err(),
        "receiver should remain live in passive-complete state before runtime handoff"
    );

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    assert_plain_complete_round(&mut capture, 1).await;

    assert_eq!(
        timeout(Duration::from_secs(3), session.wait())
            .await
            .expect("receiver should eventually finish and hand off replay"),
        SessionOutcome::Completed
    );

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    assert_plain_complete_round(&mut capture, 1).await;

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "runtime replay must drop stale SourceDone after handoff"
    );

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 2),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "runtime replay must not synthesize future-round Need after handoff"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_receiver_does_not_replay_complete_on_late_duplicate_block_data() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 4,
        DST_PORT + 4,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.peer_report_timeout_ms = 200;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xA11C_E305;

    let mut session = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await
        .expect("receiver should start");

    runtime
        .deliver(
            session_id,
            common::manifest_frame(session_id, SOURCE_NODE_ID, 16, 16, 1),
        )
        .await;
    assert_ready(&mut capture).await;

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert_plain_complete(&mut capture).await;
    assert_eq!(session.wait().await, SessionOutcome::Completed);

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "completed receiver replay should only trigger on duplicate SourceDone after T4"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passive_complete_receiver_ignores_duplicate_payload_after_later_round_begins() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 7,
        DST_PORT + 7,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.peer_report_timeout_ms = 200;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xA11C_E308;

    let mut session = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: None,
            progress: None,
        })
        .await
        .expect("receiver should start");

    runtime
        .deliver(
            session_id,
            common::manifest_frame(session_id, SOURCE_NODE_ID, 16, 16, 1),
        )
        .await;
    assert_ready(&mut capture).await;

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 0),
        )
        .await;
    assert_plain_complete_round(&mut capture, 0).await;

    runtime
        .deliver(
            session_id,
            common::source_done_frame(session_id, SOURCE_NODE_ID, 1),
        )
        .await;
    assert_plain_complete_round(&mut capture, 1).await;

    runtime
        .deliver(
            session_id,
            common::block_data_frame(session_id, SOURCE_NODE_ID, 0, b"abcdefghijklmnop"),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "duplicate payload must not trigger wrong-round replay after a later round begins"
    );

    assert_eq!(
        timeout(Duration::from_secs(3), session.wait())
            .await
            .expect("receiver should eventually finish"),
        SessionOutcome::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "covered by multi_receiver.rs; runtime-ready scope stays on topology and replay"]
async fn sender_converges_across_plain_multireceiver_retransmit_round() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT + 5,
        DST_PORT + 5,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = false;
    runtime_cfg.ready_grace_ms = 300;
    let peer_report_timeout_ms = runtime_cfg.peer_report_timeout_ms;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true).await;

    let session_id = 0xA11C_E306;
    let mut session = runtime
        .start_sender(SenderRequest {
            session: capture.session_config(session_id, 16),
            route: capture.route(),
            pacing: None,
            receiver_ids: vec![RECEIVER_NODE_ID, RECEIVER_B_NODE_ID],
            total_bytes: 32,
            source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456"),
            ready_grace_ms: 300,
            peer_report_timeout_ms,
        })
        .await
        .expect("sender should start");

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    runtime
        .deliver(
            session_id,
            common::ready_frame(session_id, RECEIVER_NODE_ID),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::ready_frame(session_id, RECEIVER_B_NODE_ID),
        )
        .await;

    let mut saw_first_round_blocks = 0;
    let mut saw_first_round_source_done = false;
    while saw_first_round_blocks < 2 || !saw_first_round_source_done {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_first_round_blocks += 1;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { .. })) =
            lossless_session::decode_control(payload)
        {
            saw_first_round_source_done = true;
        }
    }

    runtime
        .deliver(
            session_id,
            common::plain_status_frame(session_id, RECEIVER_NODE_ID, 0, NeedReport::Complete),
        )
        .await;
    runtime
        .deliver(
            session_id,
            common::plain_status_frame(
                session_id,
                RECEIVER_B_NODE_ID,
                0,
                NeedReport::Plain {
                    ranges: vec![MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    }],
                },
            ),
        )
        .await;

    let mut saw_retransmit_block = false;
    let mut saw_second_source_done = false;
    while !saw_retransmit_block || !saw_second_source_done {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if let Some((_, data, _)) = lossless_session::decode_block_data(payload) {
            assert_eq!(
                data.block_id, 1,
                "sender should only retransmit the missing block"
            );
            saw_retransmit_block = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { .. })) =
            lossless_session::decode_control(payload)
        {
            saw_second_source_done = true;
            continue;
        }
    }

    assert!(
        timeout(Duration::from_millis(100), session.wait())
            .await
            .is_err(),
        "sender must stay active until every receiver reports complete"
    );

    runtime
        .deliver(
            session_id,
            common::plain_status_frame(session_id, RECEIVER_NODE_ID, 1, NeedReport::Complete),
        )
        .await;
    assert!(
        timeout(Duration::from_millis(100), session.wait())
            .await
            .is_err(),
        "sender must keep waiting until the second receiver reports for the same round"
    );

    runtime
        .deliver(
            session_id,
            common::plain_status_frame(session_id, RECEIVER_B_NODE_ID, 1, NeedReport::Complete),
        )
        .await;
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("sender wait should not time out"),
        SessionOutcome::Completed,
        "sender should complete once the missing receiver reports complete after retransmit"
    );
}

async fn assert_ready(capture: &mut common::PacketCaptureHarness) {
    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    let (hdr, control) =
        lossless_session::decode_control(ready_payload).expect("ready control should decode");
    assert_eq!(
        hdr.body_len, 0,
        "Ready should no longer carry an in-band node id"
    );
    assert!(matches!(control, LosslessSessionControl::Ready));
}

async fn assert_plain_complete(capture: &mut common::PacketCaptureHarness) {
    assert_plain_complete_round(capture, 0).await;
}

async fn assert_plain_complete_round(
    capture: &mut common::PacketCaptureHarness,
    expected_round_id: u32,
) {
    let packet = common::recv_packet(&mut capture.packet_rx).await;
    let payload = packet
        .tcp_payload()
        .expect("plain status packet should include payload");
    let (_, control) =
        lossless_session::decode_control(payload).expect("plain status should decode");
    assert_eq!(
        control,
        LosslessSessionControl::Need {
            round_id: expected_round_id,
            report: NeedReport::Complete,
        }
    );
}
