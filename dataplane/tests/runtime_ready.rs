mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::time::timeout;

use nextmini::node::session::api::{LosslessRuntimeHandle, SessionOutcome, StartError};
use nextmini::node::session::runtime::ReceiverRequest;
use nextmini::node::session::runtime::SenderRequest;
use nextmini_messages::lossless_session::{self, LosslessSessionControl};

const SOURCE_NODE_ID: usize = 41;
const RECEIVER_NODE_ID: usize = 42;
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
        })
        .await
        .expect("sender should start");

    assert!(
        timeout(Duration::from_millis(100), capture.packet_rx.recv())
            .await
            .is_err(),
        "sender should stay completely quiet while topology is not ready"
    );

    runtime.set_topology_ready(true);

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    runtime.deliver(
        session_id,
        common::ready_frame(session_id, RECEIVER_NODE_ID),
    );

    let mut saw_block_data = false;
    let mut saw_eot = false;
    while !saw_block_data || !saw_eot {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            saw_eot = true;
        }
    }

    runtime.deliver(
        session_id,
        common::block_ack_frame(session_id, RECEIVER_NODE_ID, 0),
    );
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("sender wait should not time out"),
        SessionOutcome::Completed,
        "sender should complete once topology is ready and the block is acknowledged"
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
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true);

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
    let mut saw_eot = false;
    while !saw_block_data || !saw_eot {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            saw_eot = true;
        }
    }

    runtime.deliver(
        session_id,
        common::block_ack_frame(session_id, RECEIVER_NODE_ID, 0),
    );
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("sender wait should not time out"),
        SessionOutcome::Completed,
        "sender should still complete once the block is acknowledged after grace expiry"
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
    let runtime = LosslessRuntimeHandle::new(
        capture.processors.clone(),
        capture.cfg.lossless_runtime_config.clone(),
    );
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
