mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::api::{InboundFrame, LosslessRuntimeHandle, SessionOutcome};
use nextmini::node::session::runtime::{ReceiverRequest, SenderConfig};
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, BlockStatus, FecStatus, LosslessSessionControl, LosslessSessionFecMode,
    LosslessSessionManifest, LosslessSessionMode,
};

const SOURCE_NODE_ID: usize = 71;
const RECEIVER_A: usize = 72;
const RECEIVER_B: usize = 73;
const RECEIVER_C: usize = 74;
const SRC_PORT: u16 = 4770;
const DST_PORT: u16 = 5770;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_converges_across_staggered_multi_receiver_fec_rounds() {
    let mut harness =
        common::packet_capture(SOURCE_NODE_ID, RECEIVER_A, SRC_PORT, DST_PORT, 1, 2048).await;
    let session_id = 0xFEC6_0001;
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 16),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_A, RECEIVER_B, RECEIVER_C],
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz123456"),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 32,
            total_blocks: 2,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
        },
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    for peer_id in [RECEIVER_A, RECEIVER_B, RECEIVER_C] {
        ctrl_tx
            .send(common::ready_frame(session_id, peer_id))
            .await
            .expect("ready frame should enqueue");
    }

    let mut sender_task =
        tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let first_round = collect_symbols_until_eot(&mut harness.packet_rx).await;
    assert_eq!(
        first_round,
        vec![
            (0, 0),
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3)
        ],
        "sender should emit every source symbol before waiting for round feedback"
    );

    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_A,
            FecStatus::MissingBlocks {
                blocks: vec![BlockStatus {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        ))
        .await
        .expect("receiver A round-one status should enqueue");
    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_B,
            FecStatus::MissingBlocks {
                blocks: vec![BlockStatus {
                    block_id: 1,
                    deficit_symbols: 2,
                }],
            },
        ))
        .await
        .expect("receiver B round-one status should enqueue");
    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_C,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver C round-one status should enqueue");

    let second_round = collect_symbols_until_eot(&mut harness.packet_rx).await;
    assert_eq!(
        second_round,
        vec![(0, 4), (1, 4), (1, 5)],
        "aggregate extra-symbol demand should stay deterministic across receivers"
    );

    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_A,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver A round-two complete should enqueue");
    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_C,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver C round-two complete should enqueue");
    assert!(
        timeout(Duration::from_millis(150), harness.packet_rx.recv())
            .await
            .is_err(),
        "sender must wait for a fresh report from every receiver in the new round"
    );

    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_B,
            FecStatus::MissingBlocks {
                blocks: vec![BlockStatus {
                    block_id: 1,
                    deficit_symbols: 1,
                }],
            },
        ))
        .await
        .expect("receiver B round-two status should enqueue");

    let third_round = collect_symbols_until_eot(&mut harness.packet_rx).await;
    assert_eq!(
        third_round,
        vec![(1, 6)],
        "sender should keep advancing the same block deterministically across rounds"
    );

    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_A,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver A final complete should enqueue");
    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_C,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver C final complete should enqueue");
    assert!(
        timeout(Duration::from_millis(150), &mut sender_task)
            .await
            .is_err(),
        "sender must remain active until the last receiver reports for the final round"
    );

    ctrl_tx
        .send(common::fec_status_frame(
            session_id,
            RECEIVER_B,
            FecStatus::Complete,
        ))
        .await
        .expect("receiver B final complete should enqueue");

    timeout(Duration::from_secs(5), &mut sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_fec_receiver_replays_complete_for_late_symbol_and_eot() {
    let mut capture = common::packet_capture(
        RECEIVER_A,
        SOURCE_NODE_ID,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;
    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    let session_id = 0xFEC6_0002;

    let mut session = runtime
        .start_receiver(ReceiverRequest {
            session_id,
            route: capture.route(),
            local_node_id: capture.cfg.node_id,
            capture_result: true,
            progress: None,
        })
        .await
        .expect("receiver should start");

    runtime.deliver(
        session_id,
        InboundFrame {
            bytes: lossless_session::encode_control(
                session_id,
                &LosslessSessionControl::Manifest {
                    manifest: LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: 8,
                        total_blocks: 1,
                        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(
                            4,
                            vec![1, 3],
                        )),
                    },
                },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        },
    );
    assert!(matches!(
        recv_control(&mut capture.packet_rx).await,
        LosslessSessionControl::Ready { .. }
    ));

    for (symbol_id, chunk) in [1u8, 2, 3, 4, 5, 6, 7, 8].chunks(2).enumerate() {
        runtime.deliver(
            session_id,
            InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    session_id,
                    0,
                    symbol_id as u32,
                    if symbol_id % 2 == 0 { 1 } else { 3 },
                    chunk,
                ),
                peer_id: Some(SOURCE_NODE_ID),
            },
        );
    }
    runtime.deliver(session_id, common::eot_frame(session_id, SOURCE_NODE_ID));
    assert_eq!(
        recv_control(&mut capture.packet_rx).await,
        LosslessSessionControl::FecStatus {
            status: FecStatus::Complete,
        }
    );
    assert_eq!(
        timeout(Duration::from_secs(5), session.wait())
            .await
            .expect("receiver should complete"),
        SessionOutcome::Completed
    );

    runtime.deliver(
        session_id,
        InboundFrame {
            bytes: lossless_session::encode_block_symbol(session_id, 0, 0, 1, &[1u8, 2]),
            peer_id: Some(SOURCE_NODE_ID),
        },
    );
    assert_eq!(
        recv_control(&mut capture.packet_rx).await,
        LosslessSessionControl::FecStatus {
            status: FecStatus::Complete,
        }
    );

    runtime.deliver(session_id, common::eot_frame(session_id, SOURCE_NODE_ID));
    assert_eq!(
        recv_control(&mut capture.packet_rx).await,
        LosslessSessionControl::FecStatus {
            status: FecStatus::Complete,
        }
    );

    let result = session
        .take_completed_result()
        .expect("receiver should retain completed result");
    assert_eq!(&result.payload[..], &[1, 2, 3, 4, 5, 6, 7, 8]);
}

async fn collect_symbols_until_eot(
    packet_rx: &mut mpsc::Receiver<nextmini::node::packet::Packet>,
) -> Vec<(u64, u32)> {
    let mut symbols = Vec::new();
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");
        if let Some((_, LosslessSessionControl::Eot)) = lossless_session::decode_control(payload) {
            return symbols;
        }
        if lossless_session::decode_control(payload).is_some() {
            continue;
        }
        let (_, symbol, _) =
            lossless_session::decode_block_symbol(payload).expect("expected block symbol");
        symbols.push((symbol.block_id, symbol.symbol_id));
    }
}

async fn recv_control(
    packet_rx: &mut mpsc::Receiver<nextmini::node::packet::Packet>,
) -> LosslessSessionControl {
    loop {
        let packet = common::recv_packet(packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");
        if let Some((_, control)) = lossless_session::decode_control(payload) {
            return control;
        }
    }
}
