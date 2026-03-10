mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode,
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
    let mut saw_eot = false;
    let mut all_symbol_ids = Vec::new();
    let mut extra_symbol_ids = Vec::new();
    let mut status_sent = false;

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
                LosslessSessionControl::Eot => saw_eot = true,
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
        if symbol.symbol_id == 0 && !status_sent {
            ctrl_tx
                .send(block_status_frame(session_id, 2, 0, 2))
                .await
                .expect("block status should enqueue");
            status_sent = true;
        }
        if symbol.symbol_id >= 4 {
            assert!(saw_eot, "extra symbols must not appear before EOT");
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
        .send(common::block_ack_frame(session_id, 2, 0))
        .await
        .expect("block ack should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}

fn block_status_frame(
    session_id: u64,
    peer_id: usize,
    block_id: u64,
    deficit_symbols: u16,
) -> InboundFrame {
    InboundFrame {
        bytes: lossless_session::encode_control(
            session_id,
            &LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id,
                    deficit_symbols,
                },
            },
        ),
        peer_id: Some(peer_id),
    }
}
