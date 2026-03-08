mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::session::runtime::SenderConfig;
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_stripes_symbols_across_configured_trees() {
    let mut harness = common::packet_capture(1, 2, 4110, 5220, 1, 2048).await;

    let session_id = 0xBAD5_EED0;
    let tree_ids = vec![1u16, 3, 5];
    let manifest = LosslessSessionManifest {
        block_size: 24,
        total_bytes: 24,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(6, tree_ids.clone())),
    };
    let sender_cfg = SenderConfig {
        session: harness.session_config(session_id, 24),
        route: harness.route(),
        pacing: None,
        receiver_ids: vec![2],
        total_bytes: 24,
        source_buffer: Bytes::from_static(b"abcdefghijklmnopqrstuvwx"),
        manifest,
        ready_grace_ms: 200,
        topology_ready: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
    ctrl_tx
        .send(common::ready_frame(session_id, 2))
        .await
        .expect("ready frame should enqueue");

    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, harness.processors.clone()));

    let configured = tree_ids.into_iter().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    let mut saw_eot = false;

    while observed.len() < configured.len() || !saw_eot {
        let packet = common::recv_packet(&mut harness.packet_rx).await;
        assert_eq!(packet.lossless_session_id(), Some(session_id));

        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");
        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::Manifest { .. } => {}
                LosslessSessionControl::Eot => saw_eot = true,
                other => panic!("unexpected control frame: {other:?}"),
            }
            continue;
        }

        let (_, symbol, _) =
            lossless_session::decode_block_symbol(payload).expect("expected block symbol");
        assert_eq!(packet.lossless_fec_tree_id(), Some(symbol.tree_id));
        assert!(
            configured.contains(&symbol.tree_id),
            "sender must only emit symbols on configured trees"
        );
        observed.insert(symbol.tree_id);
    }

    ctrl_tx
        .send(common::block_ack_frame(session_id, 2, 0))
        .await
        .expect("block ack should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");

    assert_eq!(
        observed, configured,
        "with uncongested trees, the sender should stripe across every configured tree"
    );
}
