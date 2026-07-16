mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{ReceiverConfig, SenderConfig};
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode, NeedReport,
};

const SOURCE_NODE_ID: usize = 11;
const RECEIVER_NODE_ID: usize = 12;
const SESSION_ID: u64 = 0xA11C_E001;
const SRC_PORT: u16 = 4700;
const DST_PORT: u16 = 5700;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_reports_complete_on_source_done_and_writes_sink() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT,
        DST_PORT,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");

    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert_eq!(
        lossless_session::decode_control(ready_payload),
        Some((
            lossless_session::LosslessSessionHeader::decode_from(ready_payload)
                .expect("ready control should decode")
                .0,
            LosslessSessionControl::Ready,
        ))
    );
    assert_eq!(
        lossless_session::LosslessSessionHeader::decode_from(ready_payload)
            .expect("ready control should decode")
            .0
            .body_len,
        0,
        "Ready should no longer carry an in-band node id"
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("source-done should reach receiver");

    let status_packet = common::recv_packet(&mut capture.packet_rx).await;
    let status_payload = status_packet
        .tcp_payload()
        .expect("plain status packet should include payload");
    let (_, status_control) =
        lossless_session::decode_control(status_payload).expect("plain status should decode");
    assert_eq!(
        status_control,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        }
    );

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");

    let sink = sink.lock().await;
    assert_eq!(&sink[..16], b"abcdefghijklmnop");
    assert_eq!(
        sink.len(),
        16,
        "receiver sink should size itself from the manifest"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_replies_complete_on_later_source_done_after_local_completion() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 20,
        DST_PORT + 20,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 20,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let mut receiver_task =
        tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 20,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");

    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready))
    ));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID + 20, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");
    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 20,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("first source-done should reach receiver");

    let first_need = common::recv_packet(&mut capture.packet_rx).await;
    let first_payload = first_need
        .tcp_payload()
        .expect("first need packet should include payload");
    let (_, first_control) =
        lossless_session::decode_control(first_payload).expect("first need should decode");
    assert_eq!(
        first_control,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        }
    );

    assert!(
        timeout(Duration::from_millis(10), &mut receiver_task)
            .await
            .is_err(),
        "receiver must stay alive in passive-complete state for later rounds"
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 20,
            &LosslessSessionControl::SourceDone { round_id: 1 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("second source-done should reach receiver");

    let second_need = common::recv_packet(&mut capture.packet_rx).await;
    let second_payload = second_need
        .tcp_payload()
        .expect("second need packet should include payload");
    let (_, second_control) =
        lossless_session::decode_control(second_payload).expect("second need should decode");
    assert_eq!(
        second_control,
        LosslessSessionControl::Need {
            round_id: 1,
            report: NeedReport::Complete,
        }
    );

    drop(tx);
    timeout(Duration::from_secs(2), &mut receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_gc_exits_after_passive_complete_idle_timeout() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 21,
        DST_PORT + 21,
        1,
        2048,
    )
    .await;
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 21,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(Arc::new(Mutex::new(Vec::new()))),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let mut receiver_task =
        tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 21,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");
    let _ = common::recv_packet(&mut capture.packet_rx).await;

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID + 21, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");
    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 21,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("source-done should reach receiver");
    let _ = common::recv_packet(&mut capture.packet_rx).await;

    timeout(Duration::from_secs(3), &mut receiver_task)
        .await
        .expect("receiver should eventually GC after passive-complete idle timeout")
        .expect("receiver task should exit cleanly");

    drop(tx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_waits_for_source_done_before_completion() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 2,
        DST_PORT + 2,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 2,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let mut receiver_task =
        tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 2,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");

    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready))
    ));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID + 2, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");

    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "plain receiver should stay quiet until SourceDone"
    );
    assert!(
        timeout(Duration::from_millis(200), &mut receiver_task)
            .await
            .is_err(),
        "plain receiver should not finish before SourceDone"
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 2,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("source-done should reach receiver");

    let status_packet = common::recv_packet(&mut capture.packet_rx).await;
    let status_payload = status_packet
        .tcp_payload()
        .expect("plain status packet should include payload");
    let (_, status_control) =
        lossless_session::decode_control(status_payload).expect("plain status should decode");
    assert_eq!(
        status_control,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        }
    );

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop after SourceDone confirms completion")
        .expect("receiver task should exit cleanly");

    assert_eq!(&*sink.lock().await, b"abcdefghijklmnop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_ignores_removed_legacy_control_ids() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 20,
        DST_PORT + 20,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 20,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let mut receiver_task =
        tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 20,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");

    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready))
    ));

    tx.send(common::legacy_control_frame(
        SESSION_ID + 20,
        SOURCE_NODE_ID,
        3,
        &[],
    ))
    .await
    .expect("legacy block-ack control should reach receiver");
    tx.send(common::legacy_control_frame(
        SESSION_ID + 20,
        SOURCE_NODE_ID,
        4,
        &[0],
    ))
    .await
    .expect("legacy block-status control should reach receiver");

    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "removed legacy control ids must not be reinterpreted as live controls"
    );
    assert!(
        timeout(Duration::from_millis(200), &mut receiver_task)
            .await
            .is_err(),
        "receiver must stay active after removed legacy control ids"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_resends_ready_for_identical_manifest_replay() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 4,
        DST_PORT + 4,
        1,
        2048,
    )
    .await;
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 4,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: None,
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));
    let manifest = LosslessSessionManifest {
        block_size: 16,
        total_bytes: 16,
        total_blocks: 1,
        mode: LosslessSessionMode::Plain,
    };

    for _ in 0..2 {
        tx.send(InboundFrame {
            bytes: lossless_session::encode_control(
                SESSION_ID + 4,
                &LosslessSessionControl::Manifest {
                    manifest: manifest.clone(),
                },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        })
        .await
        .expect("manifest should reach receiver");

        let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
        let ready_payload = ready_packet
            .tcp_payload()
            .expect("ready packet should include payload");
        assert!(matches!(
            lossless_session::decode_control(ready_payload),
            Some((_, LosslessSessionControl::Ready))
        ));
    }

    drop(tx);
    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop")
        .expect("receiver task should exit cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_receiver_ignores_conflicting_manifest_after_install() {
    let mut capture = common::packet_capture(
        RECEIVER_NODE_ID,
        SOURCE_NODE_ID,
        SRC_PORT + 5,
        DST_PORT + 5,
        1,
        2048,
    )
    .await;
    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID + 5,
        route: capture.route(),
        local_node_id: capture.cfg.node_id,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: false,
        cloudcast: None,
        carousel: Default::default(),
        mettle_decoder_budget: None,
    };
    let (tx, rx) = mpsc::channel::<InboundFrame>(64);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, capture.processors.clone()));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 5,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 16,
                    total_bytes: 16,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("manifest should reach receiver");

    let ready_packet = common::recv_packet(&mut capture.packet_rx).await;
    let ready_payload = ready_packet
        .tcp_payload()
        .expect("ready packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(ready_payload),
        Some((_, LosslessSessionControl::Ready))
    ));

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 5,
            &LosslessSessionControl::Manifest {
                manifest: LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 24,
                    total_blocks: 3,
                    mode: LosslessSessionMode::Plain,
                },
            },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("conflicting manifest should reach receiver");

    assert!(
        timeout(Duration::from_millis(200), capture.packet_rx.recv())
            .await
            .is_err(),
        "receiver should ignore conflicting manifest replays after install"
    );

    tx.send(InboundFrame {
        bytes: lossless_session::encode_block_data(SESSION_ID + 5, 0, b"abcdefghijklmnop"),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("block data should reach receiver");

    tx.send(InboundFrame {
        bytes: lossless_session::encode_control(
            SESSION_ID + 5,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("source-done should reach receiver");

    let status_packet = common::recv_packet(&mut capture.packet_rx).await;
    let status_payload = status_packet
        .tcp_payload()
        .expect("plain status packet should include payload");
    let (_, status_control) =
        lossless_session::decode_control(status_payload).expect("plain status should decode");
    assert_eq!(
        status_control,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        }
    );

    timeout(Duration::from_secs(2), receiver_task)
        .await
        .expect("receiver task should stop after SourceDone confirms completion")
        .expect("receiver task should exit cleanly");

    assert_eq!(&*sink.lock().await, b"abcdefghijklmnop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_sender_completes_after_complete_status() {
    let mut capture = common::packet_capture(
        SOURCE_NODE_ID,
        RECEIVER_NODE_ID,
        SRC_PORT + 1,
        DST_PORT + 1,
        1,
        2048,
    )
    .await;
    let sender_cfg = SenderConfig {
        session: capture.session_config(SESSION_ID + 1, 16),
        route: capture.route(),
        pacing: None,
        receiver_ids: vec![RECEIVER_NODE_ID],
        source_buffer: Bytes::from_static(b"qrstuvwxyzabcdef"),
        manifest: LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Plain,
        },
        ready_grace_ms: 500,
        peer_report_timeout_ms: 500,
        topology_ready: None,
        cloudcast: None,
    };

    let (ctrl_tx, ctrl_rx) = mpsc::channel(64);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, capture.processors.clone()));

    let manifest_packet = common::recv_packet(&mut capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    assert!(matches!(
        lossless_session::decode_control(manifest_payload),
        Some((_, LosslessSessionControl::Manifest { .. }))
    ));

    ctrl_tx
        .send(common::ready_frame(SESSION_ID + 1, RECEIVER_NODE_ID))
        .await
        .expect("ready frame should enqueue");

    let mut saw_block_data = false;
    let mut saw_source_done = false;
    while !saw_block_data || !saw_source_done {
        let packet = common::recv_packet(&mut capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if let Some((_, data, body)) = lossless_session::decode_block_data(payload) {
            assert_eq!(data.block_id, 0);
            assert_eq!(body, b"qrstuvwxyzabcdef");
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { round_id })) =
            lossless_session::decode_control(payload)
        {
            assert_eq!(round_id, 0);
            saw_source_done = true;
        }
    }

    ctrl_tx
        .send(common::plain_status_frame(
            SESSION_ID + 1,
            RECEIVER_NODE_ID,
            0,
            NeedReport::Complete,
        ))
        .await
        .expect("plain complete status should enqueue");

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");
}
