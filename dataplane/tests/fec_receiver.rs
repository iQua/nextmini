use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::receiver;
use nextmini::node::session::runtime::{ReceiverConfig, TransportRoute};
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionFecMode, LosslessSessionManifest,
    LosslessSessionMode, NeedBlock, NeedReport,
};
use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

const SESSION_ID: u64 = 0xA55A;
const SOURCE_NODE_ID: usize = 11;
const RECEIVER_NODE_ID: usize = 12;
const SRC_PORT: u16 = 4500;
const DST_PORT: u16 = 4600;

struct ReceiverHarness {
    tx: mpsc::Sender<InboundFrame>,
    packet_rx: mpsc::Receiver<Packet>,
    receiver_task: tokio::task::JoinHandle<()>,
    sink: Arc<Mutex<Vec<u8>>>,
}

async fn build_receiver_harness() -> ReceiverHarness {
    let cfg = LocalConfig {
        node_id: RECEIVER_NODE_ID,
        n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
        num_packet_processors: 1,
        channel_capacity: 1024,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let processors = ProcessorHandle::new(cfg.clone());

    let src_ip = RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let dst_ip = SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    processors
        .update_routing_table(vec![RoutingTableEntry {
            route_id: 88,
            next_hops: vec![cfg.node_id],
            src_node_id: cfg.node_id,
            dst_node_id: SOURCE_NODE_ID,
            forward_mode: RouteForwardingMode::Unicast,
        }])
        .await;

    let flow_id = Packet::flow_id_from_parts(src_ip, SRC_PORT, dst_ip, DST_PORT);
    let (packet_tx, packet_rx) = mpsc::channel(512);
    processors.connect_user_space_sender(flow_id, packet_tx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let sink = Arc::new(Mutex::new(Vec::new()));
    let receiver_cfg = ReceiverConfig {
        session_id: SESSION_ID,
        route: TransportRoute {
            src_ip,
            dst_ip,
            src_port: SRC_PORT,
            dst_port: DST_PORT,
        },
        local_node_id: RECEIVER_NODE_ID,
        sink_buffer: Some(sink.clone()),
        sink_file: None,
        progress: None,
        peer_report_timeout_ms: 500,
        fec_enabled: true,
        cloudcast: None,
    };

    let (tx, rx) = mpsc::channel::<InboundFrame>(128);
    let receiver_task = tokio::spawn(receiver::run(receiver_cfg, rx, processors));

    ReceiverHarness {
        tx,
        packet_rx,
        receiver_task,
        sink,
    }
}

fn manifest(total_bytes: u64) -> LosslessSessionManifest {
    LosslessSessionManifest {
        block_size: 8,
        total_bytes,
        total_blocks: 1,
        mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![0, 1])),
    }
}

async fn send_frame(tx: &mpsc::Sender<InboundFrame>, bytes: Vec<u8>) {
    tx.send(InboundFrame {
        bytes,
        peer_id: Some(SOURCE_NODE_ID),
    })
    .await
    .expect("frame should be delivered to receiver");
}

async fn recv_control(packet_rx: &mut mpsc::Receiver<Packet>) -> (Packet, LosslessSessionControl) {
    loop {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for receiver output")
            .expect("receiver output channel closed");
        let payload = packet
            .tcp_payload()
            .expect("receiver output should carry TCP payload");
        if let Some((_, control)) = lossless_session::decode_control(payload) {
            return (packet, control);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_reports_complete_after_source_done_and_writes_sink() {
    let mut harness = build_receiver_harness().await;

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: manifest(8),
            },
        ),
    )
    .await;

    let (_, ready) = recv_control(&mut harness.packet_rx).await;
    assert_eq!(ready, LosslessSessionControl::Ready);

    let payload = [1u8, 2, 3, 4, 5, 6, 7, 8];
    for (symbol_id, chunk) in payload.chunks(2).enumerate() {
        let tree_id = if symbol_id % 2 == 0 { 0 } else { 1 };
        let frame =
            lossless_session::encode_block_symbol(SESSION_ID, 0, symbol_id as u32, tree_id, chunk);
        send_frame(&harness.tx, frame).await;
    }

    assert!(
        timeout(Duration::from_millis(150), harness.packet_rx.recv())
            .await
            .is_err(),
        "receiver should not emit FEC completion before SourceDone"
    );

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
    )
    .await;

    let (ack_packet, ack) = recv_control(&mut harness.packet_rx).await;
    assert_eq!(ack_packet.lossless_session_id(), Some(SESSION_ID));
    assert_eq!(
        ack,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        },
        "receiver should report round completion after SourceDone"
    );

    drop(harness.tx);

    timeout(Duration::from_secs(2), harness.receiver_task)
        .await
        .expect("receiver task should stop after block completion")
        .expect("receiver task should exit cleanly");

    let sink = harness.sink.lock().await.clone();
    assert_eq!(sink[..payload.len()], payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_replies_complete_on_later_source_done_after_local_completion() {
    let mut harness = build_receiver_harness().await;

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: manifest(8),
            },
        ),
    )
    .await;

    let (_, ready) = recv_control(&mut harness.packet_rx).await;
    assert!(matches!(ready, LosslessSessionControl::Ready));

    let payload = [1u8, 2, 3, 4, 5, 6, 7, 8];
    for (symbol_id, chunk) in payload.chunks(2).enumerate() {
        let tree_id = if symbol_id % 2 == 0 { 0 } else { 1 };
        let frame =
            lossless_session::encode_block_symbol(SESSION_ID, 0, symbol_id as u32, tree_id, chunk);
        send_frame(&harness.tx, frame).await;
    }

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
    )
    .await;
    let (_, first_need) = recv_control(&mut harness.packet_rx).await;
    assert_eq!(
        first_need,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Complete,
        }
    );

    assert!(
        timeout(Duration::from_millis(10), &mut harness.receiver_task)
            .await
            .is_err(),
        "receiver must stay alive in passive-complete state for later rounds"
    );

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::SourceDone { round_id: 1 },
        ),
    )
    .await;
    let (_, second_need) = recv_control(&mut harness.packet_rx).await;
    assert_eq!(
        second_need,
        LosslessSessionControl::Need {
            round_id: 1,
            report: NeedReport::Complete,
        }
    );

    drop(harness.tx);
    timeout(Duration::from_secs(2), harness.receiver_task)
        .await
        .expect("receiver task should stop after input closes")
        .expect("receiver task should exit cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_reports_missing_blocks_after_source_done_for_incomplete_block() {
    let mut harness = build_receiver_harness().await;

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: manifest(8),
            },
        ),
    )
    .await;

    let (_, ready) = recv_control(&mut harness.packet_rx).await;
    assert!(matches!(ready, LosslessSessionControl::Ready));

    let frame = lossless_session::encode_block_symbol(SESSION_ID, 0, 0, 0, &[1u8, 2]);
    send_frame(&harness.tx, frame).await;
    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::SourceDone { round_id: 0 },
        ),
    )
    .await;

    let (_, status) = recv_control(&mut harness.packet_rx).await;
    assert_eq!(
        status,
        LosslessSessionControl::Need {
            round_id: 0,
            report: NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 3,
                }],
            },
        },
        "receiver should request the remaining source symbols first"
    );

    drop(harness.tx);
    timeout(Duration::from_secs(2), harness.receiver_task)
        .await
        .expect("receiver task should drain once input closes")
        .expect("receiver task should exit cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_replays_same_missing_status_on_repeated_source_done() {
    let mut harness = build_receiver_harness().await;

    send_frame(
        &harness.tx,
        lossless_session::encode_control(
            SESSION_ID,
            &LosslessSessionControl::Manifest {
                manifest: manifest(8),
            },
        ),
    )
    .await;

    let (_, ready) = recv_control(&mut harness.packet_rx).await;
    assert!(matches!(ready, LosslessSessionControl::Ready));

    send_frame(
        &harness.tx,
        lossless_session::encode_block_symbol(SESSION_ID, 0, 0, 0, &[1u8, 2]),
    )
    .await;

    let source_done = lossless_session::encode_control(
        SESSION_ID,
        &LosslessSessionControl::SourceDone { round_id: 0 },
    );
    send_frame(&harness.tx, source_done.clone()).await;
    let (_, first) = recv_control(&mut harness.packet_rx).await;

    send_frame(&harness.tx, source_done).await;
    let (_, second) = recv_control(&mut harness.packet_rx).await;

    let expected = LosslessSessionControl::Need {
        round_id: 0,
        report: NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 3,
            }],
        },
    };
    assert_eq!(first, expected);
    assert_eq!(second, expected);

    drop(harness.tx);
    timeout(Duration::from_secs(2), harness.receiver_task)
        .await
        .expect("receiver task should drain once input closes")
        .expect("receiver task should exit cleanly");
}
