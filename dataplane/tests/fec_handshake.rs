use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::{InboundFrame, LosslessRuntimeHandle};
use nextmini::node::session::control::should_abort_fec_preflight;
use nextmini::node::session::runtime::{CommonConfig, PreflightError, SenderConfig};
use nextmini_messages::lossless_session::{
    self, FecCapabilities, FecManifest, LosslessSessionControl,
};
use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

#[test]
fn aborts_when_peer_incompatible() {
    let manifest = FecManifest::new_raptorq(64, 1400);
    let required = vec![11usize, 12usize];

    let mut capabilities = BTreeMap::new();
    capabilities.insert(11usize, FecCapabilities::default());
    capabilities.insert(12usize, FecCapabilities::empty());

    assert!(
        should_abort_fec_preflight(&required, &manifest, &capabilities),
        "strict FEC session must abort before first FEC data frame when any required peer is incompatible"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_surfaces_preflight_error_to_caller() {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 512,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let processors = ProcessorHandle::new(cfg.clone());
    let runtime = LosslessRuntimeHandle::new(processors, cfg.lossless_runtime_config.clone());

    let manifest = FecManifest::new_raptorq(4, 16);
    let sender_cfg = SenderConfig {
        common: CommonConfig {
            session_id: 0x0FEC_2001,
            dest_ip: 2usize.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            chunk_size: 16,
            src_port: 4410,
            dst_port: 5410,
            data_bucket: None,
            local_node_id: cfg.node_id,
            user_space_base_addr: cfg.user_space_base_addr,
            local_netmask: cfg.local_netmask,
        },
        receiver_ids: vec![2],
        total_bytes: 64,
        source_buffer: Bytes::from(vec![0xAA; 16]),
        fec_manifest: Some(manifest),
        fec_tree_ids: vec![0],
        fec_tree_lane_depth: 4,
        fec_dispatch_burst: 1,
        ready_grace_ms: 1,
        topology_ready: None,
    };

    let started = runtime.start_sender(sender_cfg).await;
    assert!(
        matches!(started, Err(PreflightError::DisabledByConfig)),
        "caller should get a typed preflight error instead of a best-effort session id"
    );
}

struct RuntimeHarness {
    runtime: LosslessRuntimeHandle,
    session_id: u64,
    packet_rx: mpsc::Receiver<Packet>,
}

async fn start_runtime_sender(receiver_ids: Vec<usize>, ready_grace_ms: u64) -> RuntimeHarness {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 3,
        num_packet_processors: 1,
        channel_capacity: 2048,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let processors = ProcessorHandle::new(cfg.clone());

    let src_ip = cfg
        .node_id
        .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let dst_ip = 2usize.ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let src_port = 4310;
    let dst_port = 5310;

    processors
        .update_routing_table(vec![RoutingTableEntry {
            route_id: 71,
            next_hops: vec![cfg.node_id],
            src_node_id: cfg.node_id,
            dst_node_id: 2,
            forward_mode: RouteForwardingMode::Unicast,
        }])
        .await;

    let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
    let (packet_tx, packet_rx) = mpsc::channel(1024);
    processors.connect_user_space_sender(flow_id, packet_tx);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_require_capability = true;
    runtime_cfg.ready_grace_ms = ready_grace_ms;

    let runtime = LosslessRuntimeHandle::new(processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true);

    let session_id = 0x0FEC_0001;
    let manifest = FecManifest::new_raptorq(4, 16);
    let sender_cfg = SenderConfig {
        common: CommonConfig {
            session_id,
            dest_ip: dst_ip,
            chunk_size: usize::from(manifest.symbol_size),
            src_port,
            dst_port,
            data_bucket: None,
            local_node_id: cfg.node_id,
            user_space_base_addr: cfg.user_space_base_addr,
            local_netmask: cfg.local_netmask,
        },
        receiver_ids,
        total_bytes: 64,
        source_buffer: Bytes::from(vec![0xAB; 16]),
        fec_manifest: Some(manifest),
        fec_tree_ids: vec![0],
        fec_tree_lane_depth: 16,
        fec_dispatch_burst: 1,
        ready_grace_ms,
        topology_ready: None,
    };

    let started_sid = runtime
        .start_sender(sender_cfg)
        .await
        .expect("strict preflight should accept this sender config");
    assert_eq!(
        started_sid, session_id,
        "runtime should preserve caller session_id"
    );

    RuntimeHarness {
        runtime,
        session_id,
        packet_rx,
    }
}

fn encode_control_frame(session_id: u64, control: &LosslessSessionControl) -> Vec<u8> {
    lossless_session::encode_control(session_id, control)
}

async fn observe_manifest_and_fec_data(packet_rx: &mut mpsc::Receiver<Packet>) -> (bool, bool) {
    let mut saw_manifest = false;
    let mut saw_fec_data = false;
    let deadline = Instant::now() + Duration::from_millis(400);

    while Instant::now() < deadline {
        match timeout(Duration::from_millis(25), packet_rx.recv()).await {
            Ok(Some(packet)) => {
                let Some(payload) = packet.tcp_payload() else {
                    continue;
                };

                if lossless_session::decode_fec_data(payload).is_some() {
                    saw_fec_data = true;
                }

                if let Some((_, control)) = lossless_session::decode_control(payload)
                    && matches!(control, LosslessSessionControl::FecManifest { .. })
                {
                    saw_manifest = true;
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }

    (saw_manifest, saw_fec_data)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_aborts_before_fec_data_when_capability_is_missing() {
    let mut harness = start_runtime_sender(vec![11, 12], 20).await;

    for node_id in [11u64, 12u64] {
        harness.runtime.deliver(
            harness.session_id,
            InboundFrame {
                bytes: encode_control_frame(
                    harness.session_id,
                    &LosslessSessionControl::Ready { node_id },
                ),
                peer_id: Some(node_id as usize),
            },
        );
    }

    harness.runtime.deliver(
        harness.session_id,
        InboundFrame {
            bytes: encode_control_frame(
                harness.session_id,
                &LosslessSessionControl::FecCapabilities {
                    node_id: 11,
                    capabilities: FecCapabilities::default(),
                },
            ),
            peer_id: Some(11),
        },
    );

    let completed = timeout(
        Duration::from_secs(5),
        harness.runtime.wait_completion(harness.session_id),
    )
    .await
    .expect("sender runtime wait should not time out");
    assert!(
        completed,
        "sender task should terminate after preflight rejection"
    );

    let (saw_manifest, saw_fec_data) = observe_manifest_and_fec_data(&mut harness.packet_rx).await;
    assert!(
        saw_manifest,
        "sender should still advertise FEC manifest before preflight verdict"
    );
    assert!(
        !saw_fec_data,
        "missing required peer capability must abort before first FEC data frame"
    );

    harness.runtime.stop(harness.session_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_aborts_before_fec_data_when_peer_is_incompatible() {
    let mut harness = start_runtime_sender(vec![21], 200).await;

    harness.runtime.deliver(
        harness.session_id,
        InboundFrame {
            bytes: encode_control_frame(
                harness.session_id,
                &LosslessSessionControl::Ready { node_id: 21 },
            ),
            peer_id: Some(21),
        },
    );
    harness.runtime.deliver(
        harness.session_id,
        InboundFrame {
            bytes: encode_control_frame(
                harness.session_id,
                &LosslessSessionControl::FecCapabilities {
                    node_id: 21,
                    capabilities: FecCapabilities::empty(),
                },
            ),
            peer_id: Some(21),
        },
    );

    let completed = timeout(
        Duration::from_secs(5),
        harness.runtime.wait_completion(harness.session_id),
    )
    .await
    .expect("sender runtime wait should not time out");
    assert!(
        completed,
        "sender task should terminate after incompatibility rejection"
    );

    let (saw_manifest, saw_fec_data) = observe_manifest_and_fec_data(&mut harness.packet_rx).await;
    assert!(
        saw_manifest,
        "sender should emit FEC manifest in strict FEC mode"
    );
    assert!(
        !saw_fec_data,
        "incompatible peer capability must abort before first FEC data frame"
    );

    harness.runtime.stop(harness.session_id);
}
