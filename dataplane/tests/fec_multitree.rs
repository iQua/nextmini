use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::InboundFrame;
use nextmini::node::session::runtime::{CommonConfig, SenderConfig};
use nextmini::node::session::sender;
use nextmini_messages::lossless_session::{self, FecManifest, LosslessSessionControl};
use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

struct TreeCapture {
    symbol_trees: BTreeMap<(u64, u32), u16>,
    distinct_trees: BTreeSet<u16>,
}

async fn run_sender_and_capture_trees(session_id: u64, fec_tree_ids: Vec<u16>) -> TreeCapture {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 2048,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let processors = ProcessorHandle::new(cfg.clone());

    let src_ip = (cfg.node_id).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let dst_ip = 2usize.ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let src_port = 4110;
    let dst_port = 5220;

    processors
        .update_routing_table(vec![RoutingTableEntry {
            route_id: 70,
            next_hops: vec![cfg.node_id],
            src_node_id: cfg.node_id,
            dst_node_id: 2,
            forward_mode: RouteForwardingMode::Unicast,
        }])
        .await;

    let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
    let (packet_tx, mut packet_rx) = mpsc::channel(1024);
    processors.connect_user_space_sender(flow_id, packet_tx);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let manifest = FecManifest::new_raptorq(8, 32);
    let common = CommonConfig {
        session_id,
        dest_ip: dst_ip,
        chunk_size: usize::from(manifest.symbol_size),
        src_port,
        dst_port,
        data_bucket: None,
        local_node_id: cfg.node_id,
        user_space_base_addr: cfg.user_space_base_addr,
        local_netmask: cfg.local_netmask,
    };

    let sender_cfg = SenderConfig {
        common,
        receiver_ids: vec![],
        total_bytes: 1024,
        source_buffer: Bytes::from(vec![0xCD; 32]),
        fec_manifest: Some(manifest),
        fec_tree_ids,
        fec_tree_lane_depth: 32,
        fec_dispatch_burst: 1,
        ready_grace_ms: 1,
        topology_ready: None,
    };

    let (_ctrl_tx, ctrl_rx) = mpsc::channel::<InboundFrame>(8);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, processors.clone()));

    let mut saw_manifest = false;
    let mut saw_eot = false;
    let mut symbol_trees = BTreeMap::new();
    let mut distinct_trees = BTreeSet::new();

    while !saw_eot {
        let packet = timeout(Duration::from_secs(5), packet_rx.recv())
            .await
            .expect("timed out waiting for sender output")
            .expect("sender output channel closed");

        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");

        if let Some((_, fec_data, _)) = lossless_session::decode_fec_data(payload) {
            symbol_trees.insert((fec_data.block_id, fec_data.symbol_id), fec_data.tree_id);
            distinct_trees.insert(fec_data.tree_id);
            continue;
        }

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::FecManifest { .. } => saw_manifest = true,
                LosslessSessionControl::Eot { .. } => saw_eot = true,
                _ => {}
            }
        }
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");

    assert!(saw_manifest, "sender should emit a FEC manifest");
    assert!(saw_eot, "sender should emit EOT");
    assert!(!symbol_trees.is_empty(), "sender should emit FEC symbols");

    TreeCapture {
        symbol_trees,
        distinct_trees,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn symbols_are_collaboratively_dispatched_across_configured_trees() {
    let configured_trees = vec![1u16, 3, 5];
    let capture = run_sender_and_capture_trees(0xBAD5EED, configured_trees.clone()).await;
    let configured_tree_set: BTreeSet<u16> = configured_trees.into_iter().collect();

    assert!(
        capture
            .symbol_trees
            .values()
            .all(|tree_id| configured_tree_set.contains(tree_id)),
        "sender must not emit symbols on unconfigured tree IDs"
    );
    assert!(
        capture.distinct_trees.is_subset(&configured_tree_set),
        "all observed trees must be a subset of configured fec_tree_ids"
    );
    assert!(
        capture.distinct_trees.len() >= 2,
        "unblocked collaborative dispatch should use at least two distinct trees"
    );
}
