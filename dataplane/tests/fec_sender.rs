use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

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
use nextmini_messages::{RouteForwardingMode, RoutingTableEntry, TokenBucketSpec};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_emits_repairs_with_budget() {
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
    let src_port = 4100;
    let dst_port = 5200;

    processors
        .update_routing_table(vec![RoutingTableEntry {
            route_id: 7,
            next_hops: vec![cfg.node_id],
            src_node_id: cfg.node_id,
            dst_node_id: 2,
            forward_mode: RouteForwardingMode::Unicast,
        }])
        .await;

    let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
    let (packet_tx, mut packet_rx) = mpsc::channel(512);
    processors.connect_user_space_sender(flow_id, packet_tx);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let manifest = FecManifest::new_raptorq(4, 16);
    let common = CommonConfig {
        session_id: 55,
        dest_ip: dst_ip,
        chunk_size: usize::from(manifest.symbol_size),
        src_port,
        dst_port,
        data_bucket: Some(TokenBucketSpec {
            rate: 160,
            bucket_size: 16,
        }),
        local_node_id: cfg.node_id,
        user_space_base_addr: cfg.user_space_base_addr,
        local_netmask: cfg.local_netmask,
    };
    let sender_cfg = SenderConfig {
        common,
        receiver_ids: vec![],
        total_bytes: 64,
        source_buffer: Bytes::from(vec![0xAB; 16]),
        fec_manifest: Some(manifest),
        ready_grace_ms: 1,
        topology_ready: None,
    };

    let (_ctrl_tx, ctrl_rx) = mpsc::channel::<InboundFrame>(16);
    let sender_task = tokio::spawn(sender::run(sender_cfg, ctrl_rx, processors.clone()));

    let mut saw_manifest = false;
    let mut saw_eot = false;
    let mut symbols: Vec<(u64, u32, usize)> = Vec::new();
    let mut first_symbol_at: Option<Instant> = None;
    let mut last_symbol_at: Option<Instant> = None;

    while !saw_eot {
        let packet = timeout(Duration::from_secs(5), packet_rx.recv())
            .await
            .expect("timed out waiting for sender output")
            .expect("sender output channel closed");

        let payload = packet
            .tcp_payload()
            .expect("captured packet should include TCP payload");

        if let Some((_, fec_data, body)) = lossless_session::decode_fec_data(payload) {
            let now = Instant::now();
            first_symbol_at.get_or_insert(now);
            last_symbol_at = Some(now);
            symbols.push((fec_data.block_id, fec_data.symbol_id, body.len()));
            continue;
        }

        if let Some((_, control)) = lossless_session::decode_control(payload) {
            match control {
                LosslessSessionControl::FecManifest { .. } => {
                    saw_manifest = true;
                }
                LosslessSessionControl::Eot { .. } => {
                    saw_eot = true;
                }
                _ => {}
            }
        }
    }

    timeout(Duration::from_secs(5), sender_task)
        .await
        .expect("sender task timed out")
        .expect("sender task failed");

    assert!(saw_manifest, "sender should emit a FEC manifest");
    assert!(saw_eot, "sender should emit EOT after draining symbols");
    assert!(!symbols.is_empty(), "sender should emit FEC symbols");
    assert!(
        symbols.iter().all(|(block_id, _, _)| *block_id == 0),
        "single-block transfer should stay within block 0"
    );
    assert!(
        symbols.iter().all(|(_, _, payload_len)| *payload_len == 16),
        "all emitted symbols should match manifest symbol_size"
    );

    let mut source_ids: Vec<u32> = symbols
        .iter()
        .filter_map(|(_, symbol_id, _)| (*symbol_id < 4).then_some(*symbol_id))
        .collect();
    source_ids.sort_unstable();
    assert_eq!(
        source_ids,
        vec![0, 1, 2, 3],
        "sender should emit systematic symbols for the full block"
    );

    let repair_count = symbols
        .iter()
        .filter(|(_, symbol_id, _)| *symbol_id >= 4)
        .count();
    assert_eq!(
        repair_count, 2,
        "repair symbols must respect per-block budget"
    );

    let first = first_symbol_at.expect("expected first symbol timestamp");
    let last = last_symbol_at.expect("expected last symbol timestamp");
    assert!(
        last.duration_since(first) >= Duration::from_millis(350),
        "token-bucket pacing should apply across systematic and repair emission"
    );
}
