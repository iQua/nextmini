mod common;

use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::LocalConfig;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::LosslessRuntimeHandle;
use nextmini::node::session::runtime::{CommonConfig, PreflightError, SenderRequest};
use nextmini_messages::lossless_session::{self, LosslessSessionControl, LosslessSessionMode};

struct RuntimeHarness {
    runtime: LosslessRuntimeHandle,
    session_id: u64,
    capture: common::PacketCaptureHarness,
}

async fn start_runtime_sender(
    fec_enabled: bool,
    ready_grace_ms: u64,
    session_id: u64,
) -> RuntimeHarness {
    let capture = common::packet_capture(1, 2, 4310, 5310, 1, 2048).await;

    let mut runtime_cfg = capture.cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = fec_enabled;
    runtime_cfg.ready_grace_ms = ready_grace_ms;
    runtime_cfg.fec_default_symbols_per_block = 4;
    runtime_cfg.fec_default_tree_ids = vec![1, 3];
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true);

    let started_sid = runtime
        .start_sender(SenderRequest {
            common: capture.common_config(session_id, 16),
            receiver_ids: vec![2],
            total_bytes: 16,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms,
        })
        .await
        .expect("sender should start");
    assert_eq!(started_sid, session_id);

    RuntimeHarness {
        runtime,
        session_id,
        capture,
    }
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
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_tree_ids.clear();
    let runtime = LosslessRuntimeHandle::new(processors, runtime_cfg);

    let started = runtime
        .start_sender(SenderRequest {
            common: CommonConfig {
                session_id: 0x0FEC_2001,
                dest_ip: 2usize.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
                block_size: 16,
                src_port: 4410,
                dst_port: 5410,
                data_bucket: None,
                local_node_id: cfg.node_id,
                user_space_base_addr: cfg.user_space_base_addr,
                local_netmask: cfg.local_netmask,
            },
            receiver_ids: vec![2],
            total_bytes: 64,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms: 1,
        })
        .await;

    assert!(
        matches!(started, Err(PreflightError::MissingTreeIds)),
        "caller should get a typed preflight error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_sender_waits_for_ready_before_emitting_block_data() {
    let mut harness = start_runtime_sender(false, 400, 0x0FEC_0001).await;

    let manifest_packet = common::recv_packet(&mut harness.capture.packet_rx).await;
    assert_eq!(
        manifest_packet.lossless_session_id(),
        Some(harness.session_id)
    );
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    let (_, control) =
        lossless_session::decode_control(manifest_payload).expect("expected manifest control");
    match control {
        LosslessSessionControl::Manifest { manifest } => {
            assert_eq!(manifest.mode, LosslessSessionMode::Plain);
        }
        other => panic!("unexpected control frame: {other:?}"),
    }

    let quiet_deadline = Instant::now() + Duration::from_millis(100);
    while Instant::now() < quiet_deadline {
        if let Ok(Some(packet)) =
            timeout(Duration::from_millis(20), harness.capture.packet_rx.recv()).await
        {
            let payload = packet
                .tcp_payload()
                .expect("captured packet should include payload");
            assert!(
                lossless_session::decode_block_data(payload).is_none(),
                "sender must not emit block data before Ready"
            );
        }
    }

    harness.runtime.deliver(
        harness.session_id,
        common::ready_frame(harness.session_id, 2),
    );

    let mut saw_block_data = false;
    let mut saw_eot = false;
    while !saw_block_data || !saw_eot {
        let packet = common::recv_packet(&mut harness.capture.packet_rx).await;
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

    harness.runtime.deliver(
        harness.session_id,
        common::block_ack_frame(harness.session_id, 2, 0),
    );
    let completed = timeout(
        Duration::from_secs(5),
        harness.runtime.wait_completion(harness.session_id),
    )
    .await
    .expect("sender runtime wait should not time out");
    assert!(completed, "sender task should report completion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_sender_derives_fec_manifest_from_runtime_config() {
    let mut harness = start_runtime_sender(true, 400, 0x0FEC_0002).await;

    let manifest_packet = common::recv_packet(&mut harness.capture.packet_rx).await;
    assert_eq!(
        manifest_packet.lossless_session_id(),
        Some(harness.session_id)
    );
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    let (_, control) =
        lossless_session::decode_control(manifest_payload).expect("expected manifest control");
    match control {
        LosslessSessionControl::Manifest { manifest } => match manifest.mode {
            LosslessSessionMode::Fec(fec) => {
                assert_eq!(fec.symbols_per_block, 4);
                assert_eq!(fec.tree_ids, vec![1, 3]);
            }
            other => panic!("expected FEC mode, got {other:?}"),
        },
        other => panic!("unexpected control frame: {other:?}"),
    }

    harness.runtime.stop(harness.session_id);
}
