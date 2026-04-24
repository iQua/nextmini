mod common;

use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::time::timeout;

use nextmini::node::NodeIdExt;
use nextmini::node::config::{LocalConfig, LosslessFecScheme};
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::session::api::{
    LosslessRuntimeHandle, LosslessSessionHandle, SessionOutcome, StartError,
};
use nextmini::node::session::runtime::{
    PreflightError, SenderRequest, SessionConfig, TransportRoute,
};
use nextmini_messages::OperatingMode;
use nextmini_messages::lossless_session::{
    self, LosslessSessionControl, LosslessSessionMode, NeedReport,
};

struct RuntimeHarness {
    runtime: LosslessRuntimeHandle,
    session: LosslessSessionHandle,
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
    let peer_report_timeout_ms = runtime_cfg.peer_report_timeout_ms;
    let runtime = LosslessRuntimeHandle::new(capture.processors.clone(), runtime_cfg);
    runtime.set_topology_ready(true);

    let session = runtime
        .start_sender(SenderRequest {
            session: capture.session_config(session_id, 16),
            route: capture.route(),
            pacing: None,
            receiver_ids: vec![2],
            total_bytes: 16,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms,
            peer_report_timeout_ms,
        })
        .await
        .expect("sender should start");
    assert_eq!(session.id(), session_id);

    RuntimeHarness {
        runtime,
        session,
        session_id,
        capture,
    }
}

async fn start_sender_with_runtime_config(
    cfg: LocalConfig,
    runtime_cfg: nextmini::node::config::LosslessConfig,
    session_id: u64,
) -> Result<u64, StartError> {
    let processors = ProcessorHandle::new(cfg.clone());
    let peer_report_timeout_ms = runtime_cfg.peer_report_timeout_ms;
    let runtime = LosslessRuntimeHandle::new(processors, runtime_cfg);

    runtime
        .start_sender(SenderRequest {
            session: SessionConfig {
                session_id,
                block_size: 16,
            },
            route: TransportRoute {
                src_ip: cfg
                    .node_id
                    .ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
                dst_ip: 2usize.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
                src_port: 4410,
                dst_port: 5410,
            },
            pacing: None,
            receiver_ids: vec![2],
            total_bytes: 64,
            source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
            ready_grace_ms: 1,
            peer_report_timeout_ms,
        })
        .await
        .map(|session| session.id())
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
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_tree_ids.clear();

    let started = start_sender_with_runtime_config(cfg, runtime_cfg, 0x0FEC_2001).await;

    assert!(
        matches!(
            started,
            Err(StartError::Preflight(PreflightError::MissingTreeIds))
        ),
        "caller should get a typed preflight error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_rejects_zero_symbols_per_block_without_clamping() {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 512,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_symbols_per_block = 0;
    runtime_cfg.fec_default_tree_ids = vec![1];

    let started = start_sender_with_runtime_config(cfg, runtime_cfg, 0x0FEC_2002).await;

    assert!(
        matches!(
            started,
            Err(StartError::Preflight(PreflightError::ZeroSymbolsPerBlock))
        ),
        "zero configured symbols_per_block should be rejected directly"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_defaults_to_raptorq_fec_scheme() {
    let mut harness = start_runtime_sender(true, 400, 0x0FEC_2005).await;

    let manifest_packet = common::recv_packet(&mut harness.capture.packet_rx).await;
    let manifest_payload = manifest_packet
        .tcp_payload()
        .expect("manifest packet should include payload");
    let (_, control) =
        lossless_session::decode_control(manifest_payload).expect("expected manifest control");
    let LosslessSessionControl::Manifest { manifest } = control else {
        panic!("unexpected control frame");
    };
    let LosslessSessionMode::Fec(fec) = manifest.mode else {
        panic!("expected FEC manifest");
    };

    assert_eq!(
        fec.scheme_kind(),
        Some(nextmini_messages::lossless_session::FecScheme::RaptorQ)
    );

    harness.runtime.deliver(
        harness.session_id,
        common::ready_frame(harness.session_id, 2),
    );
    drop(harness.session);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_rejects_mettle_below_minimum_symbols_per_block() {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 512,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_scheme = LosslessFecScheme::Mettle;
    runtime_cfg.fec_default_symbols_per_block = 2399;
    runtime_cfg.fec_default_tree_ids = vec![1];

    let started = start_sender_with_runtime_config(cfg, runtime_cfg, 0x0FEC_2006).await;

    assert!(
        matches!(
            started,
            Err(StartError::Preflight(
                PreflightError::MettleSymbolsPerBlockTooSmall { value: 2399, .. }
            ))
        ),
        "METTLE should reject K below the sender policy minimum"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_rejects_unsorted_duplicate_fec_tree_ids() {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 512,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_symbols_per_block = 4;
    runtime_cfg.fec_default_tree_ids = vec![3, 1, 3];

    let started = start_sender_with_runtime_config(cfg, runtime_cfg, 0x0FEC_2003).await;

    assert!(
        matches!(
            started,
            Err(StartError::Preflight(
                PreflightError::TreeIdsMustBeSortedUnique { .. }
            ))
        ),
        "tree ids should be validated, not canonicalized"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_sender_rejects_multitree_fec_when_ingress_contract_is_shared_queue() {
    let cfg = LocalConfig {
        node_id: 1,
        n_nodes: 2,
        num_packet_processors: 1,
        channel_capacity: 512,
        operating_mode: OperatingMode::Max,
        feature: nextmini::node::config::Feature::Sequential,
        channel_backpressure: true,
        user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
        local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        ..Default::default()
    };
    let mut runtime_cfg = cfg.lossless_runtime_config.clone();
    runtime_cfg.fec_enabled = true;
    runtime_cfg.fec_default_symbols_per_block = 4;
    runtime_cfg.fec_default_tree_ids = vec![1, 3];

    let started = start_sender_with_runtime_config(cfg, runtime_cfg, 0x0FEC_2004).await;

    assert!(
        matches!(
            started,
            Err(StartError::Preflight(
                PreflightError::MultiTreeRequiresTreeVisibleIngress
            ))
        ),
        "multi-tree FEC should be rejected when processor ingress collapses onto a shared queue"
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
    let mut saw_source_done = false;
    while !saw_block_data || !saw_source_done {
        let packet = common::recv_packet(&mut harness.capture.packet_rx).await;
        let payload = packet
            .tcp_payload()
            .expect("captured packet should include payload");
        if lossless_session::decode_block_data(payload).is_some() {
            saw_block_data = true;
            continue;
        }
        if let Some((_, LosslessSessionControl::SourceDone { .. })) =
            lossless_session::decode_control(payload)
        {
            saw_source_done = true;
        }
    }

    harness.runtime.deliver(
        harness.session_id,
        common::plain_status_frame(harness.session_id, 2, 0, NeedReport::Complete),
    );
    let completed = timeout(Duration::from_secs(5), harness.session.wait())
        .await
        .expect("sender runtime wait should not time out");
    assert_eq!(completed, SessionOutcome::Completed);
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

    harness.session.abort();
    assert_eq!(harness.session.wait().await, SessionOutcome::Aborted);
}
