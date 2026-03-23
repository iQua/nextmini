mod controller_helpers;
mod hash;

use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{error, info};

use nextmini_messages::GroupRouteTree;

use crate::node::NodeIdExt;
use crate::node::config::{IntegrationNodeRole, IntegrationTestConfig, LocalConfig};
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::python::interface::PythonInterfaceHandle;
use crate::node::session::api::{LosslessRuntimeHandle, SessionOutcome};
use crate::node::session::runtime::{
    ReceiverProgress, ReceiverRequest, SenderRequest, SessionConfig, TransportRoute,
};

use self::controller_helpers::ControllerHarness;
use self::hash::write_artifact_with_hash;

pub fn spawn(
    config: LocalConfig,
    controller: ControllerInterfaceHandle,
    lossless_runtime: LosslessRuntimeHandle,
    events: PythonInterfaceHandle,
) {
    tokio::spawn(async move {
        run(config, controller, lossless_runtime, events).await;
    });
}

pub async fn run(
    config: LocalConfig,
    controller: ControllerInterfaceHandle,
    lossless_runtime: LosslessRuntimeHandle,
    events: PythonInterfaceHandle,
) {
    let role = match config.integration_test.role_for_node(config.node_id) {
        Some(IntegrationNodeRole::Source) => IntegrationNodeRole::Source,
        Some(IntegrationNodeRole::Receiver) => IntegrationNodeRole::Receiver,
        Some(IntegrationNodeRole::Router) | None => return,
    };

    if let Err(err) = fs::create_dir_all(&config.integration_test.artifact_dir) {
        error!(
            node_id = config.node_id,
            artifact_dir = %config.integration_test.artifact_dir,
            "Failed to create integration artifact directory: {}",
            err
        );
        return;
    }

    let result = match role {
        IntegrationNodeRole::Source => {
            run_source(config.clone(), controller, lossless_runtime, events).await
        }
        IntegrationNodeRole::Receiver => {
            run_receiver(config.clone(), controller, lossless_runtime, events).await
        }
        IntegrationNodeRole::Router => Ok(()),
    };

    if let Err(err) = result {
        error!(
            case = %config.integration_test.case_name,
            node_id = config.node_id,
            "Integration test role failed: {}",
            err
        );
        let _ = write_status(
            &config.integration_test,
            config.node_id,
            role,
            &format!("error: {err}"),
        );
    }
}

async fn run_source(
    config: LocalConfig,
    controller: ControllerInterfaceHandle,
    lossless_runtime: LosslessRuntimeHandle,
    events: PythonInterfaceHandle,
) -> Result<(), String> {
    let harness_cfg = &config.integration_test;
    let timeout = Duration::from_millis(harness_cfg.group_timeout_ms);
    let mut control = ControllerHarness::new(controller, events);

    if !control.wait_for_topology_ready(timeout).await {
        return Err("timed out waiting for TopologyReady".to_string());
    }

    let (group_id, group_ip, src_node_id) = control
        .create_group(harness_cfg.group_label.clone(), timeout)
        .await?;

    write_group_info(harness_cfg, group_id, group_ip, src_node_id)?;

    match harness_cfg.trees.as_slice() {
        [] => return Err("integration_test.trees must contain at least one tree".to_string()),
        [tree] => {
            control
                .set_group_routes(group_id, src_node_id, tree.edges.clone(), timeout)
                .await?;
        }
        trees => {
            control
                .set_group_routes_multi(
                    group_id,
                    src_node_id,
                    trees
                        .iter()
                        .map(|tree| GroupRouteTree {
                            tree_id: tree.tree_id,
                            weight: None,
                            edges: tree.edges.clone(),
                        })
                        .collect(),
                    timeout,
                )
                .await?;
        }
    }

    wait_for_ready_receivers(harness_cfg, timeout).await?;

    let source_bytes = fs::read(&harness_cfg.payload_path).map_err(|err| {
        format!(
            "failed to read source file {}: {err}",
            harness_cfg.payload_path
        )
    })?;
    if source_bytes.is_empty() {
        return Err("source file is empty".to_string());
    }

    let source_artifact = source_artifact_path(harness_cfg);
    write_artifact_with_hash(&source_artifact, &source_bytes)?;

    let block_size = if harness_cfg.block_size > 0 {
        harness_cfg.block_size
    } else {
        config.lossless_runtime_config.default_block_size
    };

    let sid = multicast_session_id(group_id as u64, config.node_id);
    let sender_cfg = SenderRequest {
        session: SessionConfig {
            session_id: sid,
            block_size,
        },
        route: TransportRoute {
            src_ip: config.user_space_address,
            dst_ip: group_ip,
            src_port: harness_cfg.src_port,
            dst_port: harness_cfg.dst_port,
        },
        pacing: config.lossless_runtime_config.data_bucket.clone(),
        receiver_ids: harness_cfg.receiver_ids.clone(),
        total_bytes: source_bytes.len() as u64,
        source_buffer: Bytes::from(source_bytes),
        ready_grace_ms: config.lossless_runtime_config.ready_grace_ms,
    };

    let mut session = lossless_runtime
        .start_sender(sender_cfg)
        .await
        .map_err(|err| format!("lossless sender preflight rejected session {sid}: {err}"))?;
    let session_id = session.id();
    let transfer_started_at = Instant::now();

    let outcome = tokio::time::timeout(
        Duration::from_millis(harness_cfg.receive_timeout_ms),
        session.wait(),
    )
    .await
    .map_err(|_| format!("sender session {session_id} timed out waiting for completion"))?;

    if outcome != SessionOutcome::Completed {
        return Err(format!(
            "sender session {session_id} did not complete successfully"
        ));
    }

    write_performance_metrics(
        harness_cfg,
        config.node_id,
        IntegrationNodeRole::Source,
        fs::metadata(&source_artifact)
            .map_err(|err| {
                format!(
                    "failed to stat source artifact {}: {err}",
                    source_artifact.display()
                )
            })?
            .len(),
        transfer_started_at.elapsed(),
    )?;

    write_status(
        harness_cfg,
        config.node_id,
        IntegrationNodeRole::Source,
        "ok",
    )?;
    info!(
        case = %harness_cfg.case_name,
        node_id = config.node_id,
        session_id,
        "Integration test source finished successfully."
    );
    Ok(())
}

async fn run_receiver(
    config: LocalConfig,
    controller: ControllerInterfaceHandle,
    lossless_runtime: LosslessRuntimeHandle,
    events: PythonInterfaceHandle,
) -> Result<(), String> {
    let harness_cfg = &config.integration_test;
    let timeout = Duration::from_millis(harness_cfg.group_timeout_ms);
    let mut control = ControllerHarness::new(controller, events);

    if !control.wait_for_topology_ready(timeout).await {
        return Err("timed out waiting for TopologyReady".to_string());
    }

    let (group_id, _, _) = wait_for_group_info(harness_cfg, timeout).await?;
    control.join_group(group_id).await;

    let sink = Arc::new(Mutex::new(Vec::new()));
    let progress = Arc::new(ReceiverProgress::default());
    let sid = multicast_session_id(group_id as u64, harness_cfg.source_node_id);
    let mut session = lossless_runtime
        .start_receiver(ReceiverRequest {
            session_id: sid,
            route: TransportRoute {
                src_ip: config.user_space_address,
                dst_ip: harness_cfg
                    .source_node_id
                    .ip_addr(config.user_space_base_addr, config.local_netmask),
                src_port: harness_cfg.src_port,
                dst_port: harness_cfg.dst_port,
            },
            local_node_id: config.node_id,
            sink_buffer: Some(sink.clone()),
            progress: Some(progress.clone()),
        })
        .await
        .map_err(|err| format!("lossless receiver start rejected session {sid}: {err}"))?;
    let session_id = session.id();

    write_ready_marker(harness_cfg, config.node_id)?;

    let outcome = tokio::time::timeout(
        Duration::from_millis(harness_cfg.receive_timeout_ms),
        session.wait(),
    )
    .await
    .map_err(|_| format!("receiver session {session_id} timed out waiting for completion"))?;

    if outcome != SessionOutcome::Completed {
        return Err(format!(
            "receiver session {session_id} did not complete successfully"
        ));
    }

    let transfer_finished_at = Instant::now();
    let transfer_started_at = progress.first_payload_unit_at().ok_or_else(|| {
        format!("receiver session {session_id} completed without recording payload arrival")
    })?;

    let sink_bytes = sink.lock().await.clone();
    if sink_bytes.is_empty() {
        return Err("receiver sink is empty".to_string());
    }
    write_artifact_with_hash(
        &receiver_artifact_path(harness_cfg, config.node_id),
        &sink_bytes,
    )?;
    write_performance_metrics(
        harness_cfg,
        config.node_id,
        IntegrationNodeRole::Receiver,
        sink_bytes.len() as u64,
        transfer_finished_at.saturating_duration_since(transfer_started_at),
    )?;
    write_status(
        harness_cfg,
        config.node_id,
        IntegrationNodeRole::Receiver,
        "ok",
    )?;
    info!(
        case = %harness_cfg.case_name,
        node_id = config.node_id,
        session_id,
        "Integration test receiver finished successfully."
    );
    Ok(())
}

async fn wait_for_ready_receivers(
    cfg: &IntegrationTestConfig,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cfg
            .receiver_ids
            .iter()
            .all(|node_id| ready_marker_path(cfg, *node_id).exists())
        {
            return Ok(());
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for receiver readiness markers in {}",
                cfg.artifact_dir
            ));
        }

        tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
    }
}

async fn wait_for_group_info(
    cfg: &IntegrationTestConfig,
    timeout: Duration,
) -> Result<(usize, Ipv4Addr, usize), String> {
    let path = group_info_path(cfg);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if path.exists() {
            let contents = fs::read_to_string(&path)
                .map_err(|err| format!("failed to read group info {}: {err}", path.display()))?;
            return parse_group_info(&contents);
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {}", path.display()));
        }

        tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
    }
}

fn parse_group_info(contents: &str) -> Result<(usize, Ipv4Addr, usize), String> {
    let mut group_id = None;
    let mut group_ip = None;
    let mut source_node_id = None;

    for line in contents.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key.trim() {
            "group_id" => {
                group_id = value.trim().parse::<usize>().ok();
            }
            "group_ip" => {
                group_ip = value.trim().parse::<Ipv4Addr>().ok();
            }
            "source_node_id" => {
                source_node_id = value.trim().parse::<usize>().ok();
            }
            _ => {}
        }
    }

    match (group_id, group_ip, source_node_id) {
        (Some(group_id), Some(group_ip), Some(source_node_id)) => {
            Ok((group_id, group_ip, source_node_id))
        }
        _ => Err("group info file is incomplete".to_string()),
    }
}

fn write_group_info(
    cfg: &IntegrationTestConfig,
    group_id: usize,
    group_ip: Ipv4Addr,
    source_node_id: usize,
) -> Result<(), String> {
    let path = group_info_path(cfg);
    let payload =
        format!("group_id={group_id}\ngroup_ip={group_ip}\nsource_node_id={source_node_id}\n");
    fs::write(&path, payload)
        .map_err(|err| format!("failed to write group info {}: {err}", path.display()))
}

fn write_ready_marker(cfg: &IntegrationTestConfig, node_id: usize) -> Result<(), String> {
    fs::write(ready_marker_path(cfg, node_id), "ready\n")
        .map_err(|err| format!("failed to write readiness marker for node {node_id}: {err}"))
}

fn write_status(
    cfg: &IntegrationTestConfig,
    node_id: usize,
    role: IntegrationNodeRole,
    status: &str,
) -> Result<(), String> {
    fs::write(status_path(cfg, node_id, role), format!("{status}\n"))
        .map_err(|err| format!("failed to write status for node {node_id}: {err}"))
}

fn write_performance_metrics(
    cfg: &IntegrationTestConfig,
    node_id: usize,
    role: IntegrationNodeRole,
    payload_bytes: u64,
    duration: Duration,
) -> Result<(), String> {
    let duration_seconds = duration.as_secs_f64();
    let throughput_gbps = if duration_seconds > 0.0 {
        (payload_bytes as f64 * 8.0) / duration_seconds / 1_000_000_000.0
    } else {
        0.0
    };
    let path = performance_metrics_path(cfg, node_id, role);
    let role_label = match role {
        IntegrationNodeRole::Source => "source",
        IntegrationNodeRole::Receiver => "receiver",
        IntegrationNodeRole::Router => "router",
    };
    let payload = format!(
        "role={role_label}\nnode_id={node_id}\npayload_bytes={payload_bytes}\nduration_seconds={duration_seconds:.9}\nthroughput_gbps={throughput_gbps:.9}\n"
    );
    fs::write(&path, payload).map_err(|err| {
        format!(
            "failed to write performance metrics for node {node_id} to {}: {err}",
            path.display()
        )
    })
}

fn group_info_path(cfg: &IntegrationTestConfig) -> PathBuf {
    Path::new(&cfg.artifact_dir).join("group-info.txt")
}

fn ready_marker_path(cfg: &IntegrationTestConfig, node_id: usize) -> PathBuf {
    Path::new(&cfg.artifact_dir).join(format!("receiver-ready-{node_id}.txt"))
}

fn status_path(cfg: &IntegrationTestConfig, node_id: usize, role: IntegrationNodeRole) -> PathBuf {
    let label = match role {
        IntegrationNodeRole::Source => "source",
        IntegrationNodeRole::Receiver => "receiver",
        IntegrationNodeRole::Router => "router",
    };
    Path::new(&cfg.artifact_dir).join(format!("{label}-{node_id}.status"))
}

fn performance_metrics_path(
    cfg: &IntegrationTestConfig,
    node_id: usize,
    role: IntegrationNodeRole,
) -> PathBuf {
    let label = match role {
        IntegrationNodeRole::Source => "source",
        IntegrationNodeRole::Receiver => "receiver",
        IntegrationNodeRole::Router => "router",
    };
    Path::new(&cfg.artifact_dir).join(format!("{label}-{node_id}.metrics"))
}

fn source_artifact_path(cfg: &IntegrationTestConfig) -> PathBuf {
    Path::new(&cfg.artifact_dir).join("source.bin")
}

fn receiver_artifact_path(cfg: &IntegrationTestConfig, node_id: usize) -> PathBuf {
    Path::new(&cfg.artifact_dir).join(format!("receiver-{node_id}.bin"))
}

fn multicast_session_id(group_id: u64, source_node_id: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    group_id.hash(&mut hasher);
    source_node_id.hash(&mut hasher);
    let raw = hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF;
    raw | 0x8000_0000_0000_0000
}
