use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tracing::{debug, error, warn};

use nextmini_messages::{Flow, FlowLen, TokenBucketSpec};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::reliable::api::{ReliableHandle, SessionId};
use crate::node::reliable::session::{CommonConfig, ReceiverConfig, SenderConfig};
use crate::node::{FlowId, NodeId, NodeIdExt};

/// Handles controller-managed reliable unicast flows on a dataplane node.
#[derive(Clone)]
pub struct ReliableUnicastFlowHandle {
    cfg: LocalConfig,
    processors: ProcessorHandle,
    flowstats: FlowStatsReporterHandle,
    reliable: ReliableHandle,
}

impl ReliableUnicastFlowHandle {
    pub fn new(
        cfg: LocalConfig,
        processors: ProcessorHandle,
        flowstats: FlowStatsReporterHandle,
        reliable: ReliableHandle,
    ) -> Self {
        Self {
            cfg,
            processors,
            flowstats,
            reliable,
        }
    }

    /// Installs any reliable-unicast flows that target the local node (as source and/or destination).
    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            if flow.src_node_id == self.cfg.node_id {
                self.spawn_sender(flow.clone());
            }
            if flow.dst_node_id == self.cfg.node_id {
                self.spawn_receiver(flow.clone());
            }
        }
    }

    fn spawn_sender(&self, flow: Flow) {
        let Some(total_bytes) = flow_bytes(&flow) else {
            return;
        };
        if total_bytes == 0 {
            warn!("ReliableUnicastFlow: sender received zero-byte flow; skipping");
            return;
        }

        let cfg = self.cfg.clone();
        let processors = self.processors.clone();
        let flowstats = self.flowstats.clone();
        let reliable = self.reliable.clone();

        tokio::spawn(async move {
            let sid = session_id_for_flow(&flow);
            let reliable_cfg = cfg.reliable.clone();
            let dst_ip =
                (flow.dst_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let src_port = cfg.user_space_client_port;
            let dst_port = cfg.user_space_server_port;
            let data_bucket =
                bucket_from_flow_rate(flow.flow_spec.flow_rate, &reliable_cfg.data_bucket);

            let total_bytes_usize = match usize::try_from(total_bytes) {
                Ok(value) => value,
                Err(_) => {
                    error!(
                        controller_id = flow.controller_id,
                        bytes = total_bytes,
                        "ReliableUnicastFlow: flow too large to allocate payload buffer"
                    );
                    return;
                }
            };

            let source_buffer = Bytes::from(vec![0xAAu8; total_bytes_usize]);

            let common = CommonConfig {
                session_id: sid,
                group_ip: dst_ip,
                chunk_size: reliable_cfg.default_chunk_size,
                src_port,
                dst_port,
                control_weight: reliable_cfg.control_weight,
                data_bucket,
                local_node_id: cfg.node_id,
                user_space_base_addr: cfg.user_space_base_addr,
                local_netmask: cfg.local_netmask,
            };

            let sender_cfg = SenderConfig {
                common,
                receiver_ids: vec![flow.dst_node_id],
                total_bytes,
                source_buffer,
                ready_grace_ms: reliable_cfg.ready_grace_ms,
                topology_ready: None,
                routes_ready: None,
            };

            if let Some(weight) = flow.flow_spec.flow_weight {
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                processors.set_flow_weight(flow_id, weight);
            }

            if let Some(controller_id) = flow.controller_id {
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                flowstats.report_user_flow_start(flow_id, controller_id);
            } else {
                warn!(
                    "ReliableUnicastFlow: flow {:?}->{:?} missing controller_id; start not reported",
                    flow.src_node_id, flow.dst_node_id
                );
            }

            let started_sid = reliable.start_sender(sender_cfg).await;
            reliable.set_group_routes_ready(dst_ip, cfg.node_id);
            let ok = reliable.wait_completion(started_sid).await;

            let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
            flowstats.report_flow_finished(flow_id, flow.controller_id);

            if !ok {
                warn!(
                    session_id = started_sid,
                    "ReliableUnicastFlow: sender completion reported failure"
                );
            } else {
                debug!(
                    session_id = started_sid,
                    "ReliableUnicastFlow: sender finished"
                );
            }

            reliable.stop(started_sid);
        });
    }

    fn spawn_receiver(&self, flow: Flow) {
        let Some(expected_bytes) = flow_bytes(&flow) else {
            return;
        };
        if expected_bytes == 0 {
            warn!("ReliableUnicastFlow: receiver expected zero bytes; skipping");
            return;
        }

        let cfg = self.cfg.clone();
        let reliable = self.reliable.clone();

        tokio::spawn(async move {
            let sid = session_id_for_flow(&flow);
            let reliable_cfg = cfg.reliable.clone();
            let dst_ip =
                (flow.dst_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let data_bucket =
                bucket_from_flow_rate(flow.flow_spec.flow_rate, &reliable_cfg.data_bucket);

            let common = CommonConfig {
                session_id: sid,
                group_ip: dst_ip,
                chunk_size: reliable_cfg.default_chunk_size,
                src_port: cfg.user_space_client_port,
                dst_port: cfg.user_space_server_port,
                control_weight: reliable_cfg.control_weight,
                data_bucket,
                local_node_id: cfg.node_id,
                user_space_base_addr: cfg.user_space_base_addr,
                local_netmask: cfg.local_netmask,
            };

            let receiver_cfg = ReceiverConfig {
                common,
                source_node_id: flow.src_node_id,
                expected_bytes,
                sink_buffer: None,
            };

            let started_sid = reliable.start_receiver(receiver_cfg).await;
            let _ = reliable.wait_completion(started_sid).await;
            reliable.stop(started_sid);
        });
    }
}

fn session_id_for_flow(flow: &Flow) -> SessionId {
    let mut hasher = DefaultHasher::new();
    flow.controller_id.hash(&mut hasher);
    flow.src_node_id.hash(&mut hasher);
    flow.dst_node_id.hash(&mut hasher);
    flow.flow_spec.flow_len.hash(&mut hasher);
    let raw = hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF;
    raw | 0x8000_0000_0000_0000
}

fn flow_bytes(flow: &Flow) -> Option<u64> {
    match flow.flow_spec.flow_len {
        FlowLen::Bytes(bytes) => Some(bytes as u64),
        FlowLen::Duration(secs) => {
            let Some(rate) = flow.flow_spec.flow_rate else {
                warn!(
                    controller_id = flow.controller_id,
                    "ReliableUnicastFlow: duration-based flow without flow_rate"
                );
                return None;
            };
            let seconds = secs.max(0.0);
            let bytes = (seconds * rate as f64).round();
            Some(bytes.max(0.0) as u64)
        }
    }
}

fn bucket_from_flow_rate(
    flow_rate: Option<usize>,
    default_bucket: &Option<TokenBucketSpec>,
) -> Option<TokenBucketSpec> {
    if let Some(rate) = flow_rate {
        Some(TokenBucketSpec {
            rate,
            bucket_size: rate.saturating_mul(2),
        })
    } else {
        default_bucket.clone()
    }
}

fn flow_id_for_unicast(cfg: &LocalConfig, flow: &Flow, src_port: u16, dst_port: u16) -> FlowId {
    let src_ip = flow
        .src_node_id
        .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
    let dst_ip = flow
        .dst_node_id
        .ip_addr(cfg.user_space_base_addr, cfg.local_netmask);

    ((u32::from(src_ip) as u128) << 96)
        | ((u32::from(dst_ip) as u128) << 64)
        | ((src_port as u128) << 48)
        | ((dst_port as u128) << 32)
}
