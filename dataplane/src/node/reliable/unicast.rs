use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tracing::{debug, error, warn};

use nextmini_messages::{Flow, FlowLen, TokenBucketSpec};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::reliable::api::{ReliableHandle, SessionId};
use crate::node::reliable::session::{
    CommonConfig, PendingReceiverKey, ReceiverConfig, SenderConfig,
};
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
            // Flows can involve the local node as the sender, receiver, or both
            // (loopback). Spin up whichever side matches.
            if flow.src_node_id == self.cfg.node_id {
                self.spawn_sender(flow.clone());
            }
            if flow.dst_node_id == self.cfg.node_id {
                self.spawn_receiver(flow.clone());
            }
        }
    }

    fn spawn_sender(&self, flow: Flow) {
        // The controller might hand us duration-based flows that do not resolve
        // to a byte count; we skip those early so we do not start half-baked
        // sessions.
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

            // We currently inject a fixed pattern; higher-level APIs fill the
            // buffer before the flow is scheduled.
            let source_buffer = Bytes::from(vec![0xAAu8; total_bytes_usize]);

            let common = CommonConfig {
                session_id: sid,
                dest_ip: dst_ip,
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
                // Update the processor scheduler before any packets leave the
                // node so the control plane's prioritization takes effect
                // immediately.
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                processors.set_flow_weight(flow_id, weight);
            }

            if let Some(controller_id) = flow.controller_id {
                // Report flow start once we know the flow ID so the controller
                // can track successes as soon as the sender is live.
                let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);
                flowstats.report_user_flow_start(flow_id, controller_id);
            } else {
                warn!(
                    "ReliableUnicastFlow: flow {:?}->{:?} missing controller_id; start not reported",
                    flow.src_node_id, flow.dst_node_id
                );
            }

            let started_sid = reliable.start_sender(sender_cfg).await;
            reliable.set_dest_routes_ready(dst_ip, cfg.node_id);
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
        // The receiver mirrors the sender's byte budget so the two sides agree
        // on when to terminate.
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
            let reliable_cfg = cfg.reliable.clone();
            let dest_ip =
                (flow.dst_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let data_bucket =
                bucket_from_flow_rate(flow.flow_spec.flow_rate, &reliable_cfg.data_bucket);

            let common = CommonConfig {
                // Placeholder; will be overwritten by adopt_pending_receiver.
                session_id: 0,
                dest_ip,
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

            let key = PendingReceiverKey {
                dest_ip,
                source_node_id: flow.src_node_id,
            };

            // Stage the receiver; the runtime will materialize it when the first frame arrives.
            let started_sid = reliable.start_receiver_pending(receiver_cfg, key).await;
            let _ = reliable.wait_completion(started_sid).await;
            reliable.stop(started_sid);
        });
    }
}

/// Generates a deterministic session ID for a flow so senders and receivers can
/// rendezvous without additional signaling.
fn session_id_for_flow(flow: &Flow) -> SessionId {
    let mut hasher = DefaultHasher::new();
    flow.controller_id.hash(&mut hasher);
    flow.src_node_id.hash(&mut hasher);
    flow.dst_node_id.hash(&mut hasher);
    flow.flow_spec.flow_len.hash(&mut hasher);
    let raw = hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF;
    raw | 0x8000_0000_0000_0000
}

/// Converts user-visible flow specs into the exact number of bytes the runtime
/// should push across the wire.
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

/// Builds a per-flow token bucket so bandwidth can be shaped in line with the
/// controller's desired rate, falling back to the reliable defaults when no
/// rate override was provided.
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

/// Encodes the 5-tuple identifying a unicast flow into the 128-bit identifier
/// expected by the dataplane processor.
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
