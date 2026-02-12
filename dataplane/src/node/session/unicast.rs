use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tracing::{debug, warn};

use nextmini_messages::lossless_session::FecCapabilities;
use nextmini_messages::{Flow, FlowLen, TokenBucketSpec};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{LosslessRuntimeHandle, SessionId};
use crate::node::session::runtime::{CommonConfig, ReceiverConfig, SenderConfig};
use crate::node::{FlowId, NodeId, NodeIdExt};

/// Manages controller-assigned lossless unicast flows on a dataplane node.
#[derive(Clone)]
pub struct LosslessUnicastFlowManager {
    cfg: LocalConfig,
    processors: ProcessorHandle,
    flowstats: FlowStatsReporterHandle,
    lossless_runtime: LosslessRuntimeHandle,
}

impl LosslessUnicastFlowManager {
    pub fn new(
        cfg: LocalConfig,
        processors: ProcessorHandle,
        flowstats: FlowStatsReporterHandle,
        lossless_runtime: LosslessRuntimeHandle,
    ) -> Self {
        Self {
            cfg,
            processors,
            flowstats,
            lossless_runtime,
        }
    }

    /// Installs any lossless unicast flows that target the local node (as source and/or destination).
    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            // Compute deterministic session_id and client_port from Flow fields.
            // Both sender and receiver compute the same values, enabling proper matching.
            let session_id = session_id_for_flow(&flow);
            let client_port = client_port_for_flow(&flow, self.cfg.user_space_client_port);

            // Flows can involve the local node as the sender, receiver, or both
            // (loopback). Spin up whichever side matches.
            if flow.src_node_id == self.cfg.node_id {
                self.spawn_sender(flow.clone(), session_id, client_port);
            }
            if flow.dst_node_id == self.cfg.node_id {
                self.spawn_receiver(flow.clone(), session_id, client_port);
            }
        }
    }

    fn spawn_sender(&self, flow: Flow, session_id: SessionId, client_port: u16) {
        // The controller might hand us duration-based flows that do not resolve
        // to a byte count; we skip those early so we do not start half-baked
        // sessions.
        let Some(total_bytes) = flow_bytes(&flow) else {
            return;
        };
        if total_bytes == 0 {
            warn!("LosslessUnicastFlow: sender received zero-byte flow; skipping");
            return;
        }

        let cfg = self.cfg.clone();
        let processors = self.processors.clone();
        let flowstats = self.flowstats.clone();
        let lossless_runtime = self.lossless_runtime.clone();

        tokio::spawn(async move {
            let runtime_config = cfg.lossless_runtime_config.clone();
            let dst_ip =
                (flow.dst_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let src_port = client_port;
            let dst_port = cfg.user_space_server_port;
            let data_bucket =
                bucket_from_flow_rate(flow.flow_spec.flow_rate, &runtime_config.data_bucket);
            let flow_id = flow_id_for_unicast(&cfg, &flow, src_port, dst_port);

            if let Some(route_id) = flow.route_id {
                processors.pin_route_for_flow(flow_id, route_id);
            }

            // We currently inject a fixed pattern; higher-level APIs fill the
            // buffer before the flow is scheduled. Reuse a single chunk-sized
            // template instead of allocating the entire payload up front.
            let template_len = runtime_config.default_chunk_size.max(1);
            let source_buffer = Bytes::from(vec![0xAAu8; template_len]);

            let common = CommonConfig {
                session_id,
                dest_ip: dst_ip,
                chunk_size: runtime_config.default_chunk_size,
                src_port,
                dst_port,
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
                fec_manifest: None,
                fec_num_trees: None,
                ready_grace_ms: runtime_config.ready_grace_ms,
                topology_ready: None,
            };

            if let Some(weight) = flow.flow_spec.flow_weight {
                // Update the processor scheduler before any packets leave the
                // node so the control plane's prioritization takes effect
                // immediately.
                processors.set_flow_weight(flow_id, weight);
            }

            if let Some(controller_id) = flow.controller_id {
                // Report flow start once we know the flow ID so the controller
                // can track successes as soon as the sender is live.
                flowstats.report_user_flow_start(flow_id, controller_id);
            } else {
                warn!(
                    "LosslessUnicastFlow: flow {:?}->{:?} missing controller_id; start not reported",
                    flow.src_node_id, flow.dst_node_id
                );
            }

            let started_sid = lossless_runtime.start_sender(sender_cfg).await;
            let ok = lossless_runtime.wait_completion(started_sid).await;

            flowstats.report_flow_finished(flow_id, flow.controller_id);

            if !ok {
                warn!(
                    session_id = started_sid,
                    "LosslessUnicastFlow: sender completion reported failure"
                );
            } else {
                debug!(
                    session_id = started_sid,
                    "LosslessUnicastFlow: sender finished"
                );
            }

            lossless_runtime.stop(started_sid);
        });
    }

    fn spawn_receiver(&self, flow: Flow, session_id: SessionId, client_port: u16) {
        // The receiver mirrors the sender's byte budget so the two sides agree
        // on when to terminate.
        let Some(expected_bytes) = flow_bytes(&flow) else {
            return;
        };
        if expected_bytes == 0 {
            warn!("LosslessUnicastFlow: receiver expected zero bytes; skipping");
            return;
        }

        let cfg = self.cfg.clone();
        let lossless_runtime = self.lossless_runtime.clone();

        tokio::spawn(async move {
            let runtime_config = cfg.lossless_runtime_config.clone();
            let dest_ip =
                (flow.dst_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let src_port = client_port;
            let dst_port = cfg.user_space_server_port;
            let data_bucket =
                bucket_from_flow_rate(flow.flow_spec.flow_rate, &runtime_config.data_bucket);

            let common = CommonConfig {
                session_id,
                dest_ip,
                chunk_size: runtime_config.default_chunk_size,
                src_port,
                dst_port,
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
                fec_capabilities: FecCapabilities::default(),
            };

            // Register receiver directly with the pre-computed session_id.
            // Both sender and receiver compute the same session_id from Flow fields,
            // so packets will be routed correctly.
            let started_sid = lossless_runtime.start_receiver(receiver_cfg).await;
            let _ = lossless_runtime.wait_completion(started_sid).await;
            lossless_runtime.stop(started_sid);
        });
    }
}

/// Generates a deterministic session ID from Flow fields.
/// Both sender and receiver compute the same session_id, enabling direct matching
/// without needing the pending receiver mechanism.
fn session_id_for_flow(flow: &Flow) -> SessionId {
    let mut hasher = DefaultHasher::new();
    flow.controller_id.hash(&mut hasher);
    flow.src_node_id.hash(&mut hasher);
    flow.dst_node_id.hash(&mut hasher);
    flow.flow_spec.flow_len.hash(&mut hasher);
    let raw = hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF;
    raw | 0x8000_0000_0000_0000
}

/// Generates a deterministic client port from Flow fields.
/// Both sender and receiver compute the same port, ensuring ACKs are routed correctly.
/// Uses controller_id directly (not hashed) since it's unique per flow.
fn client_port_for_flow(flow: &Flow, base_port: u16) -> u16 {
    // controller_id is unique per flow from the controller, use it directly
    // to guarantee unique ports for concurrent flows.
    let offset = flow.controller_id.unwrap_or(0) as u16;
    base_port.wrapping_add(offset)
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
                    "LosslessUnicastFlow: duration-based flow without flow_rate"
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
/// controller's desired rate, falling back to the lossless defaults when no
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

    Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port)
}
