//! Controller-facing orchestration for lossless unicast flows.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tracing::{debug, warn};

use nextmini_messages::{Flow, FlowLen, TokenBucketSpec};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{LosslessRuntimeHandle, SessionId, SessionOutcome};
use crate::node::session::runtime::{
    ReceiverRequest, SenderRequest, SessionConfig, TransportRoute,
};
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
    /// Construct a flow manager backed by the shared lossless runtime.
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

    /// Install controller-assigned flows that involve the local node.
    pub fn add_flows(&self, flows: Vec<Flow>) {
        for flow in flows {
            // Compute deterministic session_id and client_port from Flow fields.
            // Both sender and receiver compute the same values, enabling proper matching.
            let session_id = session_id_for_flow(&flow);
            let client_port = client_port_for_flow(&flow, self.cfg.user_space_client_port);

            // Spin up whichever side matches the local node.
            if flow.src_node_id == self.cfg.node_id {
                self.run_sender_flow(flow.clone(), session_id, client_port);
            }
            if flow.dst_node_id == self.cfg.node_id {
                self.run_receiver_flow(flow.clone(), session_id, client_port);
            }
        }
    }

    /// Start the sender side of one controller-assigned lossless flow.
    fn run_sender_flow(&self, flow: Flow, session_id: SessionId, client_port: u16) {
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
            let src_ip =
                (cfg.node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
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
            // buffer before the flow is scheduled. Build the exact payload here
            // so the session sender remains a straightforward block slicer.
            let Ok(source_len) = usize::try_from(total_bytes) else {
                warn!(
                    flow_id = flow_id,
                    total_bytes, "LosslessUnicastFlow: flow too large for explicit source buffer"
                );
                return;
            };
            let source_buffer = Bytes::from(vec![0xAAu8; source_len]);

            let session = SessionConfig {
                session_id,
                block_size: runtime_config.default_block_size,
            };
            let route = TransportRoute {
                src_ip,
                dst_ip,
                src_port,
                dst_port,
            };

            let sender_cfg = SenderRequest {
                session,
                route,
                pacing: data_bucket,
                receiver_ids: vec![flow.dst_node_id],
                total_bytes,
                source_buffer,
                ready_grace_ms: runtime_config.ready_grace_ms,
                peer_report_timeout_ms: runtime_config.peer_report_timeout_ms,
            };

            if let Some(weight) = flow.flow_spec.flow_weight {
                // Update the processor scheduler before any packets leave the
                // node so the control plane's prioritization takes effect
                // immediately.
                processors.set_flow_weight(flow_id, weight);
            }

            let mut session = match lossless_runtime.start_sender(sender_cfg).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        flow_id = flow_id,
                        reason = %err,
                        "LosslessUnicastFlow: sender start rejected by runtime preflight"
                    );
                    flowstats.report_flow_finished(flow_id, flow.controller_id);
                    return;
                }
            };
            let session_id = session.id();

            if let Some(controller_id) = flow.controller_id {
                flowstats.report_user_flow_start(flow_id, controller_id);
            } else {
                warn!(
                    "LosslessUnicastFlow: flow {:?}->{:?} missing controller_id; start not reported",
                    flow.src_node_id, flow.dst_node_id
                );
            }

            let outcome = session.wait().await;

            flowstats.report_flow_finished(flow_id, flow.controller_id);

            match outcome {
                SessionOutcome::Completed => {
                    debug!(session_id, "LosslessUnicastFlow: sender finished");
                }
                SessionOutcome::Aborted => {
                    warn!(
                        session_id,
                        "LosslessUnicastFlow: sender completion reported failure"
                    );
                }
            }
        });
    }

    /// Start the receiver side of one controller-assigned lossless flow.
    fn run_receiver_flow(&self, flow: Flow, session_id: SessionId, client_port: u16) {
        let Some(_total_bytes) = flow_bytes(&flow) else {
            return;
        };
        if _total_bytes == 0 {
            warn!("LosslessUnicastFlow: receiver received zero-byte flow; skipping");
            return;
        }

        let cfg = self.cfg.clone();
        let lossless_runtime = self.lossless_runtime.clone();

        tokio::spawn(async move {
            let src_ip =
                (cfg.node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let dst_ip =
                (flow.src_node_id as NodeId).ip_addr(cfg.user_space_base_addr, cfg.local_netmask);
            let src_port = client_port;
            let dst_port = cfg.user_space_server_port;
            let route = TransportRoute {
                src_ip,
                dst_ip,
                src_port,
                dst_port,
            };

            let receiver_cfg = ReceiverRequest {
                session_id,
                route,
                local_node_id: cfg.node_id,
                sink_buffer: None,
                sink_file: None,
                progress: None,
            };

            // Register receiver directly with the pre-computed session_id.
            // Both sender and receiver compute the same session_id from Flow fields,
            // so packets will be routed correctly.
            let mut session = match lossless_runtime.start_receiver(receiver_cfg).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        session_id,
                        reason = %err,
                        "LosslessUnicastFlow: receiver start rejected by runtime"
                    );
                    return;
                }
            };
            let _ = session.wait().await;
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

#[cfg(test)]
mod tests {
    use super::*;

    use nextmini_messages::{FlowSpec, FlowTransport};

    fn lossless_flow(flow_len: FlowLen) -> Flow {
        Flow {
            controller_id: Some(17),
            src_node_id: 3,
            dst_node_id: 7,
            route_id: Some(11),
            flow_spec: FlowSpec {
                flow_len,
                flow_rate: Some(200),
                flow_weight: Some(5),
                transport: FlowTransport::LosslessUnicast,
            },
        }
    }

    #[test]
    fn session_id_for_flow_is_deterministic() {
        let flow = lossless_flow(FlowLen::Bytes(4096));
        assert_eq!(session_id_for_flow(&flow), session_id_for_flow(&flow));
    }

    #[test]
    fn client_port_for_flow_uses_controller_id_offset() {
        let flow = lossless_flow(FlowLen::Bytes(1));
        assert_eq!(client_port_for_flow(&flow, 4000), 4017);
    }

    #[test]
    fn flow_bytes_returns_exact_byte_length() {
        let flow = lossless_flow(FlowLen::Bytes(4096));
        assert_eq!(flow_bytes(&flow), Some(4096));
    }

    #[test]
    fn flow_bytes_derives_duration_length_from_rate() {
        let flow = lossless_flow(FlowLen::Duration(2.5));
        assert_eq!(flow_bytes(&flow), Some(500));
    }

    #[test]
    fn flow_bytes_rejects_duration_without_rate() {
        let mut flow = lossless_flow(FlowLen::Duration(2.5));
        flow.flow_spec.flow_rate = None;
        assert_eq!(flow_bytes(&flow), None);
    }

    #[test]
    fn bucket_from_flow_rate_uses_override_or_default() {
        let default = Some(TokenBucketSpec {
            rate: 100,
            bucket_size: 300,
        });

        assert_eq!(
            bucket_from_flow_rate(Some(250), &default),
            Some(TokenBucketSpec {
                rate: 250,
                bucket_size: 500,
            })
        );
        assert_eq!(bucket_from_flow_rate(None, &default), default);
    }
}
