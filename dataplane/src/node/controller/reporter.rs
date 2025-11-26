use std::fmt;

use ahash::AHashMap;
use chrono::Utc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::error;

use nextmini_messages::{DataplaneToController, LinkProbeResult, Metric};

use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::{FlowId, NodeId};

const METRICS_INTERVAL_SECS: u64 = 5;

pub struct FlowMetric {
    pub flow_id: FlowId,
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    pub bytes: usize,
}

pub enum FlowMetricMessage {
    FlowMetric(FlowMetric),
}

#[derive(Clone)]
pub struct ControllerReporterHandle {
    sender: UnboundedSender<FlowMetricMessage>,
    controller: ControllerInterfaceHandle,
}

impl fmt::Debug for ControllerReporterHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControllerReporterHandle").finish()
    }
}

impl ControllerReporterHandle {
    pub fn new(controller: ControllerInterfaceHandle) -> Self {
        let (sender, receiver) = unbounded_channel();
        let controller_clone = controller.clone();

        let mut reporter = ControllerReporter::new(controller, receiver);

        tokio::spawn(async move {
            reporter.run().await;
        });

        Self {
            sender,
            controller: controller_clone,
        }
    }

    pub fn send(&self, metrics: Vec<FlowMetric>) {
        for metric in metrics {
            if let Err(e) = self.sender.send(FlowMetricMessage::FlowMetric(metric)) {
                error!(
                    "Error sending a flow metric to the controller reporter: {}",
                    e
                );
            }
        }
    }

    pub async fn send_link_probe_results(&self, results: Vec<LinkProbeResult>) {
        if results.is_empty() {
            return;
        }

        self.controller
            .send(DataplaneToController::LinkProbeResults { results })
            .await;
    }
}

pub struct ControllerReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowMetricMessage>,
    flow_metrics: AHashMap<FlowId, FlowMetric>,
}

impl ControllerReporter {
    pub fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowMetricMessage>,
    ) -> Self {
        Self {
            controller,
            receiver,
            flow_metrics: AHashMap::default(),
        }
    }

    pub async fn run(&mut self) {
        // transmits metrics every 5 seconds
        let mut metrics_tick = interval(Duration::from_secs(METRICS_INTERVAL_SECS));

        loop {
            tokio::select! {
                // receives new metrics data
                Some(msg) = self.receiver.recv() => {
                    let FlowMetricMessage::FlowMetric(metric) = msg;

                    let flow_metric = self.flow_metrics.entry(metric.flow_id).or_insert(
                        FlowMetric {
                            flow_id: metric.flow_id,
                            local_node_id: metric.local_node_id,
                            remote_node_id: metric.remote_node_id,
                            bytes: 0,
                        },
                    );

                    flow_metric.bytes += metric.bytes;
                }
                // timer tick: calculates flow rates and transmits to the controller
                _ = metrics_tick.tick() => {
                    if !self.flow_metrics.is_empty() {
                        let now = Utc::now();
                        let mut metrics_array = Vec::new();
                        let mut per_remote: AHashMap<NodeId, (usize, u32)> = AHashMap::default();

                        for flow_metric in self.flow_metrics.values() {
                            per_remote
                                .entry(flow_metric.remote_node_id)
                                .and_modify(|(bytes, samples)| {
                                    *bytes += flow_metric.bytes;
                                    *samples += 1;
                                })
                                .or_insert((flow_metric.bytes, 1));

                            metrics_array.push(Metric {
                                flow_id: flow_metric.flow_id.to_be_bytes(),
                                bytes: flow_metric.bytes,
                                local_node_id: flow_metric.local_node_id,
                                remote_node_id: flow_metric.remote_node_id,
                                time_read: now,
                            });
                        }

                    if !metrics_array.is_empty() {
                        let msg = DataplaneToController::Metrics {
                            metrics: metrics_array,
                        };

                        let src_node_id = self.controller.config.node_id;

                        self.controller.send(msg).await;

                        let mut link_results = Vec::new();
                        let interval_secs = METRICS_INTERVAL_SECS as f64;
                        for (remote_node_id, (bytes, samples)) in per_remote {
                            if bytes == 0 {
                                continue;
                            }

                            let mbps = (bytes as f64 * 8.0) / interval_secs / 1_000_000.0;
                            link_results.push(LinkProbeResult {
                                src_node_id,
                                dst_node_id: remote_node_id,
                                rtt_ms: None,
                                loss_pct: None,
                                mbps: Some(mbps),
                                samples,
                                time_read: now,
                            });
                        }

                        if !link_results.is_empty() {
                            self.controller
                                .send(DataplaneToController::LinkProbeResults {
                                    results: link_results,
                                })
                                .await;
                        }
                    }

                        self.flow_metrics.clear();
                    }
                }
            }
        }
    }
}
