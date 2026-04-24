use ahash::AHashMap;
use chrono::Utc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::error;

use nextmini_messages::{DataplaneToController, Metric};

use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::{FlowId, NodeId};

pub struct FlowMetric {
    pub flow_id: FlowId,
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    pub bytes: usize,
}

pub enum FlowMetricMessage {
    FlowMetric(FlowMetric),
    ProbeResult {
        probe_id: u64,
        from_node_id: usize,
        to_node_id: usize,
        bandwidth_mbps: f64,
    },
}

#[derive(Debug, Clone)]
pub struct ControllerReporterHandle {
    sender: UnboundedSender<FlowMetricMessage>,
}

impl ControllerReporterHandle {
    pub fn new(controller: ControllerInterfaceHandle, interval_secs: u64) -> Self {
        let (sender, receiver) = unbounded_channel();

        let mut reporter = ControllerReporter::new(controller, receiver, interval_secs);

        tokio::spawn(async move {
            reporter.run().await;
        });

        Self { sender }
    }

    pub fn send_probe_result(
        &self,
        probe_id: u64,
        from_node_id: usize,
        to_node_id: usize,
        bandwidth_mbps: f64,
    ) {
        if let Err(e) = self.sender.send(FlowMetricMessage::ProbeResult {
            probe_id,
            from_node_id,
            to_node_id,
            bandwidth_mbps,
        }) {
            error!(
                "Error sending probe result to the controller reporter: {}",
                e
            );
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
}

pub struct ControllerReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowMetricMessage>,
    flow_metrics: AHashMap<FlowId, FlowMetric>,
    interval_secs: u64,
}

impl ControllerReporter {
    pub fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowMetricMessage>,
        interval_secs: u64,
    ) -> Self {
        Self {
            controller,
            receiver,
            flow_metrics: AHashMap::default(),
            interval_secs: interval_secs.max(1),
        }
    }

    pub async fn run(&mut self) {
        // transmits metrics every interval
        let mut metrics_tick = interval(Duration::from_secs(self.interval_secs));

        loop {
            tokio::select! {
                // receives new metrics data
                Some(msg) = self.receiver.recv() => {
                    match msg {
                        FlowMetricMessage::ProbeResult { probe_id, from_node_id, to_node_id, bandwidth_mbps } => {
                            self.controller.send(DataplaneToController::ProbeLinkResult {
                                probe_id,
                                from_node_id,
                                to_node_id,
                                bandwidth_mbps,
                            }).await;
                        }
                        FlowMetricMessage::FlowMetric(metric) => {
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
                    }
                }
                // timer tick: calculates flow rates and transmits to the controller
                _ = metrics_tick.tick() => {
                    if !self.flow_metrics.is_empty() {
                        let now = Utc::now();
                        let mut metrics_array = Vec::new();

                        for flow_metric in self.flow_metrics.values() {
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

                            self.controller.send(msg).await;
                        }

                        self.flow_metrics.clear();
                    }
                }
            }
        }
    }
}
