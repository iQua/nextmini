use ahash::AHashMap;
use chrono::Utc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::error;

use nextmini_messages::{DataplaneToController, LinkProbeResult, Metric};

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
}

#[derive(Clone)]
pub struct ControllerReporterHandle {
    sender: UnboundedSender<FlowMetricMessage>,
    controller: ControllerInterfaceHandle,
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
        let mut metrics_tick = interval(Duration::from_secs(5));

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
