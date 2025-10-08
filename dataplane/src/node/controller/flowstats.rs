use ahash::AHashMap;
use chrono::Utc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::error;

use nextmini_messages::{AppFlows, DataplaneToController};

use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::{FlowId, NodeId};

pub struct NewFlow {
    pub flow_id: FlowId,
    pub src_node_id: NodeId,
    pub dst_node_id: NodeId,
}

pub struct RouteAssigned {
    pub flow_id: FlowId,
    pub route_id: usize,
}

pub struct FlowFinished {
    pub flow_id: FlowId,
    pub controller_id: Option<i32>,
}

pub enum FlowStatsMessage {
    NewFlow(NewFlow),
    RouteAssigned(RouteAssigned),
    FlowFinished(FlowFinished),
}

#[derive(Debug, Clone)]
pub struct FlowStatsReporterHandle {
    sender: UnboundedSender<FlowStatsMessage>,
}

impl FlowStatsReporterHandle {
    pub fn new(controller: ControllerInterfaceHandle) -> Self {
        let (sender, receiver) = unbounded_channel();

        let mut flowstats_reporter = FlowStatsReporter::new(controller, receiver);

        tokio::spawn(async move {
            flowstats_reporter.run().await;
        });

        Self { sender }
    }

    pub fn report_new_appflows(&self, appflows: Vec<AppFlow>) {
        for appflow in appflows {
            if let Err(e) = self.sender.send(FlowStatsMessage::NewFlow) {
                error!(
                    "Error sending registering new app flow messages to the FlowStats reporter: {}.",
                    e
                );
            }
        }
    }

    pub fn report_flow_finished(&self, flow_finished: FlowFinished) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::FlowFinished(FlowFinished))
        {
            error!(
                "Error sending a flow finished message to the FlowStats reporter: {}.",
                e
            );
        }
    }

    pub fn report_route_assigned(&self, route_assigned: RouteAssigned) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::RouteAssigned(RouteAssigned))
        {
            error!(
                "Error sending a route assigned message to the FlowStats reporter: {}.",
                e
            );
        }
    }
}

pub struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,
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
                    match msg {
                        FlowMetricMessage::FlowMetric(metric) => {
                            let flow_metric = self.flow_metrics.entry(metric.flow_id).or_insert(
                                FlowMetric {
                                    flow_id: metric.flow_id,
                                    local_node_id: metric.local_node_id,
                                    remote_node_id: metric.remote_node_id,
                                    bytes: 0
                                });

                            (*flow_metric).bytes += metric.bytes;
                        }
                        FlowMetricMessage::FlowFinished(controller_id) => {
                            let msg = DataplaneToController::FlowFinished { controller_id };
                            self.controller.send(msg).await;
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
