use ahash::AHashSet;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::error;

use nextmini_messages::{AppFlow, DataplaneToController};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::{FlowId, NodeId};

pub struct AppFlowInfo {
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
    AppFlowStart(AppFlowInfo),
    RouteAssigned(RouteAssigned),
    FlowFinished(FlowFinished),
}

#[derive(Debug, Clone)]
pub struct FlowStatsReporterHandle {
    sender: UnboundedSender<FlowStatsMessage>,
    config: LocalConfig,
}

impl FlowStatsReporterHandle {
    pub fn new(controller: ControllerInterfaceHandle, config: LocalConfig) -> Self {
        let (sender, receiver) = unbounded_channel();

        let mut flowstats_reporter = FlowStatsReporter::new(controller, receiver);

        tokio::spawn(async move {
            flowstats_reporter.run().await;
        });

        Self { sender, config }
    }

    pub fn report_app_flow(&self, flow_id: FlowId) {
        let (src_node_id, dst_node_id) = self.config.extract_node_ids_from_flow(flow_id);

        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::AppFlowStart(AppFlowInfo {
                flow_id,
                src_node_id,
                dst_node_id,
            }))
        {
            error!(
                "Error sending app flow message to the flowstats reporter: {}.",
                e
            );
        }
    }

    pub fn report_route_assigned(&self, flow_id: FlowId, route_id: usize) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::RouteAssigned(RouteAssigned {
                flow_id,
                route_id,
            }))
        {
            error!(
                "Error sending route assigned message to the flowstats reporter: {}.",
                e
            );
        }
    }

    pub fn report_flow_finished(&self, flow_id: FlowId, controller_id: Option<i32>) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::FlowFinished(FlowFinished {
                flow_id,
                controller_id,
            }))
        {
            error!(
                "Error sending flow finished message to the flowstats reporter: {}.",
                e
            );
        }
    }

    // since userspace flows doesn't need to check if received FIN/RST,
    // we can report them directly
    pub fn check_and_report_finished(&self, packet: &Packet) {
        if packet.is_tcp_fin_or_rst() {
            self.report_flow_finished(packet.flow_id, None);
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,
    flow_metrics: AHashMap<FlowId, FlowMetric>,
}

impl FlowStatsReporter {
    fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowStatsMessage>,
    ) -> Self {
        Self {
            controller,
            receiver,
            flow_metrics: AHashMap::default(),
        }
    }

    pub async fn run(&mut self) {
        // transmits app flows every 5 seconds
        let mut flowstats_tick = interval(Duration::from_secs(5));

        loop {
            tokio::select! {
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
                _ = flowstats_tick.tick() => {
                    if !self.app_flows.is_empty() {
                        let mut appflows = Vec::new();

                        for app_flow in &self.app_flows {
                            appflows.push(AppFlow {
                                flow_id: app_flow.flow_id.to_be_bytes(),
                                src_node_id: app_flow.src_node_id,
                                dst_node_id: app_flow.dst_node_id,
                            });
                        }

                        if !appflows.is_empty() {
                            let msg = DataplaneToController::AppFlowStart {
                                appflows,
                            };
                            self.controller.send(msg).await;
                        }

                        self.app_flows.clear();
                    }
                }
            }
        }
    }
}
