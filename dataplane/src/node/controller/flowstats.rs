use ahash::{AHashMap, AHashSet};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::{error, info};

use nextmini_messages::{AppFlow, DataplaneToController, FlowFinishedInfo, RouteAssignment};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::{FlowId, NodeId};

pub struct AppFlowStart {
    pub flow_id: FlowId,
    pub src_node_id: NodeId,
    pub dst_node_id: NodeId,
    pub time: i64,
}

pub struct RouteAssigned {
    pub flow_id: FlowId,
    pub route_id: usize,
    pub time: i64,
}

pub struct FlowFinished {
    pub flow_id: FlowId,
    pub controller_id: Option<i32>,
    pub time: i64,
}

pub enum FlowStatsMessage {
    AppFlowStart(AppFlowStart),
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

    /// Report packet-related flow stats: app flow start and flow finish (if FIN/RST).
    pub fn report_packet(&self, packet: &Packet) {
        // checks and reports if flow finished (FIN/RST)
        if packet.is_tcp_fin_or_rst() {
            self.report_flow_finished(packet.flow_id, None);
        } else {
            // only reports app flow start for non-terminating packets
            self.report_app_flow(packet.flow_id);
        }
    }

    /// Report route assigned for a flow.
    pub fn report_route_assigned(&self, flow_id: FlowId, route_id: usize) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::RouteAssigned(RouteAssigned {
                flow_id,
                route_id,
                time: 0, // will be filled by FlowStatsReporter
            }))
        {
            error!(
                "Error sending route assigned message to the flowstats reporter: {}.",
                e
            );
        }
    }

    /// Report app flow start for a flow.
    pub fn report_app_flow(&self, flow_id: FlowId) {
        let (src_node_id, dst_node_id) = self.config.extract_node_ids_from_flow(flow_id);

        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::AppFlowStart(AppFlowStart {
                flow_id,
                src_node_id,
                dst_node_id,
                time: 0, // will be filled by FlowStatsReporter on first occurrence
            }))
        {
            error!(
                "Error sending app flow message to the flowstats reporter: {}.",
                e
            );
        }
    }

    /// Report flow finished for a flow.
    pub fn report_flow_finished(&self, flow_id: FlowId, controller_id: Option<i32>) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::FlowFinished(FlowFinished {
                flow_id,
                controller_id,
                time: 0, // will be filled by FlowStatsReporter
            }))
        {
            error!(
                "Error sending flow finished message to the flowstats reporter: {}.",
                e
            );
        }
    }
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,
    app_flows: Vec<AppFlowStart>,
    route_assignments: Vec<RouteAssigned>,
    finished_flows: Vec<FlowFinished>,
    flow_times: AHashMap<FlowId, i64>,
    reported_app_flows: AHashSet<(FlowId, i64)>,
    reported_route_assignments: AHashMap<(FlowId, i64), usize>,
    reported_finished_flows: AHashSet<(FlowId, i64)>,
}

impl FlowStatsReporter {
    fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowStatsMessage>,
    ) -> Self {
        Self {
            controller,
            receiver,
            app_flows: Vec::new(),
            route_assignments: Vec::new(),
            finished_flows: Vec::new(),
            flow_times: AHashMap::default(),
            reported_app_flows: AHashSet::default(),
            reported_route_assignments: AHashMap::default(),
            reported_finished_flows: AHashSet::default(),
        }
    }

    pub async fn run(&mut self) {
        // transmits all buffered flow stat every 1 second
        let mut flowstats_tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    match msg {
                        FlowStatsMessage::AppFlowStart(app_flow) => {
                            if !self.reported_app_flows.contains(&app_flow.flow_id) {
                                let flow_id = app_flow.flow_id;
                                self.reported_app_flows.insert(flow_id);
                                self.app_flows.push(app_flow);

                                // if FlowFinished arrived before AppFlowStart, remove it from reported set
                                if self.reported_finished_flows.contains(&flow_id) {
                                    self.reported_finished_flows.remove(&flow_id);
                                }
                            }
                        }
                        FlowStatsMessage::RouteAssigned(route_assigned) => {
                            // checks if route has changed or is first time
                            let should_report = self.reported_route_assignments
                                .get(&route_assigned.flow_id)
                                .map_or(true, |&last_route| last_route != route_assigned.route_id);

                            if should_report {
                                // updates immediately to prevent duplicates within the same tick period
                                self.reported_route_assignments.insert(route_assigned.flow_id, route_assigned.route_id);

                                // for batch sending at next tick
                                self.route_assignments.push(route_assigned);
                            }
                        }
                        FlowStatsMessage::FlowFinished(flow_finished) => {
                            // only buffers once per flow to avoid duplicates
                            let flow_id = flow_finished.flow_id;
                            if self.reported_finished_flows.insert(flow_id) {
                                self.finished_flows.push(flow_finished);
                            }
                        }
                    }
                }
                _ = flowstats_tick.tick() => {
                    // sends app flows message
                    if !self.app_flows.is_empty() {
                        let mut appflows = Vec::new();

                        for app_flow in &self.app_flows {
                            appflows.push(AppFlow {
                                flow_id: app_flow.flow_id.to_be_bytes(),
                                src_node_id: app_flow.src_node_id,
                                dst_node_id: app_flow.dst_node_id,
                            });
                        }

                        let msg = DataplaneToController::AppFlowStart { appflows };
                        self.controller.send(msg).await;
                        self.app_flows.clear();
                    }

                    // sends route assignments message
                    if !self.route_assignments.is_empty() {
                        let mut assignments = Vec::new();

                        for route_assigned in &self.route_assignments {
                            assignments.push(RouteAssignment {
                                flow_id: route_assigned.flow_id.to_be_bytes(),
                                route_id: route_assigned.route_id,
                            });
                        }

                        let msg = DataplaneToController::RouteAssigned { assignments };
                        self.controller.send(msg).await;
                        self.route_assignments.clear();
                    }

                    // sends flow finished message
                    if !self.finished_flows.is_empty() {
                        let mut flows = Vec::new();

                        for flow_finished in &self.finished_flows {
                            flows.push(FlowFinishedInfo {
                                flow_id: flow_finished.flow_id.to_be_bytes(),
                                controller_id: flow_finished.controller_id,
                            });
                        }

                        let msg = DataplaneToController::FlowFinished { flows };
                        self.controller.send(msg).await;

                        // cleans up finished flows after sending all messages
                        // ensures AppFlowStart and RouteAssigned are sent before cleanup
                        for flow_finished in &self.finished_flows {
                            self.reported_app_flows.remove(&flow_finished.flow_id);
                            self.reported_route_assignments.remove(&flow_finished.flow_id);
                        }

                        self.finished_flows.clear();
                    }
                }
            }
        }
    }
}
