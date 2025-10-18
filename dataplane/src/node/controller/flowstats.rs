use ahash::AHashMap;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::{error, info, debug};

use nextmini_messages::{AppFlow, DataplaneToController, FlowFinishedInfo, RouteAssignment};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::{FlowId, NodeId};

pub struct AppFlowStart {
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
        } else if packet.has_tcp_payload() {
            // only reports app flow start for packets with payload; ignores zero-payload ACKs
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
            }))
        {
            error!(
                "Error sending flow finished message to the flowstats reporter: {}.",
                e
            );
        }
    }
}

#[derive(Debug, Clone)]
struct FlowStats {
    flow_id: FlowId,
    start_time: i64,
    finish_time: Option<i64>,
    src_node_id: NodeId,
    dst_node_id: NodeId,
    route_id: Option<usize>,
    controller_id: Option<i32>,
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,
    
    // The active flows that are still in progress.
    active_flows: AHashMap<FlowId, FlowStats>,
    
    // The flows waiting to be sent in the next tick, both active and completed.
    pending_send: AHashMap<(FlowId, i64), FlowStats>,
}

impl FlowStatsReporter {
    fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowStatsMessage>,
    ) -> Self {
        Self {
            controller,
            receiver,
            active_flows: AHashMap::default(),
            pending_send: AHashMap::default(),
        }
    }

    pub async fn run(&mut self) {
        // transmits all buffered flow stats every 1 second
        let mut flowstats_tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    match msg {
                        FlowStatsMessage::AppFlowStart(app_flow) => {
                            let flow_id = app_flow.flow_id;

                            // checks if the flow_id is already in the active_flows
                            if self.active_flows.contains_key(&flow_id) {
                                // if the flow_id is already in the active_flows, ignores the duplicate AppFlowStart
                                debug!("Duplicate AppFlowStart ignored for flow_id {:?} (flow still active).", flow_id);
                            } else {
                                // for a new flow, creates a FlowStats record
                                let time = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64;

                                let flow_stats = FlowStats {
                                    flow_id,
                                    start_time: time,
                                    finish_time: None,
                                    src_node_id: app_flow.src_node_id,
                                    dst_node_id: app_flow.dst_node_id,
                                    route_id: None,
                                    controller_id: None,
                                };

                                self.active_flows.insert(flow_id, flow_stats.clone());
                                // adds to pending_send to be sent in the next tick
                                self.pending_send.insert((flow_id, time), flow_stats);
                            }
                        }
                        FlowStatsMessage::RouteAssigned(route_assigned) => {
                            let flow_id = route_assigned.flow_id;

                            // updates the route_id in the flow stats
                            if let Some(flow_stats) = self.active_flows.get_mut(&flow_id) {
                                // only updates if route changed
                                if flow_stats.route_id != Some(route_assigned.route_id) {
                                    flow_stats.route_id = Some(route_assigned.route_id);
                                    // adds to pending_send to be sent in the next tick
                                    let key = (flow_id, flow_stats.start_time);
                                    self.pending_send.insert(key, flow_stats.clone());
                                }
                            } else {
                                info!(
                                    "RouteAssigned for unknown flow_id {:?}, ignoring (flow may have already finished or not started yet).",
                                    flow_id
                                );
                            }
                        }
                        FlowStatsMessage::FlowFinished(flow_finished) => {
                            let flow_id = flow_finished.flow_id;

                            // immediately removes from active_flows to allow flow_id reuse
                            if let Some(mut flow_stats) = self.active_flows.remove(&flow_id) {
                                let finish_time = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64;

                                flow_stats.finish_time = Some(finish_time);
                                flow_stats.controller_id = flow_finished.controller_id;

                                // adds to pending_send to be sent in the next tick
                                let key = (flow_id, flow_stats.start_time);
                                self.pending_send.insert(key, flow_stats);
                                
                                info!(
                                    "Flow {:?} finished. Queued for sending (flow_id now available for reuse).",
                                    flow_id
                                );
                            } else {
                                info!(
                                    "FlowFinished for unknown flow_id {:?} ignored - no matching AppFlowStart found. \
                                    This could be: (1) FIN-only connection, (2) flow already cleaned up, or (3) FIN from non-source node.",
                                    flow_id
                                );
                            }
                        }
                    }
                }
                _ = flowstats_tick.tick() => {
                    if self.pending_send.is_empty() {
                        continue;
                    }

                    // builds three message types from pending flows
                    let mut appflows = Vec::new();
                    let mut assignments = Vec::new();
                    let mut finished_infos = Vec::new();

                    for flow_stats in self.pending_send.values() {
                        appflows.push(AppFlow {
                            flow_id: flow_stats.flow_id.to_be_bytes(),
                            src_node_id: flow_stats.src_node_id,
                            dst_node_id: flow_stats.dst_node_id,
                            time: flow_stats.start_time,
                        });

                        if let Some(route_id) = flow_stats.route_id {
                            assignments.push(RouteAssignment {
                                flow_id: flow_stats.flow_id.to_be_bytes(),
                                route_id,
                                time: flow_stats.start_time, 
                            });
                        }

                        if let Some(finish_time) = flow_stats.finish_time {
                            finished_infos.push(FlowFinishedInfo {
                                flow_id: flow_stats.flow_id.to_be_bytes(),
                                controller_id: flow_stats.controller_id,
                                time: flow_stats.start_time, 
                                finish_time,
                            });
                        }
                    }

                    // sends messages in order: Start -> Assign -> Finish
                    if !appflows.is_empty() {
                        let msg = DataplaneToController::AppFlowStart { appflows };
                        self.controller.send(msg).await;
                    }

                    if !assignments.is_empty() {
                        let msg = DataplaneToController::RouteAssigned { assignments };
                        self.controller.send(msg).await;
                    }

                    if !finished_infos.is_empty() {
                        let msg = DataplaneToController::FlowFinished { flows: finished_infos };
                        self.controller.send(msg).await;
                    }

                    // clears pending send buffer after sending
                    self.pending_send.clear();
                }
            }
        }
    }
}
