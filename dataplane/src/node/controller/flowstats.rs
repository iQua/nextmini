use ahash::AHashMap;
use std::collections::HashSet;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

use nextmini_messages::{
    AppFlow, DataplaneToController, FlowFinishedInfo, RouteAssignment, UserFlowStart,
};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::packet::Packet;
use crate::node::{FlowId, NodeId};

/// Returns the current time in milliseconds since the Unix epoch.
#[inline]
fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct AppFlowStart {
    pub flow_id: FlowId,
    pub src_node_id: NodeId,
    pub dst_node_id: NodeId,
    pub start_time: i64,
}

pub struct RouteAssigned {
    pub flow_id: FlowId,
    pub assignment_time: i64,
    pub route_id: usize,
}

pub struct FlowFinished {
    pub flow_id: FlowId,
    pub finish_time: i64,
    pub controller_id: Option<i32>,
}

pub struct UserSpaceFlowStart {
    pub flow_id: FlowId,
    pub controller_id: i32,
    pub start_time: i64,
}

pub enum FlowStatsMessage {
    AppFlowStart(AppFlowStart),
    RouteAssigned(RouteAssigned),
    FlowFinished(FlowFinished),
    UserFlowStart(UserSpaceFlowStart),
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
                assignment_time: current_time_millis(),
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

        let start_time = current_time_millis();

        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::AppFlowStart(AppFlowStart {
                flow_id,
                src_node_id,
                dst_node_id,
                start_time,
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
                finish_time: current_time_millis(),
                controller_id,
            }))
        {
            error!(
                "Error sending flow finished message to the flowstats reporter: {}.",
                e
            );
        }
    }

    /// Report the start of a user-space flow (controller-managed).
    pub fn report_user_flow_start(&self, flow_id: FlowId, controller_id: i32) {
        if let Err(e) = self
            .sender
            .send(FlowStatsMessage::UserFlowStart(UserSpaceFlowStart {
                flow_id,
                controller_id,
                start_time: current_time_millis(),
            }))
        {
            error!(
                "Error sending user-space flow start message to the flowstats reporter: {}.",
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

#[derive(Debug, Clone)]
struct UserFlowStats {
    start_time: i64,
    controller_id: i32,
}

#[derive(Debug, Clone)]
struct PendingUserFlowStart {
    flow_id: FlowId,
    stats: UserFlowStats,
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,

    /// The active flows that are still in progress.
    active_flows: AHashMap<FlowId, FlowStats>,

    /// The flows that have finished but are waiting for a route_id.
    finished_flows: AHashMap<FlowId, FlowStats>,

    /// The routes that have been assigned before the flow started.
    pending_routes: AHashMap<FlowId, (usize, i64)>,

    /// The flows waiting to be sent in the next tick, both active and completed.
    pending_send: AHashMap<(FlowId, i64), FlowStats>,

    /// The route assignments waiting to be sent in the next tick.
    pending_assignments: HashSet<(FlowId, usize, i64)>,

    /// The user-space flow finishes waiting to be sent (they don't emit AppFlowStart).
    pending_user_flow_finishes: Vec<FlowFinishedInfo>,

    /// The user-space flow starts waiting to be sent.
    pending_user_flow_starts: Vec<PendingUserFlowStart>,

    /// Tracks user-space flow start metadata for pairing with their finish events.
    user_flow_starts: AHashMap<FlowId, UserFlowStats>,
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
            finished_flows: AHashMap::default(),
            pending_routes: AHashMap::default(),
            pending_send: AHashMap::default(),
            pending_assignments: HashSet::default(),
            pending_user_flow_finishes: Vec::new(),
            pending_user_flow_starts: Vec::new(),
            user_flow_starts: AHashMap::default(),
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
                                let time = app_flow.start_time;

                                // if a new flow starts, removes any finished flow with the same flow_id
                                self.finished_flows.remove(&flow_id);

                                let flow_stats = FlowStats {
                                    flow_id,
                                    start_time: time,
                                    finish_time: None,
                                    src_node_id: app_flow.src_node_id,
                                    dst_node_id: app_flow.dst_node_id,
                                    route_id: self.pending_routes.remove(&flow_id).map(|(r, _)| r),
                                    controller_id: None,
                                };

                                self.active_flows.insert(flow_id, flow_stats.clone());
                                // adds to pending_send to be sent in the next tick
                                self.pending_send.insert((flow_id, time), flow_stats);
                            }
                        }
                        FlowStatsMessage::RouteAssigned(route_assigned) => {
                            let flow_id = route_assigned.flow_id;
                            let new_route_id = route_assigned.route_id;
                            let assignment_time = route_assigned.assignment_time;

                            // updates route and queue assignment
                            let update_flow_route = |flow_stats: &mut FlowStats,
                                                     pending_assignments: &mut HashSet<(
                                FlowId,
                                usize,
                                i64,
                            )>,
                                                     is_finished: bool| {
                                // ignores late RouteAssigned events from previous flow instances
                                if assignment_time < flow_stats.start_time {
                                    debug!("Ignored a late RouteAssigned event for a reused FlowId {:?}", flow_id);
                                    return false;
                                }

                                if flow_stats.route_id != Some(new_route_id) {
                                    flow_stats.route_id = Some(new_route_id);
                                    pending_assignments
                                        .insert((flow_id, new_route_id, flow_stats.start_time));

                                    // DEBUG!: for debugging.
                                    debug!(
                                        "RouteAssigned processed for {} flow: flow_id={:?}, route_id={}",
                                        if is_finished { "finished" } else { "active" },
                                        flow_id,
                                        new_route_id
                                    );
                                    true
                                } else {
                                    false
                                }
                            };

                            // tries to update active flows first
                            if let Some(flow_stats) = self.active_flows.get_mut(&flow_id) {
                                update_flow_route(flow_stats, &mut self.pending_assignments, false);
                            }
                            // then tries to update finished flows
                            else if let Some(flow_stats) = self.finished_flows.get_mut(&flow_id) {
                                update_flow_route(flow_stats, &mut self.pending_assignments, true);
                            }
                            // if the flow hasn't started yet
                            else {
                                let now = current_time_millis();
                                self.pending_routes.insert(flow_id, (new_route_id, now));
                            }
                        }
                        FlowStatsMessage::FlowFinished(flow_finished) => {
                            let flow_id = flow_finished.flow_id;

                            // immediately removes from active_flows to allow flow_id reuse
                            if let Some(mut flow_stats) = self.active_flows.remove(&flow_id) {
                                // ignores late FINs from previous flow instances
                                if flow_finished.finish_time < flow_stats.start_time {
                                    // this is a late event, ignore it and put the active flow back
                                    self.active_flows.insert(flow_id, flow_stats);
                                    info!("Ignored a late FlowFinished event for a reused FlowId {:?}", flow_id);
                                    continue;
                                }

                                flow_stats.finish_time = Some(flow_finished.finish_time);
                                flow_stats.controller_id = flow_finished.controller_id;

                                // adds to pending_send to be sent in the next tick
                                let key = (flow_id, flow_stats.start_time);
                                self.pending_send.insert(key, flow_stats.clone());

                                // keeps the finished flow for a while in case of a late RouteAssigned
                                self.finished_flows.insert(flow_id, flow_stats.clone());

                                info!(
                                    "Flow {:?} finished. Queued for sending (route_id: {:?}), flow_id now available for reuse.",
                                    flow_id, flow_stats.route_id
                                );
                            } else if let Some(controller_id) = flow_finished.controller_id {
                                let finish_time = flow_finished.finish_time;
                                if let Some(stats) = self.user_flow_starts.remove(&flow_id) {
                                    if controller_id != stats.controller_id {
                                        warn!(
                                            "Controller ID mismatch for flow {:?}: start={}, finish={:?}. Using finish controller_id.",
                                            flow_id, stats.controller_id, controller_id
                                        );
                                    }

                                    self.pending_user_flow_finishes.push(FlowFinishedInfo {
                                        flow_id: flow_id.to_be_bytes(),
                                        controller_id: Some(controller_id),
                                        time: stats.start_time,
                                        finish_time,
                                    });

                                    info!(
                                        "Flow {:?} finished (user space). Queued for sending with start_time {}.",
                                        flow_id, stats.start_time
                                    );
                                } else {
                                    // No recorded start; fall back to finish time for reporting.
                                    self.pending_user_flow_finishes.push(FlowFinishedInfo {
                                        flow_id: flow_id.to_be_bytes(),
                                        controller_id: Some(controller_id),
                                        time: finish_time,
                                        finish_time,
                                    });

                                    warn!(
                                        "Flow {:?} finished (user space) without a recorded start. Using finish_time as start_time.",
                                        flow_id
                                    );
                                }
                            }
                        }
                        FlowStatsMessage::UserFlowStart(user_flow_start) => {
                            let stats = UserFlowStats {
                                start_time: user_flow_start.start_time,
                                controller_id: user_flow_start.controller_id,
                            };

                            if let Some(existing) =
                                self.user_flow_starts.insert(user_flow_start.flow_id, stats.clone())
                            {
                                warn!(
                                    "Replacing existing user-space flow start for {:?}. Old start_time={}, controller_id={}",
                                    user_flow_start.flow_id, existing.start_time, existing.controller_id
                                );
                            }

                            self.pending_user_flow_starts.push(PendingUserFlowStart {
                                flow_id: user_flow_start.flow_id,
                                stats,
                            });
                        }
                    }
                }
                _ = flowstats_tick.tick() => {
                    // evicts outdated finished flows & pending routes (keep them up to 30s)
                    const TTL_MS: i64 = 30_000; // 30 seconds
                    let now_ms = current_time_millis();

                    self.finished_flows.retain(|_, fs| {
                        fs.finish_time.map_or(true, |ft| now_ms - ft <= TTL_MS)
                    });

                    self.pending_routes.retain(|_, (_, ts)| now_ms - *ts <= TTL_MS);

                    if self.pending_send.is_empty()
                        && self.pending_assignments.is_empty()
                        && self.pending_user_flow_finishes.is_empty()
                        && self.pending_user_flow_starts.is_empty()
                    {
                        continue;
                    }

                    // builds three message types from pending flows
                    let mut appflows = Vec::new();
                    let mut assignments = Vec::new();
                    let mut finished_infos = Vec::new();
                    let mut user_flow_starts = Vec::new();

                    for flow_stats in self.pending_send.values() {
                        appflows.push(AppFlow {
                            flow_id: flow_stats.flow_id.to_be_bytes(),
                            src_node_id: flow_stats.src_node_id,
                            dst_node_id: flow_stats.dst_node_id,
                            time: flow_stats.start_time,
                        });

                        if let Some(route_id) = flow_stats.route_id {
                            self.pending_assignments.insert((flow_stats.flow_id, route_id, flow_stats.start_time));
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

                    for (flow_id, route_id, time) in self.pending_assignments.drain() {
                        assignments.push(RouteAssignment {
                            flow_id: flow_id.to_be_bytes(),
                            route_id,
                            time,
                        });
                    }

                    for pending_start in self.pending_user_flow_starts.drain(..) {
                        user_flow_starts.push(UserFlowStart {
                            controller_id: pending_start.stats.controller_id,
                            flow_id: pending_start.flow_id.to_be_bytes(),
                            start_time: pending_start.stats.start_time,
                        });
                    }

                    finished_infos.extend(self.pending_user_flow_finishes.drain(..));

                    // sends messages in order: Start -> Assign -> Finish
                    if !appflows.is_empty() {
                        debug!("Sending {} AppFlowStart messages", appflows.len());
                        let msg = DataplaneToController::AppFlowStart { appflows };
                        self.controller.send(msg).await;
                    }

                    if !user_flow_starts.is_empty() {
                        debug!("Sending {} UserFlowStart messages", user_flow_starts.len());
                        let msg = DataplaneToController::UserFlowStart { flows: user_flow_starts };
                        self.controller.send(msg).await;
                    }

                    if !assignments.is_empty() {
                        debug!("Sending {} RouteAssigned messages", assignments.len());
                        let msg = DataplaneToController::RouteAssigned { assignments };
                        self.controller.send(msg).await;
                    }

                    if !finished_infos.is_empty() {
                        debug!("Sending {} FlowFinished messages", finished_infos.len());
                        let msg = DataplaneToController::FlowFinished { flows: finished_infos };
                        self.controller.send(msg).await;
                    }

                    // clears pending send buffer after sending
                    self.pending_send.clear();
                    self.pending_assignments.clear();
                }
            }
        }
    }
}
