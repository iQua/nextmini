use ahash::AHashMap;
use std::collections::HashSet;
#[cfg(test)]
use std::sync::atomic::{AtomicI64, Ordering};
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
#[cfg(not(test))]
#[inline]
fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
#[inline]
fn current_time_millis() -> i64 {
    TEST_TIME_MILLIS.load(Ordering::Relaxed)
}

#[cfg(test)]
static TEST_TIME_MILLIS: AtomicI64 = AtomicI64::new(0);

#[cfg(test)]
pub(super) fn set_current_time_millis_for_test(value: i64) {
    TEST_TIME_MILLIS.store(value, Ordering::Relaxed);
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
    UserFlowStart(UserSpaceFlowStart),
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
                "Error sending an AppFlowStart message to FlowStatsReporter: {}.",
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
                "Error sending a FlowFinished message to FlowStatsReporter: {}.",
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
                "Error sending a UserFlowStart message to FlowStatsReporter: {}.",
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

#[derive(Debug, Clone)]
struct PendingRoute {
    route_id: usize,
    assignment_time: i64,
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,

    /// The active flows that are still in progress.
    active_flows: AHashMap<FlowId, FlowStats>,

    /// The flows that have finished but are waiting for a route_id.
    finished_flows: AHashMap<FlowId, FlowStats>,

    /// The routes that have been assigned before the flow started.
    pending_routes: AHashMap<FlowId, PendingRoute>,

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
            pending_user_flow_starts: Vec::new(),
            pending_user_flow_finishes: Vec::new(),
            user_flow_starts: AHashMap::default(),
        }
    }

    pub async fn run(&mut self) {
        // transmits all buffered flow stats every 1 second
        let mut flowstats_tick = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg);
                }
                _ = flowstats_tick.tick() => {
                    self.flush().await;
                }
            }
        }
    }

    fn handle_message(&mut self, msg: FlowStatsMessage) {
        match msg {
            FlowStatsMessage::AppFlowStart(app_flow) => {
                let flow_id = app_flow.flow_id;

                if self.active_flows.contains_key(&flow_id) {
                    debug!(
                        "Duplicate AppFlowStart ignored for flow_id {:?} (flow still active).",
                        flow_id
                    );
                } else {
                    let start_time = app_flow.start_time;

                    self.finished_flows.remove(&flow_id);

                    let pending_route = self.pending_routes.remove(&flow_id);

                    if let Some(pending) = pending_route.as_ref() {
                        let wait_ms = start_time.saturating_sub(pending.assignment_time);
                        if wait_ms > 0 {
                            debug!(
                                "Flow {:?} started {} ms after route assignment.",
                                flow_id, wait_ms
                            );
                        }
                    }

                    let flow_stats = FlowStats {
                        flow_id,
                        start_time,
                        finish_time: None,
                        src_node_id: app_flow.src_node_id,
                        dst_node_id: app_flow.dst_node_id,
                        route_id: pending_route.map(|pending| pending.route_id),
                        controller_id: None,
                    };

                    self.active_flows.insert(flow_id, flow_stats.clone());
                    self.pending_send.insert((flow_id, start_time), flow_stats);
                }
            }
            FlowStatsMessage::RouteAssigned(route_assigned) => {
                let flow_id = route_assigned.flow_id;
                let new_route_id = route_assigned.route_id;
                let assignment_time = route_assigned.assignment_time;

                let update_flow_route =
                    |flow_stats: &mut FlowStats,
                     pending_assignments: &mut HashSet<(FlowId, usize, i64)>,
                     is_finished: bool| {
                        if assignment_time < flow_stats.start_time {
                            debug!(
                                "Ignored a late RouteAssigned event for a reused FlowId {:?}",
                                flow_id
                            );
                            return false;
                        }

                        if flow_stats.route_id != Some(new_route_id) {
                            flow_stats.route_id = Some(new_route_id);
                            pending_assignments.insert((
                                flow_id,
                                new_route_id,
                                flow_stats.start_time,
                            ));

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

                if let Some(flow_stats) = self.active_flows.get_mut(&flow_id) {
                    update_flow_route(flow_stats, &mut self.pending_assignments, false);
                } else if let Some(flow_stats) = self.finished_flows.get_mut(&flow_id) {
                    update_flow_route(flow_stats, &mut self.pending_assignments, true);
                } else {
                    let pending = PendingRoute {
                        route_id: new_route_id,
                        assignment_time,
                    };

                    if self.pending_routes.insert(flow_id, pending).is_some() {
                        debug!(
                            "Updated pending route assignment for flow {:?} before start.",
                            flow_id
                        );
                    }
                }
            }
            FlowStatsMessage::FlowFinished(flow_finished) => {
                let flow_id = flow_finished.flow_id;

                if let Some(mut flow_stats) = self.active_flows.remove(&flow_id) {
                    if flow_finished.finish_time < flow_stats.start_time {
                        self.active_flows.insert(flow_id, flow_stats);
                        info!(
                            "Ignored a late FlowFinished event for a reused FlowId {:?}",
                            flow_id
                        );
                        return;
                    }

                    flow_stats.finish_time = Some(flow_finished.finish_time);
                    flow_stats.controller_id = flow_finished.controller_id;

                    let key = (flow_id, flow_stats.start_time);
                    self.pending_send.insert(key, flow_stats.clone());

                    self.finished_flows.insert(flow_id, flow_stats.clone());

                    info!(
                        "Flow {:?} finished. Queued for sending (route_id: {:?}), flow_id now available for reuse.",
                        flow_id, flow_stats.route_id
                    );

                    if self.pending_routes.remove(&flow_id).is_some() {
                        debug!(
                            "Cleared stale pending route for flow {:?} when finishing active flow.",
                            flow_id
                        );
                    }
                } else if let Some(controller_id) = flow_finished.controller_id {
                    let finish_time = flow_finished.finish_time;

                    if let Some(stats) = self.user_flow_starts.remove(&flow_id) {
                        if controller_id != stats.controller_id {
                            error!("Controller ID mismatch for flow {}.", flow_id);
                        }

                        self.pending_user_flow_finishes.push(FlowFinishedInfo {
                            flow_id: flow_id.to_be_bytes(),
                            controller_id: Some(controller_id),
                            start_time: stats.start_time,
                            finish_time,
                        });

                        info!(
                            "Flow {:?} finished (user space). Queued for sending with start_time {}.",
                            flow_id, stats.start_time
                        );
                    } else {
                        self.pending_user_flow_finishes.push(FlowFinishedInfo {
                            flow_id: flow_id.to_be_bytes(),
                            controller_id: Some(controller_id),
                            start_time: finish_time,
                            finish_time,
                        });

                        error!(
                            "User-space flow {:?} finished without a recorded start.",
                            flow_id
                        );
                    }
                    if self.pending_routes.remove(&flow_id).is_some() {
                        debug!(
                            "Removed pending route for user-space flow {:?} after it finished.",
                            flow_id
                        );
                    }
                } else if self.pending_routes.remove(&flow_id).is_some() {
                    error!(
                        "Removed pending route for flow {:?} because it finished before AppFlowStart.",
                        flow_id
                    );
                }
            }
            FlowStatsMessage::UserFlowStart(user_flow_start) => {
                let stats = UserFlowStats {
                    start_time: user_flow_start.start_time,
                    controller_id: user_flow_start.controller_id,
                };

                if let Some(existing) = self
                    .user_flow_starts
                    .insert(user_flow_start.flow_id, stats.clone())
                {
                    warn!(
                        "Replacing existing user-space flow start for {:?}. Old start_time: {}, controller_id: {}",
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

    async fn flush(&mut self) {
        // evicts outdated finished flows (keep them up to 30s)
        const TTL_MS: i64 = 30_000; // 30 seconds
        let now_ms = current_time_millis();

        self.finished_flows
            .retain(|_, fs| fs.finish_time.map_or(true, |ft| now_ms - ft <= TTL_MS));

        if self.pending_send.is_empty()
            && self.pending_assignments.is_empty()
            && self.pending_user_flow_finishes.is_empty()
            && self.pending_user_flow_starts.is_empty()
        {
            return;
        }

        let mut appflows = Vec::new();
        let mut assignments = Vec::new();
        let mut finished_infos = Vec::new();
        let mut user_flow_starts = Vec::new();

        for flow_stats in self.pending_send.values() {
            appflows.push(AppFlow {
                flow_id: flow_stats.flow_id.to_be_bytes(),
                src_node_id: flow_stats.src_node_id,
                dst_node_id: flow_stats.dst_node_id,
                start_time: flow_stats.start_time,
            });

            if let Some(route_id) = flow_stats.route_id {
                self.pending_assignments.insert((
                    flow_stats.flow_id,
                    route_id,
                    flow_stats.start_time,
                ));
            }

            if let Some(finish_time) = flow_stats.finish_time {
                finished_infos.push(FlowFinishedInfo {
                    flow_id: flow_stats.flow_id.to_be_bytes(),
                    controller_id: flow_stats.controller_id,
                    start_time: flow_stats.start_time,
                    finish_time,
                });
            }
        }

        for (flow_id, route_id, start_time) in self.pending_assignments.drain() {
            assignments.push(RouteAssignment {
                flow_id: flow_id.to_be_bytes(),
                route_id,
                time: start_time,
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

        if !appflows.is_empty() {
            debug!("Sending {} AppFlowStart messages", appflows.len());
            let msg = DataplaneToController::AppFlowStart { appflows };
            self.controller.send(msg).await;
        }

        if !user_flow_starts.is_empty() {
            debug!("Sending {} UserFlowStart messages.", user_flow_starts.len());
            let msg = DataplaneToController::UserFlowStart {
                flows: user_flow_starts,
            };
            self.controller.send(msg).await;
        }

        if !assignments.is_empty() {
            debug!("Sending {} RouteAssigned messages.", assignments.len());
            let msg = DataplaneToController::RouteAssigned { assignments };
            self.controller.send(msg).await;
        }

        if !finished_infos.is_empty() {
            debug!("Sending {} FlowFinished messages.", finished_infos.len());
            let msg = DataplaneToController::FlowFinished {
                flows: finished_infos,
            };
            self.controller.send(msg).await;
        }

        self.pending_send.clear();
        self.pending_assignments.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn pending_route_survives_long_gap_before_start() {
        set_current_time_millis_for_test(0);

        let (controller, mut controller_rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller.clone(), receiver);

        let flow_id: FlowId = 42;
        let route_id = 7;

        reporter.handle_message(FlowStatsMessage::RouteAssigned(RouteAssigned {
            flow_id,
            assignment_time: 0,
            route_id,
        }));

        assert!(
            reporter.pending_routes.contains_key(&flow_id),
            "pending route should be buffered before start"
        );

        set_current_time_millis_for_test(35_000);
        reporter.flush().await;

        assert!(
            reporter.pending_routes.contains_key(&flow_id),
            "pending route was evicted before AppFlowStart"
        );

        set_current_time_millis_for_test(40_000);
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 40_000,
        }));

        let stats = reporter
            .active_flows
            .get(&flow_id)
            .expect("flow should be tracked as active after AppFlowStart");
        assert_eq!(
            stats.route_id,
            Some(route_id),
            "route assignment should be retained until flow start"
        );

        reporter.flush().await;

        let app_flow_msg = controller_rx
            .recv()
            .await
            .expect("expected AppFlowStart message");
        if let DataplaneToController::AppFlowStart { appflows } = app_flow_msg {
            assert_eq!(appflows.len(), 1);
            assert_eq!(appflows[0].flow_id, flow_id.to_be_bytes());
            assert_eq!(appflows[0].start_time, 40_000);
        } else {
            panic!("first message should be AppFlowStart");
        }

        let assignment_msg = controller_rx
            .recv()
            .await
            .expect("expected RouteAssigned message");
        if let DataplaneToController::RouteAssigned { assignments } = assignment_msg {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].flow_id, flow_id.to_be_bytes());
            assert_eq!(assignments[0].route_id, route_id);
            assert_eq!(assignments[0].time, 40_000);
        } else {
            panic!("second message should be RouteAssigned");
        }
    }

    #[tokio::test]
    async fn test_flowid_reuse_with_late_finish() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // First flow starts and finishes
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1000,
        }));

        set_current_time_millis_for_test(2000);
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 42,
            finish_time: 2000,
            controller_id: None,
        }));

        reporter.flush().await;
        rx.recv().await; // AppFlowStart
        rx.recv().await; // FlowFinished

        // FlowId reused with new flow
        set_current_time_millis_for_test(3000);
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 3,
            dst_node_id: 4,
            start_time: 3000,
        }));

        // Late finish from old flow arrives (timestamp earlier than new start)
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 42,
            finish_time: 1999, // Before new start!
            controller_id: None,
        }));

        reporter.flush().await;

        // Should only see new flow start, late finish should be ignored
        let msg = rx.recv().await.unwrap();
        if let DataplaneToController::AppFlowStart { appflows } = msg {
            assert_eq!(appflows.len(), 1);
            assert_eq!(appflows[0].start_time, 3000);
        }

        // No FlowFinished should be sent (late one ignored)
        assert!(rx.try_recv().is_err(), "Late finish should be ignored");
    }

    #[tokio::test]
    async fn test_duplicate_app_flow_start() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // First start
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1000,
        }));

        // Duplicate start (should be ignored while flow is active)
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1100,
        }));

        reporter.flush().await;

        // Should only get one AppFlowStart
        let msg = rx.recv().await.unwrap();
        if let DataplaneToController::AppFlowStart { appflows } = msg {
            assert_eq!(appflows.len(), 1);
            assert_eq!(appflows[0].start_time, 1000); // First one
        }

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_route_changes_for_active_flow() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // Flow starts
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1000,
        }));

        // First route assignment
        reporter.handle_message(FlowStatsMessage::RouteAssigned(RouteAssigned {
            flow_id: 42,
            assignment_time: 1100,
            route_id: 5,
        }));

        // Route changes to different path
        reporter.handle_message(FlowStatsMessage::RouteAssigned(RouteAssigned {
            flow_id: 42,
            assignment_time: 1200,
            route_id: 7,
        }));

        reporter.flush().await;

        rx.recv().await; // AppFlowStart
        let msg = rx.recv().await.unwrap();

        // Should receive both route assignments (order-agnostic)
        if let DataplaneToController::RouteAssigned { assignments } = msg {
            assert_eq!(assignments.len(), 2);
            let mut ids: Vec<_> = assignments.iter().map(|a| a.route_id).collect();
            ids.sort();
            assert_eq!(ids, vec![5, 7]);
        } else {
            panic!("Expected RouteAssigned");
        }
    }

    #[tokio::test]
    async fn test_user_flow_without_start() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // User flow finishes without a start event
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 99,
            finish_time: 1000,
            controller_id: Some(1),
        }));

        reporter.flush().await;

        // Should send finish with finish_time as start_time
        let msg = rx.recv().await.unwrap();
        if let DataplaneToController::FlowFinished { flows } = msg {
            assert_eq!(flows.len(), 1);
            assert_eq!(flows[0].start_time, 1000); // start_time = finish_time
            assert_eq!(flows[0].finish_time, 1000);
            assert_eq!(flows[0].controller_id, Some(1));
        } else {
            panic!("Expected FlowFinished");
        }
    }

    #[tokio::test]
    async fn test_user_flow_controller_id_mismatch() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // User flow starts with controller_id = 1
        reporter.handle_message(FlowStatsMessage::UserFlowStart(UserSpaceFlowStart {
            flow_id: 42,
            controller_id: 1,
            start_time: 1000,
        }));

        // Finishes with different controller_id = 2 (should warn but handle)
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 42,
            finish_time: 2000,
            controller_id: Some(2),
        }));

        reporter.flush().await;

        rx.recv().await; // UserFlowStart
        let msg = rx.recv().await.unwrap();

        if let DataplaneToController::FlowFinished { flows } = msg {
            assert_eq!(flows.len(), 1);
            assert_eq!(flows[0].controller_id, Some(2)); // Uses finish controller_id
            assert_eq!(flows[0].start_time, 1000); // Uses start time
        } else {
            panic!("Expected FlowFinished");
        }
    }

    #[tokio::test]
    async fn test_finished_flow_ttl_cleanup() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // Flow 1 completes
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 1,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1000,
        }));
        set_current_time_millis_for_test(2000);
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 1,
            finish_time: 2000,
            controller_id: None,
        }));

        reporter.flush().await;
        rx.recv().await; // Clear messages

        // Flow 2 completes much later
        set_current_time_millis_for_test(10000);
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 2,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 10000,
        }));
        set_current_time_millis_for_test(11000);
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 2,
            finish_time: 11000,
            controller_id: None,
        }));

        // Time advances beyond TTL for flow 1 (30s)
        set_current_time_millis_for_test(35000);
        reporter.flush().await;

        // Flow 1 should be cleaned up (finished > 30s ago)
        assert!(!reporter.finished_flows.contains_key(&1));
        // Flow 2 should still be there
        assert!(reporter.finished_flows.contains_key(&2));
    }

    #[tokio::test]
    async fn test_batch_send_multiple_flows() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // Start 10 flows simultaneously
        for i in 1..=10 {
            reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
                flow_id: i,
                src_node_id: 1,
                dst_node_id: 2,
                start_time: 1000 + i as i64,
            }));
        }

        reporter.flush().await;

        // All 10 should be sent in a single batched message
        let msg = rx.recv().await.unwrap();
        if let DataplaneToController::AppFlowStart { appflows } = msg {
            assert_eq!(appflows.len(), 10, "Should batch all 10 flow starts");
        } else {
            panic!("Expected batched AppFlowStart");
        }
    }

    #[tokio::test]
    async fn test_route_assignment_for_finished_flow() {
        set_current_time_millis_for_test(1000);
        let (controller, mut rx) = ControllerInterfaceHandle::test_handle();
        let (_sender, receiver) = unbounded_channel();
        let mut reporter = FlowStatsReporter::new(controller, receiver);

        // Flow starts and finishes quickly
        reporter.handle_message(FlowStatsMessage::AppFlowStart(AppFlowStart {
            flow_id: 42,
            src_node_id: 1,
            dst_node_id: 2,
            start_time: 1000,
        }));

        set_current_time_millis_for_test(2000);
        reporter.handle_message(FlowStatsMessage::FlowFinished(FlowFinished {
            flow_id: 42,
            finish_time: 2000,
            controller_id: None,
        }));

        reporter.flush().await;
        rx.recv().await; // AppFlowStart
        rx.recv().await; // FlowFinished

        // Route assigned after flow already finished
        reporter.handle_message(FlowStatsMessage::RouteAssigned(RouteAssigned {
            flow_id: 42,
            assignment_time: 2100,
            route_id: 5,
        }));

        reporter.flush().await;

        // Route should be assigned to finished flow
        let msg = rx.recv().await.unwrap();
        if let DataplaneToController::RouteAssigned { assignments } = msg {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].route_id, 5);
            assert_eq!(assignments[0].time, 1000); // Uses flow start_time
        } else {
            panic!("Expected RouteAssigned for finished flow");
        }
    }
}
