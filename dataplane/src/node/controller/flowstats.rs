use std::collections::HashSet;
use std::collections::hash_map::Entry;
#[cfg(test)]
use std::sync::atomic::{AtomicI64, Ordering};

use ahash::AHashMap;
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
        // checks and reports if a flow has finished (FIN/RST)
        if packet.is_tcp_fin_or_rst() {
            self.report_flow_finished(packet.flow_id, None);
        } else if packet.is_tcp_data() {
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

#[derive(Debug)]
struct PendingRoute {
    route_id: Option<usize>,
}

#[derive(Debug)]
struct AppFlowEntry {
    flow_id: FlowId,
    start_time: i64,
    finish_time: Option<i64>,
    src_node_id: NodeId,
    dst_node_id: NodeId,
    route_id: Option<usize>,
    controller_id: Option<i32>,
    stage: AppStage,
    pending: AppPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppStage {
    Active,
    Finished,
}

#[derive(Debug, Default)]
struct AppPending {
    send_start: bool,
    route_updates: HashSet<usize>,
    send_finish: bool,
}

#[derive(Debug)]
struct UserFlowEntry {
    flow_id: FlowId,
    controller_id: i32,
    start_time: i64,
    stage: UserStage,
    pending_start: bool,
    pending_finish: Option<FlowFinishedInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UserStage {
    Active,
    Finished,
}

#[derive(Debug, Default)]
struct FlowStore {
    entries: AHashMap<FlowId, FlowEntry>,
}

#[derive(Debug)]
enum FlowEntry {
    Pending(PendingRoute),
    App(AppFlowEntry),
    User(UserFlowEntry),
}

#[derive(Debug, Default)]
struct FlushOutput {
    app_flows: Vec<AppFlow>,
    assignments: Vec<RouteAssignment>,
    finishes: Vec<FlowFinishedInfo>,
    user_starts: Vec<UserFlowStart>,
}

impl AppFlowEntry {
    fn new(start: AppFlowStart, pending_route: Option<usize>) -> Self {
        let mut pending = AppPending::default();
        if let Some(route_id) = pending_route {
            pending.route_updates.insert(route_id);
        }

        Self {
            flow_id: start.flow_id,
            start_time: start.start_time,
            finish_time: None,
            src_node_id: start.src_node_id,
            dst_node_id: start.dst_node_id,
            route_id: pending_route,
            controller_id: None,
            stage: AppStage::Active,
            pending,
        }
    }

    fn apply_route_assignment(
        &mut self,
        route_id: usize,
        assignment_time: i64,
    ) -> RouteUpdateResult {
        if assignment_time < self.start_time {
            return RouteUpdateResult::IgnoredLate;
        }

        if self.route_id == Some(route_id) {
            return RouteUpdateResult::Unchanged;
        }

        self.route_id = Some(route_id);
        self.pending.route_updates.insert(route_id);

        match self.stage {
            AppStage::Active => RouteUpdateResult::AppliedActive,
            AppStage::Finished => RouteUpdateResult::AppliedFinished,
        }
    }

    fn mark_finished(&mut self, finish_time: i64, controller_id: Option<i32>) -> FlowFinishStatus {
        if finish_time < self.start_time {
            return FlowFinishStatus::Late;
        }

        if matches!(self.stage, AppStage::Finished) {
            return FlowFinishStatus::Duplicate;
        }

        self.finish_time = Some(finish_time);
        self.controller_id = controller_id;
        self.stage = AppStage::Finished;
        self.pending.send_finish = true;

        FlowFinishStatus::Accepted
    }

    fn finished_snapshot(&self) -> Option<FlowFinishedInfo> {
        self.finish_time.map(|finish_time| FlowFinishedInfo {
            flow_id: self.flow_id.to_be_bytes(),
            controller_id: self.controller_id,
            start_time: self.start_time,
            finish_time,
        })
    }
}

impl UserFlowEntry {
    fn new(start: UserSpaceFlowStart) -> Self {
        Self {
            flow_id: start.flow_id,
            controller_id: start.controller_id,
            start_time: start.start_time,
            stage: UserStage::Active,
            pending_start: true,
            pending_finish: None,
        }
    }

    fn finish(&mut self, finish_time: i64, controller_id: i32) -> UserFinishStatus {
        if self.controller_id != controller_id {
            self.controller_id = controller_id;
            self.pending_finish = Some(self.finished_info(finish_time));
            self.stage = UserStage::Finished;
            UserFinishStatus::ControllerMismatch
        } else {
            self.pending_finish = Some(self.finished_info(finish_time));
            self.stage = UserStage::Finished;
            UserFinishStatus::Ok
        }
    }

    fn finished_without_start(flow_id: FlowId, controller_id: i32, finish_time: i64) -> Self {
        let mut entry = Self {
            flow_id,
            controller_id,
            start_time: finish_time,
            stage: UserStage::Finished,
            pending_start: false,
            pending_finish: None,
        };
        entry.pending_finish = Some(entry.finished_info(finish_time));
        entry
    }

    fn finished_info(&self, finish_time: i64) -> FlowFinishedInfo {
        FlowFinishedInfo {
            flow_id: self.flow_id.to_be_bytes(),
            controller_id: Some(self.controller_id),
            start_time: self.start_time,
            finish_time,
        }
    }
}

impl FlowStore {
    fn handle_app_flow_start(&mut self, app_flow: AppFlowStart) {
        let flow_id = app_flow.flow_id;

        if let Some(entry) = self.entries.get(&flow_id)
            && let FlowEntry::App(app_entry) = entry
            && matches!(app_entry.stage, AppStage::Active)
        {
            return;
        }

        let pending_route = match self.entries.remove(&flow_id) {
            Some(FlowEntry::Pending(pending)) => pending.route_id,
            Some(FlowEntry::App(_)) => None,
            Some(FlowEntry::User(_)) => None,
            None => None,
        };

        let mut entry = AppFlowEntry::new(app_flow, pending_route);
        entry.pending.send_start = true;
        self.entries.insert(flow_id, FlowEntry::App(entry));
    }

    fn handle_route_assigned(&mut self, route: RouteAssigned) {
        match self.entries.entry(route.flow_id) {
            Entry::Occupied(mut occupied) => match occupied.get_mut() {
                FlowEntry::App(app_entry) => {
                    match app_entry.apply_route_assignment(route.route_id, route.assignment_time) {
                        RouteUpdateResult::AppliedActive => debug!(
                            "RouteAssigned processed for active flow: flow_id={:?}, route_id={}",
                            route.flow_id, route.route_id
                        ),
                        RouteUpdateResult::AppliedFinished => debug!(
                            "RouteAssigned processed for finished flow: flow_id={:?}, route_id={}",
                            route.flow_id, route.route_id
                        ),
                        RouteUpdateResult::IgnoredLate => debug!(
                            "Ignored a late RouteAssigned event for a reused FlowId {:?}",
                            route.flow_id
                        ),
                        RouteUpdateResult::Unchanged => {}
                    }
                }
                FlowEntry::Pending(pending) => {
                    let replaced = pending.route_id.replace(route.route_id);
                    if replaced.is_some() {
                        debug!(
                            "Updated pending route assignment for flow {:?} before start.",
                            route.flow_id
                        );
                    }
                }
                FlowEntry::User(_) => {
                    debug!(
                        "Ignoring route assignment for user-space flow {:?}.",
                        route.flow_id
                    );
                }
            },
            Entry::Vacant(vacant) => {
                vacant.insert(FlowEntry::Pending(PendingRoute {
                    route_id: Some(route.route_id),
                }));
            }
        };
    }

    fn handle_flow_finished(&mut self, flow_finished: FlowFinished) {
        let flow_id = flow_finished.flow_id;
        let finish_time = flow_finished.finish_time;
        let mut untracked_user_finish = None;

        match self.entries.entry(flow_id) {
            Entry::Occupied(mut occupied) => match occupied.get_mut() {
                FlowEntry::App(app_entry) => {
                    match app_entry.mark_finished(finish_time, flow_finished.controller_id) {
                        FlowFinishStatus::Accepted => {
                            info!(
                                "Flow {:?} finished. Queued for sending (route_id: {:?}), flow_id now available for reuse.",
                                flow_id, app_entry.route_id
                            );
                        }
                        FlowFinishStatus::Late => {
                            info!(
                                "Ignored a late FlowFinished event for a reused FlowId {:?}",
                                flow_id
                            );
                        }
                        FlowFinishStatus::Duplicate => {}
                    }
                    return;
                }
                FlowEntry::Pending(_) => {
                    if let Some(controller_id) = flow_finished.controller_id {
                        debug!(
                            "Removed pending route for user-space flow {:?} after it finished.",
                            flow_id
                        );
                        untracked_user_finish = Some(controller_id);
                        occupied.remove();
                    } else {
                        error!(
                            "Removed pending route for flow {:?} because it finished before AppFlowStart.",
                            flow_id
                        );
                        occupied.remove();
                    }
                }
                FlowEntry::User(user_entry) => {
                    if let Some(controller_id) = flow_finished.controller_id {
                        match user_entry.finish(finish_time, controller_id) {
                            UserFinishStatus::ControllerMismatch => {
                                error!("Controller ID mismatch for flow {}.", flow_id);
                            }
                            UserFinishStatus::Ok => {}
                        }
                        info!(
                            "Flow {:?} finished (user space). Queued for sending with start_time {}.",
                            flow_id, user_entry.start_time
                        );
                    } else {
                        warn!(
                            "Flow {:?} finished without controller id while tracked as user flow.",
                            flow_id
                        );
                    }
                    return;
                }
            },
            Entry::Vacant(_) => {
                if let Some(controller_id) = flow_finished.controller_id {
                    untracked_user_finish = Some(controller_id);
                }
            }
        }

        if let Some(controller_id) = untracked_user_finish {
            error!(
                "User-space flow {:?} finished without a recorded start.",
                flow_id
            );
            let entry = UserFlowEntry::finished_without_start(flow_id, controller_id, finish_time);
            info!(
                "Flow {:?} finished (user space). Queued for sending with start_time {}.",
                flow_id, entry.start_time
            );
            self.entries.insert(flow_id, FlowEntry::User(entry));
        }
    }

    fn handle_user_flow_start(&mut self, user_flow_start: UserSpaceFlowStart) {
        let flow_id = user_flow_start.flow_id;

        if let Some(existing) = self.entries.insert(
            flow_id,
            FlowEntry::User(UserFlowEntry::new(user_flow_start)),
        ) && let FlowEntry::User(prev) = existing
        {
            warn!(
                "Replacing existing user-space flow start for {:?}. Old start_time: {}, controller_id: {}",
                flow_id, prev.start_time, prev.controller_id
            );
        }
    }

    fn flush(&mut self, now_ms: i64) -> FlushOutput {
        const TTL_MS: i64 = 30_000;
        let mut output = FlushOutput::default();

        self.entries.retain(|_flow_id, entry| match entry {
            FlowEntry::Pending(_) => true,
            FlowEntry::App(app_entry) => {
                if app_entry.pending.send_start {
                    output.app_flows.push(AppFlow {
                        flow_id: app_entry.flow_id.to_be_bytes(),
                        src_node_id: app_entry.src_node_id,
                        dst_node_id: app_entry.dst_node_id,
                        start_time: app_entry.start_time,
                    });
                    app_entry.pending.send_start = false;
                }

                for route_id in app_entry.pending.route_updates.drain() {
                    output.assignments.push(RouteAssignment {
                        flow_id: app_entry.flow_id.to_be_bytes(),
                        route_id,
                        time: app_entry.start_time,
                    });
                }

                if app_entry.pending.send_finish {
                    if let Some(finished) = app_entry.finished_snapshot() {
                        output.finishes.push(finished);
                    }
                    app_entry.pending.send_finish = false;
                }

                if matches!(app_entry.stage, AppStage::Finished)
                    && let Some(finish_time) = app_entry.finish_time
                    && now_ms - finish_time > TTL_MS
                {
                    return false;
                }

                true
            }
            FlowEntry::User(user_entry) => {
                if user_entry.pending_start {
                    output.user_starts.push(UserFlowStart {
                        controller_id: user_entry.controller_id,
                        flow_id: user_entry.flow_id.to_be_bytes(),
                        start_time: user_entry.start_time,
                    });
                    user_entry.pending_start = false;
                }

                if let Some(info) = user_entry.pending_finish.take() {
                    output.finishes.push(info);
                }

                if matches!(user_entry.stage, UserStage::Finished)
                    && !user_entry.pending_start
                    && user_entry.pending_finish.is_none()
                {
                    return false;
                }

                true
            }
        });

        output
    }

    #[cfg(test)]
    fn pending_route(&self, flow_id: FlowId) -> Option<usize> {
        self.entries.get(&flow_id).and_then(|entry| match entry {
            FlowEntry::Pending(pending) => pending.route_id,
            FlowEntry::App(app_entry) if matches!(app_entry.stage, AppStage::Active) => {
                app_entry.route_id
            }
            _ => None,
        })
    }

    #[cfg(test)]
    fn app_entry(&self, flow_id: FlowId) -> Option<&AppFlowEntry> {
        self.entries.get(&flow_id).and_then(|entry| match entry {
            FlowEntry::App(app_entry) => Some(app_entry),
            _ => None,
        })
    }

    #[cfg(test)]
    fn has_finished_app_entry(&self, flow_id: FlowId) -> bool {
        self.entries
            .get(&flow_id)
            .map_or(false, |entry| match entry {
                FlowEntry::App(app_entry) => matches!(app_entry.stage, AppStage::Finished),
                _ => false,
            })
    }
}

#[derive(Debug)]
enum RouteUpdateResult {
    AppliedActive,
    AppliedFinished,
    IgnoredLate,
    Unchanged,
}

#[derive(Debug)]
enum FlowFinishStatus {
    Accepted,
    Late,
    Duplicate,
}

#[derive(Debug)]
enum UserFinishStatus {
    Ok,
    ControllerMismatch,
}

struct FlowStatsReporter {
    controller: ControllerInterfaceHandle,
    receiver: UnboundedReceiver<FlowStatsMessage>,
    flows: FlowStore,
}

impl FlowStatsReporter {
    fn new(
        controller: ControllerInterfaceHandle,
        receiver: UnboundedReceiver<FlowStatsMessage>,
    ) -> Self {
        Self {
            controller,
            receiver,
            flows: FlowStore::default(),
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
                self.flows.handle_app_flow_start(app_flow);
            }
            FlowStatsMessage::RouteAssigned(route_assigned) => {
                self.flows.handle_route_assigned(route_assigned);
            }
            FlowStatsMessage::FlowFinished(flow_finished) => {
                self.flows.handle_flow_finished(flow_finished);
            }
            FlowStatsMessage::UserFlowStart(user_flow_start) => {
                self.flows.handle_user_flow_start(user_flow_start);
            }
        }
    }

    async fn flush(&mut self) {
        let now_ms = current_time_millis();
        let FlushOutput {
            app_flows,
            assignments,
            finishes,
            user_starts,
        } = self.flows.flush(now_ms);

        if app_flows.is_empty()
            && assignments.is_empty()
            && finishes.is_empty()
            && user_starts.is_empty()
        {
            return;
        }

        if !app_flows.is_empty() {
            debug!("Sending {} AppFlowStart messages", app_flows.len());
            let msg = DataplaneToController::AppFlowStart {
                appflows: app_flows,
            };
            self.controller.send(msg).await;
        }

        if !user_starts.is_empty() {
            debug!("Sending {} UserFlowStart messages.", user_starts.len());
            let msg = DataplaneToController::UserFlowStart { flows: user_starts };
            self.controller.send(msg).await;
        }

        if !assignments.is_empty() {
            debug!("Sending {} RouteAssigned messages.", assignments.len());
            let msg = DataplaneToController::RouteAssigned { assignments };
            self.controller.send(msg).await;
        }

        if !finishes.is_empty() {
            debug!("Sending {} FlowFinished messages.", finishes.len());
            let msg = DataplaneToController::FlowFinished { flows: finishes };
            self.controller.send(msg).await;
        }
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

        assert_eq!(
            reporter.flows.pending_route(flow_id),
            Some(route_id),
            "pending route should be buffered before start"
        );

        set_current_time_millis_for_test(35_000);
        reporter.flush().await;

        assert_eq!(
            reporter.flows.pending_route(flow_id),
            Some(route_id),
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
            .flows
            .app_entry(flow_id)
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
        assert!(
            !reporter.flows.has_finished_app_entry(1),
            "first flow should be cleaned up"
        );
        // Flow 2 should still be there
        assert!(
            reporter.flows.has_finished_app_entry(2),
            "second flow should remain"
        );
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
