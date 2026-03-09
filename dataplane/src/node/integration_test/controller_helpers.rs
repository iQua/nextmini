use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use nextmini_messages::{DataplaneToController, GroupRouteTree};

use crate::node::controller::interface::ControllerInterfaceHandle;
use crate::node::python::interface::{PythonEvent, PythonInterfaceHandle};

pub(crate) struct ControllerHarness {
    controller: ControllerInterfaceHandle,
    events: PythonInterfaceHandle,
    stash: VecDeque<PythonEvent>,
}

impl ControllerHarness {
    pub(crate) fn new(
        controller: ControllerInterfaceHandle,
        events: PythonInterfaceHandle,
    ) -> Self {
        Self {
            controller,
            events,
            stash: VecDeque::new(),
        }
    }

    pub(crate) async fn wait_for_topology_ready(&mut self, timeout: Duration) -> bool {
        self.wait_for_event_matching(Some(timeout), |event| {
            matches!(event, PythonEvent::TopologyReady)
        })
        .await
        .is_some()
    }

    pub(crate) async fn create_group(
        &mut self,
        label: String,
        timeout: Duration,
    ) -> Result<(usize, Ipv4Addr, usize), String> {
        self.controller
            .send(DataplaneToController::CreateGroup { label })
            .await;

        match self
            .wait_for_event_matching(Some(timeout), |event| {
                matches!(event, PythonEvent::GroupCreated { .. })
            })
            .await
        {
            Some(PythonEvent::GroupCreated {
                group_id,
                src_node_id,
                group_ip,
            }) => Ok((group_id, group_ip, src_node_id)),
            _ => Err("timed out waiting for GroupCreated".to_string()),
        }
    }

    pub(crate) async fn join_group(&self, group_id: usize) {
        self.controller
            .send(DataplaneToController::JoinGroup { group_id })
            .await;
    }

    pub(crate) async fn set_group_routes(
        &mut self,
        group_id: usize,
        src_node_id: usize,
        edges: Vec<(u32, u32)>,
        timeout: Duration,
    ) -> Result<(), String> {
        self.controller
            .send(DataplaneToController::SetGroupRoutes { group_id, edges })
            .await;

        self.wait_for_routes_installed(group_id, src_node_id, timeout)
            .await
    }

    pub(crate) async fn set_group_routes_multi(
        &mut self,
        group_id: usize,
        src_node_id: usize,
        trees: Vec<GroupRouteTree>,
        timeout: Duration,
    ) -> Result<(), String> {
        self.controller
            .send(DataplaneToController::SetGroupRoutesMulti { group_id, trees })
            .await;

        self.wait_for_routes_installed(group_id, src_node_id, timeout)
            .await
    }

    async fn wait_for_routes_installed(
        &mut self,
        group_id: usize,
        src_node_id: usize,
        timeout: Duration,
    ) -> Result<(), String> {
        match self
            .wait_for_event_matching(Some(timeout), |event| {
                matches!(
                    event,
                    PythonEvent::GroupRoutesInstalled {
                        group_id: gid,
                        src_node_id: sid,
                        routes,
                    } if *gid == group_id && *sid == src_node_id && !routes.is_empty()
                )
            })
            .await
        {
            Some(PythonEvent::GroupRoutesInstalled { .. }) => Ok(()),
            _ => Err(format!(
                "timed out waiting for GroupRoutesInstalled for group {group_id}"
            )),
        }
    }

    async fn wait_for_event_matching<F>(
        &mut self,
        timeout: Option<Duration>,
        mut matcher: F,
    ) -> Option<PythonEvent>
    where
        F: FnMut(&PythonEvent) -> bool,
    {
        let deadline = timeout.map(|dur| Instant::now() + dur);

        for idx in 0..self.stash.len() {
            if matcher(&self.stash[idx]) {
                return self.stash.remove(idx);
            }
        }

        loop {
            if let Some(dl) = deadline
                && Instant::now() >= dl
            {
                return None;
            }

            let remaining = deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
            let event = self.recv_event_with_timeout(remaining).await?;

            if matcher(&event) {
                return Some(event);
            }

            self.stash.push_back(event);
        }
    }

    async fn recv_event_with_timeout(&self, timeout: Option<Duration>) -> Option<PythonEvent> {
        match timeout {
            Some(duration) => tokio::time::timeout(duration, self.events.next_event())
                .await
                .ok()
                .flatten(),
            None => self.events.next_event().await,
        }
    }
}
