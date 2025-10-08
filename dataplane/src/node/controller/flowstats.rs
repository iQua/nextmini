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

pub enum FlowStatsMessage {
    NewFlow(NewFlow),
    RouteAssigned(RouteAssigned),
    FlowFinished(i32),
}
