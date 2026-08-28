//! A processor actor forwards packets from its upstream actors (LocalInterface and
//! NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
//! multiple processor tasks to handle incoming packets concurrently, allowing efficient
//! processing and routing of network packets.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use ahash::AHashMap;
use jumphash::JumpHasher;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, timeout};
use tracing::{debug, error, info, warn};

use nextmini_messages::{
    GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, INVALID, OperatingMode,
    RoutingTableEntry, TokenBucketSpec,
};

use crate::node::config::{Feature, LocalConfig};
use crate::node::connector::Connector;
use crate::node::connector::ConnectorMessage;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
use crate::node::network::scope::{ScopedNode, TransportScope};
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
#[cfg(feature = "python-extension")]
use crate::node::python::interface::PythonInterfaceHandle;
use crate::node::route::RoutingTable;
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::session::api::{InboundFrame as LosslessInboundFrame, LosslessRuntimeHandle};
use crate::node::{FlowId, FlowIdExt, NodeId};

// Keep tree-aware ingress hashing deterministic and aligned with FlowIdExt::hash.
const FLOW_TREE_HASH_KEY_0: u64 = 0x1234567890ABCDEF;
const FLOW_TREE_HASH_KEY_1: u64 = 0xFEDCBA0987654321;
const PROCESSOR_CONTROL_BROADCAST_CAPACITY: usize = 1024;
const FANOUT_STATS_LOG_MASK: u64 = (1 << 12) - 1;

#[derive(Debug)]
struct SyncTracker {
    current_nonce: AtomicU64,
    ack_count: AtomicUsize,
    notify: Notify,
}

impl SyncTracker {
    fn new() -> Self {
        Self {
            current_nonce: AtomicU64::new(0),
            ack_count: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    fn begin(&self, nonce: u64) {
        self.ack_count.store(0, Ordering::Release);
        self.current_nonce.store(nonce, Ordering::Release);
    }

    fn note_ack(&self, nonce: u64) {
        if self.current_nonce.load(Ordering::Acquire) == nonce {
            self.ack_count.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
        }
    }
}

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
    SendLinkProbePackets {
        node_id: NodeId,
        packets: Vec<Packet>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
    Queued,
    WouldBlock,
    Closed,
}

/// Describes how lossless/FEC senders can interpret non-blocking processor ingress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LosslessIngressContract {
    /// Non-blocking submission is supported and `WouldBlock` is scoped to the
    /// tree-selected ingress lane. Collaborative multi-tree FEC is allowed.
    TreeVisibleNonBlocking,
    /// Non-blocking submission is supported, but all trees collapse onto one
    /// shared queue. `WouldBlock` is global and collaborative multi-tree FEC
    /// must be treated as unsupported on this path.
    SharedQueueNonBlocking,
}

/// Result of a non-blocking lossless submission attempt at processor ingress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LosslessIngressSubmission {
    pub contract: LosslessIngressContract,
    pub outcome: SendOutcome,
}

#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    UpdateRoutingTable(Vec<RoutingTableEntry>),
    UpdateGroupDirectory(Vec<GroupDirectoryEntry>),
    UpdateGroupRoutes {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    AddNode {
        scoped_node: ScopedNode,
        scheduler: SchedulerHandle,
        dispatcher: FanoutDispatcher,
    },
    ConnectLocalInterface(LocalInterfaceHandle),
    ConnectServerHandle(Box<UserSpaceServerHandle>),
    ConnectUserSpaceSender {
        flow_id: FlowId,
        sender: UserSpaceSender,
    },
    DisconnectUserSpaceSender(FlowId),
    RateLimit(NodeId, TokenBucketSpec),
    SetFlowWeight(FlowId, usize),
    PinRouteForFlow(FlowId, usize),
    SetFlowStatsReporter(Box<FlowStatsReporterHandle>),
    #[cfg(feature = "python-extension")]
    ConnectPythonInterface(PythonInterfaceHandle),
    ConnectLosslessHandle(LosslessRuntimeHandle),
    Sync(u64),
}

#[derive(Clone, Debug)]
pub enum ProcessorHandle {
    Sequential(SequentialProcHandle),
    Concurrent(ConcurrentProcHandle),
}

impl ProcessorHandle {
    pub fn new(config: LocalConfig) -> Self {
        match config.feature {
            Feature::Sequential => ProcessorHandle::Sequential(SequentialProcHandle::new(config)),
            Feature::Concurrent => ProcessorHandle::Concurrent(ConcurrentProcHandle::new(config)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_sequential_stub_for_test(
        config: LocalConfig,
    ) -> (Self, mpsc::Receiver<ProcessorPacket>) {
        let capacity = config.channel_capacity.max(1);
        let (packet_sender, packet_receiver) = mpsc::channel(capacity);
        let (connector_packet_sender, _connector_packet_receiver) = mpsc::channel(capacity);
        let (connector_message_sender, _connector_message_receiver) = mpsc::channel(capacity);
        let (broadcast_sender, _) = broadcast::channel(capacity);
        let handle = SequentialProcHandle {
            config,
            broadcast_sender,
            packet_senders: vec![packet_sender],
            connector_packet_sender,
            connector_message_sender,
            sync_tracker: Arc::new(SyncTracker::new()),
            next_sync_nonce: Arc::new(AtomicU64::new(1)),
        };

        (ProcessorHandle::Sequential(handle), packet_receiver)
    }

    pub fn broadcast_sender(&self) -> &broadcast::Sender<ProcessorMessage> {
        match self {
            ProcessorHandle::Sequential(handle) => &handle.broadcast_sender,
            ProcessorHandle::Concurrent(handle) => &handle.broadcast_sender,
        }
    }

    pub fn connector_message_sender(&self) -> &mpsc::Sender<ConnectorMessage> {
        match self {
            ProcessorHandle::Sequential(handle) => &handle.connector_message_sender,
            ProcessorHandle::Concurrent(handle) => &handle.connector_message_sender,
        }
    }

    pub fn add_node(
        &self,
        node_id: NodeId,
        scope: TransportScope,
        scheduler: SchedulerHandle,
    ) -> Result<(), SendError<ProcessorMessage>> {
        let scoped_node = ScopedNode::new(node_id, scope);
        let config = match self {
            ProcessorHandle::Sequential(handle) => &handle.config,
            ProcessorHandle::Concurrent(handle) => &handle.config,
        };
        let capacity = config.effective_fanout_pending_capacity();
        let dispatcher = FanoutDispatcher::new(
            scoped_node,
            scheduler.clone(),
            capacity,
            config.channel_backpressure,
        );
        let _ = self.broadcast_sender().send(ProcessorMessage::AddNode {
            scoped_node,
            scheduler,
            dispatcher,
        })?;

        Ok(())
    }

    pub fn send_link_probe_packets(&self, node_id: NodeId, packets: Vec<Packet>) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.send_link_probe_packets(node_id, packets),
            ProcessorHandle::Concurrent(handle) => handle.send_link_probe_packets(node_id, packets),
        }
    }

    // connects the local interface to the processor
    pub fn connect_local_interface(&self, local_interface: LocalInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectLocalInterface(local_interface))
        {
            error!(
                "Error connecting the processors to the local interface: {}.",
                e
            );
        };
    }

    /// Connects the client handle to the processor.
    pub fn connect_user_space_sender(&self, flow_id: FlowId, sender: UserSpaceSender) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectUserSpaceSender { flow_id, sender })
        {
            error!(
                "Error connecting the client handle to the processors: {}.",
                e
            );
        };
    }

    /// Connects the in-process Python interface so local packets can be delivered directly.
    #[cfg(feature = "python-extension")]
    #[allow(dead_code)]
    pub fn connect_python_interface(&self, interface: PythonInterfaceHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectPythonInterface(interface))
        {
            error!(
                "Error sending the ConnectPythonInterface message to the processors: {}",
                e
            );
        };
    }

    pub fn connect_lossless_handle(&self, handle: LosslessRuntimeHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectLosslessHandle(handle))
        {
            error!(
                "Error sending the ConnectLosslessHandle message to the processors: {}",
                e
            );
        };
    }

    /// Disconnects the user-space packet sender from the processor's hashmap of senders.
    /// This is needed when a user-space TCP flow finishes.
    pub fn disconnect_user_space_sender(&self, flow_id: FlowId) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::DisconnectUserSpaceSender(flow_id))
        {
            error!(
                "Error sending the DisconnectUserSpaceSender message to the processors: {}",
                e
            );
        }
    }

    pub async fn update_routing_table(&self, routes: Vec<RoutingTableEntry>) {
        // broadcasts to all processors
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateRoutingTable(routes.clone()))
        {
            error!(
                "Error sending the UpdateRoutingTable message to the processors: {}",
                e
            );
        };

        // sends to the connector
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::UpdateRoutingTable(routes))
            .await
        {
            error!(
                "Error sending the UpdateRoutingTable message to the connector: {}",
                e
            );
        }
    }

    pub async fn update_group_directory(&self, groups: Vec<GroupDirectoryEntry>) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateGroupDirectory(groups))
        {
            error!(
                "Error sending the UpdateGroupDirectory message to the processors: {}",
                e
            );
        }
    }

    pub async fn update_group_routes(
        &self,
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    ) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            })
        {
            error!(
                "Error sending the UpdateGroupRoutes message to the processors: {}",
                e
            );
        }
    }

    pub async fn sync_workers(&self) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.sync_workers().await,
            ProcessorHandle::Concurrent(handle) => handle.sync_workers().await,
        }
    }

    #[cfg(test)]
    pub(crate) fn sync_snapshot_for_test(&self) -> (u64, usize, usize) {
        match self {
            ProcessorHandle::Sequential(handle) => (
                handle.sync_tracker.current_nonce.load(Ordering::Acquire),
                handle.sync_tracker.ack_count.load(Ordering::Acquire),
                handle.packet_senders.len(),
            ),
            ProcessorHandle::Concurrent(handle) => (
                handle.sync_tracker.current_nonce.load(Ordering::Acquire),
                handle.sync_tracker.ack_count.load(Ordering::Acquire),
                handle.worker_count,
            ),
        }
    }

    pub fn limit_rate(&self, node_id: NodeId, spec: TokenBucketSpec) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::RateLimit(node_id, spec))
        {
            error!(
                "Error sending the SetRateLimiter message to the processors: {}",
                e
            );
        };
    }

    pub async fn process_packet(&self, packet: Packet) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.process_packet(packet).await,
            ProcessorHandle::Concurrent(handle) => handle.process_packet(packet).await,
        }
    }

    pub fn try_process_packet(&self, packet: Packet) -> SendOutcome {
        match self {
            ProcessorHandle::Sequential(handle) => handle.try_process_packet(packet),
            ProcessorHandle::Concurrent(handle) => handle.try_process_packet(packet),
        }
    }

    /// Returns how a lossless sender should interpret non-blocking submission
    /// for this packet's ingress path.
    pub fn lossless_ingress_contract(&self, packet: &Packet) -> LosslessIngressContract {
        match self {
            ProcessorHandle::Sequential(handle) => handle.lossless_ingress_contract(packet),
            ProcessorHandle::Concurrent(handle) => handle.lossless_ingress_contract(packet),
        }
    }

    /// Non-blocking packet submission for lossless/FEC senders. The returned
    /// contract makes it explicit whether `WouldBlock` is tree-specific or a
    /// shared-queue/global signal for the selected ingress path.
    pub fn try_submit_lossless_packet(&self, packet: Packet) -> LosslessIngressSubmission {
        let contract = self.lossless_ingress_contract(&packet);
        let outcome = self.try_process_packet(packet);
        LosslessIngressSubmission { contract, outcome }
    }

    /// For synchronous producers (Python bindings, smoltcp virtual NIC).
    pub fn process_packet_blocking(&self, packet: Packet) {
        match self {
            ProcessorHandle::Sequential(handle) => handle.process_packet_blocking(packet),
            ProcessorHandle::Concurrent(handle) => handle.process_packet_blocking(packet),
        }
    }

    pub fn connect_server(&self, server: UserSpaceServerHandle) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::ConnectServerHandle(Box::new(server)))
        {
            error!(
                "Error sending the ConnectServerHandle message to the processors: {}",
                e
            );
        };
    }

    pub fn set_flow_weight(&self, flow_id: FlowId, weight: usize) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::SetFlowWeight(flow_id, weight))
        {
            error!(
                "Error sending the SetFlowWeight message to the processors: {}",
                e
            );
        };
    }

    /// Pins a specific route for a flow, bypassing normal route selection.
    pub fn pin_route_for_flow(&self, flow_id: FlowId, route_id: usize) {
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::PinRouteForFlow(flow_id, route_id))
        {
            error!(
                "Error sending the PinRouteForFlow message to the processors: {}",
                e
            );
        };
    }

    pub async fn set_flowstats_reporter(&self, flowstats_reporter: FlowStatsReporterHandle) {
        let broadcast_reporter = flowstats_reporter.clone();
        if let Err(e) = self
            .broadcast_sender()
            .send(ProcessorMessage::SetFlowStatsReporter(Box::new(
                broadcast_reporter,
            )))
        {
            error!(
                "Error sending the SetFlowStatsReporter message to the processors: {}",
                e
            );
        };

        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::SetFlowStatsReporter(Box::new(
                flowstats_reporter,
            )))
            .await
        {
            error!(
                "Error sending the SetFlowStatsReporter message to the connector: {}",
                e
            );
        };
    }

    pub async fn add_node_address(&self, node_id: NodeId, remote_addr: String) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::AddNodeAddress(node_id, remote_addr))
            .await
        {
            error!(
                "Error sending the AddNodeAddress message to the connector: {}",
                e
            );
        }
    }

    pub async fn connect_tcp_max_client(&self, tcp_max_client: TcpMaxClient) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::ConnectTcpMaxClient(Box::new(
                tcp_max_client,
            )))
            .await
        {
            error!(
                "Error sending the ConnectTcpMaxClient message to the connector: {}",
                e
            );
        }
    }

    pub async fn inbound_max_request(&self, flow_id: FlowId, stream: TcpStream) {
        if let Err(e) = self
            .connector_message_sender()
            .send(ConnectorMessage::InboundMaxRequest(flow_id, stream))
            .await
        {
            error!(
                "Error sending the InboundMaxRequest message to the connector: {}",
                e
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequentialProcHandle {
    config: LocalConfig,
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_senders: Vec<mpsc::Sender<ProcessorPacket>>,
    connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    connector_message_sender: mpsc::Sender<ConnectorMessage>,
    sync_tracker: Arc<SyncTracker>,
    next_sync_nonce: Arc<AtomicU64>,
}

impl SequentialProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(
            config
                .channel_capacity
                .max(PROCESSOR_CONTROL_BROADCAST_CAPACITY),
        );
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);
        let (sync_ack_sender, mut sync_ack_receiver) = mpsc::unbounded_channel();
        let sync_tracker = Arc::new(SyncTracker::new());
        let next_sync_nonce = Arc::new(AtomicU64::new(1));
        let sync_tracker_task = sync_tracker.clone();
        tokio::spawn(async move {
            while let Some(nonce) = sync_ack_receiver.recv().await {
                sync_tracker_task.note_ack(nonce);
            }
        });

        for _ in 0..config.num_packet_processors {
            // creates an mpsc channel for each processor
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let mut proc = Processor::new(
                PacketReceiver::Sequential(packet_receiver),
                broadcast_sender.subscribe(),
                sync_ack_sender.clone(),
                config.clone(),
            );

            tokio::spawn(async move {
                proc.run().await;
            });
        }

        // creates a packet channel for the connector
        let (connector_packet_sender, connector_packet_receiver) =
            mpsc::channel(config.channel_capacity);

        // creates a message channel for the connector
        let (connector_message_sender, connector_message_receiver) =
            mpsc::channel(config.channel_capacity);

        // creates a new connector
        let mut connector = Connector::new(
            connector_packet_receiver,
            connector_message_receiver,
            config.clone(),
        );

        // spawns a single connector task
        tokio::spawn(async move {
            connector.run().await;
        });

        Self {
            config,
            broadcast_sender,
            packet_senders,
            connector_packet_sender,
            connector_message_sender,
            sync_tracker,
            next_sync_nonce,
        }
    }

    async fn sync_workers(&self) {
        let worker_count = self.packet_senders.len();
        if worker_count == 0 {
            return;
        }

        let nonce = self.next_sync_nonce.fetch_add(1, Ordering::Relaxed);
        info!(
            nonce,
            worker_count, "Starting sequential processor worker sync"
        );
        self.sync_tracker.begin(nonce);
        if let Err(e) = self.broadcast_sender.send(ProcessorMessage::Sync(nonce)) {
            error!("Error sending the Sync message to the processors: {}", e);
            return;
        }

        while self.sync_tracker.ack_count.load(Ordering::Acquire) < worker_count {
            let notified = self.sync_tracker.notify.notified();
            if self.sync_tracker.ack_count.load(Ordering::Acquire) >= worker_count {
                break;
            }
            if timeout(Duration::from_millis(200), notified).await.is_err() {
                warn!(
                    nonce,
                    ack_count = self.sync_tracker.ack_count.load(Ordering::Acquire),
                    worker_count,
                    "Still waiting for sequential processor worker sync acknowledgements"
                );
            }
        }
        info!(
            nonce,
            worker_count, "Finished sequential processor worker sync"
        );
    }

    pub async fn process_packet(&self, packet: Packet) {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            self.send_to_processor(packet).await;
        } else {
            // sends according to the operating mode
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    self.send_to_processor(packet).await;
                }
                OperatingMode::Max => {
                    self.send_to_connector(packet).await;
                }
            }
        }
    }

    pub fn try_process_packet(&self, packet: Packet) -> SendOutcome {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            self.try_send_to_processor(packet)
        } else {
            // sends according to the operating mode
            match self.config.operating_mode {
                OperatingMode::Normal => self.try_send_to_processor(packet),
                OperatingMode::Max => self.try_send_to_connector(packet),
            }
        }
    }

    fn lossless_ingress_contract(&self, packet: &Packet) -> LosslessIngressContract {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());
        let uses_processor_ingress = dst_node_id == self.config.node_id
            || matches!(self.config.operating_mode, OperatingMode::Normal);
        if uses_processor_ingress
            && let TransportScope::Tree(tree_id) = TransportScope::from_packet(packet)
            && usize::from(tree_id) + 1 < self.packet_senders.len()
        {
            return LosslessIngressContract::TreeVisibleNonBlocking;
        }
        LosslessIngressContract::SharedQueueNonBlocking
    }

    fn select_processor_ingress_lane(&self, packet: &Packet) -> usize {
        let lane_count = self.packet_senders.len();
        let scope = TransportScope::from_packet(packet);
        match scope {
            TransportScope::Default => 0,
            TransportScope::Tree(tree_id)
                if lane_count > 1 && usize::from(tree_id) + 1 < lane_count =>
            {
                usize::from(tree_id) + 1
            }
            TransportScope::Tree(tree_id) => {
                let hasher = JumpHasher::new_with_keys(FLOW_TREE_HASH_KEY_0, FLOW_TREE_HASH_KEY_1);
                hasher.slot(&(packet.flow_id, tree_id), lane_count as u32) as usize
            }
        }
    }

    async fn send_to_processor(&self, packet: Packet) {
        let idx = self.select_processor_ingress_lane(&packet);
        let sender = &self.packet_senders[idx];

        if self.config.channel_backpressure {
            if let Err(e) = sender.send(ProcessorPacket::ProcessPacket(packet)).await {
                error!("SequentialProcHandle: processor channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
            warn!("SequentialProcHandle: processor channel full; dropping packet: {e}");
        }
    }

    async fn send_to_connector(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .connector_packet_sender
                .send(ProcessorPacket::ProcessPacket(packet))
                .await
            {
                error!("SequentialProcHandle: connector channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = self
            .connector_packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("SequentialProcHandle: connector channel full; dropping packet: {e}");
        }
    }

    fn send_link_probe_packets(&self, node_id: NodeId, packets: Vec<Packet>) {
        let msg = ProcessorPacket::SendLinkProbePackets { node_id, packets };
        let _ = self.packet_senders[0].try_send(msg);
    }

    fn try_send_to_processor(&self, packet: Packet) -> SendOutcome {
        let idx = self.select_processor_ingress_lane(&packet);
        let sender = &self.packet_senders[idx];
        map_tokio_try_send_outcome(sender.try_send(ProcessorPacket::ProcessPacket(packet)))
    }

    fn try_send_to_connector(&self, packet: Packet) -> SendOutcome {
        map_tokio_try_send_outcome(
            self.connector_packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet)),
        )
    }

    /// For sync producers (Python API, TCP readers, and QUIC readers).
    pub fn process_packet_blocking(&self, packet: Packet) {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        if dst_node_id == self.config.node_id {
            self.send_to_processor_blocking(packet);
        } else {
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    self.send_to_processor_blocking(packet);
                }
                OperatingMode::Max => {
                    self.send_to_connector_blocking(packet);
                }
            }
        }
    }

    fn send_to_processor_blocking(&self, packet: Packet) {
        let idx = self.select_processor_ingress_lane(&packet);
        let sender = &self.packet_senders[idx];

        if self.config.channel_backpressure {
            if let Err(e) = sender.blocking_send(ProcessorPacket::ProcessPacket(packet)) {
                warn!(
                    "SequentialProcHandle: blocking send to processor failed; dropping packet: {e}"
                );
            }
        } else if let Err(e) = sender.try_send(ProcessorPacket::ProcessPacket(packet)) {
            warn!("SequentialProcHandle: processor channel full; dropping packet: {e}");
        }
    }

    fn send_to_connector_blocking(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .connector_packet_sender
                .blocking_send(ProcessorPacket::ProcessPacket(packet))
            {
                warn!(
                    "SequentialProcHandle: blocking send to connector failed; dropping packet: {e}"
                );
            }
        } else if let Err(e) = self
            .connector_packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("SequentialProcHandle: connector channel full; dropping packet: {e}");
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConcurrentProcHandle {
    config: LocalConfig,
    broadcast_sender: broadcast::Sender<ProcessorMessage>,
    packet_sender: flume::Sender<ProcessorPacket>,
    connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    connector_message_sender: mpsc::Sender<ConnectorMessage>,
    worker_count: usize,
    sync_tracker: Arc<SyncTracker>,
    next_sync_nonce: Arc<AtomicU64>,
}

impl ConcurrentProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(
            config
                .channel_capacity
                .max(PROCESSOR_CONTROL_BROADCAST_CAPACITY),
        );
        let worker_count = config.num_packet_processors;
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);
        let (sync_ack_sender, mut sync_ack_receiver) = mpsc::unbounded_channel();
        let sync_tracker = Arc::new(SyncTracker::new());
        let next_sync_nonce = Arc::new(AtomicU64::new(1));
        let sync_tracker_task = sync_tracker.clone();
        tokio::spawn(async move {
            while let Some(nonce) = sync_ack_receiver.recv().await {
                sync_tracker_task.note_ack(nonce);
            }
        });

        for _ in 0..config.num_packet_processors {
            let mut proc = Processor::new(
                PacketReceiver::Concurrent(packet_receiver.clone()),
                broadcast_sender.subscribe(),
                sync_ack_sender.clone(),
                config.clone(),
            );

            tokio::spawn(async move {
                proc.run().await;
            });
        }

        // creates a new connector
        let (connector_packet_sender, connector_packet_receiver) =
            mpsc::channel(config.channel_capacity);
        let (connector_message_sender, connector_message_receiver) =
            mpsc::channel(config.channel_capacity);

        let mut connector = Connector::new(
            connector_packet_receiver,
            connector_message_receiver,
            config.clone(),
        );

        tokio::spawn(async move {
            connector.run().await;
        });

        Self {
            config,
            broadcast_sender,
            packet_sender,
            connector_packet_sender,
            connector_message_sender,
            worker_count,
            sync_tracker,
            next_sync_nonce,
        }
    }

    async fn sync_workers(&self) {
        if self.worker_count == 0 {
            return;
        }

        let nonce = self.next_sync_nonce.fetch_add(1, Ordering::Relaxed);
        info!(
            nonce,
            worker_count = self.worker_count,
            "Starting concurrent processor worker sync"
        );
        self.sync_tracker.begin(nonce);
        if let Err(e) = self.broadcast_sender.send(ProcessorMessage::Sync(nonce)) {
            error!("Error sending the Sync message to the processors: {}", e);
            return;
        }

        while self.sync_tracker.ack_count.load(Ordering::Acquire) < self.worker_count {
            let notified = self.sync_tracker.notify.notified();
            if self.sync_tracker.ack_count.load(Ordering::Acquire) >= self.worker_count {
                break;
            }
            if timeout(Duration::from_millis(200), notified).await.is_err() {
                warn!(
                    nonce,
                    ack_count = self.sync_tracker.ack_count.load(Ordering::Acquire),
                    worker_count = self.worker_count,
                    "Still waiting for concurrent processor worker sync acknowledgements"
                );
            }
        }
        info!(
            nonce,
            worker_count = self.worker_count,
            "Finished concurrent processor worker sync"
        );
    }

    pub async fn process_packet(&self, packet: Packet) {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            self.send_to_processor(packet).await;
        } else {
            // sends according to the operating mode
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    self.send_to_processor(packet).await;
                }
                OperatingMode::Max => {
                    self.send_to_connector(packet).await;
                }
            }
        }
    }

    pub fn try_process_packet(&self, packet: Packet) -> SendOutcome {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        // sends through the processor for local delivery
        if dst_node_id == self.config.node_id {
            self.try_send_to_processor(packet)
        } else {
            // sends according to the operating mode
            match self.config.operating_mode {
                OperatingMode::Normal => self.try_send_to_processor(packet),
                OperatingMode::Max => self.try_send_to_connector(packet),
            }
        }
    }

    fn lossless_ingress_contract(&self, _packet: &Packet) -> LosslessIngressContract {
        LosslessIngressContract::SharedQueueNonBlocking
    }

    pub fn process_packet_blocking(&self, packet: Packet) {
        let dst_node_id = self.config.ip_to_node_id(packet.flow_id.dst_ip());

        if dst_node_id == self.config.node_id {
            self.send_to_processor_blocking(packet);
        } else {
            match self.config.operating_mode {
                OperatingMode::Normal => {
                    self.send_to_processor_blocking(packet);
                }
                OperatingMode::Max => {
                    self.send_to_connector_blocking(packet);
                }
            }
        }
    }

    async fn send_to_processor(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .packet_sender
                .send_async(ProcessorPacket::ProcessPacket(packet))
                .await
            {
                warn!("ConcurrentProcHandle: flume channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = self
            .packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("ConcurrentProcHandle: flume channel full; dropping packet: {e}");
        }
    }

    async fn send_to_connector(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .connector_packet_sender
                .send(ProcessorPacket::ProcessPacket(packet))
                .await
            {
                warn!("ConcurrentProcHandle: connector channel closed; dropping packet: {e}");
            }
        } else if let Err(e) = self
            .connector_packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("ConcurrentProcHandle: connector channel full; dropping packet: {e}");
        }
    }

    fn send_link_probe_packets(&self, node_id: NodeId, packets: Vec<Packet>) {
        let msg = ProcessorPacket::SendLinkProbePackets { node_id, packets };
        let _ = self.packet_sender.try_send(msg);
    }

    fn try_send_to_processor(&self, packet: Packet) -> SendOutcome {
        map_flume_try_send_outcome(
            self.packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet)),
        )
    }

    fn try_send_to_connector(&self, packet: Packet) -> SendOutcome {
        map_tokio_try_send_outcome(
            self.connector_packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet)),
        )
    }

    fn send_to_processor_blocking(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .packet_sender
                .send(ProcessorPacket::ProcessPacket(packet))
            {
                warn!(
                    "ConcurrentProcHandle: blocking send to flume channel failed; dropping packet: {e}"
                );
            }
        } else if let Err(e) = self
            .packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("ConcurrentProcHandle: flume channel full; dropping packet: {e}");
        }
    }

    fn send_to_connector_blocking(&self, packet: Packet) {
        if self.config.channel_backpressure {
            if let Err(e) = self
                .connector_packet_sender
                .blocking_send(ProcessorPacket::ProcessPacket(packet))
            {
                warn!(
                    "ConcurrentProcHandle: blocking send to connector failed; dropping packet: {e}"
                );
            }
        } else if let Err(e) = self
            .connector_packet_sender
            .try_send(ProcessorPacket::ProcessPacket(packet))
        {
            warn!("ConcurrentProcHandle: connector channel full; dropping packet: {e}");
        }
    }
}

fn map_tokio_try_send_outcome<T>(result: Result<(), mpsc::error::TrySendError<T>>) -> SendOutcome {
    match result {
        Ok(()) => SendOutcome::Queued,
        Err(mpsc::error::TrySendError::Full(_)) => SendOutcome::WouldBlock,
        Err(mpsc::error::TrySendError::Closed(_)) => SendOutcome::Closed,
    }
}

fn map_flume_try_send_outcome<T>(result: Result<(), flume::TrySendError<T>>) -> SendOutcome {
    match result {
        Ok(()) => SendOutcome::Queued,
        Err(flume::TrySendError::Full(_)) => SendOutcome::WouldBlock,
        Err(flume::TrySendError::Disconnected(_)) => SendOutcome::Closed,
    }
}

pub enum PacketReceiver {
    Sequential(mpsc::Receiver<ProcessorPacket>),
    Concurrent(flume::Receiver<ProcessorPacket>),
}

impl PacketReceiver {
    pub async fn recv(&mut self) -> Option<ProcessorPacket> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver.recv().await,
            PacketReceiver::Concurrent(receiver) => receiver.recv_async().await.ok(),
        }
    }

    pub fn try_recv(&mut self) -> Option<ProcessorPacket> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver.try_recv().ok(),
            PacketReceiver::Concurrent(receiver) => receiver.try_recv().ok(),
        }
    }
}

/// Process-wide admission lane shared by every processor worker for one scoped node.
///
/// The single drain task is the cross-worker merge point: Sequential lane affinity preserves
/// per-path FIFO, while Concurrent mode keeps its pre-existing reorder window. Pending memory is
/// bounded by `scoped_nodes × (fanout_pending_capacity + 1)` packets before scheduler buffering.
#[derive(Clone, Debug)]
pub struct FanoutDispatcher {
    sender: mpsc::Sender<Packet>,
    backpressure: bool,
    stats: Arc<FanoutDispatcherStats>,
    abort_handle: tokio::task::AbortHandle,
}

#[derive(Debug)]
struct FanoutDispatcherStats {
    scoped_node: ScopedNode,
    admitted_packets: AtomicU64,
    stall_entries: AtomicU64,
    pending_high_water: AtomicUsize,
    cumulative_blocked_nanos: AtomicU64,
}

impl FanoutDispatcherStats {
    fn new(scoped_node: ScopedNode) -> Self {
        Self {
            scoped_node,
            admitted_packets: AtomicU64::new(0),
            stall_entries: AtomicU64::new(0),
            pending_high_water: AtomicUsize::new(0),
            cumulative_blocked_nanos: AtomicU64::new(0),
        }
    }

    fn record_admitted(&self, pending: usize) {
        self.pending_high_water
            .fetch_max(pending, Ordering::Relaxed);
        let admitted = self.admitted_packets.fetch_add(1, Ordering::Relaxed) + 1;
        if admitted & FANOUT_STATS_LOG_MASK == 0 {
            self.log();
        }
    }

    fn record_stall_entry(&self) {
        let stall_entries = self.stall_entries.fetch_add(1, Ordering::Relaxed) + 1;
        // Report the first park immediately so a lane that never unblocks is still visible.
        if stall_entries == 1 || stall_entries & FANOUT_STATS_LOG_MASK == 0 {
            self.log();
        }
    }

    fn record_blocked(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.cumulative_blocked_nanos
            .fetch_add(nanos, Ordering::Relaxed);
    }

    fn log(&self) {
        debug!(
            remote_node_id = self.scoped_node.remote_node_id,
            scope = ?self.scoped_node.scope,
            admitted_packets = self.admitted_packets.load(Ordering::Relaxed),
            stall_entries = self.stall_entries.load(Ordering::Relaxed),
            pending_high_water = self.pending_high_water.load(Ordering::Relaxed),
            cumulative_blocked_ms = self.cumulative_blocked_nanos.load(Ordering::Relaxed)
                / 1_000_000,
            "Child-scoped fan-out dispatcher statistics"
        );
    }
}

impl FanoutDispatcher {
    fn new(
        scoped_node: ScopedNode,
        scheduler: SchedulerHandle,
        capacity: usize,
        backpressure: bool,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(capacity);
        let stats = Arc::new(FanoutDispatcherStats::new(scoped_node));
        let drain_task = tokio::spawn(async move {
            while let Some(packet) = receiver.recv().await {
                // Closed schedulers retain the pre-existing SchedulerHandle log-and-drop behavior.
                scheduler.send(packet).await;
            }
        });
        let abort_handle = drain_task.abort_handle();
        drop(drain_task);

        Self {
            sender,
            backpressure,
            stats,
            abort_handle,
        }
    }

    fn abort(&self) {
        self.abort_handle.abort();
    }

    async fn send(&self, packet: Packet) {
        if self.backpressure {
            match self.sender.try_send(packet) {
                Ok(()) => {
                    self.record_pending();
                }
                Err(mpsc::error::TrySendError::Full(packet)) => {
                    self.stats.record_stall_entry();
                    let blocked_at = Instant::now();
                    let result = self.sender.send(packet).await;
                    self.stats.record_blocked(blocked_at.elapsed());
                    match result {
                        Ok(()) => self.record_pending(),
                        Err(e) => {
                            error!("FanoutDispatcher: channel closed; dropping packet: {e}");
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    error!("FanoutDispatcher: channel closed; dropping packet");
                }
            }
        } else if let Err(e) = self.sender.try_send(packet) {
            error!("FanoutDispatcher: channel full or closed; dropping packet: {e}");
        } else {
            self.record_pending();
        }
    }

    fn record_pending(&self) {
        self.stats.record_admitted(
            self.sender
                .max_capacity()
                .saturating_sub(self.sender.capacity()),
        );
    }
}

// Processes packets and forwards them to the next hop.
struct Processor {
    config: LocalConfig,

    // receives packets from the network interface, local interface, or user-space TCP flows
    packet_receiver: PacketReceiver,

    // receives messages from the broadcast channel (from the controller interface or the conductor)
    broadcast_receiver: broadcast::Receiver<ProcessorMessage>,

    // the local TUN interface
    local_interface: Option<Arc<LocalInterfaceHandle>>,

    // channel senders for packets in user-space TCP flows
    user_space_senders: AHashMap<FlowId, UserSpaceSender>,

    // the user-space TCP server handle
    server: Option<UserSpaceServerHandle>,

    // the routing table
    routing_table: RoutingTable,

    // optional flow stats reporter for route telemetry
    flowstats_reporter: Option<FlowStatsReporterHandle>,

    // schedulers keyed by remote node and transport scope
    schedulers: AHashMap<ScopedNode, SchedulerHandle>,

    // child-scoped pending lanes keyed by the same remote node and transport scope
    fanout_dispatchers: AHashMap<ScopedNode, FanoutDispatcher>,

    // optional in-process Python delivery path
    #[cfg(feature = "python-extension")]
    python_interface: Option<PythonInterfaceHandle>,
    lossless_handle: Option<LosslessRuntimeHandle>,
    sync_ack_sender: mpsc::UnboundedSender<u64>,
}

impl Processor {
    pub fn new(
        packet_receiver: PacketReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
        sync_ack_sender: mpsc::UnboundedSender<u64>,
        config: LocalConfig,
    ) -> Self {
        Self {
            packet_receiver,
            broadcast_receiver,
            local_interface: None,
            user_space_senders: AHashMap::new(),
            server: None,
            routing_table: RoutingTable::new(config.clone()),
            flowstats_reporter: None,
            schedulers: AHashMap::new(),
            fanout_dispatchers: AHashMap::new(),
            config,
            #[cfg(feature = "python-extension")]
            python_interface: None,
            lossless_handle: None,
            sync_ack_sender,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for the first packet or a broadcast message
                Some(first_msg) = self.packet_receiver.recv() => {
                    self.handle_packet_message(first_msg).await;

                    // starts processing queued packet messages in batches
                    while let Some(msg) = self.packet_receiver.try_recv() {
                        self.handle_packet_message(msg).await;
                    }
                }
                recv_result = self.broadcast_receiver.recv() => {
                    match recv_result {
                        Ok(broadcast_msg) => self.handle_message(broadcast_msg).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "Processor control broadcast lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                else => break,
            }
        }
    }

    async fn handle_packet_message(&mut self, msg: ProcessorPacket) {
        match msg {
            ProcessorPacket::ProcessPacket(packet) => {
                self.process_packet(packet).await;
            }
            ProcessorPacket::SendLinkProbePackets { node_id, packets } => {
                self.send_link_probe_packets(node_id, packets);
            }
        }
    }

    fn send_link_probe_packets(&self, node_id: NodeId, packets: Vec<Packet>) {
        if let Some(scheduler) = self
            .schedulers
            .get(&ScopedNode::new(node_id, TransportScope::Default))
        {
            scheduler.send_link_probe_packets(packets);
        }
    }

    async fn handle_message(&mut self, msg: ProcessorMessage) {
        match msg {
            ProcessorMessage::UpdateRoutingTable(routes) => {
                self.routing_table.install_routes(routes);
            }
            ProcessorMessage::UpdateGroupDirectory(groups) => {
                self.routing_table.install_group_directory(groups);
            }
            ProcessorMessage::UpdateGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                self.routing_table
                    .install_group_routes(group_id, src_node_id, routes);
            }
            ProcessorMessage::AddNode {
                scoped_node,
                scheduler,
                dispatcher,
            } => {
                if let Some(replaced) = self.fanout_dispatchers.insert(scoped_node, dispatcher) {
                    // Reconnects re-register the scoped node. Abort the old drain task so packets
                    // queued for the dead scheduler are discarded instead of leaking into the new
                    // connection, matching pre-dispatcher replacement semantics.
                    replaced.abort();
                }
                self.schedulers.insert(scoped_node, scheduler);
            }
            ProcessorMessage::ConnectLocalInterface(local_interface) => {
                self.local_interface = Some(Arc::new(local_interface));
            }
            ProcessorMessage::ConnectUserSpaceSender { flow_id, sender } => {
                self.user_space_senders.insert(flow_id, sender);
            }
            ProcessorMessage::DisconnectUserSpaceSender(flow_id) => {
                self.user_space_senders.remove(&flow_id);
            }
            ProcessorMessage::RateLimit(node_id, spec) => {
                for (scoped_node, scheduler) in self.schedulers.iter() {
                    if scoped_node.remote_node_id == node_id {
                        scheduler.limit_rate(spec.clone());
                    }
                }
            }
            ProcessorMessage::ConnectServerHandle(user_space_server) => {
                self.server = Some(*user_space_server);
            }
            ProcessorMessage::SetFlowWeight(flow_id, weight) => {
                // updates the flow weight for all schedulers
                for (_, scheduler) in self.schedulers.iter_mut() {
                    scheduler.set_flow_weight(flow_id, weight);
                }
            }
            ProcessorMessage::PinRouteForFlow(flow_id, route_id) => {
                if let Err(e) = self.routing_table.pin_route_for_flow(flow_id, route_id) {
                    warn!(
                        "Failed to pin route {} for flow {:032x}: {}",
                        route_id, flow_id, e
                    );
                }
            }
            ProcessorMessage::SetFlowStatsReporter(flowstats_reporter) => {
                self.flowstats_reporter = Some(*flowstats_reporter);
            }
            #[cfg(feature = "python-extension")]
            ProcessorMessage::ConnectPythonInterface(interface) => {
                self.python_interface = Some(interface);
            }
            ProcessorMessage::ConnectLosslessHandle(handle) => {
                self.lossless_handle = Some(handle);
            }
            ProcessorMessage::Sync(nonce) => {
                let _ = self.sync_ack_sender.send(nonce);
            }
        }
    }

    /// Processes inbound packets for outbound delivery.
    async fn process_packet(&mut self, packet: Packet) {
        let fec_tree_id = packet.lossless_fec_tree_id();
        self.forward_packet(packet, fec_tree_id).await;
    }

    async fn forward_packet(&mut self, packet: Packet, fec_tree_id: Option<u16>) {
        let packet_flow_id = packet.flow_id;

        let reporter = self.flowstats_reporter.as_ref();
        match self.routing_table.get_next_hops_by_flow_and_tree(
            packet_flow_id,
            fec_tree_id,
            reporter,
        ) {
            Ok(next_hops) => {
                if next_hops.is_empty() {
                    error!("No next hops available for flow {}.", packet_flow_id);
                    return;
                }

                let last = next_hops.len() - 1;
                let mut primary_packet = Some(packet);

                for (idx, next_hop_id) in next_hops.into_iter().enumerate() {
                    let pkt = if idx == last {
                        primary_packet
                            .take()
                            .expect("packet already dispatched to last hop")
                    } else {
                        primary_packet
                            .as_ref()
                            .expect("packet missing during multicast fan-out")
                            .clone()
                    };

                    self.send_packet(pkt, next_hop_id).await;
                }
            }
            Err(e) => {
                if let Some(tree_id) = fec_tree_id
                    && e.contains("Unknown multicast tree route")
                {
                    warn!(
                        flow_id = packet_flow_id,
                        tree_id,
                        reason = %e,
                        "Dropping packet because multicast tree route is unknown"
                    );
                    return;
                }
                error!("Error resolving route for flow {}: {}", packet_flow_id, e);
            }
        }
    }

    /// Locates a channel sender for delivering packets in user-space flows, based on the flow ID.
    fn user_space_sender(&mut self, flow_id: FlowId) -> Option<UserSpaceSender> {
        if let Some(sender) = self.user_space_senders.get(&flow_id) {
            Some(sender.clone())
        } else {
            if flow_id.dst_port() != self.config.user_space_server_port {
                return None;
            }

            let server_handle = self
                .server
                .clone()
                .expect("The user-space server has not yet been connected.");

            let sender = server_handle.add_server(flow_id);
            self.user_space_senders.insert(flow_id, sender.clone());

            Some(sender)
        }
    }

    /// Sends a packet to its destined next hop, including local delivery to the TUN interface,
    /// a user-space TCP client, or a user-space TCP server.
    async fn send_packet(&mut self, packet: Packet, next_hop_id: NodeId) {
        #[allow(unused_mut)]
        let mut packet = packet;

        // checks if the next hop is the dst node
        if next_hop_id == self.routing_table.local_id {
            // if possible, deliver to the lossless transport subsystem
            if self.try_deliver_lossless(&packet).await {
                return;
            }

            // local TUN delivery: use the destination IP address to distinguish between the TUN interface
            // and user-space TCP clients or servers
            if packet.flow_id.dst_ip() == self.config.local_address {
                if let Some(ref local_interface) = self.local_interface {
                    local_interface.write_packet(packet);
                } else {
                    error!("The local interface has not yet been connected.");
                }
            } else {
                #[cfg(feature = "python-extension")]
                if let Some(ref py_if) = self.python_interface {
                    match py_if.deliver(packet).await {
                        Ok(()) => return,
                        Err(returned_packet) => {
                            packet = returned_packet;
                        }
                    }
                }

                let flow_id = packet.flow_id;

                let dest = self.user_space_sender(flow_id);
                if let Some(sender) = dest
                    && sender.try_send(packet).is_err()
                {
                    error!("Failed to send a packet in user-space flows to its local destination.");
                }
            }
        } else {
            let scope = TransportScope::from_packet(&packet);
            let key = ScopedNode::new(next_hop_id, scope);
            if let Some(dispatcher) = self.fanout_dispatchers.get(&key).cloned() {
                dispatcher.send(packet).await;
            } else {
                error!(
                    next_hop_id,
                    ?scope,
                    "No fan-out dispatcher available for scoped remote transport"
                );
            }
        }
    }

    async fn try_deliver_lossless(&mut self, packet: &Packet) -> bool {
        let Some(handle) = self.lossless_handle.clone() else {
            return false;
        };
        let Some(session_id) = packet.lossless_session_id() else {
            return false;
        };
        let Some(payload) = packet.tcp_payload() else {
            return false;
        };

        let src_node = self.config.ip_to_node_id(packet.flow_id.src_ip());
        let peer_id = if src_node == INVALID {
            None
        } else {
            Some(src_node)
        };

        let payload_vec = payload.to_vec();

        handle
            .deliver(
                session_id,
                LosslessInboundFrame {
                    bytes: payload_vec,
                    peer_id,
                },
            )
            .await;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use nextmini_messages::MULTITREE_STRIDE;

    use super::*;
    use crate::node::scheduler::sched::SchedulerReaderMessage;

    fn base_config(operating_mode: OperatingMode) -> LocalConfig {
        LocalConfig {
            node_id: 1,
            local_address: Ipv4Addr::new(10, 0, 0, 1),
            operating_mode,
            channel_capacity: 1,
            num_packet_processors: 1,
            channel_backpressure: true,
            ..Default::default()
        }
    }

    fn make_packet(dst_ip: Ipv4Addr) -> Packet {
        Packet::build_ipv4_tcp_packet(Ipv4Addr::new(10, 0, 0, 9), 4000, dst_ip, 5000, b"x")
    }

    fn make_fec_packet(dst_ip: Ipv4Addr, tree_id: u16) -> Packet {
        Packet::build_ipv4_tcp_packet_with_lossless_meta(
            Ipv4Addr::new(10, 0, 0, 9),
            4000,
            dst_ip,
            5000,
            Some(crate::node::packet::LosslessTransportMeta {
                session_id: 17,
                tree_id: Some(tree_id),
            }),
            b"x",
        )
    }

    fn make_multicast_fec_packet(dst_ip: Ipv4Addr, tree_id: u16, sequence: u8) -> Packet {
        Packet::build_ipv4_tcp_packet_with_lossless_meta(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            dst_ip,
            5000,
            Some(crate::node::packet::LosslessTransportMeta {
                session_id: 17,
                tree_id: Some(tree_id),
            }),
            &[sequence],
        )
    }

    fn packet_sequence(packet: &Packet) -> u8 {
        *packet
            .tcp_payload()
            .and_then(|payload| payload.first())
            .expect("forwarded packet should carry its sequence")
    }

    async fn recv_scheduler_sequence(receiver: &mut mpsc::Receiver<SchedulerReaderMessage>) -> u8 {
        let SchedulerReaderMessage::InboundPacket(packet) = receiver
            .recv()
            .await
            .expect("scheduler channel closed before packet arrived");
        packet_sequence(&packet)
    }

    async fn wait_for_dispatcher_capacity(dispatcher: &FanoutDispatcher, capacity: usize) {
        timeout(Duration::from_secs(1), async {
            while dispatcher.sender.capacity() != capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dispatcher did not reach the expected pending capacity");
    }

    fn make_sequential_handle_with_lanes(
        config: LocalConfig,
        packet_senders: Vec<mpsc::Sender<ProcessorPacket>>,
        connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    ) -> SequentialProcHandle {
        let (broadcast_sender, _) = broadcast::channel(1);
        let (connector_message_sender, _) = mpsc::channel(1);
        SequentialProcHandle {
            config,
            broadcast_sender,
            packet_senders,
            connector_packet_sender,
            connector_message_sender,
            sync_tracker: Arc::new(SyncTracker::new()),
            next_sync_nonce: Arc::new(AtomicU64::new(1)),
        }
    }

    fn make_sequential_handle(
        config: LocalConfig,
        packet_sender: mpsc::Sender<ProcessorPacket>,
        connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    ) -> SequentialProcHandle {
        make_sequential_handle_with_lanes(config, vec![packet_sender], connector_packet_sender)
    }

    #[tokio::test]
    async fn child_scoped_fanout_isolates_blocked_first_child_and_preserves_fifo() {
        const GROUP_ID: GroupId = 7;
        const TREE_ID: u16 = 3;
        const BLOCKED_CHILD: NodeId = 3;
        const HEALTHY_CHILD: NodeId = 4;
        const PACKET_COUNT: usize = 2;
        const FANOUT_PENDING_CAPACITY: usize = 1;
        const COMPLETION_BOUND: Duration = Duration::from_secs(1);

        let group_ip = Ipv4Addr::new(10, 0, 0, 200);
        let mut config = base_config(OperatingMode::Normal);
        config.node_id = 2;
        config.local_address = Ipv4Addr::new(10, 0, 0, 2);
        config.virtual_base_addr = Ipv4Addr::new(10, 0, 0, 0);
        config.local_netmask = Ipv4Addr::new(255, 255, 255, 0);
        // Capacity one makes the single packet visible in the blocked scheduler deterministic:
        // its second packet waits in the dispatcher drain until the first packet is consumed.
        config.fanout_pending_capacity = Some(FANOUT_PENDING_CAPACITY);

        let (_packet_sender, packet_receiver) = mpsc::channel(1);
        let (_broadcast_sender, broadcast_receiver) = broadcast::channel(8);
        let (sync_ack_sender, _sync_ack_receiver) = mpsc::unbounded_channel();
        let mut processor = Processor::new(
            PacketReceiver::Sequential(packet_receiver),
            broadcast_receiver,
            sync_ack_sender,
            config,
        );

        processor
            .handle_message(ProcessorMessage::UpdateGroupDirectory(vec![
                GroupDirectoryEntry {
                    group_id: GROUP_ID,
                    group_ip,
                },
            ]))
            .await;
        processor
            .handle_message(ProcessorMessage::UpdateGroupRoutes {
                group_id: GROUP_ID,
                src_node_id: 1,
                routes: vec![GroupRoutingTableEntry {
                    route_id: GROUP_ID * MULTITREE_STRIDE + usize::from(TREE_ID),
                    next_hops: vec![BLOCKED_CHILD, HEALTHY_CHILD],
                    src_node_id: 1,
                    group_id: GROUP_ID,
                }],
            })
            .await;

        let (blocked_sender, mut blocked_receiver) = mpsc::channel(1);
        processor
            .handle_message({
                let scoped_node = ScopedNode::new(BLOCKED_CHILD, TransportScope::Tree(TREE_ID));
                let scheduler = SchedulerHandle::new_for_test(blocked_sender, true);
                ProcessorMessage::AddNode {
                    scoped_node,
                    dispatcher: FanoutDispatcher::new(
                        scoped_node,
                        scheduler.clone(),
                        FANOUT_PENDING_CAPACITY,
                        true,
                    ),
                    scheduler,
                }
            })
            .await;

        let (healthy_sender, mut healthy_receiver) = mpsc::channel(1);
        processor
            .handle_message({
                let scoped_node = ScopedNode::new(HEALTHY_CHILD, TransportScope::Tree(TREE_ID));
                let scheduler = SchedulerHandle::new_for_test(healthy_sender, true);
                ProcessorMessage::AddNode {
                    scoped_node,
                    dispatcher: FanoutDispatcher::new(
                        scoped_node,
                        scheduler.clone(),
                        FANOUT_PENDING_CAPACITY,
                        true,
                    ),
                    scheduler,
                }
            })
            .await;

        let forward_task = tokio::spawn(async move {
            for sequence in 0..PACKET_COUNT as u8 {
                processor
                    .process_packet(make_multicast_fec_packet(group_ip, TREE_ID, sequence))
                    .await;
            }
        });

        let healthy_sequences = timeout(COMPLETION_BOUND, async {
            let mut sequences = Vec::with_capacity(PACKET_COUNT);
            while sequences.len() < PACKET_COUNT {
                let SchedulerReaderMessage::InboundPacket(packet) = healthy_receiver
                    .recv()
                    .await
                    .expect("healthy child scheduler channel closed");
                sequences.push(packet_sequence(&packet));
            }
            sequences
        })
        .await
        .expect("healthy child did not receive all packets within the HOL bound");

        assert_eq!(healthy_sequences, vec![0, 1]);
        assert_eq!(
            blocked_receiver.len(),
            1,
            "blocked child scheduler should remain full"
        );
        timeout(COMPLETION_BOUND, forward_task)
            .await
            .expect("child-scoped fan-out should finish admission within the bound")
            .expect("fan-out task panicked");

        let blocked_sequences = timeout(COMPLETION_BOUND, async {
            let mut sequences = Vec::with_capacity(PACKET_COUNT);
            while sequences.len() < PACKET_COUNT {
                sequences.push(recv_scheduler_sequence(&mut blocked_receiver).await);
            }
            sequences
        })
        .await
        .expect("blocked child did not eventually receive all admitted packets");
        assert_eq!(blocked_sequences, vec![0, 1]);
    }

    #[tokio::test]
    async fn fanout_capacity_exhaustion_parks_producer_without_dropping() {
        const SENTINEL: u8 = u8::MAX;
        const TREE_ID: u16 = 7;
        const COMPLETION_BOUND: Duration = Duration::from_secs(1);

        let dst_ip = Ipv4Addr::new(10, 0, 0, 3);
        let scoped_node = ScopedNode::new(3, TransportScope::Tree(TREE_ID));
        let (scheduler_sender, mut scheduler_receiver) = mpsc::channel(1);
        scheduler_sender
            .try_send(SchedulerReaderMessage::InboundPacket(
                make_multicast_fec_packet(dst_ip, TREE_ID, SENTINEL),
            ))
            .expect("failed to prefill scheduler channel");
        let scheduler = SchedulerHandle::new_for_test(scheduler_sender, true);
        let dispatcher = FanoutDispatcher::new(scoped_node, scheduler, 1, true);

        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 0))
            .await;
        wait_for_dispatcher_capacity(&dispatcher, 1).await;
        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 1))
            .await;

        let parked_dispatcher = dispatcher.clone();
        let parked_send = tokio::spawn(async move {
            parked_dispatcher
                .send(make_multicast_fec_packet(dst_ip, TREE_ID, 2))
                .await;
        });
        timeout(COMPLETION_BOUND, async {
            while dispatcher.stats.stall_entries.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer did not park after exhausting pending capacity");
        assert!(!parked_send.is_finished());

        assert_eq!(
            recv_scheduler_sequence(&mut scheduler_receiver).await,
            SENTINEL
        );
        let mut sequences = Vec::new();
        for _ in 0..3 {
            sequences.push(
                timeout(
                    COMPLETION_BOUND,
                    recv_scheduler_sequence(&mut scheduler_receiver),
                )
                .await
                .expect("backpressured packet was dropped"),
            );
        }
        timeout(COMPLETION_BOUND, parked_send)
            .await
            .expect("parked producer did not resume")
            .expect("parked producer task panicked");
        assert_eq!(sequences, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn fanout_without_backpressure_drops_when_pending_capacity_is_full() {
        const SENTINEL: u8 = u8::MAX;
        const TREE_ID: u16 = 7;

        let dst_ip = Ipv4Addr::new(10, 0, 0, 3);
        let scoped_node = ScopedNode::new(3, TransportScope::Tree(TREE_ID));
        let (scheduler_sender, mut scheduler_receiver) = mpsc::channel(1);
        scheduler_sender
            .try_send(SchedulerReaderMessage::InboundPacket(
                make_multicast_fec_packet(dst_ip, TREE_ID, SENTINEL),
            ))
            .expect("failed to prefill scheduler channel");
        // Keep the scheduler lossless here so the observed drop is specifically the
        // dispatcher's legacy non-backpressured full-channel behavior.
        let scheduler = SchedulerHandle::new_for_test(scheduler_sender, true);
        let dispatcher = FanoutDispatcher::new(scoped_node, scheduler, 1, false);

        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 0))
            .await;
        wait_for_dispatcher_capacity(&dispatcher, 1).await;
        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 1))
            .await;
        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 2))
            .await;

        assert_eq!(
            recv_scheduler_sequence(&mut scheduler_receiver).await,
            SENTINEL
        );
        assert_eq!(recv_scheduler_sequence(&mut scheduler_receiver).await, 0);
        assert_eq!(recv_scheduler_sequence(&mut scheduler_receiver).await, 1);
        assert!(
            timeout(Duration::from_millis(50), scheduler_receiver.recv())
                .await
                .is_err(),
            "non-backpressured dispatcher should drop the packet that exceeds capacity"
        );
        assert_eq!(dispatcher.stats.admitted_packets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn fanout_pending_capacity_is_independent_of_scheduler_channel_capacity() {
        const SENTINEL: u8 = u8::MAX;
        const TREE_ID: u16 = 7;
        const FANOUT_PENDING_CAPACITY: usize = 3;

        let config = LocalConfig {
            channel_capacity: 1,
            fanout_pending_capacity: Some(FANOUT_PENDING_CAPACITY),
            ..Default::default()
        };
        let dst_ip = Ipv4Addr::new(10, 0, 0, 3);
        let scoped_node = ScopedNode::new(3, TransportScope::Tree(TREE_ID));
        let (scheduler_sender, mut scheduler_receiver) = mpsc::channel(config.channel_capacity);
        scheduler_sender
            .try_send(SchedulerReaderMessage::InboundPacket(
                make_multicast_fec_packet(dst_ip, TREE_ID, SENTINEL),
            ))
            .expect("failed to prefill scheduler channel");
        let scheduler = SchedulerHandle::new_for_test(scheduler_sender, true);
        let dispatcher = FanoutDispatcher::new(
            scoped_node,
            scheduler,
            config.effective_fanout_pending_capacity(),
            true,
        );

        dispatcher
            .send(make_multicast_fec_packet(dst_ip, TREE_ID, 0))
            .await;
        wait_for_dispatcher_capacity(&dispatcher, FANOUT_PENDING_CAPACITY).await;
        for sequence in 1..=FANOUT_PENDING_CAPACITY as u8 {
            dispatcher
                .send(make_multicast_fec_packet(dst_ip, TREE_ID, sequence))
                .await;
        }

        assert_eq!(dispatcher.sender.max_capacity(), FANOUT_PENDING_CAPACITY);
        assert_eq!(dispatcher.sender.capacity(), 0);

        assert_eq!(
            recv_scheduler_sequence(&mut scheduler_receiver).await,
            SENTINEL
        );
        for expected in 0..=FANOUT_PENDING_CAPACITY as u8 {
            assert_eq!(
                timeout(
                    Duration::from_secs(1),
                    recv_scheduler_sequence(&mut scheduler_receiver),
                )
                .await
                .expect("buffered packet did not reach scheduler"),
                expected
            );
        }
    }

    #[tokio::test]
    async fn fanout_dispatcher_is_shared_and_replacement_aborts_old_drain() {
        let config = base_config(OperatingMode::Normal);
        let (_packet_sender, packet_receiver) = mpsc::channel(1);
        let (_broadcast_sender, broadcast_receiver) = broadcast::channel(1);
        let (sync_ack_sender, _sync_ack_receiver) = mpsc::unbounded_channel();
        let mut processor = Processor::new(
            PacketReceiver::Sequential(packet_receiver),
            broadcast_receiver,
            sync_ack_sender,
            config,
        );
        let scoped_node = ScopedNode::new(3, TransportScope::Tree(7));

        let (old_scheduler_sender, _old_scheduler_receiver) = mpsc::channel(1);
        let old_scheduler = SchedulerHandle::new_for_test(old_scheduler_sender, true);
        let old_dispatcher = FanoutDispatcher::new(scoped_node, old_scheduler.clone(), 1, true);
        let shared_dispatcher = old_dispatcher.clone();
        assert!(
            old_dispatcher
                .sender
                .same_channel(&shared_dispatcher.sender),
            "dispatcher clones should share one pending lane"
        );
        assert!(Arc::ptr_eq(&old_dispatcher.stats, &shared_dispatcher.stats));
        processor
            .handle_message(ProcessorMessage::AddNode {
                scoped_node,
                scheduler: old_scheduler,
                dispatcher: shared_dispatcher,
            })
            .await;

        let (new_scheduler_sender, _new_scheduler_receiver) = mpsc::channel(1);
        let new_scheduler = SchedulerHandle::new_for_test(new_scheduler_sender, true);
        processor
            .handle_message(ProcessorMessage::AddNode {
                scoped_node,
                dispatcher: FanoutDispatcher::new(scoped_node, new_scheduler.clone(), 1, true),
                scheduler: new_scheduler,
            })
            .await;
        tokio::task::yield_now().await;

        assert!(old_dispatcher.abort_handle.is_finished());
    }

    fn make_concurrent_handle(
        config: LocalConfig,
        packet_sender: flume::Sender<ProcessorPacket>,
        connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    ) -> ConcurrentProcHandle {
        let (broadcast_sender, _) = broadcast::channel(1);
        let (connector_message_sender, _) = mpsc::channel(1);
        ConcurrentProcHandle {
            config,
            broadcast_sender,
            packet_sender,
            connector_packet_sender,
            connector_message_sender,
            worker_count: 1,
            sync_tracker: Arc::new(SyncTracker::new()),
            next_sync_nonce: Arc::new(AtomicU64::new(1)),
        }
    }

    #[test]
    fn sequential_try_process_packet_routes_local_packets_to_processor_in_max_mode() {
        let config = base_config(OperatingMode::Max);
        let local_ip = config.local_address;

        let (processor_sender, _processor_receiver) = mpsc::channel(1);
        processor_sender
            .try_send(ProcessorPacket::ProcessPacket(make_packet(local_ip)))
            .expect("failed to fill processor lane");

        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);

        let handle = make_sequential_handle(config, processor_sender, connector_sender);
        assert_eq!(
            handle.try_process_packet(make_packet(local_ip)),
            SendOutcome::WouldBlock
        );
    }

    #[test]
    fn sequential_try_process_packet_routes_remote_packets_to_connector_in_max_mode() {
        let config = base_config(OperatingMode::Max);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (processor_sender, processor_receiver) = mpsc::channel(1);
        drop(processor_receiver);

        let (connector_sender, _connector_receiver) = mpsc::channel(1);
        connector_sender
            .try_send(ProcessorPacket::ProcessPacket(make_packet(remote_ip)))
            .expect("failed to fill connector lane");

        let handle = make_sequential_handle(config, processor_sender, connector_sender);
        assert_eq!(
            handle.try_process_packet(make_packet(remote_ip)),
            SendOutcome::WouldBlock
        );
    }

    #[test]
    fn sequential_try_process_packet_reports_closed_when_processor_lane_closed() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (processor_sender, processor_receiver) = mpsc::channel(1);
        drop(processor_receiver);
        let (connector_sender, _connector_receiver) = mpsc::channel(1);

        let handle = make_sequential_handle(config, processor_sender, connector_sender);
        assert_eq!(
            handle.try_process_packet(make_packet(remote_ip)),
            SendOutcome::Closed
        );
    }

    #[test]
    fn sequential_non_fec_ingress_lane_uses_flow_hash() {
        let mut config = base_config(OperatingMode::Normal);
        config.num_packet_processors = 4;
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let mut processor_senders = Vec::new();
        for _ in 0..config.num_packet_processors {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            processor_senders.push(sender);
        }
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle_with_lanes(config, processor_senders, connector_sender);

        let packet = make_packet(remote_ip);
        assert_eq!(
            handle.select_processor_ingress_lane(&packet),
            packet.flow_id.hash(handle.packet_senders.len())
        );
    }

    #[test]
    fn sequential_lossless_ingress_contract_is_tree_visible_on_dedicated_lane() {
        let mut config = base_config(OperatingMode::Normal);
        config.num_packet_processors = 4;
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let mut processor_senders = Vec::new();
        for _ in 0..config.num_packet_processors {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            processor_senders.push(sender);
        }
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle_with_lanes(config, processor_senders, connector_sender);
        let packet = make_fec_packet(remote_ip, 1);

        assert_eq!(
            handle.lossless_ingress_contract(&packet),
            LosslessIngressContract::TreeVisibleNonBlocking
        );
    }

    #[test]
    fn sequential_lossless_ingress_contract_is_shared_with_one_lane() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);
        let (processor_sender, processor_receiver) = mpsc::channel(1);
        drop(processor_receiver);
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle(config, processor_sender, connector_sender);

        assert_eq!(
            handle.lossless_ingress_contract(&make_fec_packet(remote_ip, 0)),
            LosslessIngressContract::SharedQueueNonBlocking
        );
    }

    #[test]
    fn sequential_lossless_ingress_contract_is_shared_on_hashed_lane() {
        let mut config = base_config(OperatingMode::Normal);
        config.num_packet_processors = 4;
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);
        let mut processor_senders = Vec::new();
        for _ in 0..config.num_packet_processors {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            processor_senders.push(sender);
        }
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle_with_lanes(config, processor_senders, connector_sender);

        assert_eq!(
            handle.lossless_ingress_contract(&make_fec_packet(remote_ip, 7)),
            LosslessIngressContract::SharedQueueNonBlocking
        );
    }

    #[test]
    fn sequential_try_process_packet_exposes_per_tree_backpressure_domains() {
        let mut config = base_config(OperatingMode::Normal);
        config.num_packet_processors = 4;
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let mut processor_senders = Vec::new();
        // Keep receivers alive so lanes can become Full instead of Closed.
        let mut _processor_receivers = Vec::new();
        for _ in 0..config.num_packet_processors {
            let (sender, receiver) = mpsc::channel(1);
            processor_senders.push(sender);
            _processor_receivers.push(receiver);
        }
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle_with_lanes(config, processor_senders, connector_sender);

        let blocked_tree = 0;
        let writable_tree = 1;
        let blocked_lane =
            handle.select_processor_ingress_lane(&make_fec_packet(remote_ip, blocked_tree));
        let writable_lane =
            handle.select_processor_ingress_lane(&make_fec_packet(remote_ip, writable_tree));
        assert_ne!(blocked_lane, writable_lane);
        handle.packet_senders[blocked_lane]
            .try_send(ProcessorPacket::ProcessPacket(make_packet(remote_ip)))
            .expect("failed to fill selected tree lane");

        let blocked_attempt = ProcessorHandle::Sequential(handle.clone())
            .try_submit_lossless_packet(make_fec_packet(remote_ip, blocked_tree));
        assert_eq!(
            blocked_attempt.contract,
            LosslessIngressContract::TreeVisibleNonBlocking
        );
        assert_eq!(blocked_attempt.outcome, SendOutcome::WouldBlock);

        let writable_attempt = ProcessorHandle::Sequential(handle)
            .try_submit_lossless_packet(make_fec_packet(remote_ip, writable_tree));
        assert_eq!(
            writable_attempt.contract,
            LosslessIngressContract::TreeVisibleNonBlocking
        );
        assert_eq!(writable_attempt.outcome, SendOutcome::Queued);
    }

    #[test]
    fn sequential_process_packet_blocking_routes_fec_tree_to_selected_lane() {
        let mut config = base_config(OperatingMode::Normal);
        config.num_packet_processors = 4;
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let mut processor_senders = Vec::new();
        let mut processor_receivers = Vec::new();
        for _ in 0..config.num_packet_processors {
            let (sender, receiver) = mpsc::channel(1);
            processor_senders.push(sender);
            processor_receivers.push(receiver);
        }
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);
        let handle = make_sequential_handle_with_lanes(config, processor_senders, connector_sender);

        let packet = make_fec_packet(remote_ip, 3);
        let expected_lane = handle.select_processor_ingress_lane(&packet);
        handle.process_packet_blocking(packet);

        for (idx, receiver) in processor_receivers.iter_mut().enumerate() {
            let recv_result = receiver.try_recv();
            if idx == expected_lane {
                assert!(
                    recv_result.is_ok(),
                    "expected selected lane to receive packet"
                );
            } else {
                assert!(
                    matches!(recv_result, Err(mpsc::error::TryRecvError::Empty)),
                    "unexpected packet on non-selected lane {idx}"
                );
            }
        }
    }

    #[test]
    fn sequential_max_remote_lossless_ingress_contract_uses_shared_queue() {
        let config = base_config(OperatingMode::Max);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (processor_sender, processor_receiver) = mpsc::channel(1);
        drop(processor_receiver);
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);

        let handle = make_sequential_handle(config, processor_sender, connector_sender);
        let packet = make_fec_packet(remote_ip, 9);

        assert_eq!(
            handle.lossless_ingress_contract(&packet),
            LosslessIngressContract::SharedQueueNonBlocking
        );
    }

    #[test]
    fn concurrent_try_process_packet_routes_remote_packets_to_connector_in_max_mode() {
        let config = base_config(OperatingMode::Max);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (packet_sender, packet_receiver) = flume::bounded(1);
        drop(packet_receiver);

        let (connector_sender, _connector_receiver) = mpsc::channel(1);
        connector_sender
            .try_send(ProcessorPacket::ProcessPacket(make_packet(remote_ip)))
            .expect("failed to fill connector lane");

        let handle = make_concurrent_handle(config, packet_sender, connector_sender);
        assert_eq!(
            handle.try_process_packet(make_packet(remote_ip)),
            SendOutcome::WouldBlock
        );
    }

    #[test]
    fn concurrent_try_process_packet_reports_closed_when_processor_lane_closed() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (packet_sender, packet_receiver) = flume::bounded(1);
        drop(packet_receiver);
        let (connector_sender, _connector_receiver) = mpsc::channel(1);

        let handle = make_concurrent_handle(config, packet_sender, connector_sender);
        assert_eq!(
            handle.try_process_packet(make_packet(remote_ip)),
            SendOutcome::Closed
        );
    }

    #[test]
    fn concurrent_try_submit_lossless_packet_reports_shared_queue_backpressure() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (packet_sender, _packet_receiver) = flume::bounded(1);
        let (connector_sender, connector_receiver) = mpsc::channel(1);
        drop(connector_receiver);

        packet_sender
            .try_send(ProcessorPacket::ProcessPacket(make_packet(remote_ip)))
            .expect("failed to fill shared concurrent ingress queue");

        let handle = make_concurrent_handle(config, packet_sender, connector_sender);
        let attempt = ProcessorHandle::Concurrent(handle)
            .try_submit_lossless_packet(make_fec_packet(remote_ip, 5));

        assert_eq!(
            attempt.contract,
            LosslessIngressContract::SharedQueueNonBlocking
        );
        assert_eq!(attempt.outcome, SendOutcome::WouldBlock);
    }

    #[test]
    fn processor_handle_try_process_packet_reports_queued() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (processor_sender, _processor_receiver) = mpsc::channel(1);
        let (connector_sender, _connector_receiver) = mpsc::channel(1);
        let inner = make_sequential_handle(config, processor_sender, connector_sender);
        let handle = ProcessorHandle::Sequential(inner);

        assert_eq!(
            handle.try_process_packet(make_packet(remote_ip)),
            SendOutcome::Queued
        );
    }

    #[test]
    fn processor_handle_try_submit_lossless_packet_reports_contract_and_outcome() {
        let config = base_config(OperatingMode::Normal);
        let remote_ip = Ipv4Addr::new(10, 0, 0, 2);

        let (processor_sender, _processor_receiver) = mpsc::channel(1);
        let (connector_sender, _connector_receiver) = mpsc::channel(1);
        let inner = make_sequential_handle(config, processor_sender, connector_sender);
        let handle = ProcessorHandle::Sequential(inner);

        let attempt = handle.try_submit_lossless_packet(make_fec_packet(remote_ip, 3));
        assert_eq!(
            attempt.contract,
            LosslessIngressContract::SharedQueueNonBlocking
        );
        assert_eq!(attempt.outcome, SendOutcome::Queued);
    }
}
