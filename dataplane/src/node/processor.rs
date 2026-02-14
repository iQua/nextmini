/// A processor actor is designed to forward packets from its upstream actors (LocalInterface
/// and NetworkInterface) to its downstream actors (LocalInterface and Scheduler). It launches
/// multiple processor tasks to handle incoming packets concurrently, allowing for efficient
/// processing and routing of network packets.
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Once};

use ahash::AHashMap;
use jumphash::JumpHasher;
use tokio;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::mpsc;
use tracing::{error, warn};

use nextmini_messages::{
    GroupDirectoryEntry, GroupId, GroupRoutingTableEntry, INVALID, OperatingMode,
    RoutingTableEntry, TokenBucketSpec, lossless_session,
};

use crate::node::config::{Feature, LocalConfig};
use crate::node::connector::Connector;
use crate::node::connector::ConnectorMessage;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::flow::UserSpaceSender;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::local::interface::LocalInterfaceHandle;
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
static CONCURRENT_FEC_INGRESS_POLICY_WARN_ONCE: Once = Once::new();

// Message types for the processor actor.
pub enum ProcessorPacket {
    ProcessPacket(Packet),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum SendOutcome {
    Queued,
    WouldBlock,
    Closed,
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
    AddNode(NodeId, SchedulerHandle),
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
        scheduler: SchedulerHandle,
    ) -> Result<(), SendError<ProcessorMessage>> {
        let _ = self
            .broadcast_sender()
            .send(ProcessorMessage::AddNode(node_id, scheduler))?;

        Ok(())
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn try_process_packet(&self, packet: Packet) -> SendOutcome {
        match self {
            ProcessorHandle::Sequential(handle) => handle.try_process_packet(packet),
            ProcessorHandle::Concurrent(handle) => handle.try_process_packet(packet),
        }
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
}

impl SequentialProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let mut packet_senders = Vec::with_capacity(config.num_packet_processors);

        for _ in 0..config.num_packet_processors {
            // creates an mpsc channel for each processor
            let (packet_sender, packet_receiver) = mpsc::channel(config.channel_capacity);
            packet_senders.push(packet_sender);

            let mut proc = Processor::new(
                PacketReceiver::Sequential(packet_receiver),
                broadcast_sender.subscribe(),
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
        }
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

    #[cfg_attr(not(test), allow(dead_code))]
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

    fn select_processor_ingress_lane(&self, packet: &Packet) -> usize {
        let lane_count = self.packet_senders.len();
        if let Some(tree_id) = packet.lossless_fec_tree_id() {
            let hasher = JumpHasher::new_with_keys(FLOW_TREE_HASH_KEY_0, FLOW_TREE_HASH_KEY_1);
            hasher.slot(&(packet.flow_id, tree_id), lane_count as u32) as usize
        } else {
            packet.flow_id.hash(lane_count)
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

    #[cfg_attr(not(test), allow(dead_code))]
    fn try_send_to_processor(&self, packet: Packet) -> SendOutcome {
        let idx = self.select_processor_ingress_lane(&packet);
        let sender = &self.packet_senders[idx];
        map_tokio_try_send_outcome(sender.try_send(ProcessorPacket::ProcessPacket(packet)))
    }

    #[cfg_attr(not(test), allow(dead_code))]
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
}

impl ConcurrentProcHandle {
    pub fn new(config: LocalConfig) -> Self {
        let (broadcast_sender, _) = broadcast::channel(config.channel_capacity);
        let (packet_sender, packet_receiver) = flume::bounded(config.channel_capacity);

        for _ in 0..config.num_packet_processors {
            let mut proc = Processor::new(
                PacketReceiver::Concurrent(packet_receiver.clone()),
                broadcast_sender.subscribe(),
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
        }
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

    #[cfg_attr(not(test), allow(dead_code))]
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

    fn maybe_warn_collaborative_multitree_policy(&self, packet: &Packet) {
        if packet.lossless_fec_tree_id().is_some() {
            CONCURRENT_FEC_INGRESS_POLICY_WARN_ONCE.call_once(|| {
                warn!(
                    "ConcurrentProcHandle: FEC ingress uses a shared queue across all trees; collaborative multi-tree mode is supported only with sequential ingress."
                );
            });
        }
    }

    async fn send_to_processor(&self, packet: Packet) {
        self.maybe_warn_collaborative_multitree_policy(&packet);
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

    #[cfg_attr(not(test), allow(dead_code))]
    fn try_send_to_processor(&self, packet: Packet) -> SendOutcome {
        self.maybe_warn_collaborative_multitree_policy(&packet);
        map_flume_try_send_outcome(
            self.packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet)),
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn try_send_to_connector(&self, packet: Packet) -> SendOutcome {
        map_tokio_try_send_outcome(
            self.connector_packet_sender
                .try_send(ProcessorPacket::ProcessPacket(packet)),
        )
    }

    fn send_to_processor_blocking(&self, packet: Packet) {
        self.maybe_warn_collaborative_multitree_policy(&packet);
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

#[cfg_attr(not(test), allow(dead_code))]
fn map_tokio_try_send_outcome<T>(result: Result<(), mpsc::error::TrySendError<T>>) -> SendOutcome {
    match result {
        Ok(()) => SendOutcome::Queued,
        Err(mpsc::error::TrySendError::Full(_)) => SendOutcome::WouldBlock,
        Err(mpsc::error::TrySendError::Closed(_)) => SendOutcome::Closed,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
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

#[derive(Debug)]
pub enum PacketTryRecvError {
    FlumeRecvError(flume::TryRecvError),
    MpscRecvError(mpsc::error::TryRecvError),
}

impl Display for PacketTryRecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketTryRecvError::FlumeRecvError(err) => {
                write!(f, "Error receiving from a flume mpmc channel: {}", err)
            }
            PacketTryRecvError::MpscRecvError(err) => {
                write!(f, "Error receiving from an MPSC channel: {}", err)
            }
        }
    }
}

impl PacketReceiver {
    pub async fn recv(&mut self) -> Option<ProcessorPacket> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver.recv().await,
            PacketReceiver::Concurrent(receiver) => receiver.recv_async().await.ok(),
        }
    }

    pub fn try_recv(&mut self) -> Result<ProcessorPacket, PacketTryRecvError> {
        match self {
            PacketReceiver::Sequential(receiver) => receiver
                .try_recv()
                .map_err(PacketTryRecvError::MpscRecvError),
            PacketReceiver::Concurrent(receiver) => receiver
                .try_recv()
                .map_err(PacketTryRecvError::FlumeRecvError),
        }
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

    // a unified hashmap for schedulers in normal mode
    schedulers: AHashMap<NodeId, SchedulerHandle>,

    // optional in-process Python delivery path
    #[cfg(feature = "python-extension")]
    python_interface: Option<PythonInterfaceHandle>,
    lossless_handle: Option<LosslessRuntimeHandle>,
}

impl Processor {
    pub fn new(
        packet_receiver: PacketReceiver,
        broadcast_receiver: broadcast::Receiver<ProcessorMessage>,
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
            config,
            #[cfg(feature = "python-extension")]
            python_interface: None,
            lossless_handle: None,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // waits for the first packet or a broadcast message
                Some(msg) = self.packet_receiver.recv() => {
                    match msg {
                        ProcessorPacket::ProcessPacket(first_packet) => {
                            // starts a batch with the first packet
                            self.process_packet(first_packet).await;

                            // starts processing packets in batches
                            while let Ok(ProcessorPacket::ProcessPacket(packet)) = self.packet_receiver.try_recv() {
                                self.process_packet(packet).await;
                            }
                        }
                    }
                }
                Ok(broadcast_msg) = self.broadcast_receiver.recv() => {
                    self.handle_message(broadcast_msg).await;
                }
            }
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
            ProcessorMessage::AddNode(node_id, scheduler) => {
                self.schedulers.insert(node_id, scheduler);
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
                if let Some(scheduler) = self.schedulers.get(&node_id) {
                    scheduler.limit_rate(spec);
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
        }
    }

    /// Processes inbound packets for outbound delivery.
    async fn process_packet(&mut self, packet: Packet) {
        let packet_flow_id = packet.flow_id;
        let fec_tree_id = packet.lossless_fec_tree_id();

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
            if self.try_deliver_lossless(&packet) {
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
        } else if let Some(scheduler) = self.schedulers.get(&next_hop_id) {
            scheduler.send(packet).await;
        }
    }

    fn try_deliver_lossless(&mut self, packet: &Packet) -> bool {
        let Some(handle) = self.lossless_handle.clone() else {
            return false;
        };
        let Some(payload) = packet.tcp_payload() else {
            return false;
        };
        let session_id = if let Some((hdr, _, _)) = lossless_session::decode_data(payload) {
            hdr.session_id
        } else if let Some((hdr, _)) = lossless_session::decode_control(payload) {
            hdr.session_id
        } else if let Some((hdr, _, _)) = lossless_session::decode_fec_data(payload) {
            hdr.session_id
        } else {
            return false;
        };

        let src_node = self.config.ip_to_node_id(packet.flow_id.src_ip());
        let peer_id = if src_node == INVALID {
            None
        } else {
            Some(src_node)
        };

        let payload_vec = payload.to_vec();

        handle.deliver(
            session_id,
            LosslessInboundFrame {
                bytes: payload_vec,
                peer_id,
            },
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

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
        let payload = lossless_session::encode_fec_data(17, 3, 9, tree_id, b"x");
        Packet::build_ipv4_tcp_packet(Ipv4Addr::new(10, 0, 0, 9), 4000, dst_ip, 5000, &payload)
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
        }
    }

    fn make_sequential_handle(
        config: LocalConfig,
        packet_sender: mpsc::Sender<ProcessorPacket>,
        connector_packet_sender: mpsc::Sender<ProcessorPacket>,
    ) -> SequentialProcHandle {
        make_sequential_handle_with_lanes(config, vec![packet_sender], connector_packet_sender)
    }

    fn find_distinct_fec_tree_lanes(
        handle: &SequentialProcHandle,
        dst_ip: Ipv4Addr,
    ) -> ((u16, usize), (u16, usize)) {
        let first_tree = 0u16;
        let first_lane = handle.select_processor_ingress_lane(&make_fec_packet(dst_ip, first_tree));
        for tree_id in 1u16..=255 {
            let lane = handle.select_processor_ingress_lane(&make_fec_packet(dst_ip, tree_id));
            if lane != first_lane {
                return ((first_tree, first_lane), (tree_id, lane));
            }
        }
        panic!("expected at least two distinct ingress lanes for FEC tree IDs");
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

        let ((blocked_tree, blocked_lane), (writable_tree, _writable_lane)) =
            find_distinct_fec_tree_lanes(&handle, remote_ip);
        handle.packet_senders[blocked_lane]
            .try_send(ProcessorPacket::ProcessPacket(make_packet(remote_ip)))
            .expect("failed to fill selected tree lane");

        assert_eq!(
            handle.try_process_packet(make_fec_packet(remote_ip, blocked_tree)),
            SendOutcome::WouldBlock
        );
        assert_eq!(
            handle.try_process_packet(make_fec_packet(remote_ip, writable_tree)),
            SendOutcome::Queued
        );
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
}
