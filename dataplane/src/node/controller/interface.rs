use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
#[cfg(feature = "python-extension")]
use std::sync::Arc;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
#[cfg(feature = "python-extension")]
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, interval, timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};
use tracing::{error, info, warn};

use nextmini_messages::{
    ControllerToDataplane, DataplaneToController, Flow, FlowTransport, GroupId,
    GroupRoutingTableEntry, MULTITREE_STRIDE,
};

use crate::node::config::LocalConfig;
use crate::node::controller::flowstats::FlowStatsReporterHandle;
use crate::node::controller::lossless_unicast::LosslessUnicastFlowManager;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::client::UserSpaceClientHandle;
use crate::node::flow::server::UserSpaceServerHandle;
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::network::scope::TransportScope;
use crate::node::network::tcp_max::TcpMaxClient;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
#[cfg(feature = "python-extension")]
use crate::node::python::interface::{PythonEvent, PythonInterfaceHandle};
use crate::node::scheduler::sched::SchedulerHandle;
use crate::node::session::api::LosslessRuntimeHandle;

const LOSSLESS_GROUP_ROUTE_QUIET_PERIOD: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    pub config: LocalConfig,
    pub processors: ProcessorHandle,
    northbridge_sender: mpsc::UnboundedSender<DataplaneToController>,
    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,
}

/// The handle for the controller interface, which allows sending messages to the controller.
impl ControllerInterfaceHandle {
    pub async fn new(
        config: LocalConfig,
    ) -> (
        Self,
        LosslessRuntimeHandle,
        ControllerReporterHandle,
        FlowStatsReporterHandle,
    ) {
        // creates an unbounded channel, the 'northbridge', for sending messages to the controller
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();

        // connects to the controller over WebSockets
        let (config, processors, ws_stream) = ControllerInterfaceHandle::connect(config).await;

        let (sender_stream, receiver_stream) = ws_stream.split();

        // initializes the controller sender and receiver
        let mut controller_sender = DataplaneToControllerSender {
            sender_stream,
            northbridge_receiver,
        };

        #[cfg(feature = "python-extension")]
        let python_interface = Arc::new(Mutex::new(None));

        let controller_interface = Self {
            config: config.clone(),
            processors: processors.clone(),
            northbridge_sender,
            #[cfg(feature = "python-extension")]
            python_interface: python_interface.clone(),
        };

        let reporter = ControllerReporterHandle::new(
            controller_interface.clone(),
            config.metrics_collection_interval,
        );

        let flowstats_reporter =
            FlowStatsReporterHandle::new(controller_interface.clone(), config.clone());

        // passes flowstats reporter to routing table for automatic route assignment reporting
        processors
            .set_flowstats_reporter(flowstats_reporter.clone())
            .await;

        // adds flowstats reporter to report flow finish
        let user_space_client = UserSpaceClientHandle::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
        );

        // creates the server handle for the processor to use
        let user_space_server = UserSpaceServerHandle::new(config.clone(), processors.clone());
        processors.connect_server(user_space_server.clone());

        // creates a TCP max client for the processor to use
        let tcp_max_client =
            TcpMaxClient::new(config.clone(), processors.clone(), reporter.clone());
        processors.connect_tcp_max_client(tcp_max_client).await;

        // creates the lossless runtime handle with the correct processors
        let lossless_runtime =
            LosslessRuntimeHandle::new(processors.clone(), config.lossless_runtime_config.clone());
        processors.connect_lossless_handle(lossless_runtime.clone());

        // creates the lossless unicast flow manager with the correct processors
        let lossless_unicast = LosslessUnicastFlowManager::new(
            config.clone(),
            processors.clone(),
            flowstats_reporter.clone(),
            lossless_runtime.clone(),
        );

        let (local_event_sender, local_event_receiver) = mpsc::unbounded_channel();

        let mut controller_receiver = ControllerToDataplaneReceiver {
            controller: controller_interface.clone(),
            config: config.clone(),
            receiver_stream,
            processors: processors.clone(),
            reporter: reporter.clone(),
            user_space_client,
            user_space_server,
            #[cfg(feature = "python-extension")]
            python_interface,
            lossless_runtime: lossless_runtime.clone(),
            group_ip_by_id: HashMap::new(),
            lossless_unicast,
            topology_ready: false,
            lossless_topology_ready: false,
            pending_tcp_flows: Vec::new(),
            pending_lossless_flows: Vec::new(),
            expected_neighbor_count: 0,
            connected_neighbor_count: 0,
            neighbor_addrs: HashMap::new(),
            connected_scopes: HashSet::new(),
            routes_installed: false,
            group_directory_installed: false,
            installed_group_route_ids: HashSet::new(),
            local_topology_ready_sent: false,
            local_event_sender,
            local_event_receiver,
            last_group_route_update_at: None,
            latest_group_route_nonce: 0,
        };

        tokio::spawn(async move {
            controller_sender.run().await;
        });
        tokio::spawn(async move {
            controller_receiver.run().await;
        });

        (
            controller_interface,
            lossless_runtime,
            reporter,
            flowstats_reporter,
        )
    }

    pub async fn connect(
        mut config: LocalConfig,
    ) -> (
        LocalConfig,
        ProcessorHandle,
        WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) {
        let url = url::Url::parse(&config.controller_addr).unwrap();
        let mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>;
        let connect_timeout = Duration::from_millis(config.controller_connect_timeout_ms.max(1));

        loop {
            let connect_fut = connect_async(url.as_str());

            match timeout(connect_timeout, connect_fut).await {
                Ok(Ok((ws, _))) => {
                    ws_stream = ws;
                    info!("WebSocket handshake has been successfully completed.");
                    break;
                }
                Ok(Err(e)) => {
                    // Connection attempt failed quickly (e.g., refused, handshake error)
                    warn!("Failed to connect to the controller: {}. Retrying...", e);
                }
                Err(_) => {
                    // Timed out
                    warn!(
                        "Timed out attempting to connect to the controller after {:?}. Retrying...",
                        connect_timeout
                    );
                }
            }

            // Linear backoff with small jitter (0 - 500ms)
            let jitter_ms = rand::random_range(0..500);
            let backoff = Duration::from_secs(2) + Duration::from_millis(jitter_ms);

            tokio::time::sleep(backoff).await;
        }

        let startup_msg = DataplaneToController::StartUp {
            private_network_name: config.private_network_name.clone(),
            private_network_addr: config.private_network_addr.clone()
                + ":"
                + &config.private_network_port.clone(),
            public_network_addr: config.public_network_addr.clone()
                + ":"
                + &config.public_network_port.clone(),
            node_id: config.node_id.to_string().parse().ok(),
        };

        ws_stream
            .send(Message::binary(rmp_serde::to_vec(&startup_msg).unwrap()))
            .await
            .expect("Failed to send the startup message to the controller");

        // waits for the controller's response
        if let Some(response) = ws_stream.next().await {
            // updates the local configuration with settings from the controller
            config.update(response);
        } else {
            error!("No response has been received from controller.");
        }

        // starts the processor actor
        let processors = ProcessorHandle::new(config.clone());

        (config, processors, ws_stream)
    }

    /// Sends a message to the controller.
    pub async fn send(&self, msg: DataplaneToController) {
        if let Err(e) = self.northbridge_sender.send(msg) {
            error!(
                "Error sending messages to the controller interface actor: {}",
                e
            );
        };
    }

    #[cfg(feature = "python-extension")]
    pub async fn attach_python_interface(&self, interface: PythonInterfaceHandle) {
        let mut guard = self.python_interface.lock().await;
        *guard = Some(interface);
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use bytes::Bytes;
    use futures::StreamExt;
    use nextmini_messages::lossless_session::{self, LosslessSessionControl};
    use nextmini_messages::{GroupDirectoryEntry, MULTITREE_STRIDE};
    use tokio::net::TcpListener;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::accept_async;

    use super::*;
    use crate::node::network::scope::TransportScope;
    use crate::node::packet::{LosslessTransportMeta, Packet};
    use crate::node::scheduler::sched::{SchedulerHandle, SchedulerReaderMessage};
    use crate::node::session::runtime::{SenderRequest, SessionConfig, TransportRoute};

    async fn test_receiver(
        config: LocalConfig,
        processors: ProcessorHandle,
        lossless_runtime: LosslessRuntimeHandle,
    ) -> (ControllerToDataplaneReceiver, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test websocket listener");
        let addr = listener.local_addr().expect("test listener address");
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept test websocket");
            let _websocket = accept_async(stream)
                .await
                .expect("accept websocket upgrade");
            pending::<()>().await;
        });
        let (websocket, _) = connect_async(format!("ws://{addr}"))
            .await
            .expect("connect test websocket");
        let (_, receiver_stream) = websocket.split();

        let (northbridge_sender, _northbridge_receiver) = mpsc::unbounded_channel();
        let controller = ControllerInterfaceHandle {
            config: config.clone(),
            processors: processors.clone(),
            northbridge_sender,
            #[cfg(feature = "python-extension")]
            python_interface: Arc::new(Mutex::new(None)),
        };
        let reporter = ControllerReporterHandle::new(controller.clone(), 3600);
        let flowstats = FlowStatsReporterHandle::new(controller.clone(), config.clone());
        let user_space_client =
            UserSpaceClientHandle::new(config.clone(), processors.clone(), flowstats.clone());
        let user_space_server = UserSpaceServerHandle::new(config.clone(), processors.clone());
        processors.connect_server(user_space_server.clone());
        processors.connect_lossless_handle(lossless_runtime.clone());
        let lossless_unicast = LosslessUnicastFlowManager::new(
            config.clone(),
            processors.clone(),
            flowstats,
            lossless_runtime.clone(),
        );
        let (local_event_sender, local_event_receiver) = mpsc::unbounded_channel();

        (
            ControllerToDataplaneReceiver {
                controller,
                config,
                receiver_stream,
                processors,
                reporter,
                user_space_client,
                user_space_server,
                #[cfg(feature = "python-extension")]
                python_interface: Arc::new(Mutex::new(None)),
                group_ip_by_id: HashMap::new(),
                lossless_runtime,
                lossless_unicast,
                topology_ready: false,
                lossless_topology_ready: false,
                pending_tcp_flows: Vec::new(),
                pending_lossless_flows: Vec::new(),
                expected_neighbor_count: 0,
                connected_neighbor_count: 0,
                neighbor_addrs: HashMap::new(),
                connected_scopes: HashSet::new(),
                routes_installed: false,
                group_directory_installed: false,
                installed_group_route_ids: HashSet::new(),
                local_topology_ready_sent: false,
                local_event_sender,
                local_event_receiver,
                last_group_route_update_at: None,
                latest_group_route_nonce: 0,
            },
            server_task,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn startup_holds_sender_until_group_route_workers_ack() {
        const SOURCE_NODE_ID: usize = 1;
        const RECEIVER_NODE_ID: usize = 2;
        const GROUP_ID: usize = 1;
        const SESSION_ID: u64 = 0xA11C_E401;

        let group_ip = Ipv4Addr::new(239, 1, 1, 1);
        let mut config = LocalConfig {
            node_id: SOURCE_NODE_ID,
            num_packet_processors: 3,
            channel_capacity: 8,
            queue_capacity: 8,
            fanout_pending_capacity: Some(1),
            channel_backpressure: true,
            ..Default::default()
        };
        config.lossless_runtime_config.fec_enabled = false;
        config.lossless_runtime_config.ready_grace_ms = 1_000;

        let processors = ProcessorHandle::new(config.clone());
        let lossless_runtime =
            LosslessRuntimeHandle::new(processors.clone(), config.lossless_runtime_config.clone());
        let (mut receiver, server_task) =
            test_receiver(config.clone(), processors.clone(), lossless_runtime.clone()).await;

        let (default_scheduler_sender, mut default_scheduler_receiver) = mpsc::channel(8);
        processors
            .add_node(
                RECEIVER_NODE_ID,
                TransportScope::Default,
                SchedulerHandle::new_for_test(default_scheduler_sender, true),
            )
            .expect("install default test scheduler");
        let (tree_scheduler_sender, mut tree_scheduler_receiver) = mpsc::channel(1);
        processors
            .add_node(
                RECEIVER_NODE_ID,
                TransportScope::Tree(0),
                SchedulerHandle::new_for_test(tree_scheduler_sender, true),
            )
            .expect("install tree test scheduler");
        processors.sync_workers().await;

        let directory = vec![GroupDirectoryEntry {
            group_id: GROUP_ID,
            group_ip,
        }];
        let routes = vec![GroupRoutingTableEntry {
            route_id: GROUP_ID * MULTITREE_STRIDE,
            next_hops: vec![RECEIVER_NODE_ID],
            src_node_id: SOURCE_NODE_ID,
            group_id: GROUP_ID,
        }];

        let base_route_nonce = processors.sync_snapshot_for_test().0;
        receiver
            .process_control_msg(ControllerToDataplane::InstallRoutes { routes: Vec::new() })
            .await;
        let (installed_nonce, installed_ack_count, worker_count) =
            processors.sync_snapshot_for_test();
        assert!(
            installed_nonce > base_route_nonce,
            "base topology readiness must include a worker route-ACK barrier"
        );
        assert_eq!(
            installed_ack_count, worker_count,
            "base routes were marked installed before every worker acknowledged them"
        );
        receiver
            .process_control_msg(ControllerToDataplane::InstallGroupDirectory {
                groups: Vec::new(),
            })
            .await;
        receiver
            .process_control_msg(ControllerToDataplane::TopologyReady)
            .await;
        receiver
            .process_control_msg(ControllerToDataplane::InstallGroupDirectory { groups: directory })
            .await;
        receiver
            .process_control_msg(ControllerToDataplane::InstallGroupRoutes {
                group_id: GROUP_ID,
                src_node_id: SOURCE_NODE_ID,
                routes: routes.clone(),
            })
            .await;

        sleep(LOSSLESS_GROUP_ROUTE_QUIET_PERIOD + Duration::from_millis(20)).await;
        let initial_ready_event = receiver
            .local_event_receiver
            .recv()
            .await
            .expect("initial route quiet-period event");
        receiver.handle_local_event(initial_ready_event).await;
        assert!(receiver.lossless_topology_ready);

        let parked_packet = Packet::build_ipv4_tcp_packet_with_lossless_meta(
            config.user_space_address,
            45000,
            group_ip,
            46000,
            Some(LosslessTransportMeta {
                session_id: SESSION_ID,
                tree_id: Some(0),
            }),
            b"park",
        );
        for _ in 0..4 {
            processors.process_packet(parked_packet.clone()).await;
        }

        let baseline_nonce = processors.sync_snapshot_for_test().0;
        let update_task = tokio::spawn(async move {
            receiver
                .process_control_msg(ControllerToDataplane::InstallGroupRoutes {
                    group_id: GROUP_ID,
                    src_node_id: SOURCE_NODE_ID,
                    routes,
                })
                .await;
            receiver
        });

        timeout(Duration::from_secs(2), async {
            loop {
                let (nonce, ack_count, worker_count) = processors.sync_snapshot_for_test();
                if nonce > baseline_nonce && ack_count + 1 == worker_count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("route update should reach a real N-1/N worker barrier");

        let mut session = lossless_runtime
            .start_sender(SenderRequest {
                session: SessionConfig {
                    session_id: SESSION_ID,
                    block_size: 16,
                },
                route: TransportRoute {
                    src_ip: config.user_space_address,
                    dst_ip: group_ip,
                    src_port: 45000,
                    dst_port: 46000,
                },
                pacing: None,
                receiver_ids: vec![RECEIVER_NODE_ID],
                total_bytes: 16,
                source_buffer: Bytes::from_static(b"abcdefghijklmnop"),
                ready_grace_ms: 1_000,
                peer_report_timeout_ms: 1_000,
            })
            .await
            .expect("start sender behind route barrier");

        assert!(
            timeout(
                Duration::from_millis(100),
                default_scheduler_receiver.recv()
            )
            .await
            .is_err(),
            "sender emitted before the last processor worker acknowledged the route barrier"
        );

        timeout(Duration::from_secs(1), tree_scheduler_receiver.recv())
            .await
            .expect("free the blocked tree scheduler")
            .expect("blocked tree scheduler should still be open");
        let mut receiver = timeout(Duration::from_secs(2), update_task)
            .await
            .expect("route update should finish after the parked worker resumes")
            .expect("route update task should not panic");
        let (_, ack_count, worker_count) = processors.sync_snapshot_for_test();
        assert_eq!(
            ack_count, worker_count,
            "route update returned before the last ACK"
        );

        for _ in 0..3 {
            timeout(Duration::from_secs(1), tree_scheduler_receiver.recv())
                .await
                .expect("drain parked tree packet")
                .expect("tree scheduler should remain open");
        }
        assert!(
            timeout(
                Duration::from_millis(100),
                default_scheduler_receiver.recv()
            )
            .await
            .is_err(),
            "sender emitted before the post-barrier quiet period completed"
        );

        let ready_event = timeout(
            LOSSLESS_GROUP_ROUTE_QUIET_PERIOD + Duration::from_secs(1),
            receiver.local_event_receiver.recv(),
        )
        .await
        .expect("route quiet-period event should fire")
        .expect("local event channel should remain open");
        receiver.handle_local_event(ready_event).await;

        let manifest = timeout(Duration::from_secs(1), default_scheduler_receiver.recv())
            .await
            .expect("sender should emit after the complete route barrier")
            .expect("default scheduler should remain open");
        let SchedulerReaderMessage::InboundPacket(manifest) = manifest;
        assert!(matches!(
            manifest
                .tcp_payload()
                .and_then(lossless_session::decode_control),
            Some((_, LosslessSessionControl::Manifest { .. }))
        ));

        session.abort();
        assert!(matches!(
            timeout(Duration::from_secs(1), session.wait()).await,
            Ok(crate::node::session::api::SessionOutcome::Aborted)
        ));
        server_task.abort();
    }
}

/// An actor used for sending messages from the dataplane to the controller over WebSockets.
pub struct DataplaneToControllerSender {
    northbridge_receiver: mpsc::UnboundedReceiver<DataplaneToController>,
    sender_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl DataplaneToControllerSender {
    pub async fn run(&mut self) {
        let mut ping_interval = interval(Duration::from_secs(30)); // Send ping every 30 seconds

        loop {
            tokio::select! {
                Some(msg) = self.northbridge_receiver.recv() => {
                    if let Err(e) = self.sender_stream
                        .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                        .await
                    {
                        error!("Failed to send message to controller: {}. Closing sender.", e);
                        break;
                    }
                }
                _ = ping_interval.tick() => {
                    if let Err(e) = self
                        .sender_stream
                        .send(Message::Ping(Vec::new().into()))
                        .await
                    {
                        warn!("Failed to send ping to controller: {}. Closing sender.", e);
                        break;
                    }
                }
            }
        }

        info!("DataplaneToController sender stopped.");
    }
}

/// An actor used for receiving messages from the controller and broadcasts them to the processors.
pub struct ControllerToDataplaneReceiver {
    controller: ControllerInterfaceHandle,
    config: LocalConfig,
    receiver_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    processors: ProcessorHandle,

    // reports metrics to controller
    reporter: ControllerReporterHandle,

    // handles for user-space TCP flows
    user_space_client: UserSpaceClientHandle,
    user_space_server: UserSpaceServerHandle,

    #[cfg(feature = "python-extension")]
    python_interface: Arc<Mutex<Option<PythonInterfaceHandle>>>,

    group_ip_by_id: HashMap<GroupId, Ipv4Addr>,
    lossless_runtime: LosslessRuntimeHandle,
    lossless_unicast: LosslessUnicastFlowManager,
    topology_ready: bool,
    lossless_topology_ready: bool,
    pending_tcp_flows: Vec<Flow>,
    pending_lossless_flows: Vec<Flow>,
    expected_neighbor_count: usize,
    connected_neighbor_count: usize,
    neighbor_addrs: HashMap<usize, String>,
    connected_scopes: HashSet<(usize, TransportScope)>,
    routes_installed: bool,
    group_directory_installed: bool,
    installed_group_route_ids: HashSet<GroupId>,
    local_topology_ready_sent: bool,
    local_event_sender: mpsc::UnboundedSender<ControllerLocalEvent>,
    local_event_receiver: mpsc::UnboundedReceiver<ControllerLocalEvent>,
    last_group_route_update_at: Option<Instant>,
    latest_group_route_nonce: u64,
}

enum ControllerLocalEvent {
    AttemptActivateLossless { nonce: u64 },
}

impl ControllerToDataplaneReceiver {
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                maybe_msg = self.receiver_stream.next() => {
                    let msg = match maybe_msg.unwrap() {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("Disconnected from the controller. Restarting the node...");
                            error!("{:?}", e);
                            break;
                        }
                    };

                    match msg {
                        Message::Binary(data) => {
                            let ctrl_msg: ControllerToDataplane =
                                rmp_serde::from_slice(&data).expect("Failed to parse control message");
                            let msg_kind = controller_msg_name(&ctrl_msg);
                            info!(
                                node_id = self.config.node_id,
                                msg_kind,
                                "Controller receiver handling control message"
                            );
                            self.process_control_msg(ctrl_msg).await;
                            info!(
                                node_id = self.config.node_id,
                                msg_kind,
                                "Controller receiver finished control message"
                            );
                        }
                        Message::Pong(_) => {
                            continue;
                        }
                        _ => {
                            error!("Received a message that is not a binary or a ping message.");
                        }
                    };
                }
                Some(event) = self.local_event_receiver.recv() => {
                    info!(
                        node_id = self.config.node_id,
                        "Controller receiver handling deferred local event"
                    );
                    self.handle_local_event(event).await;
                    info!(
                        node_id = self.config.node_id,
                        "Controller receiver finished deferred local event"
                    );
                }
            }
        }
    }

    async fn process_control_msg(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::AddNode {
                remote_node_id,
                remote_addr,
            } => {
                self.expected_neighbor_count += 1;
                self.neighbor_addrs
                    .insert(remote_node_id, remote_addr.clone());
                self.ensure_scope_connection(
                    remote_node_id,
                    remote_addr,
                    TransportScope::Default,
                    true,
                )
                .await;
            }

            ControllerToDataplane::AddNodeAddress {
                remote_node_id,
                remote_max_server_addr,
            } => {
                self.neighbor_addrs
                    .insert(remote_node_id, remote_max_server_addr.clone());
                self.processors
                    .add_node_address(remote_node_id, remote_max_server_addr)
                    .await;
            }

            ControllerToDataplane::SetLinkRate { node_id, spec } => {
                info!(
                    "The link rate from node {} to node {} is now set to {} bytes/second, with a bucket size of {} bytes.",
                    self.config.node_id, node_id, spec.rate, spec.bucket_size,
                );

                self.processors.limit_rate(node_id, spec);
            }

            ControllerToDataplane::InstallRoutes { routes } => {
                info!(
                    "Installing {} routes on node {}.",
                    routes.len(),
                    self.config.node_id
                );

                self.processors.update_routing_table(routes).await;
                self.processors.sync_workers().await;
                self.routes_installed = true;
                self.maybe_send_local_topology_ready().await;
            }

            ControllerToDataplane::AddFlows { flows } => {
                let mut tcp_flows = Vec::new();
                let mut lossless_flows = Vec::new();

                for flow in &flows {
                    match flow.flow_spec.transport {
                        FlowTransport::Tcp => {
                            if flow.dst_node_id == self.config.node_id {
                                // this node is the server for this flow
                                self.user_space_server.store_flow_spec(flow.clone());
                            }
                            if flow.src_node_id == self.config.node_id {
                                // this node is the client for this flow
                                tcp_flows.push(flow.clone());
                            }
                        }
                        FlowTransport::LosslessUnicast => {
                            if flow.src_node_id == self.config.node_id
                                || flow.dst_node_id == self.config.node_id
                            {
                                lossless_flows.push(flow.clone());
                            }
                        }
                    }
                }

                if !tcp_flows.is_empty() {
                    if self.topology_ready {
                        self.start_tcp_flows(tcp_flows);
                    } else {
                        info!(
                            "Deferring {} user-space TCP flows on node {} until the topology is ready.",
                            tcp_flows.len(),
                            self.config.node_id
                        );
                        self.pending_tcp_flows.extend(tcp_flows);
                    }
                }

                if !lossless_flows.is_empty() {
                    if self.lossless_topology_ready {
                        info!(
                            "Adding {} lossless unicast flows to node {}.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.lossless_unicast.add_flows(lossless_flows);
                    } else {
                        info!(
                            "Deferring {} lossless unicast flows on node {} until the topology is ready.",
                            lossless_flows.len(),
                            self.config.node_id
                        );
                        self.pending_lossless_flows.extend(lossless_flows);
                    }
                }
            }

            ControllerToDataplane::TopologyReady => {
                info!(
                    "Controller signaled that all nodes are connected; topology state is ready on node {}.",
                    self.config.node_id
                );

                self.topology_ready = true;
                info!(
                    node_id = self.config.node_id,
                    group_directory_installed = self.group_directory_installed,
                    installed_group_routes = self.installed_group_route_ids.len(),
                    expected_group_routes = self.group_ip_by_id.len(),
                    "Controller topology-ready arrived; evaluating lossless topology gate"
                );

                // Emit Python event so Python code can wait for topology ready
                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if.publish_event(PythonEvent::TopologyReady).await;
                }

                self.flush_pending_tcp_flows();
                self.maybe_activate_lossless_topology().await;
            }

            ControllerToDataplane::GroupCreated {
                group_id,
                group_ip,
                src_node_id,
            } => {
                info!(
                    "Registered multicast group {} ({}) owned by node {}.",
                    group_id, group_ip, src_node_id
                );

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupCreated {
                            group_id,
                            src_node_id,
                            group_ip,
                        })
                        .await;
                }
            }

            ControllerToDataplane::InstallGroupDirectory { groups } => {
                info!(
                    "Installing multicast group directory ({} entries) on node {}.",
                    groups.len(),
                    self.config.node_id
                );
                if !groups.is_empty() {
                    self.deactivate_lossless_topology().await;
                }
                self.processors.update_group_directory(groups.clone()).await;
                self.group_ip_by_id.clear();
                self.installed_group_route_ids.clear();
                for entry in &groups {
                    self.group_ip_by_id.insert(entry.group_id, entry.group_ip);
                }

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupDirectoryUpdated { entries: groups })
                        .await;
                }

                self.group_directory_installed = true;
                self.maybe_activate_lossless_topology().await;
                self.maybe_send_local_topology_ready().await;
            }

            ControllerToDataplane::InstallGroupRoutes {
                group_id,
                src_node_id,
                routes,
            } => {
                self.deactivate_lossless_topology().await;
                info!(
                    "Installing multicast routes for group {} from src {} on node {} ({} entries).",
                    group_id,
                    src_node_id,
                    self.config.node_id,
                    routes.len()
                );

                #[cfg(feature = "python-extension")]
                let cloned_routes = routes.clone();

                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    route_count = routes.len(),
                    "Preparing scoped transports before multicast route install"
                );
                self.ensure_tree_scope_connections(&routes).await;
                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    "Finished preparing scoped transports before multicast route install"
                );
                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    "Waiting for processor workers to sync before multicast route update"
                );
                self.processors.sync_workers().await;
                info!(
                    node_id = self.config.node_id,
                    group_id, src_node_id, "Processor workers synced before multicast route update"
                );

                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    "Broadcasting multicast route update to processor workers"
                );
                self.processors
                    .update_group_routes(group_id, src_node_id, routes)
                    .await;
                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    "Broadcasted multicast route update to processor workers"
                );
                info!(
                    node_id = self.config.node_id,
                    group_id,
                    src_node_id,
                    "Waiting for processor workers to sync after multicast route update"
                );
                self.processors.sync_workers().await;
                info!(
                    node_id = self.config.node_id,
                    group_id, src_node_id, "Processor workers synced after multicast route update"
                );
                self.installed_group_route_ids.insert(group_id);
                self.last_group_route_update_at = Some(Instant::now());
                self.latest_group_route_nonce += 1;
                self.schedule_lossless_activation_attempt(self.latest_group_route_nonce);
                self.maybe_activate_lossless_topology().await;
                self.maybe_send_local_topology_ready().await;

                #[cfg(feature = "python-extension")]
                if let Some(py_if) = self.python_handle().await {
                    py_if
                        .publish_event(PythonEvent::GroupRoutesInstalled {
                            group_id,
                            src_node_id,
                            routes: cloned_routes.clone(),
                        })
                        .await;

                    let node_id = self.config.node_id;
                    for entry in &cloned_routes {
                        if entry.next_hops.contains(&node_id) {
                            py_if
                                .publish_event(PythonEvent::LocalMemberJoined { group_id, node_id })
                                .await;
                            break;
                        }
                    }
                }
            }

            ControllerToDataplane::ProbeLink {
                remote_node_id,
                probe_id,
                probe_bytes,
            } => {
                self.send_probe(remote_node_id, probe_id, probe_bytes);
            }

            _ => error!("Received a message with an unknown type from the controller."),
        }
    }

    async fn handle_local_event(&mut self, event: ControllerLocalEvent) {
        match event {
            ControllerLocalEvent::AttemptActivateLossless { nonce } => {
                info!(
                    node_id = self.config.node_id,
                    nonce,
                    latest_group_route_nonce = self.latest_group_route_nonce,
                    "Received deferred lossless topology activation event"
                );
                if nonce == self.latest_group_route_nonce {
                    info!(
                        node_id = self.config.node_id,
                        nonce, "Re-checking lossless topology activation after route quiet period"
                    );
                    self.maybe_activate_lossless_topology().await;
                } else {
                    info!(
                        node_id = self.config.node_id,
                        nonce,
                        latest_group_route_nonce = self.latest_group_route_nonce,
                        "Ignoring stale deferred lossless topology activation event"
                    );
                }
            }
        }
    }

    fn schedule_lossless_activation_attempt(&self, nonce: u64) {
        info!(
            node_id = self.config.node_id,
            nonce,
            quiet_period_ms = LOSSLESS_GROUP_ROUTE_QUIET_PERIOD.as_millis(),
            "Scheduling deferred lossless topology activation check"
        );
        let sender = self.local_event_sender.clone();
        let node_id = self.config.node_id;
        tokio::spawn(async move {
            tokio::time::sleep(LOSSLESS_GROUP_ROUTE_QUIET_PERIOD).await;
            info!(
                node_id,
                nonce, "Deferred lossless topology activation timer fired"
            );
            if sender
                .send(ControllerLocalEvent::AttemptActivateLossless { nonce })
                .is_err()
            {
                warn!(
                    node_id,
                    nonce, "Failed to enqueue deferred lossless topology activation event"
                );
            } else {
                info!(
                    node_id,
                    nonce, "Enqueued deferred lossless topology activation event"
                );
            }
        });
    }

    async fn record_neighbor_connected(&mut self, remote_node_id: usize) {
        self.connected_neighbor_count += 1;
        info!(
            "Dataplane node {} connected to neighbor {} ({}/{} ready).",
            self.config.node_id,
            remote_node_id,
            self.connected_neighbor_count,
            self.expected_neighbor_count
        );
        self.maybe_send_local_topology_ready().await;
    }

    async fn ensure_scope_connection(
        &mut self,
        remote_node_id: usize,
        remote_addr: String,
        scope: TransportScope,
        count_toward_topology: bool,
    ) {
        if !self.connected_scopes.insert((remote_node_id, scope)) {
            return;
        }

        let network_interface = NetworkInterfaceHandle::new_as_client(
            self.config.clone(),
            remote_node_id,
            remote_addr,
            scope,
            self.processors.clone(),
            self.reporter.clone(),
        )
        .await;

        let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);
        let _ = self.processors.add_node(remote_node_id, scope, scheduler);

        if count_toward_topology {
            self.record_neighbor_connected(remote_node_id).await;
        }
    }

    async fn ensure_tree_scope_connections(&mut self, routes: &[GroupRoutingTableEntry]) {
        let mut pending = Vec::new();
        let mut seen = HashSet::new();

        for route in routes {
            let tree_id = (route.route_id % MULTITREE_STRIDE) as u16;
            let scope = TransportScope::Tree(tree_id);
            for &next_hop_id in &route.next_hops {
                if next_hop_id == self.config.node_id {
                    continue;
                }
                if seen.insert((next_hop_id, scope)) {
                    if let Some(remote_addr) = self.neighbor_addrs.get(&next_hop_id).cloned() {
                        pending.push((next_hop_id, remote_addr, scope));
                    } else {
                        warn!(
                            next_hop_id,
                            ?scope,
                            "Missing remote address while preparing scoped transport"
                        );
                    }
                }
            }
        }

        for (next_hop_id, remote_addr, scope) in pending {
            self.ensure_scope_connection(next_hop_id, remote_addr, scope, false)
                .await;
        }
    }

    /// Probe payload:  [flags:1][probe_id:8][sender_node_id:8][padding]
    /// 1360 bytes of TCP payload + 20 IP + 20 TCP header = 1400-byte virtual packet.
    const PROBE_PAYLOAD_SIZE: usize = 1360;

    fn send_probe(&self, remote_node_id: usize, probe_id: u64, probe_bytes: usize) {
        let num_packets = (probe_bytes / Self::PROBE_PAYLOAD_SIZE).max(1);
        let sender_id = self.config.node_id as u64;
        let mut packets = Vec::with_capacity(num_packets);

        for i in 0..num_packets {
            let is_last = i == num_packets - 1;
            let mut payload = vec![0u8; Self::PROBE_PAYLOAD_SIZE];
            payload[0] = u8::from(is_last); // 0x00 data, 0x01 last
            payload[1..9].copy_from_slice(&probe_id.to_be_bytes());
            payload[9..17].copy_from_slice(&sender_id.to_be_bytes());

            packets.push(Packet::build_ipv4_tcp_packet(
                std::net::Ipv4Addr::new(127, 0, 0, 1),
                0,
                std::net::Ipv4Addr::new(127, 0, 0, 2),
                0,
                &payload,
            ));
        }

        info!(
            "Probe {}: sending {} packets (~{} bytes) to node {}.",
            probe_id,
            num_packets,
            num_packets * Self::PROBE_PAYLOAD_SIZE,
            remote_node_id,
        );
        self.processors
            .send_link_probe_packets(remote_node_id, packets);
    }

    async fn maybe_send_local_topology_ready(&mut self) {
        if self.local_topology_ready_sent {
            return;
        }

        if self.connected_neighbor_count < self.expected_neighbor_count {
            return;
        }

        if !self.routes_installed || !self.group_directory_installed {
            return;
        }

        self.local_topology_ready_sent = true;
        info!(
            "Local topology ready on node {}; notifying controller.",
            self.config.node_id
        );

        self.controller
            .send(DataplaneToController::NodeTopologyReady {
                node_id: self.config.node_id,
            })
            .await;
    }

    fn start_tcp_flows(&mut self, flows: Vec<Flow>) {
        if flows.is_empty() {
            return;
        }

        info!(
            "Adding {} user-space TCP flows to node {}.",
            flows.len(),
            self.config.node_id
        );
        self.user_space_client.add_flows(flows);
    }

    fn flush_pending_tcp_flows(&mut self) {
        if !self.pending_tcp_flows.is_empty() {
            let pending = std::mem::take(&mut self.pending_tcp_flows);

            info!(
                "Topology ready on node {}; starting {} deferred user-space flows.",
                self.config.node_id,
                pending.len()
            );

            self.start_tcp_flows(pending);
        }
    }

    fn flush_pending_lossless_flows(&mut self) {
        if !self.pending_lossless_flows.is_empty() {
            let pending = std::mem::take(&mut self.pending_lossless_flows);

            info!(
                "Lossless topology ready on node {}; starting {} deferred lossless unicast flows.",
                self.config.node_id,
                pending.len()
            );

            self.lossless_unicast.add_flows(pending);
        }
    }

    async fn maybe_activate_lossless_topology(&mut self) {
        let elapsed_since_last_route_ms = self
            .last_group_route_update_at
            .map(|instant| instant.elapsed().as_millis() as u64);
        info!(
            node_id = self.config.node_id,
            controller_topology_ready = self.topology_ready,
            lossless_topology_ready = self.lossless_topology_ready,
            group_directory_installed = self.group_directory_installed,
            installed_group_routes = self.installed_group_route_ids.len(),
            expected_group_routes = self.group_ip_by_id.len(),
            last_group_route_elapsed_ms = elapsed_since_last_route_ms,
            "Evaluating lossless topology activation gate"
        );

        if self.lossless_topology_ready {
            return;
        }

        if !self.topology_ready {
            info!(
                node_id = self.config.node_id,
                "Lossless topology activation is waiting for controller topology-ready"
            );
            return;
        }

        if let Some(last_update_at) = self.last_group_route_update_at
            && last_update_at.elapsed() < LOSSLESS_GROUP_ROUTE_QUIET_PERIOD
        {
            info!(
                node_id = self.config.node_id,
                remaining_quiet_ms = (LOSSLESS_GROUP_ROUTE_QUIET_PERIOD
                    .saturating_sub(last_update_at.elapsed()))
                .as_millis(),
                "Lossless topology activation is waiting for group-route quiet period"
            );
            return;
        }

        if !self.group_directory_installed {
            info!(
                node_id = self.config.node_id,
                group_directory_installed = self.group_directory_installed,
                group_count = self.group_ip_by_id.len(),
                "Lossless topology activation is waiting for multicast group directory"
            );
            return;
        }

        if self.installed_group_route_ids.len() < self.group_ip_by_id.len() {
            info!(
                node_id = self.config.node_id,
                installed_group_routes = self.installed_group_route_ids.len(),
                expected_group_routes = self.group_ip_by_id.len(),
                "Lossless topology activation is waiting for all multicast group routes"
            );
            return;
        }

        self.lossless_topology_ready = true;
        info!(
            "Lossless topology ready on node {}; enabling runtime and flows.",
            self.config.node_id
        );
        info!(
            node_id = self.config.node_id,
            pending_lossless_flows = self.pending_lossless_flows.len(),
            "Publishing topology-ready to lossless runtime"
        );
        self.lossless_runtime.set_topology_ready(true).await;
        self.flush_pending_lossless_flows();
    }

    async fn deactivate_lossless_topology(&mut self) {
        if !self.lossless_topology_ready {
            return;
        }

        self.lossless_topology_ready = false;
        info!(
            node_id = self.config.node_id,
            "Lossless topology changed; closing runtime admission until worker route sync completes"
        );
        self.lossless_runtime.set_topology_ready(false).await;
    }

    #[cfg(feature = "python-extension")]
    async fn python_handle(&self) -> Option<PythonInterfaceHandle> {
        self.python_interface.lock().await.clone()
    }
}

fn controller_msg_name(msg: &ControllerToDataplane) -> &'static str {
    match msg {
        ControllerToDataplane::StartUp { .. } => "StartUp",
        ControllerToDataplane::AddNode { .. } => "AddNode",
        ControllerToDataplane::AddNodeAddress { .. } => "AddNodeAddress",
        ControllerToDataplane::SetLinkRate { .. } => "SetLinkRate",
        ControllerToDataplane::InstallRoutes { .. } => "InstallRoutes",
        ControllerToDataplane::AddFlows { .. } => "AddFlows",
        ControllerToDataplane::TopologyReady => "TopologyReady",
        ControllerToDataplane::GroupCreated { .. } => "GroupCreated",
        ControllerToDataplane::InstallGroupDirectory { .. } => "InstallGroupDirectory",
        ControllerToDataplane::InstallGroupRoutes { .. } => "InstallGroupRoutes",
        ControllerToDataplane::ProbeLink { .. } => "ProbeLink",
    }
}

#[cfg(test)]
impl ControllerInterfaceHandle {
    pub fn test_handle() -> (Self, mpsc::UnboundedReceiver<DataplaneToController>) {
        let config = LocalConfig::default();
        let processors = ProcessorHandle::new(config.clone());
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();
        #[cfg(feature = "python-extension")]
        let python_interface = Arc::new(Mutex::new(None));

        (
            Self {
                config,
                processors,
                northbridge_sender,
                #[cfg(feature = "python-extension")]
                python_interface,
            },
            northbridge_receiver,
        )
    }
}
