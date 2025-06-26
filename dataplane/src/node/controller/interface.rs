use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tracing::{error, info};

use nextmini_messages::{ControllerToDataplane, DataplaneToController};

use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::tcp::{UserSpaceTcpHandle, UserSpaceTcpSource};
use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::processor::{ProcessorHandle, ProcessorMessage};
use crate::node::scheduler::scheduler::SchedulerHandle;

#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    pub config: LocalConfig,
    pub processors: ProcessorHandle,
    northbridge_sender: mpsc::UnboundedSender<DataplaneToController>,
}

/// The handle for the controller interface, which allows sending messages to the controller.
impl ControllerInterfaceHandle {
    pub async fn new(config: LocalConfig) -> (Self, ControllerReporterHandle) {
        // creates an unbounded channel, the 'northbridge', for sending messages to the controller
        let (northbridge_sender, northbridge_receiver) = mpsc::unbounded_channel();

        // connects to the controller over WebSockets
        let (config, processors, ws_stream) = ControllerInterfaceHandle::connect(config).await;

        let (sender_stream, receiver_stream) = ws_stream.split();

        // Initialize the controller sender and receiver
        let mut controller_sender = DataplaneToControllerSender {
            sender_stream,
            northbridge_receiver,
        };

        let controller_interface = Self {
            config: config.clone(),
            processors: processors.clone(),
            northbridge_sender,
        };

        let reporter = ControllerReporterHandle::new(controller_interface.clone());

        let mut controller_receiver = ControllerToDataplaneReceiver {
            config,
            receiver_stream,
            processors,
            reporter: reporter.clone(),
            user_space_tcp_handle: None,
        };

        tokio::spawn(async move {
            controller_sender.run().await;
        });
        tokio::spawn(async move {
            controller_receiver.run().await;
        });

        (controller_interface, reporter)
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
        loop {
            match connect_async(url.as_str()).await {
                Ok((ws, _)) => {
                    ws_stream = ws;
                    info!("WebSocket handshake has been successfully completed.");
                    break;
                }
                Err(e) => {
                    error!("Failed to connect to the controller: {}. Retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
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
                    self.sender_stream
                        .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
                        .await
                        .expect("Failed to send message to controller");
                }
                _ = ping_interval.tick() => {
                    self.sender_stream
                        .send(Message::Ping(vec![].into()))
                        .await
                        .expect("Failed to send ping to controller");
                }
            }
        }
    }
}

/// An actor used for receiving messages from the controller and broadcasts them to the processors.
pub struct ControllerToDataplaneReceiver {
    config: LocalConfig,
    receiver_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    processors: ProcessorHandle,

    // reports metrics to controller
    reporter: ControllerReporterHandle,

    // sends AddFlows message user-space TCP
    // the user-space TCP source is only created on the first AddFlows message.
    user_space_tcp_handle: Option<UserSpaceTcpHandle>,
}

impl ControllerToDataplaneReceiver {
    pub async fn run(&mut self) {
        loop {
            let msg = match self.receiver_stream.next().await.unwrap() {
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
                    self.process_control_msg(ctrl_msg).await;
                }
                Message::Pong(_) => {
                    // received a ping message to keep the connection alive. Do nothing.
                    continue;
                }
                _ => {
                    error!("Received a message that is not a binary or a ping message.");
                }
            };
        }
    }

    async fn process_control_msg(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::AddNode {
                remote_node_id,
                remote_addr,
            } => {
                let network_interface = NetworkInterfaceHandle::new_as_client(
                    self.config.clone(),
                    remote_node_id,
                    remote_addr.clone(),
                    self.processors.clone(),
                    self.reporter.clone(),
                )
                .await;

                let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

                if let Err(e) = self.processors.add_node(remote_node_id, scheduler) {
                    error!(
                        "Failed to add node {} with address {}: {}.",
                        remote_node_id, remote_addr, e
                    );
                }
            }

            ControllerToDataplane::SetLinkRate { node_id, spec } => {
                info!(
                    "Setting the link rate for node {} to {} bytes/second with a bucket size of {} bytes.",
                    node_id, spec.rate, spec.bucket_size,
                );

                self.processors.limit_rate(node_id, spec);
            }

            ControllerToDataplane::InstallRoutes { routes } => {
                info!(
                    "Installing {} routes on node {}.",
                    routes.len(),
                    self.config.node_id
                );

                self.processors.update_routing_table(routes);
            }

            ControllerToDataplane::AddFlows { flows } => {
                info!(
                    "Add {} flows for node {}.",
                    flows.len(),
                    self.config.node_id
                );

                // Creates the user space TCP source on the first AddFlows message.
                // This should avoids the ip assignment issue.
                // if we creates a new tcp_source for each AddFlows message or for each flow
                // for now, since we are using <Ipv4Addr, Arc<dyn LocalDestination>>
                // processor doesn't know about which tcp_source to send.
                if self.user_space_tcp_handle.is_none()
                    && !self.config.user_space_address.is_unspecified()
                {
                    let (tcp_source, flow_receiver) = UserSpaceTcpSource::new(
                        self.config.clone(),
                        self.config.user_space_address,
                        self.processors.clone(),
                    );

                    let tcp_source_arc = Arc::new(tcp_source);

                    self.processors
                        .broadcast_sender()
                        .send(ProcessorMessage::ConnectLocalDestination(
                            tcp_source_arc.ip_addr,
                            tcp_source_arc.clone(),
                        ))
                        .expect("Failed to connect to the user-space TCP source.");

                    // starts the user-space tcp thread,
                    // passes the receiver end of the channel.
                    tcp_source_arc.start(flow_receiver);

                    // creates and saves a user-space TCP handle
                    self.user_space_tcp_handle =
                        Some(UserSpaceTcpHandle::new(tcp_source_arc.flow_sender.clone()));
                }

                // sends the new flows to the user-space TCP.
                if let Some(user_space_tcp_source) = &self.user_space_tcp_handle {
                    user_space_tcp_source.add_flows(flows);
                }
            }
            // Pending changes according to user space tcp implementation
            ControllerToDataplane::SetFlowWeight {
                src_ip,
                dst_ip,
                src_port,
                dst_port,
                weight,
            } => {
                info!(
                    "Setting the flow weight {} at node {}.",
                    weight, self.config.node_id
                );

                // Convert the 4-tuple to a flow_id
                let flow_id = ((src_ip as u128) << 96)
                    | ((dst_ip as u128) << 64)
                    | ((src_port as u128) << 48)
                    | ((dst_port as u128) << 32);

                self.processors.set_flow_weight(flow_id, weight);
            }
            _ => error!("Received a message with an unknown type from the controller."),
        }
    }
}
