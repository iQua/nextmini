use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::{SchedulingDiscipline, SchedulerHandle};
use crate::node::drop::DropStrategy;
use crate::node::NodeId;
use crate::node::utils::RateLimiter;
use crate::node::protocols_io::NetworkInterface;
use nextmini_messages::{ControllerToDataplane, DataplaneToController};

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{Duration, interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};

use tracing::{info,error};


#[derive(Clone)]
pub struct ControllerInterfaceHandle {
    config: LocalConfig,
    sender: mpsc::UnboundedSender<DataplaneToController>,
}

// TODO : Implement shutdown logic
// TODO : Decide how to pass processor handle to protocol reader
impl ControllerInterfaceHandle {
    pub async fn new(
        config: LocalConfig,
        processor_handle: ProcessorHandle,
        shutdown_send: mpsc::UnboundedSender<()>,
    ) -> Self {
        // create unbounded channel for controller sender
        let (sender, receiver) = mpsc::unbounded_channel();

        // Initialize the controller interface handle and connect to the controller
        let mut controller_interface_handle = Self { config: config.clone(), sender};
        let ws_stream = controller_interface_handle.connect().await;

        let (controller_sender_stream, controller_receiver_stream) = ws_stream.split();

        // Initialize the controller sender and receiver
        let mut controller_sender = ControllerSender {
            controller_sender_stream,
            receiver,
        };

        let mut controller_receiver = ControllerReceiver {
            controller_receiver_stream,
            processor_handle: processor_handle,
            scheduler_mpsc_channel_size: config.scheduler_mpsc_channel_size,
            scheduler_type: config.scheduler_type,
            controller_interface_handle: controller_interface_handle.clone(),
            local_id: config.node_id,
            scheduler_queue_capacity: config.scheduler_queue_capacity,
            scheduler_drop_strategy: config.scheduler_drop_strategy.clone(),
            scheduler_rate_limiter: Arc::new(RwLock::new(None)),
        };

        tokio::spawn(async move { controller_sender.run().await });
        tokio::spawn(async move { controller_receiver.run().await });

        controller_interface_handle
    }

    pub async fn send_metrics(&self, msg: DataplaneToController){
        self.sender
        .send(msg);
    }

    pub async fn connect(&mut self) -> WebSocketStream<MaybeTlsStream<TcpStream>>{
        let url = url::Url::parse(&self.config.controller_addr).unwrap();
        let mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>;
        loop {
            match connect_async(url.as_str()).await {
                Ok((ws, _)) => {
                    ws_stream = ws;
                    info!("WebSocket handshake has been successfully completed.");
                    break;
                }
                Err(e) => {
                    info!("Failed to connect to the controller: {}. Retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }

        // Need to check if  current message information is correct
        let startup_msg = DataplaneToController::StartUp {
            private_network_name: self.config.private_network_name.clone(),
            private_network_addr: self.config.private_network_addr.clone()
                + ":"
                + &self.config.private_network_port.clone(),
            public_network_addr: self.config.public_network_addr.clone()
                + ":"
                + &self.config.public_network_port.clone(),
            node_id: self.config.node_id.to_string().parse().ok(),
        };

        
        ws_stream
            .send(Message::binary(rmp_serde::to_vec(&startup_msg).unwrap()))
            .await
            .expect("Failed to send startup request to server");

        // Need to check what each method does here
        let msg = ws_stream.next().await.unwrap().unwrap();
        let controller_configs_raw = match msg {
            Message::Binary(data) => {
                rmp_serde::from_slice(&data).expect("Invalid MessagePack message")
            }
            _ => panic!("Invalid data received from server, expected binary"),
        };

        // Obtain the complete config and store inside the controller interface handle
        let config = self.config.init(controller_configs_raw);
        self.config = config;

        ws_stream
    }
}

pub struct ControllerReceiver{
    controller_receiver_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    processor_handle: ProcessorHandle,

    scheduler_mpsc_channel_size: usize,
    scheduler_type: SchedulingDiscipline,
    controller_interface_handle: ControllerInterfaceHandle,
    local_id: NodeId,
    scheduler_queue_capacity: usize,
    scheduler_drop_strategy: DropStrategy,
    scheduler_rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
}

impl ControllerReceiver{
    pub async fn run(&mut self) {
        loop {
            let msg = match self.controller_receiver_stream.next().await.unwrap() {
                Ok(msg) => msg,
                Err(e) => {
                    info!("Connection with the controller is broken. Restarting node state..");
                    info!("Connection Lost with Error: {:?}", e);

                    // TODO : update the shutdown logic according to how controller interface is implemented
                    // self.shutdown_tx
                    //     .send(true)
                    //     .expect("Failed to send shutdown signal to main task");

                    return;
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
                protocol,
                node_id,
                addr,
            } => {
                // TODO : Implement network interface handle
                let protocol_writer;

                let scheduler_handle = SchedulerHandle::new(
                    self.scheduler_mpsc_channel_size,
                    protocol_writer,
                    self.scheduler_type,
                    self.controller_interface_handle.clone(),
                    self.local_id,
                    self.scheduler_queue_capacity,
                    self.scheduler_drop_strategy.clone(),
                    self.scheduler_rate_limiter.clone(),
                );
                self.processor_handle.add_node(node_id, scheduler_handle).await;
            }
            ControllerToDataplane::SetLinkRate { node_id, rate } => {
                // TODO : Implement set link rate
                // info!("Setting link rate for node: {}, rate: {}", node_id, rate);
                // let mut guard = self.link_rate_limiters.write().await;

                // match guard.get(&node_id) {
                //     None => {
                //         let rate_limiter =
                //             Arc::new(RwLock::new(Some(RateLimiter::new(rate as f64))));
                //         guard.insert(node_id, rate_limiter);
                //     }
                //     Some(entry) => {
                //         *(entry.write().await) = Some(RateLimiter::new(rate as f64));
                //     }
                // }
                return;
            }
            ControllerToDataplane::InstallRoutes { routes } => {
                info!("Installing {} routes.", routes.len());
                self.processor_handle.update_routing_table(routes).await;
            }
            _ => error!("Received unsupported message type from controller."),
        }
    }
}

pub struct ControllerSender{
    receiver: mpsc::UnboundedReceiver<DataplaneToController>,
    controller_sender_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl ControllerSender{
    pub async fn run(&mut self) {
        let mut ping_interval = interval(Duration::from_secs(30)); // Send ping every 30 seconds

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.send_msg(msg).await;
                }
                _ = ping_interval.tick() => {
                    self.controller_sender_stream
                        .send(Message::Ping(vec![].into()))
                        .await
                        .expect("Failed to send ping to controller");
                }
            }
        }
    }

    async fn send_msg(&mut self, msg: DataplaneToController) {
        self.controller_sender_stream
            .send(Message::binary(rmp_serde::to_vec(&msg).unwrap()))
            .await
            .expect("Failed to send message to controller");
    }
}
