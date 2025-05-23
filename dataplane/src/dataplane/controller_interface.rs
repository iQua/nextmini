use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::time::{Duration, interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde_json::Value;

use nextmini_messages::{ControllerToDataplane, DataplaneToController, Protocol};

use crate::dataplane::RateLimiterMap;
use crate::dataplane::configs::{ControllerConfigs, LocalConfigs};
use crate::dataplane::context::Context;
use crate::dataplane::metrics::Collector;
use crate::dataplane::processor::ProcessorManager;
use crate::dataplane::protocols_client;
use nextmini_messages::RouteMapping;
use crate::dataplane::utils::RateLimiter;

pub struct Controller {
    controller_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    sender_tx: UnboundedSender<DataplaneToController>,
    sender_rx: UnboundedReceiver<DataplaneToController>,
    shutdown_tx: watch::Sender<bool>,
    context: Context,
    metrics_collector: Option<Collector>,
    link_rate_limiters: Arc<RwLock<RateLimiterMap>>,
    controller_configs: ControllerConfigs,
    processor_manager: Arc<RwLock<ProcessorManager>>,
}

impl Controller {
    pub async fn connect(configs: LocalConfigs, shutdown_tx: watch::Sender<bool>) -> Controller {
        let url = url::Url::parse(&configs.server_addr).unwrap();
        let mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>;
        loop {
            match connect_async(url.as_str()).await {
                Ok((ws, _)) => {
                    ws_stream = ws;
                    println!("WebSocket handshake has been successfully completed");
                    break;
                }
                Err(e) => {
                    println!("Failed to connect to controller: {}. Retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        let startup_msg = DataplaneToController::StartUp {
            private_network_name: configs.private_network_name.clone(),
            private_network_addr: configs.private_network_addr.clone()
                + ":"
                + &configs.private_network_port.clone(),
            public_network_addr: configs.public_network_addr.clone()
                + ":"
                + &configs.public_network_port.clone(),
            node_id: configs.node_id.parse().ok(),
        };
        ws_stream
            .send(Message::binary(rmp_serde::to_vec(&startup_msg).unwrap()))
            .await
            .expect("Failed to send startup request to server");

        // Parse the message received from server.
        let msg = ws_stream.next().await.unwrap().unwrap();
        let controller_configs_raw = match msg {
            Message::Binary(data) => {
                rmp_serde::from_slice(&data).expect("Invalid MessagePack message")
            }
            _ => panic!("Invalid data received from server, expected binary"),
        };
        let controller_configs = ControllerConfigs::new(controller_configs_raw);

        let (sender_tx, sender_rx) = unbounded_channel::<DataplaneToController>();

        // Create metrics collector.
        let metrics_collector =
            Collector::new(sender_tx.clone(), configs.metrics_collection_interval);

        // Create link rate limiters.
        let link_rate_limiters = Arc::new(RwLock::new(HashMap::new()));

        // Create node manager.
        let mut context = Context::new(
            configs.clone(),
            controller_configs.node_id,
            metrics_collector.get_metrics_tx(),
            link_rate_limiters.clone(),
            controller_configs.scheduler_type,
        );

        if controller_configs.protocol == Protocol::Udp {
            context
                .init_udp_socket(configs.private_network_port.clone())
                .await;
        }
        
        // Create and start the single TUN device
        context.start_tun_device(&configs, &controller_configs).await;

        let processor_manager = Arc::new(RwLock::new(ProcessorManager::new(
            context.clone(),
            context.get_processor_rxs().await,
        )));

        Controller {
            controller_stream: ws_stream,
            sender_tx,
            sender_rx,
            shutdown_tx,
            context,
            metrics_collector: Some(metrics_collector),
            link_rate_limiters,
            controller_configs,
            processor_manager,
        }
    }

    pub fn take_metrics_collector(&mut self) -> Collector {
        self.metrics_collector
            .take()
            .expect("Failed to take metrics collector: already taken")
    }

    pub fn get_context(&self) -> Context {
        self.context.clone()
    }

    pub fn get_session_id(&self) -> [u8; 4] {
        self.controller_configs.session_id
    }

    pub fn get_protocol(&self) -> Protocol {
        self.controller_configs.protocol.clone()
    }

    pub fn get_processor_manager(&self) -> Arc<RwLock<ProcessorManager>> {
        self.processor_manager.clone()
    }

    pub async fn split(self) -> (ControllerSender, ControllerReceiver) {
        // Split the controller interface to sender and receiver ends.
        let (controller_sender_stream, controller_receiver_stream) = self.controller_stream.split();

        let sender = ControllerSender {
            controller_sender_stream,
            tx: self.sender_tx,
            rx: self.sender_rx,
        };
        let receiver = ControllerReceiver {
            shutdown_tx: self.shutdown_tx,
            controller_configs: self.controller_configs,
            processor_manager: self.processor_manager,
            context: self.context,
            link_rate_limiters: self.link_rate_limiters,
            controller_receiver_stream,
        };

        (sender, receiver)
    }
}

pub struct ControllerReceiver {
    shutdown_tx: watch::Sender<bool>,
    controller_configs: ControllerConfigs,
    processor_manager: Arc<RwLock<ProcessorManager>>,
    context: Context,
    link_rate_limiters: Arc<RwLock<RateLimiterMap>>,
    controller_receiver_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl ControllerReceiver {
    pub async fn run(&mut self) {
        loop {
            let msg = match self.controller_receiver_stream.next().await.unwrap() {
                Ok(msg) => msg,
                Err(e) => {
                    println!("Connection with the controller is broken. Restarting node state..");
                    println!("Connection Lost with Error: {:?}", e);
                    self.shutdown_tx
                        .send(true)
                        .expect("Failed to send shutdown signal to main task");
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
                    // received a pong message to keep the connection alive. Do nothing.
                    continue;
                }
                _ => {
                    println!(
                        "Received a message that is not a binary or a ping message. There may be something wrong."
                    );
                }
            };
        }
    }

    async fn process_control_msg(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::InstallFlow { flows } => {
                println!("Installing simple routes..");
                // Convert flows to RouteMapping format for simple routing
                let routes: Vec<RouteMapping> = flows.into_iter().enumerate().map(|(idx, _flow)| {
                    RouteMapping {
                        route_id: idx,
                        next_hop: 2, // Default next hop - should be configured properly
                        src_addr: [10, 0, 0, 1],
                        dst_addr: [10, 0, 0, 4],
                    }
                }).collect();
                
                self.processor_manager
                    .write()
                    .await
                    .update_simple_routes(routes)
                    .await;
            }
            ControllerToDataplane::AddNode {
                protocol,
                node_id,
                addr,
            } => {
                match protocol {
                    Protocol::Tcp => {
                        self.new_tcp_node(&serde_json::json!({"node_id": node_id, "addr": addr}))
                            .await
                    }
                    Protocol::Udp => {
                        self.new_udp_node(&serde_json::json!({"node_id": node_id, "addr": addr}))
                            .await
                    }
                    Protocol::Quic => {
                        self.new_quic_node(&serde_json::json!({"node_id": node_id, "addr": addr}))
                            .await
                    }
                };
                self.processor_manager
                    .write()
                    .await
                    .update_processors()
                    .await;
            }
            ControllerToDataplane::SetLinkRate { node_id, rate } => {
                println!("Setting link rate for node: {}, rate: {}", node_id, rate);
                let mut guard = self.link_rate_limiters.write().await;

                match guard.get(&node_id) {
                    None => {
                        let rate_limiter =
                            Arc::new(RwLock::new(Some(RateLimiter::new(rate as f64))));
                        guard.insert(node_id, rate_limiter);
                    }
                    Some(entry) => {
                        *(entry.write().await) = Some(RateLimiter::new(rate as f64));
                    }
                }
            }
            ControllerToDataplane::InstallRoutes { routes } => {
                println!("Installing simplified routes..");
                self.processor_manager
                    .write()
                    .await
                    .update_simple_routes(routes)
                    .await;
                println!("Simplified routes installed.");
            }
            _ => println!("Received unsupported message type"),
        }
    }

    async fn new_tcp_node(&mut self, ctrl_msg: &Value) {
        let node_id: usize = ctrl_msg["node_id"]
            .as_u64()
            .expect("Invalid control message: expected to contain the field 'node_id'")
            as usize;
        let addr = ctrl_msg["addr"]
            .as_str()
            .expect("Invalid control message: expected to contain the field 'addr'");
        let local_id = self.context.local_id;
        let session_id = self.controller_configs.session_id;

        let stream = protocols_client::connect_tcp_node(local_id, addr, node_id, &session_id).await;
        self.context.add_tcp_node(node_id, stream).await;
    }

    async fn new_udp_node(&mut self, ctrl_msg: &Value) {
        let node_id: usize = ctrl_msg["node_id"]
            .as_u64()
            .expect("Invalid control message: expected to contain the field 'node_id'")
            as usize;
        let addr = ctrl_msg["addr"]
            .as_str()
            .expect("Invalid control message: expected to contain the field 'addr'");

        self.context.add_udp_node(node_id, addr.to_string()).await;
    }

    async fn new_quic_node(&mut self, ctrl_msg: &Value) {
        let node_id: usize = ctrl_msg["node_id"]
            .as_u64()
            .expect("Invalid control message: expected to contain the field 'node_id'")
            as usize;
        let addr = ctrl_msg["addr"]
            .as_str()
            .expect("Invalid control message: expected to contain the field 'addr'");
        let local_id = self.context.local_id;
        let session_id = self.controller_configs.session_id;

        let stream =
            protocols_client::connect_quic_node(local_id, addr, node_id, &session_id).await;
        self.context.add_quic_node(node_id, stream).await;
    }
}

pub struct ControllerSender {
    controller_sender_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    #[allow(unused)] // for future use
    tx: UnboundedSender<DataplaneToController>,
    rx: UnboundedReceiver<DataplaneToController>,
}

impl ControllerSender {
    #[allow(unused)] // for future use
    pub fn get_tx(&self) -> UnboundedSender<DataplaneToController> {
        self.tx.clone()
    }

    pub async fn run(&mut self) {
        let mut ping_interval = interval(Duration::from_secs(30)); // Send ping every 30 seconds

        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
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
