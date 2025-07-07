use std::io::Cursor;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::node::config::LocalConfig;
use crate::node::splice::connector::ConnectorHandle;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub struct TcpMaxServer {
    config: LocalConfig,
    connector: ConnectorHandle,
}

impl TcpMaxServer {
    pub fn new(config: LocalConfig, connector: ConnectorHandle) -> Self {
        Self { config, connector }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut flow_id_buf: [u8; 16] = [0; 16];

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Tcp max connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(e) => {
                    error!("Failed to accept TCP max connection: {}", e);
                    continue;
                }
            };

            if let Err(e) = stream.read_exact(&mut flow_id_buf).await {
                error!("Failed to read flow ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&flow_id_buf);

            let flow_id: FlowId = match cursor.read_u128().await {
                Ok(id) => id,
                Err(e) => {
                    error!("Failed to parse flow ID: {}", e);
                    continue;
                }
            };

            let remote_node_id = self.config.ip_to_node_id(flow_id.src_ip());

            info!("Incoming TCP max connection from node {}...", remote_node_id);

            // Tell the connector to splice the upstream
            self.connector.splice_connection(flow_id, stream);

            info!("Connected to node {} with TCP max.", remote_node_id);
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpMaxClient {
    config: LocalConfig,
    connector: ConnectorHandle,
    reporter: ControllerReporterHandle,
}

impl TcpMaxClient {
    pub fn new(
        config: LocalConfig,
        connector: ConnectorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        Self {
            config,
            connector,
            reporter,
        }
    }

    pub async fn connect_as_dst_node(&self, stream: TcpStream) -> SchedulerHandle {
        let network_interface = NetworkInterfaceHandle::new(
            self.config.clone(),
            NetworkStream::Tcp(stream),
            self.connector.clone(),
            self.reporter.clone(),
            self.config.node_id,
        )
        .await;

        let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

        scheduler
    }

    pub async fn connect_as_src_node(
        &self,
        flow_id: FlowId,
        remote_addr: &str,
        remote_node_id: NodeId,
    ) -> SchedulerHandle {
        let stream = self.connect_as_relay(flow_id, remote_addr).await;

        let network_interface = NetworkInterfaceHandle::new(
            self.config.clone(),
            NetworkStream::Tcp(stream),
            self.connector.clone(),
            self.reporter.clone(),
            remote_node_id,
        )
        .await;
        let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

        scheduler
    }

    pub async fn connect_as_relay(&self, flow_id: FlowId, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    stream
                        .write_all(&flow_id.to_be_bytes())
                        .await
                        .expect("Failed to send local node id to the node");

                    info!(
                        "Connected to node {} with TCP max.",
                        self.config.ip_to_node_id(flow_id.src_ip())
                    );

                    return stream;
                }
                Err(e) => {
                    error!(
                        "Failed to connect to node address {} with error: {}, retrying in {}s.",
                        remote_addr,
                        e,
                        delay.as_secs()
                    );
                    tokio::time::sleep(delay).await;
                    retry_count += 1;

                    if retry_count >= MAX_RETRY {
                        panic!("Maximum retry reached for TCP connection to {remote_addr}");
                    }

                    delay = delay.mul_f32(1.5); // Exponential backoff
                }
            }
        }
    }
}
