use std::io::Cursor;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::scheduler::SchedulerHandle;
use crate::node::{FlowId, FlowIdExt, NodeId};

pub struct TcpMaxServer {
    config: LocalConfig,
    processors: ProcessorHandle,
}

impl TcpMaxServer {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self { config, processors }
    }

    /// accepts incoming tcp connections and receives the first packet.
    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(_) => {
                    error!("Failed to accept TCP connection");
                    continue;
                }
            };

            // reads the first packet from the stream
            let first_packet = match self.read_packet(&mut stream).await {
                Ok(packet) => packet,
                Err(e) => {
                    error!("Failed to read first packet: {}", e);
                    continue;
                }
            };

            // TODO: Change the logging to print correct information
            // gets the next hop's node ID from the first packet's flow ID for logging
            let remote_node_id = self.config.ip_to_node_id(first_packet.flow_id.src_ip());

            // TODO: Corrected logs below
            // info!("Incoming connection from node {}...", remote_node_id);

            // tells the processor to splice the upstream
            self.processors
                .inbound_max_request(first_packet, stream)
                .await;

            // TODO: Corrected logs below
            info!("Connected to node {}.", remote_node_id);
        }
    }

    /// Reads a single packet from the TCP connection.
    async fn read_packet(&mut self, stream: &mut TcpStream) -> Result<Packet, std::io::Error> {
        let mut buf = vec![0; RECEIVE_BUF_SIZE];
        stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;
        stream.read_exact(&mut buf[4..msg_len]).await?;

        Ok(Packet::new(msg_len, buf))
    }
}

#[derive(Debug, Clone)]
pub struct TcpMaxClient {
    config: LocalConfig,
    processor: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl TcpMaxClient {
    pub fn new(
        config: LocalConfig,
        processor: ProcessorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        Self {
            config,
            processor,
            reporter,
        }
    }

    /// Requests a tcp connection and writes the first packet.
    pub async fn request_remote(&self, packet: Packet, remote_addr: &str) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    stream
                        .write_all(&packet.buf[0..packet.packet_size])
                        .await
                        .expect("Failed to send local node id to the node");

                    // TODO : Modified this log to print correct information
                    info!(
                        "Connected to node {} with TCP MAX.",
                        self.config.ip_to_node_id(packet.flow_id.src_ip())
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
                        panic!("Maximum retry reached for TCP MAX connection to {remote_addr}");
                    }

                    delay = delay.mul_f32(1.5); // Exponential backoff
                }
            }
        }
    }

    /// Initializes a scheduler with a network interface for src node and dst node
    /// Used by dst node only when dst node is in max mode.
    pub async fn initialize_scheduler(
        &self,
        stream: TcpStream,
        remote_node_id: NodeId,
    ) -> SchedulerHandle {
        let network_interface = NetworkInterfaceHandle::new(
            self.config.clone(),
            NetworkStream::Tcp(stream),
            self.processor.clone(),
            self.reporter.clone(),
            remote_node_id,
        )
        .await;

        SchedulerHandle::new(self.config.clone(), network_interface)
    }
}
