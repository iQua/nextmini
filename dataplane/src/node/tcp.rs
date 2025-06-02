use std::io::Cursor;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::network_interface::NetworkInterfaceHandle;
use crate::node::network_interface::NetworkInterfaceMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::SchedulerHandle;

pub struct TcpServer {
    config: LocalConfig,
    processors: ProcessorHandle,
}

impl TcpServer {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self { config, processors }
    }

    /// Create a network interface from an inbound TCP connection
    async fn from_inbound_connection(
        stream: tokio::net::TcpStream,
        processors: ProcessorHandle,
        remote_node_id: usize,
    ) -> NetworkInterfaceHandle {
        let (reader, writer) = tokio::io::split(stream);
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(100);

        let tcp_reader = TcpReader::new(reader, processors);
        let tcp_writer = TcpWriter::new(writer, receiver);

        tokio::spawn(async move {
            tcp_reader.run().await;
        });

        tokio::spawn(async move {
            tcp_writer.run().await;
        });

        NetworkInterfaceHandle { sender }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut node_id_buf: [u8; 8] = [0; 8];

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    continue;
                }
            };

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            info!("Incoming connection from node {}...", node_id);

            // handles inbound connections
            let network_interface =
                TcpServer::from_inbound_connection(stream, self.processors.clone(), node_id).await;

            // creates the scheduler handle
            let scheduler = SchedulerHandle::new(self.config.clone(), node_id, network_interface);

            // Use processor hashmap
            self.processors.add_node(node_id, scheduler).await;
            info!("Connected to node {}.", node_id);
        }
    }
}

pub struct TcpClient {
    pub config: LocalConfig,
}

impl TcpClient {
    pub async fn connect(&self, remote_addr: &str, remote_node_id: usize) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);

        loop {
            match TcpStream::connect(remote_addr).await {
                Ok(mut stream) => {
                    stream
                        .write_all(&self.config.node_id.to_be_bytes())
                        .await
                        .expect("Failed to send local node id to the node");

                    info!("Connected to node {} with TCP.", remote_node_id);

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

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processors: ProcessorHandle,
}

impl TcpReader {
    pub fn new(stream: ReadHalf<TcpStream>, processors: ProcessorHandle) -> Self {
        Self { stream, processors }
    }

    pub async fn run(mut self) {
        loop {
            // reads a packet from the TCP connection
            if let Ok(packet) = self.read_packet().await {
                // forwards the packet to the processor
                self.processors.process_packet(packet).await;
            }
        }
    }

    /// Reads a single packet from the TCP connection.
    async fn read_packet(&mut self) -> Result<Packet, std::io::Error> {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];
        self.stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;
        self.stream.read_exact(&mut buf[4..msg_len]).await?;

        Ok(Packet::new(msg_len, buf))
    }
}

pub struct TcpWriter {
    stream: WriteHalf<TcpStream>,
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl TcpWriter {
    pub fn new(
        stream: WriteHalf<TcpStream>,
        receiver: mpsc::Receiver<NetworkInterfaceMessage>,
    ) -> Self {
        Self { stream, receiver }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                NetworkInterfaceMessage::SendPacket(packet) => {
                    let result = self.write_packet(&packet).await;
                    if let Err(e) = result {
                        error!("Failed to write packet: {}", e);
                    }
                }
                NetworkInterfaceMessage::Shutdown => {
                    break;
                }
            }
        }
    }

    /// Write packet to the stream
    async fn write_packet(&mut self, packet: &Packet) -> Result<(), std::io::Error> {
        self.stream
            .write_all(&packet.buf[0..packet.packet_size])
            .await?;
        Ok(())
    }
}
