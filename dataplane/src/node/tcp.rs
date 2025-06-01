use crate::node::RECEIVE_BUF_SIZE;
use crate::node::network_interface::NetworkInterfaceMessage;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::SchedulerHandle;
use crate::node::network_interface::NetworkInterfaceHandle;
use nextmini_messages::Protocol;
use crate::node::config::LocalConfig;

use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};

pub struct TcpServer {
    config: LocalConfig,
    processor_handle: ProcessorHandle,
}

impl TcpServer {
    pub fn new(config: LocalConfig, processor_handle: ProcessorHandle) -> Self {
        Self { config, processor_handle }
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
            
            // Consider redesign network interface to avoid creation here

            //Split the stream into reader and writer
            let (reader, writer) = tokio::io::split(stream);
            // set up the mpsc channel for the network interface
            let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(100);
            // create the reader and writer
            let reader = TcpReader::new(reader, self.processor_handle.clone());
            let writer = TcpWriter::new(Arc::new(Mutex::new(writer)), receiver);
            tokio::spawn(async move {
                reader.run().await;
            });
            tokio::spawn(async move {
                writer.run().await;
            });
            // create the network interface handle manually
            let network_interface = NetworkInterfaceHandle { sender };
            // create the scheduler handle
            let scheduler = SchedulerHandle::new(self.config.clone(), node_id, network_interface);

            
            // Use processor hashmap
            self.processor_handle.add_node(node_id, scheduler);
            info!("Connected to node {}.", node_id);
        }
    }
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processor_handle: ProcessorHandle,
}

impl TcpReader {
    pub fn new(stream: ReadHalf<TcpStream>, processor_handle: ProcessorHandle) -> Self {
        Self {
            stream,
            processor_handle,
        }
    }

    pub async fn run(mut self) {
        loop {
            // Try to read a packet from the network
            match self.read_packet().await {
                Ok(packet) => {
                    // Forward the packet to the processor
                    self.processor_handle.process_packet(packet).await;
                }
                Err(e) => {
                    error!("Failed to read TCP packet: {}", e);
                    break;
                }
            }
        }
    }

    /// Read a single packet from the stream
    async fn read_packet(&mut self) -> Result<Packet, std::io::Error> {
        let mut buf = [0u8; RECEIVE_BUF_SIZE];

        self.stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;

        self.stream.read_exact(&mut buf[4..msg_len]).await?;

        // Return the packet
        Ok(Packet::new(msg_len, buf))
    }
}

pub struct TcpWriter {
    stream: Arc<Mutex<WriteHalf<TcpStream>>>,
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl TcpWriter {
    pub fn new(
        stream: Arc<Mutex<WriteHalf<TcpStream>>>,
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
        let mut stream_guard = self.stream.lock().await;
        stream_guard
            .write_all(&packet.buf[0..packet.packet_size])
            .await?;
        Ok(())
    }
}
