use crate::node::processor::ProcessorHandleforReader;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::quic::{QuicReaderHandle, QuicWriterHandle};
use crate::node::tcp::{TcpReaderHandle, TcpWriterHandle};
use crate::node::udp::{UdpReaderHandle, UdpWriterHandle};
use nextmini_messages::Protocol;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{error, info};

pub enum NetworkInterfaceMessage {
    CreateConnection {
        node_id: usize,
        addr: String,
        local_id: usize,
        processor_handle: ProcessorHandleforReader,
        protocol: Protocol,
        // We need a response channel to return the writer handle
        response_tx: oneshot::Sender<Result<NetworkInterfaceWriter, String>>,
    },
    Shutdown,
}

// Actor
pub struct NetworkInterface {
    receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl NetworkInterface {
    pub fn new(receiver: mpsc::Receiver<NetworkInterfaceMessage>) -> Self {
        Self { receiver }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                NetworkInterfaceMessage::CreateConnection {
                    node_id,
                    addr,
                    local_id,
                    processor_handle,
                    protocol,
                    response_tx,
                } => {
                    let result = self
                        .create_connection(node_id, &addr, local_id, processor_handle, protocol)
                        .await;
                    let _ = response_tx.send(result);
                }
                NetworkInterfaceMessage::Shutdown => {
                    info!("NetworkInterface actor shutting down");
                    break;
                }
            }
        }
    }

    async fn create_connection(
        &self,
        node_id: usize,
        addr: &str,
        local_id: usize,
        processor_handle: ProcessorHandleforReader,
        protocol: Protocol,
    ) -> Result<NetworkInterfaceWriter, String> {
        match protocol {
            Protocol::Tcp => {
                let stream = connect_tcp_node(local_id, addr, node_id).await;
                let (reader, writer) = tokio::io::split(stream);

                // No need to return handle
                let _tcp_reader_handle = TcpReaderHandle::new(reader, processor_handle);

                // Return writer handle
                let tcp_writer_handle = TcpWriterHandle::new(Arc::new(Mutex::new(writer)));

                Ok(NetworkInterfaceWriter::Tcp(tcp_writer_handle))
            }
            Protocol::Quic => Err("QUIC protocol not supported yet".to_string()),
            Protocol::Udp => Err("UDP protocol not supported yet".to_string()),
        }
    }
}

// NetworkInterface Handle
#[derive(Clone)]
pub struct NetworkInterfaceHandle {
    sender: mpsc::Sender<NetworkInterfaceMessage>,
}

impl NetworkInterfaceHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let actor = NetworkInterface::new(receiver);

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub async fn create_connection(
        &self,
        node_id: usize,
        addr: String,
        local_id: usize,
        processor_handle: ProcessorHandleforReader,
        protocol: Protocol,
    ) -> Result<NetworkInterfaceWriter, String> {
        let (response_tx, response_rx) = oneshot::channel();

        if let Err(e) = self
            .sender
            .send(NetworkInterfaceMessage::CreateConnection {
                node_id,
                addr,
                local_id,
                processor_handle,
                protocol,
                response_tx,
            })
            .await
        {
            return Err(format!("Failed to send create connection message: {}", e));
        }

        match response_rx.await {
            Ok(result) => result,
            Err(e) => Err(format!("Failed to receive response: {}", e)),
        }
    }

    pub async fn shutdown(&self) {
        if let Err(e) = self.sender.send(NetworkInterfaceMessage::Shutdown).await {
            error!("Failed to send shutdown message: {}", e);
        }
    }
}

// NetworkInterfaceWriter - only TCP is currently supported
#[derive(Clone)]
pub enum NetworkInterfaceWriter {
    Tcp(TcpWriterHandle),
    // Udp(UdpWritererHandle),
    // Quic(QuicWriterHandle),
}

impl NetworkInterfaceWriter {
    pub async fn send(&mut self, data: &[u8]) {
        match self {
            Self::Tcp(writer) => writer.write(data).await,
            // Self::Udp(writer) => writer.send(data).await,
            // Self::Quic(writer) => writer.send(data).await,
        }
    }
}
