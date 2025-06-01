use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::protocols_client::connect_quic_node;
use crate::node::tcp::{TcpReader, TcpWriter};
use crate::node::quic::{QuicReader, QuicWriter};
use nextmini_messages::Protocol;

use s2n_quic::stream::BidirectionalStream;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tracing::info;

/// Messages sent to the network interface actor, which manages NetworkReader and Writer actors
#[derive(Debug)]
pub enum NetworkInterfaceMessage {
    SendPacket(Packet),
    Shutdown,
}

/// Should be created in controller_interface and used in scheduler to send packets
pub struct NetworkInterfaceHandle {
    pub sender: mpsc::Sender<NetworkInterfaceMessage>,
}

impl NetworkInterfaceHandle {
    /// Create a new network interface connected to a remote node
    pub async fn new(
        processor_handle: ProcessorHandle,
        addr: &str,
        local_node_id: usize,
        remote_node_id: usize,
        protocol: Protocol,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(100);
        let network_interface = Self { sender };

        network_interface
            .create_connection(
                addr,
                local_node_id,
                remote_node_id,
                protocol,
                processor_handle,
                receiver,
            )
            .await;

        network_interface
    }

    pub async fn create_connection(
        &self,
        addr: &str,
        local_node_id: usize,
        remote_node_id: usize,
        protocol: Protocol,
        processor_handle: ProcessorHandle,
        receiver: mpsc::Receiver<NetworkInterfaceMessage>,
    ) {
        match protocol {
            Protocol::Tcp => {
                // Request connection to remote node server
                let stream = connect_tcp_node(local_node_id, addr, remote_node_id).await;
                let (reader, writer) = tokio::io::split(stream);

                let tcp_reader = TcpReader::new(reader, processor_handle);

                let tcp_writer = TcpWriter::new(Arc::new(Mutex::new(writer)), receiver);

                tokio::spawn(async move {
                    tcp_reader.run().await;
                });

                tokio::spawn(async move {
                    tcp_writer.run().await;
                });
            }
            Protocol::Quic => {
                let stream = connect_quic_node(local_node_id, addr, remote_node_id).await;
                let (receive_stream, send_stream) = stream.split();

                let mut quic_reader = QuicReader::new(processor_handle, receive_stream);
                let mut quic_writer = QuicWriter::new(Arc::new(Mutex::new(send_stream)), receiver);

                tokio::spawn(async move {
                    quic_reader.run().await;
                });

                tokio::spawn(async move {
                    quic_writer.run().await;
                });
            }
            Protocol::Udp => {
                // UDP implementation (not implemented yet)
                info!("UDP protocol not implemented yet");
            }
        }
    }

    /// Send a packet through the network interface
    pub async fn send(&self, packet: Packet) {
        self.sender
            .send(NetworkInterfaceMessage::SendPacket(packet))
            .await
            .unwrap();
    }
}
