use std::sync::Arc;

use s2n_quic::stream::BidirectionalStream;
use tokio::sync::{Mutex, mpsc};

use nextmini_messages::Protocol;

use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::protocols_client::connect_quic_node;
use crate::node::protocols_client::connect_tcp_node;
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::tcp::{TcpReader, TcpWriter};

/// Messages sent to the network interface actor, which manages NetworkReader and Writer actors
#[derive(Debug)]
pub enum NetworkInterfaceMessage {
    SendPacket(Packet),
    Shutdown,
}

/// The network interface handle, used for sending and receiving packets over the network.
pub struct NetworkInterfaceHandle {
    pub sender: mpsc::Sender<NetworkInterfaceMessage>,
}

impl NetworkInterfaceHandle {
    /// Creates and runs a new network interface actor.
    pub async fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(100);

        let network_interface = NetworkInterface {
            config,
            processors,
            receiver,
        };

        network_interface.run().await;

        Self { sender }
    }

    /// Send a packet through the network interface
    pub async fn send(&self, packet: Packet) {
        self.sender
            .send(NetworkInterfaceMessage::SendPacket(packet))
            .await
            .unwrap();
    }
}

/// The network interface actor, used for sending and receiving packets over the network.
pub struct NetworkInterface {
    config: LocalConfig,
    processors: ProcessorHandle,
    pub receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl NetworkInterface {
    pub async fn run(&self) {
        match self.config.protocol {
            Protocol::Tcp => {
                /// requests a connection to the remote node
                let stream = connect_tcp_node(local_node_id, addr, remote_node_id).await;
                let (reader, writer) = tokio::io::split(stream);

                let tcp_reader = TcpReader::new(reader, self.processors);
                let tcp_writer = TcpWriter::new(Arc::new(Mutex::new(writer)), self.receiver);

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
                // To be implemented
            }
        }
    }
}
