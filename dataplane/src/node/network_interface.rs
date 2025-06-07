use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

use s2n_quic::stream::BidirectionalStream;

use nextmini_messages::Protocol;

use crate::node::NodeId;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::{QuicClient, QuicReader, QuicWriter};
use crate::node::tcp::{TcpClient, TcpReader, TcpWriter};
use crate::node::udp::UdpRelay;

/// Messages sent to the network interface actor, which manages NetworkReader and Writer actors
#[derive(Debug)]
pub enum NetworkInterfaceMessage {
    SendPacket(Packet),
    Shutdown,
}

pub enum NetworkStream {
    Tcp(TcpStream),
    Quic(BidirectionalStream),
}

/// The network interface handle, used for sending and receiving packets over the network.
pub struct NetworkInterfaceHandle {
    pub sender: mpsc::Sender<NetworkInterfaceMessage>,
}

impl NetworkInterfaceHandle {
    /// Creates and runs a new network interface actor from an existing TCP or QUIC stream.
    pub async fn new(
        config: LocalConfig,
        stream: NetworkStream,
        processors: ProcessorHandle,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(config.channel_capacity);

        let network_interface = NetworkInterface {
            config,
            processors,
            receiver,
        };

        network_interface.run(stream);

        Self { sender }
    }

    /// Creates and runs a new network interface actor as a client.
    pub async fn new_as_client(
        config: LocalConfig,
        remote_node_id: NodeId,
        remote_addr: String,
        processors: ProcessorHandle,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(config.channel_capacity);

        let network_interface = NetworkInterface {
            config,
            processors,
            receiver,
        };

        // there is no need to call tokio::spawn here, as the reader and writer tasks will be
        // spawned in run() itself
        network_interface
            .run_as_client(remote_node_id, remote_addr)
            .await;

        Self { sender }
    }

    // Sends a packet through the network interface.
    pub async fn send(&self, packet: Packet) -> Result<(), SendError<NetworkInterfaceMessage>> {
        let _ = self
            .sender
            .send(NetworkInterfaceMessage::SendPacket(packet))
            .await?;

        Ok(())
    }
}

/// The network interface actor, used for sending and receiving packets over the network.
pub struct NetworkInterface {
    config: LocalConfig,
    processors: ProcessorHandle,
    pub receiver: mpsc::Receiver<NetworkInterfaceMessage>,
}

impl NetworkInterface {
    pub async fn run_as_client(self, remote_node_id: NodeId, remote_addr: String) {
        // connects to the remote node
        match self.config.protocol {
            Protocol::Tcp => {
                let tcp_client = TcpClient {
                    config: self.config.clone(),
                };

                let stream = tcp_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.run(NetworkStream::Tcp(stream));
            }
            Protocol::Quic => {
                let quic_client = QuicClient {
                    config: self.config.clone(),
                };

                let stream = quic_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.run(NetworkStream::Quic(stream));
            }
            Protocol::Udp => {
                tokio::spawn(async move {
                    let udp_relay =
                        UdpRelay::new(self.config.clone(), self.receiver, remote_addr).await;
                    udp_relay.run().await;
                });
            }
        }
    }

    pub fn run(self, stream: NetworkStream) {
        match stream {
            NetworkStream::Tcp(stream) => {
                let (reader, writer) = tokio::io::split(stream);

                let tcp_reader = TcpReader::new(reader, self.processors);
                let tcp_writer = TcpWriter::new(writer, self.receiver);

                tokio::spawn(async move {
                    tcp_reader.run().await;
                });

                tokio::spawn(async move {
                    tcp_writer.run().await;
                });
            }
            NetworkStream::Quic(stream) => {
                let (receive_stream, send_stream) = stream.split();

                let mut quic_reader = QuicReader::new(receive_stream, self.processors);
                let mut quic_writer = QuicWriter::new(send_stream, self.receiver);

                tokio::spawn(async move {
                    quic_reader.run().await;
                });

                tokio::spawn(async move {
                    quic_writer.run().await;
                });
            }
        }
    }
}
