use tokio::net::TcpStream;
use tokio::sync::mpsc;

use s2n_quic::stream::BidirectionalStream;
use tracing::{error, info};

use crate::node::NodeId;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::{QuicReader, QuicWriter};
use crate::node::tcp::{TcpClient, TcpReader, TcpWriter};

/// Messages sent to the network interface actor, which manages NetworkReader and Writer actors
#[derive(Debug)]
pub enum NetworkInterfaceMessage {
    SendPacket(Packet),
    Shutdown,
}

enum NetworkStream {
    Tcp(TcpStream),
    Quic(BidirectionalStream),
}

/// The network interface handle, used for sending and receiving packets over the network.
pub struct NetworkInterfaceHandle {
    pub sender: mpsc::Sender<NetworkInterfaceMessage>,
}

impl NetworkInterfaceHandle {
    /// Creates and runs a new network interface actor as a client.
    pub async fn new_as_client(
        config: LocalConfig,
        remote_addr: String,
        remote_node_id: NodeId,
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

    /// Creates and runs a new network interface actor from an existing TCP or QUIC stream.
    pub async fn new(
        config: LocalConfig,
        stream: NetworkStream,
        remote_node_id: NodeId,
        processors: ProcessorHandle,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<NetworkInterfaceMessage>(config.channel_capacity);

        let network_interface = NetworkInterface {
            config,
            processors,
            receiver,
        };

        network_interface.run(stream, remote_node_id);

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
    pub async fn run_as_client(self, remote_node_id: NodeId, remote_addr: String) {
        // connects to the remote node
        let tcp_client = TcpClient {
            config: self.config.clone(),
        };

        let stream = tcp_client
            .connect(remote_addr.as_str(), remote_node_id)
            .await;

        self.run(NetworkStream::Tcp(stream), remote_node_id);
    }

    pub fn run(self, stream: NetworkStream, remote_node_id: NodeId) {
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
                let mut quic_reader = QuicReader::new(self.processors, receive_stream);
                let mut quic_writer = QuicWriter::new(send_stream, self.receiver);

                info!("Connected to node {}.", remote_node_id);

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
