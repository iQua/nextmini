use std::io::Error;

use tokio::net::TcpStream;

use s2n_quic::stream::BidirectionalStream;

use nextmini_messages::Protocol;

use crate::node::NodeId;
use crate::node::config::LocalConfig;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::quic::{QuicClient, QuicReader, QuicWriter};
use crate::node::tcp::{TcpClient, TcpReader, TcpWriter};
use crate::node::udp::UdpWriter;

pub enum NetworkStream {
    Tcp(TcpStream),
    Quic(BidirectionalStream),
}

pub enum ProtocolWriter {
    Tcp(TcpWriter),
    Udp(UdpWriter),
    Quic(QuicWriter),
}

impl ProtocolWriter {
    /// Writes a vector of packets to the underlying protocol writer.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<(), Error> {
        match self {
            ProtocolWriter::Tcp(writer) => writer.write_packets(packets).await,
            ProtocolWriter::Udp(writer) => writer.write_packets(packets).await,
            ProtocolWriter::Quic(writer) => writer.write_packets(packets).await,
        }
    }
}

/// The network interface handle, used for sending and receiving packets over the network.
pub struct NetworkInterfaceHandle {
    pub writer: ProtocolWriter,
}

impl NetworkInterfaceHandle {
    /// Creates and runs a new network interface actor from an existing TCP or QUIC stream.
    pub async fn new(
        config: LocalConfig,
        stream: NetworkStream,
        processors: ProcessorHandle,
    ) -> Self {
        // unlike a typical actor that uses a channel for sending messages to the network interface actor,
        // we directly return the protocol's writer (such as TcpWriter or QuicWriter) to the caller,
        // for the sake of improved performance and simplicity.
        let network_interface = NetworkInterface { config, processors };

        let writer = network_interface.init(stream);

        Self { writer }
    }

    /// Creates and runs a new network interface actor as a client.
    pub async fn new_as_client(
        config: LocalConfig,
        remote_node_id: NodeId,
        remote_addr: String,
        processors: ProcessorHandle,
    ) -> Self {
        let network_interface = NetworkInterface { config, processors };

        // there is no need to call tokio::spawn here, as the reader task will be
        // spawned in init() itself
        let writer = network_interface
            .init_as_client(remote_node_id, remote_addr)
            .await;

        Self { writer }
    }

    // Sends packets in batch through the network interface.
    pub async fn send(&mut self, packets: Vec<Packet>) -> Result<(), Error> {
        let _ = self.writer.write_packets(packets).await?;

        Ok(())
    }
}

/// The network interface actor, used for sending and receiving packets over the network.
pub struct NetworkInterface {
    config: LocalConfig,
    processors: ProcessorHandle,
}

impl NetworkInterface {
    pub async fn init_as_client(
        self,
        remote_node_id: NodeId,
        remote_addr: String,
    ) -> ProtocolWriter {
        // connects to the remote node
        match self.config.protocol {
            Protocol::Tcp => {
                let tcp_client = TcpClient {
                    config: self.config.clone(),
                };

                let stream = tcp_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.init(NetworkStream::Tcp(stream))
            }
            Protocol::Quic => {
                let quic_client = QuicClient {
                    config: self.config.clone(),
                };

                let stream = quic_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.init(NetworkStream::Quic(stream))
            }
            Protocol::Udp => {
                let udp_writer = UdpWriter::new(self.config.clone(), remote_addr);

                ProtocolWriter::Udp(udp_writer)
            }
        }
    }

    pub fn init(self, stream: NetworkStream) -> ProtocolWriter {
        match stream {
            NetworkStream::Tcp(stream) => {
                let (reader, writer) = tokio::io::split(stream);

                let tcp_reader = TcpReader::new(reader, self.processors);
                let tcp_writer = TcpWriter::new(writer);

                tokio::spawn(async move {
                    tcp_reader.run().await;
                });

                ProtocolWriter::Tcp(tcp_writer)
            }
            NetworkStream::Quic(stream) => {
                let (receive_stream, send_stream) = stream.split();

                let mut quic_reader = QuicReader::new(receive_stream, self.processors);
                let quic_writer = QuicWriter::new(send_stream);

                tokio::spawn(async move {
                    quic_reader.run().await;
                });

                ProtocolWriter::Quic(quic_writer)
            }
        }
    }
}
