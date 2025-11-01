use std::io::Error;

use tokio::net::TcpStream;

use s2n_quic::stream::BidirectionalStream;

use nextmini_messages::Protocol;

use crate::node::NodeId;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::{ControllerReporterHandle, FlowMetric};
use crate::node::network::quic::{QuicClient, QuicReader, QuicWriter};
use crate::node::network::tcp::{TcpClient, TcpReader, TcpWriter};
use crate::node::network::udp::{UdpClient, UdpReader, UdpStream, UdpWriter};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub enum NetworkStream {
    Tcp(TcpStream),
    Quic(BidirectionalStream),
    Udp(UdpStream),
}

pub enum ProtocolWriter {
    Tcp(TcpWriter),
    Quic(QuicWriter),
    Udp(UdpWriter),
}

impl ProtocolWriter {
    /// Writes a vector of packets to the underlying protocol writer.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<(), Error> {
        match self {
            ProtocolWriter::Tcp(writer) => writer.write_packets(packets).await,
            ProtocolWriter::Quic(writer) => writer.write_packets(packets).await,
            ProtocolWriter::Udp(writer) => writer.write_packets(packets).await,
        }
    }
}

/// The network interface handle, used for sending and receiving packets over the network.
pub struct NetworkInterfaceHandle {
    local_id: NodeId,
    remote_node_id: NodeId,
    reporter: ControllerReporterHandle,
    pub writer: ProtocolWriter,
}

impl NetworkInterfaceHandle {
    /// Creates and runs a new network interface actor from an existing TCP or QUIC stream.
    pub async fn new(
        config: LocalConfig,
        stream: NetworkStream,
        processors: ProcessorHandle,
        reporter: ControllerReporterHandle,
        remote_node_id: NodeId,
    ) -> Self {
        // unlike a typical actor that uses a channel for sending messages to the network interface actor,
        // we directly return the protocol's writer (such as TcpWriter or QuicWriter) to the caller,
        // for the sake of improved performance and simplicity.
        let local_id = config.node_id;
        let network_interface = NetworkInterface::new(config, processors);

        let writer = network_interface.init(stream);

        Self {
            local_id,
            remote_node_id,
            reporter,
            writer,
        }
    }

    /// Creates and runs a new network interface actor as a client.
    pub async fn new_as_client(
        config: LocalConfig,
        remote_node_id: NodeId,
        remote_addr: String,
        processors: ProcessorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        let local_id = config.node_id;

        let mut network_interface = NetworkInterface::new(config, processors);

        // there is no need to call tokio::spawn here, as the reader task will be
        // spawned in init() itself
        let writer = network_interface
            .init_as_client(remote_node_id, remote_addr)
            .await;

        Self {
            local_id,
            remote_node_id,
            reporter,
            writer,
        }
    }

    // Sends packets in batch through the network interface.
    pub async fn send(&mut self, packets: Vec<Packet>) -> Result<(), Error> {
        let mut flow_metrics: Vec<FlowMetric> = Vec::new();

        for packet in packets.iter() {
            let metric = FlowMetric {
                flow_id: packet.flow_id,
                local_node_id: self.local_id,
                remote_node_id: self.remote_node_id,
                bytes: packet.packet_size,
            };

            flow_metrics.push(metric);
        }

        self.writer.write_packets(packets).await?;

        self.reporter.send(flow_metrics);

        Ok(())
    }
}

/// The network interface actor, used for sending and receiving packets over the network.
pub struct NetworkInterface {
    config: LocalConfig,
    processors: ProcessorHandle,
}

impl NetworkInterface {
    pub fn new(config: LocalConfig, processors: ProcessorHandle) -> Self {
        Self { config, processors }
    }

    pub async fn init_as_client(
        &mut self,
        remote_node_id: NodeId,
        remote_addr: String,
    ) -> ProtocolWriter {
        // connects to the remote node
        match self.config.protocol {
            Protocol::Tcp => {
                let tcp_client = TcpClient {
                    config: self.config.clone(),
                };

                let tcp_stream = tcp_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.init(NetworkStream::Tcp(tcp_stream))
            }
            Protocol::Quic => {
                let quic_client = QuicClient {
                    config: self.config.clone(),
                };

                let quic_stream = quic_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.init(NetworkStream::Quic(quic_stream))
            }
            Protocol::Udp => {
                let udp_client = UdpClient {
                    config: self.config.clone(),
                };

                let udp_stream = udp_client
                    .connect(remote_node_id, remote_addr.as_str())
                    .await;

                self.init(NetworkStream::Udp(udp_stream))
            }
        }
    }

    pub fn init(&self, stream: NetworkStream) -> ProtocolWriter {
        match stream {
            NetworkStream::Tcp(stream) => {
                let (reader, writer) = tokio::io::split(stream);

                let tcp_reader = TcpReader::new(reader, self.processors.clone());
                let tcp_writer = TcpWriter::new(writer);

                tokio::spawn(async move {
                    tcp_reader.run().await;
                });

                ProtocolWriter::Tcp(tcp_writer)
            }
            NetworkStream::Quic(stream) => {
                let (receive_stream, send_stream) = stream.split();

                let mut quic_reader = QuicReader::new(receive_stream, self.processors.clone());
                let quic_writer = QuicWriter::new(send_stream);

                tokio::spawn(async move {
                    quic_reader.run().await;
                });

                ProtocolWriter::Quic(quic_writer)
            }
            NetworkStream::Udp(udp_stream) => {
                let mut udp_reader = UdpReader::new(udp_stream.receiver, self.processors.clone());
                let udp_writer = UdpWriter::new(udp_stream.socket, udp_stream.remote_addr);

                tokio::spawn(async move {
                    udp_reader.run().await;
                });

                ProtocolWriter::Udp(udp_writer)
            }
        }
    }
}
