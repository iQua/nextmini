use std::io::Error;

use tokio::net::TcpStream;

use s2n_quic::stream::BidirectionalStream;

use nextmini_messages::Protocol;

use crate::node::NodeId;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::{ControllerReporterHandle, FlowMetric};
use crate::node::network::quic::{QuicClient, QuicReader, QuicWriter};
#[cfg(any(feature = "quic_datagram", feature = "quic_per_flow"))]
use crate::node::network::quic::QuicConnectOutcome;
#[cfg(feature = "quic_per_flow")]
use crate::node::network::quic::QuicMuxWriter;
#[cfg(feature = "quic_datagram")]
use crate::node::network::quic::{QuicDatagramReader, QuicDatagramWriter};
use crate::node::network::tcp::{TcpClient, TcpReader, TcpWriter};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;

pub enum NetworkStream {
    Tcp(TcpStream),
    Quic(BidirectionalStream),
    #[cfg(feature = "quic_per_flow")]
    QuicConn(s2n_quic::connection::Handle),
    #[cfg(feature = "quic_per_flow")]
    QuicConnAccept(s2n_quic::connection::Handle, s2n_quic::connection::StreamAcceptor),
    #[cfg(feature = "quic_datagram")]
    QuicDatagram(s2n_quic::connection::Handle),
}

pub enum ProtocolWriter {
    Tcp(TcpWriter),
    Quic(QuicWriter),
    #[cfg(feature = "quic_per_flow")]
    QuicMux(QuicMuxWriter),
    #[cfg(feature = "quic_datagram")]
    QuicDatagram(QuicDatagramWriter),
}

impl ProtocolWriter {
    /// Writes a vector of packets to the underlying protocol writer.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<(), Error> {
        match self {
            ProtocolWriter::Tcp(writer) => writer.write_packets(packets).await,
            ProtocolWriter::Quic(writer) => writer.write_packets(packets).await,
            #[cfg(feature = "quic_per_flow")]
            ProtocolWriter::QuicMux(writer) => writer.write_packets(packets).await,
            #[cfg(feature = "quic_datagram")]
            ProtocolWriter::QuicDatagram(writer) => writer.write_packets(packets).await,
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

                #[cfg(any(feature = "quic_datagram", feature = "quic_per_flow"))]
                {
                    let outcome = quic_client
                        .connect_unified(remote_node_id, remote_addr.as_str())
                        .await;
                    match outcome {
                        QuicConnectOutcome::SingleStream(s) => self.init(NetworkStream::Quic(s)),
                        #[cfg(feature = "quic_per_flow")]
                        QuicConnectOutcome::PerFlow(h, a) => {
                            self.init(NetworkStream::QuicConnAccept(h, a))
                        }
                        #[cfg(feature = "quic_datagram")]
                        QuicConnectOutcome::Datagram(h, _) => {
                            self.init(NetworkStream::QuicDatagram(h))
                        }
                    }
                }
                #[cfg(all(not(feature = "quic_datagram"), not(feature = "quic_per_flow")))]
                {
                    let quic_stream = quic_client
                        .connect(remote_node_id, remote_addr.as_str())
                        .await;
                    self.init(NetworkStream::Quic(quic_stream))
                }
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
            #[cfg(feature = "quic_per_flow")]
            NetworkStream::QuicConn(handle) => {
                // No reader spawned here; the server runs a global accept loop spawning
                // per-flow readers. We only need a per-flow multiplexing writer.
                let quic_writer = QuicMuxWriter::new(handle);
                ProtocolWriter::QuicMux(quic_writer)
            }
            #[cfg(feature = "quic_per_flow")]
            NetworkStream::QuicConnAccept(handle, mut acceptor) => {
                let processors = self.processors.clone();
                tokio::spawn(async move {
                    loop {
                        match acceptor.accept_bidirectional_stream().await {
                            Ok(Some(stream)) => {
                                let (rx, _tx) = stream.split();
                                let mut reader = QuicReader::new(rx, processors.clone());
                                tokio::spawn(async move {
                                    reader.run().await;
                                });
                            }
                            Ok(None) => break,
                            Err(e) => {
                                tracing::error!("client accept error: {}", e);
                                break;
                            }
                        }
                    }
                });
                let quic_writer = QuicMuxWriter::new(handle);
                ProtocolWriter::QuicMux(quic_writer)
            }
            #[cfg(feature = "quic_datagram")]
            NetworkStream::QuicDatagram(handle) => {
                let mut dgram_reader = QuicDatagramReader::new(handle.clone(), self.processors.clone());
                let dgram_writer = QuicDatagramWriter::new(handle);

                tokio::spawn(async move {
                    dgram_reader.run().await;
                });

                ProtocolWriter::QuicDatagram(dgram_writer)
            }
        }
    }
}
