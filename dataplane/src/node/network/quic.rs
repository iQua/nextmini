use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use std::io::IoSlice;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use s2n_quic::provider::congestion_controller;
use s2n_quic::stream::BidirectionalStream;
use s2n_quic::stream::{ReceiveStream, SendStream};
use s2n_quic::{Client, Server, client};
use tracing::{error, info};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::CongestionControl;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::{Packet, PacketBuf};
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;

pub struct QuicServer {
    config: LocalConfig,
    processors: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl QuicServer {
    pub fn new(
        config: LocalConfig,
        processors: ProcessorHandle,
        reporter: ControllerReporterHandle,
    ) -> Self {
        Self {
            config,
            processors,
            reporter,
        }
    }

    pub async fn start_listening(&mut self, addr: &str) {
        let server_addr: SocketAddr = addr.parse().unwrap();

        let mut server = match self.config.quic_congestion_control {
            CongestionControl::Cubic => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Cubic::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
            CongestionControl::Bbr => Server::builder()
                .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                .expect("Failed to set TLS config")
                .with_congestion_controller(congestion_controller::Bbr::default())
                .expect("Failed to set congestion controller")
                .with_io(server_addr)
                .expect("Failed to bind to address")
                .start()
                .expect("Failed to start server"),
        };

        while let Some(mut connection) = server.accept().await {
            let _ = connection.keep_alive(true);
            let config = self.config.clone();
            let processors = self.processors.clone();
            let num_streams = config.num_quic_streams.max(1);

            let remote_addr_snapshot = connection.remote_addr();
            info!("Connection accepted from {:?}.", remote_addr_snapshot);

            // accepts the first stream and reads the handshake (node ID)
            let first_stream = match connection.accept_bidirectional_stream().await {
                Ok(Some(stream)) => stream,
                Ok(None) => {
                    info!("Connection closed before first stream.");
                    connection.close(0u32.into());
                    continue;
                }
                Err(e) => {
                    info!("Failed to accept first stream: {}.", e);
                    connection.close(0u32.into());
                    continue;
                }
            };

            let mut node_id_buf: [u8; 8] = [0; 8];
            let mut first_stream = first_stream;

            if let Err(e) = first_stream.read_exact(&mut node_id_buf).await {
                info!("Failed to read node ID: {}.", e);
                connection.close(0u32.into());
                continue;
            }

            let remote_node_id = u64::from_be_bytes(node_id_buf) as usize;

            info!(
                "Incoming connection from node {} ({} streams expected)...",
                remote_node_id, num_streams
            );

            // collects all streams (first one and additional ones)
            let mut streams = Vec::with_capacity(num_streams);
            streams.push(first_stream);

            // accepts remaining streams
            for i in 1..num_streams {
                match connection.accept_bidirectional_stream().await {
                    Ok(Some(stream)) => {
                        streams.push(stream);
                    }
                    Ok(None) => {
                        error!(
                            "Connection closed before all streams accepted (got {}/{}).",
                            i, num_streams
                        );
                        connection.close(0u32.into());
                        continue;
                    }
                    Err(e) => {
                        error!(
                            "Failed to accept stream {} of {}: {}",
                            i, num_streams, e
                        );
                        connection.close(0u32.into());
                        continue;
                    }
                }
            }

            // handles an inbound connection from a new client
            let network_interface = NetworkInterfaceHandle::new(
                config.clone(),
                NetworkStream::Quic(streams),
                processors.clone(),
                self.reporter.clone(),
                remote_node_id,
            )
            .await;

            // creates the scheduler handle
            let scheduler = SchedulerHandle::new(config.clone(), network_interface);

            // adds the scheduler to send packets to the new node
            if let Err(e) = processors.add_node(remote_node_id, scheduler) {
                let remote_addr_for_log = remote_addr_snapshot
                    .as_ref()
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|_| "unknown:0".to_string());
                error!(
                    "Failed to add node {} with address {}: {}.",
                    remote_node_id, remote_addr_for_log, e
                );
                connection.close(0u32.into());
                continue;
            }

            info!(
                "Connected to node {} with QUIC ({} streams).",
                remote_node_id, num_streams
            );
        }
    }
}

pub struct QuicClient {
    pub config: LocalConfig,
}

impl QuicClient {
    /// Connects to a remote node and opens `num_quic_streams` bidirectional streams.
    /// The first stream is used for the handshake (sending local node ID).
    /// Returns a Vec of streams for multi-stream packet dispatch.
    pub async fn connect(&self, remote_node_id: usize, remote_addr: &str) -> Vec<BidirectionalStream> {
        let client = Client::builder()
            .with_tls(Path::new("server_cert.pem"))
            .expect("Failed to set TLS configuration")
            .with_io("0.0.0.0:0")
            .expect("Failed to bind the client")
            .start()
            .expect("Failed to start client");

        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;

        let mut connection = loop {
            let addr: SocketAddr = remote_addr.parse().unwrap();
            let connect = client::Connect::new(addr).with_server_name("Nextmini");

            match client.connect(connect).await {
                Ok(mut connection) => {
                    connection
                        .keep_alive(true)
                        .expect("Unable to keep the connection alive");
                    break connection;
                }
                Err(e) => {
                    info!(
                        "Failed to initiate quic connection to {addr}, error: {e} retrying in 1 second"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }

            retry_count += 1;

            if retry_count >= MAX_RETRY {
                panic!(
                    "Maximum retry reached to establish a QUIC connection to {}. Aborting.",
                    addr
                );
            }
        };

        let num_streams = self.config.num_quic_streams.max(1);
        let mut streams = Vec::with_capacity(num_streams);

        // Open all streams
        for i in 0..num_streams {
            let stream = connection
                .open_bidirectional_stream()
                .await
                .expect(&format!("Failed to open QUIC stream {}", i));
            streams.push(stream);
        }

        info!(
            "Connecting to node {} with QUIC ({} streams)...",
            remote_node_id, num_streams
        );

        // Send handshake on the first stream
        let local_node_id = self.config.node_id;
        streams[0]
            .send(Bytes::copy_from_slice(&local_node_id.to_be_bytes()))
            .await
            .expect("Failed to send local node id to the node");

        info!(
            "Connected to node {} with QUIC ({} streams).",
            remote_node_id, num_streams
        );

        streams
    }
}

/// An actor that reads packets from a QUIC stream.
pub struct QuicReader {
    stream: ReceiveStream,
    processors: ProcessorHandle,
}

impl QuicReader {
    pub fn new(stream: ReceiveStream, processors: ProcessorHandle) -> Self {
        Self { processors, stream }
    }

    pub async fn run(&mut self) {
        loop {
            if let Ok(packet) = self.read_packet().await {
                self.processors.process_packet(packet).await;
            }
        }
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = PacketBuf::new();
        buf.prepare_uninit(4);
        self.stream.read_exact(buf.as_mut_slice()).await?;

        let header = buf.as_slice();
        let msg_len = header[2] as usize * 256 + header[3] as usize;
        if !(20..=RECEIVE_BUF_SIZE).contains(&msg_len) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid IPv4 total length: {}", msg_len),
            ));
        }
        buf.prepare_uninit(msg_len);
        self.stream
            .read_exact(&mut buf.as_mut_slice()[4..msg_len])
            .await?;

        Ok(Packet::new(msg_len, buf))
    }
}

/// An actor that writes packets to multiple QUIC streams with flow-based dispatch.
/// Uses hash(flow_id) % num_streams to select which stream to use for each packet.
pub struct QuicMultiWriter {
    streams: Vec<SendStream>,
}

impl QuicMultiWriter {
    pub fn new(streams: Vec<SendStream>) -> Self {
        Self { streams }
    }

    /// Selects a stream index based on flow_id using simple modulo hash
    #[inline]
    fn select_stream(&self, flow_id: u128, num_streams: usize) -> usize {
        (flow_id as usize) % num_streams
    }

    /// Writes multiple packets to the appropriate QUIC streams based on flow_id.
    /// Packets are grouped by their target stream and written in PARALLEL.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        let num_streams = self.streams.len();

        // For single stream, use the simple fast path
        if num_streams == 1 {
            return Self::write_packets_to_stream(&mut self.streams[0], packets).await;
        }

        // Group packets by target stream
        let mut stream_packets: Vec<Vec<Packet>> = vec![Vec::new(); num_streams];
        for packet in packets {
            let stream_idx = self.select_stream(packet.flow_id, num_streams);
            stream_packets[stream_idx].push(packet);
        }

        // Write to ALL streams in PARALLEL using futures
        let write_futures: Vec<_> = self.streams
            .iter_mut()
            .zip(stream_packets.into_iter())
            .filter_map(|(stream, packets)| {
                if packets.is_empty() {
                    None
                } else {
                    Some(Self::write_packets_to_stream(stream, packets))
                }
            })
            .collect();

        // Wait for all writes to complete
        let results = futures::future::join_all(write_futures).await;
        
        // Check for any errors
        for result in results {
            result?;
        }

        Ok(())
    }

    /// Writes packets to a specific stream (static method to avoid borrow issues)
    async fn write_packets_to_stream(stream: &mut SendStream, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // Creates IoSlice objects from packet buffers
        let mut io_slices: Vec<IoSlice> = packets
            .iter()
            .map(|packet| IoSlice::new(packet.bytes()))
            .collect();

        let mut slices = io_slices.as_mut_slice();

        while !slices.is_empty() {
            let written_this_call = stream.write_vectored(slices).await?;

            if written_this_call == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write_vectored returned 0",
                ));
            }

            // Advances the slices to skip the written data
            IoSlice::advance_slices(&mut slices, written_this_call);
        }

        Ok(())
    }
}
