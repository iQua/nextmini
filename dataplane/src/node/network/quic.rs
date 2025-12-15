use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use std::collections::VecDeque;
use std::io::{Error as IoError, ErrorKind, IoSlice};
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use s2n_quic::connection::Handle as QuicConnectionHandle;
use s2n_quic::provider::congestion_controller;

use s2n_quic::stream::{ReceiveStream, SendStream};
use s2n_quic::{Client, Server, client};
use tracing::{error, info, trace, warn};

use ahash::AHashMap;

use crate::node::FlowId;
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
                "Incoming connection from node {} (per-flow streams)...",
                remote_node_id
            );

            // splits connection into handle (for opening streams) and acceptor (for receiving).
            let (handle, mut acceptor) = connection.split();

            // spawns a task to accept incoming streams (for receive side).
            let proc_clone = processors.clone();
            tokio::spawn(async move {
                // processes the handshake stream for reading.
                let (receive_stream, _send_stream) = first_stream.split();
                let mut quic_reader = QuicReader::new(receive_stream, proc_clone.clone());
                tokio::spawn(async move {
                    quic_reader.run().await;
                });

                // accepts additional streams as they come.
                loop {
                    match acceptor.accept_bidirectional_stream().await {
                        Ok(Some(stream)) => {
                            let (receive_stream, _send_stream) = stream.split();
                            let mut reader = QuicReader::new(receive_stream, proc_clone.clone());
                            tokio::spawn(async move {
                                reader.run().await;
                            });
                        }
                        Ok(None) => {
                            info!("Connection closed by peer.");
                            break;
                        }
                        Err(e) => {
                            error!("Error accepting stream: {}", e);
                            break;
                        }
                    }
                }
            });

            // handles an inbound connection from a new client with per-flow writer.
            let network_interface = NetworkInterfaceHandle::new(
                config.clone(),
                NetworkStream::QuicPerFlow(handle),
                processors.clone(),
                self.reporter.clone(),
                remote_node_id,
            )
            .await;

            // creates the scheduler handle.
            let scheduler = SchedulerHandle::new(config.clone(), network_interface);

            // adds the scheduler to send packets to the new node.
            if let Err(e) = processors.add_node(remote_node_id, scheduler) {
                let remote_addr_for_log = remote_addr_snapshot
                    .as_ref()
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|_| "unknown:0".to_string());
                error!(
                    "Failed to add node {} with address {}: {}.",
                    remote_node_id, remote_addr_for_log, e
                );
                continue;
            }

            info!(
                "Connected to node {} with QUIC (per-flow streams).",
                remote_node_id
            );
        }
    }
}

pub struct QuicClient {
    pub config: LocalConfig,
}

impl QuicClient {
    /// Connects to a remote node and returns a connection Handle for per-flow stream creation.
    /// Opens one handshake stream to send the local node ID.
    /// Returns the connection Handle for dynamic stream creation per flow_id.
    pub async fn connect(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
        processors: ProcessorHandle,
    ) -> QuicConnectionHandle {
        let client = Client::builder()
            .with_tls(Path::new("server_cert.pem"))
            .expect("Failed to set TLS configuration")
            .with_io("0.0.0.0:0")
            .expect("Failed to bind the client")
            .start()
            .expect("Failed to start client");

        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;

        let connection = loop {
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

        info!(
            "Connecting to node {} with QUIC (per-flow streams)...",
            remote_node_id
        );

        // splits connection into handle (for sending) and acceptor (for receiving)
        let (mut handle, mut acceptor) = connection.split();

        // opens and sends handshake on first stream
        let mut handshake_stream = handle
            .open_bidirectional_stream()
            .await
            .expect("Failed to open handshake stream");

        let local_node_id = self.config.node_id;
        handshake_stream
            .send(Bytes::copy_from_slice(&local_node_id.to_be_bytes()))
            .await
            .expect("Failed to send local node id to the node");

        // closes our send direction for the handshake stream so the peer reader can exit cleanly
        let _ = handshake_stream.finish();

        // spawns receiver task to handle incoming streams
        tokio::spawn(async move {
            // processes handshake stream for reading
            let (receive_stream, _send_stream) = handshake_stream.split();
            let mut quic_reader = QuicReader::new(receive_stream, processors.clone());
            tokio::spawn(async move {
                quic_reader.run().await;
            });

            // accepts additional streams as they come
            loop {
                match acceptor.accept_bidirectional_stream().await {
                    Ok(Some(stream)) => {
                        let (receive_stream, _send_stream) = stream.split();
                        let mut reader = QuicReader::new(receive_stream, processors.clone());
                        tokio::spawn(async move {
                            reader.run().await;
                        });
                    }
                    Ok(None) => {
                        info!("Connection closed by peer.");
                        break;
                    }
                    Err(e) => {
                        error!("Error accepting stream: {}.", e);
                        break;
                    }
                }
            }
        });

        info!(
            "Connected to node {} with QUIC (per-flow streams).",
            remote_node_id
        );

        handle
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
        while let Ok(packet) = self.read_packet().await {
            self.processors.process_packet(packet).await;
        }

        // exits once the stream ends or an error occurs (e.g., peer reset)
        trace!("QUIC receive stream closed; reader task exiting");
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

/// Per-flow QUIC writer that dynamically creates one stream per flow_id.
/// Each unique flow_id gets its own dedicated QUIC stream for true HoL blocking avoidance.
pub struct QuicPerFlowWriter {
    /// Connection handle for opening new streams
    handle: QuicConnectionHandle,
    /// Map from flow_id to its dedicated send stream
    streams: AHashMap<FlowId, SendStream>,
    /// FIFO of stream usage for cheap eviction when too many flows accumulate
    lru: VecDeque<FlowId>,
}

impl QuicPerFlowWriter {
    // Limit open streams per connection to avoid exhausting stream credits / memory.
    const MAX_OPEN_STREAMS: usize = 1024;

    pub fn new(handle: QuicConnectionHandle) -> Self {
        Self {
            handle,
            streams: AHashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Gets or creates a stream for the given flow_id.
    /// Returns None if stream creation fails.
    async fn get_or_create_stream(&mut self, flow_id: FlowId) -> std::io::Result<&mut SendStream> {
        // Fast path: stream exists, mark recent use.
        if self.streams.contains_key(&flow_id) {
            self.mark_used(flow_id);
            // Safe unwrap; we just checked.
            return Ok(self.streams.get_mut(&flow_id).unwrap());
        }

        // evicts oldest if we're at capacity.
        if self.streams.len() >= Self::MAX_OPEN_STREAMS {
            if let Some(evicted_flow) = self.lru.pop_front() {
                if let Some(mut old_stream) = self.streams.remove(&evicted_flow) {
                    // Finish the send side to release stream credit; ignore errors.
                    if let Err(e) = old_stream.finish() {
                        warn!(flow_id = evicted_flow, error = %e, "Failed to finish QUIC stream during eviction");
                    }
                }
            }
        }

        // Create a fresh bidirectional stream for this flow.
        match self.handle.open_bidirectional_stream().await {
            Ok(stream) => {
                let (_recv, send) = stream.split();
                trace!(flow_id = flow_id, "Opened new QUIC stream for flow");
                self.streams.insert(flow_id, send);
                self.mark_used(flow_id);
                // Safe unwrap; just inserted.
                Ok(self.streams.get_mut(&flow_id).unwrap())
            }
            Err(e) => {
                error!(flow_id = flow_id, error = %e, "Failed to open QUIC stream for flow");
                Err(IoError::new(ErrorKind::Other, e.to_string()))
            }
        }
    }

    fn mark_used(&mut self, flow_id: FlowId) {
        // Remove any existing entry then push to back (most recently used).
        if let Some(pos) = self.lru.iter().position(|f| *f == flow_id) {
            self.lru.remove(pos);
        }
        self.lru.push_back(flow_id);
    }

    /// Writes packets to their respective per-flow streams.
    /// Creates new streams on-demand for new flow_ids.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // groups packets by flow_id
        let mut flow_packets: AHashMap<FlowId, Vec<Packet>> = AHashMap::new();
        for packet in packets {
            flow_packets.entry(packet.flow_id).or_default().push(packet);
        }

        // writes each group to its dedicated stream
        for (flow_id, packets) in flow_packets {
            let stream = self.get_or_create_stream(flow_id).await?;

            if let Err(e) = Self::write_packets_to_stream(stream, packets).await {
                // Drop the broken stream to allow future recreation.
                self.streams.remove(&flow_id);
                warn!(flow_id = flow_id, error = %e, "QUIC stream write failed; stream removed and packets not sent");
                return Err(e);
            }
        }

        Ok(())
    }

    /// Writes packets to a specific stream
    async fn write_packets_to_stream(stream: &mut SendStream, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // creates IoSlice objects from packet buffers.
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

            // advances the slices to skip the written data.
            IoSlice::advance_slices(&mut slices, written_this_call);
        }

        Ok(())
    }
}

impl Drop for QuicPerFlowWriter {
    fn drop(&mut self) {
        // Best-effort FIN on all active streams to free peer resources.
        let mut streams = std::mem::take(&mut self.streams);
        // Spawn async tasks because Drop cannot block; they will finish/close in the background.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for (flow_id, mut stream) in streams.drain() {
                    if let Err(e) = stream.finish() {
                        warn!(flow_id = flow_id, error = %e, "Failed to finish QUIC stream on drop");
                    }
                }
            });
        }
    }
}
