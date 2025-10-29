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
use crate::node::packet::Packet;
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

            if let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                let mut node_id_buf: [u8; 8] = [0; 8];

                if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                    info!("Failed to read node ID: {}", e);
                    connection.close(0u32.into());
                    return;
                }

                let remote_node_id = u64::from_be_bytes(node_id_buf) as usize;

                info!("Incoming connection from node {}...", remote_node_id);

                // handles an inbound connection from a new client
                let network_interface = NetworkInterfaceHandle::new(
                    config.clone(),
                    NetworkStream::Quic(stream),
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
                        "Failed to add node {} with address {}: {}",
                        remote_node_id, remote_addr_for_log, e
                    );
                    connection.close(0u32.into());
                    continue;
                }

                info!("Connected to node {} with QUIC.", remote_node_id);
            } else {
                connection.close(0u32.into());
            }
        }
    }
}

pub struct QuicClient {
    pub config: LocalConfig,
}

impl QuicClient {
    pub async fn connect(&self, remote_node_id: usize, remote_addr: &str) -> BidirectionalStream {
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

        let mut stream = connection
            .open_bidirectional_stream()
            .await
            .expect("Failed to establish handshake stream");

        info!("Connecting to node {} with QUIC...", remote_node_id);

        let local_node_id = self.config.node_id;

        stream
            .send(Bytes::copy_from_slice(&local_node_id.to_be_bytes()))
            .await
            .expect("Failed to send local node id to the node");

        info!("Connected to node {} with QUIC.", remote_node_id);

        stream
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
                self.processors.process_packet(packet);
            }
        }
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        let mut buf = vec![0; RECEIVE_BUF_SIZE];
        self.stream.read_exact(&mut buf[0..4]).await?;

        let msg_len = buf[2] as usize * 256 + buf[3] as usize;
        if !(20..=RECEIVE_BUF_SIZE).contains(&msg_len) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid IPv4 total length: {}", msg_len),
            ));
        }
        self.stream.read_exact(&mut buf[4..msg_len]).await?;

        Ok(Packet::new(msg_len, buf))
    }
}

/// An actor that writes packets to a QUIC stream.
pub struct QuicWriter {
    stream: SendStream,
}

impl QuicWriter {
    pub fn new(stream: SendStream) -> Self {
        Self { stream }
    }

    /// Writes multiple packets to the QUIC network stream.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // first creates IoSlice objects from packet buffers
        let mut io_slices: Vec<IoSlice> = packets
            .iter()
            .map(|packet| IoSlice::new(&packet.buf[0..packet.packet_size]))
            .collect();

        let mut slices = io_slices.as_mut_slice();

        while !slices.is_empty() {
            let written_this_call = self.stream.write_vectored(slices).await?;

            if written_this_call == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write_vectored returned 0",
                ));
            }

            // advances the slices to skip the written data
            IoSlice::advance_slices(&mut slices, written_this_call);
        }

        Ok(())
    }
}
