use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use std::io::IoSlice;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "quic_per_flow")]
use s2n_quic::connection::{Handle as QuicHandle, StreamAcceptor};
use s2n_quic::provider::congestion_controller;
#[cfg(feature = "quic_datagram")]
use s2n_quic::provider::datagram::default::{
    Endpoint as DatagramEndpoint, Receiver as DatagramReceiver, Sender as DatagramSender,
};
use s2n_quic::provider::limits::Limits;
use s2n_quic::stream::BidirectionalStream;
use s2n_quic::stream::{ReceiveStream, SendStream};
use s2n_quic::{Client, Server, client};
use tracing::{error, info, warn};

// Helper: spawn an ack‑eliciting heartbeat on a QUIC send stream.
#[cfg(feature = "quic_datagram")]
fn spawn_quic_stream_heartbeat(mut tx: SendStream, interval: Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            // 1-byte write is enough to elicit ACKs and reset idle timeout.
            if let Err(e) = tx.send(Bytes::from_static(&[0u8])).await {
                warn!("QUIC heartbeat stopped (send failed): {}", e);
                break;
            }
        }
    });
}

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::CongestionControl as CcMode;
use crate::node::config::{LocalConfig, QuicTransportMode};
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;
#[cfg(feature = "quic_per_flow")]
use ahash::AHashMap;
#[cfg(feature = "quic_per_flow")]
use tokio::sync::mpsc;
// no JoinHandle needed

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
            CcMode::Cubic => {
                let builder = Server::builder()
                    .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                    .expect("Failed to set TLS config")
                    .with_congestion_controller(congestion_controller::Cubic::default())
                    .expect("Failed to set congestion controller")
                    .with_io(server_addr)
                    .expect("Failed to bind to address");

                let builder = {
                    #[cfg(feature = "quic_datagram")]
                    {
                        let dgram = DatagramEndpoint::builder()
                            .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                            .expect("Failed to set datagram recv capacity")
                            .with_send_capacity(self.config.quic_dgram_send_capacity)
                            .expect("Failed to set datagram send capacity")
                            .build()
                            .expect("Failed to build datagram endpoint");
                        builder
                            .with_datagram(dgram)
                            .expect("Failed to enable datagrams on server")
                    }
                    #[cfg(not(feature = "quic_datagram"))]
                    {
                        builder
                    }
                };

                let builder = builder
                    .with_limits({
                        let limits = Limits::default();
                        let limits = limits
                            .with_data_window(64 * 1024 * 1024)
                            .expect("invalid data window");
                        let limits = limits
                            .with_bidirectional_local_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi local window");
                        let limits = limits
                            .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi remote window");
                        limits
                            .with_max_send_buffer_size(64 * 1024 * 1024)
                            .expect("invalid send buffer")
                    })
                    .expect("Failed to set QUIC limits");

                builder.start().expect("Failed to start server")
            }
            CcMode::Bbr => {
                let builder = Server::builder()
                    .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                    .expect("Failed to set TLS config")
                    .with_congestion_controller(congestion_controller::Bbr::default())
                    .expect("Failed to set congestion controller")
                    .with_io(server_addr)
                    .expect("Failed to bind to address");

                let builder = {
                    #[cfg(feature = "quic_datagram")]
                    {
                        let dgram = DatagramEndpoint::builder()
                            .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                            .expect("Failed to set datagram recv capacity")
                            .with_send_capacity(self.config.quic_dgram_send_capacity)
                            .expect("Failed to set datagram send capacity")
                            .build()
                            .expect("Failed to build datagram endpoint");
                        builder
                            .with_datagram(dgram)
                            .expect("Failed to enable datagrams on server")
                    }
                    #[cfg(not(feature = "quic_datagram"))]
                    {
                        builder
                    }
                };

                let builder = builder
                    .with_limits({
                        let limits = Limits::default();
                        let limits = limits
                            .with_data_window(64 * 1024 * 1024)
                            .expect("invalid data window");
                        let limits = limits
                            .with_bidirectional_local_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi local window");
                        let limits = limits
                            .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi remote window");
                        limits
                            .with_max_send_buffer_size(64 * 1024 * 1024)
                            .expect("invalid send buffer")
                    })
                    .expect("Failed to set QUIC limits");

                builder.start().expect("Failed to start server")
            }
            CcMode::Disabled => {
                let builder = Server::builder()
                    .with_tls((Path::new("server_cert.pem"), Path::new("server_key.pem")))
                    .expect("Failed to set TLS config")
                    .with_congestion_controller({
                        #[cfg(feature = "quic_no_cc")]
                        {
                            no_cc::NoopCcEndpoint
                        }
                        #[cfg(not(feature = "quic_no_cc"))]
                        {
                            warn!("quic_no_cc not compiled; falling back to BBR");
                            congestion_controller::Bbr::default()
                        }
                    })
                    .expect("Failed to set congestion controller")
                    .with_io(server_addr)
                    .expect("Failed to bind to address");

                let builder = {
                    #[cfg(feature = "quic_datagram")]
                    {
                        let dgram = DatagramEndpoint::builder()
                            .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                            .expect("Failed to set datagram recv capacity")
                            .with_send_capacity(self.config.quic_dgram_send_capacity)
                            .expect("Failed to set datagram send capacity")
                            .build()
                            .expect("Failed to build datagram endpoint");
                        builder
                            .with_datagram(dgram)
                            .expect("Failed to enable datagrams on server")
                    }
                    #[cfg(not(feature = "quic_datagram"))]
                    {
                        builder
                    }
                };

                let builder = builder
                    .with_limits({
                        let limits = Limits::default();
                        let limits = limits
                            .with_data_window(64 * 1024 * 1024)
                            .expect("invalid data window");
                        let limits = limits
                            .with_bidirectional_local_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi local window");
                        let limits = limits
                            .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi remote window");
                        limits
                            .with_max_send_buffer_size(64 * 1024 * 1024)
                            .expect("invalid send buffer")
                    })
                    .expect("Failed to set QUIC limits");

                builder.start().expect("Failed to start server")
            }
        };

        while let Some(mut connection) = server.accept().await {
            // Ensure server side also emits keepalive PINGs if the peer goes quiet.
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

                // Choose transport mode by runtime config; fall back gracefully if the compiled
                // features don't support the selected mode.
                let network_interface = match config.quic_transport_mode {
                    QuicTransportMode::SingleStream => {
                        NetworkInterfaceHandle::new(
                            config.clone(),
                            NetworkStream::Quic(stream),
                            processors.clone(),
                            self.reporter.clone(),
                            remote_node_id,
                        )
                        .await
                    }
                    QuicTransportMode::PerFlow => {
                        #[cfg(feature = "quic_per_flow")]
                        {
                            let (handle, mut acceptor): (QuicHandle, StreamAcceptor) =
                                connection.split();

                            let proc_clone = processors.clone();
                            tokio::spawn(async move {
                                loop {
                                    match acceptor.accept_bidirectional_stream().await {
                                        Ok(Some(stream)) => {
                                            let (rx, _tx) = stream.split();
                                            let mut reader =
                                                QuicReader::new(rx, proc_clone.clone());
                                            tokio::spawn(async move {
                                                reader.run().await;
                                            });
                                        }
                                        Ok(None) => break,
                                        Err(e) => {
                                            error!("Error accepting QUIC stream: {}", e);
                                            break;
                                        }
                                    }
                                }
                            });

                            NetworkInterfaceHandle::new(
                                config.clone(),
                                NetworkStream::QuicConn(handle),
                                processors.clone(),
                                self.reporter.clone(),
                                remote_node_id,
                            )
                            .await
                        }
                        #[cfg(not(feature = "quic_per_flow"))]
                        {
                            warn!("quic_per_flow not compiled; falling back to single stream mode");
                            NetworkInterfaceHandle::new(
                                config.clone(),
                                NetworkStream::Quic(stream),
                                processors.clone(),
                                self.reporter.clone(),
                                remote_node_id,
                            )
                            .await
                        }
                    }
                    QuicTransportMode::Datagram => {
                        #[cfg(feature = "quic_datagram")]
                        {
                            // Keep the handshake stream open and send periodic ack‑eliciting heartbeats.
                            let (_rx, tx) = stream.split();
                            spawn_quic_stream_heartbeat(
                                tx,
                                Duration::from_secs(config.quic_dgram_heartbeat_secs),
                            );
                            let handle = connection.handle();
                            let mut dgram_reader =
                                QuicDatagramReader::new(handle.clone(), processors.clone());
                            tokio::spawn(async move {
                                dgram_reader.run().await;
                            });

                            NetworkInterfaceHandle::new(
                                config.clone(),
                                NetworkStream::QuicDatagram(handle),
                                processors.clone(),
                                self.reporter.clone(),
                                remote_node_id,
                            )
                            .await
                        }
                        #[cfg(not(feature = "quic_datagram"))]
                        {
                            warn!("quic_datagram not compiled; falling back to single stream mode");
                            NetworkInterfaceHandle::new(
                                config.clone(),
                                NetworkStream::Quic(stream),
                                processors.clone(),
                                self.reporter.clone(),
                                remote_node_id,
                            )
                            .await
                        }
                    }
                };

                // creates the scheduler handle
                let scheduler = SchedulerHandle::new(config.clone(), network_interface);

                // adds the scheduler to send packets to the new node
                if let Err(e) = processors.add_node(remote_node_id, scheduler) {
                    let remote_addr_for_log =
                        remote_addr_snapshot.unwrap_or_else(|_| "unknown:0".parse().unwrap());
                    error!(
                        "Failed to add node {} with address {}: {}",
                        remote_node_id, remote_addr_for_log, e
                    );
                    #[cfg(not(feature = "quic_per_flow"))]
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

#[cfg(any(feature = "quic_datagram", feature = "quic_per_flow"))]
pub enum QuicConnectOutcome {
    SingleStream(BidirectionalStream),
    #[cfg(feature = "quic_per_flow")]
    PerFlow(
        s2n_quic::connection::Handle,
        s2n_quic::connection::StreamAcceptor,
    ),
    #[cfg(feature = "quic_datagram")]
    Datagram(s2n_quic::connection::Handle, ()),
}

impl QuicClient {
    // Backward-compatible single-stream connect
    #[cfg(all(not(feature = "quic_datagram"), not(feature = "quic_per_flow")))]
    pub async fn connect(&self, remote_node_id: usize, remote_addr: &str) -> BidirectionalStream {
        self.connect_single(remote_node_id, remote_addr).await
    }

    // Always-available single-stream connect used for SingleStream mode or fallback
    async fn connect_single(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
    ) -> BidirectionalStream {
        let client = Client::builder()
            // For clients, configure the trusted server certificate instead of presenting one.
            .with_tls(Path::new("server_cert.pem"))
            .expect("Failed to set TLS configuration")
            .with_io("0.0.0.0:0")
            .expect("Failed to bind the client")
            .with_limits({
                let limits = Limits::default();
                let limits = limits
                    .with_data_window(64 * 1024 * 1024)
                    .expect("invalid data window");
                let limits = limits
                    .with_bidirectional_local_data_window(64 * 1024 * 1024)
                    .expect("invalid bidi local window");
                let limits = limits
                    .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                    .expect("invalid bidi remote window");
                limits
                    .with_max_send_buffer_size(64 * 1024 * 1024)
                    .expect("invalid send buffer")
            })
            .expect("Failed to set QUIC limits")
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

    #[cfg(feature = "quic_per_flow")]
    pub async fn connect_per_flow(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
    ) -> (QuicHandle, StreamAcceptor) {
        let client = match self.config.quic_congestion_control {
            CcMode::Cubic => Client::builder()
                .with_tls(Path::new("server_cert.pem"))
                .expect("Failed to set TLS configuration")
                .with_congestion_controller(congestion_controller::Cubic::default())
                .expect("Failed to set congestion controller")
                .with_io("0.0.0.0:0")
                .expect("Failed to bind the client")
                .with_limits({
                    let limits = Limits::default();
                    let limits = limits
                        .with_data_window(64 * 1024 * 1024)
                        .expect("invalid data window");
                    let limits = limits
                        .with_bidirectional_local_data_window(64 * 1024 * 1024)
                        .expect("invalid bidi local window");
                    let limits = limits
                        .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                        .expect("invalid bidi remote window");
                    limits
                        .with_max_send_buffer_size(64 * 1024 * 1024)
                        .expect("invalid send buffer")
                })
                .expect("Failed to set QUIC limits")
                .start()
                .expect("Failed to start client"),
            CcMode::Bbr => Client::builder()
                .with_tls(Path::new("server_cert.pem"))
                .expect("Failed to set TLS configuration")
                .with_congestion_controller(congestion_controller::Bbr::default())
                .expect("Failed to set congestion controller")
                .with_io("0.0.0.0:0")
                .expect("Failed to bind the client")
                .with_limits({
                    let limits = Limits::default();
                    let limits = limits
                        .with_data_window(64 * 1024 * 1024)
                        .expect("invalid data window");
                    let limits = limits
                        .with_bidirectional_local_data_window(64 * 1024 * 1024)
                        .expect("invalid bidi local window");
                    let limits = limits
                        .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                        .expect("invalid bidi remote window");
                    limits
                        .with_max_send_buffer_size(64 * 1024 * 1024)
                        .expect("invalid send buffer")
                })
                .expect("Failed to set QUIC limits")
                .start()
                .expect("Failed to start client"),
            CcMode::Disabled => {
                #[cfg(feature = "quic_no_cc")]
                {
                    Client::builder()
                        .with_tls(Path::new("server_cert.pem"))
                        .expect("Failed to set TLS configuration")
                        .with_congestion_controller(no_cc::NoopCcEndpoint)
                        .expect("Failed to set congestion controller")
                        .with_io("0.0.0.0:0")
                        .expect("Failed to bind the client")
                        .with_limits({
                            let limits = Limits::default();
                            let limits = limits
                                .with_data_window(64 * 1024 * 1024)
                                .expect("invalid data window");
                            let limits = limits
                                .with_bidirectional_local_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi local window");
                            let limits = limits
                                .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi remote window");
                            limits
                                .with_max_send_buffer_size(64 * 1024 * 1024)
                                .expect("invalid send buffer")
                        })
                        .expect("Failed to set QUIC limits")
                        .start()
                        .expect("Failed to start client")
                }
                #[cfg(not(feature = "quic_no_cc"))]
                {
                    warn!("quic_no_cc not compiled; falling back to BBR");
                    Client::builder()
                        .with_tls(Path::new("server_cert.pem"))
                        .expect("Failed to set TLS configuration")
                        .with_congestion_controller(congestion_controller::Bbr::default())
                        .expect("Failed to set congestion controller")
                        .with_io("0.0.0.0:0")
                        .expect("Failed to bind the client")
                        .with_limits({
                            let limits = Limits::default();
                            let limits = limits
                                .with_data_window(64 * 1024 * 1024)
                                .expect("invalid data window");
                            let limits = limits
                                .with_bidirectional_local_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi local window");
                            let limits = limits
                                .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi remote window");
                            limits
                                .with_max_send_buffer_size(64 * 1024 * 1024)
                                .expect("invalid send buffer")
                        })
                        .expect("Failed to set QUIC limits")
                        .start()
                        .expect("Failed to start client")
                }
            }
        };

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

        // Split for handle + per-flow acceptor after handshake
        let (handle, acceptor) = connection.split();
        (handle, acceptor)
    }

    #[cfg(feature = "quic_datagram")]
    pub async fn connect_datagram(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
    ) -> s2n_quic::connection::Handle {
        let client = match self.config.quic_congestion_control {
            CcMode::Cubic => {
                let dgram = DatagramEndpoint::builder()
                    .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                    .expect("Failed to set datagram recv capacity")
                    .with_send_capacity(self.config.quic_dgram_send_capacity)
                    .expect("Failed to set datagram send capacity")
                    .build()
                    .expect("Failed to build datagram endpoint");
                let builder = Client::builder()
                    .with_tls(Path::new("server_cert.pem"))
                    .expect("Failed to set TLS configuration")
                    .with_congestion_controller(congestion_controller::Cubic::default())
                    .expect("Failed to set congestion controller")
                    .with_io("0.0.0.0:0")
                    .expect("Failed to bind the client")
                    .with_datagram(dgram)
                    .expect("Failed to enable datagrams on client")
                    .with_limits({
                        let limits = Limits::default();
                        let limits = limits
                            .with_data_window(64 * 1024 * 1024)
                            .expect("invalid data window");
                        let limits = limits
                            .with_bidirectional_local_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi local window");
                        let limits = limits
                            .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi remote window");
                        limits
                            .with_max_send_buffer_size(64 * 1024 * 1024)
                            .expect("invalid send buffer")
                    })
                    .expect("Failed to set QUIC limits");
                builder.start().expect("Failed to start client")
            }
            CcMode::Bbr => {
                let dgram = DatagramEndpoint::builder()
                    .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                    .expect("Failed to set datagram recv capacity")
                    .with_send_capacity(self.config.quic_dgram_send_capacity)
                    .expect("Failed to set datagram send capacity")
                    .build()
                    .expect("Failed to build datagram endpoint");
                let builder = Client::builder()
                    .with_tls(Path::new("server_cert.pem"))
                    .expect("Failed to set TLS configuration")
                    .with_congestion_controller(congestion_controller::Bbr::default())
                    .expect("Failed to set congestion controller")
                    .with_io("0.0.0.0:0")
                    .expect("Failed to bind the client")
                    .with_datagram(dgram)
                    .expect("Failed to enable datagrams on client")
                    .with_limits({
                        let limits = Limits::default();
                        let limits = limits
                            .with_data_window(64 * 1024 * 1024)
                            .expect("invalid data window");
                        let limits = limits
                            .with_bidirectional_local_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi local window");
                        let limits = limits
                            .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                            .expect("invalid bidi remote window");
                        limits
                            .with_max_send_buffer_size(64 * 1024 * 1024)
                            .expect("invalid send buffer")
                    })
                    .expect("Failed to set QUIC limits");
                builder.start().expect("Failed to start client")
            }
            CcMode::Disabled => {
                #[cfg(feature = "quic_no_cc")]
                {
                    let dgram = DatagramEndpoint::builder()
                        .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                        .expect("Failed to set datagram recv capacity")
                        .with_send_capacity(self.config.quic_dgram_send_capacity)
                        .expect("Failed to set datagram send capacity")
                        .build()
                        .expect("Failed to build datagram endpoint");
                    let builder = Client::builder()
                        .with_tls(Path::new("server_cert.pem"))
                        .expect("Failed to set TLS configuration")
                        .with_congestion_controller(no_cc::NoopCcEndpoint)
                        .expect("Failed to set congestion controller")
                        .with_io("0.0.0.0:0")
                        .expect("Failed to bind the client")
                        .with_datagram(dgram)
                        .expect("Failed to enable datagrams on client")
                        .with_limits({
                            let limits = Limits::default();
                            let limits = limits
                                .with_data_window(64 * 1024 * 1024)
                                .expect("invalid data window");
                            let limits = limits
                                .with_bidirectional_local_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi local window");
                            let limits = limits
                                .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi remote window");
                            limits
                                .with_max_send_buffer_size(64 * 1024 * 1024)
                                .expect("invalid send buffer")
                        })
                        .expect("Failed to set QUIC limits");
                    builder.start().expect("Failed to start client")
                }
                #[cfg(not(feature = "quic_no_cc"))]
                {
                    warn!("quic_no_cc not compiled; falling back to BBR");
                    let dgram = DatagramEndpoint::builder()
                        .with_recv_capacity(self.config.quic_dgram_recv_capacity)
                        .expect("Failed to set datagram recv capacity")
                        .with_send_capacity(self.config.quic_dgram_send_capacity)
                        .expect("Failed to set datagram send capacity")
                        .build()
                        .expect("Failed to build datagram endpoint");
                    let builder = Client::builder()
                        .with_tls(Path::new("server_cert.pem"))
                        .expect("Failed to set TLS configuration")
                        .with_congestion_controller(congestion_controller::Bbr::default())
                        .expect("Failed to set congestion controller")
                        .with_io("0.0.0.0:0")
                        .expect("Failed to bind the client")
                        .with_datagram(dgram)
                        .expect("Failed to enable datagrams on client")
                        .with_limits({
                            let limits = Limits::default();
                            let limits = limits
                                .with_data_window(64 * 1024 * 1024)
                                .expect("invalid data window");
                            let limits = limits
                                .with_bidirectional_local_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi local window");
                            let limits = limits
                                .with_bidirectional_remote_data_window(64 * 1024 * 1024)
                                .expect("invalid bidi remote window");
                            limits
                                .with_max_send_buffer_size(64 * 1024 * 1024)
                                .expect("invalid send buffer")
                        })
                        .expect("Failed to set QUIC limits");
                    builder.start().expect("Failed to start client")
                }
            }
        };

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

        info!("Connecting to node {} with QUIC...", remote_node_id);

        let local_node_id = self.config.node_id;

        stream
            .send(Bytes::copy_from_slice(&local_node_id.to_be_bytes()))
            .await
            .expect("Failed to send local node id to the node");

        info!("Connected to node {} with QUIC.", remote_node_id);

        // Keep the handshake stream open and send periodic ack‑eliciting heartbeats.
        let (_rx, tx) = stream.split();
        spawn_quic_stream_heartbeat(
            tx,
            Duration::from_secs(self.config.quic_dgram_heartbeat_secs),
        );

        connection.handle()
    }

    // Unified connect that returns a runtime-selected outcome
    #[cfg(any(feature = "quic_datagram", feature = "quic_per_flow"))]
    pub async fn connect_unified(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
    ) -> QuicConnectOutcome {
        match self.config.quic_transport_mode {
            QuicTransportMode::SingleStream => {
                let s = self.connect_single(remote_node_id, remote_addr).await;
                QuicConnectOutcome::SingleStream(s)
            }
            QuicTransportMode::PerFlow => {
                #[cfg(feature = "quic_per_flow")]
                {
                    let (h, a) = self.connect_per_flow(remote_node_id, remote_addr).await;
                    QuicConnectOutcome::PerFlow(h, a)
                }
                #[cfg(not(feature = "quic_per_flow"))]
                {
                    let s = self.connect_single(remote_node_id, remote_addr).await;
                    QuicConnectOutcome::SingleStream(s)
                }
            }
            QuicTransportMode::Datagram => {
                #[cfg(feature = "quic_datagram")]
                {
                    let h = self.connect_datagram(remote_node_id, remote_addr).await;
                    QuicConnectOutcome::Datagram(h, ())
                }
                #[cfg(not(feature = "quic_datagram"))]
                {
                    let s = self.connect_single(remote_node_id, remote_addr).await;
                    QuicConnectOutcome::SingleStream(s)
                }
            }
        }
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

// No-op congestion controller (disables QUIC CC). Use in controlled env only.
#[cfg(feature = "quic_no_cc")]
mod no_cc {
    use s2n_quic::provider::congestion_controller::{
        CongestionController, Endpoint as CcEndpoint, PathInfo, Publisher, RandomGenerator,
        RttEstimator, Timestamp,
    };
    use std::fmt::Debug;

    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopCc;

    impl CongestionController for NoopCc {
        type PacketInfo = ();

        fn congestion_window(&self) -> u32 {
            u32::MAX
        }
        fn bytes_in_flight(&self) -> u32 {
            0
        }
        fn is_congestion_limited(&self) -> bool {
            false
        }
        fn requires_fast_retransmission(&self) -> bool {
            false
        }
        fn on_packet_sent<Pub: Publisher>(
            &mut self,
            _time_sent: Timestamp,
            _sent_bytes: usize,
            _app_limited: Option<bool>,
            _rtt_estimator: &RttEstimator,
            _publisher: &mut Pub,
        ) -> Self::PacketInfo {
        }
        fn on_rtt_update<Pub: Publisher>(
            &mut self,
            _time_sent: Timestamp,
            _now: Timestamp,
            _rtt_estimator: &RttEstimator,
            _publisher: &mut Pub,
        ) {
        }
        fn on_ack<Pub: Publisher>(
            &mut self,
            _newest_acked_time_sent: Timestamp,
            _bytes_acknowledged: usize,
            _newest_acked_packet_info: Self::PacketInfo,
            _rtt_estimator: &RttEstimator,
            _random_generator: &mut dyn RandomGenerator,
            _ack_receive_time: Timestamp,
            _publisher: &mut Pub,
        ) {
        }
        fn on_packet_lost<Pub: Publisher>(
            &mut self,
            _lost_bytes: u32,
            _packet_info: Self::PacketInfo,
            _persistent_congestion: bool,
            _new_loss_burst: bool,
            _random_generator: &mut dyn RandomGenerator,
            _timestamp: Timestamp,
            _publisher: &mut Pub,
        ) {
        }
        fn on_explicit_congestion<Pub: Publisher>(
            &mut self,
            _ce_count: u64,
            _event_time: Timestamp,
            _publisher: &mut Pub,
        ) {
        }
        fn on_mtu_update<Pub: Publisher>(&mut self, _max_data_size: u16, _publisher: &mut Pub) {}
        fn on_packet_discarded<Pub: Publisher>(
            &mut self,
            _bytes_sent: usize,
            _publisher: &mut Pub,
        ) {
        }
        fn earliest_departure_time(&self) -> Option<Timestamp> {
            None
        }
    }

    #[derive(Debug, Default)]
    pub struct NoopCcEndpoint;
    impl CcEndpoint for NoopCcEndpoint {
        type CongestionController = NoopCc;
        fn new_congestion_controller(
            &mut self,
            _path_info: PathInfo,
        ) -> Self::CongestionController {
            NoopCc
        }
    }
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

// QUIC datagram reader/writer for TUN payloads (unreliable, unordered).
#[cfg(feature = "quic_datagram")]
pub struct QuicDatagramReader {
    handle: s2n_quic::connection::Handle,
    processors: ProcessorHandle,
}

#[cfg(feature = "quic_datagram")]
impl QuicDatagramReader {
    pub fn new(handle: s2n_quic::connection::Handle, processors: ProcessorHandle) -> Self {
        Self { handle, processors }
    }

    pub async fn run(&mut self) {
        loop {
            let recv_result = futures::future::poll_fn(|cx| {
                match self
                    .handle
                    .datagram_mut(|recv: &mut DatagramReceiver| recv.poll_recv_datagram(cx))
                {
                    Ok(core::task::Poll::Ready(Ok(bytes))) => core::task::Poll::Ready(Ok(bytes)),
                    Ok(core::task::Poll::Ready(Err(_))) => core::task::Poll::Ready(Err(())),
                    Ok(core::task::Poll::Pending) => core::task::Poll::Pending,
                    Err(_) => core::task::Poll::Ready(Err(())),
                }
            })
            .await;

            match recv_result {
                Ok(dat) => {
                    let buf = dat.to_vec();
                    if buf.len() < 20 {
                        warn!(
                            "QUIC datagram too small to be IPv4, dropping: {} bytes",
                            buf.len()
                        );
                        continue;
                    }
                    let msg_len = ((buf[2] as usize) << 8) | (buf[3] as usize);
                    if msg_len > buf.len() || !(20..=RECEIVE_BUF_SIZE).contains(&msg_len) {
                        warn!(
                            "Invalid IPv4 total length in QUIC datagram: {} (buf len {}), dropping",
                            msg_len,
                            buf.len()
                        );
                        continue;
                    }
                    let packet = Packet::new(msg_len, buf);
                    self.processors.process_packet(packet);
                }
                Err(err) => {
                    warn!("QUIC datagram receive error: {:?}", err);
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

#[cfg(feature = "quic_datagram")]
pub struct QuicDatagramWriter {
    handle: s2n_quic::connection::Handle,
}

#[cfg(feature = "quic_datagram")]
impl QuicDatagramWriter {
    pub fn new(handle: s2n_quic::connection::Handle) -> Self {
        Self { handle }
    }
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        // Conservative safe payload for QUIC DATAGRAM (IPv4 total length).
        // Matches the clamp used on the TUN side to avoid PMTU black holes.
        const SAFE_QUIC_DGRAM_PAYLOAD: usize = 1150;

        let mut any_sent = false;

        for packet in packets.into_iter() {
            if packet.packet_size > SAFE_QUIC_DGRAM_PAYLOAD {
                // Defensive: we should not see these after the MTU clamp; skip to avoid wedging.
                warn!(
                    "Skipping oversize IPv4 packet for QUIC DATAGRAM: {} bytes > {}",
                    packet.packet_size, SAFE_QUIC_DGRAM_PAYLOAD
                );
                continue;
            }

            let bytes = Bytes::copy_from_slice(&packet.buf[0..packet.packet_size]);
            let send_res = self
                .handle
                .datagram_mut(|sender: &mut DatagramSender| sender.send_datagram(bytes));

            match send_res {
                Ok(Ok(())) => {
                    any_sent = true;
                }
                Ok(Err(e)) => {
                    // Queue full/unsupported; log and continue instead of bailing the whole batch.
                    warn!(
                        "QUIC datagram send rejected (queue full/unsupported): {:?}",
                        e
                    );
                }
                Err(e) => {
                    // Sender temporarily unavailable; log and continue to avoid stalling.
                    warn!("QUIC datagram sender unavailable: {:?}", e);
                }
            }
        }

        // We deliberately return Ok even if none were sent in this round, letting upstream
        // backoff/retry without treating it as a fatal channel error.
        if !any_sent {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

// A per-flow multiplexing QUIC writer using one QUIC send stream per inner flow.
#[cfg(feature = "quic_per_flow")]
struct FlowBatch {
    packets: Vec<Packet>,
    close: bool,
}

#[cfg(feature = "quic_per_flow")]
pub struct QuicMuxWriter {
    handle: QuicHandle,
    flows: AHashMap<crate::node::FlowId, mpsc::Sender<FlowBatch>>, // per-flow worker channels
}

#[cfg(feature = "quic_per_flow")]
impl QuicMuxWriter {
    pub fn new(handle: QuicHandle) -> Self {
        Self {
            handle,
            flows: AHashMap::default(),
        }
    }

    async fn spawn_flow_task(mut handle: QuicHandle) -> mpsc::Sender<FlowBatch> {
        let (tx, mut rx) = mpsc::channel::<FlowBatch>(64);
        tokio::spawn(async move {
            let mut send = match handle.open_send_stream().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to open QUIC send stream: {}", e);
                    return;
                }
            };

            while let Some(batch) = rx.recv().await {
                if !batch.packets.is_empty() {
                    let mut io_slices: Vec<IoSlice> = batch
                        .packets
                        .iter()
                        .map(|p| IoSlice::new(&p.buf[0..p.packet_size]))
                        .collect();

                    let mut slices = io_slices.as_mut_slice();
                    while !slices.is_empty() {
                        match send.write_vectored(slices).await {
                            Ok(0) => {
                                error!("write_vectored returned 0 on per-flow stream");
                                break;
                            }
                            Ok(written) => IoSlice::advance_slices(&mut slices, written),
                            Err(e) => {
                                error!("QUIC per-flow write error: {}", e);
                                break;
                            }
                        }
                    }
                }

                if batch.close {
                    // Close the QUIC send stream; ignore errors but await completion.
                    let _ = send.close().await;
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
        tx
    }

    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }

        let mut grouped: AHashMap<crate::node::FlowId, Vec<Packet>> = AHashMap::default();
        for p in packets.into_iter() {
            grouped.entry(p.flow_id).or_default().push(p);
        }

        for (flow_id, mut group) in grouped.into_iter() {
            let close = group.iter().any(|p| p.is_tcp_fin_or_rst());
            let tx = match self.flows.get(&flow_id) {
                Some(tx) => tx.clone(),
                None => {
                    let tx = Self::spawn_flow_task(self.handle.clone()).await;
                    self.flows.insert(flow_id, tx.clone());
                    tx
                }
            };
            if tx
                .send(FlowBatch {
                    packets: std::mem::take(&mut group),
                    close,
                })
                .await
                .is_err()
            {
                self.flows.remove(&flow_id);
            }
            if close {
                self.flows.remove(&flow_id);
            }
        }

        Ok(())
    }
}
