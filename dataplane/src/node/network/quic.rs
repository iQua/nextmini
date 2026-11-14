use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::io::Result;
use tokio::io::{AsyncRead, AsyncReadExt};

use s2n_quic::provider::congestion_controller;
use s2n_quic::stream::BidirectionalStream;
use s2n_quic::stream::{ReceiveStream, SendStream};
use s2n_quic::{Client, Server, client};
use tracing::{debug, error, info, warn};

use crate::node::config::CongestionControl;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::framing;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::network::scope::TransportScope;
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
                let mut handshake_buf = [0u8; TransportScope::ENCODED_LEN];

                if let Err(e) = stream.read_exact(&mut handshake_buf).await {
                    info!("Failed to read scoped transport handshake: {}", e);
                    connection.close(0u32.into());
                    return;
                }

                let Some((remote_node_id, scope)) =
                    TransportScope::decode_handshake(&handshake_buf)
                else {
                    info!("Failed to decode scoped transport handshake");
                    connection.close(0u32.into());
                    return;
                };

                info!(
                    "Incoming {:?} connection from node {}...",
                    scope, remote_node_id
                );

                // handles an inbound connection from a new client
                let network_interface = NetworkInterfaceHandle::new(
                    config.clone(),
                    NetworkStream::Quic(stream),
                    processors.clone(),
                    self.reporter.clone(),
                    remote_node_id,
                    scope,
                )
                .await;

                // creates the scheduler handle
                let scheduler = SchedulerHandle::new(config.clone(), network_interface);

                // adds the scheduler to send packets to the new node
                if let Err(e) = processors.add_node(remote_node_id, scope, scheduler) {
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
    pub async fn connect(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
        scope: TransportScope,
    ) -> BidirectionalStream {
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

        stream
            .send(Bytes::copy_from_slice(
                &scope.encode_handshake(self.config.node_id),
            ))
            .await
            .expect("Failed to send scoped transport handshake to the node");

        info!(
            "Connected to node {} with QUIC {:?}.",
            remote_node_id, scope
        );

        stream
    }
}

/// An actor that reads packets from a QUIC stream.
pub struct QuicReader<S = ReceiveStream> {
    stream: S,
    processors: ProcessorHandle,
}

impl QuicReader<ReceiveStream> {
    pub fn new(stream: ReceiveStream, processors: ProcessorHandle, scope: TransportScope) -> Self {
        let _ = scope;
        Self { processors, stream }
    }
}

impl<S: AsyncRead + Unpin> QuicReader<S> {
    pub async fn run(&mut self) {
        loop {
            match self.read_packet().await {
                Ok(packet) => self.processors.process_packet(packet).await,
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        debug!(error = %error, "QUIC packet reader reached EOF");
                    } else {
                        warn!(error = %error, "QUIC packet reader stopped");
                    }
                    break;
                }
            }
        }
    }

    async fn read_packet(&mut self) -> Result<Packet> {
        framing::read_packet(&mut self.stream).await
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
        framing::write_packets(&mut self.stream, &packets).await
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::time::timeout;

    use super::*;

    async fn assert_reader_stops_after(input: &[u8]) {
        let (mut writer, stream) = duplex(64);
        writer.write_all(input).await.expect("write test input");
        writer.shutdown().await.expect("close test writer");
        let mut reader = QuicReader {
            stream,
            processors: ProcessorHandle::new(Default::default()),
        };

        timeout(Duration::from_secs(1), reader.run())
            .await
            .expect("reader loop should stop after framing error");
    }

    #[tokio::test]
    async fn quic_reader_stops_on_oversized_short_and_eof_frames() {
        assert_reader_stops_after(&u32::MAX.to_be_bytes()).await;

        let mut short_frame = (20u32).to_be_bytes().to_vec();
        short_frame.extend_from_slice(&[0u8; 5]);
        assert_reader_stops_after(&short_frame).await;

        assert_reader_stops_after(&[]).await;
    }
}
