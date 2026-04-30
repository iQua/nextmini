use std::time::{Duration, Instant};

use ahash::AHashMap;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::PROBE_FLOW_ID;
use crate::node::network::framing;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::network::scope::TransportScope;
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;

pub struct TcpServer {
    config: LocalConfig,
    processors: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl TcpServer {
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
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut handshake_buf = [0u8; TransportScope::ENCODED_LEN];

        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    info!("Connection accepted from {:?}.", socket_addr);
                    stream
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    continue;
                }
            };

            // disables Nagle's algorithm to reduce extra latency in the outer TCP
            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY on accepted stream: {}", e);
            }

            if let Err(e) = stream.read_exact(&mut handshake_buf).await {
                error!("Failed to read scoped transport handshake: {}", e);
                continue;
            }

            let Some((remote_node_id, scope)) = TransportScope::decode_handshake(&handshake_buf)
            else {
                warn!("Failed to decode scoped transport handshake.");
                continue;
            };

            info!(
                "Incoming {:?} transport connection from node {}...",
                scope, remote_node_id
            );

            // handles an inbound connection from a new client
            let network_interface = NetworkInterfaceHandle::new(
                self.config.clone(),
                NetworkStream::Tcp(stream),
                self.processors.clone(),
                self.reporter.clone(),
                remote_node_id,
                scope,
            )
            .await;

            // creates the scheduler handle
            let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

            // adds the scheduler to send packets to the new node
            if let Err(e) = self.processors.add_node(remote_node_id, scope, scheduler) {
                error!(
                    "Failed to add node {} with address {}: {}",
                    remote_node_id,
                    listener.local_addr().unwrap(),
                    e
                );
                continue;
            }

            info!("Connected to node {}.", remote_node_id);
        }
    }
}

pub struct TcpClient {
    pub config: LocalConfig,
}

impl TcpClient {
    pub async fn connect(
        &self,
        remote_node_id: usize,
        remote_addr: &str,
        scope: TransportScope,
    ) -> TcpStream {
        let mut retry_count = 0;
        const MAX_RETRY: usize = 10;
        let mut delay = Duration::from_secs(1);
        let connect_timeout = Duration::from_secs(1);

        loop {
            match timeout(connect_timeout, TcpStream::connect(remote_addr)).await {
                Ok(Ok(mut stream)) => {
                    // disables Nagle's algorithm to reduce extra latency in the outer TCP
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY on client stream: {}", e);
                    }

                    stream
                        .write_all(&scope.encode_handshake(self.config.node_id))
                        .await
                        .expect("Failed to send scoped transport handshake to the node.");

                    info!("Connected to node {} with TCP {:?}.", remote_node_id, scope);
                    return stream;
                }
                Ok(Err(e)) => {
                    warn!(
                        "Failed to connect to node address {} with error: {}, retrying in {} seconds.",
                        remote_addr,
                        e,
                        delay.as_secs()
                    );
                }
                Err(_) => {
                    warn!(
                        "Timed out connecting to node address {} after {:?}; retrying in {} seconds.",
                        remote_addr,
                        connect_timeout,
                        delay.as_secs()
                    );
                }
            }

            tokio::time::sleep(delay).await;
            retry_count += 1;

            if retry_count >= MAX_RETRY {
                error!(
                    "Maximum retry reached for establishing a TCP connection to {}.",
                    remote_addr
                );
            }

            delay = delay.mul_f32(1.5); // Exponential backoff
        }
    }
}

pub struct TcpReader {
    stream: ReadHalf<TcpStream>,
    processors: ProcessorHandle,
    local_node_id: usize,
    remote_node_id: usize,
    scope: TransportScope,
    reporter: Option<ControllerReporterHandle>,
    active_probes: AHashMap<u64, ProbeState>,
    started_at: Instant,
    last_stats_log_at: Instant,
    forwarded_packets: u64,
    process_packet_time: Duration,
}

/// Tracks an in-flight bandwidth probe on the receive side.
struct ProbeState {
    first_arrival: Instant,
    bytes_received: usize,
}

/// Probe payload layout (17-byte header inside TCP payload):
///   [0]       flags  (0x00 = data, 0x01 = last packet)
///   [1..9]    probe_id       (u64 BE)
///   [9..17]   sender_node_id (u64 BE)
///   [17..]    padding
const PROBE_HEADER_LEN: usize = 17;

impl TcpReader {
    pub fn new(
        stream: ReadHalf<TcpStream>,
        processors: ProcessorHandle,
        local_node_id: usize,
        remote_node_id: usize,
        scope: TransportScope,
        reporter: Option<ControllerReporterHandle>,
    ) -> Self {
        let now = Instant::now();
        Self {
            stream,
            processors,
            local_node_id,
            remote_node_id,
            scope,
            reporter,
            active_probes: AHashMap::new(),
            started_at: now,
            last_stats_log_at: now,
            forwarded_packets: 0,
            process_packet_time: Duration::ZERO,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Ok(packet) = framing::read_packet(&mut self.stream).await {
                if packet.flow_id == PROBE_FLOW_ID {
                    self.handle_probe(packet);
                } else {
                    let started = Instant::now();
                    self.processors.process_packet(packet).await;
                    self.process_packet_time += started.elapsed();
                    self.forwarded_packets += 1;
                    self.maybe_log_process_share();
                }
            }
        }
    }

    fn maybe_log_process_share(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_stats_log_at) < Duration::from_millis(250) {
            return;
        }

        self.last_stats_log_at = now;
        let elapsed = now.duration_since(self.started_at);
        let process_secs = self.process_packet_time.as_secs_f64();
        let elapsed_secs = elapsed.as_secs_f64();
        let process_pct = if elapsed_secs > 0.0 {
            (process_secs / elapsed_secs) * 100.0
        } else {
            0.0
        };
        let avg_process_ms = if self.forwarded_packets > 0 {
            (process_secs * 1000.0) / self.forwarded_packets as f64
        } else {
            0.0
        };

        info!(
            local_node_id = self.local_node_id,
            remote_node_id = self.remote_node_id,
            scope = ?self.scope,
            forwarded_packets = self.forwarded_packets,
            elapsed_ms = elapsed.as_millis() as u64,
            process_packet_ms_total = self.process_packet_time.as_millis() as u64,
            process_packet_pct = process_pct,
            avg_process_packet_ms = avg_process_ms,
            "TcpReader process_packet.await share"
        );
    }

    fn handle_probe(&mut self, packet: Packet) {
        let payload = match packet.tcp_payload() {
            Some(p) if p.len() >= PROBE_HEADER_LEN => p,
            _ => return,
        };

        let flags = payload[0];
        let probe_id = u64::from_be_bytes(payload[1..9].try_into().unwrap());
        let sender_node_id = u64::from_be_bytes(payload[9..17].try_into().unwrap()) as usize;

        let state = self
            .active_probes
            .entry(probe_id)
            .or_insert_with(|| ProbeState {
                first_arrival: Instant::now(),
                bytes_received: 0,
            });
        state.bytes_received += packet.packet_size;

        if flags == 0x01 {
            let elapsed = state.first_arrival.elapsed();
            let bandwidth_mbps = if elapsed.as_nanos() > 0 {
                (state.bytes_received as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0
            } else {
                0.0
            };

            info!(
                "Probe {} from node {}: {} bytes in {:.3} ms = {:.2} Mbps",
                probe_id,
                sender_node_id,
                state.bytes_received,
                elapsed.as_secs_f64() * 1000.0,
                bandwidth_mbps,
            );

            if let Some(reporter) = &self.reporter {
                reporter.send_probe_result(
                    probe_id,
                    sender_node_id,
                    self.local_node_id,
                    bandwidth_mbps,
                );
            }

            self.active_probes.remove(&probe_id);
        }
    }
}

pub struct TcpWriter {
    stream: WriteHalf<TcpStream>,
}

impl TcpWriter {
    pub fn new(stream: WriteHalf<TcpStream>) -> Self {
        Self { stream }
    }

    /// Writes multiple packets to the TCP network stream.
    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> Result<()> {
        framing::write_packets(&mut self.stream, &packets).await
    }
}
