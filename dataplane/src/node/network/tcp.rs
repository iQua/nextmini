use std::io::Cursor;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{error, info, warn};

use crate::node::config::LocalConfig;
use crate::node::controller::interface::ProbeSchedulerRegistration;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::flow::PROBE_FLOW_ID;
use crate::node::network::framing;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;
use tokio::sync::mpsc;

pub struct TcpServer {
    config: LocalConfig,
    processors: ProcessorHandle,
    reporter: ControllerReporterHandle,
    probe_scheduler_sender: mpsc::UnboundedSender<ProbeSchedulerRegistration>,
}

impl TcpServer {
    pub fn new(
        config: LocalConfig,
        processors: ProcessorHandle,
        reporter: ControllerReporterHandle,
        probe_scheduler_sender: mpsc::UnboundedSender<ProbeSchedulerRegistration>,
    ) -> Self {
        Self {
            config,
            processors,
            reporter,
            probe_scheduler_sender,
        }
    }

    pub async fn start_listening(&mut self, addr: &String) {
        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to address {}: {}", addr, e);
                return;
            }
        };

        let mut node_id_buf: [u8; 8] = [0; 8];

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

            if let Err(e) = stream.read_exact(&mut node_id_buf).await {
                error!("Failed to read node ID: {}", e);
                continue;
            }

            let mut cursor = Cursor::new(&node_id_buf);

            let remote_node_id = match cursor.read_u64().await {
                Ok(id) => id as usize,
                Err(e) => {
                    error!("Failed to parse node ID: {}", e);
                    continue;
                }
            };

            info!("Incoming connection from node {}...", remote_node_id);

            // handles an inbound connection from a new client
            let network_interface = NetworkInterfaceHandle::new(
                self.config.clone(),
                NetworkStream::Tcp(stream),
                self.processors.clone(),
                self.reporter.clone(),
                remote_node_id,
            )
            .await;

            // creates the scheduler handle
            let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);
            let probe_scheduler = scheduler.clone();

            // adds the scheduler to send packets to the new node
            if let Err(e) = self.processors.add_node(remote_node_id, scheduler) {
                error!(
                    "Failed to add node {} with address {}: {}",
                    remote_node_id,
                    listener.local_addr().unwrap(),
                    e
                );
                continue;
            }
            if self
                .probe_scheduler_sender
                .send((remote_node_id, probe_scheduler))
                .is_err()
            {
                warn!("Probe scheduler registration channel is closed.");
            }

            info!("Connected to node {}.", remote_node_id);
        }
    }
}

pub struct TcpClient {
    pub config: LocalConfig,
}

impl TcpClient {
    pub async fn connect(&self, remote_node_id: usize, remote_addr: &str) -> TcpStream {
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

                    let local_node_id = self.config.node_id; // gets the updated local node_id

                    stream
                        .write_all(&local_node_id.to_be_bytes())
                        .await
                        .expect("Failed to send local node id to the node.");

                    info!("Connected to node {} with TCP.", remote_node_id);
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
    reporter: Option<ControllerReporterHandle>,
    active_probes: AHashMap<u64, ProbeState>,
}

/// Tracks an in-flight bandwidth probe on the receive side.
#[allow(dead_code)]
struct ProbeState {
    first_arrival: Instant,
    bytes_received: usize,
    sender_node_id: usize,
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
        reporter: Option<ControllerReporterHandle>,
    ) -> Self {
        Self {
            stream,
            processors,
            local_node_id,
            reporter,
            active_probes: AHashMap::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Ok(packet) = framing::read_packet(&mut self.stream).await {
                if packet.flow_id == PROBE_FLOW_ID {
                    self.handle_probe(packet);
                } else {
                    self.processors.process_packet(packet).await;
                }
            }
        }
    }

    fn handle_probe(&mut self, packet: Packet) {
        let payload = match packet.tcp_payload() {
            Some(p) if p.len() >= PROBE_HEADER_LEN => p,
            _ => return,
        };

        let flags = payload[0];
        let probe_id = u64::from_be_bytes(payload[1..9].try_into().unwrap());
        let sender_node_id = u64::from_be_bytes(payload[9..17].try_into().unwrap()) as usize;

        let state = self.active_probes.entry(probe_id).or_insert_with(|| ProbeState {
            first_arrival: Instant::now(),
            bytes_received: 0,
            sender_node_id,
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
