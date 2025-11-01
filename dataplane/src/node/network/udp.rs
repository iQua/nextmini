use std::collections::HashMap;
use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;

use socket2::SockRef;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, error, info, warn};

use crate::node::RECEIVE_BUF_SIZE;
use crate::node::config::LocalConfig;
use crate::node::controller::reporter::ControllerReporterHandle;
use crate::node::network::interface::{NetworkInterfaceHandle, NetworkStream};
use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::sched::SchedulerHandle;

const HANDSHAKE_MAGIC: u8 = 0xAA;
const HANDSHAKE_LEN: usize = 1 + std::mem::size_of::<u64>();
const SOCKET_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_UDP_WORKERS: usize = 2;

#[derive(Clone, Debug)]
struct PeerState {
    sender: mpsc::Sender<Vec<u8>>,
    remote_node_id: usize,
}

/// A UDP listener that accepts handshake datagrams and multiplexes packets per peer.
pub struct UdpServer {
    config: LocalConfig,
    processors: ProcessorHandle,
    reporter: ControllerReporterHandle,
}

impl UdpServer {
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
        let raw_socket = match UdpSocket::bind(addr).await {
            Ok(sock) => sock,
            Err(e) => {
                error!("Failed to bind UDP socket on {addr}: {e}");
                return;
            }
        };

        configure_socket_buffers(&raw_socket);

        let socket = Arc::new(raw_socket);
        let peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let worker_count = determine_worker_count(self.config.num_packet_processors);

        for _ in 0..worker_count {
            let socket = Arc::clone(&socket);
            let peers = Arc::clone(&peers);
            let config = self.config.clone();
            let processors = self.processors.clone();
            let reporter = self.reporter.clone();

            tokio::spawn(async move {
                let mut buf = vec![0u8; RECEIVE_BUF_SIZE];
                loop {
                    let (len, remote_addr) = match socket.recv_from(&mut buf).await {
                        Ok(res) => res,
                        Err(e) => {
                            warn!("UDP server recv_from failed: {e}");
                            continue;
                        }
                    };

                    if len == 0 {
                        continue;
                    }

                    let payload = &buf[..len];
                    let is_handshake = payload[0] == HANDSHAKE_MAGIC && len == HANDSHAKE_LEN;

                    if is_handshake {
                        let mut node_id_bytes = [0u8; std::mem::size_of::<u64>()];
                        node_id_bytes.copy_from_slice(&payload[1..]);
                        let remote_node_id = u64::from_be_bytes(node_id_bytes) as usize;

                        {
                            let peers_guard = peers.lock().await;
                            if peers_guard.contains_key(&remote_addr) {
                                debug!(
                                    "UDP server received duplicate handshake from {remote_addr} (node {remote_node_id}), ignoring."
                                );
                                continue;
                            }
                        }

                        let (sender, receiver) = mpsc::channel(config.channel_capacity);
                        let udp_stream = UdpStream::new(socket.clone(), remote_addr, receiver);

                        let network_interface = NetworkInterfaceHandle::new(
                            config.clone(),
                            NetworkStream::Udp(udp_stream),
                            processors.clone(),
                            reporter.clone(),
                            remote_node_id,
                        )
                        .await;

                        let scheduler = SchedulerHandle::new(config.clone(), network_interface);

                        if let Err(e) = processors.add_node(remote_node_id, scheduler) {
                            error!(
                                "Failed to add UDP peer {} (addr {}): {}",
                                remote_node_id, remote_addr, e
                            );
                            continue;
                        }

                        {
                            let mut peers_guard = peers.lock().await;
                            peers_guard.insert(
                                remote_addr,
                                PeerState {
                                    sender: sender.clone(),
                                    remote_node_id,
                                },
                            );
                        }

                        info!(
                            "UDP connection established with node {} at {}.",
                            remote_node_id, remote_addr
                        );

                        continue;
                    }

                    let peer_sender = {
                        let peers_guard = peers.lock().await;
                        peers_guard
                            .get(&remote_addr)
                            .map(|state| (state.sender.clone(), state.remote_node_id))
                    };

                    if let Some((sender, remote_node_id)) = peer_sender {
                        let mut packet = Vec::with_capacity(len);
                        packet.extend_from_slice(payload);

                        if let Err(e) = sender.send(packet).await {
                            warn!(
                                "UDP packet delivery to node {} (addr {}) failed: {}",
                                remote_node_id, remote_addr, e
                            );

                            let mut peers_guard = peers.lock().await;
                            peers_guard.remove(&remote_addr);
                        }
                    } else {
                        debug!(
                            "UDP server received packet from unknown peer {remote_addr}, dropping."
                        );
                    }
                }
            });
        }

        pending::<()>().await;
    }
}

/// UDP client for connecting to a remote node.
pub struct UdpClient {
    pub config: LocalConfig,
}

impl UdpClient {
    pub async fn connect(&self, _remote_node_id: usize, remote_addr: &str) -> UdpStream {
        let socket = Arc::new(
            UdpSocket::bind("0.0.0.0:0")
                .await
                .expect("Failed to bind UDP socket"),
        );

        configure_socket_buffers(socket.as_ref());

        let remote_addr: SocketAddr = remote_addr.parse().expect("Invalid UDP remote address");

        socket
            .connect(remote_addr)
            .await
            .expect("Failed to connect UDP socket");

        let local_node_id = self.config.node_id;
        let mut handshake = [0u8; HANDSHAKE_LEN];
        handshake[0] = HANDSHAKE_MAGIC;
        handshake[1..].copy_from_slice(&u64::to_be_bytes(local_node_id as u64));

        socket
            .send(&handshake)
            .await
            .expect("Failed to send UDP handshake");

        let (packet_sender, packet_receiver) = mpsc::channel(self.config.channel_capacity);
        let socket_for_recv = socket.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; RECEIVE_BUF_SIZE];
            while let Ok(len) = socket_for_recv.recv(&mut buf).await {
                if len == 0 {
                    continue;
                }

                let packet = &buf[..len];

                if packet.first() == Some(&HANDSHAKE_MAGIC) && len == HANDSHAKE_LEN {
                    continue;
                }

                let mut owned = Vec::with_capacity(len);
                owned.extend_from_slice(packet);

                if packet_sender.send(owned).await.is_err() {
                    break;
                }
            }
        });

        UdpStream::new(socket, remote_addr, packet_receiver)
    }
}

/// Stream state shared with the network interface for UDP communication.
pub struct UdpStream {
    pub socket: Arc<UdpSocket>,
    pub remote_addr: SocketAddr,
    pub receiver: mpsc::Receiver<Vec<u8>>,
}

impl UdpStream {
    pub fn new(
        socket: Arc<UdpSocket>,
        remote_addr: SocketAddr,
        receiver: mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            socket,
            remote_addr,
            receiver,
        }
    }
}

/// Reads packets for a specific UDP peer from an in-memory queue.
pub struct UdpReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    processors: ProcessorHandle,
}

impl UdpReader {
    pub fn new(receiver: mpsc::Receiver<Vec<u8>>, processors: ProcessorHandle) -> Self {
        Self {
            receiver,
            processors,
        }
    }

    pub async fn run(&mut self) {
        while let Some(buf) = self.receiver.recv().await {
            let packet_size = buf.len();
            if packet_size < 20 {
                continue;
            }

            let packet = Packet::new(packet_size, buf);
            self.processors.process_packet(packet);
        }
    }
}

/// Sends packets to a specific UDP peer.
pub struct UdpWriter {
    socket: Arc<UdpSocket>,
    remote_addr: SocketAddr,
}

impl UdpWriter {
    pub fn new(socket: Arc<UdpSocket>, remote_addr: SocketAddr) -> Self {
        Self {
            socket,
            remote_addr,
        }
    }

    pub async fn write_packets(&mut self, packets: Vec<Packet>) -> std::io::Result<()> {
        for packet in packets {
            let slice = &packet.buf[..packet.packet_size];
            let written = self
                .socket
                .send_to(slice, self.remote_addr)
                .await
                .map_err(|e| std::io::Error::new(e.kind(), format!("udp send failed: {e}")))?;

            if written != slice.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "UDP send_to wrote fewer bytes than expected",
                ));
            }
        }

        Ok(())
    }
}

fn configure_socket_buffers(socket: &UdpSocket) {
    let sock_ref = SockRef::from(socket);

    if let Err(e) = sock_ref.set_recv_buffer_size(SOCKET_BUFFER_BYTES) {
        warn!(
            "Failed to set UDP receive buffer size to {} bytes: {}",
            SOCKET_BUFFER_BYTES, e
        );
    }

    if let Err(e) = sock_ref.set_send_buffer_size(SOCKET_BUFFER_BYTES) {
        warn!(
            "Failed to set UDP send buffer size to {} bytes: {}",
            SOCKET_BUFFER_BYTES, e
        );
    }
}

fn determine_worker_count(num_packet_processors: usize) -> usize {
    if num_packet_processors == 0 {
        DEFAULT_UDP_WORKERS
    } else {
        num_packet_processors
    }
    .max(1)
}
