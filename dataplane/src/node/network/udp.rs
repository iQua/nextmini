use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
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

#[derive(Debug)]
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
        let socket = match UdpSocket::bind(addr).await {
            Ok(sock) => Arc::new(sock),
            Err(e) => {
                error!("Failed to bind UDP socket on {addr}: {e}");
                return;
            }
        };

        let mut peers: HashMap<SocketAddr, PeerState> = HashMap::new();

        loop {
            let mut buf = vec![0u8; RECEIVE_BUF_SIZE];
            let (len, remote_addr) = match socket.recv_from(&mut buf).await {
                Ok(res) => res,
                Err(e) => {
                    warn!("UDP server recv_from failed: {e}");
                    continue;
                }
            };

            buf.truncate(len);

            let is_handshake = buf.first() == Some(&HANDSHAKE_MAGIC) && buf.len() == HANDSHAKE_LEN;

            if is_handshake {
                let mut node_id_bytes = [0u8; std::mem::size_of::<u64>()];
                node_id_bytes.copy_from_slice(&buf[1..]);
                let remote_node_id = u64::from_be_bytes(node_id_bytes) as usize;

                if peers.contains_key(&remote_addr) {
                    debug!(
                        "UDP server received duplicate handshake from {remote_addr} (node {remote_node_id}), ignoring."
                    );
                    continue;
                }

                let (sender, receiver) = mpsc::channel(self.config.channel_capacity);
                let udp_stream = UdpStream::new(socket.clone(), remote_addr, receiver);

                let network_interface = NetworkInterfaceHandle::new(
                    self.config.clone(),
                    NetworkStream::Udp(udp_stream),
                    self.processors.clone(),
                    self.reporter.clone(),
                    remote_node_id,
                )
                .await;

                let scheduler = SchedulerHandle::new(self.config.clone(), network_interface);

                if let Err(e) = self.processors.add_node(remote_node_id, scheduler) {
                    error!(
                        "Failed to add UDP peer {} (addr {}): {}",
                        remote_node_id, remote_addr, e
                    );
                    continue;
                }

                peers.insert(
                    remote_addr,
                    PeerState {
                        sender,
                        remote_node_id,
                    },
                );

                info!(
                    "UDP connection established with node {} at {}.",
                    remote_node_id, remote_addr
                );
                continue;
            }

            if let Some(peer) = peers.get(&remote_addr) {
                if buf.is_empty() {
                    continue;
                }

                match peer.sender.try_send(buf) {
                    Ok(()) => {}
                    Err(e) => {
                        warn!(
                            "Dropping UDP packet from node {} (addr {}) due to channel backpressure: {}",
                            peer.remote_node_id, remote_addr, e
                        );
                    }
                }
            } else {
                debug!("UDP server received packet from unknown peer {remote_addr}, dropping.");
            }
        }
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
            loop {
                let mut buf = vec![0u8; RECEIVE_BUF_SIZE];
                match socket_for_recv.recv(&mut buf).await {
                    Ok(len) => {
                        buf.truncate(len);
                        if buf.first() == Some(&HANDSHAKE_MAGIC) && buf.len() == HANDSHAKE_LEN {
                            continue;
                        }

                        if packet_sender.send(buf).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("UDP client recv failed: {e}");
                        break;
                    }
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
