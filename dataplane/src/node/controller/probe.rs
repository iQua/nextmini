use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Instant;

use ahash::AHashMap;
use chrono::Utc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Duration, Interval, interval};
use tracing::{debug, error, info};

use nextmini_messages::LinkProbeResult;

use crate::node::NodeId;
use crate::node::controller::reporter::ControllerReporterHandle;

const PROBE_MAGIC: &[u8; 2] = b"NP";

#[derive(Debug)]
enum ProbeCommand {
    RegisterPeer(NodeId, String),
}

#[derive(Clone)]
pub struct ProbeServiceHandle {
    sender: UnboundedSender<ProbeCommand>,
}

impl ProbeServiceHandle {
    pub fn new(
        local_node_id: NodeId,
        probe_port: u16,
        probe_interval_secs: u64,
        enable_probe_rtt: bool,
        enable_probe_throughput: bool,
        reporter: ControllerReporterHandle,
    ) -> Self {
        let (sender, receiver) = unbounded_channel();

        let mut service = ProbeService {
            local_node_id,
            reporter,
            probe_port,
            peers: AHashMap::default(),
            pending: HashMap::new(),
            rtts: HashMap::new(),
            sent: HashMap::new(),
            sent_bytes: HashMap::new(),
            recv_bytes: HashMap::new(),
            seq: 1,
            interval: interval(Duration::from_secs(probe_interval_secs.max(1))),
            enable_probe_rtt,
            enable_probe_throughput,
        };

        tokio::spawn(async move {
            service.run(receiver).await;
        });

        Self { sender }
    }

    pub fn register_peer(&self, node_id: NodeId, addr: String) {
        if let Err(e) = self.sender.send(ProbeCommand::RegisterPeer(node_id, addr)) {
            error!("Failed to register peer {} for probing: {}", node_id, e);
        }
    }
}

struct ProbeService {
    local_node_id: NodeId,
    reporter: ControllerReporterHandle,
    probe_port: u16,
    peers: AHashMap<NodeId, String>,
    pending: HashMap<u64, (NodeId, Instant)>,
    rtts: HashMap<NodeId, Vec<f64>>,
    sent: HashMap<NodeId, u32>,
    sent_bytes: HashMap<NodeId, usize>,
    recv_bytes: HashMap<NodeId, usize>,
    seq: u64,
    interval: Interval,
    enable_probe_rtt: bool,
    enable_probe_throughput: bool,
}

impl ProbeService {
    async fn run(&mut self, mut receiver: UnboundedReceiver<ProbeCommand>) {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], self.probe_port));
        let socket = match UdpSocket::bind(bind_addr).await {
            Ok(sock) => sock,
            Err(e) => {
                error!("ProbeService failed to bind on {}: {}", bind_addr, e);
                return;
            }
        };

        info!(
            "ProbeService started on port {} for node {}.",
            self.probe_port, self.local_node_id
        );

        let mut buf = [0u8; 1500];

        loop {
            tokio::select! {
                Some(cmd) = receiver.recv() => {
                    match cmd {
                        ProbeCommand::RegisterPeer(node_id, addr) => {
                            self.peers.insert(node_id, addr);
                        }
                    }
                }
                _ = self.interval.tick() => {
                    self.finish_round().await;
                    self.start_round(&socket).await;
                }
                recv = socket.recv_from(&mut buf) => {
                    match recv {
                        Ok((len, addr)) => self.handle_packet(&socket, &buf[..len], addr).await,
                        Err(e) => error!("ProbeService recv error: {}", e),
                    }
                }
            }
        }
    }

    async fn start_round(&mut self, socket: &UdpSocket) {
        self.rtts.clear();
        self.sent.clear();
        self.sent_bytes.clear();
        self.recv_bytes.clear();
        self.pending.clear();

        if self.enable_probe_rtt {
            for (peer_id, addr) in self.peers.clone() {
                if let Some(dest) = self.addr_with_probe_port(&addr) {
                    let seq = self.next_seq();
                    let payload = self.build_payload(seq, 0);
                    self.pending.insert(seq, (peer_id, Instant::now()));
                    *self.sent.entry(peer_id).or_insert(0) += 1;

                    if let Err(e) = socket.send_to(&payload, dest).await {
                        error!(
                            "ProbeService failed to send probe to {} ({}): {}",
                            peer_id, dest, e
                        );
                        self.pending.remove(&seq);
                    } else {
                        *self.sent_bytes.entry(peer_id).or_insert(0) += payload.len();
                    }
                }
            }
        }

        if self.enable_probe_throughput {
            // Active throughput burst: small capped-size burst per peer
            let burst_payload = vec![0u8; 1200];
            let burst_duration = Duration::from_millis(500);
            let burst_end = Instant::now() + burst_duration;
            for (peer_id, addr) in self.peers.clone() {
                if let Some(dest) = self.addr_with_probe_port(&addr) {
                    let mut sent_bytes = 0usize;
                    while Instant::now() < burst_end {
                        let seq = self.next_seq();
                        let payload = self.build_payload_with_data(seq, &burst_payload);
                        match socket.send_to(&payload, dest).await {
                            Ok(n) => {
                                sent_bytes += n;
                            }
                            Err(e) => {
                                debug!(
                                    "ProbeService burst send failure to {} ({}): {}",
                                    peer_id, dest, e
                                );
                                break;
                            }
                        }
                        // Light pacing to avoid hogging bandwidth
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    if sent_bytes > 0 {
                        self.sent_bytes
                            .entry(peer_id)
                            .and_modify(|b| *b += sent_bytes)
                            .or_insert(sent_bytes);
                    }
                }
            }
        }
    }

    async fn finish_round(&mut self) {
        if self.sent.is_empty() && self.recv_bytes.is_empty() {
            return;
        }

        let mut results = Vec::new();
        let now = Utc::now();

        for (peer_id, sent) in self.sent.drain() {
            let received = self.rtts.get(&peer_id).map(|v| v.len()).unwrap_or(0) as u32;
            let loss = if sent > 0 {
                Some(((sent.saturating_sub(received)) as f64 / sent as f64) * 100.0)
            } else {
                None
            };

            let rtt_ms = self.rtts.get(&peer_id).and_then(|vals| {
                if vals.is_empty() {
                    None
                } else {
                    Some(avg(vals))
                }
            });

            let mbps = match (
                self.sent_bytes.get(&peer_id).copied(),
                self.recv_bytes.get(&peer_id).copied(),
            ) {
                (Some(sent_b), Some(recv_b)) if sent_b > 0 && received > 0 => {
                    // Use received bytes over the burst duration to avoid counting loss twice.
                    let burst_secs = 0.5f64;
                    Some((recv_b as f64 * 8.0) / burst_secs / 1_000_000.0)
                }
                _ => None,
            };

            results.push(LinkProbeResult {
                src_node_id: self.local_node_id,
                dst_node_id: peer_id,
                rtt_ms,
                loss_pct: loss,
                mbps,
                samples: sent,
                time_read: now,
            });
        }

        if !results.is_empty() {
            self.reporter.send_link_probe_results(results).await;
        }
    }

    async fn handle_packet(&mut self, socket: &UdpSocket, packet: &[u8], addr: SocketAddr) {
        if packet.len() < 2 || &packet[..2] != PROBE_MAGIC {
            return;
        }

        if packet.len() < 14 {
            return;
        }

        let src_node_id =
            u32::from_be_bytes([packet[2], packet[3], packet[4], packet[5]]) as NodeId;
        let seq = u64::from_be_bytes([
            packet[6], packet[7], packet[8], packet[9], packet[10], packet[11], packet[12],
            packet[13],
        ]);

        if src_node_id == self.local_node_id {
            if let Some((peer_id, sent_at)) = self.pending.remove(&seq) {
                let rtt = sent_at.elapsed().as_secs_f64() * 1000.0;
                self.rtts.entry(peer_id).or_default().push(rtt);
                self.recv_bytes
                    .entry(peer_id)
                    .and_modify(|b| *b += packet.len())
                    .or_insert(packet.len());
            }
            return;
        }

        // Echo back to sender
        if let Err(e) = socket.send_to(packet, addr).await {
            debug!("ProbeService failed to echo probe to {}: {}", addr, e);
        }
    }

    fn build_payload(&self, seq: u64, pad_len: usize) -> Vec<u8> {
        let mut payload = Vec::with_capacity(14 + pad_len);
        payload.extend_from_slice(PROBE_MAGIC);
        payload.extend_from_slice(&(self.local_node_id as u32).to_be_bytes());
        payload.extend_from_slice(&seq.to_be_bytes());
        payload.resize(14 + pad_len, 0u8);
        payload
    }

    fn build_payload_with_data(&self, seq: u64, data: &[u8]) -> Vec<u8> {
        let mut payload = self.build_payload(seq, data.len());
        let header_len = 14;
        payload[header_len..header_len + data.len()].copy_from_slice(data);
        payload
    }

    fn next_seq(&mut self) -> u64 {
        let current = self.seq;
        self.seq = self.seq.wrapping_add(1).max(1);
        current
    }

    fn addr_with_probe_port(&self, addr: &str) -> Option<SocketAddr> {
        match addr.rsplit_once(':') {
            Some((host, _)) => format!("{}:{}", host, self.probe_port)
                .to_socket_addrs()
                .ok()
                .and_then(|mut iter| iter.next()),
            None => None,
        }
    }
}

fn avg(vals: &[f64]) -> f64 {
    let sum: f64 = vals.iter().sum();
    sum / vals.len() as f64
}
