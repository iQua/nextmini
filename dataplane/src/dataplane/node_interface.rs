//This struct represents the connection of a node in the network.
use std::sync::Arc;

use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::info;

use s2n_quic::stream::BidirectionalStream;

use crate::dataplane::metrics::MetricsTx;
use crate::dataplane::packet::Packet;
use crate::dataplane::processor::SenderLoadBalancer;
use crate::dataplane::protocols_io::{
    ProtocolReader, ProtocolWriter, QuicReader, QuicWriter, TcpReader, TcpWriter, UdpReader,
    UdpWriter,
};
use crate::dataplane::scheduler::SchedulingDiscipline;
use crate::dataplane::scheduler::{Fifo, Scheduler};
use crate::dataplane::utils::RateLimiter;
use crate::dataplane::{FlowId, INTERNAL_Q_SIZE, NodeId, RECEIVE_BUF_SIZE};
use tracing::error;

pub fn create_tcp_node_interfaces(
    stream: TcpStream,
    remote_node_id: NodeId,
    send_rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    txs: Vec<mpsc::Sender<Packet>>,
    scheduler_type: SchedulingDiscipline,
) -> (NodeReceiver, NodeSender) {
    let (reader, writer) = tokio::io::split(stream);

    (
        NodeReceiver {
            remote_node_id,
            reader: ProtocolReader::Tcp(TcpReader::new(reader)),
            tx: SenderLoadBalancer::new(txs),
        },
        NodeSender::new(
            ProtocolWriter::Tcp(TcpWriter::new(Arc::new(Mutex::new(writer)))),
            send_rate_limiter,
            scheduler_type,
        ),
    )
}

pub async fn create_quic_node_interfaces(
    stream: BidirectionalStream,
    remote_node_id: NodeId,
    send_rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    txs: Vec<mpsc::Sender<Packet>>,
    scheduler_type: SchedulingDiscipline,
) -> (NodeReceiver, NodeSender) {
    let (receive_stream, send_stream) = stream.split();

    let node_receiver = NodeReceiver {
        remote_node_id,
        reader: ProtocolReader::Quic(QuicReader::new(receive_stream)),
        tx: SenderLoadBalancer::new(txs),
    };

    let send_stream_arc = Arc::new(Mutex::new(send_stream));
    let node_sender = NodeSender::new(
        ProtocolWriter::Quic(QuicWriter::new(send_stream_arc)),
        send_rate_limiter,
        scheduler_type,
    );

    (node_receiver, node_sender)
}

pub fn create_udp_node_sender(
    sock: Arc<UdpSocket>,
    addr: String,
    send_rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    scheduler_type: SchedulingDiscipline,
) -> NodeSender {
    NodeSender::new(
        ProtocolWriter::Udp(UdpWriter::new(sock, addr)),
        send_rate_limiter,
        scheduler_type,
    )
}

pub fn create_udp_node_receiver(
    sock: Arc<UdpSocket>,
    remote_node_id: NodeId,
    txs: Vec<mpsc::Sender<Packet>>,
) -> NodeReceiver {
    NodeReceiver {
        remote_node_id,
        reader: ProtocolReader::Udp(UdpReader::new(sock)),
        tx: SenderLoadBalancer::new(txs),
    }
}

pub struct NodeReceiver {
    remote_node_id: NodeId,
    reader: ProtocolReader,
    tx: SenderLoadBalancer,
}

impl NodeReceiver {
    pub async fn start_receiving(&mut self, metrics_tx: MetricsTx) {
        loop {
            let mut buf = [0; RECEIVE_BUF_SIZE];
            let n = self.reader.recv(&mut buf).await;

            // Skip empty or invalid packets to prevent downstream errors
            if n == 0 {
                info!(
                    "WARNING: NodeReceiver from node {} received empty packet - connection may be closing",
                    self.remote_node_id
                );
                continue;
            }

            let packet = Packet::new(n, buf);
            let flow_id = packet.flow_id;

            self.record_metrics(flow_id, n, metrics_tx.clone()).await;

            self.tx.try_send(packet);
        }
    }

    async fn record_metrics(&self, flow_id: FlowId, n_bytes: usize, metrics_tx: MetricsTx) {
        // metrics reported are in the format of (flow_id, node_id, n_bytes)
        if let Err(e) = metrics_tx.send((flow_id, self.remote_node_id, n_bytes)) {
            error!(
                "Failed to send metrics for flow {:#x} from node {} ({} bytes): {:?} - metrics collector may be offline",
                flow_id, self.remote_node_id, n_bytes, e
            );
        }
    }
}

pub struct NodeSender {
    writer: ProtocolWriter,
    rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
    scheduler: Box<dyn Scheduler + Send + Sync>,
    scheduler_type: SchedulingDiscipline,
}

impl NodeSender {
    pub fn new(
        writer: ProtocolWriter,
        rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
        scheduler_type: SchedulingDiscipline,
    ) -> Self {
        let writer_clone = writer.reproduce();
        let limiter_clone = rate_limiter.clone();

        let mut scheduler: Box<dyn Scheduler + Send + Sync> = match scheduler_type {
            SchedulingDiscipline::Fifo => Box::new(Fifo::new(
                INTERNAL_Q_SIZE,
                crate::dataplane::drop::DropStrategy::Red,
                writer,
                rate_limiter,
            )),
            SchedulingDiscipline::Wrr => {
                panic!("WRR scheduler is not implemented yet");
            }
        };

        scheduler.run();

        Self {
            writer: writer_clone,
            rate_limiter: limiter_clone,
            scheduler,
            scheduler_type,
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.scheduler.enqueue(packet);
    }

    pub async fn reproduce(&self) -> Self {
        Self::new(
            self.writer.reproduce(),
            self.rate_limiter.clone(),
            self.scheduler_type,
        )
    }
}
