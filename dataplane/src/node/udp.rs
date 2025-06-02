// use crate::node::NodeId;
// use crate::node::PacketBuf;
// use crate::node::packet::Packet;
// use crate::node::protocols_io::{ProtocolReader, ProtocolWriter};
// use crate::node::scheduler::SchedulingDiscipline;
// use crate::node::utils::RateLimiter;
// use std::sync::Arc;
// use tokio::net::UdpSocket;
// use tokio::sync::{RwLock, mpsc};

// #[derive(Clone)]
// pub struct UdpReader {
//     sock: Arc<UdpSocket>,
// }

// impl UdpReader {
//     pub fn new(sock: Arc<UdpSocket>) -> Self {
//         Self { sock }
//     }

//     pub fn create_reader(
//         sock: Arc<UdpSocket>,
//         remote_node_id: NodeId,
//         txs: Vec<mpsc::Sender<Packet>>,
//     ) -> NodeReceiver {
//         NodeReceiver {
//             remote_node_id,
//             reader: ProtocolReader::Udp(UdpReader::new(sock)),
//             tx: SenderLoadBalancer::new(txs),
//         }
//     }

//     pub async fn read(&self, buf: &mut PacketBuf) -> usize {
//         match self.sock.recv(&mut buf[..]).await {
//             Ok(n) => n,
//             Err(e) => panic!("{e}"),
//         }
//     }
// }

// #[derive(Clone)]
// pub struct UdpWriter {
//     sock: Arc<UdpSocket>,
//     addr: String,
// }

// impl UdpWriter {
//     pub fn new(sock: Arc<UdpSocket>, addr: String) -> Self {
//         Self { sock, addr }
//     }

//     pub fn create_writer(
//         sock: Arc<UdpSocket>,
//         addr: String,
//         send_rate_limiter: Arc<RwLock<Option<RateLimiter>>>,
//         scheduler_type: SchedulingDiscipline,
//     ) -> NodeSender {
//         NodeSender::new(
//             ProtocolWriter::Udp(UdpWriter::new(sock, addr)),
//             send_rate_limiter,
//             scheduler_type,
//         )
//     }

//     pub async fn write(&self, data: &[u8]) {
//         match self.sock.send_to(data, self.addr.as_str()).await {
//             Ok(_) => (),
//             Err(e) => panic!("{e}"),
//         }
//     }
// }
