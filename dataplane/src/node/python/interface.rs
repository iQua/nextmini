//! Python dataplane interface that exposes per-flow delivery handles so Python
//! receivers can tap raw packets or TCP payloads without bespoke socket glue.

use std::fmt;
use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::{error, warn};

use crate::node::packet::Packet;
use crate::node::{FlowId, FlowIdExt, NodeId};
use nextmini_messages::{GroupDirectoryEntry, GroupId, GroupRoutingTableEntry};

#[derive(Clone)]
/// Shared entry point used by the dataplane to push packets or payloads toward
/// Python receivers.
pub struct PythonInterfaceHandle {
    inner: Arc<Inner>,
}

impl fmt::Debug for PythonInterfaceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PythonInterfaceHandle")
            .field("capacity", &self.inner.capacity)
            .finish()
    }
}

struct Inner {
    capacity: usize,
    senders: Mutex<AHashMap<FlowId, ReceiverEntry>>,
    event_tx: mpsc::Sender<PythonEvent>,
    event_rx: Mutex<mpsc::Receiver<PythonEvent>>,
}

#[derive(Clone, Debug)]
struct ReceiverEntry {
    mode: DeliveryMode,
    sender: mpsc::Sender<PythonDelivery>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryMode {
    RawPacket,
    PayloadOnly,
}

#[derive(Clone, Debug)]
pub enum PythonDelivery {
    Raw(Packet),
    Payload(PayloadDelivery),
}

#[derive(Clone, Debug)]
/// Payload-only delivery metadata consumed by Python receivers.
pub struct PayloadDelivery {
    pub flow_id: FlowId,
    pub bytes: Bytes,
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
    pub message_id: Option<u64>,
    pub total_len: Option<u32>,
    pub fragment_count: Option<u16>,
    pub payload_format: PayloadFormat,
}

impl PayloadDelivery {
    fn log_queue_drop(&self) {
        warn!(
            flow = %self.flow_id,
            src = %self.src_ip,
            dst = %self.dst_ip,
            src_port = self.src_port,
            dst_port = self.dst_port,
            payload_len = self.bytes.len(),
            total_len = ?self.total_len,
            fragment_count = ?self.fragment_count,
            message_id = ?self.message_id,
            format = ?self.payload_format,
            "PythonInterface: queue unavailable for flow {}; dropping payload delivery.",
            self.flow_id
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PayloadFormat {
    Payload,
    RawPacket,
}

impl PythonInterfaceHandle {
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        let (event_tx, event_rx) = mpsc::channel(capacity);
        Self {
            inner: Arc::new(Inner {
                capacity,
                senders: Mutex::new(AHashMap::new()),
                event_tx,
                event_rx: Mutex::new(event_rx),
            }),
        }
    }

    #[allow(dead_code)]
    pub async fn register_receiver(
        &self,
        flow_id: FlowId,
        payload_only: bool,
    ) -> mpsc::Receiver<PythonDelivery> {
        let mode = if payload_only {
            DeliveryMode::PayloadOnly
        } else {
            DeliveryMode::RawPacket
        };
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, ReceiverEntry { mode, sender: tx });
        rx
    }

    #[allow(dead_code)]
    pub async fn unregister_receiver(&self, flow_id: FlowId) {
        let mut map = self.inner.senders.lock().await;
        map.remove(&flow_id);
    }

    #[allow(dead_code)]
    pub async fn has_receiver(&self, flow_id: FlowId) -> bool {
        self.inner.senders.lock().await.contains_key(&flow_id)
    }

    pub async fn deliver(&self, packet: Packet) -> Result<(), Packet> {
        let flow_id = packet.flow_id;
        let entry = {
            let map = self.inner.senders.lock().await;
            map.get(&flow_id).cloned()
        };

        let Some(entry) = entry else {
            return Err(packet);
        };

        match entry.mode {
            DeliveryMode::RawPacket => self.deliver_raw(entry, packet).await,
            DeliveryMode::PayloadOnly => self.deliver_payload(entry, packet).await,
        }
    }

    async fn deliver_raw(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        Self::send_raw(entry, packet)
    }

    async fn deliver_payload(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        let delivery = raw_payload_delivery(&packet);
        if Self::send_payload(entry, delivery).is_err() {
            return Err(packet);
        }
        Ok(())
    }

    fn send_payload(entry: ReceiverEntry, payload: PayloadDelivery) -> Result<(), ()> {
        match entry.sender.try_send(PythonDelivery::Payload(payload)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(delivery)) | Err(TrySendError::Closed(delivery)) => {
                match delivery {
                    PythonDelivery::Payload(dropped) => dropped.log_queue_drop(),
                    PythonDelivery::Raw(pkt) => {
                        warn!(
                            flow = %pkt.flow_id,
                            "PythonInterface: queue returned unexpected raw packet in payload path; dropping packet."
                        );
                    }
                }
                Err(())
            }
        }
    }

    fn send_raw(entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        match entry.sender.try_send(PythonDelivery::Raw(packet)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(PythonDelivery::Raw(pkt)))
            | Err(TrySendError::Closed(PythonDelivery::Raw(pkt))) => {
                warn!(
                    "PythonInterface: queue unavailable for flow {}; dropping packet.",
                    pkt.flow_id
                );
                Err(pkt)
            }
            Err(_) => unreachable!("unexpected delivery variant"),
        }
    }

    pub async fn publish_event(&self, event: PythonEvent) {
        if let Err(err) = self.inner.event_tx.send(event).await {
            error!(
                "PythonInterface: failed to publish event to Python: {}",
                err
            );
        }
    }

    #[allow(dead_code)]
    pub async fn next_event(&self) -> Option<PythonEvent> {
        let mut rx = self.inner.event_rx.lock().await;
        rx.recv().await
    }
}

fn tcp_payload_offset(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 20 || (bytes[0] >> 4) != 4 {
        return None;
    }
    let ihl = (bytes[0] & 0x0F) as usize;
    let ip_header_len = ihl * 4;
    if ip_header_len < 20 || bytes.len() < ip_header_len + 20 {
        return None;
    }
    let tcp_data_offset = (bytes[ip_header_len + 12] >> 4) as usize;
    let tcp_header_len = tcp_data_offset * 4;
    if tcp_header_len < 20 || bytes.len() < ip_header_len + tcp_header_len {
        return None;
    }
    Some(ip_header_len + tcp_header_len)
}

fn tcp_payload_from_frame(bytes: &[u8]) -> Option<&[u8]> {
    tcp_payload_offset(bytes).map(|offset| &bytes[offset..])
}

fn raw_payload_delivery(packet: &Packet) -> PayloadDelivery {
    let slice = tcp_payload_from_frame(packet.bytes()).unwrap_or(packet.bytes());
    PayloadDelivery {
        flow_id: packet.flow_id,
        bytes: Bytes::copy_from_slice(slice),
        src_ip: packet.flow_id.src_ip(),
        dst_ip: packet.flow_id.dst_ip(),
        src_port: packet.flow_id.src_port(),
        dst_port: packet.flow_id.dst_port(),
        message_id: None,
        total_len: None,
        fragment_count: None,
        payload_format: PayloadFormat::RawPacket,
    }
}

#[derive(Clone, Debug)]
/// Events forwarded to Python code so it can mirror multicast group state.
#[allow(dead_code)]
pub enum PythonEvent {
    GroupCreated {
        group_id: GroupId,
        src_node_id: NodeId,
        group_ip: Ipv4Addr,
    },
    GroupDirectoryUpdated {
        entries: Vec<GroupDirectoryEntry>,
    },
    GroupRoutesInstalled {
        group_id: GroupId,
        src_node_id: NodeId,
        routes: Vec<GroupRoutingTableEntry>,
    },
    LocalMemberJoined {
        group_id: GroupId,
        node_id: NodeId,
    },
    LocalMemberLeft {
        group_id: GroupId,
        node_id: NodeId,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn deliver_success_for_registered_flow() {
        let handle = PythonInterfaceHandle::new(4);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id, false).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        match receiver.recv().await {
            Some(PythonDelivery::Raw(received)) => {
                assert_eq!(received.bytes(), packet.bytes())
            }
            other => panic!("unexpected delivery: {:?}", other),
        }
    }

    #[tokio::test]
    async fn deliver_fails_when_queue_full() {
        let handle = PythonInterfaceHandle::new(1);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id, false).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        assert!(handle.deliver(packet.clone()).await.is_err());
        receiver.recv().await;
    }

    #[tokio::test]
    async fn deliver_returns_error_without_receiver() {
        let handle = PythonInterfaceHandle::new(4);
        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet).await.is_err());
    }

    #[tokio::test]
    async fn payload_only_receives_tcp_payload() {
        let handle = PythonInterfaceHandle::new(4);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id, true).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet).await.is_ok());
        match receiver.recv().await {
            Some(PythonDelivery::Payload(payload)) => {
                assert_eq!(payload.bytes.as_ref(), &[1, 2, 3, 4]);
                assert_eq!(payload.message_id, None);
                assert_eq!(payload.total_len, None);
                assert_eq!(payload.fragment_count, None);
                assert_eq!(payload.payload_format, PayloadFormat::RawPacket);
            }
            other => panic!("unexpected delivery: {:?}", other),
        }
    }
}
