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

use nextmini_messages::{GroupDirectoryEntry, GroupId, GroupRoutingTableEntry};

use crate::node::packet::Packet;
use crate::node::{FlowId, FlowIdExt, NodeId};

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
    backpressure: bool,
}

#[derive(Clone, Debug)]
struct ReceiverEntry {
    sender: mpsc::Sender<PayloadDelivery>,
}

/// Payload delivery to Python receivers (previously supported Raw packets, now payload-only)
pub type PythonDelivery = PayloadDelivery;

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
            "PythonInterface: queue unavailable for flow {}; dropping payload delivery.",
            self.flow_id
        );
    }
}

impl PythonInterfaceHandle {
    #[allow(dead_code)]
    pub fn new(capacity: usize, backpressure: bool) -> Self {
        let (event_tx, event_rx) = mpsc::channel(capacity);

        Self {
            inner: Arc::new(Inner {
                capacity,
                senders: Mutex::new(AHashMap::new()),
                event_tx,
                event_rx: Mutex::new(event_rx),
                backpressure,
            }),
        }
    }

    #[allow(dead_code)]
    pub async fn register_receiver(&self, flow_id: FlowId) -> mpsc::Receiver<PythonDelivery> {
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, ReceiverEntry { sender: tx });

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

        // Always deliver payload (no raw packet mode)
        self.deliver_payload(entry, packet).await
    }

    async fn deliver_payload(&self, entry: ReceiverEntry, packet: Packet) -> Result<(), Packet> {
        let slice = packet.tcp_payload().unwrap_or(packet.bytes());
        let delivery = PayloadDelivery {
            flow_id: packet.flow_id,
            bytes: Bytes::copy_from_slice(slice),
            src_ip: packet.flow_id.src_ip(),
            dst_ip: packet.flow_id.dst_ip(),
            src_port: packet.flow_id.src_port(),
            dst_port: packet.flow_id.dst_port(),
            message_id: None,
            total_len: None,
            fragment_count: None,
        };

        if Self::send_payload(entry, delivery, self.inner.backpressure)
            .await
            .is_err()
        {
            return Err(packet);
        }
        Ok(())
    }

    async fn send_payload(
        entry: ReceiverEntry,
        payload: PayloadDelivery,
        backpressure: bool,
    ) -> Result<(), ()> {
        if backpressure {
            // With backpressure enabled, wait for capacity
            entry.sender.send(payload).await.map_err(|_| ())
        } else {
            // Without backpressure, drop on full queue
            match entry.sender.try_send(payload) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(dropped)) | Err(TrySendError::Closed(dropped)) => {
                    dropped.log_queue_drop();
                    Err(())
                }
            }
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
        let handle = PythonInterfaceHandle::new(4, false);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );

        let mut receiver = handle.register_receiver(flow_id).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );

        assert!(handle.deliver(packet.clone()).await.is_ok());

        // Now receives payload (headers stripped)
        let payload = receiver.recv().await.expect("should receive payload");
        assert_eq!(payload.bytes.as_ref(), &[1, 2, 3, 4]);
        assert_eq!(payload.src_ip, Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(payload.dst_ip, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(payload.src_port, 4000);
        assert_eq!(payload.dst_port, 5000);
    }

    #[tokio::test]
    async fn deliver_fails_when_queue_full() {
        let handle = PythonInterfaceHandle::new(1, false);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id).await;

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
        let handle = PythonInterfaceHandle::new(4, false);
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
    async fn delivers_tcp_payload_with_metadata() {
        let handle = PythonInterfaceHandle::new(4, false);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet).await.is_ok());

        let payload = receiver.recv().await.expect("should receive payload");
        assert_eq!(payload.bytes.as_ref(), &[1, 2, 3, 4]);
        assert_eq!(payload.message_id, None);
        assert_eq!(payload.total_len, None);
        assert_eq!(payload.fragment_count, None);
    }

    #[tokio::test]
    async fn backpressure_waits_instead_of_dropping() {
        // Create handle with backpressure enabled and capacity of 1
        let handle = PythonInterfaceHandle::new(1, true);
        let flow_id = Packet::flow_id_from_parts(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
        );
        let mut receiver = handle.register_receiver(flow_id).await;

        // First packet should succeed
        let packet1 = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet1.clone()).await.is_ok());

        // Second packet should also succeed (waits instead of dropping)
        let packet2 = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[5, 6, 7, 8],
        );

        // Spawn delivery in background since it will block
        let handle_clone = handle.clone();
        let deliver_task = tokio::spawn(async move { handle_clone.deliver(packet2).await });

        // Give it a moment to start blocking
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Receive first packet to make space
        let payload1 = receiver.recv().await.expect("should receive first payload");
        assert_eq!(payload1.bytes.as_ref(), &[1, 2, 3, 4]);

        // Now the second delivery should complete
        assert!(deliver_task.await.unwrap().is_ok());

        // Verify second packet was received
        let payload2 = receiver
            .recv()
            .await
            .expect("should receive second payload");
        assert_eq!(payload2.bytes.as_ref(), &[5, 6, 7, 8]);
    }
}
