use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, warn};

use crate::node::FlowId;
use crate::node::NodeId;
use crate::node::packet::Packet;
use nextmini_messages::{GroupDirectoryEntry, GroupId, GroupRoutingTableEntry};

#[derive(Clone, Debug)]
pub struct PythonInterfaceHandle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    // Capacity is consumed through the async registration API exposed to the Python bridge.
    #[allow(dead_code)] // Only read when the python bindings register flows.
    capacity: usize,
    senders: Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>,
    event_tx: mpsc::Sender<PythonEvent>,
    event_rx: Mutex<mpsc::Receiver<PythonEvent>>,
}

impl PythonInterfaceHandle {
    #[allow(dead_code)] // Constructed from the python bindings crate.
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

    #[allow(dead_code)] // Invoked from the python bindings crate.
    pub async fn register_receiver(&self, flow_id: FlowId) -> mpsc::Receiver<Packet> {
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, tx);
        rx
    }

    #[allow(dead_code)] // Invoked from the python bindings crate.
    pub async fn unregister_receiver(&self, flow_id: FlowId) {
        let mut map = self.inner.senders.lock().await;
        map.remove(&flow_id);
    }

    #[allow(dead_code)] // Invoked from the python bindings crate.
    pub async fn has_receiver(&self, flow_id: FlowId) -> bool {
        self.inner.senders.lock().await.contains_key(&flow_id)
    }

    pub async fn deliver(&self, packet: Packet) -> Result<(), Packet> {
        let flow_id = packet.flow_id;
        let sender = {
            let map = self.inner.senders.lock().await;
            map.get(&flow_id).cloned()
        };

        if let Some(sender) = sender {
            match sender.try_send(packet) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(packet)) | Err(TrySendError::Closed(packet)) => {
                    warn!(
                        "PythonInterface: queue unavailable for flow {flow_id}; dropping packet."
                    );
                    Err(packet)
                }
            }
        } else {
            Err(packet)
        }
    }

    /// Pushes an event into the Python bridge, dropping if the consumer is unavailable.
    pub async fn publish_event(&self, event: PythonEvent) {
        if let Err(err) = self.inner.event_tx.send(event).await {
            error!(
                "PythonInterface: failed to publish event to Python: {}",
                err
            );
        }
    }

    /// Returns the next event enqueued for Python consumers.
    #[allow(dead_code)] // Called from the python bindings crate.
    pub async fn next_event(&self) -> Option<PythonEvent> {
        let mut rx = self.inner.event_rx.lock().await;
        rx.recv().await
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)] // Defined for the python bindings crate.
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
        let mut receiver = handle.register_receiver(flow_id).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        let received = receiver.recv().await.expect("packet should be delivered");
        assert_eq!(received.bytes(), packet.bytes());
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
        let mut receiver = handle.register_receiver(flow_id).await;

        let packet = Packet::build_ipv4_tcp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            4000,
            Ipv4Addr::new(10, 0, 0, 2),
            5000,
            &[1, 2, 3, 4],
        );
        assert!(handle.deliver(packet.clone()).await.is_ok());
        // Keep the item enqueued so the next send hits capacity.
        assert!(handle.deliver(packet.clone()).await.is_err());

        // Drain to avoid hanging tasks.
        receiver.recv().await;
    }

    #[tokio::test]
    async fn deliver_returns_false_without_receiver() {
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
    async fn roundtrips_group_created_event() {
        let handle = PythonInterfaceHandle::new(4);
        let event = PythonEvent::GroupCreated {
            group_id: 7,
            src_node_id: 1,
            group_ip: Ipv4Addr::new(239, 255, 0, 10),
        };

        handle.publish_event(event.clone()).await;
        let received = handle
            .next_event()
            .await
            .expect("event should be delivered");

        match received {
            PythonEvent::GroupCreated {
                group_id,
                src_node_id,
                group_ip,
            } => {
                assert_eq!(group_id, 7);
                assert_eq!(src_node_id, 1);
                assert_eq!(group_ip, Ipv4Addr::new(239, 255, 0, 10));
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }
}
