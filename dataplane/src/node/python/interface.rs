use std::sync::Arc;

use ahash::AHashMap;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc};
use tracing::warn;

use crate::node::FlowId;
use crate::node::packet::Packet;

#[derive(Clone, Debug)]
pub struct PythonInterfaceHandle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
#[allow(dead_code)] // Methods are exercised from the optional `nextmini_py` crate.
struct Inner {
    // Capacity is consumed through the async registration API exposed to the Python bridge.
    capacity: usize,
    senders: Mutex<AHashMap<FlowId, mpsc::Sender<Packet>>>,
}

impl PythonInterfaceHandle {
    #[allow(dead_code)] // External callers construct the handle from Python.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                senders: Mutex::new(AHashMap::new()),
            }),
        }
    }

    #[allow(dead_code)] // Registration happens through PyO3 bindings.
    pub async fn register_receiver(&self, flow_id: FlowId) -> mpsc::Receiver<Packet> {
        let (tx, rx) = mpsc::channel(self.inner.capacity);
        let mut map = self.inner.senders.lock().await;
        map.insert(flow_id, tx);
        rx
    }

    #[allow(dead_code)] // Deregistration is handled on the Python side.
    pub async fn unregister_receiver(&self, flow_id: FlowId) {
        let mut map = self.inner.senders.lock().await;
        map.remove(&flow_id);
    }

    #[allow(dead_code)] // Queried from the Python bindings to short-circuit deliveries.
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
}
