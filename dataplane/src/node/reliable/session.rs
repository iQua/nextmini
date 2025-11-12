use std::collections::VecDeque;
use std::net::Ipv4Addr;
use tokio::task::JoinHandle;

use ahash::AHashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use nextmini_messages::TokenBucketSpec;

use crate::node::processor::ProcessorHandle;

use super::api::{InboundFrame, SessionId};

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
pub enum AckPolicy {
    All,
    KofN(usize),
    Fraction(f32),
}

#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub group_ip: Ipv4Addr,
    pub chunk_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub control_weight: usize,
    pub data_bucket: Option<TokenBucketSpec>,
    pub local_node_id: usize,
    pub user_space_base_addr: Ipv4Addr,
    pub local_netmask: Ipv4Addr,
}

#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_path: Option<String>,
    pub checksum_out: bool,
    pub ack_policy: AckPolicy,
    pub repair_backoff_ms: u64,
    pub fec_k: Option<u16>,
    pub fec_p: u8,
    pub ready_grace_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub verify_checksum: bool,
    pub sink_path: Option<String>,
    pub nack_min_interval_ms: u64,
    pub nack_jitter_ms: u64,
    pub sack_interval_ms: u64,
}

pub struct SessionManager {
    processors: ProcessorHandle,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    pending: AHashMap<PendingReceiverKey, VecDeque<PendingReceiver>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingReceiverKey {
    pub group_ip: Ipv4Addr,
    pub source_node_id: usize,
}

struct PendingReceiver {
    cfg: ReceiverConfig,
    reply: oneshot::Sender<SessionId>,
}

impl SessionManager {
    /// Removes and returns the join handle for a session task, if present.
    pub fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    pub fn new(processors: ProcessorHandle) -> Self {
        Self {
            processors,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            pending: AHashMap::default(),
        }
    }

    pub fn spawn_sender(&mut self, cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        tracing::debug!(
            session_id = sid,
            receiver_count = cfg.receiver_ids.len(),
            "SessionManager: spawn_sender called"
        );
        let processors = self.processors.clone();
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx.clone());
        tracing::debug!(
            session_id = sid,
            "SessionManager: sender input channel created and stored in inputs map"
        );
        let handle = tokio::spawn(super::sender::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        tracing::debug!(
            session_id = sid,
            total_sessions = self.tasks.len(),
            "SessionManager: sender task spawned and stored"
        );
        sid
    }

    pub fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        tracing::debug!(
            session_id = sid,
            source_node = cfg.source_node_id,
            "SessionManager: spawn_receiver called"
        );
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);
        tracing::debug!(
            session_id = sid,
            "SessionManager: receiver input channel created and stored in inputs map"
        );
        let processors = self.processors.clone();
        let handle = tokio::spawn(super::receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        tracing::debug!(
            session_id = sid,
            total_sessions = self.tasks.len(),
            "SessionManager: receiver task spawned and stored"
        );
        sid
    }

    pub async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) {
            h.abort();
        }
        self.remove_inputs(sid);
    }

    pub fn remove_inputs(&mut self, sid: SessionId) {
        self.inputs.remove(&sid);
    }

    /// Returns a clone of the inbound channel for a session, if present.
    pub fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        let result = self.inputs.get(&sid).cloned();
        if result.is_some() {
            tracing::debug!(
                session_id = sid,
                "SessionManager: input_sender found for session"
            );
        } else {
            tracing::warn!(
                session_id = sid,
                available_sessions = ?self.inputs.keys().collect::<Vec<_>>(),
                "SessionManager: input_sender NOT found for session"
            );
        }
        result
    }

    pub fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        sid
    }

    pub fn enqueue_pending_receiver(
        &mut self,
        key: PendingReceiverKey,
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    ) {
        self.pending
            .entry(key)
            .or_default()
            .push_back(PendingReceiver { cfg, reply });
    }

    pub fn adopt_pending_receiver(
        &mut self,
        key: PendingReceiverKey,
        session_id: SessionId,
    ) -> Option<(ReceiverConfig, oneshot::Sender<SessionId>)> {
        let queue = self.pending.get_mut(&key)?;
        let mut pending = queue.pop_front()?;
        pending.cfg.common.session_id = session_id;
        if queue.is_empty() {
            self.pending.remove(&key);
        }
        Some((pending.cfg, pending.reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_policy_variants_constructible() {
        let policies = [AckPolicy::All, AckPolicy::KofN(2), AckPolicy::Fraction(0.5)];
        for policy in policies {
            match policy {
                AckPolicy::All => {}
                AckPolicy::KofN(n) => assert!(n >= 1),
                AckPolicy::Fraction(f) => {
                    assert!(f > 0.0);
                    assert!(f <= 1.0);
                }
            }
        }
    }
}
