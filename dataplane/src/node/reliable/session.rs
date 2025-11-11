use std::net::Ipv4Addr;
use tokio::task::JoinHandle;

use ahash::AHashMap;
use tokio::sync::mpsc;

use nextmini_messages::TokenBucketSpec;

use crate::node::processor::ProcessorHandle;

use super::api::{InboundFrame, SessionId};

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
}

impl SessionManager {
    /// Removes and returns the join handle for a session task, if present.
    pub fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        let handle = self.tasks.remove(&sid);
        self.inputs.remove(&sid);
        handle
    }

    pub fn new(processors: ProcessorHandle) -> Self {
        Self {
            processors,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
        }
    }

    pub fn spawn_sender(&mut self, cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);
        let handle = tokio::spawn(super::sender::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        sid
    }

    pub fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);
        let processors = self.processors.clone();
        let handle = tokio::spawn(super::receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, handle);
        sid
    }

    pub async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) {
            h.abort();
        }
        self.inputs.remove(&sid);
    }

    /// Returns a clone of the inbound channel for a session, if present.
    pub fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    pub fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        sid
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
