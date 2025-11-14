use std::collections::VecDeque;
use std::net::Ipv4Addr;

use ahash::AHashMap;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use nextmini_messages::TokenBucketSpec;

use crate::node::config::TfmccRuntimeConfig;
use crate::node::processor::ProcessorHandle;

use super::api::{InboundFrame, SessionId};

/// Configures how strongly the sender waits for receiver acknowledgements.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug)]
pub enum AckPolicy {
    All,
    KofN(usize),
    Fraction(f32),
}

/// Sender-side congestion control mode.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum CongestionControl {
    /// Current behavior: static window + optional token bucket.
    Static,
    /// TFMCC: rate-based controller driven by receiver feedback.
    Tfmcc(TfmccConfig),
}

/// Parameters that govern the TFMCC controller.
#[derive(Clone, Debug)]
pub struct TfmccConfig {
    pub min_rate_bps: f64,
    pub max_rate_bps: f64,
    pub initial_rate_bps: f64,
    pub feedback_interval_ms: u64,
    pub rate_smooth_alpha: f64,
    pub max_increase_per_rtt_pkts: f64,
    pub clr_hysteresis_pct: f64,
}

/// Socket addressing and runtime knobs shared by senders and receivers.
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

/// Sender-only configuration (fan-out, source path, FEC knobs, etc.).
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
    pub cc: CongestionControl,
    pub topology_ready: Option<watch::Receiver<bool>>,
    pub routes_ready: Option<watch::Receiver<bool>>,
}

/// Receiver-only configuration (source node, reliability timers, sinks, etc.).
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
    pub cc: CongestionControl,
}

impl From<&TfmccRuntimeConfig> for TfmccConfig {
    fn from(cfg: &TfmccRuntimeConfig) -> Self {
        Self {
            min_rate_bps: cfg.min_rate_bps.max(1.0),
            max_rate_bps: cfg.max_rate_bps.max(cfg.min_rate_bps.max(1.0)),
            initial_rate_bps: cfg
                .initial_rate_bps
                .clamp(cfg.min_rate_bps.max(1.0), cfg.max_rate_bps.max(1.0)),
            feedback_interval_ms: cfg.feedback_interval_ms.max(10),
            rate_smooth_alpha: cfg.rate_smooth_alpha.clamp(0.0, 1.0),
            max_increase_per_rtt_pkts: cfg.max_increase_per_rtt_pkts.max(0.5),
            clr_hysteresis_pct: cfg.clr_hysteresis_pct.clamp(0.0, 1.0),
        }
    }
}

/// Tracks running reliable sessions along with their inboxes and join handles.
pub struct SessionManager {
    processors: ProcessorHandle,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    pending: AHashMap<PendingReceiverKey, VecDeque<PendingReceiver>>,
    topology_ready_tx: watch::Sender<bool>,
    route_ready: AHashMap<(Ipv4Addr, usize), watch::Sender<bool>>,
}

/// Key that allows a receiver to be created speculatively and paired once the
/// control plane decides which session ID to use for a (group, source) tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingReceiverKey {
    pub group_ip: Ipv4Addr,
    pub source_node_id: usize,
}

/// Wrapper that stores receiver config until a session ID is assigned.
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
        let (topology_ready_tx, _) = watch::channel(false);
        Self {
            processors,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            pending: AHashMap::default(),
            topology_ready_tx,
            route_ready: AHashMap::default(),
        }
    }

    pub fn spawn_sender(&mut self, mut cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();
        cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        let route_key = (cfg.common.group_ip, cfg.common.local_node_id);
        let routes_ready_sender = self.route_ready.entry(route_key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(false);
            tx
        });
        cfg.routes_ready = Some(routes_ready_sender.subscribe());
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
        self.remove_inputs(sid);
    }

    pub fn remove_inputs(&mut self, sid: SessionId) {
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

    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.topology_ready_tx.send(ready);
    }

    pub fn set_group_routes_ready(&mut self, group_ip: Ipv4Addr, src_node_id: usize) {
        let key = (group_ip, src_node_id);
        let entry = self.route_ready.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(false);
            tx
        });
        let _ = entry.send(true);
    }

    pub fn enqueue_pending_receiver(
        &mut self,
        key: PendingReceiverKey,
        cfg: ReceiverConfig,
        reply: oneshot::Sender<SessionId>,
    ) {
        // Multiple listeners may race to attach; keep them queued until the
        // control plane picks a session ID.
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
