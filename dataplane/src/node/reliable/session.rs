use std::net::Ipv4Addr;
use tokio::task::JoinHandle;

use ahash::AHashMap;

use nextmini_messages::TokenBucketSpec;

use crate::node::network::interface::NetworkInterfaceHandle;
use crate::node::processor::ProcessorHandle;
use crate::node::FlowId;

use super::api::SessionId;

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
    pub sack_interval_ms: u64,
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
}

pub struct SessionManager {
    // Optional until the network writer is fully wired for reliable sessions.
    net: Option<NetworkInterfaceHandle>,
    processors: ProcessorHandle,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
}

impl SessionManager {
    pub fn new(net: NetworkInterfaceHandle, processors: ProcessorHandle) -> Self {
        Self { net: Some(net), processors, tasks: AHashMap::default() }
    }

    /// Removes and returns the join handle for a session task, if present.
    pub fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    /// Temporary constructor while the network writer hookup is decided.
    /// Spawns no-op tasks for senders/receivers and logs warnings.
    pub fn new_without_net(processors: ProcessorHandle) -> Self {
        Self { net: None, processors, tasks: AHashMap::default() }
    }

    pub fn spawn_sender(&mut self, cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let processors = self.processors.clone();
        let handle = tokio::spawn(super::sender::run(cfg, processors));
        self.tasks.insert(sid, handle);
        sid
    }

    pub fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let sid = cfg.common.session_id;
        let handle = tokio::spawn(super::receiver::run(cfg, None));
        self.tasks.insert(sid, handle);
        sid
    }

    pub async fn stop(&mut self, sid: SessionId) {
        if let Some(h) = self.tasks.remove(&sid) {
            h.abort();
        }
    }

    /// Returns true if the session task has finished (or does not exist).
    pub fn is_finished(&self, sid: SessionId) -> bool {
        match self.tasks.get(&sid) {
            Some(h) => h.is_finished(),
            None => true,
        }
    }

    /// Apply per-flow scheduler policy, e.g., raise control-flow priority.
    /// Callers should pass the flow_id representing the control channel.
    pub fn apply_scheduler_policy(&self, flow_id: FlowId, control_weight: usize) {
        // Set WRR weight via processors; token-bucket per flow can be wired when exposed.
        self.processors.set_flow_weight(flow_id, control_weight);
    }
}
