use std::net::Ipv4Addr;
use std::sync::Arc;

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{FecCapabilities, FecManifest};

use crate::node::config::{Feature, LosslessConfig};
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{Command, InboundFrame, SessionId};
use crate::node::session::{receiver, sender};

/// Socket addressing and runtime knobs shared by senders and receivers.
#[derive(Clone, Debug)]
pub struct CommonConfig {
    pub session_id: SessionId,
    pub dest_ip: Ipv4Addr,
    pub chunk_size: usize,
    pub src_port: u16,
    pub dst_port: u16,
    pub data_bucket: Option<TokenBucketSpec>,
    pub local_node_id: usize,
    pub user_space_base_addr: Ipv4Addr,
    pub local_netmask: Ipv4Addr,
}

/// Sender-only configuration (fan-out, source path, ready grace, etc.).
#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub common: CommonConfig,
    pub receiver_ids: Vec<usize>,
    pub total_bytes: u64,
    pub source_buffer: Bytes,
    /// Optional FEC declaration. When present, sender uses strict FEC-only negotiation
    /// and switches retirement semantics from cumulative chunk ACKs to per-block FEC status.
    pub fec_manifest: Option<FecManifest>,
    /// Explicit sender-allowed tree IDs for collaborative FEC dispatch.
    pub fec_tree_ids: Vec<u16>,
    /// Per-tree lane depth for collaborative FEC dispatch.
    pub fec_tree_lane_depth: usize,
    /// Max FEC symbols to dispatch per sender scheduler cycle.
    pub fec_dispatch_burst: usize,
    pub ready_grace_ms: u64,
    pub topology_ready: Option<watch::Receiver<bool>>,
}

/// Receiver-only configuration (source node, expected bytes, sink path, etc.).
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub common: CommonConfig,
    pub source_node_id: usize,
    pub expected_bytes: u64,
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Advertised FEC capabilities for sender preflight compatibility checks.
    pub fec_capabilities: FecCapabilities,
}

/// Handle for communicating with the lossless runtime actor.
/// This handle can be cloned and used to manage lossless sessions.
#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl LosslessRuntimeHandle {
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let runtime = LosslessRuntime::new(processors, config, command_rx);

        // spawns the lossless runtime actor task
        tokio::spawn(async move {
            let mut runtime = runtime;

            runtime.run().await;
        });

        Self { command_tx }
    }

    /// Requests that the runtime spin up a sender session with the supplied
    /// configuration and return its session ID.
    pub async fn start_sender(&self, cfg: SenderConfig) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.command_tx.send(Command::StartSender {
            cfg,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Request that the runtime spin up a receiver immediately.
    pub async fn start_receiver(&self, cfg: ReceiverConfig) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self.command_tx.send(Command::StartReceiver {
            cfg,
            reply: reply_tx,
        });

        reply_rx.await.expect("The session ID.")
    }

    /// Cancels a session regardless of whether it is a sender or receiver.
    pub fn stop(&self, session: SessionId) {
        let _ = self.command_tx.send(Command::Stop { session });
    }

    /// Delivers an inbound frame to the owning session's queue.
    pub fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self.command_tx.send(Command::Deliver { session, frame });
    }

    /// Waits until the runtime observes completion (EOT/ACKs) for a session.
    pub async fn wait_completion(&self, session: SessionId) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::Wait {
            session,
            reply: reply_tx,
        });
        reply_rx.await.unwrap_or(false)
    }

    /// Reserves the next session identifier from the runtime's allocator.
    #[allow(dead_code)]
    pub async fn allocate_session_id(&self) -> SessionId {
        let (reply_tx, reply_rx) = oneshot::channel();

        let _ = self
            .command_tx
            .send(Command::AllocateSession { reply: reply_tx });

        reply_rx.await.expect("The session ID.")
    }

    /// Notifies the runtime that the control plane finished setting up the topology.
    pub fn set_topology_ready(&self, ready: bool) {
        let _ = self.command_tx.send(Command::SetTopologyReady { ready });
    }
}

/// Tracks running lossless sessions along with their inboxes and join handles.
/// This is the actor that processes commands and manages session lifecycle.
struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    tasks: AHashMap<SessionId, JoinHandle<()>>,
    inputs: AHashMap<SessionId, mpsc::Sender<InboundFrame>>,
    next_session_id: SessionId,
    topology_ready_tx: watch::Sender<bool>,
    topology_ready: bool,
    command_rx: mpsc::UnboundedReceiver<Command>,
}

impl LosslessRuntime {
    /// Constructs a runtime that can spawn sender/receiver tasks and track their lifetimes.
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        command_rx: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (topology_ready_tx, _) = watch::channel(false);

        Self {
            processors,
            config,
            tasks: AHashMap::default(),
            inputs: AHashMap::default(),
            next_session_id: 1,
            topology_ready_tx,
            topology_ready: false,
            command_rx,
        }
    }

    /// Main event loop for the lossless runtime actor that processes inbound commands.
    async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                Command::StartSender { cfg, reply } => {
                    let sid = self.spawn_sender(cfg);

                    let _ = reply.send(sid);
                }
                Command::StartReceiver { cfg, reply } => {
                    let sid = self.spawn_receiver(cfg);

                    let _ = reply.send(sid);
                }
                Command::Stop { session } => {
                    self.stop(session).await;
                }
                Command::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                Command::Wait { session, reply } => {
                    // handles a wait command asynchronously without blocking the main loop
                    self.handle_wait(session, reply);
                }
                Command::AllocateSession { reply } => {
                    let sid = self.allocate_session_id();

                    let _ = reply.send(sid);
                }
                Command::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Delivers inbound frames to sessions.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(tx) = self.input_sender(session) {
            if tx.send(frame).await.is_err() {
                warn!(
                    session_id = session,
                    "Lossless runtime: receiver dropped inbound frame."
                );
            }
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: no receiver for inbound frame."
            );
        }
    }

    /// Handles wait command by spawning a separate task.
    fn handle_wait(&mut self, session: SessionId, reply: oneshot::Sender<bool>) {
        // Take ownership of the task handle. Note: we don't remove inputs here
        // because in-flight frames may still arrive. Cleanup happens in stop()
        // after wait completes.
        let handle = self.take_task(session);

        tokio::spawn(async move {
            if let Some(handle) = handle {
                let _ = handle.await; // ignore join errors; treat as completion
                let _ = reply.send(true);
            } else {
                let _ = reply.send(false);
            }
        });
    }

    /// Removes and returns the join handle for a session task, if present.
    fn take_task(&mut self, sid: SessionId) -> Option<JoinHandle<()>> {
        self.tasks.remove(&sid)
    }

    /// Spawns a sender task, wiring up control-plane readiness watchers and
    /// returning its assigned session ID.
    fn spawn_sender(&mut self, mut cfg: SenderConfig) -> SessionId {
        let sid = cfg.common.session_id;
        cfg.fec_tree_lane_depth = self.config.fec_tree_lane_depth;
        cfg.fec_dispatch_burst = self.config.fec_dispatch_burst;
        if let Err(err) = validate_fec_sender_config(&self.config, &cfg) {
            self.reject_sender_preflight(sid, err);
            return sid;
        }
        let processors = self.processors.clone();

        // subscribes to topology readiness if not already ready
        if self.topology_ready {
            cfg.topology_ready = None;
        } else {
            cfg.topology_ready = Some(self.topology_ready_tx.subscribe());
        }

        let (tx, rx) = mpsc::channel(1024);
        self.inputs.insert(sid, tx);

        let sender_handle = tokio::spawn(sender::run(cfg, rx, processors));
        self.tasks.insert(sid, sender_handle);

        sid
    }

    /// Spawns a receiver task and hand it a bounded inbox for inbound frames.
    fn spawn_receiver(&mut self, cfg: ReceiverConfig) -> SessionId {
        let mut cfg = cfg;
        if !self.config.fec_enabled {
            cfg.fec_capabilities = FecCapabilities::empty();
        }

        let sid = cfg.common.session_id;
        let processors = self.processors.clone();

        let (tx, rx) = mpsc::channel::<InboundFrame>(1024);
        self.inputs.insert(sid, tx);

        let receiver_handle = tokio::spawn(receiver::run(cfg, rx, processors));
        self.tasks.insert(sid, receiver_handle);

        sid
    }

    /// Aborts a running session and drops its inbox, if still active.
    async fn stop(&mut self, sid: SessionId) {
        if let Some(handle) = self.tasks.remove(&sid) {
            handle.abort();
        }

        self.inputs.remove(&sid);
    }

    /// Returns a clone of the inbound channel for a session, if present.
    fn input_sender(&self, sid: SessionId) -> Option<mpsc::Sender<InboundFrame>> {
        self.inputs.get(&sid).cloned()
    }

    /// Reserves a unique session identifier for future tasks.
    fn allocate_session_id(&mut self) -> SessionId {
        let sid = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);

        sid
    }

    /// Broadcasts topology readiness so all senders may advance their state gates.
    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        let _ = self.topology_ready_tx.send(ready);
    }

    fn reject_sender_preflight(&mut self, sid: SessionId, err: FecPreflightError) {
        if let Some(handle) = self.tasks.remove(&sid) {
            handle.abort();
        }
        self.inputs.remove(&sid);
        warn!(
            session_id = sid,
            reason = %err,
            "Lossless runtime: rejected sender session during deterministic preflight"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FecPreflightError {
    DisabledByConfig,
    CapabilityRequirementDisabled,
    UnknownScheme { scheme: u8 },
    CollaborativeMultiTreeDisabled,
    InvalidTreeLaneDepth { value: usize },
    InvalidDispatchBurst { value: usize },
    InvalidMaxTreeLanes { value: usize },
    MissingTreeIds,
    TreeIdsMustBeSortedUnique { tree_ids: Vec<u16> },
    TooManyTreeIds { configured: usize, max: usize },
    MultiTreeRequiresSequentialIngress { feature: Feature },
    SymbolsPerBlockOutOfBounds { value: u16, min: u16, max: u16 },
    SymbolSizeOutOfBounds { value: u16, min: u16, max: u16 },
    ChunkSizeExceedsSymbolSize { chunk_size: usize, symbol_size: u16 },
}

fn feature_mode_label(feature: &Feature) -> &'static str {
    match feature {
        Feature::Sequential => "sequential",
        Feature::Concurrent => "concurrent",
    }
}

impl std::fmt::Display for FecPreflightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DisabledByConfig => write!(f, "fec is disabled by local runtime configuration"),
            Self::CapabilityRequirementDisabled => write!(
                f,
                "fec_require_capability=false is unsupported with strict no-fallback sessions"
            ),
            Self::UnknownScheme { scheme } => write!(f, "unknown fec scheme {scheme} requested"),
            Self::CollaborativeMultiTreeDisabled => write!(
                f,
                "collaborative multi-tree fec is disabled by local runtime configuration"
            ),
            Self::InvalidTreeLaneDepth { value } => {
                write!(f, "fec_tree_lane_depth must be >= 1 (got {value})")
            }
            Self::InvalidDispatchBurst { value } => {
                write!(f, "fec_dispatch_burst must be >= 1 (got {value})")
            }
            Self::InvalidMaxTreeLanes { value } => {
                write!(f, "fec_max_tree_lanes must be >= 1 (got {value})")
            }
            Self::MissingTreeIds => {
                write!(f, "fec_tree_ids must be non-empty for fec sender sessions")
            }
            Self::TreeIdsMustBeSortedUnique { tree_ids } => write!(
                f,
                "fec_tree_ids must be sorted ascending and unique (got {tree_ids:?})"
            ),
            Self::TooManyTreeIds { configured, max } => write!(
                f,
                "configured fec_tree_ids length {configured} exceeds fec_max_tree_lanes {max}"
            ),
            Self::MultiTreeRequiresSequentialIngress { feature } => {
                write!(
                    f,
                    "collaborative multi-tree fec requires sequential ingress (feature={})",
                    feature_mode_label(feature)
                )
            }
            Self::SymbolsPerBlockOutOfBounds { value, min, max } => write!(
                f,
                "fec symbols_per_block {value} out of bounds [{min}, {max}]"
            ),
            Self::SymbolSizeOutOfBounds { value, min, max } => {
                write!(f, "fec symbol_size {value} out of bounds [{min}, {max}]")
            }
            Self::ChunkSizeExceedsSymbolSize {
                chunk_size,
                symbol_size,
            } => write!(
                f,
                "chunk_size {chunk_size} exceeds fec symbol_size {symbol_size}"
            ),
        }
    }
}

fn validate_fec_sender_config(
    runtime_config: &LosslessConfig,
    sender_cfg: &SenderConfig,
) -> Result<(), FecPreflightError> {
    let Some(manifest) = sender_cfg.fec_manifest else {
        return Ok(());
    };

    if !runtime_config.fec_enabled {
        return Err(FecPreflightError::DisabledByConfig);
    }
    if !runtime_config.fec_require_capability {
        return Err(FecPreflightError::CapabilityRequirementDisabled);
    }
    if manifest.scheme_kind().is_none() {
        return Err(FecPreflightError::UnknownScheme {
            scheme: manifest.scheme,
        });
    }

    if sender_cfg.fec_tree_lane_depth == 0 {
        return Err(FecPreflightError::InvalidTreeLaneDepth {
            value: sender_cfg.fec_tree_lane_depth,
        });
    }
    if sender_cfg.fec_dispatch_burst == 0 {
        return Err(FecPreflightError::InvalidDispatchBurst {
            value: sender_cfg.fec_dispatch_burst,
        });
    }
    if runtime_config.fec_max_tree_lanes == 0 {
        return Err(FecPreflightError::InvalidMaxTreeLanes {
            value: runtime_config.fec_max_tree_lanes,
        });
    }

    if sender_cfg.fec_tree_ids.is_empty() {
        return Err(FecPreflightError::MissingTreeIds);
    }
    if !sender_cfg
        .fec_tree_ids
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return Err(FecPreflightError::TreeIdsMustBeSortedUnique {
            tree_ids: sender_cfg.fec_tree_ids.clone(),
        });
    }

    let tree_count = sender_cfg.fec_tree_ids.len();
    if tree_count > runtime_config.fec_max_tree_lanes {
        return Err(FecPreflightError::TooManyTreeIds {
            configured: tree_count,
            max: runtime_config.fec_max_tree_lanes,
        });
    }

    if tree_count > 1 {
        if !runtime_config.fec_collaborative_multitree_enabled {
            return Err(FecPreflightError::CollaborativeMultiTreeDisabled);
        }
        if runtime_config.ingress_feature != Feature::Sequential {
            return Err(FecPreflightError::MultiTreeRequiresSequentialIngress {
                feature: runtime_config.ingress_feature.clone(),
            });
        }
    }

    let (symbols_min, symbols_max) = runtime_config.fec_symbols_per_block_bounds();
    if manifest.symbols_per_block < symbols_min || manifest.symbols_per_block > symbols_max {
        return Err(FecPreflightError::SymbolsPerBlockOutOfBounds {
            value: manifest.symbols_per_block,
            min: symbols_min,
            max: symbols_max,
        });
    }

    let (size_min, size_max) = runtime_config.fec_symbol_size_bounds();
    if manifest.symbol_size < size_min || manifest.symbol_size > size_max {
        return Err(FecPreflightError::SymbolSizeOutOfBounds {
            value: manifest.symbol_size,
            min: size_min,
            max: size_max,
        });
    }

    if sender_cfg.common.chunk_size > usize::from(manifest.symbol_size) {
        return Err(FecPreflightError::ChunkSizeExceedsSymbolSize {
            chunk_size: sender_cfg.common.chunk_size,
            symbol_size: manifest.symbol_size,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use bytes::Bytes;

    use nextmini_messages::lossless_session::FecManifest;

    use super::{CommonConfig, SenderConfig, validate_fec_sender_config};
    use crate::node::config::{Feature, LosslessConfig};

    fn sender_cfg_with_manifest(manifest: Option<FecManifest>, chunk_size: usize) -> SenderConfig {
        SenderConfig {
            common: CommonConfig {
                session_id: 17,
                dest_ip: Ipv4Addr::new(10, 0, 0, 2),
                chunk_size,
                src_port: 3000,
                dst_port: 4000,
                data_bucket: None,
                local_node_id: 1,
                user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            },
            receiver_ids: vec![2],
            total_bytes: 128,
            source_buffer: Bytes::from(vec![0xAB; 64]),
            fec_manifest: manifest,
            fec_tree_ids: vec![0],
            fec_tree_lane_depth: 32,
            fec_dispatch_burst: 1,
            ready_grace_ms: 1,
            topology_ready: None,
        }
    }

    #[test]
    fn fec_preflight_rejects_when_disabled() {
        let runtime = LosslessConfig::default();
        let manifest = FecManifest::new_raptorq(8, 1400);
        let sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "FEC sessions should be deterministically rejected when kill-switch is off"
        );
    }

    #[test]
    fn fec_preflight_rejects_out_of_bounds_manifest() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_symbols_per_block_min: 8,
            fec_symbols_per_block_max: 16,
            fec_symbol_size_min: 1400,
            fec_symbol_size_max: 1600,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(32, 1400);
        let sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "invalid manifest bounds must fail preflight"
        );
    }

    #[test]
    fn fec_preflight_accepts_in_bounds_manifest_when_enabled() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_symbols_per_block_min: 8,
            fec_symbols_per_block_max: 64,
            fec_symbol_size_min: 1200,
            fec_symbol_size_max: 2000,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(result.is_ok());
    }

    #[test]
    fn fec_preflight_rejects_missing_tree_ids() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_tree_ids.clear();

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "FEC sessions must provide explicit, non-empty tree IDs"
        );
    }

    #[test]
    fn fec_preflight_rejects_unsorted_or_duplicate_tree_ids() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_tree_ids = vec![3, 1, 1];

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "tree IDs must be strictly ascending and duplicate-free"
        );
    }

    #[test]
    fn fec_preflight_rejects_multitree_when_collaborative_mode_disabled() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_collaborative_multitree_enabled: false,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_tree_ids = vec![1, 3];

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "multi-tree mode should honor the explicit collaborative on/off gate"
        );
    }

    #[test]
    fn fec_preflight_rejects_multitree_when_ingress_mode_is_concurrent() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            ingress_feature: Feature::Concurrent,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_tree_ids = vec![1, 3];

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "T5 Option A requires deterministic rejection for concurrent ingress"
        );
    }

    #[test]
    fn fec_preflight_rejects_tree_count_exceeding_runtime_limit() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            fec_max_tree_lanes: 2,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_tree_ids = vec![1, 3, 5];

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "configured tree IDs must not exceed runtime lane cap"
        );
    }

    #[test]
    fn fec_preflight_rejects_zero_dispatch_burst() {
        let runtime = LosslessConfig {
            fec_enabled: true,
            fec_require_capability: true,
            ..Default::default()
        };
        let manifest = FecManifest::new_raptorq(16, 1400);
        let mut sender_cfg = sender_cfg_with_manifest(Some(manifest), 1200);
        sender_cfg.fec_dispatch_burst = 0;

        let result = validate_fec_sender_config(&runtime, &sender_cfg);
        assert!(
            result.is_err(),
            "dispatch burst must be validated as a positive tunable"
        );
    }
}
