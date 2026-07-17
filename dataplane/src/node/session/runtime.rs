//! Background runtime that owns lossless sender and receiver session tasks.

use std::fs::File;
use std::net::Ipv4Addr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ahash::AHashMap;
use bytes::Bytes;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tracing::{debug, info, warn};

use nextmini_messages::TokenBucketSpec;
use nextmini_messages::lossless_session::{self, LosslessSessionControl, LosslessSessionManifest};

use crate::node::config::LosslessConfig;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{
    CompletedReceiverReplay, InboundFrame, LosslessRuntimeMessage, LosslessSessionHandle,
    SessionId, SessionOutcome, SessionState, StartError,
};
pub use crate::node::session::fec_policy::PreflightError;
use crate::node::session::plan::BlockPlan;
use crate::node::session::{control, fec_policy, receiver, sender};

/// Settings shared by sender and receiver session tasks.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Canonical logical block size for this transfer.
    pub block_size: usize,
}

/// Precomputed transport envelope used for outbound lossless session frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportRoute {
    /// Local source IP address for the synthetic TCP wrapper.
    pub src_ip: Ipv4Addr,
    /// Remote destination IP address for the synthetic TCP wrapper.
    pub dst_ip: Ipv4Addr,
    /// Local TCP source port used for outbound frames.
    pub src_port: u16,
    /// Remote TCP destination port used for outbound frames.
    pub dst_port: u16,
}

/// Local tree-striping settings for Cloudcast mode.
#[derive(Clone, Debug, PartialEq)]
pub struct CloudcastRuntimeConfig {
    tree_ids: Vec<u16>,
    stripe_tree_ids: Vec<u16>,
}

/// Validated carousel timing values shared by sender and receiver tasks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarouselRuntimeConfig {
    pub ack_debounce: Duration,
    pub ack_heartbeat: Duration,
    pub ack_probe_interval: Duration,
    pub peer_silence_timeout: Duration,
    pub peer_stall_timeout: Duration,
    pub passive_margin: Duration,
    pub receiver_passive_window: Duration,
    pub session_complete_repeats: u8,
    pub session_complete_interval: Duration,
    pub mettle_repair_reorder_budget: Duration,
    pub mettle_repair_no_progress_epochs: u32,
}

/// Process-wide admission pool for dense METTLE prefix decoders.
#[derive(Clone, Debug)]
pub struct MettleDecoderBudget {
    reservation_bytes: usize,
    permits: Arc<Semaphore>,
}

/// One logical dense-decoder reservation.
#[derive(Debug)]
pub struct MettleDecoderPermit {
    _permit: OwnedSemaphorePermit,
}

/// Invalid decoder-budget configuration or exhausted process admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MettleDecoderBudgetError {
    ZeroReservation,
    ZeroPermits,
    TooManyPermits,
    AggregateOverflow,
    Exhausted,
}

impl std::fmt::Display for MettleDecoderBudgetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroReservation => formatter.write_str("METTLE decoder reservation is zero"),
            Self::ZeroPermits => formatter.write_str("METTLE decoder permit count is zero"),
            Self::TooManyPermits => {
                formatter.write_str("METTLE decoder permit count exceeds semaphore capacity")
            }
            Self::AggregateOverflow => {
                formatter.write_str("METTLE decoder aggregate reservation overflows")
            }
            Self::Exhausted => formatter.write_str("METTLE decoder permits are exhausted"),
        }
    }
}

impl std::error::Error for MettleDecoderBudgetError {}

impl MettleDecoderBudget {
    pub(super) fn from_lossless(config: &LosslessConfig) -> Result<Self, MettleDecoderBudgetError> {
        let reservation_bytes = config.mettle_decoder_reservation_bytes;
        if reservation_bytes == 0 {
            return Err(MettleDecoderBudgetError::ZeroReservation);
        }
        let max_concurrent = config.mettle_decoder_max_concurrent;
        if max_concurrent == 0 {
            return Err(MettleDecoderBudgetError::ZeroPermits);
        }
        if max_concurrent > Semaphore::MAX_PERMITS {
            return Err(MettleDecoderBudgetError::TooManyPermits);
        }
        reservation_bytes
            .checked_mul(max_concurrent)
            .ok_or(MettleDecoderBudgetError::AggregateOverflow)?;

        Ok(Self {
            reservation_bytes,
            permits: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    pub(super) fn try_acquire(&self) -> Result<MettleDecoderPermit, MettleDecoderBudgetError> {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| MettleDecoderBudgetError::Exhausted)?;
        Ok(MettleDecoderPermit { _permit: permit })
    }

    pub(super) const fn reservation_bytes(&self) -> usize {
        self.reservation_bytes
    }

    #[cfg(test)]
    pub(super) fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

impl CarouselRuntimeConfig {
    pub(super) fn from_lossless(config: &LosslessConfig) -> Self {
        Self {
            ack_debounce: Duration::from_millis(config.carousel_ack_debounce_ms),
            ack_heartbeat: Duration::from_millis(config.carousel_ack_heartbeat_ms),
            ack_probe_interval: Duration::from_millis(config.carousel_ack_probe_interval_ms),
            peer_silence_timeout: Duration::from_millis(config.carousel_peer_silence_timeout_ms),
            peer_stall_timeout: Duration::from_millis(config.carousel_peer_stall_timeout_ms),
            passive_margin: Duration::from_millis(config.carousel_passive_margin_ms),
            receiver_passive_window: Duration::from_millis(
                config.carousel_receiver_passive_window_ms,
            ),
            session_complete_repeats: config.carousel_session_complete_repeats,
            session_complete_interval: Duration::from_millis(
                config.carousel_session_complete_interval_ms,
            ),
            mettle_repair_reorder_budget: Duration::from_millis(
                config.mettle_repair_reorder_budget_ms,
            ),
            mettle_repair_no_progress_epochs: config.mettle_repair_no_progress_epochs,
        }
    }

    pub(super) fn validate(self) -> Result<(), PreflightError> {
        let nonzero = [
            self.ack_debounce,
            self.ack_heartbeat,
            self.ack_probe_interval,
            self.peer_silence_timeout,
            self.peer_stall_timeout,
            self.receiver_passive_window,
            self.session_complete_interval,
            self.mettle_repair_reorder_budget,
        ];
        if nonzero.iter().any(Duration::is_zero)
            || self.session_complete_repeats == 0
            || self.mettle_repair_no_progress_epochs == 0
        {
            return Err(PreflightError::InvalidCarouselTiming {
                reason: "all carousel intervals and repeat counts must be non-zero",
            });
        }
        if self.ack_debounce >= self.ack_heartbeat {
            return Err(PreflightError::InvalidCarouselTiming {
                reason: "ack debounce must be shorter than the ack heartbeat",
            });
        }
        if self.ack_heartbeat >= self.peer_silence_timeout
            || self.ack_probe_interval >= self.peer_silence_timeout
        {
            return Err(PreflightError::InvalidCarouselTiming {
                reason: "ack heartbeat and probe intervals must be shorter than peer silence timeout",
            });
        }
        if self.peer_silence_timeout >= self.peer_stall_timeout {
            return Err(PreflightError::InvalidCarouselTiming {
                reason: "peer stall timeout must be longer than peer silence timeout",
            });
        }
        let required_passive_window = self
            .peer_stall_timeout
            .checked_add(self.passive_margin)
            .ok_or(PreflightError::InvalidCarouselTiming {
                reason: "carousel abort budget plus passive margin overflows",
            })?;
        if self.receiver_passive_window < required_passive_window {
            return Err(PreflightError::InvalidCarouselTiming {
                reason: "receiver passive window must cover sender abort budget plus margin",
            });
        }
        Ok(())
    }
}

impl Default for CarouselRuntimeConfig {
    fn default() -> Self {
        let config = Self::from_lossless(&LosslessConfig::default());
        config
            .validate()
            .expect("default carousel timing configuration must be valid");
        config
    }
}

impl CloudcastRuntimeConfig {
    #[must_use]
    pub fn new(tree_ids: Vec<u16>, stripe_tree_ids: Vec<u16>) -> Self {
        Self {
            tree_ids,
            stripe_tree_ids,
        }
    }

    pub fn tree_ids(&self) -> &[u16] {
        &self.tree_ids
    }

    pub fn stripe_tree_ids(&self) -> &[u16] {
        &self.stripe_tree_ids
    }
}

/// User-facing request used to start a sender session.
#[derive(Clone, Debug)]
pub struct SenderRequest {
    /// Shared per-session settings.
    pub session: SessionConfig,
    /// Precomputed transport envelope for sender traffic.
    pub route: TransportRoute,
    /// Optional pacing configuration applied to outbound data.
    pub pacing: Option<TokenBucketSpec>,
    /// Receiver node IDs expected to provide lossless feedback.
    pub receiver_ids: Vec<usize>,
    /// Total logical object length in bytes.
    pub total_bytes: u64,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Maximum time to wait for READY frames during session start.
    pub ready_grace_ms: u64,
    /// Maximum time to wait for frozen-quorum feedback after `SourceDone`.
    pub peer_report_timeout_ms: u64,
}

/// User-facing request used to start a receiver session.
#[derive(Clone, Debug)]
pub struct ReceiverRequest {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Precomputed transport envelope for receiver control traffic.
    pub route: TransportRoute,
    /// Local node identifier advertised in READY.
    pub local_node_id: usize,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Optional file sink populated with completed blocks at their object offsets.
    pub sink_file: Option<Arc<Mutex<File>>>,
    /// Optional progress tracker updated when the first payload unit arrives.
    pub progress: Option<Arc<ReceiverProgress>>,
}

/// Shared receiver-side progress markers exported to integration harnesses.
#[derive(Debug, Default)]
pub struct ReceiverProgress {
    first_payload_unit_at: OnceLock<Instant>,
    object_complete_at: OnceLock<Instant>,
}

impl ReceiverProgress {
    /// Record when the first payload unit arrived at the receiver.
    pub fn mark_first_payload_unit(&self) {
        let _ = self.first_payload_unit_at.set(Instant::now());
    }

    /// Return the timestamp of the first payload unit, if any.
    pub fn first_payload_unit_at(&self) -> Option<Instant> {
        self.first_payload_unit_at.get().copied()
    }

    /// Record when the receiver first reached local object completion.
    pub fn mark_object_complete(&self) {
        let _ = self.object_complete_at.set(Instant::now());
    }

    /// Return the timestamp of local object completion, if any.
    #[allow(dead_code)]
    pub fn object_complete_at(&self) -> Option<Instant> {
        self.object_complete_at.get().copied()
    }
}

/// Fully derived sender configuration passed to the sender task.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Shared per-session settings.
    pub session: SessionConfig,
    /// Precomputed transport envelope for sender traffic.
    pub route: TransportRoute,
    /// Optional pacing configuration applied to outbound data.
    pub pacing: Option<TokenBucketSpec>,
    /// Receiver node IDs expected to provide lossless feedback.
    pub receiver_ids: Vec<usize>,
    /// Source bytes used to build payload blocks.
    pub source_buffer: Bytes,
    /// Validated manifest emitted during the READY handshake.
    pub manifest: LosslessSessionManifest,
    /// Maximum time to wait for READY frames during session start.
    pub ready_grace_ms: u64,
    /// Maximum time to wait for frozen-quorum feedback after `SourceDone`.
    pub peer_report_timeout_ms: u64,
    /// Optional topology-ready gate shared by newly spawned senders.
    pub topology_ready: Option<watch::Receiver<bool>>,
    /// Optional local Cloudcast tree-striping mode.
    pub cloudcast: Option<CloudcastRuntimeConfig>,
}

/// Fully derived receiver configuration passed to the receiver task.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    /// Session identifier used for frame routing.
    pub session_id: SessionId,
    /// Precomputed transport envelope for receiver control traffic.
    pub route: TransportRoute,
    /// Local node identifier advertised in READY.
    pub local_node_id: usize,
    /// Optional in-memory sink populated with completed blocks.
    pub sink_buffer: Option<Arc<Mutex<Vec<u8>>>>,
    /// Optional file sink populated with completed blocks at their object offsets.
    pub sink_file: Option<Arc<Mutex<File>>>,
    /// Optional progress tracker updated when the first payload unit arrives.
    pub progress: Option<Arc<ReceiverProgress>>,
    /// Maximum time to keep a passive-complete receiver alive while waiting for later rounds.
    pub peer_report_timeout_ms: u64,
    /// Whether FEC manifests are accepted by this runtime.
    pub fec_enabled: bool,
    /// Optional local Cloudcast tree-boundary mode.
    pub cloudcast: Option<CloudcastRuntimeConfig>,
    /// Timing validated if and when a carousel manifest is installed.
    pub carousel: CarouselRuntimeConfig,
    /// Shared process-wide dense METTLE decoder admission pool.
    pub mettle_decoder_budget: Option<MettleDecoderBudget>,
}

/// Handle for interacting with the background lossless runtime actor.
#[derive(Clone, Debug)]
pub struct LosslessRuntimeHandle {
    message_sender: mpsc::Sender<LosslessRuntimeMessage>,
}

struct SessionEntry {
    control_inbox: mpsc::Sender<InboundFrame>,
    data_inbox: Option<mpsc::Sender<InboundFrame>>,
    state_sender: watch::Sender<SessionState>,
    abort_handle: tokio::task::AbortHandle,
}

impl LosslessRuntimeHandle {
    /// Spawn a new runtime actor bound to the provided processor handle.
    pub fn new(processors: ProcessorHandle, config: LosslessConfig) -> Self {
        let (message_sender, message_receiver) = mpsc::channel(config.runtime_message_capacity);
        let runtime =
            LosslessRuntime::new(processors, config, message_sender.clone(), message_receiver);

        tokio::spawn(async move {
            let mut runtime = runtime;
            runtime.run().await;
        });

        Self { message_sender }
    }

    /// Start a sender task after deriving and validating its manifest.
    pub async fn start_sender(
        &self,
        cfg: SenderRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .message_sender
            .send(LosslessRuntimeMessage::StartSender {
                cfg,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return Err(StartError::RuntimeChannelClosed);
        }

        reply_rx
            .await
            .unwrap_or(Err(StartError::RuntimeChannelClosed))
    }

    /// Start a receiver task for a precomputed session identifier.
    pub async fn start_receiver(
        &self,
        cfg: ReceiverRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .message_sender
            .send(LosslessRuntimeMessage::StartReceiver {
                cfg,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return Err(StartError::RuntimeChannelClosed);
        }
        reply_rx
            .await
            .unwrap_or(Err(StartError::RuntimeChannelClosed))
    }

    /// Deliver one already-decoded frame to a running session task.
    pub async fn deliver(&self, session: SessionId, frame: InboundFrame) {
        let _ = self
            .message_sender
            .send(LosslessRuntimeMessage::Deliver { session, frame })
            .await;
    }

    /// Update the topology-ready gate shared by newly spawned senders.
    pub async fn set_topology_ready(&self, ready: bool) {
        let _ = self
            .message_sender
            .send(LosslessRuntimeMessage::SetTopologyReady { ready })
            .await;
    }
}

/// Background actor that owns live sender and receiver tasks.
struct LosslessRuntime {
    processors: ProcessorHandle,
    config: LosslessConfig,
    sessions: AHashMap<SessionId, SessionEntry>,
    completed_receivers: AHashMap<SessionId, CompletedReceiverReplay>,
    topology_ready_sender: watch::Sender<bool>,
    topology_ready: bool,
    message_sender: mpsc::Sender<LosslessRuntimeMessage>,
    message_receiver: mpsc::Receiver<LosslessRuntimeMessage>,
    mettle_decoder_budget: Option<MettleDecoderBudget>,
}

impl LosslessRuntime {
    /// Build a new runtime actor with an initially closed topology gate.
    fn new(
        processors: ProcessorHandle,
        config: LosslessConfig,
        message_sender: mpsc::Sender<LosslessRuntimeMessage>,
        message_receiver: mpsc::Receiver<LosslessRuntimeMessage>,
    ) -> Self {
        let (topology_ready_sender, _) = watch::channel(false);
        let mettle_decoder_budget = match MettleDecoderBudget::from_lossless(&config) {
            Ok(budget) => Some(budget),
            Err(error) => {
                warn!(%error, "Lossless runtime disabled METTLE decoder admission because its budget is invalid");
                None
            }
        };

        Self {
            processors,
            config,
            sessions: AHashMap::default(),
            completed_receivers: AHashMap::default(),
            topology_ready_sender,
            topology_ready: false,
            message_sender,
            message_receiver,
            mettle_decoder_budget,
        }
    }

    /// Main command loop for the runtime actor.
    async fn run(&mut self) {
        while let Some(message) = self.message_receiver.recv().await {
            match message {
                LosslessRuntimeMessage::StartSender { cfg, reply } => {
                    let _ = reply.send(self.start_sender_session(cfg));
                }
                LosslessRuntimeMessage::StartReceiver { cfg, reply } => {
                    let _ = reply.send(self.start_receiver_session(cfg));
                }
                LosslessRuntimeMessage::Abort { session_id } => {
                    self.abort_session(session_id);
                }
                LosslessRuntimeMessage::Deliver { session, frame } => {
                    self.deliver_frame(session, frame).await;
                }
                LosslessRuntimeMessage::ReceiverCompleted {
                    session_id,
                    replay,
                    ack,
                } => {
                    self.cache_completed_receiver(session_id, replay);
                    let _ = ack.send(());
                }
                LosslessRuntimeMessage::SessionExited {
                    session_id,
                    outcome,
                } => {
                    self.finish_session(session_id, outcome);
                }
                LosslessRuntimeMessage::SetTopologyReady { ready } => {
                    self.set_topology_ready(ready);
                }
            }
        }
    }

    /// Forward one inbound frame to the matching session task.
    async fn deliver_frame(&mut self, session: SessionId, frame: InboundFrame) {
        if let Some(header) = lossless_session::peek_header(&frame.bytes) {
            if header.session_id != session {
                warn!(
                    session_id = session,
                    wire_session_id = header.session_id,
                    kind = header.kind,
                    ctrl_kind = header.ctrl_kind,
                    "Lossless runtime: dropping frame from a stale session incarnation."
                );
                return;
            }
            if header.version != lossless_session::LOSSLESS_SESSION_VERSION {
                warn!(
                    session_id = session,
                    wire_session_id = header.session_id,
                    observed_version = header.version,
                    expected_version = lossless_session::LOSSLESS_SESSION_VERSION,
                    kind = header.kind,
                    ctrl_kind = header.ctrl_kind,
                    "Lossless runtime: dropping frame with unsupported session version."
                );
                return;
            }
        }

        let decoded_control =
            lossless_session::decode_control(&frame.bytes).map(|(_, control)| control);
        let control_kind = decoded_control.as_ref().map(control_kind_name);

        // Once the receiver has installed its final carousel replay, probes
        // and completion frames belong to that replay even until SessionExited
        // removes the now-idle live inboxes. This closes the handoff window in
        // which a control could otherwise be accepted by an inbox nobody reads.
        if matches!(
            decoded_control,
            Some(LosslessSessionControl::AckProbe { .. } | LosslessSessionControl::SessionComplete)
        ) && matches!(
            self.completed_receivers.get(&session),
            Some(CompletedReceiverReplay::Carousel { .. })
        ) && self.replay_completed_receiver(session, frame.clone()).await
        {
            return;
        }

        match self.deliver_live_receiver(session, &frame) {
            LiveDeliveryOutcome::Delivered => return,
            LiveDeliveryOutcome::Full if control_kind.is_none() => {
                debug!(
                    session_id = session,
                    "Lossless runtime dropped data from a full receiver inbox"
                );
                return;
            }
            LiveDeliveryOutcome::Full
            | LiveDeliveryOutcome::Closed
            | LiveDeliveryOutcome::Missing => {}
        }

        if self.replay_completed_receiver(session, frame.clone()).await {
            return;
        }

        if let Some(control_kind) = control_kind {
            warn!(
                session_id = session,
                peer_id = frame.peer_id,
                control_kind,
                "Lossless runtime: no live session while delivering control frame."
            );
        }
        warn!(
            session_id = session,
            "Lossless runtime: no session for inbound frame."
        );
    }

    fn abort_session(&mut self, session_id: SessionId) {
        if let Some(entry) = self.sessions.remove(&session_id) {
            entry.abort_handle.abort();
            let _ = entry
                .state_sender
                .send(SessionState::Finished(SessionOutcome::Aborted));
        }
        self.completed_receivers.remove(&session_id);
    }

    fn finish_session(&mut self, session_id: SessionId, outcome: SessionOutcome) {
        let keep_completed_replay = outcome == SessionOutcome::Completed;
        if let Some(entry) = self.sessions.remove(&session_id) {
            let _ = entry.state_sender.send(SessionState::Finished(outcome));
        }
        if !keep_completed_replay {
            self.completed_receivers.remove(&session_id);
        }
    }

    /// Derive sender state, allocate an ingress channel, and spawn the sender task.
    fn start_sender_session(
        &mut self,
        req: SenderRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let session = req.session;
        let sid = session.session_id;
        if self.sessions.contains_key(&sid) {
            return Err(StartError::SessionAlreadyActive { session_id: sid });
        }
        self.completed_receivers.remove(&sid);

        let cloudcast = self.derive_cloudcast_config()?;
        let block_size = fec_policy::validate_block_size(session.block_size)?;
        let policy = if cloudcast.is_some() {
            fec_policy::SenderPolicy {
                mode: lossless_session::LosslessSessionMode::Plain,
            }
        } else {
            fec_policy::derive_sender_policy(&self.config, block_size, req.total_bytes)?
        };
        let plan = BlockPlan::new(req.total_bytes, session.block_size).map_err(|_| {
            PreflightError::InvalidBlockSize {
                value: session.block_size,
            }
        })?;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: req.total_bytes,
            total_blocks: plan.total_blocks(),
            mode: policy.mode,
        };
        let mut cfg = SenderConfig {
            session,
            route: req.route,
            pacing: req.pacing,
            receiver_ids: req.receiver_ids,
            source_buffer: req.source_buffer,
            manifest,
            ready_grace_ms: req.ready_grace_ms,
            peer_report_timeout_ms: req.peer_report_timeout_ms,
            topology_ready: None,
            cloudcast,
        };
        if !self.topology_ready {
            cfg.topology_ready = Some(self.topology_ready_sender.subscribe());
        }

        let processors = self.processors.clone();
        let (control_inbox, control_inbox_receiver) =
            mpsc::channel(self.config.session_control_inbox_capacity);
        let (state_sender, state_receiver) = watch::channel(SessionState::Running);

        let carousel = CarouselRuntimeConfig::from_lossless(&self.config);
        let task = tokio::spawn(sender::run_with_carousel(
            cfg,
            control_inbox_receiver,
            processors,
            carousel,
        ));
        let abort_handle = task.abort_handle();
        let message_sender = self.message_sender.clone();
        info!(
            session_id = sid,
            topology_ready = self.topology_ready,
            "Lossless runtime started sender session task"
        );
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(outcome) => outcome,
                Err(_) => SessionOutcome::Aborted,
            };
            let _ = message_sender
                .send(LosslessRuntimeMessage::SessionExited {
                    session_id: sid,
                    outcome,
                })
                .await;
        });

        self.sessions.insert(
            sid,
            SessionEntry {
                control_inbox,
                data_inbox: None,
                state_sender,
                abort_handle,
            },
        );

        Ok(LosslessSessionHandle::new(
            sid,
            state_receiver,
            self.message_sender.clone(),
        ))
    }

    /// Allocate an ingress channel and spawn the receiver task.
    fn start_receiver_session(
        &mut self,
        req: ReceiverRequest,
    ) -> Result<LosslessSessionHandle, StartError> {
        let sid = req.session_id;
        let local_node_id = req.local_node_id;
        if self.sessions.contains_key(&sid) {
            return Err(StartError::SessionAlreadyActive { session_id: sid });
        }
        self.completed_receivers.remove(&sid);

        let cfg = ReceiverConfig {
            session_id: req.session_id,
            route: req.route,
            local_node_id: req.local_node_id,
            sink_buffer: req.sink_buffer,
            sink_file: req.sink_file,
            progress: req.progress,
            peer_report_timeout_ms: self.config.peer_report_timeout_ms,
            fec_enabled: self.config.fec_enabled,
            cloudcast: self.derive_cloudcast_config()?,
            carousel: CarouselRuntimeConfig::from_lossless(&self.config),
            mettle_decoder_budget: self.mettle_decoder_budget.clone(),
        };
        let processors = self.processors.clone();

        let (control_inbox, control_inbox_receiver) =
            mpsc::channel(self.config.session_control_inbox_capacity);
        let (data_inbox, data_inbox_receiver) = mpsc::channel(self.config.session_inbox_capacity);
        let (state_sender, state_receiver) = watch::channel(SessionState::Running);

        let message_sender = self.message_sender.clone();
        let task = tokio::spawn(receiver::run_with_runtime(
            cfg,
            control_inbox_receiver,
            data_inbox_receiver,
            processors,
            Some(message_sender.clone()),
        ));
        let abort_handle = task.abort_handle();
        info!(
            session_id = sid,
            local_node_id, "Lossless runtime started receiver session task"
        );
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(outcome) => outcome,
                Err(_) => SessionOutcome::Aborted,
            };
            let _ = message_sender
                .send(LosslessRuntimeMessage::SessionExited {
                    session_id: sid,
                    outcome,
                })
                .await;
        });

        self.sessions.insert(
            sid,
            SessionEntry {
                control_inbox,
                data_inbox: Some(data_inbox),
                state_sender,
                abort_handle,
            },
        );

        Ok(LosslessSessionHandle::new(
            sid,
            state_receiver,
            self.message_sender.clone(),
        ))
    }

    /// Publish the current topology-ready state to newly waiting senders.
    fn set_topology_ready(&mut self, ready: bool) {
        self.topology_ready = ready;
        info!(ready, "Lossless runtime updated topology-ready gate");
        let _ = self.topology_ready_sender.send(ready);
    }

    async fn replay_completed_receiver(&mut self, session: SessionId, frame: InboundFrame) -> bool {
        let expired = self
            .completed_receivers
            .get(&session)
            .is_some_and(|replay| replay.retain_until() <= tokio::time::Instant::now());
        if expired {
            self.completed_receivers.remove(&session);
            return false;
        }

        let Some(replay) = self.completed_receivers.get(&session) else {
            return false;
        };
        let Some((_, received_control)) = lossless_session::decode_control(&frame.bytes) else {
            return false;
        };
        match replay {
            CompletedReceiverReplay::Plain {
                round_id: replay_round_id,
                route,
                report,
                ..
            } => {
                let LosslessSessionControl::SourceDone { round_id } = received_control else {
                    return false;
                };
                if round_id != *replay_round_id {
                    debug!(
                        session_id = session,
                        round_id,
                        replay_round_id,
                        "Lossless runtime dropped out-of-round replay attempt for a completed plain receiver"
                    );
                    return false;
                }
                control::send_control(
                    &self.processors,
                    control::FrameRoute {
                        session_id: session,
                        tree_id: None,
                        src_ip: route.src_ip,
                        src_port: route.src_port,
                        dst_ip: route.dst_ip,
                        dst_port: route.dst_port,
                    },
                    &LosslessSessionControl::Need {
                        round_id,
                        report: report.clone(),
                    },
                )
                .await;
                true
            }
            CompletedReceiverReplay::Fec {
                round_id: replay_round_id,
                route,
                report,
                ..
            } => {
                let LosslessSessionControl::SourceDone { round_id } = received_control else {
                    return false;
                };
                if round_id != *replay_round_id {
                    debug!(
                        session_id = session,
                        round_id,
                        replay_round_id,
                        "Lossless runtime dropped out-of-round replay attempt for a completed FEC receiver"
                    );
                    return false;
                }
                control::send_control(
                    &self.processors,
                    control::FrameRoute {
                        session_id: session,
                        tree_id: None,
                        src_ip: route.src_ip,
                        src_port: route.src_port,
                        dst_ip: route.dst_ip,
                        dst_port: route.dst_port,
                    },
                    &LosslessSessionControl::Need {
                        round_id,
                        report: report.clone(),
                    },
                )
                .await;
                true
            }
            CompletedReceiverReplay::Carousel {
                route,
                ack,
                local_node_id,
                ..
            } => match received_control {
                LosslessSessionControl::AckProbe { target_peer_id }
                    if u64::try_from(*local_node_id).ok() == Some(target_peer_id) =>
                {
                    control::send_control(
                        &self.processors,
                        control::FrameRoute {
                            session_id: session,
                            tree_id: None,
                            src_ip: route.src_ip,
                            src_port: route.src_port,
                            dst_ip: route.dst_ip,
                            dst_port: route.dst_port,
                        },
                        &LosslessSessionControl::BlockAck { ack: ack.clone() },
                    )
                    .await;
                    true
                }
                LosslessSessionControl::SessionComplete => true,
                _ => false,
            },
        }
    }

    fn cache_completed_receiver(&mut self, session_id: SessionId, replay: CompletedReceiverReplay) {
        let now = tokio::time::Instant::now();
        self.completed_receivers
            .retain(|_, cached| cached.retain_until() > now);
        if replay.retain_until() > now {
            self.completed_receivers.insert(session_id, replay);
        }
    }

    fn deliver_live_receiver(
        &self,
        session: SessionId,
        frame: &InboundFrame,
    ) -> LiveDeliveryOutcome {
        let Some(entry) = self.sessions.get(&session) else {
            return LiveDeliveryOutcome::Missing;
        };

        let inbox = if lossless_session::decode_control(&frame.bytes).is_some() {
            entry.control_inbox.clone()
        } else if let Some(data_inbox) = &entry.data_inbox {
            data_inbox.clone()
        } else {
            warn!(
                session_id = session,
                "Lossless runtime: dropping unexpected data frame for sender session."
            );
            return LiveDeliveryOutcome::Delivered;
        };
        match inbox.try_send(frame.clone()) {
            Ok(()) => LiveDeliveryOutcome::Delivered,
            Err(mpsc::error::TrySendError::Full(_)) => LiveDeliveryOutcome::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => LiveDeliveryOutcome::Closed,
        }
    }

    fn derive_cloudcast_config(&self) -> Result<Option<CloudcastRuntimeConfig>, StartError> {
        if !self.config.session_mode.is_cloudcast() {
            return Ok(None);
        }
        let tree_ids = fec_policy::derive_sender_tree_ids(&self.config)?;
        let stripe_tree_ids = derive_cloudcast_stripe_tree_ids(&self.config, &tree_ids);
        let used_tree_ids = cloudcast_used_tree_ids(&stripe_tree_ids);
        Ok(Some(CloudcastRuntimeConfig::new(
            used_tree_ids,
            stripe_tree_ids,
        )))
    }
}

fn normalize_cloudcast_tree_weights(tree_count: usize, configured: &[f64]) -> Vec<f64> {
    if tree_count == 0 {
        return Vec::new();
    }
    if configured.len() != tree_count {
        return vec![1.0; tree_count];
    }
    configured
        .iter()
        .map(|weight| {
            if weight.is_finite() && *weight > 0.0 {
                *weight
            } else {
                1.0
            }
        })
        .collect()
}

fn derive_cloudcast_stripe_tree_ids(config: &LosslessConfig, tree_ids: &[u16]) -> Vec<u16> {
    let explicit = config
        .cloudcast_stripe_tree_ids
        .iter()
        .copied()
        .filter(|tree_id| tree_ids.contains(tree_id))
        .collect::<Vec<_>>();
    if !explicit.is_empty() {
        return explicit;
    }

    let stripe_count = if config.cloudcast_stripes > 0 {
        config.cloudcast_stripes
    } else {
        tree_ids.len()
    };
    quantize_cloudcast_tree_weights(
        tree_ids,
        &normalize_cloudcast_tree_weights(tree_ids.len(), &config.fec_default_tree_weights),
        stripe_count,
    )
}

fn quantize_cloudcast_tree_weights(
    tree_ids: &[u16],
    weights: &[f64],
    stripe_count: usize,
) -> Vec<u16> {
    if tree_ids.is_empty() || stripe_count == 0 {
        return Vec::new();
    }
    let normalized_weights = if weights.len() == tree_ids.len() {
        weights.to_vec()
    } else {
        vec![1.0; tree_ids.len()]
    };
    let total_weight = normalized_weights
        .iter()
        .copied()
        .filter(|weight| weight.is_finite() && *weight > 0.0)
        .sum::<f64>();
    if total_weight <= 0.0 {
        return (0..stripe_count)
            .map(|idx| tree_ids[idx % tree_ids.len()])
            .collect();
    }

    let mut allocations = normalized_weights
        .iter()
        .enumerate()
        .map(|(idx, weight)| {
            let safe_weight = if weight.is_finite() && *weight > 0.0 {
                *weight
            } else {
                0.0
            };
            let exact = safe_weight / total_weight * stripe_count as f64;
            let floor = exact.floor() as usize;
            (idx, floor, exact - floor as f64)
        })
        .collect::<Vec<_>>();
    let assigned = allocations
        .iter()
        .map(|(_, floor, _)| *floor)
        .sum::<usize>();
    let remaining = stripe_count.saturating_sub(assigned);
    allocations.sort_by(|left, right| {
        right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| tree_ids[left.0].cmp(&tree_ids[right.0]))
    });
    let allocation_count = allocations.len();
    for idx in 0..remaining {
        allocations[idx % allocation_count].1 += 1;
    }
    allocations.sort_by_key(|(idx, _, _)| *idx);

    let mut stripes = Vec::with_capacity(stripe_count);
    for (idx, count, _) in allocations {
        stripes.extend(std::iter::repeat_n(tree_ids[idx], count));
    }
    stripes
}

fn cloudcast_used_tree_ids(stripe_tree_ids: &[u16]) -> Vec<u16> {
    let mut tree_ids = stripe_tree_ids.to_vec();
    tree_ids.sort_unstable();
    tree_ids.dedup();
    tree_ids
}

fn control_kind_name(control: &LosslessSessionControl) -> &'static str {
    match control {
        LosslessSessionControl::Manifest { .. } => "Manifest",
        LosslessSessionControl::Ready => "Ready",
        LosslessSessionControl::Need { .. } => "Need",
        LosslessSessionControl::SourceDone { .. } => "SourceDone",
        LosslessSessionControl::BlockAck { .. } => "BlockAck",
        LosslessSessionControl::AckProbe { .. } => "AckProbe",
        LosslessSessionControl::SessionComplete => "SessionComplete",
        LosslessSessionControl::DepartureCheckpoint { .. } => "DepartureCheckpoint",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveDeliveryOutcome {
    Delivered,
    Full,
    Closed,
    Missing,
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use tokio::sync::watch;
    use tokio::time::timeout;

    use nextmini_messages::lossless_session::{self, BlockAck, FecFeedbackMode, NeedReport};
    use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

    use super::*;
    use crate::node::NodeIdExt;
    use crate::node::config::LocalConfig;
    use crate::node::packet::Packet;

    const SOURCE_NODE_ID: usize = 61;
    const RECEIVER_NODE_ID: usize = 62;

    #[test]
    fn mettle_decoder_budget_defaults_and_fifth_permit_rejection_are_checked() {
        let config = LosslessConfig::default();
        let budget = MettleDecoderBudget::from_lossless(&config).expect("valid default budget");
        assert_eq!(budget.reservation_bytes(), 192 * 1024 * 1024);
        assert_eq!(budget.available_permits(), 4);

        let mut permits = Vec::new();
        for _ in 0..4 {
            permits.push(budget.try_acquire().expect("one of four decoder permits"));
        }
        assert_eq!(budget.available_permits(), 0);
        assert!(matches!(
            budget.try_acquire(),
            Err(MettleDecoderBudgetError::Exhausted)
        ));

        permits.pop();
        assert!(budget.try_acquire().is_ok());
    }

    #[test]
    fn mettle_decoder_budget_rejects_invalid_configuration() {
        let zero_reservation = LosslessConfig {
            mettle_decoder_reservation_bytes: 0,
            ..LosslessConfig::default()
        };
        assert!(matches!(
            MettleDecoderBudget::from_lossless(&zero_reservation),
            Err(MettleDecoderBudgetError::ZeroReservation)
        ));

        let zero_permits = LosslessConfig {
            mettle_decoder_max_concurrent: 0,
            ..LosslessConfig::default()
        };
        assert!(matches!(
            MettleDecoderBudget::from_lossless(&zero_permits),
            Err(MettleDecoderBudgetError::ZeroPermits)
        ));
    }

    #[tokio::test]
    async fn non_carousel_session_start_ignores_invalid_carousel_timing() {
        let (mut runtime, _packet_rx, route) = test_runtime().await;
        runtime.config.carousel_ack_debounce_ms = 0;

        runtime.config.fec_enabled = false;
        let plain_session_id = 0xA11C_E410;
        let plain = runtime.start_sender_session(SenderRequest {
            session: SessionConfig {
                session_id: plain_session_id,
                block_size: 16,
            },
            route,
            pacing: None,
            receiver_ids: Vec::new(),
            total_bytes: 0,
            source_buffer: Bytes::new(),
            ready_grace_ms: 1,
            peer_report_timeout_ms: 10,
        });
        assert!(plain.is_ok(), "plain startup must ignore carousel timing");
        runtime.abort_session(plain_session_id);

        runtime.config.fec_enabled = true;
        runtime.config.fec_feedback_mode = FecFeedbackMode::Rounds;
        let rounds_session_id = 0xA11C_E411;
        let rounds = runtime.start_sender_session(SenderRequest {
            session: SessionConfig {
                session_id: rounds_session_id,
                block_size: 16,
            },
            route,
            pacing: None,
            receiver_ids: Vec::new(),
            total_bytes: 0,
            source_buffer: Bytes::new(),
            ready_grace_ms: 1,
            peer_report_timeout_ms: 10,
        });
        assert!(rounds.is_ok(), "rounds startup must ignore carousel timing");
        runtime.abort_session(rounds_session_id);

        let receiver_session_id = 0xA11C_E412;
        let receiver = runtime.start_receiver_session(ReceiverRequest {
            session_id: receiver_session_id,
            route,
            local_node_id: RECEIVER_NODE_ID,
            sink_buffer: Some(Arc::new(Mutex::new(Vec::new()))),
            sink_file: None,
            progress: None,
        });
        assert!(
            receiver.is_ok(),
            "receiver startup must defer carousel timing validation until manifest install"
        );
        runtime.abort_session(receiver_session_id);
    }

    #[tokio::test]
    async fn deliver_frame_falls_back_to_completed_plain_receiver_on_source_done() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E401;
        let (inbox, inbox_rx) = mpsc::channel(1);
        drop(inbox_rx);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                control_inbox: inbox,
                data_inbox: None,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert_plain_complete(&mut packet_rx).await;
    }

    #[tokio::test]
    async fn deliver_frame_replays_fec_complete_for_source_done() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E402;

        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_fec_complete(&mut packet_rx).await;

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_fec_complete(&mut packet_rx).await;
    }

    #[tokio::test]
    async fn deliver_frame_replays_final_carousel_ack_for_probe() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E40B;
        let final_ack = BlockAck::Blocks {
            completed_watermark: 3,
            extra_completed: vec![],
        };
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Carousel {
                route,
                ack: final_ack.clone(),
                local_node_id: RECEIVER_NODE_ID,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::AckProbe {
                            target_peer_id: u64::try_from(RECEIVER_NODE_ID + 1)
                                .expect("test receiver id fits u64"),
                        },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert!(
            timeout(Duration::from_millis(20), packet_rx.recv())
                .await
                .is_err(),
            "completed replay must ignore another receiver's probe"
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::AckProbe {
                            target_peer_id: u64::try_from(RECEIVER_NODE_ID)
                                .expect("test receiver id fits u64"),
                        },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        assert_carousel_ack(&mut packet_rx, final_ack).await;
    }

    #[tokio::test]
    async fn carousel_conformance_full_data_inbox_cannot_deadlock_completion_handoff() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E40D;
        let (control_inbox, _control_rx) = mpsc::channel(1);
        let (data_inbox, _data_rx) = mpsc::channel(1);
        let tail = InboundFrame {
            bytes: lossless_session::encode_block_symbol(session_id, 0, 4, 0, b"tail"),
            peer_id: Some(SOURCE_NODE_ID),
        };
        data_inbox
            .try_send(tail.clone())
            .expect("receiver data inbox should be full before handoff");
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        runtime.sessions.insert(
            session_id,
            SessionEntry {
                control_inbox,
                data_inbox: Some(data_inbox),
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );

        let message_sender = runtime.message_sender.clone();
        let runtime_task = tokio::spawn(async move {
            runtime.run().await;
        });
        message_sender
            .send(LosslessRuntimeMessage::Deliver {
                session: session_id,
                frame: tail,
            })
            .await
            .expect("tail delivery should enqueue ahead of completion handoff");

        let final_ack = BlockAck::Blocks {
            completed_watermark: 1,
            extra_completed: Vec::new(),
        };
        let (handoff_ack_tx, handoff_ack_rx) = oneshot::channel();
        message_sender
            .send(LosslessRuntimeMessage::ReceiverCompleted {
                session_id,
                replay: CompletedReceiverReplay::Carousel {
                    route,
                    ack: final_ack.clone(),
                    local_node_id: RECEIVER_NODE_ID,
                    retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
                },
                ack: handoff_ack_tx,
            })
            .await
            .expect("completion handoff should enqueue behind the tail");
        timeout(Duration::from_secs(1), handoff_ack_rx)
            .await
            .expect("runtime actor deadlocked behind a full receiver data inbox")
            .expect("runtime dropped the handoff acknowledgement");

        message_sender
            .send(LosslessRuntimeMessage::Deliver {
                session: session_id,
                frame: InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::AckProbe {
                            target_peer_id: u64::try_from(RECEIVER_NODE_ID)
                                .expect("receiver id fits u64"),
                        },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            })
            .await
            .expect("post-handoff probe should enqueue");
        assert_carousel_ack(&mut packet_rx, final_ack).await;

        runtime_task.abort();
        abort_task.abort();
    }

    #[tokio::test]
    async fn completed_carousel_replay_expires_at_configured_deadline() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E40C;
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Carousel {
                route,
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    extra_completed: vec![],
                },
                local_node_id: RECEIVER_NODE_ID,
                retain_until: tokio::time::Instant::now(),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::AckProbe {
                            target_peer_id: u64::try_from(RECEIVER_NODE_ID)
                                .expect("test receiver id fits u64"),
                        },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        assert!(!runtime.completed_receivers.contains_key(&session_id));
        assert!(
            timeout(Duration::from_millis(50), packet_rx.recv())
                .await
                .is_err(),
            "expired replay must not emit a BlockAck"
        );
    }

    #[tokio::test]
    async fn completed_replay_insert_sweeps_expired_entries_in_every_mode() {
        let (mut runtime, _packet_rx, route) = test_runtime().await;
        let now = tokio::time::Instant::now();
        runtime.completed_receivers.insert(
            1,
            CompletedReceiverReplay::Plain {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: now,
            },
        );
        runtime.completed_receivers.insert(
            2,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: now,
            },
        );
        runtime.completed_receivers.insert(
            3,
            CompletedReceiverReplay::Carousel {
                route,
                ack: BlockAck::Blocks {
                    completed_watermark: 1,
                    extra_completed: Vec::new(),
                },
                local_node_id: RECEIVER_NODE_ID,
                retain_until: now,
            },
        );

        runtime.cache_completed_receiver(
            4,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route,
                report: NeedReport::Complete,
                retain_until: now + Duration::from_secs(1),
            },
        );

        assert_eq!(runtime.completed_receivers.len(), 1);
        assert!(runtime.completed_receivers.contains_key(&4));
    }

    #[tokio::test]
    async fn deliver_frame_replays_completed_receiver_for_the_cached_round_only() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E40A;

        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route,
                report: NeedReport::Complete,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale round replay must be dropped"
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 1 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert_plain_complete_for_round(&mut packet_rx, 1).await;

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 2 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "future round replay must be dropped"
        );
    }

    #[tokio::test]
    async fn deliver_frame_prefers_live_plain_inbox_over_completed_replay_during_handoff() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E403;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                control_inbox: inbox,
                data_inbox: None,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Plain {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "runtime-owned replay must not preempt a live receiver inbox during handoff"
        );
        let delivered = timeout(Duration::from_secs(2), inbox_rx.recv())
            .await
            .expect("timed out waiting for live plain handoff delivery")
            .expect("live plain inbox should receive the duplicate frame");
        assert!(matches!(
            lossless_session::decode_control(&delivered.bytes),
            Some((_, LosslessSessionControl::SourceDone { round_id: 0 }))
        ));
    }

    #[tokio::test]
    async fn deliver_frame_prefers_live_fec_inbox_over_completed_replay_during_handoff() {
        let (mut runtime, mut packet_rx, route) = test_runtime().await;
        let session_id = 0xA11C_E404;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                control_inbox: inbox,
                data_inbox: None,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );
        runtime.completed_receivers.insert(
            session_id,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route,
                report: NeedReport::Complete,
                retain_until: tokio::time::Instant::now() + Duration::from_secs(1),
            },
        );

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes: lossless_session::encode_control(
                        session_id,
                        &LosslessSessionControl::SourceDone { round_id: 0 },
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "runtime-owned replay must not preempt a live receiver inbox during handoff"
        );
        let delivered = timeout(Duration::from_secs(2), inbox_rx.recv())
            .await
            .expect("timed out waiting for live FEC handoff delivery")
            .expect("live FEC inbox should receive the duplicate frame");
        assert!(matches!(
            lossless_session::decode_control(&delivered.bytes),
            Some((_, LosslessSessionControl::SourceDone { round_id: 0 }))
        ));
    }

    #[tokio::test]
    async fn deliver_frame_drops_unsupported_version_before_dispatch() {
        let (mut runtime, _packet_rx, _route) = test_runtime().await;
        let session_id = 0xA11C_E405;
        let (inbox, mut inbox_rx) = mpsc::channel(1);
        let (state_sender, _) = watch::channel(SessionState::Running);
        let abort_task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        runtime.sessions.insert(
            session_id,
            SessionEntry {
                control_inbox: inbox,
                data_inbox: None,
                state_sender,
                abort_handle: abort_task.abort_handle(),
            },
        );

        let mut bytes =
            lossless_session::encode_control(session_id, &LosslessSessionControl::Ready);
        bytes[4] = lossless_session::LOSSLESS_SESSION_VERSION - 1;

        runtime
            .deliver_frame(
                session_id,
                InboundFrame {
                    bytes,
                    peer_id: Some(SOURCE_NODE_ID),
                },
            )
            .await;

        abort_task.abort();
        assert!(
            timeout(Duration::from_millis(100), inbox_rx.recv())
                .await
                .is_err(),
            "unsupported-version frames should be dropped before session delivery"
        );
    }

    async fn test_runtime() -> (LosslessRuntime, mpsc::Receiver<Packet>, TransportRoute) {
        let cfg = LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 2048,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        };
        let processors = ProcessorHandle::new(cfg.clone());
        processors
            .update_routing_table(vec![RoutingTableEntry {
                route_id: 1,
                next_hops: vec![cfg.node_id],
                src_node_id: cfg.node_id,
                dst_node_id: SOURCE_NODE_ID,
                forward_mode: RouteForwardingMode::Unicast,
            }])
            .await;

        let route = TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4760,
            dst_port: 5760,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (message_sender, message_receiver) =
            mpsc::channel(cfg.lossless_runtime_config.runtime_message_capacity);
        (
            LosslessRuntime::new(
                processors,
                cfg.lossless_runtime_config.clone(),
                message_sender,
                message_receiver,
            ),
            packet_rx,
            route,
        )
    }

    async fn assert_plain_complete(packet_rx: &mut mpsc::Receiver<Packet>) {
        assert_plain_complete_for_round(packet_rx, 0).await;
    }

    async fn assert_plain_complete_for_round(
        packet_rx: &mut mpsc::Receiver<Packet>,
        round_id: u32,
    ) {
        let packet = tokio::time::timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for replayed plain status")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("plain status packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("plain status should decode");
        assert_eq!(
            control,
            LosslessSessionControl::Need {
                round_id,
                report: NeedReport::Complete,
            }
        );
    }

    async fn assert_fec_complete(packet_rx: &mut mpsc::Receiver<Packet>) {
        let packet = tokio::time::timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for replayed fec status")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("fec status packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("fec status should decode");
        assert_eq!(
            control,
            LosslessSessionControl::Need {
                round_id: 0,
                report: NeedReport::Complete,
            }
        );
    }

    async fn assert_carousel_ack(packet_rx: &mut mpsc::Receiver<Packet>, expected: BlockAck) {
        let packet = tokio::time::timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for replayed BlockAck")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("BlockAck packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("BlockAck should decode");
        assert_eq!(control, LosslessSessionControl::BlockAck { ack: expected });
    }
}
